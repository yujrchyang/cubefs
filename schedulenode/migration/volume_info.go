package migration

import (
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/unit"
	"golang.org/x/net/context"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	VolLastUpdateIntervalTime = 30 * 60 // s
)

const (
	VolInit uint32 = iota
	VolRunning
	VolClosing
)

const (
	rootDir                    = "/"
	refreshInodeFilterDuration = 120
	dirSeparator               = ","
)

type VolumeInfo struct {
	sync.RWMutex
	Name               string
	ClusterName        string
	State              uint32
	LastUpdate         int64
	RunningMPCnt       uint32
	RunningMpIds       map[uint64]struct{}
	RunningInoCnt      uint32
	Mcc                *ControlConfig
	MetaClient         *meta.MetaWrapper
	DataClient         *data.ExtentClient
	NormalDataClient   *data.ExtentClient
	GetLayerPolicies   func(cluster, volName string) (layerPolicies []interface{}, exist bool)
	GetDpMediumType    func(cluster, volName string, dpId uint64) (mediumType string)
	GetMigrationConfig func(cluster, volName string) (volumeConfig proto.MigrationConfig)
	inodeFilter        sync.Map
	stopC              chan struct{}
}

func NewVolumeInfo(clusterName, volName string, nodes []string, mcc *ControlConfig, extentClientType data.ExtentClientType,
	getLayerPolicies func(cluster, volName string) (layerPolicies []interface{}, exist bool),
	getDpMediumType func(cluster, volName string, dpId uint64) (mediumType string),
	getMigrationConfig func(cluster, volName string) (volumeConfig proto.MigrationConfig)) (vol *VolumeInfo, err error) {
	vol = &VolumeInfo{
		stopC: make(chan struct{}, 0),
	}
	if err = vol.Init(clusterName, volName, nodes, mcc, extentClientType); err != nil {
		return
	}
	vol.GetLayerPolicies = getLayerPolicies
	vol.GetDpMediumType = getDpMediumType
	vol.GetMigrationConfig = getMigrationConfig
	if extentClientType == data.Smart {
		vol.updateInodeFilter()
		go vol.refreshInodeFilter()
		err = vol.createNormalExtentClient(volName, nodes)
	}
	log.LogDebugf("new volume info cluster(%v) volume(%v) extentClientType(%v)", clusterName, volName, extentClientType)
	return
}

func (vol *VolumeInfo) Init(clusterName, volName string, nodes []string, mcc *ControlConfig, extentClientType data.ExtentClientType) (err error) {
	vol.Name = volName
	vol.ClusterName = clusterName
	vol.Mcc = mcc
	vol.RunningMpIds = make(map[uint64]struct{}, 0)
	var metaConfig = &meta.MetaConfig{
		Volume:        volName,
		Masters:       nodes,
		Authenticate:  false,
		ValidateOwner: false,
	}

	var metaWrapper *meta.MetaWrapper
	if metaWrapper, err = meta.NewMetaWrapper(metaConfig); err != nil {
		return
	}

	var extentConfig = &data.ExtentConfig{
		Volume:              volName,
		Masters:             nodes,
		FollowerRead:        true,
		TinySize:            unit.MB * 8,
		OnInsertExtentKey:   metaWrapper.InsertExtentKey,
		OnGetExtents:        metaWrapper.GetExtentsNoModifyAccessTime,
		OnTruncate:          metaWrapper.Truncate,
		OnInodeMergeExtents: metaWrapper.InodeMergeExtents_ll,
		MetaWrapper:         metaWrapper,
		ExtentClientType:    extentClientType,
	}
	var extentClient *data.ExtentClient
	if extentClient, err = data.NewExtentClient(extentConfig, nil); err != nil {
		metaWrapper.Close()
		return
	}

	vol.MetaClient = metaWrapper
	vol.DataClient = extentClient
	vol.State = VolInit
	return
}

// 作为hdd向ssd迁移数据的客户端
func (vol *VolumeInfo) createNormalExtentClient(volName string, nodes []string) (err error) {
	var extentConfig = &data.ExtentConfig{
		Volume:              volName,
		Masters:             nodes,
		FollowerRead:        true,
		TinySize:            unit.MB * 8,
		OnInsertExtentKey:   vol.MetaClient.InsertExtentKey,
		OnGetExtents:        vol.MetaClient.GetExtentsNoModifyAccessTime,
		OnTruncate:          vol.MetaClient.Truncate,
		OnInodeMergeExtents: vol.MetaClient.InodeMergeExtents_ll,
		MetaWrapper:         vol.MetaClient,
	}
	var normalExtentClient *data.ExtentClient
	if normalExtentClient, err = data.NewExtentClient(extentConfig, nil); err != nil {
		vol.MetaClient.Close()
		return
	}
	vol.NormalDataClient = normalExtentClient
	return
}

func (vol *VolumeInfo) ReleaseResource() {
	if err := vol.MetaClient.Close(); err != nil {
		log.LogErrorf("vol[%s-%s] close meta wrapper failed:%s", vol.ClusterName, vol.Name, err.Error())
	}
	if err := vol.DataClient.Close(context.Background()); err != nil {
		log.LogErrorf("vol[%s-%s] close data wrapper failed:%s", vol.ClusterName, vol.Name, err.Error())
	}
	if vol.NormalDataClient == nil {
		return
	}
	if err := vol.NormalDataClient.Close(context.Background()); err != nil {
		log.LogErrorf("vol[%s-%s] close normalDataClient data wrapper failed:%s", vol.ClusterName, vol.Name, err.Error())
	}
	close(vol.stopC)
}

func (vol *VolumeInfo) ReleaseResourceMeetCondition() bool {
	vol.Lock()
	defer vol.Unlock()
	curTime := time.Now().Unix()
	if !(vol.RunningMPCnt == 0 && vol.RunningInoCnt == 0 && curTime-vol.LastUpdate > VolLastUpdateIntervalTime) {
		return false
	}
	vol.ReleaseResource()
	return true
}

func (vol *VolumeInfo) IsRunning() (flag bool) {
	vol.RLock()
	defer vol.RUnlock()
	if vol.State == VolRunning || vol.State == VolInit {
		flag = true
	}
	return
}

func (vol *VolumeInfo) UpdateVolLastTime() {
	vol.Lock()
	defer vol.Unlock()
	vol.LastUpdate = time.Now().Unix()
}

func (vol *VolumeInfo) UpdateState(state uint32) {
	vol.Lock()
	defer vol.Unlock()
	vol.State = state
}

func (vol *VolumeInfo) UpdateStateToInit() {
	vol.Lock()
	defer vol.Unlock()
	if vol.State == VolClosing {
		vol.State = VolInit
	}
}

func (vol *VolumeInfo) AddMPRunningCnt(mpId uint64) bool {
	vol.Lock()
	defer vol.Unlock()
	if vol.State == VolRunning || vol.State == VolInit {
		vol.RunningMPCnt += 1
		vol.RunningMpIds[mpId] = struct{}{}
		return true
	}
	return false
}

func (vol *VolumeInfo) DelMPRunningCnt(mpId uint64) {
	vol.Lock()
	defer vol.Unlock()
	if vol.RunningMPCnt == 0 {
		return
	}
	vol.RunningMPCnt -= 1
	delete(vol.RunningMpIds, mpId)
	vol.LastUpdate = time.Now().Unix()
}

func (vol *VolumeInfo) AddInodeRunningCnt() bool {
	vol.Lock()
	defer vol.Unlock()
	if vol.State == VolRunning || vol.State == VolInit {
		vol.RunningInoCnt += 1
		return true
	}
	return false
}

func (vol *VolumeInfo) DelInodeRunningCnt() {
	vol.Lock()
	defer vol.Unlock()
	if vol.RunningInoCnt == 0 {
		return
	}
	vol.RunningInoCnt -= 1
}

func (vol *VolumeInfo) GetInodeCheckStep() int {
	return vol.Mcc.InodeCheckStep
}

func (vol *VolumeInfo) GetInodeConcurrentPerMP() int {
	return vol.Mcc.InodeConcurrent
}

func (vol *VolumeInfo) GetInodeFilterParams() (minEkLen int, minInodeSize uint64, maxEkAvgSize uint64) {
	return vol.Mcc.MinEkLen, vol.Mcc.MinInodeSize, vol.Mcc.MaxEkAvgSize
}

func (vol *VolumeInfo) GetMetaClient() *meta.MetaWrapper {
	return vol.MetaClient
}

func (vol *VolumeInfo) SetMetaClient(metaClient *meta.MetaWrapper) {
	vol.MetaClient = metaClient
}

func (vol *VolumeInfo) GetDataClient() *data.ExtentClient {
	return vol.DataClient
}

func (vol *VolumeInfo) SetDataClient(dataClient *data.ExtentClient) {
	vol.DataClient = dataClient
}

func (vol *VolumeInfo) GetName() string {
	return vol.Name
}

func (vol *VolumeInfo) GetVolumeView() *proto.VolumeDataMigView {
	vol.RLock()
	defer vol.RUnlock()
	var mpIds = make([]uint64, 0, len(vol.RunningMpIds))
	for mpId := range vol.RunningMpIds {
		mpIds = append(mpIds, mpId)
	}
	sort.Slice(mpIds, func(i, j int) bool { return mpIds[i] < mpIds[j] })
	return &proto.VolumeDataMigView{
		ClusterName:   vol.ClusterName,
		Name:          vol.Name,
		State:         vol.State,
		LastUpdate:    vol.LastUpdate,
		RunningMPCnt:  vol.RunningMPCnt,
		RunningMpIds:  mpIds,
		RunningInoCnt: vol.RunningInoCnt,
	}
}

func (vol *VolumeInfo) refreshInodeFilter() {
	timer := time.NewTimer(0)
	for {
		hddDirs := vol.GetMigrationConfig(vol.ClusterName, vol.Name).HddDirs
		log.LogDebugf("[refreshInodeFilter] refresh inode filter is running, cluster(%v) volume(%v) hddDirs(%v)", vol.ClusterName, vol.Name, hddDirs)
		select {
		case <-timer.C:
			vol.updateInodeFilter()
			timer.Reset(time.Second * refreshInodeFilterDuration)
		case <-vol.stopC:
			timer.Stop()
			return
		}
	}
}

func (vol *VolumeInfo) updateInodeFilter() {
	vol.inodeFilter = sync.Map{}
	ctx := context.Background()
	hddDirs := vol.GetMigrationConfig(vol.ClusterName, vol.Name).HddDirs
	hddDirArr := strings.Split(hddDirs, dirSeparator)
	for _, dir := range hddDirArr {
		if dir == rootDir {
			vol.inodeFilter.Store(rootDir, struct{}{})
			return
		}
	}
	for _, dir := range hddDirArr {
		dir = strings.TrimSpace(dir)
		if len(dir) == 0 {
			continue
		}
		inode, err := vol.MetaClient.LookupPath(ctx, proto.RootIno, dir)
		if err != nil {
			log.LogErrorf("look up path dir:%v err:%v", dir, err)
			continue
		}
		err = vol.readDir(ctx, inode)
		if err != nil {
			log.LogErrorf("read inode:%v err:%v", inode, err)
		}
	}
}

func (vol *VolumeInfo) readDir(ctx context.Context, parentID uint64) (err error) {
	dentrys, err := vol.MetaClient.ReadDir_ll(ctx, parentID)
	if err != nil {
		return
	}
	for _, d := range dentrys {
		if proto.IsRegular(d.Type) {
			vol.inodeFilter.Store(d.Inode, struct{}{})
			log.LogDebugf("will migrate cluster:%v volume:%v inode:%v", vol.ClusterName, vol.Name, d.Inode)
		} else if proto.IsDir(d.Type) {
			err = vol.readDir(ctx, d.Inode)
		}
	}
	return
}
