package migration

import (
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/sdk/s3"
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
	RootDir                    = "/"
	refreshInodeFilterDuration = 120
	dirSeparator               = ","
)

const (
	refreshS3ClientDuration = 120
)

type VolumeInfo struct {
	sync.RWMutex
	Name                 string
	VolId                uint64
	ClusterName          string
	State                uint32
	LastUpdate           int64
	RunningMpIds         map[uint64]struct{}
	RunningInodes        map[uint64]struct{}
	RunningInoCnt        uint32
	ControlConfig        *ControlConfig
	MetaClient           *meta.MetaWrapper
	WriteToHddDataClient *data.ExtentClient // 选择HDD的dp写数据
	DataClient           *data.ExtentClient // 写数据不区分介质
	S3Client             *s3.S3Client
	Bucket               string
	GetLayerPolicies     func(cluster, volName string) (layerPolicies []interface{}, exist bool)
	GetDpMediumType      func(cluster, volName string, dpId uint64) (mediumType string)
	GetMigrationConfig   func(cluster, volName string) (volumeConfig proto.MigrationConfig)
	InodeFilter          sync.Map
	stopC                chan struct{}
}

func NewVolumeInfo(clusterName, volName string, nodes []string, mcc *ControlConfig, extentClientType data.ExtentClientType,
	getLayerPolicies func(cluster, volName string) (layerPolicies []interface{}, exist bool),
	getDpMediumType func(cluster, volName string, dpId uint64) (mediumType string),
	getMigrationConfig func(cluster, volName string) (volumeConfig proto.MigrationConfig)) (vol *VolumeInfo, err error) {
	vol = &VolumeInfo{
		Name:               volName,
		ClusterName:        clusterName,
		ControlConfig:      mcc,
		GetLayerPolicies:   getLayerPolicies,
		GetDpMediumType:    getDpMediumType,
		GetMigrationConfig: getMigrationConfig,
		stopC:              make(chan struct{}, 0),
		RunningMpIds:       make(map[uint64]struct{}, 0),
		RunningInodes:      make(map[uint64]struct{}, 0),
	}
	if err = vol.Init(nodes, extentClientType); err != nil {
		return
	}

	log.LogDebugf("new volume info cluster(%v) volume(%v) extentClientType(%v)", clusterName, volName, extentClientType)
	return
}

func (vol *VolumeInfo) Init(nodes []string, extentClientType data.ExtentClientType) (err error) {
	var metaConfig = &meta.MetaConfig{
		Volume:        vol.Name,
		Masters:       nodes,
		Authenticate:  false,
		ValidateOwner: false,
	}
	if vol.MetaClient, err = meta.NewMetaWrapper(metaConfig); err != nil {
		return
	}
	if err = vol.initTargetClient(nodes); err != nil {
		vol.MetaClient.Close()
		return
	}
	if err = vol.createExtentClient(vol.Name, nodes); err != nil {
		vol.MetaClient.Close()
		return
	}
	if extentClientType == data.Smart {
		vol.updateInodeFilter()
		go vol.refreshMigrationConfig()
	}
	vol.State = VolInit
	return
}

func (vol *VolumeInfo) initTargetClient(nodes []string) (err error) {
	migConfig := vol.GetMigrationConfig(vol.ClusterName, vol.Name)
	vol.VolId = migConfig.VolId
	if strings.Contains(migConfig.SmartRules, proto.MediumS3Name) {
		log.LogDebugf("init s3 client")
		if len(migConfig.Endpoint) > 0 && len(migConfig.Bucket) > 0 {
			vol.S3Client = s3.NewS3Client(migConfig.Region, migConfig.Endpoint, migConfig.AccessKey, migConfig.SecretKey, false)
			vol.Bucket = migConfig.Bucket
		} else {
			err = fmt.Errorf("migConfig cluster(%v) volume(%v) s3 config region(%v) endpoint(%v) accessKey(%v) secretKey(%v) is invaild",
				vol.ClusterName, vol.Name, migConfig.Region, migConfig.Endpoint, migConfig.AccessKey, migConfig.SecretKey)
		}
	} else if strings.Contains(migConfig.SmartRules, proto.MediumHDDName){
		log.LogDebugf("init hdd client")
		var extentConfig = &data.ExtentConfig{
			Volume:              vol.Name,
			Masters:             nodes,
			FollowerRead:        true,
			TinySize:            unit.MB * 8,
			OnInsertExtentKey:   vol.MetaClient.InsertExtentKey,
			OnGetExtents:        vol.MetaClient.GetExtentsNoModifyAccessTime,
			OnTruncate:          vol.MetaClient.Truncate,
			OnInodeMergeExtents: vol.MetaClient.InodeMergeExtents_ll,
			MetaWrapper:         vol.MetaClient,
			ExtentClientType:    data.Smart,
		}
		vol.WriteToHddDataClient, err = data.NewExtentClient(extentConfig, nil)
	}
	return
}

func (vol *VolumeInfo) createExtentClient(volName string, nodes []string) (err error) {
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
	vol.DataClient, err = data.NewExtentClient(extentConfig, nil)
	return
}

func (vol *VolumeInfo) ReleaseResource() {
	if err := vol.MetaClient.Close(); err != nil {
		log.LogErrorf("vol[%s-%s] close meta wrapper failed:%s", vol.ClusterName, vol.Name, err.Error())
	}
	if err := vol.DataClient.Close(context.Background()); err != nil {
		log.LogErrorf("vol[%s-%s] close data wrapper failed:%s", vol.ClusterName, vol.Name, err.Error())
	}
	if vol.WriteToHddDataClient == nil {
		return
	}
	if err := vol.WriteToHddDataClient.Close(context.Background()); err != nil {
		log.LogErrorf("vol[%s-%s] close HddDataClient data wrapper failed:%s", vol.ClusterName, vol.Name, err.Error())
	}
	close(vol.stopC)
}

func (vol *VolumeInfo) ReleaseResourceMeetCondition() bool {
	vol.Lock()
	defer vol.Unlock()
	curTime := time.Now().Unix()
	if !(len(vol.RunningMpIds) == 0 && len(vol.RunningInodes) == 0 && curTime-vol.LastUpdate > VolLastUpdateIntervalTime) {
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
		vol.RunningMpIds[mpId] = struct{}{}
		return true
	}
	return false
}

func (vol *VolumeInfo) DelMPRunningCnt(mpId uint64) {
	vol.Lock()
	defer vol.Unlock()
	delete(vol.RunningMpIds, mpId)
	vol.LastUpdate = time.Now().Unix()
}

func (vol *VolumeInfo) AddInodeRunningCnt(inoId uint64) bool {
	vol.Lock()
	defer vol.Unlock()
	if vol.State == VolRunning || vol.State == VolInit {
		vol.RunningInodes[inoId] = struct{}{}
		return true
	}
	return false
}

func (vol *VolumeInfo) DelInodeRunningCnt(inoId uint64) {
	vol.Lock()
	defer vol.Unlock()
	delete(vol.RunningInodes, inoId)
}

func (vol *VolumeInfo) GetInodeCheckStep() int {
	return vol.ControlConfig.InodeCheckStep
}

func (vol *VolumeInfo) GetInodeConcurrentPerMP() int {
	return vol.ControlConfig.InodeConcurrent
}

func (vol *VolumeInfo) GetInodeFilterParams() (minEkLen int, minInodeSize uint64, maxEkAvgSize uint64) {
	return vol.ControlConfig.MinEkLen, vol.ControlConfig.MinInodeSize, vol.ControlConfig.MaxEkAvgSize
}

func (vol *VolumeInfo) GetMetaClient() *meta.MetaWrapper {
	return vol.MetaClient
}

func (vol *VolumeInfo) SetMetaClient(metaClient *meta.MetaWrapper) {
	vol.MetaClient = metaClient
}

func (vol *VolumeInfo) GetName() string {
	return vol.Name
}

func (vol *VolumeInfo) GetVolumeView() *proto.VolumeDataMigView {
	vol.RLock()
	defer vol.RUnlock()
	var (
		mpIds = make([]uint64, 0, len(vol.RunningMpIds))
		inodes = make([]uint64, 0, len(vol.RunningInodes))
	)
	for mpId := range vol.RunningMpIds {
		mpIds = append(mpIds, mpId)
	}
	sort.Slice(mpIds, func(i, j int) bool { return mpIds[i] < mpIds[j] })
	for inode := range vol.RunningInodes {
		inodes = append(inodes, inode)
	}
	sort.Slice(inodes, func(i, j int) bool { return inodes[i] < inodes[j] })
	return &proto.VolumeDataMigView{
		ClusterName:   vol.ClusterName,
		Name:          vol.Name,
		State:         vol.State,
		LastUpdate:    vol.LastUpdate,
		RunningMpCnt:  len(mpIds),
		RunningMpIds:  mpIds,
		RunningInodeCnt: len(inodes),
		RunningInodes: inodes,
	}
}

func (vol *VolumeInfo) refreshMigrationConfig() {
	inodeFilterTimer := time.NewTimer(time.Second * refreshInodeFilterDuration)
	s3ClientTimer := time.NewTimer(time.Second * refreshS3ClientDuration)
	defer func() {
		inodeFilterTimer.Stop()
		s3ClientTimer.Stop()
	}()
	for {
		select {
		case <-inodeFilterTimer.C:
			vol.updateInodeFilter()
			inodeFilterTimer.Reset(time.Second * refreshInodeFilterDuration)
		case <-s3ClientTimer.C:
			vol.updateS3Client()
			s3ClientTimer.Reset(time.Second * refreshS3ClientDuration)
		case <-vol.stopC:
			log.LogDebugf("stop refreshMigrationConfig vol:%v\n", vol.Name)
			return
		}
	}
}

func (vol *VolumeInfo) updateInodeFilter() {
	vol.InodeFilter = sync.Map{}
	ctx := context.Background()
	hddDirs := vol.GetMigrationConfig(vol.ClusterName, vol.Name).HddDirs
	hddDirArr := strings.Split(hddDirs, dirSeparator)
	for _, dir := range hddDirArr {
		if dir == RootDir {
			vol.InodeFilter.Store(RootDir, struct{}{})
			return
		}
	}
	for _, dir := range hddDirArr {
		dir = strings.TrimSpace(dir)
		if len(dir) == 0 {
			continue
		}
		regs := findRegStr(dir)
		fileNameReg := ""
		if regs != nil {
			dir = strings.ReplaceAll(dir, regs[0], "")
			fileNameReg = regs[1]
		}
		inode, err := vol.MetaClient.LookupPath(ctx, proto.RootIno, dir)
		if err != nil {
			log.LogErrorf("look up path dir:%v err:%v", dir, err)
			continue
		}
		err = vol.readDir(ctx, inode, fileNameReg)
		if err != nil {
			log.LogErrorf("read inode:%v err:%v", inode, err)
		}
	}
	log.LogDebugf("refresh inode filter")
}

func (vol *VolumeInfo) updateS3Client() {
	migConfig := vol.GetMigrationConfig(vol.ClusterName, vol.Name)
	if migConfig.Bucket != vol.Bucket {
		vol.S3Client = s3.NewS3Client(migConfig.Region, migConfig.Endpoint, migConfig.AccessKey, migConfig.SecretKey, false)
		vol.Bucket = migConfig.Bucket
	}
	vol.VolId = migConfig.VolId
	log.LogDebugf("refresh s3 client volume:%v volId:%v endpoint:%v bucket:%v", vol.Name, migConfig.VolId, migConfig.Endpoint, migConfig.Bucket)
}

func (vol *VolumeInfo) readDir(ctx context.Context, parentID uint64, fileNameReg string) (err error) {
	dentrys, err := vol.MetaClient.ReadDir_ll(ctx, parentID)
	if err != nil {
		return
	}
	for _, d := range dentrys {
		if proto.IsRegular(d.Type) {
			if len(fileNameReg) == 0 || (len(fileNameReg) != 0 && checkMatchRegexp(d.Name, fileNameReg)) {
				vol.InodeFilter.Store(d.Inode, struct{}{})
				log.LogDebugf("will migrate cluster:%v volume:%v inode:%v name:%v", vol.ClusterName, vol.Name, d.Inode, d.Name)
			}
		} else if proto.IsDir(d.Type) {
			err = vol.readDir(ctx, d.Inode, fileNameReg)
		}
	}
	return
}
