package migration

import (
	"fmt"
	"github.com/cubefs/cubefs/cmd/common"
	"github.com/cubefs/cubefs/proto"
	cm "github.com/cubefs/cubefs/schedulenode/common"
	"github.com/cubefs/cubefs/schedulenode/worker"
	"github.com/cubefs/cubefs/sdk/data"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/sdk/mysql"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	stringutil "github.com/cubefs/cubefs/util/string"
	"golang.org/x/net/context"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	DefaultMpConcurrency             = 5
	DefaultCompactVolumeLoadDuration = 60
	RetryDoGetMaxCnt                 = 3
	PageSize                         = 1000
)

const (
	ConfigKeyCompact         = "compact"
	ConfigKeyFileMig         = "fileMig"
	ConfigKeyInodeCheckStep  = "inodeCheckStep"
	ConfigKeyInodeConcurrent = "inodeConcurrent"
	ConfigKeyMinEkLen        = "minEkLen"
	ConfigKeyMinInodeSize    = "minInodeSize"
	ConfigKeyMaxEkAvgSize    = "maxEkAvgSize"
)

const (
	DefaultInodeCheckStep  = 100
	DefaultInodeConcurrent = 10
	DefaultMinEkLen        = 2
	DefaultMinInodeSize    = 1  //MB
	DefaultMaxEkAvgSize    = 32 //MB
)

const (
	ParamKeyCluster       = "cluster"
	ParamKeyVolume        = "volume"
	ParamKeySmart         = "smart"
	paramKeySmartRules    = "smartRules"
	paramKeyHddDirs       = "hddDirs"
	paramKeySsdDirs       = "ssdDirs"
	paramKeyMigrationBack = "migrationBack"
	paramKeyCompact       = "compact"
)

const (
	migClose = iota
	ssdToHdd
	HddToSsd
)

const (
	notModify    = -1
	notModifyStr = "-1"
)

const (
	noMigrationBack = 0
	migrationBack   = 1
)

type volumeMap map[string]struct{}

type Worker struct {
	worker.BaseWorker
	masterAddr            map[string][]string // key：cluster
	masterClients         sync.Map            // key: cluster value: *master.MasterClient
	clusterMap            sync.Map            // key: cluster value: *ClusterInfo
	volumeView            sync.Map            // key: cluster value: *proto.DataMigVolumeView
	lastFileMigrateVolume sync.Map            // key: cluster value: map[volume]
	volumeTaskPos         sync.Map
	volumeTaskCnt         sync.Map //key: clusterVolumeKey value: taskCnt
	controlConfig         *ControlConfig
	limiter               *cm.ConcurrencyLimiter
	volumeInfoCheckMutex  sync.Mutex
}

func NewWorker(workerType proto.WorkerType) (w *Worker) {
	w = &Worker{}
	w.WorkerType = workerType
	return
}

func (w *Worker) Start(cfg *config.Config) (err error) {
	return w.Control.Start(w, cfg, doStartWorker)
}

func doStartWorker(s common.Server, cfg *config.Config) (err error) {
	w, ok := s.(*Worker)
	if !ok {
		err = errors.New("Invalid Node Type")
		return
	}
	w.StopC = make(chan struct{}, 0)
	w.TaskChan = make(chan *proto.Task, worker.DefaultTaskChanLength)
	w.limiter = cm.NewConcurrencyLimiter(DefaultMpConcurrency)
	if err = w.parseConfig(cfg); err != nil {
		log.LogErrorf("[doStart] parse config info failed, error(%v)", err)
		return
	}
	if err = w.parseControlConfig(cfg, w.WorkerType); err != nil {
		log.LogErrorf("[doStart] parse metaNode control config failed, error(%v)", err)
		return err
	}
	if err = mysql.InitMysqlClient(w.MysqlConfig); err != nil {
		log.LogErrorf("[doStart] init mysql client failed, error(%v)", err)
		return
	}
	if err = w.RegisterAllWorker(); err != nil {
		log.LogErrorf("[doStart] register worker failed, error(%v)", err)
		return
	}
	go w.loadVolumeInfo()
	go w.releaseVolume()
	w.registerHandler()
	return
}

func loadAllMigrationConfig() (volumeConfigs []*proto.MigrationConfig) {
	var offset = 0
	for {
		vcs, err := mysql.SelectAllMigrationConfig(PageSize, offset)
		if err != nil {
			log.LogErrorf("[loadAllMigrationConfig] load migration configs failed, err(%v)", err)
			break
		}
		if len(vcs) == 0 {
			break
		}
		log.LogDebugf("[loadAllMigrationConfig] load migration configs finished pageSize(%v) offset(%v)", PageSize, offset)
		volumeConfigs = append(volumeConfigs, vcs...)
		offset += len(volumeConfigs)
	}
	return
}

func (w *Worker) RegisterAllWorker() (err error) {
	switch w.WorkerType {
	case proto.WorkerTypeCompact:
		err = w.RegisterWorker(proto.WorkerTypeCompact, w.ConsumeCompactTask)
	case proto.WorkerTypeInodeMigration:
		err = w.RegisterWorker(proto.WorkerTypeInodeMigration, w.ConsumeFileMigTask)
	}
	return
}

func (w *Worker) Shutdown() {
	w.Control.Shutdown(w, doShutdownWorker)
}

func doShutdownWorker(s common.Server) {
	m, ok := s.(*Worker)
	if !ok {
		return
	}
	close(m.StopC)
}

func (w *Worker) Sync() {
	w.Control.Sync()
}

func (w *Worker) ConsumeCompactTask(task *proto.Task) (restore bool, err error) {
	metrics := exporter.NewModuleTP(fmt.Sprintf("%v_%v", Compact, ConsumeTask))
	defer metrics.Set(err)

	w.limiter.Add()
	defer func() {
		w.limiter.Done()
	}()
	if w.NodeException {
		return
	}
	if task.TaskInfo != proto.CompactOpenName {
		log.LogWarnf("ConsumeTask has canceled the compact task(%v)", task)
		return false, nil
	}
	if err = w.checkVolumeInfo(task.Cluster, task.VolName, data.Normal); err != nil {
		return
	}
	var isFinished bool
	isFinished, err = w.execMigrationTask(task)
	return !isFinished, err
}

func (w *Worker) ConsumeFileMigTask(task *proto.Task) (restore bool, err error) {
	metrics := exporter.NewModuleTP(UmpKeySuffix(FileMig, ConsumeTask))
	defer metrics.Set(err)

	w.limiter.Add()
	defer func() {
		w.limiter.Done()
	}()
	if w.NodeException {
		return
	}
	if task.TaskInfo != FileMigrateOpen {
		log.LogWarnf("ConsumeTask has canceled the file migrate task(%v)", task)
		return false, nil
	}
	storeMode := w.getMetaNodeStoreMode(task.Cluster, task.VolName)
	if storeMode != proto.StoreModeMem {
		log.LogWarnf("ConsumeTask getMetaNodeStoreMode task(%v) storeMode(%v), can not migrate", task, storeMode)
		return false, nil
	}
	if _, exist := w.getInodeATimePolicies(task.Cluster, task.VolName); !exist {
		log.LogWarnf("ConsumeTask getFileMigrateVolumeExist task(%v) has no AccessTime policies, can not migrate", task)
		return false, nil
	}
	if err = w.checkVolumeInfo(task.Cluster, task.VolName, data.Smart); err != nil {
		return
	}
	var isFinished bool
	isFinished, err = w.execMigrationTask(task)
	return !isFinished, err
}

func (w *Worker) execMigrationTask(task *proto.Task) (isFinished bool, err error) {
	var (
		masterClient *master.MasterClient
		volumeInfo   *VolumeInfo
		ok           bool
	)
	if masterClient, ok = w.getMasterClient(task.Cluster); !ok {
		log.LogErrorf("ConsumeTask getMasterClient cluster(%v) does not exist", task.Cluster)
		return true, nil
	}
	if volumeInfo, ok = w.getVolumeInfo(task.Cluster, task.VolName); !ok {
		log.LogErrorf("ConsumeTask getVolumeInfo cluster(%v) volName(%v) does not exist", task.Cluster, task.VolName)
		return true, nil
	}
	migTask := NewMigrateTask(task, masterClient, volumeInfo)
	isFinished, err = migTask.RunOnce()
	if err != nil {
		log.LogErrorf("ConsumeTask mpOp RunOnce cluster(%v) volName(%v) mpId(%v) err(%v)", task.Cluster, task.VolName, migTask.mpId, err)
		switch err.Error() {
		case proto.ErrMetaPartitionNotExists.Error(), proto.ErrVolNotExists.Error():
			isFinished = true
			err = nil
		default:
			isFinished = false
		}
	}
	return
}

func (w *Worker) checkVolumeInfo(cluster, volume string, clientType data.ExtentClientType) (err error) {
	w.volumeInfoCheckMutex.Lock()
	defer w.volumeInfoCheckMutex.Unlock()
	var (
		clusterInfo *ClusterInfo
		volumeInfo  *VolumeInfo
		ok          bool
	)
	clusterInfo = w.getClusterInfo(cluster)
	if clusterInfo == nil {
		err = fmt.Errorf("can not support cluster[%s]", cluster)
		return
	}
	if volumeInfo, ok = w.getVolumeInfo(cluster, volume); ok {
		volumeInfo.UpdateStateToInit()
		return
	}
	var (
		getInodeATimePolicies func(cluster, volName string) (layerPolicies []interface{}, exist bool)
		getDpMediumType       func(cluster, volName string, dpId uint64) (mediumType string)
		getMigrationConfig    func(cluster, volName string) (migConfig proto.MigrationConfig)
	)
	switch clientType {
	case data.Normal:
		getInodeATimePolicies = nil
		getDpMediumType = nil
		getMigrationConfig = nil
	case data.Smart:
		getInodeATimePolicies = w.getInodeATimePolicies
		getDpMediumType = w.getDpMediumType
		getMigrationConfig = w.getMigrationConfig
	default:
		err = fmt.Errorf("volumeInfo clientType(%v) invalid cluster(%v) volume(%v) ", cluster, volume, clientType)
		return
	}
	if volumeInfo, err = NewVolumeInfo(cluster, volume, clusterInfo.MasterClient.Nodes(), w.controlConfig, clientType,
		getInodeATimePolicies, getDpMediumType, getMigrationConfig); err != nil {
		err = fmt.Errorf("NewFileMigrateVolume cluster(%v) volume(%v) info failed:%v", cluster, volume, err)
		return
	}
	if err = w.addVolume(cluster, volume, volumeInfo); err != nil {
		return
	}
	return
}

func (w *Worker) getVolumeInfo(clusterName, volName string) (volInfo *VolumeInfo, ok bool) {
	var value interface{}
	value, ok = w.clusterMap.Load(clusterName)
	if !ok {
		return
	}
	var vol interface{}
	if vol, ok = value.(*ClusterInfo).GetVolByName(volName); ok {
		volInfo = vol.(*VolumeInfo)
	}
	return
}

func (w *Worker) getMetaNodeStoreMode(cluster, volName string) (storeMode proto.StoreMode) {
	value, ok := w.volumeView.Load(cluster)
	if !ok {
		return
	}
	volViews := value.(*FileMigrateVolumeView)
	if sv, has := volViews.SmartVolumes[volName]; has {
		storeMode = sv.StoreMode
	}
	return
}

func (w *Worker) getInodeATimePolicies(cluster, volName string) (layerPolicies []interface{}, exist bool) {
	value, ok := w.volumeView.Load(cluster)
	if !ok {
		return
	}
	volViews := value.(*FileMigrateVolumeView)
	if sv, has := volViews.SmartVolumes[volName]; has {
		layerPolicies = sv.LayerPolicies[proto.LayerTypeInodeATime]
		exist = true
	}
	return
}

func (w *Worker) getMigrationConfig(cluster, volName string) (volumeConfig proto.MigrationConfig) {
	value, ok := w.volumeView.Load(cluster)
	if !ok {
		return
	}
	volViews := value.(*FileMigrateVolumeView)
	if sv, has := volViews.SmartVolumes[volName]; has {
		volumeConfig = proto.MigrationConfig{
			Smart:         sv.Smart,
			HddDirs:       sv.HddDirs,
			SsdDirs:       sv.SsdDirs,
			MigrationBack: sv.MigrationBack,
		}
	}
	return
}

func (w *Worker) getDpMediumType(cluster, volName string, dpId uint64) (mediumType string) {
	f := func(dataPartitions []*proto.DataPartitionResponse) string {
		for _, dp := range dataPartitions {
			if dp.PartitionID == dpId {
				return dp.MediumType
			}
		}
		return ""
	}
	value, ok := w.volumeView.Load(cluster)
	if !ok {
		return
	}
	volViews := value.(*FileMigrateVolumeView)
	if sv, has := volViews.SmartVolumes[volName]; has {
		mediumType = f(sv.DataPartitions)
	}
	return
}

func (w *Worker) registerHandler() {
	if w.WorkerType == proto.WorkerTypeCompact {
		http.HandleFunc(CompactList, w.workerViewInfo)
		http.HandleFunc(CompactInfo, w.info)
		http.HandleFunc(CompactStop, w.stop)
		http.HandleFunc(CompactInode, w.compactInode)
		http.HandleFunc(CompactConcurrencySetLimit, w.setLimit)
		http.HandleFunc(CompactConcurrencyGetLimit, w.getLimit)
	}
	if w.WorkerType == proto.WorkerTypeInodeMigration {
		http.HandleFunc(FileMigrateList, w.workerViewInfo)
		http.HandleFunc(FileMigrateInfo, w.info)
		http.HandleFunc(FileMigrateStop, w.stop)
		http.HandleFunc(FileMigrateConcurrencySetLimit, w.setLimit)
		http.HandleFunc(FileMigrateConcurrencyGetLimit, w.getLimit)
		http.HandleFunc(MigrationConfigAddOrUpdate, w.migrationConfigAddOrUpdate)
		http.HandleFunc(MigrationConfigDelete, w.migrationConfigDelete)
	}
}

func (w *Worker) releaseVolume() {
	timer := time.NewTimer(0)
	for {
		select {
		case <-timer.C:
			w.releaseUnusedVolume()
			timer.Reset(time.Second * DefaultCompactVolumeLoadDuration)
		case <-w.StopC:
			timer.Stop()
			return
		}
	}
}

func (w *Worker) parseControlConfig(cfg *config.Config, workerType proto.WorkerType) (err error) {
	var (
		mcc    = &ControlConfig{}
		mnInfo map[string]interface{}
	)
	if workerType == proto.WorkerTypeCompact {
		mnInfo = cfg.GetMap(ConfigKeyCompact)
	} else if workerType == proto.WorkerTypeInodeMigration {
		mnInfo = cfg.GetMap(ConfigKeyFileMig)
	}
	if mnInfo == nil {
		err = fmt.Errorf("metanode control config is empty")
		return
	}

	for key, value := range mnInfo {
		v := value.(float64)
		switch key {
		case ConfigKeyInodeCheckStep:
			mcc.InodeCheckStep = int(v)
		case ConfigKeyInodeConcurrent:
			mcc.InodeConcurrent = int(v)
		case ConfigKeyMinEkLen:
			mcc.MinEkLen = int(v)
		case ConfigKeyMinInodeSize:
			mcc.MinInodeSize = uint64(v)
		case ConfigKeyMaxEkAvgSize:
			mcc.MaxEkAvgSize = uint64(v)
		}
	}
	if mcc.InodeCheckStep <= 0 {
		mcc.InodeCheckStep = DefaultInodeCheckStep
	}
	if mcc.InodeConcurrent <= 0 {
		mcc.InodeConcurrent = DefaultInodeConcurrent
	}
	if mcc.MinEkLen < 0 {
		mcc.MinEkLen = DefaultMinEkLen
	}
	if mcc.MinInodeSize < 0 {
		mcc.MinInodeSize = DefaultMinInodeSize
	}
	if mcc.MaxEkAvgSize <= 0 {
		mcc.MaxEkAvgSize = DefaultMaxEkAvgSize
	}
	w.controlConfig = mcc
	return
}

func (w *Worker) parseMasterAddr(cfg *config.Config) {
	masters := make(map[string][]string)
	baseInfo := cfg.GetMap(config.ConfigKeyClusterAddr)
	for clusterName, value := range baseInfo {
		addresses := make([]string, 0)
		if valueSlice, ok := value.([]interface{}); ok {
			for _, item := range valueSlice {
				if addr, ok := item.(string); ok {
					addresses = append(addresses, addr)
				}
			}
		}
		masters[clusterName] = addresses
	}
	w.masterAddr = masters

	for clusterName, nodes := range w.masterAddr {
		materClient := master.NewMasterClient(nodes, false)
		w.masterClients.Store(clusterName, materClient)
		w.clusterMap.Store(clusterName, NewClusterInfo(clusterName, materClient))
	}
}

func (w *Worker) getClusterInfo(cluster string) *ClusterInfo {
	value, ok := w.clusterMap.Load(cluster)
	if !ok {
		return nil
	}
	return value.(*ClusterInfo)
}

func (w *Worker) addVolume(cluster, volName string, vol *VolumeInfo) (err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("Add volume, cluster[%v] volumeName[%v] failed:%v", cluster, volName, err)
		}
	}()
	clusterInfo := w.getClusterInfo(cluster)
	if clusterInfo == nil {
		err = fmt.Errorf("can not support cluster[%v]", cluster)
		return
	}
	clusterInfo.StoreVol(volName, vol)
	return
}

func (w *Worker) getMasterClient(cluster string) (mc *master.MasterClient, ok bool) {
	var value interface{}
	value, ok = w.clusterMap.Load(cluster)
	if !ok {
		return
	}
	mc = value.(*ClusterInfo).MasterClient
	return
}

func (w *Worker) releaseUnusedVolume() {
	w.clusterMap.Range(func(key, value interface{}) bool {
		clusterInfo := value.(*ClusterInfo)
		clusterInfo.ReleaseUnusedVolume()
		return true
	})
}

const (
	CompactList                = "/compact/list"
	CompactInfo                = "/compact/info"
	CompactStop                = "/compact/stop"
	CompactInode               = "/compact/inode"
	CompactConcurrencySetLimit = "/compact/setLimit"
	CompactConcurrencyGetLimit = "/compact/getLimit"
)

const (
	FileMigrateList                = "/fileMigrate/list"
	FileMigrateStop                = "/fileMigrate/stop"
	FileMigrateInfo                = "/fileMigrate/info"
	FileMigrateConcurrencySetLimit = "/fileMigrate/setLimit"
	FileMigrateConcurrencyGetLimit = "/fileMigrate/getLimit"
)

const (
	MigrationConfigAddOrUpdate = "/migrationConfig/addOrUpdate"
	MigrationConfigDelete      = "/migrationConfig/delete"
)

func (w *Worker) workerViewInfo(res http.ResponseWriter, r *http.Request) {
	view := w.collectWorkerViewInfo()
	SendReply(res, r, &proto.HTTPReply{Code: proto.ErrCodeSuccess, Msg: "Success", Data: view})
}

func (w *Worker) collectWorkerViewInfo() (view *proto.DataMigWorkerViewInfo) {
	view = &proto.DataMigWorkerViewInfo{}
	view.Port = w.Port
	view.Clusters = make([]*proto.ClusterDataMigView, 0)
	w.clusterMap.Range(func(key, value interface{}) bool {
		clusterInfo := value.(*ClusterInfo)
		clusterDateMigView := &proto.ClusterDataMigView{
			ClusterName: clusterInfo.Name,
			Nodes:       clusterInfo.Nodes(),
			VolumeInfo:  make([]*proto.VolumeDataMigView, 0),
		}
		clusterInfo.VolumeMap.Range(func(key, value interface{}) bool {
			vol := value.(*VolumeInfo)
			clusterDateMigView.VolumeInfo = append(clusterDateMigView.VolumeInfo, vol.GetVolumeView())
			return true
		})
		sort.SliceStable(clusterDateMigView.VolumeInfo, func(i, j int) bool {
			return clusterDateMigView.VolumeInfo[i].Name < clusterDateMigView.VolumeInfo[j].Name
		})
		view.Clusters = append(view.Clusters, clusterDateMigView)
		sort.SliceStable(view.Clusters, func(i, j int) bool {
			return view.Clusters[i].ClusterName < view.Clusters[j].ClusterName
		})
		return true
	})
	return
}

func (w *Worker) info(res http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		err = fmt.Errorf("parse form fail: %v", err)
		SendReply(res, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError,
			Msg: err.Error()})
		return
	}
	clusterName := r.FormValue(ClusterKey)
	volumeName := r.FormValue(VolNameKey)
	volInfo := w.volInfo(clusterName, volumeName)
	var migView *proto.VolumeDataMigView
	if volInfo != nil {
		migView = volInfo.GetVolumeView()
	}
	SendReply(res, r, &proto.HTTPReply{Code: proto.ErrCodeSuccess, Msg: "Success", Data: migView})
}

func (w *Worker) volInfo(clusterName, volumeName string) (volInfo *VolumeInfo) {
	value, ok := w.clusterMap.Load(clusterName)
	if !ok {
		return
	}
	clusterInfo := value.(*ClusterInfo)
	var vol interface{}
	if vol, ok = clusterInfo.GetVolByName(volumeName); !ok {
		return
	}
	volInfo = vol.(*VolumeInfo)
	return
}

func (w *Worker) stop(res http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		err = fmt.Errorf("parse form fail: %v", err)
		SendReply(res, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	cluster := r.FormValue(ClusterKey)
	volume := r.FormValue(VolNameKey)
	var msg string
	if vol, ok := w.getVolumeInfo(cluster, volume); ok {
		vol.State = VolClosing
		msg = "Success"
	} else {
		msg = fmt.Sprintf("cluster(%v) volume(%v) doesn't exist", cluster, volume)
	}
	SendReply(res, r, &proto.HTTPReply{Code: proto.ErrCodeSuccess, Msg: msg})
}

func (w *Worker) setLimit(res http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		err = fmt.Errorf("parse form fail: %v", err)
		SendReply(res, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	limit := r.FormValue(LimitSizeKey)
	var limitNum int64
	var err error
	if limitNum, err = strconv.ParseInt(limit, 10, 32); err != nil {
		msg := fmt.Sprintf("limit(%v) should be numeric", limit)
		SendReply(res, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: msg})
		return
	}
	if limitNum <= 0 {
		msg := fmt.Sprintf("limit(%v) should be greater than 0", limit)
		SendReply(res, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: msg})
		return
	}
	w.limiter.Reset(int32(limitNum))
	SendReply(res, r, &proto.HTTPReply{Code: proto.ErrCodeSuccess, Msg: "Success"})
}

func (w *Worker) getLimit(res http.ResponseWriter, r *http.Request) {
	limit := w.limiter.Limit()
	SendReply(res, r, &proto.HTTPReply{Code: proto.ErrCodeSuccess, Msg: "Success", Data: limit})
}

func (w *Worker) compactInode(res http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		err = fmt.Errorf("parse form fail: %v", err)
		SendReply(res, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: err.Error()})
		return
	}
	cluster := r.FormValue(ClusterKey)
	volume := r.FormValue(VolNameKey)
	mpId := r.FormValue(MpIdKey)
	inodeId := r.FormValue(InodeIdKey)
	if len(volume) == 0 {
		SendReply(res, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: "volume name should not be an empty string"})
		return
	}
	var (
		mpIdNum int
		err     error
	)
	if mpIdNum, err = strconv.Atoi(mpId); err != nil {
		msg := fmt.Sprintf("mpId(%v) should be numeric", mpId)
		SendReply(res, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: msg})
		return
	}
	var inodeIdNum int
	if inodeIdNum, err = strconv.Atoi(inodeId); err != nil {
		msg := fmt.Sprintf("inodeId(%v) should be numeric", inodeId)
		SendReply(res, r, &proto.HTTPReply{Code: proto.ErrCodeParamError, Msg: msg})
		return
	}

	if err = w.checkVolumeInfo(cluster, volume, data.Normal); err != nil {
		msg := fmt.Sprintf("checkVolumeInfo err(%v)", err)
		SendReply(res, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: msg})
		return
	}
	var (
		mc *master.MasterClient
		volumeInfo *VolumeInfo
		ok bool
	)
	if mc, ok = w.getMasterClient(cluster); !ok {
		msg := "failed to get master client"
		err = errors.New(msg)
		SendReply(res, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: msg})
		return
	}
	if volumeInfo, ok = w.getVolumeInfo(cluster, volume); !ok {
		msg := fmt.Sprintf("cluster(%v) volName(%v) does not exist", cluster, volume)
		SendReply(res, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: msg})
		return
	}
	var (
		inodeConcurrentPerMP                 = volumeInfo.GetInodeConcurrentPerMP()
		minEkLen, minInodeSize, maxEkAvgSize = volumeInfo.GetInodeFilterParams()
		inodeExtents                         []*proto.InodeExtents
	)
	inodeExtents, err = volumeInfo.MetaClient.GetInodeExtents_ll(context.Background(), uint64(mpIdNum), []uint64{uint64(inodeIdNum)}, inodeConcurrentPerMP, minEkLen, minInodeSize, maxEkAvgSize)
	if err != nil {
		msg := fmt.Sprintf("cluster(%v) volName(%v) mpIdNum(%v) err(%v)", cluster, volume, mpIdNum, err)
		SendReply(res, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: msg})
		return
	}
	if len(inodeExtents) <= 0 {
		SendReply(res, r, &proto.HTTPReply{Code: proto.ErrCodeSuccess, Msg: "need not compact"})
		return
	}
	go func() {
		task := &proto.Task{
			TaskType:   proto.WorkerTypeCompact,
			Cluster:    cluster,
			VolName:    volume,
			MpId:       uint64(mpIdNum),
			WorkerAddr: w.LocalIp,
		}
		var migrateInode *MigrateInode
		migrateTask := NewMigrateTask(task, mc, volumeInfo)
		if err = migrateTask.GetMpInfo(); err != nil {
			log.LogErrorf("GetMpInfo cluster(%v) volName(%v) mpId(%v) inodeId(%v) err(%v)", cluster, volume, mpId, inodeId, err)
			return
		}
		if err = migrateTask.GetProfPort(); err != nil {
			log.LogErrorf("GetProfPort cluster(%v) volName(%v) mpId(%v) inodeId(%v) err(%v)", cluster, volume, mpId, inodeId, err)
			return
		}
		migrateInode, err = NewMigrateInode(migrateTask, inodeExtents[0])
		if err != nil {
			log.LogErrorf("NewMigrateInode cluster(%v) volName(%v) mpId(%v) inodeId(%v) err(%v)", cluster, volume, mpId, inodeId, err)
			return
		}
		var isFinished bool
		if isFinished, err = migrateInode.RunOnce(); err != nil {
			log.LogErrorf("compactInode migrateInode.RunOnce cluster(%v) volName(%v) mpId(%v) inodeId(%v) err(%v)", cluster, volume, mpId, inodeId, err)
		} else {
			log.LogInfof("compactInode migrateInode.RunOnce cluster(%v) volName(%v) mpId(%v) inodeId(%v) compact finished, result(%v)", cluster, volume, mpId, inodeId, isFinished)
		}
	}()
	SendReply(res, r, &proto.HTTPReply{Code: proto.ErrCodeSuccess, Msg: "start exec inode compact task"})
}

func (w *Worker) migrationConfigAddOrUpdate(res http.ResponseWriter, r *http.Request) {
	var (
		migConfig *proto.MigrationConfig
		err       error
	)
	migConfig, err = parseParamVolumeConfig(w, r)
	if err != nil {
		SendReply(res, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}
	var volumeConfigs []*proto.MigrationConfig
	volumeConfigs, err = mysql.SelectVolumeConfig(migConfig.ClusterName, migConfig.VolName)
	if err != nil {
		SendReply(res, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}
	if len(volumeConfigs) == 0 {
		if migConfig.Smart == notModify {
			migConfig.Smart = migClose
		}
		if migConfig.SmartRules == notModifyStr {
			migConfig.SmartRules = ""
		}
		if migConfig.HddDirs == notModifyStr {
			migConfig.HddDirs = ""
		}
		if migConfig.SsdDirs == notModifyStr {
			migConfig.SsdDirs = ""
		}
		if migConfig.MigrationBack == notModify {
			migConfig.MigrationBack = noMigrationBack
		}
		if migConfig.Compact == notModify {
			migConfig.Compact = compactDisabled
		}
		err = mysql.AddMigrationConfig(migConfig)
		if err != nil {
			SendReply(res, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
			return
		}
		SendReply(res, r, &proto.HTTPReply{Code: proto.ErrCodeSuccess, Msg: "success"})
		return
	}
	volumeConfigInMysql := volumeConfigs[0]
	if migConfig.Smart != notModify {
		volumeConfigInMysql.Smart = migConfig.Smart
	}
	if migConfig.SmartRules != notModifyStr {
		volumeConfigInMysql.SmartRules = migConfig.SmartRules
	}
	if migConfig.HddDirs != notModifyStr {
		volumeConfigInMysql.HddDirs = migConfig.HddDirs
	}
	if migConfig.SsdDirs != notModifyStr {
		volumeConfigInMysql.SsdDirs = migConfig.SsdDirs
	}
	if migConfig.MigrationBack != notModify {
		volumeConfigInMysql.MigrationBack = migConfig.MigrationBack
	}
	if migConfig.Compact != notModify {
		volumeConfigInMysql.Compact = migConfig.Compact
	}
	err = mysql.UpdateMigrationConfig(volumeConfigInMysql)
	if err != nil && !strings.Contains(err.Error(), "affected rows less then one") {
		SendReply(res, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}
	SendReply(res, r, &proto.HTTPReply{Code: proto.ErrCodeSuccess, Msg: "success"})
}

func (w *Worker) migrationConfigDelete(res http.ResponseWriter, r *http.Request) {
	var (
		cluster string
		volume  string
		err     error
	)
	if err = r.ParseForm(); err != nil {
		err = fmt.Errorf("parse form fail: %v", err)
		SendReply(res, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}
	cluster = r.FormValue(ParamKeyCluster)
	if stringutil.IsStrEmpty(cluster) {
		err = fmt.Errorf("param %v can not be empty", ParamKeyCluster)
		SendReply(res, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}
	volume = r.FormValue(ParamKeyVolume)
	if stringutil.IsStrEmpty(volume) {
		err = fmt.Errorf("param %v can not be empty", ParamKeyVolume)
		SendReply(res, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}
	if err = mysql.DeleteMigrationConfig(cluster, volume); err != nil {
		SendReply(res, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: err.Error()})
		return
	}
	SendReply(res, r, &proto.HTTPReply{Code: proto.ErrCodeSuccess, Msg: "success"})
}

func parseParamVolumeConfig(w *Worker, r *http.Request) (config *proto.MigrationConfig, err error) {
	var (
		cluster       string
		volume        string
		smart         int
		smartRules    string
		hddDirs       string
		ssdDirs       string
		migBack       int
		compact       int
	)
	if err = r.ParseForm(); err != nil {
		err = fmt.Errorf("parse form fail: %v", err)
		return
	}
	cluster = r.FormValue(ParamKeyCluster)
	if stringutil.IsStrEmpty(cluster) {
		err = fmt.Errorf("param %v can not be empty", ParamKeyCluster)
		return
	}
	volume = r.FormValue(ParamKeyVolume)
	if stringutil.IsStrEmpty(volume) {
		err = fmt.Errorf("param %v can not be empty", ParamKeyVolume)
		return
	}
	var (
		value interface{}
		ok bool
	)
	if value, ok = w.masterClients.Load(cluster); !ok {
		err = fmt.Errorf("cluster %v not support", cluster)
		return
	}
	mc := value.(*master.MasterClient)
	var volInfo *proto.SimpleVolView
	if volInfo, err = mc.AdminAPI().GetVolumeSimpleInfo(volume); err != nil {
		err = fmt.Errorf("cluster: %v volume: %v %v", cluster, volume, err)
		return
	}
	if volInfo != nil && volInfo.MarkDeleteTime > 0 {
		err = fmt.Errorf("cluster: %v volume: %v has been mark delete", cluster, volume)
		return
	}
	smartReqValue := r.FormValue(ParamKeySmart)
	if len(smartReqValue) == 0 {
		smart = notModify
	} else {
		if smart, err = strconv.Atoi(smartReqValue); err != nil {
			err = fmt.Errorf("parse param %v fail: %v", ParamKeySmart, err)
			return
		}
	}
	if smart > 0 {
		if smart != ssdToHdd && smart != HddToSsd {
			err = fmt.Errorf("parse param %v fail: %v", ParamKeySmart, "should be 1 or 2")
			return
		}
	}
	smartRules = r.FormValue(paramKeySmartRules)
	if len(smartRules) == 0 {
		smartRules = notModifyStr
	} else {
		if err = proto.CheckLayerPolicy(cluster, volume, strings.Split(smartRules, ",")); err != nil {
			err = fmt.Errorf("valid smart rules invalid err: %v", err)
			return
		}
	}
	hddDirs = r.FormValue(paramKeyHddDirs)
	if len(hddDirs) == 0 {
		hddDirs = notModifyStr
	}
	ssdDirs = r.FormValue(paramKeySsdDirs)
	if len(ssdDirs) == 0 {
		ssdDirs = notModifyStr
	}
	migrationBackReqValue := r.FormValue(paramKeyMigrationBack)
	if len(migrationBackReqValue) == 0 {
		migBack = notModify
	} else {
		if migBack, err = strconv.Atoi(migrationBackReqValue); err != nil {
			err = fmt.Errorf("parse param %v fail: %v", paramKeyMigrationBack, err)
			return
		}
	}
	compactReqValue := r.FormValue(paramKeyCompact)
	if len(compactReqValue) == 0 {
		compact = notModify
	} else {
		if compact, err = strconv.Atoi(compactReqValue); err != nil {
			err = fmt.Errorf("parse param %v fail: %v", paramKeyCompact, err)
			return
		}
	}
	config = &proto.MigrationConfig{
		ClusterName:   cluster,
		VolName:       volume,
		Smart:         int8(smart),
		SmartRules:    smartRules,
		HddDirs:       hddDirs,
		SsdDirs:       ssdDirs,
		MigrationBack: int8(migBack),
		Compact:       int8(compact),
	}
	return
}
