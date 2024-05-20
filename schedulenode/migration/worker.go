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
	"golang.org/x/net/context"
	"net/http"
	"sort"
	"strconv"
	"sync"
	"time"
)

const (
	DefaultMpConcurrency             = 5
	DefaultCompactVolumeLoadDuration = 60
	RetryDoGetMaxCnt                 = 3
	InodeLimitSize                   = 10 * 1024 * 1024
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
	)
	switch clientType {
	case data.Normal:
		getInodeATimePolicies = nil
		getDpMediumType = nil
	case data.Smart:
		getInodeATimePolicies = w.getInodeATimePolicies
		getDpMediumType = w.getDpMediumType
	default:
		err = fmt.Errorf("volumeInfo clientType(%v) invalid cluster(%v) volume(%v) ", cluster, volume, clientType)
		return
	}
	if volumeInfo, err = NewVolumeInfo(cluster, volume, clusterInfo.MasterClient.Nodes(), w.controlConfig, clientType,
		getInodeATimePolicies, getDpMediumType); err != nil {
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
	var data *proto.VolumeDataMigView = nil
	if volInfo != nil {
		data = volInfo.GetVolumeView()
	}
	SendReply(res, r, &proto.HTTPReply{Code: proto.ErrCodeSuccess, Msg: "Success", Data: data})
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
		ok bool
	)
	if mc, ok = w.getMasterClient(cluster); !ok {
		msg := "failed to get master client"
		err = errors.New(msg)
		SendReply(res, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: msg})
		return
	}
	var volInfo *proto.SimpleVolView
	if volInfo, err = mc.AdminAPI().GetVolumeSimpleInfo(volume); err != nil {
		msg := "failed to get volume simple info"
		log.LogErrorf("addInode GetVolumeSimpleInfo cluster(%v) volName(%v) err(%v)", cluster, volume, err)
		SendReply(res, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: msg})
		return
	}
	if volInfo.CompactTag != proto.CompactOpenName {
		msg := "compact is closed"
		err = errors.New(msg)
		SendReply(res, r, &proto.HTTPReply{Code: proto.ErrCodeInternalError, Msg: msg})
		return
	}
	var volumeInfo *VolumeInfo
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
		migrateInode, err = NewMigrateInode(NewMigrateTask(task, mc, volumeInfo), inodeExtents[0])
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
