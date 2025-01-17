package intramig

import (
	"fmt"
	"github.com/cubefs/cubefs/schedulenode/migcore"
	"sort"
	"strings"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/schedulenode/worker"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/sdk/mysql"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
)

const (
	DefaultVolumeMaxExecMPNum = 5
	DefaultVolumeLoadDuration = 60
	TaskTimeInterval          = 1 * 60 * 60
)

const (
	FileMigrateOpen  = "Enabled"
	FileMigrateClose = "Disabled"
)

const (
	compactDisabled = iota
	compactEnabled
)

func NewWorkerForScheduler(cfg *config.Config, workerType proto.WorkerType) (w *Worker, err error) {
	w = &Worker{}
	w.WorkerType = workerType
	if err = w.parseConfig(cfg); err != nil {
		log.LogErrorf("[NewWorkerForScheduler] parse config info failed, worker type(%v) error(%v)", workerType, err)
		return
	}
	if err = w.initWorkerForScheduler(); err != nil {
		log.LogErrorf("[NewWorkerForScheduler] init worker failed, worker type(%v) error(%v)", workerType, err)
		return
	}
	go w.loadVolumeInfo()
	return
}

func (w *Worker) CreateTask(clusterId string, taskNum int64, runningTasks []*proto.Task, wns []*proto.WorkerNode) (newTasks []*proto.Task, err error) {
	if w.WorkerType == proto.WorkerTypeCompact {
		newTasks, err = w.createCompactTask(clusterId, taskNum, runningTasks, wns)
	} else if w.WorkerType == proto.WorkerTypeInodeMigration {
		newTasks, err = w.createFileMigrateTask(clusterId, taskNum, runningTasks, wns)
	}
	return
}

func (w *Worker) createCompactTask(clusterId string, taskNum int64, runningTasks []*proto.Task, wns []*proto.WorkerNode) (newTasks []*proto.Task, err error) {
	metric := exporter.NewModuleTP(proto.MonitorCompactCreateTask)
	defer metric.Set(err)

	value, ok := w.volumeView.Load(clusterId)
	if !ok {
		return
	}
	cv := value.(*proto.DataMigVolumeView)
	var (
		compactOpenVols  []*proto.DataMigVolume
		compactCloseVols []*proto.DataMigVolume
	)
	for _, vol := range cv.DataMigVolumes {
		log.LogDebugf("CreateTaskA cw.cmpView: cluster(%v) volName(%v) ForceROW(%v) CompactTag(%v)", clusterId, vol.Name, vol.ForceROW, vol.CompactTag)
		compact := vol.CompactTag.String()
		switch compact {
		case proto.CompactOpenName:
			compactOpenVols = append(compactOpenVols, vol)
		case proto.CompactCloseName:
			compactCloseVols = append(compactCloseVols, vol)
		default:
		}
	}
	w.closeCompactTask(clusterId, compactCloseVols, wns)
	newTasks, err = w.createCompactTaskByVolume(clusterId, compactOpenVols, taskNum, runningTasks)
	return
}

func (w *Worker) closeCompactTask(cluster string, vols []*proto.DataMigVolume, wns []*proto.WorkerNode) {
	for _, vol := range vols {
		log.LogDebugf("closeCompactTask clusterId(%v), vol(%v) wnsLength(%v)", cluster, vol.Name, len(wns))
		mpView, err := w.GetMpView(cluster, vol.Name)
		if err != nil {
			log.LogErrorf("closeCompactTask GetMpView cluster(%v) volName(%v) err(%v)", cluster, vol.Name, err)
			continue
		}

		workerAddrMap := w.processMpView(cluster, vol.Name, mpView, wns, proto.WorkerTypeCompact, vol.CompactTag.String())
		w.stopTasks(cluster, vol.Name, workerAddrMap, CompactStop)
	}
}

func (w *Worker) processMpView(cluster, volName string, mpView []*proto.MetaPartitionView, wns []*proto.WorkerNode, wt proto.WorkerType, taskInfo string) map[string]struct{} {
	workerAddrMap := make(map[string]struct{})
	for _, mp := range mpView {
		newTask := proto.NewDataTask(wt, cluster, volName, 0, mp.PartitionID, taskInfo)
		exist, oldTask, err := w.ContainMPTaskByWorkerNodes(newTask, wns)
		if err != nil {
			log.LogErrorf("ContainMPTaskByWorkerNodes failed, cluster(%v), volume(%v), err(%v)", cluster, volName, err)
			continue
		}
		if !exist {
			continue
		}

		log.LogDebugf("processMpView oldTask WorkerAddr(%v), task(%v)", oldTask.WorkerAddr, oldTask)
		if oldTask.WorkerAddr != "" {
			workerAddrMap[oldTask.WorkerAddr] = struct{}{}
		}

		if err = w.UpdateTaskInfo(oldTask.TaskId, taskInfo); err != nil {
			log.LogErrorf("UpdateTaskInfo to database failed, cluster(%v), volume(%v), task(%v), err(%v)", cluster, volName, newTask, err)
		}
	}
	return workerAddrMap
}

func (w *Worker) stopTasks(cluster, volName string, workerAddrMap map[string]struct{}, reqPath string) {
	for addr := range workerAddrMap {
		stopTaskUrl := migcore.GenStopUrl(addr, reqPath, cluster, volName)
		if err := w.stopTask(stopTaskUrl, cluster, volName); err != nil {
			log.LogErrorf("stopTasks failed, cluster(%v), volume(%v), stopTaskUrl(%v), err(%v)", cluster, volName, stopTaskUrl, err)
		}
	}
}

func (w *Worker) stopTask(url, cluster, volName string) error {
	var res *proto.QueryHTTPResult
	var err error
	for i := 0; i < RetryDoGetMaxCnt; i++ {
		if res, err = migcore.DoGet(url); err != nil {
			log.LogErrorf("CancelMpTask doGet failed, cluster(%v), volume(%v), url(%v), err(%v)", cluster, volName, url, err)
			continue
		}
		break
	}
	log.LogDebugf("CancelMpTask doGet url(%v), result(%v)", url, res)
	return err
}

func (w *Worker) createCompactTaskByVolume(cluster string, vols []*proto.DataMigVolume, taskNum int64, runningTasks []*proto.Task) (newTasks []*proto.Task, err error) {
	sortVolumesByTaskCountASC(w, proto.WorkerTypeCompact, cluster, vols, func(v *proto.DataMigVolume) string {
		return v.Name
	})
	for _, vol := range vols {
		log.LogDebugf("CompactWorker CreateTask begin clusterId(%v), volumeName(%v), taskNum(%v), runningTasksLength(%v)", cluster, vol.Name, taskNum, len(runningTasks))
		volumeTasks := make(map[string][]*proto.Task)
		for _, task := range runningTasks {
			volumeTasks[task.VolName] = append(volumeTasks[task.VolName], task)
		}

		var mpView []*proto.MetaPartitionView
		mpView, err = w.GetMpView(cluster, vol.Name)
		if err != nil {
			log.LogErrorf("CompactWorker CreateMpTask GetMpView cluster(%v) volName(%v) err(%v)", cluster, vol.Name, err)
			continue
		}
		sort.Slice(mpView, func(i, j int) bool {
			return mpView[i].PartitionID < mpView[j].PartitionID
		})
		key := migcore.WorkerTypeKey(proto.WorkerTypeCompact, cluster, vol.Name)
		w.resetTaskPosition(key, mpView)
		newTasks, err = w.createAndManageCompactTasks(cluster, vol, mpView, runningTasks, taskNum, proto.WorkerTypeCompact)
	}
	return
}

func (w *Worker) createFileMigrateTask(clusterId string, taskNum int64, runningTasks []*proto.Task, wns []*proto.WorkerNode) (newTasks []*proto.Task, err error) {
	metric := exporter.NewModuleTP(proto.MonitorSmartCreateTaskInode)
	defer metric.Set(err)

	value, ok := w.volumeView.Load(clusterId)
	if !ok {
		return
	}
	fv := value.(*FileMigrateVolumeView)
	var (
		fileMigrateOpenVols   []*proto.SmartVolume
		fileMigrateOpenVolMap = make(volumeMap) // key: cluster,volName
		fileMigrateCloseVols  []string
	)
	for volName, smartVolume := range fv.SmartVolumes {
		log.LogDebugf("FileMigrationWorker: cluster(%v) volName(%v) forceRow(%v) smartRules(%v)", clusterId, volName, smartVolume.ForceROW, smartVolume.SmartRules)
		if smartVolume.Smart <= migClose {
			log.LogWarnf("FileMigrationWorker CreateTask smart is false, cluster(%v), volume(%v), smart(%v)", clusterId, volName, smartVolume.Smart)
			continue
		}
		if len(smartVolume.LayerPolicies) == 0 {
			log.LogWarnf("FileMigrationWorker CreateTask have no layer policy, cluster(%v), volume(%v)", clusterId, volName)
			continue
		}
		if _, has := smartVolume.LayerPolicies[proto.LayerTypeInodeATime]; !has {
			log.LogWarnf("FileMigrationWorker CreateTask have no LayerTypeInodeATime layer policy, cluster(%v), volume(%v)", clusterId, volName)
			continue
		}
		if smartVolume.StoreMode != proto.StoreModeMem {
			log.LogWarnf("FileMigrationWorker CreateTask StoreMode is(%v), cluster(%v), volume(%v)", smartVolume.StoreMode, clusterId, volName)
			continue
		}
		fileMigrateOpenVols = append(fileMigrateOpenVols, smartVolume)
		fileMigrateOpenVolMap[volName] = struct{}{}
	}
	newTasks, err = w.createFileMigrateTaskByVolume(proto.WorkerTypeInodeMigration, clusterId, fileMigrateOpenVols, taskNum, runningTasks)

	fileMigrateCloseVols = w.filterSoonCloseFileMigrateVols(clusterId, fileMigrateOpenVolMap, runningTasks)
	w.CloseFileMigrateTask(clusterId, fileMigrateCloseVols, wns)
	return
}

func (w *Worker) createFileMigrateTaskByVolume(workerType proto.WorkerType, cluster string, vols []*proto.SmartVolume, taskNum int64, runningTasks []*proto.Task) (newTasks []*proto.Task, err error) {
	sortVolumesByTaskCountASC(w, workerType, cluster, vols, func(v *proto.SmartVolume) string {
		return v.Name
	})
	for _, vol := range vols {
		if len(vol.SmartRules) == 0 {
			log.LogWarnf("FileMigrationWorker CreateMpTask cluster(%v) volName(%v) verify smartRules is null", cluster, vol.Name)
			continue
		}
		log.LogDebugf("FileMigrationWorker CreateTask begin clusterId(%v), volumeName(%v), taskNum(%v), runningTasksLength(%v)", cluster, vol.Name, taskNum, len(runningTasks))
		var mpView []*proto.MetaPartitionView
		mpView, err = w.GetMpView(cluster, vol.Name)
		if err != nil {
			log.LogErrorf("FileMigrationWorker CreateMpTask GetMpView cluster(%v) volName(%v) err(%v)", cluster, vol.Name, err)
			continue
		}
		sort.Slice(mpView, func(i, j int) bool {
			return mpView[i].PartitionID < mpView[j].PartitionID
		})
		key := migcore.WorkerTypeKey(workerType, cluster, vol.Name)
		w.resetTaskPosition(key, mpView)
		newTasks, err = w.createAndManageFileMigrateTasks(vol, mpView, runningTasks, taskNum, workerType)
	}
	return
}

func (w *Worker) createAndManageFileMigrateTasks(vol *proto.SmartVolume, mpView []*proto.MetaPartitionView,
	runningTasks []*proto.Task, taskNum int64, workerType proto.WorkerType) (newTasks []*proto.Task, err error) {
	volumeTaskNum := w.getTaskCountByVolume(runningTasks, vol.Name)
	key := migcore.WorkerTypeKey(workerType, vol.ClusterId, vol.Name)
	if !w.checkCanCreateTask(key) {
		return
	}
	for i, mp := range mpView {
		var id uint64
		if value, ok := w.volumeTaskPos.Load(key); ok {
			id = value.(uint64)
		}
		if mp.PartitionID <= id {
			continue
		}
		if volumeTaskNum >= DefaultVolumeMaxExecMPNum {
			break
		}
		if mp.InodeCount == 0 {
			w.setVolumeTaskPos(i, len(mpView), key, mp.PartitionID, false)
			continue
		}
		if !checkLayerPoliciesValid(vol) {
			break
		}
		var newTask *proto.Task
		newTask, err = w.CheckAndAddTask(vol.ClusterId, vol.Name, mp.PartitionID, runningTasks, workerType, FileMigrateOpen)
		if err != nil {
			continue
		}
		w.setVolumeTaskCnt(key)
		w.setVolumeTaskPos(i, len(mpView), key, mp.PartitionID, true)
		volumeTaskNum++
		newTasks = append(newTasks, newTask)
		if int64(len(newTasks)) >= taskNum {
			break
		}
	}
	return
}

func (w *Worker) setVolumeTaskPos(index, mpLen int, workerTypeKey string, curPartitionId uint64, isSetVolumeLastTaskTime bool) {
	if index >= mpLen-1 {
		w.volumeTaskPos.Store(workerTypeKey, uint64(0))
		if isSetVolumeLastTaskTime {
			// 所有mp创建一轮后设置任务创建时间
			w.volumeLastTaskTime.Store(workerTypeKey, time.Now().Unix())
		}
	} else {
		w.volumeTaskPos.Store(workerTypeKey, curPartitionId)
	}
}

func (w *Worker) setVolumeTaskCnt(workerTypeKey string) {
	if value, ok := w.volumeTaskCnt.Load(workerTypeKey); ok {
		w.volumeTaskCnt.Store(workerTypeKey, value.(uint64)+1)
	} else {
		w.volumeTaskCnt.Store(workerTypeKey, uint64(1))
	}
}

func checkLayerPoliciesValid(vol *proto.SmartVolume) bool {
	layerPolicies, ok := vol.LayerPolicies[proto.LayerTypeInodeATime]
	if !ok {
		return false
	}
	if len(layerPolicies) == 0 {
		return false
	}
	return true
}

func (w *Worker) CheckAndAddTask(cluster, volName string, mpId uint64, runningTasks []*proto.Task, wt proto.WorkerType, taskInfo string) (task *proto.Task, err error) {
	newTask := proto.NewDataTask(wt, cluster, volName, 0, mpId, taskInfo)
	var exist bool
	exist, _, err = w.ContainMPTask(newTask, runningTasks)
	if err != nil {
		log.LogWarnf("CheckAndAddTask ContainMPTask failed, cluster(%v), volume(%v) exist(%v), err(%v)",
			cluster, volName, exist, err)
		return
	}
	if exist {
		err = fmt.Errorf("CheckAndAddTask task(%v) exist", task)
		return
	}
	var taskId uint64
	if taskId, err = w.AddTask(newTask); err != nil {
		log.LogErrorf("CheckAndAddTask to database failed, cluster(%v), volume(%v), task(%v), err(%v)",
			cluster, volName, newTask, err)
		return
	}
	newTask.TaskId = taskId
	return newTask, nil
}

func (w *Worker) checkCanCreateTask(key string) (canCreate bool) {
	var (
		lastTaskTime int64
		mpId         uint64
	)
	if v, ok := w.volumeLastTaskTime.Load(key); ok {
		lastTaskTime = v.(int64)
	}
	if v, ok := w.volumeTaskPos.Load(key); ok {
		mpId = v.(uint64)
	}
	if mpId == 0 && time.Now().Unix()-lastTaskTime < TaskTimeInterval {
		log.LogInfof("last task time create file migrate task time is too short, volume(%v)", key)
		return false
	}
	return true
}

func (w *Worker) createAndManageCompactTasks(cluster string, vol *proto.DataMigVolume, mpView []*proto.MetaPartitionView,
	runningTasks []*proto.Task, taskNum int64, workerType proto.WorkerType) (newTasks []*proto.Task, err error) {
	volumeTaskNum := w.getTaskCountByVolume(runningTasks, vol.Name)
	key := migcore.WorkerTypeKey(workerType, cluster, vol.Name)
	if !w.checkCanCreateTask(key) {
		return
	}
	for index, mp := range mpView {
		var mpId uint64 = 0
		if value, ok := w.volumeTaskPos.Load(key); ok {
			mpId = value.(uint64)
		}
		if mp.PartitionID <= mpId {
			continue
		}
		if volumeTaskNum >= DefaultVolumeMaxExecMPNum {
			break
		}
		if mp.InodeCount == 0 {
			w.setVolumeTaskPos(index, len(mpView), key, mp.PartitionID, false)
			continue
		}
		var newTask *proto.Task
		newTask, err = w.CheckAndAddTask(cluster, vol.Name, mp.PartitionID, runningTasks, workerType, vol.CompactTag.String())
		if err != nil {
			continue
		}
		w.setVolumeTaskCnt(key)
		w.setVolumeTaskPos(index, len(mpView), key, mp.PartitionID, true)
		volumeTaskNum++
		newTasks = append(newTasks, newTask)
		if int64(len(newTasks)) >= taskNum {
			return
		}
	}
	return
}

func (w *Worker) GetMpView(cluster string, volName string) (mpView []*proto.MetaPartitionView, err error) {
	var mc *master.MasterClient
	if value, ok := w.masterClients.Load(cluster); !ok {
		return nil, fmt.Errorf("GetMpView cluster(%v) does not exist", cluster)
	} else {
		mc = value.(*master.MasterClient)
	}
	mpView, err = mc.ClientAPI().GetMetaPartitions(volName)
	if err != nil {
		return
	}
	return
}

func (w *Worker) getTaskCountByVolume(runningTasks []*proto.Task, volume string) (cnt int) {
	for _, task := range runningTasks {
		if task.VolName == volume {
			cnt++
		}
	}
	return
}

func (w *Worker) resetTaskPosition(volumeKey string, mpView []*proto.MetaPartitionView) {
	var mpPos uint64
	if value, ok := w.volumeTaskPos.Load(volumeKey); ok {
		mpPos = value.(uint64)
	}
	if len(mpView) > 0 && mpView[len(mpView)-1].PartitionID <= mpPos {
		w.volumeTaskPos.Store(volumeKey, uint64(0))
	}
}

func (w *Worker) filterSoonCloseFileMigrateVols(clusterId string, fileMigrateVolMap volumeMap, runningTasks []*proto.Task) (fileMigrateCloseVols []string) {
	if _, ok := w.lastFileMigrateVolume.Load(clusterId); !ok {
		w.lastFileMigrateVolume.Store(clusterId, make(volumeMap))
	}
	value, _ := w.lastFileMigrateVolume.Load(clusterId)
	for _, task := range runningTasks {
		value.(volumeMap)[task.VolName] = struct{}{}
	}
	for volName := range value.(volumeMap) {
		if _, ok := fileMigrateVolMap[volName]; !ok {
			fileMigrateCloseVols = append(fileMigrateCloseVols, volName)
		}
	}
	w.lastFileMigrateVolume.Store(clusterId, fileMigrateVolMap)
	return
}

func (w *Worker) CloseFileMigrateTask(cluster string, vols []string, wns []*proto.WorkerNode) {
	for _, volName := range vols {
		log.LogDebugf("CloseFileMigrateTask clusterId(%v), vol(%v) wnsLength(%v)", cluster, volName, len(wns))
		mpView, err := w.GetMpView(cluster, volName)
		if err != nil {
			log.LogErrorf("CloseFileMigrateTask GetMpView cluster(%v) volName(%v) err(%v)", cluster, volName, err)
			continue
		}
		workerAddrMap := w.processMpView(cluster, volName, mpView, wns, proto.WorkerTypeInodeMigration, FileMigrateClose)
		// call worker node stop file migrate task
		w.stopTasks(cluster, volName, workerAddrMap, FileMigrateStop)
	}
}

func (w *Worker) processFileMigrateMpView(cluster, volName string, mpView []*proto.MetaPartitionView, wns []*proto.WorkerNode) map[string]struct{} {
	workerAddrMap := make(map[string]struct{})
	for _, mp := range mpView {
		newTask := proto.NewDataTask(proto.WorkerTypeInodeMigration, cluster, volName, 0, mp.PartitionID, FileMigrateClose)
		exist, oldTask, err := w.ContainMPTaskByWorkerNodes(newTask, wns)
		if err != nil {
			log.LogErrorf("FileMigrationWorker CancelMpTask ContainMPTaskByWorkerNodes failed, cluster(%v), volume(%v), err(%v)",
				cluster, volName, err)
			continue
		}
		if !exist {
			continue
		}
		log.LogDebugf("FileMigrationWorker CancelMpTask oldTask WorkerAddr(%v), task(%v)", oldTask.WorkerAddr, oldTask)
		if oldTask.WorkerAddr != "" {
			workerAddrMap[oldTask.WorkerAddr] = struct{}{}
		}

		if err = w.UpdateTaskInfo(oldTask.TaskId, FileMigrateClose); err != nil {
			log.LogErrorf("FileMigrationWorker CancelMpTask UpdateTaskInfo to database failed, cluster(%v), volume(%v), task(%v), err(%v)",
				cluster, volName, newTask, err)
		}
	}
	return workerAddrMap
}

func (w *Worker) parseConfig(cfg *config.Config) (err error) {
	err = w.ParseBaseConfig(cfg)
	if err != nil {
		return
	}
	w.parseMasterAddr(cfg)
	return
}

// initWorkerForScheduler scheduleNode进程调用此方法
func (w *Worker) initWorkerForScheduler() (err error) {
	w.TaskChan = make(chan *proto.Task, worker.DefaultTaskChanLength)
	for cluster, addresses := range w.masterAddr {
		w.masterClients.Store(cluster, master.NewMasterClient(addresses, false))
	}
	if err = mysql.InitMysqlClient(w.MysqlConfig); err != nil {
		log.LogErrorf("[initWorker] init mysql client failed, error(%v)", err)
		return
	}
	return
}

func (w *Worker) loadVolumeInfo() {
	switch w.WorkerType {
	case proto.WorkerTypeCompact:
		w.loadCompactVolume()
	case proto.WorkerTypeInodeMigration:
		w.loadSmartVolume()
	}
}

func (w *Worker) loadCompactVolume() {
	timer := time.NewTimer(0)
	defer timer.Stop()

	for {
		log.LogDebugf("[loadCompactVolume] compact volume loader is running, workerType(%v), localIp(%v)", w.WorkerType, w.LocalIp)

		select {
		case <-timer.C:
			if err := w.loadAndStoreCompactVolumes(); err != nil {
				log.LogErrorf("[loadCompactVolume] compact volume loader has exception, err(%v)", err)
			}
			timer.Reset(time.Second * DefaultVolumeLoadDuration)
		case <-w.StopC:
			return
		}
	}
}

func (w *Worker) loadAndStoreCompactVolumes() error {
	metrics := exporter.NewModuleTP(proto.MonitorCompactLoadCompactVolume)
	defer metrics.Set(nil)

	clusterVolumes := make(map[string][]*proto.DataMigVolume)
	migrationConfigs := loadAllMigrationConfig("", "")

	for _, vc := range migrationConfigs {
		mc, ok := w.getMasterClient(vc.ClusterName)
		if !ok {
			continue
		}

		volInfo, err := mc.AdminAPI().GetVolumeSimpleInfo(vc.VolName)
		if err != nil {
			continue
		}

		dataMigVolume := &proto.DataMigVolume{
			Name:       vc.VolName,
			Owner:      volInfo.Owner,
			CompactTag: convertCompactTag(vc.Compact),
			VolId:      volInfo.ID,
		}
		clusterVolumes[vc.ClusterName] = append(clusterVolumes[vc.ClusterName], dataMigVolume)
	}

	for cluster, volumes := range clusterVolumes {
		cvv := proto.NewDataMigVolumeView(cluster, volumes)
		w.volumeView.Store(cluster, cvv)
	}

	return nil
}

func convertCompactTag(compact int8) (tag proto.CompactTag) {
	if compact == compactEnabled {
		tag = proto.CompactOpen
	} else {
		tag = proto.CompactClose
	}
	return
}

func GetCompactVolumes(cluster string, mc *master.MasterClient) (cvv *proto.DataMigVolumeView, err error) {
	var volumes []*proto.DataMigVolume
	volumes, err = mc.AdminAPI().ListCompactVolumes()
	if err != nil {
		log.LogErrorf("[GetCompactVolumes] cluster(%v) masterAddr(%v) err(%v)", cluster, mc.Nodes(), err)
		return
	}
	if len(volumes) == 0 {
		return
	}
	cvv = proto.NewDataMigVolumeView(cluster, volumes)
	return
}

type FileMigrateVolumeView struct {
	Cluster      string
	SmartVolumes map[string]*proto.SmartVolume
}

func (w *Worker) loadSmartVolume() {
	timer := time.NewTimer(0)
	defer timer.Stop()

	for {
		log.LogDebugf("[loadSmartVolume] FileMigrationWorker smart volume loader is running, workerId(%v), workerAddr(%v)",
			w.WorkerId, w.WorkerAddr)

		select {
		case <-timer.C:
			if err := w.loadAndProcessVolumes(); err != nil {
				log.LogErrorf("[loadSmartVolume] FileMigrationWorker smart volume loader has exception, err(%v)", err)
			}
			timer.Reset(time.Second * DefaultVolumeLoadDuration)
		case <-w.StopC:
			return
		}
	}
}

func (w *Worker) loadAndProcessVolumes() error {
	metrics := exporter.NewModuleTP(proto.MonitorSmartLoadSmartVolumeInode)
	defer metrics.Set(nil)

	clusterVolumes := make(map[string]map[string]*proto.SmartVolume)
	volumeConfigs := loadAllMigrationConfig("", "")

	for _, vc := range volumeConfigs {
		mc, ok := w.getMasterClient(vc.ClusterName)
		if !ok {
			log.LogErrorf("cannot [getMasterClient] cluster(%v) ", vc.ClusterName)
			continue
		}

		smartVolume, err := w.createSmartVolume(mc, vc)
		if err != nil {
			log.LogErrorf("[createSmartVolume] cluster(%v) vol(%v) err(%v)", vc.ClusterName, vc.VolName, err)
			continue
		}

		if _, ok := clusterVolumes[vc.ClusterName]; !ok {
			clusterVolumes[vc.ClusterName] = make(map[string]*proto.SmartVolume)
		}
		clusterVolumes[vc.ClusterName][vc.VolName] = smartVolume
	}

	w.storeVolumeViews(clusterVolumes)
	return nil
}

func (w *Worker) createSmartVolume(mc *master.MasterClient, vc *proto.MigrationConfig) (*proto.SmartVolume, error) {
	volInfo, err := mc.AdminAPI().GetVolumeSimpleInfo(vc.VolName)
	if err != nil {
		return nil, fmt.Errorf("failed to get volume simple info: %v", err)
	}

	dpView, err := mc.ClientAPI().GetDataPartitions(vc.VolName, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to get data partitions: %v", err)
	}

	smartVolume := &proto.SmartVolume{
		ClusterId:      vc.ClusterName,
		Name:           vc.VolName,
		VolId:          volInfo.ID,
		Owner:          volInfo.Owner,
		StoreMode:      volInfo.DefaultStoreMode,
		Smart:          vc.Smart,
		SmartRules:     strings.Split(vc.SmartRules, ","),
		HddDirs:        vc.HddDirs,
		SsdDirs:        vc.SsdDirs,
		MigrationBack:  vc.MigrationBack,
		DataPartitions: dpView.DataPartitions,
		BoundBucket:    volInfo.BoundBucket,
	}

	if smartVolume.BoundBucket == nil {
		smartVolume.BoundBucket = &proto.BoundBucketInfo{}
	}

	return smartVolume, nil
}

func (w *Worker) storeVolumeViews(clusterVolumes map[string]map[string]*proto.SmartVolume) {
	for cluster, volumes := range clusterVolumes {
		cvv := &FileMigrateVolumeView{
			Cluster:      cluster,
			SmartVolumes: volumes,
		}

		for volume, vv := range cvv.SmartVolumes {
			log.LogInfof("smart info volume:%v vv:%v", volume, vv.SmartRules)
			if !w.parseLayerPolicy(vv) {
				delete(cvv.SmartVolumes, volume)
			}
		}

		w.volumeView.Store(cluster, cvv)
	}
}

func GetSmartVolumes(cluster string, mc *master.MasterClient) (svv *FileMigrateVolumeView, err error) {
	var volumes []*proto.SmartVolume
	smartVolumes := make(map[string]*proto.SmartVolume)
	svv = &FileMigrateVolumeView{
		Cluster:      cluster,
		SmartVolumes: smartVolumes,
	}
	volumes, err = mc.AdminAPI().ListSmartVolumes()
	if err != nil {
		log.LogErrorf("[GetSmartVolumes] FileMigrationWorker list smart volumes failed, cluster(%v), err(%v)", cluster, err)
		return
	}
	if len(volumes) == 0 {
		log.LogInfof("[GetSmartVolumes] FileMigrationWorker no smart volumes, cluster(%v)", cluster)
		return
	}

	var smartVolume *proto.SmartVolume
	for _, volume := range volumes {
		smartVolume, err = mc.AdminAPI().GetSmartVolume(volume.Name, migcore.CalcAuthKey(volume.Owner))
		if err != nil {
			log.LogErrorf("[GetSmartVolumes] FileMigrationWorker get volume info failed, cluster(%v), volumeName(%v), err(%v)", cluster, volume.Name, err)
			return
		}

		smartVolume.ClusterId = cluster
		smartVolumes[volume.Name] = smartVolume
		smartVolume.StoreMode = volume.StoreMode
	}
	return
}

func (w *Worker) parseLayerPolicy(volume *proto.SmartVolume) (hasInodeATimeLayer bool) {
	if volume.SmartRules == nil || len(volume.SmartRules) == 0 {
		log.LogErrorf("[parseLayerPolicy] FileMigrationWorker smart volume smart rules is empty, clusterId(%v), volumeName(%v), smartRules(%v)",
			volume.ClusterId, volume.Name, volume.SmartRules)
		return
	}

	var (
		err error
		lt  proto.LayerType
		lp  interface{}
	)
	if err = proto.CheckLayerPolicy(volume.ClusterId, volume.Name, volume.SmartRules); err != nil {
		log.LogErrorf("[parseLayerPolicy] FileMigrationWorker check volume layer policy failed, cluster(%v) ,volumeName(%v), smartRules(%v), err(%v)",
			volume.ClusterId, volume.Name, volume.SmartRules, err)
		return
	}

	volume.LayerPolicies = make(map[proto.LayerType][]interface{})
	for _, originRule := range volume.SmartRules {
		originRule = strings.ReplaceAll(originRule, " ", "")
		lt, lp, err = proto.ParseLayerPolicy(volume.ClusterId, volume.Name, originRule)
		if err != nil {
			log.LogErrorf("[parseLayerPolicy] FileMigrationWorker parse layer type failed, cluster(%v) ,volumeName(%v), originRule(%v), err(%v)",
				volume.ClusterId, volume.Name, originRule, err)
			return
		}
		switch lt {
		case proto.LayerTypeInodeATime:
			at := lp.(*proto.LayerPolicyInodeATime)
			volume.LayerPolicies[proto.LayerTypeInodeATime] = append(volume.LayerPolicies[proto.LayerTypeInodeATime], at)
			hasInodeATimeLayer = true
		}
	}
	return
}

func (w *Worker) GetCreatorDuration() int {
	return w.WorkerConfig.TaskCreatePeriod
}

func sortVolumesByTaskCountASC[T any](w *Worker, workerType proto.WorkerType, cluster string, vols []T, getVolName func(T) string) {
	sort.Slice(vols, func(i, j int) bool {
		key1 := migcore.WorkerTypeKey(workerType, cluster, getVolName(vols[i]))
		key2 := migcore.WorkerTypeKey(workerType, cluster, getVolName(vols[j]))
		var (
			taskCnt1, taskCnt2 uint64
		)
		if v, ok := w.volumeTaskCnt.Load(key1); ok {
			taskCnt1 = v.(uint64)
		}
		if v, ok := w.volumeTaskCnt.Load(key2); ok {
			taskCnt2 = v.(uint64)
		}
		return taskCnt1 < taskCnt2
	})
}