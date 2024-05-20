package migration

import (
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/schedulenode/worker"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/sdk/mysql"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"sort"
	"strings"
	"time"
)

const (
	DefaultVolumeMaxExecMPNum = 5
	DefaultVolumeLoadDuration = 60
)

const (
	FileMigrateOpen  = "Enabled"
	FileMigrateClose = "Disabled"
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
	w.cancelCompactTask(clusterId, compactCloseVols, wns)
	newTasks, err = w.createCompactTaskByVolume(clusterId, compactOpenVols, taskNum, runningTasks)
	return
}

func (w *Worker) cancelCompactTask(cluster string, vols []*proto.DataMigVolume, wns []*proto.WorkerNode) {
	for _, vol := range vols {
		log.LogDebugf("CompactWorker CancelMpTask clusterId(%v), vol(%v) wnsLength(%v)", cluster, vol.Name, len(wns))
		mpView, err := w.GetMpView(cluster, vol.Name)
		if err != nil {
			log.LogErrorf("CompactWorker CancelMpTask GetMpView cluster(%v) volName(%v) err(%v)", cluster, vol.Name, err)
			continue
		}
		workerAddrMap := make(map[string]struct{}, 0)
		for _, mp := range mpView {
			newTask := proto.NewDataTask(proto.WorkerTypeCompact, cluster, vol.Name, 0, mp.PartitionID, vol.CompactTag.String())
			var exist bool
			var oldTask *proto.Task
			if exist, oldTask, err = w.ContainMPTaskByWorkerNodes(newTask, wns); err != nil {
				log.LogErrorf("CompactWorker CancelMpTask ContainMPTaskByWorkerNodes failed, cluster(%v), volume(%v), err(%v)",
					cluster, vol.Name, err)
				continue
			}
			if !exist {
				continue
			}
			log.LogDebugf("CancelMpTask oldTask WorkerAddr(%v), task(%v)", oldTask.WorkerAddr, oldTask)
			if oldTask.WorkerAddr != "" {
				workerAddrMap[oldTask.WorkerAddr] = struct{}{}
			}

			if err = w.UpdateTaskInfo(oldTask.TaskId, vol.CompactTag.String()); err != nil {
				log.LogErrorf("CompactWorker CancelMpTask UpdateTaskInfo to database failed, cluster(%v), volume(%v), task(%v), err(%v)",
					cluster, vol.Name, newTask, err)
			}
		}
		// call worker node stop compact task
		for addr := range workerAddrMap {
			var res *proto.QueryHTTPResult
			stopCompactUrl := GenStopUrl(addr, CompactStop, cluster, vol.Name)
			for i := 0; i < RetryDoGetMaxCnt; i++ {
				if res, err = DoGet(stopCompactUrl); err != nil {
					log.LogErrorf("CompactWorker CancelMpTask doGet failed, cluster(%v), volume(%v), stopCompactUrl(%v), err(%v)",
						cluster, vol.Name, stopCompactUrl, err)
					continue
				}
				break
			}
			log.LogDebugf("CompactWorker CancelMpTask doGet stopCompactUrl(%v), result(%v)", stopCompactUrl, res)
		}
	}
}

func (w *Worker) createCompactTaskByVolume(cluster string, vols []*proto.DataMigVolume, taskNum int64, runningTasks []*proto.Task) (newTasks []*proto.Task, err error) {
	sort.Slice(vols, func(i, j int) bool {
		key1 := workerTypeKey(proto.WorkerTypeCompact, cluster, vols[i].Name)
		key2 := workerTypeKey(proto.WorkerTypeCompact, cluster, vols[j].Name)
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
		key := workerTypeKey(proto.WorkerTypeCompact, cluster, vol.Name)
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
	w.sortVolumesByTaskCountASC(workerType, cluster, vols)

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
		key := workerTypeKey(workerType, cluster, vol.Name)
		w.resetTaskPosition(key, mpView)
		newTasks, err = w.createAndManageFileMigrateTasks(vol, mpView, runningTasks, taskNum, workerType)
	}
	return
}

func (w * Worker) createAndManageFileMigrateTasks(vol *proto.SmartVolume, mpView []*proto.MetaPartitionView,
	runningTasks []*proto.Task, taskNum int64, workerType proto.WorkerType) (newTasks []*proto.Task, err error) {
	volumeTaskNum := w.getTaskCountByVolume(runningTasks, vol.Name)
	key := workerTypeKey(workerType, vol.ClusterId, vol.Name)
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
			if i >= len(mpView)-1 {
				w.volumeTaskPos.Store(key, uint64(0))
			} else {
				w.volumeTaskPos.Store(key, mp.PartitionID)
			}
			continue
		}
		if layerPolicies, ok := vol.LayerPolicies[proto.LayerTypeInodeATime]; !ok || len(layerPolicies) == 0 {
			break
		}
		// recent layer policy
		newTask := proto.NewDataTask(proto.WorkerTypeInodeMigration, vol.ClusterId, vol.Name, 0, mp.PartitionID, FileMigrateOpen)
		var exist bool
		if exist, _, err = w.ContainMPTask(newTask, runningTasks); err != nil || exist {
			log.LogErrorf("FileMigrationWorker CreateMpTask ContainMPTask failed, cluster(%v), volume(%v) exist(%v), err(%v)",
				vol.ClusterId, vol.Name, exist, err)
			continue
		}
		var taskId uint64
		if taskId, err = w.AddTask(newTask); err != nil {
			log.LogErrorf("FileMigrationWorker CreateMpTask AddTask to database failed, cluster(%v), volume(%v), task(%v), err(%v)",
				vol.ClusterId, vol.Name, newTask, err)
			continue
		}
		if value, ok := w.volumeTaskCnt.Load(key); ok {
			w.volumeTaskCnt.Store(key, value.(uint64)+1)
		} else {
			w.volumeTaskCnt.Store(key, uint64(1))
		}
		if i >= len(mpView)-1 {
			w.volumeTaskPos.Store(key, uint64(0))
		} else {
			w.volumeTaskPos.Store(key, mp.PartitionID)
		}
		newTask.TaskId = taskId
		volumeTaskNum++
		newTasks = append(newTasks, newTask)
		if int64(len(newTasks)) >= taskNum {
			return
		}
	}
	return
}

func (w * Worker) createAndManageCompactTasks(cluster string, vol *proto.DataMigVolume, mpView []*proto.MetaPartitionView,
	runningTasks []*proto.Task, taskNum int64, workerType proto.WorkerType) (newTasks []*proto.Task, err error) {
	volumeTaskNum := w.getTaskCountByVolume(runningTasks, vol.Name)
	key := workerTypeKey(workerType, cluster, vol.Name)
	for i, mp := range mpView {
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
			if i >= len(mpView)-1 {
				w.volumeTaskPos.Store(key, uint64(0))
			} else {
				w.volumeTaskPos.Store(key, mp.PartitionID)
			}
			continue
		}
		newTask := proto.NewDataTask(workerType, cluster, vol.Name, 0, mp.PartitionID, vol.CompactTag.String())
		var exist bool
		var oldTask *proto.Task
		if exist, oldTask, err = w.ContainMPTask(newTask, runningTasks); err != nil {
			log.LogErrorf("CompactWorker CreateMpTask ContainMPTask failed, cluster(%v), volume(%v), err(%v)",
				cluster, vol.Name, err)
			continue
		}
		if exist && oldTask.Status != proto.TaskStatusSucceed {
			continue
		}
		var taskId uint64
		if taskId, err = w.AddTask(newTask); err != nil {
			log.LogErrorf("CompactWorker CreateMpTask AddTask to database failed, cluster(%v), volume(%v), task(%v), err(%v)",
				cluster, vol.Name, newTask, err)
			continue
		}
		if value, ok := w.volumeTaskCnt.Load(key); ok {
			w.volumeTaskCnt.Store(key, value.(uint64)+1)
		} else {
			w.volumeTaskCnt.Store(key, uint64(1))
		}
		if i >= len(mpView)-1 {
			w.volumeTaskPos.Store(key, uint64(0))
		} else {
			w.volumeTaskPos.Store(key, mp.PartitionID)
		}
		newTask.TaskId = taskId
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

func (w * Worker) getTaskCountByVolume(runningTasks []*proto.Task, volume string) (cnt int) {
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
		log.LogDebugf("FileMigrationWorker CancelMpTask clusterId(%v), vol(%v) wnsLength(%v)", cluster, volName, len(wns))
		mpView, err := w.GetMpView(cluster, volName)
		if err != nil {
			log.LogErrorf("FileMigrationWorker CancelMpTask GetMpView cluster(%v) volName(%v) err(%v)", cluster, volName, err)
			continue
		}
		workerAddrMap := make(map[string]struct{}, 0)
		for _, mp := range mpView {
			newTask := proto.NewDataTask(proto.WorkerTypeInodeMigration, cluster, volName, 0, mp.PartitionID, FileMigrateClose)
			var exist bool
			var oldTask *proto.Task
			if exist, oldTask, err = w.ContainMPTaskByWorkerNodes(newTask, wns); err != nil {
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
		// call worker node stop file migrate task
		for addr := range workerAddrMap {
			var res *proto.QueryHTTPResult
			stopFileMigrateUrl := GenStopUrl(addr, FileMigrateStop, cluster, volName)
			for i := 0; i < RetryDoGetMaxCnt; i++ {
				if res, err = DoGet(stopFileMigrateUrl); err != nil {
					log.LogErrorf("FileMigrationWorker CancelMpTask doGet failed, cluster(%v), volume(%v), stopFileMigrateUrl(%v), err(%v)",
						cluster, volName, stopFileMigrateUrl, err)
					continue
				}
				break
			}
			log.LogDebugf("FileMigrationWorker CancelMpTask doGet stopFileMigrateUrl(%v), result(%v)", stopFileMigrateUrl, res)
		}
	}
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
	for {
		log.LogDebugf("[loadCompactVolume] compact volume loader is running, workerType(%v), localIp(%v)", w.WorkerType, w.LocalIp)
		select {
		case <-timer.C:
			loader := func() (err error) {
				metrics := exporter.NewModuleTP(proto.MonitorCompactLoadCompactVolume)
				defer metrics.Set(err)
				w.masterClients.Range(func(key, value interface{}) bool {
					cluster := key.(string)
					mc := value.(*master.MasterClient)
					var cmpVolView *proto.DataMigVolumeView
					cmpVolView, err = GetCompactVolumes(cluster, mc)
					if err != nil {
						log.LogErrorf("[loadCompactVolume] get cluster compact volumes failed, cluster(%v), err(%v)", cluster, err)
						return true
					}
					if cmpVolView == nil || len(cmpVolView.DataMigVolumes) == 0 {
						log.LogWarnf("[loadCompactVolume] got all compact volumes is empty, cluster(%v)", cluster)
						return true
					}
					w.volumeView.Store(cluster, cmpVolView)
					return true
				})
				return
			}
			if err := loader(); err != nil {
				log.LogErrorf("[loadCompactVolume] compact volume loader has exception, err(%v)", err)
			}
			timer.Reset(time.Second * DefaultVolumeLoadDuration)
		case <-w.StopC:
			timer.Stop()
			return
		}
	}
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
	for {
		log.LogDebugf("[loadSmartVolume] FileMigrationWorker smart volume loader is running, workerId(%v), workerAddr(%v)",
			w.WorkerId, w.WorkerAddr)
		select {
		case <-timer.C:
			loader := func() (err error) {
				metrics := exporter.NewModuleTP(proto.MonitorSmartLoadSmartVolumeInode)
				defer metrics.Set(err)

				w.masterClients.Range(func(key, value interface{}) bool {
					cluster := key.(string)
					mc := value.(*master.MasterClient)
					var smartVolumeView *FileMigrateVolumeView
					smartVolumeView, err = GetSmartVolumes(cluster, mc)
					if err != nil {
						log.LogErrorf("[loadSmartVolume] get cluster smart volumes failed, cluster(%v), err(%v)", cluster, err)
						return true
					}
					// parse smart volume layer policy
					for volume, vv := range smartVolumeView.SmartVolumes {
						log.LogInfof("smart info volume:%v vv:%v", volume, vv.SmartRules)
						if !w.parseLayerPolicy(vv) {
							delete(smartVolumeView.SmartVolumes, volume)
						}
					}
					w.volumeView.Store(cluster, smartVolumeView)
					return true
				})
				return
			}
			if err := loader(); err != nil {
				log.LogErrorf("[loadSmartVolume] FileMigrationWorker smart volume loader has exception, err(%v)", err)
			}
			timer.Reset(time.Second * DefaultVolumeLoadDuration)
		case <-w.StopC:
			timer.Stop()
			return
		}
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
		smartVolume, err = mc.AdminAPI().GetSmartVolume(volume.Name, CalcAuthKey(volume.Owner))
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

func (w *Worker) sortVolumesByTaskCountASC(workerType proto.WorkerType, cluster string, vols []*proto.SmartVolume) {
	sort.Slice(vols, func(i, j int) bool {
		key1 := workerTypeKey(workerType, cluster, vols[i].Name)
		key2 := workerTypeKey(workerType, cluster, vols[j].Name)
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
