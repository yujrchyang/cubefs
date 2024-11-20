package rebalance

import (
	"fmt"
	"github.com/cubefs/cubefs/sdk/master"
	"time"
)

func (rw *ReBalanceWorker) VolMigrateStart(cluster, module, srcZone, dstZone string, volList []string, clusterCurrency,
	volCurrency, partitionCurrency, waitSecond, dstMpMaxCount int, dstNodeUsage float64) (mt *MigVolTask, err error) {

	isRestart := false
	mt, err = rw.newVolMigrateCtrl(cluster, module, srcZone, dstZone, volList, clusterCurrency, volCurrency, partitionCurrency,
		waitSecond, dstMpMaxCount, dstNodeUsage, isRestart, 0)
	if err != nil {
		return
	}
	mt.VolMigrateStart()
	return
}

func (rw *ReBalanceWorker) newVolMigrateCtrl(cluster, module, srcZone, dstZone string, volList []string, clusterCurrency, volCurrency,
	partitionCurrency, waitSecond, dstMpMaxCount int, dstNodeUsage float64, isRestart bool, taskID uint64) (mt *MigVolTask, err error) {

	mt = &MigVolTask{
		taskId:                 taskID,
		cluster:                cluster,
		module:                 module,
		srcZone:                srcZone,
		dstZone:                dstZone,
		MasterClient:           master.NewMasterClient([]string{rw.getClusterHost(cluster)}, false),
		vols:                   volList,
		finishVols:             make(map[string]bool, len(volList)),
		clusterCurrency:        clusterCurrency,
		volCurrency:            volCurrency,
		volDpCurrency:          partitionCurrency,
		volMpCurrency:          partitionCurrency,
		maxDstDatanodeUsage:    dstNodeUsage,
		maxDstMetanodeUsage:    dstNodeUsage, // nodeUsage 模块通用
		maxDstMetaPartitionCnt: dstMpMaxCount,
		roundInterval:          waitSecond,
		status:                 StatusRunning,
		worker:                 rw,
	}
	if dstMpMaxCount <= 0 {
		mt.maxDstMetaPartitionCnt = defaultDstMetaNodePartitionMaxCount
	}
	if isRestart {
		err = rw.updateMigrateTaskStatus(taskID, int(StatusRunning))
	} else {
		taskID, err = rw.insertMigrateTask(mt)
	}
	if err != nil {
		return
	}
	mt.SetTaskId(taskID)
	rw.volMigrateMap.Store(taskID, mt)
	return
}

func (rw *ReBalanceWorker) resetMigrateControl(taskID uint64, clusterCurrency, volCurrency, partitionCurrency, roundInterval int) (err error) {
	// 先更数据库，再修改内存
	var taskInfo *MigVolTask
	if v, ok := rw.volMigrateMap.Load(taskID); !ok {
		return fmt.Errorf("can't find task, id(%v)", taskID)
	} else {
		taskInfo = v.(*MigVolTask)
	}
	if clusterCurrency <= 0 {
		clusterCurrency = taskInfo.clusterCurrency
	}
	if volCurrency <= 0 {
		volCurrency = taskInfo.volCurrency
	}
	if partitionCurrency <= 0 {
		if taskInfo.module == "data" {
			partitionCurrency = taskInfo.volDpCurrency
		} else {
			partitionCurrency = taskInfo.volMpCurrency
		}
	}
	if roundInterval <= 0 {
		roundInterval = taskInfo.roundInterval
	}
	err = rw.updateMigrateControl(taskID, clusterCurrency, volCurrency, partitionCurrency, roundInterval)
	if err != nil {
		return
	}
	taskInfo.clusterCurrency = clusterCurrency
	taskInfo.volCurrency = volCurrency
	taskInfo.volMpCurrency = partitionCurrency
	taskInfo.volDpCurrency = partitionCurrency
	taskInfo.roundInterval = roundInterval
	rw.volMigrateMap.Store(taskID, taskInfo)
	return
}

func (rw *ReBalanceWorker) stopVolMigrateTask(taskID uint64, isManualStop bool) (err error) {
	if taskID <= 0 {
		return fmt.Errorf("请指定任务ID")
	}
	var taskInfo *RebalancedInfoTable
	taskInfo, err = rw.GetRebalancedInfoByID(taskID)
	if err != nil {
		return
	}
	if taskInfo.Status == int(StatusStop) || taskInfo.Status == int(StatusTerminating) {
		return fmt.Errorf("taskID(%v) not in running", taskID)
	}
	if isManualStop {
		taskInfo.Status = int(StatusTerminating)
	} else {
		taskInfo.Status = int(StatusStop)
	}
	err = rw.dbHandle.Table(RebalancedInfoTable{}.TableName()).Where("id = ?", taskID).
		Updates(map[string]interface{}{
			"zone_name":  fmt.Sprintf("%v#%v", taskInfo.ZoneName, taskID),
			"status":     taskInfo.Status,
			"updated_at": time.Now(),
		}).Error
	return err
}

func (rw *ReBalanceWorker) volMigTaskStatus(taskID uint64) (status *VolMigrateTaskStatus, err error) {
	// 还在内存中，没有重启
	if v, ok := rw.volMigrateMap.Load(taskID); ok {
		mt := v.(*MigVolTask)
		status = mt.getRunningTaskStatus()
		return
	}
	// 历史任务
	info, err := rw.GetRebalancedInfoByID(taskID)
	if err != nil {
		return
	}
	status = rw.getHistoryVolMigTaskStatus(info)
	return
}

func (rw *ReBalanceWorker) updateRunningVolStatus(taskId uint64) error {
	records, err := rw.getVolRecordByStatus(taskId, StatusRunning)
	if err != nil {
		return err
	}
	if len(records) <= 0 {
		return nil
	}

	var ids []uint64
	for _, record := range records {
		ids = append(ids, record.ID)
	}
	return rw.updateVolRecordsStatus(ids, StatusTerminating)
}
