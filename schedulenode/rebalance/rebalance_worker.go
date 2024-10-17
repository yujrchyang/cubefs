package rebalance

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"github.com/cubefs/cubefs/sdk/http_client"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"gorm.io/gorm/utils"
	"path"
	"strconv"
	"strings"
	"time"
)

func (rw *ReBalanceWorker) startBackgroundTask() {
	timer := time.NewTimer(defaultDeletedTaskInterval)
	defer func() {
		timer.Stop()
	}()
	for {
		select {
		case <-timer.C:
			records, _ := rw.GetNeedDeleteReplicaDp(time.Now().Add(-defaultDeletedTaskInterval))
			log.LogDebugf("background: len(needDeleteRecord)=%v", len(records))
			for _, record := range records {
				//todo: 并发
				rw.processNeedDeleteReplicaDp(record)
			}
			rw.UpdateNeedDeleteReplicaDpRecords(records)
			timer.Reset(defaultDeletedTaskInterval)

		case <-rw.StopC:
			log.LogInfo("stop startBackgroudTask")
			return
		}
	}
}

func (rw *ReBalanceWorker) processNeedDeleteReplicaDp(record *MigrateRecordTable) {
	// 删除副本
	var (
		mc *master.MasterClient
		rc *releaseClient
	)
	if isRelease(record.ClusterName) {
		rc = newReleaseClient([]string{rw.getClusterHost(record.ClusterName)}, record.ClusterName)
	} else {
		mc = master.NewMasterClient([]string{rw.getClusterHost(record.ClusterName)}, false)
	}

	dp, err := getDataPartitionByClient(mc, rc, record.PartitionID, record.VolName)
	if err != nil {
		log.LogErrorf("processNeedDeleteReplicaDp: get dp failed, dpID(%v) err(%v)", record.PartitionID, err)
		if err == ErrDataPartitionNotFound {
			record.FinishDelete = FinishDeleteSuccess
		}
		return
	}
	if dp.IsRecover {
		return
	}

	if len(dp.Replicas)-int(dp.ReplicaNum) < 1 {
		errMsg := fmt.Sprintf("processNeedDeleteReplicaDp: wrong dp info, cluster(%v) dpID(%v) hosts(%v) src(%v) dst(%v) replicaNum(%v)",
			record.ClusterName, record.PartitionID, dp.Hosts, record.SrcAddr, record.DstAddr, dp.ReplicaNum)
		exporter.WarningBySpecialUMPKey(deleteWarnKey, errMsg)
		log.LogError(errMsg)
		return
	}

	if utils.Contains(dp.Hosts, record.SrcAddr) && utils.Contains(dp.Hosts, record.DstAddr) && dp.ReplicaNum == 2 {
		err = deleteDataPartitionReplicaByClient(mc, rc, dp.PartitionID, record.SrcAddr)
		if err != nil {
			log.LogErrorf("processNeedDeleteReplicaDp: delete replica failed: dp(%v) addr(%v) err(%v)", dp.PartitionID, record.SrcAddr, err)
			record.FinishDelete = FinishDeleteFailed // 只有删副本失败才是-1
			return
		}
	}

	// 更新record字段
	record.FinishDelete = FinishDeleteSuccess
}

func (rw *ReBalanceWorker) loadInRunningRebalanced() (err error) {
	var rInfos []*RebalancedInfoTable
	if rInfos, err = rw.GetRebalancedInfoByStatus(StatusRunning); err != nil {
		return
	}
	for _, info := range rInfos {
		if info.DstMetaNodePartitionMaxCount == 0 || info.DstMetaNodePartitionMaxCount > defaultDstMetaNodePartitionMaxCount {
			info.DstMetaNodePartitionMaxCount = defaultDstMetaNodePartitionMaxCount
		}
		var reTaskID uint64
		reTaskID, e1 := rw.restartRunningTask(info)
		if e1 != nil || reTaskID != info.ID {
			log.LogErrorf("restart taskID(%v) failed: err(%v) re_taskID(%v)", info.ID, e1, reTaskID)
			continue
		}
		log.LogInfof("restart taskID(%v) success", info.ID)
	}
	return
}

func (rw *ReBalanceWorker) restartRunningTask(info *RebalancedInfoTable) (taskID uint64, err error) {
	isRestart := true

	var ctrl *ZoneReBalanceController
	if info.TaskType == ZoneAutoReBalance {
		ctrl, err = rw.newZoneRebalanceCtrl(info.Cluster, info.ZoneName, info.RType, info.MaxBatchCount, info.HighRatio, info.LowRatio, info.GoalRatio,
			info.MigrateLimitPerDisk, info.DstMetaNodePartitionMaxCount, isRestart)
		if err != nil {
			rw.stopRebalanced(ctrl.Id, false)
			return
		}
	}
	if info.TaskType == NodesMigrate {
		ctrl, err = rw.newNodeMigrationCtrl(info.Cluster, info.RType, info.MaxBatchCount, info.DstMetaNodePartitionMaxCount,
			strings.Split(info.SrcNodes, ","), strings.Split(info.DstNodes, ","), isRestart, info.ID)
		if err != nil {
			rw.stopRebalanced(ctrl.Id, false)
			return
		}
	}
	taskID = ctrl.Id
	err = ctrl.ReBalanceStart()
	return
}

func (rw *ReBalanceWorker) NodesMigrateStart(cluster string, rType RebalanceType, maxBatchCount int, dstMetaNodePartitionMaxCount int,
	srcNodes, dstNodes []string) (taskID uint64, err error) {

	isRestart := false
	ctrl, err := rw.newNodeMigrationCtrl(cluster, rType, maxBatchCount, dstMetaNodePartitionMaxCount, srcNodes, dstNodes, isRestart, 0)
	if err != nil {
		return
	}
	taskID = ctrl.Id

	err = ctrl.ReBalanceStart()
	return
}

func (rw *ReBalanceWorker) ZoneReBalanceStart(cluster, zoneName string, rType RebalanceType, highRatio, lowRatio, goalRatio float64,
	maxBatchCount int, migrateLimitPerDisk, dstMetaNodePartitionMaxCount int) (taskID uint64, err error) {
	isRestart := false

	ctrl, err := rw.newZoneRebalanceCtrl(cluster, zoneName, rType, maxBatchCount, highRatio, lowRatio, goalRatio, migrateLimitPerDisk,
		dstMetaNodePartitionMaxCount, isRestart)
	if err != nil {
		return
	}
	taskID = ctrl.Id
	err = ctrl.ReBalanceStart()
	return
}

// 节点的control，先校验再插表
func (rw *ReBalanceWorker) newNodeMigrationCtrl(cluster string, rType RebalanceType, maxBatchCount int, dstMetaNodeMaxPartitionCount int,
	srcNodeList, dstNodeList []string, isRestart bool, taskID uint64) (ctrl *ZoneReBalanceController, err error) {
	srcNodes := strings.Join(srcNodeList, ",")
	dstNodes := strings.Join(dstNodeList, ",")

	ctrl = newNodeMigrateController(taskID, cluster, rType, srcNodeList, dstNodeList, rw)

	var rInfo *RebalancedInfoTable
	if taskID > 0 {
		rInfo, err = rw.GetRebalancedInfoByID(taskID)
		if err != nil {
			return
		}
		rInfo, err = rw.updateRestartNodesRebalanceInfo(rInfo)
		if err != nil {
			log.LogWarnf("重启node迁移任务失败：%v", err)
			return
		}
	}
	if !isRestart {
		rInfo, err = rw.createNodesRebalanceInfo(cluster, rType, maxBatchCount, dstMetaNodeMaxPartitionCount, srcNodes, dstNodes, StatusRunning)
		if err != nil {
			log.LogWarnf("node迁移任务创建失败：%v", err)
			return
		}
	}
	if maxBatchCount > 0 {
		ctrl.SetClusterMaxBatchCount(maxBatchCount)
	}
	if dstMetaNodeMaxPartitionCount > 0 {
		ctrl.SetDstMetaNodeMaxPartitionCount(dstMetaNodeMaxPartitionCount)
	}
	ctrl.SetCtrlTaskID(rInfo.ID)
	ctrl.SetCreatedUpdatedAt(rInfo.CreatedAt, rInfo.UpdatedAt)
	rw.reBalanceCtrlMap.Store(getRebalanceCtrlMapKey(cluster, rInfo.RType, rInfo.ID), ctrl)
	return
}

func (rw *ReBalanceWorker) newZoneRebalanceCtrl(cluster, zoneName string, rType RebalanceType, maxBatchCount int,
	highRatio, lowRatio, goalRatio float64, migrateLimitPerDisk, dstMetaNodeMaxPartitionCount int, isRestart bool) (ctrl *ZoneReBalanceController, err error) {

	var rInfo *RebalancedInfoTable
	rInfo, err = rw.GetRebalancedInfoByZone(cluster, zoneName, rType)
	if err != nil && err.Error() != RECORD_NOT_FOUND {
		return
	}
	if rInfo.ID > 0 && !isRestart {
		return nil, fmt.Errorf("任务已存在：%v_%v_%v", cluster, zoneName, rInfo.ID)
	}

	ctrl = newZoneReBalanceController(rInfo.ID, cluster, zoneName, rType, highRatio, lowRatio, goalRatio, rw)
	if maxBatchCount > 0 {
		ctrl.SetClusterMaxBatchCount(maxBatchCount)
	}
	if migrateLimitPerDisk > 0 {
		ctrl.SetMigrateLimitPerDisk(migrateLimitPerDisk)
	}
	if dstMetaNodeMaxPartitionCount > 0 {
		ctrl.SetDstMetaNodeMaxPartitionCount(dstMetaNodeMaxPartitionCount)
	}

	// RECORD_NOT_FOUND or restart
	if rInfo.ID > 0 && isRestart {
		if rInfo, err = rw.updateRebalancedInfo(cluster, zoneName, rType, maxBatchCount,
			highRatio, lowRatio, goalRatio, migrateLimitPerDisk, dstMetaNodeMaxPartitionCount, StatusRunning); err != nil {
			return
		}
	} else {
		if rInfo, err = rw.insertRebalanceInfo(cluster, zoneName, rType, maxBatchCount,
			highRatio, lowRatio, goalRatio, migrateLimitPerDisk, dstMetaNodeMaxPartitionCount, StatusRunning); err != nil {
			return
		}
	}

	ctrl.SetCtrlTaskID(rInfo.ID)
	ctrl.SetCreatedUpdatedAt(rInfo.CreatedAt, rInfo.UpdatedAt)
	rw.reBalanceCtrlMap.Store(getRebalanceCtrlMapKey(cluster, rType, rInfo.ID), ctrl)
	return
}

func (rw *ReBalanceWorker) ReSetControlParam(taskID uint64, goalRatio float64, maxBatchCount, migrateLimitPerDisk, dstMNPartitionMaxCount int) (err error) {
	var (
		rInfo *RebalancedInfoTable
	)
	if rInfo, err = rw.GetRebalancedInfoByID(taskID); err != nil {
		return
	}
	if rInfo.TaskType == NodesMigrate {
		return rw.resetNodeMigrateCtrl(rInfo, maxBatchCount, migrateLimitPerDisk, dstMNPartitionMaxCount)
	}
	if rInfo.TaskType == ZoneAutoReBalance {
		return rw.resetZoneAutoRebalanceCtrl(rInfo, goalRatio, maxBatchCount, migrateLimitPerDisk, dstMNPartitionMaxCount)
	}
	return
}

func (rw *ReBalanceWorker) resetNodeMigrateCtrl(rInfo *RebalancedInfoTable, maxBatchCount, migrateLimitPerDisk, dstMNPartitionMaxCount int) (err error) {
	var ctrl *ZoneReBalanceController
	// 1.找control
	// 2.修改表记录
	// 3.修改control
	if ctrl, err = rw.getRebalanceCtrl(getRebalanceCtrlMapKey(rInfo.Cluster, rInfo.RType, rInfo.ID)); err != nil {
		return
	}
	if maxBatchCount <= 0 {
		maxBatchCount = rInfo.MaxBatchCount
	}
	if migrateLimitPerDisk <= 0 {
		migrateLimitPerDisk = rInfo.MigrateLimitPerDisk
	}
	if dstMNPartitionMaxCount <= 0 || dstMNPartitionMaxCount > defaultDstMetaNodePartitionMaxCount {
		dstMNPartitionMaxCount = defaultDstMetaNodePartitionMaxCount
	}
	err = rw.updateNodesRebalanceInfo(rInfo.ID, maxBatchCount, migrateLimitPerDisk, dstMNPartitionMaxCount)
	if err == nil {
		ctrl.SetDstMetaNodeMaxPartitionCount(dstMNPartitionMaxCount)
		ctrl.SetClusterMaxBatchCount(maxBatchCount)
	}
	return
}

func (rw *ReBalanceWorker) resetZoneAutoRebalanceCtrl(rInfo *RebalancedInfoTable, goalRatio float64, maxBatchCount, migrateLimitPerDisk, dstMNPartitionMaxCount int) (err error) {
	var ctrl *ZoneReBalanceController
	if ctrl, err = rw.getRebalanceCtrl(getRebalanceCtrlMapKey(rInfo.Cluster, rInfo.RType, rInfo.ID)); err != nil {
		return
	}
	if goalRatio <= 0 {
		goalRatio = rInfo.GoalRatio
	}
	if err = checkRatio(rInfo.HighRatio, rInfo.LowRatio, goalRatio); err != nil {
		return err
	}
	if maxBatchCount <= 0 {
		maxBatchCount = rInfo.MaxBatchCount
	}
	if migrateLimitPerDisk <= 0 {
		migrateLimitPerDisk = rInfo.MigrateLimitPerDisk
	}
	if dstMNPartitionMaxCount <= 0 || dstMNPartitionMaxCount > defaultDstMetaNodePartitionMaxCount {
		dstMNPartitionMaxCount = defaultDstMetaNodePartitionMaxCount
	}
	_, err = rw.updateRebalancedInfo(rInfo.Cluster, rInfo.ZoneName, rInfo.RType, maxBatchCount,
		rInfo.HighRatio, rInfo.LowRatio, goalRatio, migrateLimitPerDisk, dstMNPartitionMaxCount, Status(rInfo.Status))
	if err == nil {
		ctrl.UpdateRatio(rInfo.HighRatio, rInfo.LowRatio, goalRatio)
		ctrl.SetDstMetaNodeMaxPartitionCount(dstMNPartitionMaxCount)
		ctrl.SetClusterMaxBatchCount(maxBatchCount)
		ctrl.SetMigrateLimitPerDisk(migrateLimitPerDisk)
	}
	return err
}

func (rw *ReBalanceWorker) ReBalanceStop(taskId uint64) (err error) {
	var (
		rInfo *RebalancedInfoTable
		ctrl  *ZoneReBalanceController
	)
	if rInfo, err = rw.GetRebalancedInfoByID(taskId); err != nil {
		return
	}
	if ctrl, err = rw.getRebalanceCtrl(getRebalanceCtrlMapKey(rInfo.Cluster, rInfo.RType, rInfo.ID)); err != nil {
		return
	}
	if err = rw.stopRebalanced(taskId, true); err != nil {
		return
	}
	if err = ctrl.ReBalanceStop(); err != nil {
		return
	}
	rw.reBalanceCtrlMap.Delete(getRebalanceCtrlMapKey(rInfo.Cluster, rInfo.RType, rInfo.ID))
	return nil
}

func (rw *ReBalanceWorker) ReBalanceStatus(cluster string, rType RebalanceType, taskID uint64) (Status, error) {
	ctrl, err := rw.getRebalanceCtrl(getRebalanceCtrlMapKey(cluster, rType, taskID))
	if err != nil {
		return -1, err
	}
	status := ctrl.Status()
	return status, nil
}

func (rw *ReBalanceWorker) ResetZoneMap() {
	rw.reBalanceCtrlMap.Range(func(key, value interface{}) bool {
		ctrl := value.(*ZoneReBalanceController)
		if ctrl.Status() == StatusStop {
			rw.reBalanceCtrlMap.Delete(key)
		}
		return true
	})
}

// 1. 节点列表 2.每个节点的dp列表
// 节点 和 count
func (rw *ReBalanceWorker) getStoppedTaskStatus(rInfo *RebalancedInfoTable, status Status) (*RebalanceStatusInfo, error) {
	result := &RebalanceStatusInfo{
		Status: status,
	}
	srcNodeList, err := rw.GetSrcNodeInfoList(rInfo.ID)
	if err != nil {
		return nil, err
	}
	for _, srcNode := range srcNodeList {
		srcNode.IsFinish = true
	}
	result.SrcNodesInfo = srcNodeList

	dstNodeList, err := rw.GetDstNodeInfoList(rInfo.ID)
	if err != nil {
		return nil, err
	}
	for _, dstNode := range dstNodeList {
		dstNode.IsFinish = true
	}
	result.DstNodesInfo = dstNodeList
	return result, nil
}

func (rw *ReBalanceWorker) getRunningTaskStatus(rInfo *RebalancedInfoTable) (*RebalanceStatusInfo, error) {
	ctrl, err := rw.getRebalanceCtrl(getRebalanceCtrlMapKey(rInfo.Cluster, rInfo.RType, rInfo.ID))
	if err != nil {
		return nil, err
	}
	result := &RebalanceStatusInfo{
		SrcNodesInfo: make([]*RebalanceNodeInfo, 0),
		DstNodesInfo: make([]*RebalanceNodeInfo, 0),
		Status:       ctrl.Status(),
	}
	if rInfo.RType == RebalanceData {
		for _, srcNode := range ctrl.srcDataNodes {
			info := &RebalanceNodeInfo{
				Addr:     srcNode.nodeInfo.Addr,
				IsFinish: srcNode.isFinished,
			}
			if rInfo.TaskType == ZoneAutoReBalance {
				info.TotalCount = len(srcNode.disks) * srcNode.migrateLimitPerDisk
				info.MigratedCount = len(srcNode.migratedDp)
			}
			if rInfo.TaskType == NodesMigrate {
				total := 0
				for _, disk := range srcNode.disks {
					total += len(disk.dpList)
				}
				info.TotalCount = total
				info.MigratedCount = len(srcNode.migratedDp)
			}
			result.SrcNodesInfo = append(result.SrcNodesInfo, info)
		}
		for _, dstNode := range ctrl.dstDataNodes {
			info := &RebalanceNodeInfo{
				Addr: dstNode.Addr,
			}
			result.DstNodesInfo = append(result.DstNodesInfo, info)
		}
	}
	if rInfo.RType == RebalanceMeta {
		for _, srcNode := range ctrl.srcMetaNodes {
			info := &RebalanceNodeInfo{
				Addr:     srcNode.nodeInfo.Addr,
				IsFinish: srcNode.isFinished,
			}
			if rInfo.TaskType == ZoneAutoReBalance {
				info.MigratedCount = len(srcNode.alreadyMigrateFinishedPartitions)
			}
			if rInfo.TaskType == NodesMigrate {
				info.TotalCount = len(srcNode.nodeInfo.PersistenceMetaPartitions)
				info.MigratedCount = len(srcNode.alreadyMigrateFinishedPartitions)
			}
			result.SrcNodesInfo = append(result.SrcNodesInfo, info)
		}
		for _, dstNode := range ctrl.dstMetaNodes {
			info := &RebalanceNodeInfo{
				Addr: dstNode.Addr,
			}
			result.DstNodesInfo = append(result.DstNodesInfo, info)
		}
	}
	return result, nil
}

func (rw *ReBalanceWorker) getRebalanceCtrl(findKey string) (*ZoneReBalanceController, error) {
	if res, ok := rw.reBalanceCtrlMap.Load(findKey); !ok {
		return nil, fmt.Errorf("get rebalance controller error with findKey:%v", findKey)
	} else {
		ctrl := res.(*ZoneReBalanceController)
		return ctrl, nil
	}
}

func getRebalanceCtrlMapKey(cluster string, module RebalanceType, taskID uint64) string {
	return path.Join(cluster, module.String(), strconv.FormatUint(taskID, 10))
}

func (rw *ReBalanceWorker) doReleaseZone(cluster, zoneName string) error {
	dataNodes, err := getZoneDataNodesByClusterName(cluster, zoneName)
	if err != nil {
		return err
	}
	for _, node := range dataNodes {
		if err = rw.doReleaseDataNodePartitions(node, ""); err != nil {
			log.LogErrorf("release dataNode error cluster: %v zone: %v dataNode %v %v", cluster, zoneName, node, err)
		}
	}
	return nil
}

func (rw *ReBalanceWorker) doReleaseDataNodePartitions(dataNodeHttpAddr, timeLocation string) (err error) {
	var (
		data   []byte
		reqURL string
		key    string
	)
	if timeLocation == "" {
		key = generateAuthKey()
	} else {
		key = generateAuthKeyWithTimeZone(timeLocation)
	}
	dataHttpClient := http_client.NewDataClient(dataNodeHttpAddr, false)
	_, err = dataHttpClient.ReleasePartitions(key)
	if err != nil {
		return fmt.Errorf("url[%v],err %v resp[%v]", reqURL, err, string(data))
	}
	log.LogInfof("action[doReleaseDataNodePartitions] url[%v] resp[%v]", reqURL, string(data))
	return
}

func generateAuthKey() string {
	date := time.Now().Format("2006-01-02 15")
	h := md5.New()
	h.Write([]byte(date))
	cipherStr := h.Sum(nil)
	return hex.EncodeToString(cipherStr)
}

func generateAuthKeyWithTimeZone(timeLocation string) string {
	var t time.Time
	if timeLocation == "" {
		t = time.Now()
	} else {
		l, _ := time.LoadLocation(timeLocation)
		t = time.Now().In(l)
	}
	date := t.Format("2006-01-02 15")
	h := md5.New()
	h.Write([]byte(date))
	cipherStr := h.Sum(nil)
	return hex.EncodeToString(cipherStr)
}
