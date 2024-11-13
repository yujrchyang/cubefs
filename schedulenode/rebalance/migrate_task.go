package rebalance

import (
	"context"
	"errors"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util/log"
	"gorm.io/gorm/utils"
	"math"
	"sort"
	"strings"
	"sync"
	"time"
)

type MigVolTask struct {
	*master.MasterClient // 不支持release集群
	cluster              string
	module               string
	srcZone              string
	dstZone              string
	dstDataNodes         []string
	dstMetaNodes         []string
	dstIndex             int
	vols                 []string
	finishVols           map[string]bool // true-真完成 false-没有资源被迫停止的
	taskId               uint64
	// 控制参数
	clusterCurrency        int
	volCurrency            int
	volDpCurrency          int
	volMpCurrency          int
	roundInterval          int
	maxDstDatanodeUsage    float64
	maxDstMetanodeUsage    float64
	maxDstMetaPartitionCnt int
	// 停止任务控制
	status Status
	ctx    context.Context
	cancel context.CancelFunc
	// 数据库句柄
	worker *ReBalanceWorker
	// index并发锁
	dstIndexLock sync.Mutex
}

func (mt *MigVolTask) String() string {
	return fmt.Sprintf("{cluster: %v, taskID: %v}", mt.cluster, mt.taskId)
}

func (mt *MigVolTask) SetTaskId(taskId uint64) {
	mt.taskId = taskId
}

func (mt *MigVolTask) StopVolTask() error {
	switch mt.status {
	case StatusRunning:
		mt.status = StatusTerminating
		mt.cancel()
	default:
		return fmt.Errorf("status error while stop volMig in %v status code: %v", mt.taskId, mt.status)
	}
	return nil
}

func (mt *MigVolTask) VolMigrateStart() {
	mt.ctx, mt.cancel = context.WithCancel(context.Background())

	switch mt.module {
	case "data":
		go mt.startVolDpMigrateTask(mt.vols)
	case "meta":
		go mt.startVolMpMigrateTask(mt.vols)
	}
}

func (mt *MigVolTask) startVolDpMigrateTask(vols []string) {
	defer func() {
		if mt.status == StatusRunning {
			mt.status = StatusStop
		}
		mt.worker.updateMigrateTaskStatus(mt.taskId, int(mt.status))
		mt.cancel()
	}()

	for _, vol := range mt.vols {
		volInfo, err := mt.AdminAPI().GetVolumeSimpleInfo(vol)
		if err != nil {
			if strings.Contains(err.Error(), proto.ErrVolNotExists.Error()) {
				mt.finishVols[vol] = true
				err = nil
				continue
			}
		}
		if !strings.Contains(volInfo.ZoneName, mt.srcZone) && !strings.Contains(volInfo.ZoneName, mt.dstZone) {
			log.LogWarnf("the zone(%v) of vol(%v) is neither srcZone(%v) nor dstZone(%v)", volInfo.ZoneName, vol, mt.srcZone, mt.dstZone)
			mt.finishVols[vol] = true
		}
	}
	mt.updateDstDataNode()
	if len(mt.dstDataNodes) <= 0 {
		log.LogErrorf("can't find dstDataNodes by usageRatio")
		return
	}

	for {
		if len(mt.finishVols) == len(vols) {
			log.LogInfof("finish all vols migrate: taskID(%v)", mt.taskId)
			return
		}
		inRecoveringDPMap, err := IsInRecoveringMoreThanMaxBatchCount(mt.MasterClient, nil, RebalanceData, mt.clusterCurrency)
		if err != nil {
			log.LogWarnf("get IsInRecoveringDPsMoreThanMaxBatchCount: err(%v)\n", err)
			time.Sleep(defaultWaitClusterRecover)
			continue
		}
		availableClusterCurrency := mt.clusterCurrency - len(inRecoveringDPMap)
		if availableClusterCurrency <= 0 {
			log.LogWarnf("clusterCurrency reach the limit: taskID(%v) clusterCurrency(%v) recoveringNum(%v)", mt.taskId, mt.clusterCurrency, len(inRecoveringDPMap))
			time.Sleep(defaultWaitClusterRecover)
			continue
		}
		currMigVolCnt := 0
		for _, vol := range vols {
			select {
			case <-mt.ctx.Done():
				mt.status = StatusTerminating
				log.LogInfof("startVolMigrateTask: receive stopped signal, taskID(%v)", mt.taskId)
				return
			default:
			}

			if mt.finishVols[vol] {
				continue
			}
			if currMigVolCnt >= mt.volCurrency {
				log.LogInfof("wait for next round: taskID(%v) migrate count(%v) reach volBatchCount(%v)", mt.taskId, currMigVolCnt, mt.volCurrency)
				break
			}
			err = mt.doChangeVolDpToTargetZone(vol, mt.dstZone, &availableClusterCurrency)
			if !mt.finishVols[vol] && err == nil {
				currMigVolCnt++
			}
			if availableClusterCurrency <= 0 {
				log.LogInfof("wait for next round: taskID(%v) actualMaxClusterCount(%v) reach the limit\n", mt.taskId, availableClusterCurrency)
				break
			}
			time.Sleep(defaultMigNodeInterval)
		}
		rest := len(vols) - len(mt.finishVols)
		if rest > 0 {
			time.Sleep(time.Duration(mt.roundInterval) * time.Second)
		}
		log.LogInfof("taskID[%v] total[%v] finish[%v] remains[%v]\n", mt.taskId, len(vols), len(mt.finishVols), rest)
	}
}

// 对每个vol进行调整
// 按照DP ID从小到大排序， 如果有不是新的zone的 就执行迁移 指定目标zone为新的 -- 这样能保证每次都先把前面的一个调整完成
func (mt *MigVolTask) doChangeVolDpToTargetZone(volName, targetZone string, availableMaxClusterCount *int) (err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("doChangeVolDpToTargetZone: taskID(%v) vol(%v) targetZone(%v) err(%v)", mt.taskId, volName, targetZone, err)
		}
	}()
	inRecoveringDPMap, err := IsInRecoveringMoreThanMaxBatchCount(mt.MasterClient, nil, RebalanceData, mt.clusterCurrency)
	if err != nil {
		log.LogErrorf("get IsInRecoveringDPsMoreThanMaxBatchCount: err(%v)\n", err)
		return
	}
	curAvailableMaxClusterCount := mt.clusterCurrency - len(inRecoveringDPMap)
	*availableMaxClusterCount = int(math.Min(float64(*availableMaxClusterCount), float64(curAvailableMaxClusterCount)))
	if *availableMaxClusterCount <= 0 {
		log.LogWarnf("wait because actualClusterCurrency <=0: %v taskID(%v)", *availableMaxClusterCount, mt.taskId)
		return ErrReachClusterCurrency
	}
	//获取所有DP
	dataPartitions, err := mt.ClientAPI().GetDataPartitions(volName, nil)
	if err != nil {
		return
	}
	isRecoverCnt := 0
	for _, partition := range dataPartitions.DataPartitions {
		if partition.IsRecover {
			isRecoverCnt++
		}
	}
	// DP ID 从小到大排序，优先小的
	sort.Slice(dataPartitions.DataPartitions, func(i, j int) bool {
		return dataPartitions.DataPartitions[i].PartitionID < dataPartitions.DataPartitions[j].PartitionID
	})
	// 选择dp
	toBeChangedDPs := mt.filterVolDataPartitions(volName, dataPartitions.DataPartitions, inRecoveringDPMap)
	if len(toBeChangedDPs) == 0 {
		trulyFinish := checkVolFinishChangeDp(volName, mt.srcZone, mt.dstZone, mt.MasterClient)
		if trulyFinish {
			mt.finishVols[volName] = true
			mt.worker.insertOrUpdateVolMigrateTask(mt.taskId, mt.cluster, volName, mt.srcZone, mt.dstZone, mt.module, len(dataPartitions.DataPartitions), StatusStop)
		}
		log.LogWarnf("no available migrateDp: taskID(%v) vol(%v)", mt.taskId, volName)
		return
	}
	actualVolDpCount := mt.volDpCurrency - isRecoverCnt
	if actualVolDpCount <= 0 {
		log.LogWarnf("vol recoverDpCount reach dpCurrency: taskID(%v) vol(%v) recoverCnt(%v) dpCurrency(%v)", mt.taskId, volName, isRecoverCnt, mt.volDpCurrency)
		return ErrReachVolCurrency
	}

	return mt.parallelChangeDp(volName, len(dataPartitions.DataPartitions), actualVolDpCount, toBeChangedDPs, availableMaxClusterCount)
}

func (mt *MigVolTask) filterVolDataPartitions(volName string, partitions []*proto.DataPartitionResponse, clusterRecoverDpMap map[uint64]int) (result []*proto.DataPartitionInfo) {
	result = make([]*proto.DataPartitionInfo, 0)
	for _, partition := range partitions {
		if clusterRecoverDpMap[partition.PartitionID] != 0 {
			continue
		}
		dp, err := mt.AdminAPI().GetDataPartition(volName, partition.PartitionID)
		if err != nil {
			log.LogWarnf("get dataPartition failed: vol(%v) dp(%v) err(%v)", volName, partition.PartitionID, err)
			continue
		}
		// 副本数
		if len(dp.Hosts) < 2 {
			log.LogWarnf("vol(%v) dp(%v) len(host)<2", volName, dp.PartitionID)
			continue
		}
		// 检测副本的zone信息, 决定是否要继续迁移
		replicaOperation := GetNextReplicaOperation(dp, mt.srcZone, mt.dstZone, int(partition.ReplicaNum))
		if replicaOperation == None || replicaOperation == Finished {
			continue
		}
		// 检测dp recover状态
		if dp.IsRecover {
			log.LogWarnf("dp in recover: vol(%v) dp(%v)", volName, dp.PartitionID)
			continue
		}
		// 检测副本raft状态
		for _, host := range dp.Hosts {
			var stopped bool
			dataClient := getDataHttpClient(host, getDefaultDataNodePProfPort(mt.cluster))
			stopped, err = checkRaftStatus(dataClient, dp.PartitionID, host)
			if err != nil || stopped {
				log.LogWarnf("checkRaftStatus: dp(%v) host(%v) err(%v) stopped(%v)", dp.PartitionID, host, err, stopped)
				err = nil
				continue
			}
		}
		// 添加到待迁移dp
		result = append(result, dp)
	}
	return
}

func (mt *MigVolTask) parallelChangeDp(volName string, volDpNum, dpCurrency int, migDps []*proto.DataPartitionInfo, clusterCurrency *int) (err error) {
	id, err := mt.worker.insertOrUpdateVolMigrateTask(mt.taskId, mt.cluster, volName, mt.srcZone, mt.dstZone, mt.module, volDpNum, StatusRunning)
	if err != nil {
		log.LogErrorf("parallelChangeDp: insert vol record failed: taskID(%v) vol(%v) err(%v)", mt.taskId, volName, err)
		return err
	}
	var stackErr error
	for _, migDp := range migDps {
		select {
		case <-mt.ctx.Done():
			mt.status = StatusTerminating
			mt.worker.updateVolRecordStatus(id, StatusTerminating)
			return
		default:
		}
		if dpCurrency <= 0 || *clusterCurrency <= 0 {
			log.LogWarnf("parallelChangeDp: reach currency limit, dpCurrency(%v) clusterCurrency(%v)", dpCurrency, *clusterCurrency)
			return
		}

		targetDataNode, getErr := mt.getTargetDataNodes(migDp.Hosts, mt.maxDstDatanodeUsage)
		if getErr != nil {
			continue
		}

		err = mt.doChangeDp(migDp, targetDataNode, mt.dstZone)
		if err != nil {
			stackErr = errors.Join(stackErr, err)
			continue
		}
		dpCurrency--
		*clusterCurrency--
	}
	//todo: 所有分片都选不出dst节点
	if stackErr == nil {
		mt.finishVols[volName] = true
		mt.worker.updateVolRecordStatus(id, StatusStop)
	}
	return
}

func (mt *MigVolTask) doChangeDp(migDp *proto.DataPartitionInfo, targetDataNode, dstZone string) (err error) {
	for i := len(migDp.Zones) - 1; i >= 0; i-- {
		if migDp.Zones[i] != dstZone {
			migAddr := migDp.Hosts[i]
			e1 := mt.migrateDpToDest(migDp, migAddr, targetDataNode, mt.srcZone, mt.dstZone)
			if e1 != nil {
				err = errors.Join(err, e1)
				log.LogErrorf("migrateDpToDest failed: dp(%v) src(%v) dst(%v) err(%v)", migDp.PartitionID, migAddr, targetDataNode, e1)
			} else {
				record := &MigrateRecordTable{
					ClusterName: mt.cluster,
					ZoneName:    dstZone,
					RType:       RebalanceData,
					VolName:     migDp.VolName,
					PartitionID: migDp.PartitionID,
					SrcAddr:     migAddr,
					DstAddr:     targetDataNode,
					TaskId:      mt.taskId,
					TaskType:    VolsMigrate,
					CreatedAt:   time.Now(),
					UpdateAt:    time.Now(),
				}
				mt.worker.insertPartitionMigRecords(record)
			}
		}
	}
	log.LogDebugf("doChangeDp: taskID(%v) dp(%v) dstZone(%v) err(%v)", mt.taskId, migDp.PartitionID, dstZone, err)
	return err
}

func (mt *MigVolTask) migrateDpToDest(dp *proto.DataPartitionInfo, srcAddr, dstAddr, srcZone, dstZone string) error {
	if dp.ReplicaNum == 2 {
		replicaOperation := GetNextReplicaOperation(dp, srcZone, dstZone, 2)
		if replicaOperation == None || replicaOperation == Finished {
			log.LogWarnf("unexpected replica operation: %s, dp(%v)", replicaOperation.String(), dp)
			return nil
		}
		if replicaOperation == DelReplica {
			// 选src下掉
			return mt.AdminAPI().DeleteDataReplica(dp.PartitionID, srcAddr)
		}
		if replicaOperation == AddReplica {
			// 加dst
			return mt.AdminAPI().AddDataReplica(dp.PartitionID, dstAddr, proto.DefaultAddReplicaType)
		}
	} else {
		return mt.AdminAPI().DecommissionDataPartition(dp.PartitionID, srcAddr, dstAddr)
	}
	return fmt.Errorf("unexpected situation")
}

// todo: 每次选节点都是现获取的，需要用background任务吗
func (mt *MigVolTask) getTargetDataNodes(hosts []string, usageLimit float64) (nodeAddr string, err error) {
	// dst节点轮换着来
	// dstIndex有并发
	for i := 0; i < len(mt.dstDataNodes); i++ {
		mt.dstIndexLock.Lock()
		if mt.dstIndex >= len(mt.dstDataNodes) {
			mt.dstIndex = 0
		}
		nodeAddr = mt.dstDataNodes[mt.dstIndex]
		mt.dstIndex++
		mt.dstIndexLock.Unlock()

		if !stringSliceContain(hosts, nodeAddr) {
			dataClient := getDataHttpClient(nodeAddr, getDefaultDataNodePProfPort(mt.cluster))
			dataStats, err := dataClient.GetDatanodeStats()
			if err != nil {
				return "", err
			}
			//if dataNode.IsActive == false {
			//	continue
			//}
			if convertActualUsageRatio(dataStats) >= usageLimit {
				continue
			}
			break
		}
	}
	if nodeAddr == "" || utils.Contains(hosts, nodeAddr) {
		return "", ErrNoSuitableDstNode
	}
	return
}

func (mt *MigVolTask) updateDstDataNode() {
	zoneDataNodes, err := getZoneDataNodesByClient(mt.MasterClient, nil, mt.dstZone)
	if err != nil {
		log.LogErrorf("updateDstDataNode: taskID(%v) error(%v)", mt.taskId, err)
		return
	}
	filterDstDataNodes := mt.filterDataNodesByUsageRatio(zoneDataNodes, mt.maxDstDatanodeUsage)
	mt.dstDataNodes = filterDstDataNodes
}

func (mt *MigVolTask) filterDataNodesByUsageRatio(zoneNodes []string, maxUsageRatio float64) (nodeList []string) {
	for _, node := range zoneNodes {
		dataClient := getDataHttpClient(node, getDefaultDataNodePProfPort(mt.cluster))
		dataStats, err := dataClient.GetDatanodeStats()
		if err != nil {
			continue
		}
		if convertActualUsageRatio(dataStats) < maxUsageRatio {
			nodeList = append(nodeList, node)
		}
	}
	return
}

func checkVolFinishChangeDp(volName, srcZone, dstZone string, masterClient *master.MasterClient) (finish bool) {
	defer func() {
		if finish {
			log.LogInfof("checkVolFinishChangeDp: vol(%v)", volName)
		}
	}()
	dataPartitions, err := masterClient.ClientAPI().GetDataPartitions(volName, nil)
	if err != nil {
		log.LogWarnf("get dataPartition failed: vol(%v) err(%v)", volName, err)
		return
	}
	count := 0
	for _, partition := range dataPartitions.DataPartitions {
		dp, err1 := masterClient.AdminAPI().GetDataPartition(volName, partition.PartitionID)
		if err1 != nil {
			return
		}
		replicaOperate := GetNextReplicaOperation(dp, srcZone, dstZone, int(dp.ReplicaNum))
		if replicaOperate == None || replicaOperate == Finished {
			count++
		}
	}
	finish = count == len(dataPartitions.DataPartitions)
	return
}

func (mt *MigVolTask) startVolMpMigrateTask(vols []string) {
	defer func() {
		if mt.status == StatusRunning {
			mt.status = StatusStop
		}
		mt.worker.updateMigrateTaskStatus(mt.taskId, int(mt.status))
		mt.cancel()
	}()

	for _, vol := range mt.vols {
		volInfo, err := mt.AdminAPI().GetVolumeSimpleInfo(vol)
		if err != nil {
			if strings.Contains(err.Error(), proto.ErrVolNotExists.Error()) {
				mt.finishVols[vol] = true
				err = nil
				continue
			}
		}
		if !strings.Contains(volInfo.ZoneName, mt.srcZone) && !strings.Contains(volInfo.ZoneName, mt.dstZone) {
			log.LogWarnf("the zone(%v) of vol(%v) is neither srcZone(%v) nor dstZone(%v)", volInfo.ZoneName, vol, mt.srcZone, mt.dstZone)
			mt.finishVols[vol] = true
		}
	}
	mt.updateDstMetaNode()
	if len(mt.dstMetaNodes) <= 0 {
		log.LogErrorf("can't find dstMetaNodes by usageRatio")
		return
	}

	for {
		if len(mt.finishVols) == len(vols) {
			log.LogInfof("finish all vols migrate: taskID(%v)", mt.taskId)
			return
		}
		inRecoveringDPMap, err := IsInRecoveringMoreThanMaxBatchCount(mt.MasterClient, nil, RebalanceMeta, mt.clusterCurrency)
		if err != nil {
			log.LogWarnf("get IsInRecoveringDPsMoreThanMaxBatchCount: err(%v)\n", err)
			time.Sleep(defaultWaitClusterRecover)
			continue
		}
		availableClusterCurrency := mt.clusterCurrency - len(inRecoveringDPMap)
		if availableClusterCurrency <= 0 {
			log.LogWarnf("clusterCurrency reach the limit: taskID(%v) clusterCurrency(%v) recoveringNum(%v)", mt.taskId, mt.clusterCurrency, len(inRecoveringDPMap))
			time.Sleep(defaultWaitClusterRecover)
			continue
		}
		currMigVolCount := 0
		for _, vol := range vols {
			select {
			case <-mt.ctx.Done():
				mt.status = StatusTerminating
				log.LogInfof("startVolMigrateTask: receive stopped signal, taskID(%v)", mt.taskId)
				return
			default:
			}

			if mt.finishVols[vol] {
				continue
			}
			if currMigVolCount >= mt.volCurrency {
				log.LogInfof("wait for next round: taskID(%v) migrate count(%v) reach volBatchCount(%v)", mt.taskId, currMigVolCount, mt.volCurrency)
				break
			}
			err := mt.doChangeVolMpToTargetZone(vol, mt.dstZone, &availableClusterCurrency)
			if !mt.finishVols[vol] && err == nil {
				currMigVolCount++
			}
			if availableClusterCurrency <= 0 {
				log.LogInfof("wait for next round: taskID(%v) actualMaxClusterCount(%v) reach the limit\n", mt.taskId, availableClusterCurrency)
				break
			}
			time.Sleep(defaultMigNodeInterval)
		}
		rest := len(vols) - len(mt.finishVols)
		if rest > 0 {
			time.Sleep(time.Second * time.Duration(mt.roundInterval))
		}
		log.LogInfof("taskID[%v] total[%v] finish[%v] remains[%v]\n", mt.taskId, len(vols), len(mt.finishVols), rest)
	}
}

func (mt *MigVolTask) doChangeVolMpToTargetZone(volName, targetZone string, clusterCurrency *int) (err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("doChangeVolMpToTargetZone: taskID(%v) vol(%v) targetZone(%v) err(%v)", mt.taskId, volName, targetZone, err)
		}
	}()
	inRecoveringMPMap, err := IsInRecoveringMoreThanMaxBatchCount(mt.MasterClient, nil, RebalanceMeta, mt.clusterCurrency)
	if err != nil {
		log.LogErrorf("get IsInRecoveringDPsMoreThanMaxBatchCount: err(%v)\n", err)
		return
	}
	curAvailableMaxClusterCount := mt.clusterCurrency - len(inRecoveringMPMap)
	*clusterCurrency = int(math.Min(float64(*clusterCurrency), float64(curAvailableMaxClusterCount)))
	if *clusterCurrency <= 0 {
		log.LogWarnf("wait because actualClusterCurrency <=0: %v taskID(%v)", *clusterCurrency, mt.taskId)
		return ErrReachClusterCurrency
	}

	metaPartitions, err := mt.ClientAPI().GetMetaPartitions(volName)
	if err != nil {
		return
	}
	isRecoverCnt := 0
	for _, partition := range metaPartitions {
		if partition.IsRecover {
			isRecoverCnt++
		}
	}
	sort.Slice(metaPartitions, func(i, j int) bool {
		// 按分片ID迁移 不需要按分片数量排序
		return metaPartitions[i].PartitionID < metaPartitions[j].PartitionID
	})
	toBeMigrateMps := mt.filterVolMetaPartitions(volName, metaPartitions, inRecoveringMPMap)
	if len(toBeMigrateMps) == 0 {
		trulyFinish := checkVolFinishChangeMp(volName, mt.srcZone, mt.MasterClient)
		if trulyFinish {
			mt.finishVols[volName] = true
			mt.worker.insertOrUpdateVolMigrateTask(mt.taskId, mt.cluster, volName, mt.srcZone, mt.dstZone, mt.module, len(metaPartitions), StatusStop)
		}
		log.LogWarnf("no available migrateMp: taskID(%v) vol(%v)", mt.taskId, volName)
		return
	}
	actualVolMpCount := mt.volMpCurrency - isRecoverCnt
	if actualVolMpCount <= 0 {
		log.LogWarnf("vol recoverMpCount reach mpCurrency: taskID(%v) vol(%v) recoverCnt(%v) mpCurrency(%v)", mt.taskId, volName, isRecoverCnt, mt.volMpCurrency)
		return ErrReachVolCurrency
	}

	return mt.serialChangeMp(volName, len(metaPartitions), actualVolMpCount, toBeMigrateMps, clusterCurrency)
}

func (mt *MigVolTask) filterVolMetaPartitions(volName string, partitions []*proto.MetaPartitionView, clusterRecoverMpMap map[uint64]int) (result []*proto.MetaPartitionInfo) {
	result = make([]*proto.MetaPartitionInfo, 0)
	for _, partition := range partitions {
		if clusterRecoverMpMap[partition.PartitionID] != 0 {
			continue
		}
		mp, err := mt.ClientAPI().GetMetaPartition(partition.PartitionID, volName)
		if err != nil {
			log.LogWarnf("get metaPartition failed: vol(%v) mp(%v) err(%v)", volName, partition.PartitionID, err)
			continue
		}
		if len(mp.Hosts) < 2 || len(mp.Replicas) < 2 {
			log.LogWarnf("vol(%v) mp(%v) len(host) or len(replicas) < 2", volName, mp.PartitionID)
			continue
		}
		if len(mp.MissNodes) > 0 {
			log.LogWarnf("vol(%v) mp(%v) len(missNodes) > 0", volName, mp.PartitionID)
			continue
		}
		remainSrcCount := 0
		for _, zone := range mp.Zones {
			if zone == mt.srcZone {
				remainSrcCount++
			}
		}
		if remainSrcCount == 0 {
			log.LogWarnf("no host in srcZone: vol(%v) mp(%v)", volName, mp.PartitionID)
			continue
		}
		if mp.IsRecover {
			log.LogWarnf("mp in recover: vol(%v) mp(%v)", volName, mp.PartitionID)
			continue
		}
		// 异常副本，有leader，数据是否追上
		hasLeader := false
		for _, replica := range mp.Replicas {
			if replica.IsLeader {
				hasLeader = true
			}
			if replica.Status == proto.Unavailable {
				return
			}
		}
		if hasLeader == false {
			log.LogWarnf("mp no leader: vol(%v) mp(%v)", volName, mp.PartitionID)
			continue
		}
		if !IsMpDataCatchUp(mp) {
			log.LogWarnf("mp data not catch up: vol(%v) mp(%v)", volName, mp.PartitionID)
			continue
		}
		result = append(result, mp)
	}
	return
}

func (mt *MigVolTask) serialChangeMp(volName string, volMpNum, mpCurrency int, migMps []*proto.MetaPartitionInfo, clusterCurrency *int) (err error) {
	id, err := mt.worker.insertOrUpdateVolMigrateTask(mt.taskId, mt.cluster, volName, mt.srcZone, mt.dstZone, mt.module, volMpNum, StatusRunning)
	if err != nil {
		log.LogErrorf("serialChangeMp: insert vol record failed: taskID(%v) vol(%v) err(%v)", mt.taskId, volMpNum, err)
	}

	var stackErr error
	for _, migMp := range migMps {
		select {
		case <-mt.ctx.Done():
			mt.status = StatusTerminating
			mt.worker.updateVolRecordStatus(id, StatusTerminating)
			return
		default:
		}
		if mpCurrency <= 0 || *clusterCurrency <= 0 {
			log.LogWarnf("serialChangeMp: reach mpCurrency limit, mpCurrency(%v)", mpCurrency)
			return
		}

		targetMetaNode, getErr := mt.getTargetMetaNodes(migMp.Hosts, mt.maxDstMetanodeUsage, mt.maxDstMetaPartitionCnt)
		if getErr != nil {
			continue
		}

		err = mt.doChangeMp(migMp, targetMetaNode, mt.dstZone)
		if err != nil {
			stackErr = errors.Join(stackErr, err)
			continue
		}
		mpCurrency--
		*clusterCurrency--
	}
	// todo: 所有分片都选不出dst节点 没有实际发生迁移
	if stackErr == nil {
		mt.finishVols[volName] = true
		mt.worker.updateVolRecordStatus(id, StatusStop)
	}
	return
}

func (mt *MigVolTask) doChangeMp(migMp *proto.MetaPartitionInfo, targetMetaNode, dstZone string) (err error) {
	// 最后对leader执行下线
	// 检测是不是只有leader 不是targetZone
	isOnlyLeaderNotInTargetZone := IsOnlyLeaderNotInTargetZone(migMp, dstZone)
	for i := len(migMp.Zones) - 1; i >= 0; i-- {
		if migMp.Zones[i] != dstZone {
			var storeMode proto.StoreMode
			migAddr := migMp.Hosts[i]
			if isOnlyLeaderNotInTargetZone == false {
				migAddrIsLeader := false
				for _, replica := range migMp.Replicas {
					if replica.Addr == migAddr {
						if replica.IsLeader {
							migAddrIsLeader = true
						}
						storeMode = replica.StoreMode
					}
				}
				if migAddrIsLeader {
					continue
				}
			}
			e1 := mt.AdminAPI().DecommissionMetaPartition(migMp.PartitionID, migAddr, targetMetaNode, int(storeMode))
			if e1 != nil {
				err = errors.Join(err, e1)
				log.LogErrorf("DecommissionMetaPartition failed: dp(%v) src(%v) dst(%v) err(%v)", migMp.PartitionID, migAddr, targetMetaNode, e1)
			} else {
				record := &MigrateRecordTable{
					ClusterName: mt.cluster,
					ZoneName:    dstZone,
					RType:       RebalanceMeta,
					VolName:     migMp.VolName,
					PartitionID: migMp.PartitionID,
					SrcAddr:     migAddr,
					DstAddr:     targetMetaNode,
					TaskId:      mt.taskId,
					TaskType:    VolsMigrate,
					CreatedAt:   time.Now(),
					UpdateAt:    time.Now(),
				}
				mt.worker.insertPartitionMigRecords(record)
			}
		}
	}
	log.LogDebugf("doChangeMp: taskID(%v) mp(%v) dstZone(%v) err(%v)", mt.taskId, migMp.PartitionID, dstZone, err)
	return err
}

func (mt *MigVolTask) getTargetMetaNodes(hosts []string, maxUsage float64, maxMpCount int) (nodeAddr string, err error) {
	for i := 0; i < len(mt.dstMetaNodes); i++ {
		mt.dstIndexLock.Lock()
		if mt.dstIndex >= len(mt.dstMetaNodes) {
			mt.dstIndex = 0
		}
		nodeAddr = mt.dstMetaNodes[mt.dstIndex]
		mt.dstIndex++
		mt.dstIndexLock.Unlock()

		if !stringSliceContain(hosts, nodeAddr) {
			// 检查 nodeAddr 的状态
			metaNode, err := mt.NodeAPI().GetMetaNode(nodeAddr)
			if err != nil {
				return "", err
			}
			if metaNode.IsActive == false {
				continue
			}
			if metaNode.Ratio >= maxUsage || metaNode.MetaPartitionCount >= maxMpCount {
				continue
			}
			return nodeAddr, err
		}
	}
	if nodeAddr == "" || utils.Contains(hosts, nodeAddr) {
		return "", fmt.Errorf("can not get a target node")
	}
	return
}

func (mt *MigVolTask) updateDstMetaNode() {
	zoneMetaNodes, err := getZoneMetaNodes(mt.MasterClient, nil, mt.dstZone)
	if err != nil {
		log.LogErrorf("updateDstMetaNode: taskID(%v) error(%v)", mt.taskId, err)
		return
	}
	filterDstMetaNodes := mt.filterMetaNodesByUsageRatioAndCount(zoneMetaNodes, mt.maxDstMetanodeUsage, mt.maxDstMetaPartitionCnt)
	mt.dstMetaNodes = filterDstMetaNodes
}

func (mt *MigVolTask) filterMetaNodesByUsageRatioAndCount(hosts []string, maxUsageRatio float64, maxCount int) (nodeList []string) {
	for _, host := range hosts {
		// todo: get info from metanode
		nodeInfo, err := mt.NodeAPI().GetMetaNode(host)
		if err != nil {
			continue
		}
		if nodeInfo.Ratio < maxUsageRatio && nodeInfo.MetaPartitionCount < maxCount {
			nodeList = append(nodeList, host)
		}
	}
	return
}

func checkVolFinishChangeMp(volName, srcZone string, masterClient *master.MasterClient) (finish bool) {
	defer func() {
		if finish {
			log.LogInfof("checkVolFinishChangeMp: vol(%v)", volName)
		}
	}()
	metaPartitions, err := masterClient.ClientAPI().GetMetaPartitions(volName)
	if err != nil {
		log.LogWarnf("get metaPartition failed: vol(%v) err(%v)", volName, err)
		return
	}
	count := 0
	for _, partition := range metaPartitions {
		dp, err1 := masterClient.ClientAPI().GetMetaPartition(partition.PartitionID, volName)
		if err1 != nil {
			return
		}
		remainSrcCount := 0
		for _, zone := range dp.Zones {
			if zone == srcZone {
				remainSrcCount++
			}
		}
		if remainSrcCount == 0 {
			count++
		}
	}
	finish = count == len(metaPartitions)
	return
}

func IsOnlyLeaderNotInTargetZone(mp *proto.MetaPartitionInfo, dstZone string) bool {
	notDstZoneCount := 0
	for _, zone := range mp.Zones {
		if zone != dstZone {
			notDstZoneCount++
		}
	}
	if notDstZoneCount == 1 {
		for i, zone := range mp.Zones {
			if zone != dstZone {
				for _, replica := range mp.Replicas {
					if replica.Addr == mp.Hosts[i] && replica.IsLeader {
						return true
					}
				}
			}
		}
	}
	return false
}

func IsMpDataCatchUp(mp *proto.MetaPartitionInfo) bool {
	return getMinusOfInodeCount(mp) < 50 && getMinusOfDentryCount(mp) < 50
}

func getMinusOfInodeCount(mp *proto.MetaPartitionInfo) (minus float64) {
	var sentry float64
	for index, replica := range mp.Replicas {
		if index == 0 {
			sentry = float64(replica.InodeCount)
			continue
		}
		diff := math.Abs(float64(replica.InodeCount) - sentry)
		if diff > minus {
			minus = diff
		}
	}
	return
}

func getMinusOfDentryCount(mp *proto.MetaPartitionInfo) (minus float64) {
	var sentry float64
	for index, replica := range mp.Replicas {
		if index == 0 {
			sentry = float64(replica.DentryCount)
			continue
		}
		diff := math.Abs(float64(replica.DentryCount) - sentry)
		if diff > minus {
			minus = diff
		}
	}
	return
}

func (mt *MigVolTask) getRunningTaskStatus() *VolMigrateTaskStatus {
	taskStatus := new(VolMigrateTaskStatus)
	taskStatus.Status = int(mt.status)
	taskStatus.VolMigrateInfos = make([]*VolMigrateInfo, 0, len(mt.vols))
	for _, vol := range mt.vols {
		volStatus := new(VolMigrateInfo)
		volStatus.Name = vol
		if mt.finishVols[vol] {
			//todo: 在finishVol中 但是vol表中没有finish(非正常情况)
			volStatus.Status = StatusStop
		}
		volMigrateInfo, err := mt.worker.GetVolMigrateRecord(mt.taskId, vol)
		if err != nil {
			if err.Error() == RECORD_NOT_FOUND {
				// 已删除的vol 或者 vol zone既不在srcZone 也不在dstZone
				taskStatus.VolMigrateInfos = append(taskStatus.VolMigrateInfos, volStatus)
			}
			continue
		}
		volStatus.Status = volMigrateInfo.Status
		volStatus.UpdateTime = volMigrateInfo.UpdateAt.Format(time.DateTime)
		volStatus.TotalCount = volMigrateInfo.Partitions
		volStatus.MigratePartitionCount, _ = mt.worker.getVolMigPartitionCount(mt.taskId, vol)
		taskStatus.VolMigrateInfos = append(taskStatus.VolMigrateInfos, volStatus)
	}
	return taskStatus
}

func (rw *ReBalanceWorker) getHistoryVolMigTaskStatus(taskInfo *RebalancedInfoTable) *VolMigrateTaskStatus {
	volList := strings.Split(taskInfo.VolName, ",")
	taskStatus := new(VolMigrateTaskStatus)
	taskStatus.Status = int(taskInfo.Status)
	taskStatus.VolMigrateInfos = make([]*VolMigrateInfo, 0, len(volList))
	for _, vol := range volList {
		volStatus := new(VolMigrateInfo)
		volStatus.Name = vol
		volMigrateInfo, err := rw.GetVolMigrateRecord(taskInfo.ID, vol)
		if err != nil {
			if err.Error() == RECORD_NOT_FOUND {
				taskStatus.VolMigrateInfos = append(taskStatus.VolMigrateInfos, volStatus)
			}
			continue
		}
		volStatus.Status = volMigrateInfo.Status
		volStatus.UpdateTime = volMigrateInfo.UpdateAt.Format(time.DateTime)
		volStatus.TotalCount = volMigrateInfo.Partitions
		volStatus.MigratePartitionCount, _ = rw.getVolMigPartitionCount(taskInfo.ID, vol)
		taskStatus.VolMigrateInfos = append(taskStatus.VolMigrateInfos, volStatus)
	}
	return taskStatus
}

func stringSliceContain(arr []string, ele string) bool {
	ele = strings.TrimSpace(ele)
	if arr == nil || len(arr) == 0 || len(ele) == 0 {
		return false
	}
	for _, e := range arr {
		if e == ele {
			return true
		}
	}
	return false
}
