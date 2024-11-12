package rebalance

import (
	"context"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util/log"
	"sort"
	"sync"
	"time"
)

// ZoneReBalanceController 用于控制每个Zone内数据的迁移
type ZoneReBalanceController struct {
	masterClient  *master.MasterClient
	releaseClient *releaseClient

	Id           uint64
	cluster      string
	zoneName     string
	rType        RebalanceType
	srcDataNodes []*DNReBalanceController       // data node待迁移的源机器
	dstDataNodes []*DataNodeStatsWithAddr       // data node目标机器
	srcMetaNodes []*MetaNodeReBalanceController // meta node待迁移的源机器
	dstMetaNodes []*proto.MetaNodeInfo          // meta node目标机器
	highRatio    float64                        // 源机器的阈值
	lowRatio     float64                        // 目标机器的阈值
	goalRatio    float64                        // 迁移需达到的阈值
	ctx          context.Context                // context控制ReBalance程序终止
	cancel       context.CancelFunc
	status       Status
	mutex        sync.Mutex

	rw *ReBalanceWorker

	minWritableDPNum     int // 最小可写DP限制
	clusterMaxBatchCount int // 最大并发量
	dstIndex             int
	migrateLimitPerDisk  int // 每块磁盘每轮迁移dp数量上限，默认-1无上限

	dstMetaNodeMaxPartitionCount int //目标机器分片最大个数阈值

	createdAt time.Time
	updatedAt time.Time

	srcNodeList []string // 节点迁移指定的 节点列表
	dstNodeList []string
	taskType    TaskType // 节点迁移任务 or rebalance任务

	isManualStop         bool     // 是否人为停止
	partitionLastMigTime sync.Map // pid:migrate time
}

func (zoneCtrl *ZoneReBalanceController) String() string {
	return fmt.Sprintf("{taskID(%v) cluster(%s) zone(%s) module(%s) type(%s)",
		zoneCtrl.Id, zoneCtrl.cluster, zoneCtrl.zoneName, zoneCtrl.rType.String(), zoneCtrl.taskType.String())
}

func newZoneReBalanceController(id uint64, cluster, zoneName string, rType RebalanceType, highRatio, lowRatio, goalRatio float64, rw *ReBalanceWorker) *ZoneReBalanceController {
	zoneCtrl := &ZoneReBalanceController{
		Id:                           id,
		cluster:                      cluster,
		zoneName:                     zoneName,
		highRatio:                    highRatio,
		lowRatio:                     lowRatio,
		goalRatio:                    goalRatio,
		status:                       StatusStop,
		minWritableDPNum:             defaultMinWritableDPNum,
		clusterMaxBatchCount:         defaultClusterMaxBatchCount,
		migrateLimitPerDisk:          -1,
		rw:                           rw,
		rType:                        rType,
		taskType:                     ZoneAutoReBalance,
		dstMetaNodeMaxPartitionCount: defaultDstMetaNodePartitionMaxCount,
	}
	if isRelease(cluster) {
		zoneCtrl.releaseClient = newReleaseClient([]string{rw.getClusterHost(cluster)}, cluster)
	} else {
		zoneCtrl.masterClient = master.NewMasterClient([]string{rw.getClusterHost(cluster)}, false)
	}
	return zoneCtrl
}

func newNodeMigrateController(id uint64, cluster string, rType RebalanceType, srcNodeList, dstNodeList []string, rw *ReBalanceWorker) *ZoneReBalanceController {
	zoneCtrl := &ZoneReBalanceController{
		Id:                           id,
		cluster:                      cluster,
		status:                       StatusStop,
		rType:                        rType,
		taskType:                     NodesMigrate,
		rw:                           rw,
		minWritableDPNum:             defaultMinWritableDPNum,
		clusterMaxBatchCount:         defaultClusterMaxBatchCount,
		migrateLimitPerDisk:          -1,
		dstMetaNodeMaxPartitionCount: defaultDstMetaNodePartitionMaxCount,
		srcNodeList:                  srcNodeList,
		dstNodeList:                  dstNodeList,
	}
	if isRelease(cluster) {
		zoneCtrl.releaseClient = newReleaseClient([]string{rw.getClusterHost(cluster)}, cluster)
	} else {
		zoneCtrl.masterClient = master.NewMasterClient([]string{rw.getClusterHost(cluster)}, false)
	}
	return zoneCtrl
}

func (zoneCtrl *ZoneReBalanceController) SetCtrlTaskID(id uint64) {
	zoneCtrl.Id = id
}

func (zoneCtrl *ZoneReBalanceController) SetCreatedUpdatedAt(createdAt, updatedAt time.Time) {
	zoneCtrl.createdAt = createdAt
	zoneCtrl.updatedAt = updatedAt
}

func (zoneCtrl *ZoneReBalanceController) UpdateRatio(highRatio, lowRatio, goalRatio float64) {
	zoneCtrl.highRatio = highRatio
	zoneCtrl.lowRatio = lowRatio
	zoneCtrl.goalRatio = goalRatio
	return
}

func (zoneCtrl *ZoneReBalanceController) SetMinWritableDPNum(minWritableDPNum int) {
	if minWritableDPNum <= 0 {
		return
	}
	zoneCtrl.minWritableDPNum = minWritableDPNum
}

func (zoneCtrl *ZoneReBalanceController) SetClusterMaxBatchCount(clusterMaxBatchCount int) {
	zoneCtrl.clusterMaxBatchCount = clusterMaxBatchCount
}

func (zoneCtrl *ZoneReBalanceController) SetDstMetaNodeMaxPartitionCount(mpCountLimit int) {
	zoneCtrl.dstMetaNodeMaxPartitionCount = mpCountLimit
}

func (zoneCtrl *ZoneReBalanceController) SetMigrateLimitPerDisk(limit int) {
	zoneCtrl.migrateLimitPerDisk = limit
	for _, node := range zoneCtrl.srcDataNodes {
		node.SetMigrateLimitPerDisk(limit)
	}
}

func (zoneCtrl *ZoneReBalanceController) SetIsManualStop(isManualStop bool) {
	zoneCtrl.isManualStop = isManualStop
}

func (zoneCtrl *ZoneReBalanceController) formatToReBalanceInfo() (reBalanceInfo *ReBalanceInfo) {
	var (
		srcNodesUsageRatio []NodeUsageInfo
		dstNodesUsageRatio []NodeUsageInfo
	)

	switch zoneCtrl.rType {
	case RebalanceData:
		for _, node := range zoneCtrl.srcDataNodes {
			diskView := convertDiskView(node.disks)
			srcNodesUsageRatio = append(srcNodesUsageRatio, NodeUsageInfo{
				Addr:       node.nodeInfo.Addr,
				UsageRatio: node.nodeInfo.UsageRatio,
				Disk:       diskView,
			})
		}
		for _, node := range zoneCtrl.dstDataNodes {
			dstNodesUsageRatio = append(dstNodesUsageRatio, NodeUsageInfo{
				Addr:       node.Addr,
				UsageRatio: node.UsageRatio,
			})
		}
	case RebalanceMeta:
		for _, node := range zoneCtrl.srcMetaNodes {
			srcNodesUsageRatio = append(srcNodesUsageRatio, NodeUsageInfo{
				Addr:       node.nodeInfo.Addr,
				UsageRatio: node.nodeInfo.Ratio,
			})
		}
		for _, node := range zoneCtrl.dstMetaNodes {
			dstNodesUsageRatio = append(dstNodesUsageRatio, NodeUsageInfo{
				Addr:       node.Addr,
				UsageRatio: node.Ratio,
			})
		}
	default:
		log.LogErrorf("error rebalance type, task info: %v, rebalance type: %v", zoneCtrl.String(), zoneCtrl.rType)
	}

	reBalanceInfo = &ReBalanceInfo{
		RebalancedInfoTable: RebalancedInfoTable{
			ID:                           zoneCtrl.Id,
			Cluster:                      zoneCtrl.cluster,
			ZoneName:                     zoneCtrl.zoneName,
			RType:                        zoneCtrl.rType,
			TaskType:                     zoneCtrl.taskType,
			Status:                       int(zoneCtrl.status),
			MaxBatchCount:                zoneCtrl.clusterMaxBatchCount,
			HighRatio:                    zoneCtrl.highRatio,
			LowRatio:                     zoneCtrl.lowRatio,
			GoalRatio:                    zoneCtrl.goalRatio,
			MigrateLimitPerDisk:          zoneCtrl.migrateLimitPerDisk,
			DstMetaNodePartitionMaxCount: zoneCtrl.dstMetaNodeMaxPartitionCount,
			CreatedAt:                    zoneCtrl.createdAt,
			UpdatedAt:                    zoneCtrl.updatedAt,
		},
		SrcNodesUsageRatio: srcNodesUsageRatio,
		DstNodesUsageRatio: dstNodesUsageRatio,
	}
	return
}

// Status 返回ReBalance状态
func (zoneCtrl *ZoneReBalanceController) Status() Status {
	zoneCtrl.mutex.Lock()
	defer zoneCtrl.mutex.Unlock()
	return zoneCtrl.status
}

// ReBalanceStop 关闭ReBalance程序
func (zoneCtrl *ZoneReBalanceController) ReBalanceStop() error {
	zoneCtrl.mutex.Lock()
	defer zoneCtrl.mutex.Unlock()
	switch zoneCtrl.status {
	case StatusRunning:
		zoneCtrl.status = StatusTerminating
		zoneCtrl.cancel()
	default:
		return fmt.Errorf("status error while stop rebalance in %v status code: %v", zoneCtrl.zoneName, zoneCtrl.status)
	}
	return nil
}

// ReBalanceStart 开启ReBalance程序
func (zoneCtrl *ZoneReBalanceController) ReBalanceStart() error {
	switch zoneCtrl.rType {
	case RebalanceData:
		return zoneCtrl.reBalanceDataStart()
	case RebalanceMeta:
		return zoneCtrl.reBalanceMetaStart()
	default:
		return fmt.Errorf("unknown rebalance type: %v", zoneCtrl.rType)
	}
}

func (zoneCtrl *ZoneReBalanceController) reBalanceDataStart() error {
	zoneCtrl.mutex.Lock()
	defer zoneCtrl.mutex.Unlock()

	// 判断当前迁移状态
	if zoneCtrl.status != StatusStop {
		return fmt.Errorf("%v: expected Stop but found %v", ErrWrongStatus, getStatusStr(zoneCtrl.status))
	}

	zoneCtrl.status = StatusRunning
	zoneCtrl.ctx, zoneCtrl.cancel = context.WithCancel(context.Background())
	if zoneCtrl.taskType != NodesMigrate {
		log.LogInfof("start rebalance zone: %v", zoneCtrl.zoneName)
	}

	go zoneCtrl.doDataReBalance()
	go zoneCtrl.refreshSrcDataNodes()
	go zoneCtrl.refreshDstDataNodes()
	return nil
}

func (zoneCtrl *ZoneReBalanceController) reBalanceMetaStart() error {
	zoneCtrl.mutex.Lock()
	defer zoneCtrl.mutex.Unlock()

	// 判断当前迁移状态
	if zoneCtrl.status != StatusStop {
		return fmt.Errorf("%v: expected Stop but found %v", ErrWrongStatus, getStatusStr(zoneCtrl.status))
	}

	zoneCtrl.status = StatusRunning
	zoneCtrl.ctx, zoneCtrl.cancel = context.WithCancel(context.Background())

	log.LogInfof("start rebalance zone: %v", zoneCtrl.zoneName)
	go zoneCtrl.doMetaReBalance()
	go zoneCtrl.refreshSrcMetaNodes()
	go zoneCtrl.refreshDstMetaNodes()
	return nil
}

// 1. 判断迁移过程是否终止
// 2. 判断是否超过最大并发量
// 3. 选择一个可迁移的DP
// 4. 选择一个用于迁入的DataNode
// 5. 执行迁移
func (zoneCtrl *ZoneReBalanceController) doDataReBalance() {
	defer func() {
		zoneCtrl.mutex.Lock()
		err := zoneCtrl.rw.stopRebalanced(zoneCtrl.Id, zoneCtrl.isManualStop)
		if err != nil {
			log.LogErrorf("doDataReBalance: stop failed, taskID(%v) cluster(%v) zone(%v) err(%v)", zoneCtrl.Id, zoneCtrl.cluster, zoneCtrl.zoneName, err)
		}
		log.LogInfof("doDataReBalance: do cancel, taskID:%v", zoneCtrl.Id)
		zoneCtrl.cancel()
		zoneCtrl.mutex.Unlock()
	}()

	// 更新源、目标节点
	zoneCtrl.separateDataNodesByRatio()
	if len(zoneCtrl.dstDataNodes) == 0 || len(zoneCtrl.srcDataNodes) == 0 {
		log.LogWarnf("no available nodes: len(dst)= %v, len(src)= %v", len(zoneCtrl.dstDataNodes), len(zoneCtrl.srcDataNodes))
		return
	}

	for _, srcNode := range zoneCtrl.srcDataNodes {
		err := srcNode.updateDataNode()
		if err != nil {
			log.LogErrorf("doDataReBalance: updateDataNode failed, zone(%v) node(%v) err(%v)", zoneCtrl.zoneName, srcNode.nodeInfo.Addr, err)
		}
		for srcNode.NeedReBalance(zoneCtrl.goalRatio) {
			select {
			case <-zoneCtrl.ctx.Done():
				zoneCtrl.SetIsManualStop(true)
				log.LogInfof("doDataReBalance: stop taskID(%v) cluster(%v) zone(%v)", zoneCtrl.Id, zoneCtrl.cluster, zoneCtrl.zoneName)
				return
			default:
			}

			inRecoveringDPMap, err := IsInRecoveringMoreThanMaxBatchCount(zoneCtrl.masterClient, zoneCtrl.releaseClient, zoneCtrl.rType, zoneCtrl.clusterMaxBatchCount)
			if err != nil {
				log.LogWarnf("doDataReBalance: %v", err.Error())
				// 等cluster中badPartition恢复
				time.Sleep(defaultWaitClusterRecover)
				continue
			}
			clusterDpCurrency := zoneCtrl.clusterMaxBatchCount - len(inRecoveringDPMap)
			log.LogInfof("doDataReBalance: taskID(%v) srcNode(%v) canBeMigCount(%v)", zoneCtrl.Id, srcNode.nodeInfo.Addr, clusterDpCurrency)
			err = srcNode.doMigrate(clusterDpCurrency)
			if err != nil {
				log.LogErrorf("doDataReBalance: doMigrate failed, taskID(%v) node(%v) err(%v)", zoneCtrl.Id, srcNode.nodeInfo.Addr, err)
				break
			}
		}
		srcNode.isFinished = true
		log.LogInfof("doDataReBalance: taskID(%v) zone(%v) srcNode(%v) finished", zoneCtrl.Id, zoneCtrl.zoneName, srcNode.nodeInfo.Addr)
	}
}

func (zoneCtrl *ZoneReBalanceController) refreshSrcDataNodes() {
	ticker := time.NewTicker(defaultRefreshNodeInterval)
	defer func() {
		ticker.Stop()
	}()
	for {
		log.LogDebugf("[refreshSrcDataNodes] cluster:%v zoneName:%v", zoneCtrl.cluster, zoneCtrl.zoneName)
		select {
		case <-ticker.C:
			for i, node := range zoneCtrl.srcDataNodes {
				addr := node.nodeInfo.Addr
				dataNode, err := zoneCtrl.getCommonDataNodeInfo(addr)
				if err != nil {
					log.LogWarnf("[refreshSrcDataNodes] taskId(%v) cluster(%v) zone(%v) datanode(%v) err(%v)",
						zoneCtrl.Id, zoneCtrl.cluster, zoneCtrl.zoneName, addr, err)
					continue
				}
				newNode := NewDataNodeStatsWithAddr(dataNode, node.nodeInfo.Addr)
				zoneCtrl.srcDataNodes[i].nodeInfo = newNode

			}
		case <-zoneCtrl.ctx.Done():
			log.LogInfof("[refreshSrcDataNodes] stopRefresh taskId:%v cluster:%v zoneName:%v", zoneCtrl.Id, zoneCtrl.cluster, zoneCtrl.zoneName)
			return
		}
	}
}

func (zoneCtrl *ZoneReBalanceController) refreshDstDataNodes() {
	ticker := time.NewTicker(defaultRefreshNodeInterval)
	defer func() {
		ticker.Stop()
	}()
	for {
		log.LogDebugf("[refreshDstDataNodes] cluster:%v zoneName:%v", zoneCtrl.cluster, zoneCtrl.zoneName)
		select {
		case <-ticker.C:
			for i, node := range zoneCtrl.dstDataNodes {
				dataNode, err := zoneCtrl.getCommonDataNodeInfo(node.Addr)
				if err != nil {
					log.LogWarnf("[refreshDstDataNodes] taskId(%v) cluster(%v) zoneName(%v) datanode(%v) err(%v)",
						zoneCtrl.Id, zoneCtrl.cluster, zoneCtrl.zoneName, node.Addr, err)
					continue
				}
				updateNode := NewDataNodeStatsWithAddr(dataNode, node.Addr)
				zoneCtrl.dstDataNodes[i] = updateNode
			}
		case <-zoneCtrl.ctx.Done():
			log.LogInfof("[refreshDstDataNodes] stopRefresh taskId:%v cluster:%v zoneName:%v", zoneCtrl.Id, zoneCtrl.cluster, zoneCtrl.zoneName)
			return
		}
	}
}

func (zoneCtrl *ZoneReBalanceController) doMetaReBalance() {
	defer func() {
		zoneCtrl.mutex.Lock()
		if err := zoneCtrl.rw.stopRebalanced(zoneCtrl.Id, zoneCtrl.isManualStop); err != nil {
			log.LogErrorf("stop task failed, task(%v) err(%v)", zoneCtrl.String(), err)
		}
		log.LogInfof("doMetaReBalance: do cancel, taskID:%v", zoneCtrl.Id)
		zoneCtrl.cancel()
		zoneCtrl.mutex.Unlock()
	}()

	// 更新源、目标节点
	zoneCtrl.separateMetaNodesByRatio()
	if len(zoneCtrl.dstMetaNodes) == 0 || len(zoneCtrl.srcMetaNodes) == 0 {
		log.LogWarnf("no available nodes: len(dst)= %v, len(src)= %v", len(zoneCtrl.dstMetaNodes), len(zoneCtrl.srcMetaNodes))
		return
	}

	for _, srcMetaNode := range zoneCtrl.srcMetaNodes {
		select {
		case <-zoneCtrl.ctx.Done():
			zoneCtrl.SetIsManualStop(true)
			log.LogInfof("doMetaReBalance: stop: %v", zoneCtrl.String())
			return
		default:
		}

		err := srcMetaNode.updateSortedMetaPartitions()
		if err != nil {
			log.LogWarnf("doMetaReBalance: updateSortedMetaPartitions failed, err(%v)", err)
			continue
		}
		needRebalance := srcMetaNode.NeedReBalance(zoneCtrl.goalRatio)
		if needRebalance {
			if err = srcMetaNode.doMigrate(); err != nil {
				log.LogErrorf("doMetaReBalance: doMigrate failed, taskID(%v) node(%v) err(%v)", zoneCtrl.Id, srcMetaNode.nodeInfo.Addr, err)
			}
		}
		srcMetaNode.isFinished = true
		log.LogInfof("doMetaReBalance cluster:%v zoneName:%v srcMetaNode:%v finished", zoneCtrl.cluster, zoneCtrl.zoneName, srcMetaNode.nodeInfo.Addr)
	}
}

func (zoneCtrl *ZoneReBalanceController) refreshSrcMetaNodes() {
	ticker := time.NewTicker(defaultRefreshNodeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			for index := 0; index < len(zoneCtrl.srcMetaNodes); index++ {
				addr := zoneCtrl.srcMetaNodes[index].nodeInfo.Addr
				newNodeInfo, err := zoneCtrl.getCommonMetaNodeInfo(addr)
				if err != nil {
					log.LogWarnf("[refreshSrcMetaNodes] failed: taskID(%v) cluster(%v) zone(%v) node(%v) err(%v)", zoneCtrl.Id, zoneCtrl.cluster, zoneCtrl.zoneName, addr, err)
					continue
				}
				zoneCtrl.srcMetaNodes[index].nodeInfo = newNodeInfo
			}
		case <-zoneCtrl.ctx.Done():
			log.LogInfof("rebalance task finished, taskInfo: %v", zoneCtrl.String())
			return
		}
	}
}

func (zoneCtrl *ZoneReBalanceController) refreshDstMetaNodes() {
	ticker := time.NewTicker(defaultRefreshNodeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			for index := 0; index < len(zoneCtrl.dstMetaNodes); index++ {
				nodeInfo := zoneCtrl.dstMetaNodes[index]
				newNodeInfo, err := zoneCtrl.getCommonMetaNodeInfo(nodeInfo.Addr)
				if err != nil {
					log.LogWarnf("[refreshDstMetaNodes] failed: taskID(%v) cluster(%v) zone(%v) node(%v) err(%v)", zoneCtrl.Id, zoneCtrl.cluster, zoneCtrl.zoneName, nodeInfo.Addr, err)
					continue
				}
				zoneCtrl.dstMetaNodes[index] = newNodeInfo
			}
		case <-zoneCtrl.ctx.Done():
			log.LogInfof("rebalance task finished, taskInfo: %v", zoneCtrl.String())
			return
		}
	}
}

// 读取当前zone中的dataNode列表，根据使用率划分为srcNode和dstNode
// srcNode: 使用率高的源机器
// dstNode: 使用率低可作为迁移目标的目标机器
func (zoneCtrl *ZoneReBalanceController) separateDataNodesByRatio() {
	zoneCtrl.srcDataNodes = make([]*DNReBalanceController, 0)
	zoneCtrl.dstDataNodes = make([]*DataNodeStatsWithAddr, 0)

	var getNodeFunc = func(nodeList []string) map[string]*proto.DataNodeStats {
		dataNodes := make(map[string]*proto.DataNodeStats)
		lock := sync.Mutex{}

		wg := sync.WaitGroup{}
		c := make(chan struct{}, 10)

		for _, addr := range nodeList {
			wg.Add(1)
			c <- struct{}{}
			go func(addr string) {
				defer func() {
					wg.Done()
					<-c
				}()
				node, err := zoneCtrl.getCommonDataNodeInfo(addr)
				if err != nil {
					return
				}
				lock.Lock()
				dataNodes[addr] = node
				lock.Unlock()
			}(addr)
		}
		wg.Wait()
		return dataNodes
	}
	switch zoneCtrl.taskType {
	case NodesMigrate:
		srcDataNodes := getNodeFunc(zoneCtrl.srcNodeList)
		dstDataNodes := getNodeFunc(zoneCtrl.dstNodeList)
		zoneCtrl.updateSrcDataNodes(srcDataNodes)
		zoneCtrl.updateDstDataNodes(dstDataNodes)

	case ZoneAutoReBalance:
		dataNodeAddrs, err := getZoneDataNodesByClient(zoneCtrl.masterClient, zoneCtrl.releaseClient, zoneCtrl.zoneName)
		if err != nil {
			log.LogErrorf("updateDataNodes: getZoneDataNodes failed, err(%v)", err)
			return
		}
		dataNodes := getNodeFunc(dataNodeAddrs)
		zoneCtrl.updateSrcDataNodes(dataNodes)
		zoneCtrl.updateDstDataNodes(dataNodes)
	}
	return
}

func (zoneCtrl *ZoneReBalanceController) separateMetaNodesByRatio() {
	zoneCtrl.dstMetaNodes = make([]*proto.MetaNodeInfo, 0)
	zoneCtrl.srcMetaNodes = make([]*MetaNodeReBalanceController, 0)

	var getMetaNodefunc = func(nodeList []string) map[string]*proto.MetaNodeInfo {
		nodesInfo := make(map[string]*proto.MetaNodeInfo)
		var (
			wg   sync.WaitGroup
			lock sync.Mutex
			c    = make(chan struct{}, 10)
		)
		for _, addr := range nodeList {
			wg.Add(1)
			c <- struct{}{}
			go func(addr string) {
				defer func() {
					wg.Done()
					<-c
				}()
				var nodeInfo *proto.MetaNodeInfo
				if isRelease(zoneCtrl.cluster) {
					node, err := zoneCtrl.releaseClient.AdminGetMetaNode(addr)
					if err != nil {
						log.LogErrorf("GetMetaNode failed, taskID[%v] node: %s, err :%v", zoneCtrl.Id, addr, err)
						return
					}
					// release集群转化成spark的
					nodeInfo = &proto.MetaNodeInfo{
						ID:                        node.ID,
						Addr:                      node.Addr,
						IsActive:                  node.IsActive,
						Ratio:                     node.Ratio,
						MetaPartitionCount:        node.MetaPartitionCount,
						PersistenceMetaPartitions: node.PersistenceMetaPartitions,
						ReportTime:                node.ReportTime,
					}
				} else {
					var errForGet error
					nodeInfo, errForGet = zoneCtrl.masterClient.NodeAPI().GetMetaNode(addr)
					if errForGet != nil {
						log.LogErrorf("GetMetaNode failed, taskID[%v] node: %s, err :%v", zoneCtrl.Id, addr, errForGet)
						return
					}
				}
				lock.Lock()
				nodesInfo[addr] = nodeInfo
				lock.Unlock()
			}(addr)
		}
		wg.Wait()
		return nodesInfo
	}

	switch zoneCtrl.taskType {
	case NodesMigrate:
		srcMetaNodes := getMetaNodefunc(zoneCtrl.srcNodeList)
		dstMetaNodes := getMetaNodefunc(zoneCtrl.dstNodeList)
		zoneCtrl.updateSrcMetaNodes(srcMetaNodes)
		zoneCtrl.updateDstMetaNodes(dstMetaNodes)

	case ZoneAutoReBalance:
		metaNodesAddrs, err := getZoneMetaNodes(zoneCtrl.masterClient, zoneCtrl.releaseClient, zoneCtrl.zoneName)
		if err != nil {
			log.LogErrorf("getZoneMetaNodes failed, taskID[%v] err(%v)", zoneCtrl.Id, err)
			return
		}
		metaNodes := getMetaNodefunc(metaNodesAddrs)
		zoneCtrl.updateDstMetaNodes(metaNodes)
		zoneCtrl.updateSrcMetaNodes(metaNodes)
	}
	return
}

func (zoneCtrl *ZoneReBalanceController) updateSrcDataNodes(dataNodes map[string]*proto.DataNodeStats) {
	for addr, node := range dataNodes {
		dataNode := NewDataNodeStatsWithAddr(node, addr)
		if dataNode.UsageRatio < zoneCtrl.highRatio {
			continue
		}
		log.LogInfof("updateSrcDataNodes: srcNode:%v usageRatio:%v", addr, dataNode.UsageRatio)
		srcNode, err := NewDNReBalanceController(zoneCtrl, dataNode, zoneCtrl.cluster, zoneCtrl.minWritableDPNum, zoneCtrl.migrateLimitPerDisk)
		if err != nil {
			log.LogErrorf("NewDNReBalanceController failed: node(%v) err(%v)", addr, err.Error())
			continue
		}
		zoneCtrl.srcDataNodes = append(zoneCtrl.srcDataNodes, srcNode)
	}
	sort.Slice(zoneCtrl.srcDataNodes, func(i, j int) bool {
		return zoneCtrl.srcDataNodes[i].nodeInfo.UsageRatio > zoneCtrl.srcDataNodes[j].nodeInfo.UsageRatio
	})
	return
}

func (zoneCtrl *ZoneReBalanceController) updateDstDataNodes(dataNodes map[string]*proto.DataNodeStats) {
	for addr, dataNode := range dataNodes {
		dstNode := NewDataNodeStatsWithAddr(dataNode, addr)
		if zoneCtrl.taskType == NodesMigrate {
			zoneCtrl.dstDataNodes = append(zoneCtrl.dstDataNodes, dstNode)
		} else {
			if dstNode.UsageRatio > zoneCtrl.lowRatio {
				continue
			}
			log.LogInfof("updateDstDataNodes dstNode:%v usageRatio:%v lowRatio:%v ", addr, dstNode.UsageRatio, zoneCtrl.lowRatio)
			zoneCtrl.dstDataNodes = append(zoneCtrl.dstDataNodes, dstNode)
		}
	}
	return
}

func (zoneCtrl *ZoneReBalanceController) updateSrcMetaNodes(metaNodesInfo map[string]*proto.MetaNodeInfo) {
	for _, nodeInfo := range metaNodesInfo {
		if nodeInfo.Ratio < zoneCtrl.highRatio {
			continue
		}
		log.LogInfof("updateSrcMetaNodes: node: %s, usedRatio: %v", nodeInfo.Addr, nodeInfo.Ratio)
		mnCtrl := NewMetaNodeReBalanceController(nodeInfo, zoneCtrl)
		zoneCtrl.srcMetaNodes = append(zoneCtrl.srcMetaNodes, mnCtrl)
	}
	sort.Slice(zoneCtrl.srcMetaNodes, func(i, j int) bool {
		return zoneCtrl.srcMetaNodes[i].nodeInfo.Ratio > zoneCtrl.srcMetaNodes[j].nodeInfo.Ratio
	})
}

func (zoneCtrl *ZoneReBalanceController) updateDstMetaNodes(metaNodesInfo map[string]*proto.MetaNodeInfo) {
	for _, nodeInfo := range metaNodesInfo {
		if zoneCtrl.taskType == NodesMigrate {
			zoneCtrl.dstMetaNodes = append(zoneCtrl.dstMetaNodes, nodeInfo)
		} else {
			if nodeInfo.Ratio > zoneCtrl.lowRatio {
				continue
			}
			zoneCtrl.dstMetaNodes = append(zoneCtrl.dstMetaNodes, nodeInfo)
			log.LogInfof("updateDstMetaNodes dstNode:%v usageRatio:%v lowRatio:%v", nodeInfo.Addr, nodeInfo.Ratio, zoneCtrl.lowRatio)
		}
	}
	sort.Slice(zoneCtrl.dstMetaNodes, func(i, j int) bool {
		return zoneCtrl.dstMetaNodes[i].Ratio < zoneCtrl.dstMetaNodes[j].Ratio
	})
}

func (zoneCtrl *ZoneReBalanceController) getCommonDataNodeInfo(addr string) (*proto.DataNodeStats, error) {
	dataClient := getDataHttpClient(addr, zoneCtrl.rw.getDataNodePProfPort(zoneCtrl.cluster))
	status, err := dataClient.GetDatanodeStats()
	if err != nil {
		log.LogErrorf("getCommonDataNodeInfo failed: cluster(%v) zone(%v) node(%v) err(%v)", zoneCtrl.cluster, zoneCtrl.zoneName, addr, err)
		return nil, err
	}
	if isRelease(zoneCtrl.cluster) {
		status.PartitionReports = status.PartitionInfo
	}
	return status, nil
}

// todo: get nodeInfo from node instead of master
func (zoneCtrl *ZoneReBalanceController) getCommonMetaNodeInfo(addr string) (nodeInfo *proto.MetaNodeInfo, err error) {
	if isRelease(zoneCtrl.cluster) {
		nodeInfo, err = zoneCtrl.releaseClient.AdminGetMetaNode(addr)
	} else {
		nodeInfo, err = zoneCtrl.masterClient.NodeAPI().GetMetaNode(addr)
	}
	if err != nil {
		log.LogErrorf("getCommonMetaNodeInfo failed: taskID(%v) node(%s) err(%v)", zoneCtrl.Id, addr, err)
		return nil, err
	}
	return nodeInfo, nil
}

func (zoneCtrl *ZoneReBalanceController) HasMigrateRecordForDp(pid uint64) bool {
	count, err := zoneCtrl.rw.GetPartitionMigCount(zoneCtrl.Id, pid)
	if err != nil {
		log.LogErrorf("HasMigrateRecordForPartition failed: pid(%v) err(%v)", pid, err)
	}
	return count > 0
}

func (zoneCtrl *ZoneReBalanceController) HasBeenMigratedTenMinutes(pid uint64) bool {
	lastMigTime, ok := zoneCtrl.partitionLastMigTime.Load(pid)
	if !ok {
		log.LogDebugf("HasBeenMigratedTenMinutes: no records for dp(%v)", pid)
		return false
	}
	migTime := lastMigTime.(time.Time)
	if time.Since(migTime) < 10*time.Minute {
		log.LogWarnf("HasBeenMigratedTenMinutes: pid(%v) task(%v)", pid, zoneCtrl.String())
		return true
	}
	zoneCtrl.partitionLastMigTime.Delete(pid)
	return false
}

func (zoneCtrl *ZoneReBalanceController) RecordMigratePartition(pid uint64) {
	zoneCtrl.partitionLastMigTime.Store(pid, time.Now())
}

func convertActualUsageRatio(node *proto.DataNodeStats) (usage float64) {
	if node == nil {
		return
	}
	var used uint64
	for _, dpReport := range node.PartitionReports {
		diskInfo := node.DiskInfos[dpReport.DiskPath]
		if dpReport.IsSFX && diskInfo.CompressionRatio != 0 {
			used += dpReport.Used * 100 / uint64(diskInfo.CompressionRatio)
		} else {
			used += dpReport.Used
		}
	}
	usage = float64(used) / float64(node.Total)
	return
}

func convertDiskView(diskMap map[string]*Disk) (diskView []DiskView) {
	for _, disk := range diskMap {
		diskView = append(diskView, DiskView{
			Path:          disk.path,
			Total:         disk.total,
			Used:          disk.used,
			MigratedSize:  disk.migratedSize,
			MigratedCount: disk.migratedCount,
			MigrateLimit:  disk.migrateLimit,
		})
	}
	sort.Slice(diskView, func(i, j int) bool {
		return diskView[i].Path < diskView[j].Path
	})
	return
}
