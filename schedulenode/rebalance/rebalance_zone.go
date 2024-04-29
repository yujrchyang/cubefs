package rebalance

import (
	"context"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util/log"
	"gorm.io/gorm/utils"
	"sort"
	"sync"
	"time"
)

// ZoneReBalanceController 用于控制每个Zone内数据的迁移
type ZoneReBalanceController struct {
	*master.MasterClient

	Id           uint64
	cluster      string
	zoneName     string
	rType        RebalanceType
	srcDataNodes []*DNReBalanceController       // data node待迁移的源机器
	dstDataNodes []*proto.DataNodeInfo          // data node目标机器
	srcMetaNodes []*MetaNodeReBalanceController //meta node待迁移的源机器
	dstMetaNodes []*proto.MetaNodeInfo          //meta node目标机器
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
}

func (zoneCtrl *ZoneReBalanceController) reBalanceTaskInfo() string {
	return fmt.Sprintf("[ID: %v, ClusterName: %s, ZoneName: %s, type: %s]", zoneCtrl.Id, zoneCtrl.cluster, zoneCtrl.zoneName, zoneCtrl.rType)
}

func NewZoneReBalanceController(id uint64, cluster, zoneName string, rType RebalanceType, highRatio, lowRatio, goalRatio float64, rw *ReBalanceWorker) *ZoneReBalanceController {

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
	zoneCtrl.MasterClient = master.NewMasterClient([]string{rw.getClusterHost(cluster)}, false)
	return zoneCtrl
}

func newNodeReBalanceController(id uint64, cluster string, rType RebalanceType, srcNodeList, dstNodeList []string, rw *ReBalanceWorker) *ZoneReBalanceController {
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
	zoneCtrl.MasterClient = master.NewMasterClient([]string{rw.getClusterHost(cluster)}, false)
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
				Addr:       node.Addr,
				UsageRatio: node.Usage(),
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
		log.LogErrorf("error rebalance type, task info: %v, rebalance type: %v", zoneCtrl.reBalanceTaskInfo(), zoneCtrl.rType)
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
	go zoneCtrl.refreshDstMetaNodes()
	return nil
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

// Status 返回ReBalance状态
func (zoneCtrl *ZoneReBalanceController) Status() Status {
	zoneCtrl.mutex.Lock()
	defer zoneCtrl.mutex.Unlock()
	return zoneCtrl.status
}

// 读取当前zone中的dataNode列表，根据使用率划分为srcNode和dstNode
// srcNode: 使用率高的源机器
// dstNode: 使用率低可作为迁移目标的目标机器
func (zoneCtrl *ZoneReBalanceController) updateDataNodes() error {
	zoneCtrl.srcDataNodes = make([]*DNReBalanceController, 0)
	zoneCtrl.dstDataNodes = make([]*proto.DataNodeInfo, 0)

	var getNodeFunc = func(nodeList []string) map[string]*proto.DataNodeInfo {
		dataNodes := make(map[string]*proto.DataNodeInfo)
		lock := sync.Mutex{}

		wg := sync.WaitGroup{}
		wg.Add(len(nodeList))

		for _, addr := range nodeList {
			go func(addr string) {
				node, err := zoneCtrl.MasterClient.NodeAPI().GetDataNode(addr)
				if err != nil {
					log.LogErrorf("updateDataNodes error Zone: %v DataNode: %v %v", zoneCtrl.zoneName, addr, err.Error())
					return
				}
				convertActualUsageRatio(node)
				lock.Lock()
				dataNodes[addr] = node
				lock.Unlock()
				wg.Done()
			}(addr)
		}
		wg.Wait()
		return dataNodes
	}
	switch zoneCtrl.taskType {
	case NodesMigrate:
		srcDataNodes := getNodeFunc(zoneCtrl.srcNodeList)
		dstDataNodes := getNodeFunc(zoneCtrl.dstNodeList)
		zoneCtrl.updateDstNodes(dstDataNodes)
		return zoneCtrl.updateSrcNodes(srcDataNodes)

	default:
		dataNodeAddrs, err := getZoneDataNodesByClient(zoneCtrl.MasterClient, zoneCtrl.zoneName)
		if err != nil {
			return err
		}
		dataNodes := getNodeFunc(dataNodeAddrs)
		zoneCtrl.updateDstNodes(dataNodes)
		return zoneCtrl.updateSrcNodes(dataNodes)
	}
}

func convertActualUsageRatio(node *proto.DataNodeInfo) {
	if node == nil {
		return
	}
	var used uint64
	for _, dpReport := range node.DataPartitionReports {
		used += dpReport.Used
	}
	node.UsageRatio = float64(used) / float64(node.Total)
}

// 更新源节点
func (zoneCtrl *ZoneReBalanceController) updateSrcNodes(dataNodes map[string]*proto.DataNodeInfo) error {
	for addr, dataNode := range dataNodes {
		if dataNode.UsageRatio < zoneCtrl.highRatio {
			continue
		}
		log.LogInfof("srcNode:%v usageRatio:%v", dataNode.Addr, dataNode.UsageRatio)
		srcNode, err := NewDNReBalanceController(zoneCtrl, *dataNode, zoneCtrl.MasterClient, zoneCtrl.cluster, zoneCtrl.minWritableDPNum, zoneCtrl.clusterMaxBatchCount, zoneCtrl.migrateLimitPerDisk)
		if err != nil {
			log.LogErrorf("newDataNode error Zone: %v DataNode: %v %v", zoneCtrl.zoneName, addr, err.Error())
			continue
		}
		zoneCtrl.srcDataNodes = append(zoneCtrl.srcDataNodes, srcNode)
	}
	sort.Slice(zoneCtrl.srcDataNodes, func(i, j int) bool {
		return zoneCtrl.srcDataNodes[i].UsageRatio > zoneCtrl.srcDataNodes[j].UsageRatio
	})
	return nil
}

func (zoneCtrl *ZoneReBalanceController) updateDstNodes(dataNodes map[string]*proto.DataNodeInfo) {
	for _, dataNode := range dataNodes {
		if zoneCtrl.taskType == NodesMigrate {
			zoneCtrl.dstDataNodes = append(zoneCtrl.dstDataNodes, dataNode)
		} else {
			if dataNode.UsageRatio < zoneCtrl.lowRatio {
				zoneCtrl.dstDataNodes = append(zoneCtrl.dstDataNodes, dataNode)
			}
			log.LogInfof("updateDstNodes dstNode:%v usageRatio:%v lowRatio:%v compare:%v", dataNode.Addr, dataNode.UsageRatio, zoneCtrl.lowRatio, dataNode.UsageRatio < zoneCtrl.lowRatio)
		}
	}
	return
}

// 1. 从当前index开始往后遍历， 找到第一个符合条件的dstNode
// 2. 将index+1，避免重复选择一个dstNode
func (zoneCtrl *ZoneReBalanceController) selectDstNode(dpInfo *proto.DataPartitionInfo) (*proto.DataNodeInfo, error) {
	var node *proto.DataNodeInfo
	offset := 0
	for offset < len(zoneCtrl.dstDataNodes) {
		index := (zoneCtrl.dstIndex + offset) % len(zoneCtrl.dstDataNodes)
		node = zoneCtrl.dstDataNodes[index]
		if node.IsActive && node.UsageRatio < 0.8 && !utils.Contains(dpInfo.Hosts, node.Addr) {
			break
		}
		offset++
	}
	if offset >= len(zoneCtrl.dstDataNodes) {
		return nil, ErrNoSuitableDstNode
	}
	zoneCtrl.dstIndex++
	zoneCtrl.dstIndex = zoneCtrl.dstIndex % len(zoneCtrl.dstDataNodes)
	if node != nil {
		log.LogInfof("selectDstNode dstIndex:%v nodeAddr:%v", zoneCtrl.dstIndex, node.Addr)
	}
	return node, nil
}

func (zoneCtrl *ZoneReBalanceController) isInRecoveringDPsMoreThanMaxBatchCount(maxBatchCount int) (inRecoveringDPMap map[uint64]int, err error) {
	inRecoveringDPMap, err = zoneCtrl.getInRecoveringDPMapIgnoreMig()
	if err != nil {
		return
	}
	if len(inRecoveringDPMap) >= maxBatchCount {
		err = fmt.Errorf("inRecoveringDPCount:%v more than maxBatchCount:%v", len(inRecoveringDPMap), maxBatchCount)
	}
	return
}

func (zoneCtrl *ZoneReBalanceController) getInRecoveringDPMapIgnoreMig() (inRecoveringDPMap map[uint64]int, err error) {
	clusterView, err := zoneCtrl.AdminAPI().GetCluster()
	if err != nil {
		return
	}
	inRecoveringDPMap = make(map[uint64]int)
	for _, badPartitionViews := range clusterView.BadPartitionIDs {
		inRecoveringDPMap[badPartitionViews.PartitionID]++
	}
	delete(inRecoveringDPMap, 0)
	return
}

// 1. 判断迁移过程是否终止
// 2. 判断是否超过最大并发量
// 3. 选择一个可迁移的DP
// 4. 选择一个用于迁入的DataNode
// 5. 执行迁移
func (zoneCtrl *ZoneReBalanceController) doDataReBalance() {
	var isManualStop bool
	defer func() {
		zoneCtrl.mutex.Lock()
		if !isManualStop {
			zoneCtrl.status = StatusStop
		}
		err := zoneCtrl.rw.stopRebalanced(zoneCtrl.Id, isManualStop)
		if err != nil {
			log.LogErrorf("stop rebalanced cluster:%v zoneName：%v  taskID: %v err:%v", zoneCtrl.cluster, zoneCtrl.zoneName, zoneCtrl.Id, err)
		}
		log.LogInfof("doDataReBalance: do cancel, taskID:%v", zoneCtrl.Id)
		zoneCtrl.cancel()
		zoneCtrl.mutex.Unlock()
	}()

	for _, srcNode := range zoneCtrl.srcDataNodes {
		err := srcNode.updateDataNode()
		if err != nil {
			log.LogErrorf("update dataNode error Zone: %v DataNode: %v %v", zoneCtrl.zoneName, srcNode.Addr, err.Error())
		}
		for srcNode.NeedReBalance(zoneCtrl.goalRatio) {
			select {
			case <-zoneCtrl.ctx.Done():
				isManualStop = true
				log.LogInfof("stop reBalance cluster:%v zoneName:%v", zoneCtrl.cluster, zoneCtrl.zoneName)
				return
			default:
			}

			// 检查最大并发量限制
			inRecoveringDPMap, err := zoneCtrl.isInRecoveringDPsMoreThanMaxBatchCount(zoneCtrl.clusterMaxBatchCount)
			if err != nil {
				log.LogWarnf("ScheduleToMigHighRatioDiskDataPartition err:%v", err.Error())
				time.Sleep(defaultInterval)
				continue
			}
			canBeMigCount := zoneCtrl.clusterMaxBatchCount - len(inRecoveringDPMap)
			log.LogInfof("ScheduleToMigHighRatioDiskDataPartition canBeMigCount:%v", canBeMigCount)
			err = zoneCtrl.createMigrateDp(srcNode, canBeMigCount)
			if err != nil {
				break
			}
			time.Sleep(defaultInterval)
		}
		srcNode.isFinished = true
		log.LogInfof("doReBalance cluster:%v zoneName:%v srcDataNode:%v finished", zoneCtrl.cluster, zoneCtrl.zoneName, srcNode.Addr)
	}
}

func (zoneCtrl *ZoneReBalanceController) createMigrateDp(srcNode *DNReBalanceController, canBeMigCount int) error {
	for canBeMigCount > 0 {
		// 选择srcNode中的一个可用dp
		disk, dpInfo, err := srcNode.SelectDP(zoneCtrl.goalRatio)
		if err != nil {
			// 当前srcNode已经没有可选的dp，break
			log.LogWarnf("dataNode select dp error Zone: %v DataNode: %v %v", zoneCtrl.zoneName, srcNode.Addr, err.Error())
			return err
		}

		// 根据选择的dp指定一个目标机器
		dstNode, err := zoneCtrl.selectDstNode(dpInfo)
		if err != nil {
			// 根据当前选定的dp找不到合适的dstNode，继续下一轮循环，选择新的dp
			log.LogWarnf("select dstNode error Zone: %v DataNode: %v %v", zoneCtrl.zoneName, srcNode.Addr, err.Error())
			continue
		}
		canBeMigCount--
		// 执行迁移
		err = zoneCtrl.doMigrate(disk, dpInfo, srcNode, dstNode)
		if err != nil {
			log.LogErrorf("migrate dp %v error Zone: %v from %v to %v %v", dpInfo.PartitionID, zoneCtrl.zoneName, srcNode.Addr, dstNode.Addr, err.Error())
			continue
		}
	}
	return nil
}

// 执行迁移
func (zoneCtrl *ZoneReBalanceController) doMigrate(disk *Disk, dpInfo *proto.DataPartitionInfo, srcNode *DNReBalanceController, dstNode *proto.DataNodeInfo) error {
	if dstNode == nil {
		return fmt.Errorf("doMigrate dpId:%v dstNode is nil", dpInfo.PartitionID)
	}
	err := zoneCtrl.AdminAPI().DecommissionDataPartition(dpInfo.PartitionID, srcNode.Addr, dstNode.Addr)
	if err != nil {
		return err
	}
	oldUsage, oldDiskUsage := srcNode.Usage(), disk.Usage()
	srcNode.UpdateMigratedDP(disk, dpInfo)
	newUsage, newDiskUsage := srcNode.Usage(), disk.Usage()
	err = zoneCtrl.rw.PutMigrateInfoToDB(&MigrateRecordTable{
		ClusterName:  zoneCtrl.cluster,
		ZoneName:     zoneCtrl.zoneName,
		RType:        RebalanceData,
		VolName:      dpInfo.VolName,
		PartitionID:  dpInfo.PartitionID,
		SrcAddr:      srcNode.Addr,
		SrcDisk:      disk.path,
		DstAddr:      dstNode.Addr,
		OldUsage:     oldUsage,
		OldDiskUsage: oldDiskUsage,
		NewUsage:     newUsage,
		NewDiskUsage: newDiskUsage,
		TaskId:       zoneCtrl.Id,
		TaskType:     zoneCtrl.taskType,
	})
	if err != nil {
		return err
	}
	log.LogInfof("DecommissionDataPartition host:%v zoneName:%v dpId:%v srcNode:%v dstNode:%v", zoneCtrl.cluster, zoneCtrl.zoneName, dpInfo.PartitionID, srcNode.Addr, dstNode.Addr)
	return nil
}

func (zoneCtrl *ZoneReBalanceController) refreshDstDataNodes() {
	ticker := time.NewTicker(time.Minute * 2)
	defer func() {
		ticker.Stop()
	}()
	for {
		log.LogDebugf("[refreshDstNodes] cluster:%v zoneName:%v", zoneCtrl.cluster, zoneCtrl.zoneName)
		select {
		case <-ticker.C:
			for _, node := range zoneCtrl.dstDataNodes {
				dataNode, err := zoneCtrl.MasterClient.NodeAPI().GetDataNode(node.Addr)
				if err != nil {
					log.LogErrorf("[refreshDstNodes] taskId:%v cluster:%v zoneName:%v datanodeAddr:%v", zoneCtrl.Id, zoneCtrl.cluster, zoneCtrl.zoneName, node.Addr)
					continue
				}
				*node = *dataNode
			}
		case <-zoneCtrl.ctx.Done():
			log.LogInfof("[refreshDstNodes] stopRefresh taskId:%v cluster:%v zoneName:%v", zoneCtrl.Id, zoneCtrl.cluster, zoneCtrl.zoneName)
			return
		}
	}
}

func (zoneCtrl *ZoneReBalanceController) refreshSrcDataNodes() {
	ticker := time.NewTicker(time.Minute * 2)
	defer func() {
		ticker.Stop()
	}()
	for {
		log.LogDebugf("[refreshSrcNodes] cluster:%v zoneName:%v", zoneCtrl.cluster, zoneCtrl.zoneName)
		select {
		case <-ticker.C:
			for _, node := range zoneCtrl.srcDataNodes {
				dataNode, err := zoneCtrl.MasterClient.NodeAPI().GetDataNode(node.Addr)
				if err != nil {
					log.LogErrorf("[refreshDstNodes] taskId:%v cluster:%v zoneName:%v datanodeAddr:%v", zoneCtrl.Id, zoneCtrl.cluster, zoneCtrl.zoneName, node.Addr)
					continue
				}
				convertActualUsageRatio(dataNode)
				(*node).DataNodeInfo = *dataNode
			}
		case <-zoneCtrl.ctx.Done():
			log.LogInfof("[refreshSrcNodes] stopRefresh taskId:%v cluster:%v zoneName:%v", zoneCtrl.Id, zoneCtrl.cluster, zoneCtrl.zoneName)
			return
		}
	}
}

func (zoneCtrl *ZoneReBalanceController) updateMetaNodes() (err error) {
	zoneCtrl.dstMetaNodes = make([]*proto.MetaNodeInfo, 0)
	zoneCtrl.srcMetaNodes = make([]*MetaNodeReBalanceController, 0)

	var getMetaNodefunc = func(nodeList []string) map[string]*proto.MetaNodeInfo {
		nodesInfo := make(map[string]*proto.MetaNodeInfo)
		var (
			wg   sync.WaitGroup
			lock sync.Mutex
		)
		for _, addr := range nodeList {
			wg.Add(1)
			go func(addr string) {
				defer wg.Done()
				nodeInfo, errForGet := zoneCtrl.NodeAPI().GetMetaNode(addr)
				if errForGet != nil {
					log.LogErrorf("GetMetaNode failed, taskID[%v] node: %s, err :%v", zoneCtrl.Id, addr, errForGet)
					return
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
		var metaNodesAddrs []string
		if metaNodesAddrs, err = zoneCtrl.getZoneMetaNodes(); err != nil {
			log.LogErrorf("getZoneMetaNodes failed, taskID[%v] err(%v)", zoneCtrl.Id, err)
			return
		}
		metaNodes := getMetaNodefunc(metaNodesAddrs)
		zoneCtrl.updateDstMetaNodes(metaNodes)
		zoneCtrl.updateSrcMetaNodes(metaNodes)
	}
	return
}

func (zoneCtrl *ZoneReBalanceController) updateSrcMetaNodes(metaNodesInfo map[string]*proto.MetaNodeInfo) {
	for _, nodeInfo := range metaNodesInfo {
		if nodeInfo.Ratio < zoneCtrl.highRatio {
			continue
		}

		log.LogInfof("nodeAddr: %s, usedRatio: %v", nodeInfo.Addr, nodeInfo.Ratio)
		nodeReBalanceCtrl := NewMetaNodeReBalanceController(nodeInfo, zoneCtrl.MasterClient, zoneCtrl)
		zoneCtrl.srcMetaNodes = append(zoneCtrl.srcMetaNodes, nodeReBalanceCtrl)
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
			if nodeInfo.Ratio <= zoneCtrl.lowRatio {
				zoneCtrl.dstMetaNodes = append(zoneCtrl.dstMetaNodes, nodeInfo)
			}
			log.LogInfof("updateDstNodes dstNode:%v usageRatio:%v lowRatio:%v compare:%v", nodeInfo.Addr, nodeInfo.Ratio, zoneCtrl.lowRatio, nodeInfo.Ratio < zoneCtrl.lowRatio)
		}
	}
	sort.Slice(zoneCtrl.dstMetaNodes, func(i, j int) bool {
		return zoneCtrl.dstMetaNodes[i].Ratio < zoneCtrl.dstMetaNodes[j].Ratio
	})
}

func (zoneCtrl *ZoneReBalanceController) getZoneMetaNodes() (metaNodeAddrs []string, err error) {
	topoView, err := zoneCtrl.AdminAPI().GetTopology()
	if err != nil {
		return
	}

	for _, zoneInfo := range topoView.Zones {
		if zoneInfo.Name != zoneCtrl.zoneName {
			continue
		}

		metaNodeAddrs = make([]string, 0)
		for _, nodeSet := range zoneInfo.NodeSet {
			for _, node := range nodeSet.MetaNodes {
				metaNodeAddrs = append(metaNodeAddrs, node.Addr)
			}
		}
		break
	}
	return
}

func (zoneCtrl *ZoneReBalanceController) doMetaReBalance() {
	var isManualStop bool
	defer func() {
		zoneCtrl.mutex.Lock()
		if !isManualStop {
			zoneCtrl.status = StatusStop
		}
		if err := zoneCtrl.rw.stopRebalanced(zoneCtrl.Id, isManualStop); err != nil {
			log.LogErrorf("stop task failed, task info: %v, err: %v", zoneCtrl.reBalanceTaskInfo(), err)
		}
		log.LogInfof("doMetaReBalance: do cancel, taskID:%v", zoneCtrl.Id)
		zoneCtrl.cancel()
		zoneCtrl.mutex.Unlock()
	}()

	for _, srcMetaNode := range zoneCtrl.srcMetaNodes {
		log.LogInfof("start do rebalance, task info: %v, node: %v", zoneCtrl.reBalanceTaskInfo(), srcMetaNode.nodeInfo.Addr)

		updateMetaNodeFailedCount := 0
		srcMetaNode.alreadyMigrateFinishedPartitions = make(map[uint64]bool)
		// goal=0表示全部迁出
		// todo：没有mp但还使用率一直不下降
		for srcMetaNode.nodeInfo.Ratio > zoneCtrl.goalRatio {
			select {
			case <-zoneCtrl.ctx.Done():
				isManualStop = true
				log.LogInfof("stop reBalance task: %v", zoneCtrl.reBalanceTaskInfo())
				return
			default:
			}

			if err := srcMetaNode.updateMetaNodeInfo(); err != nil {
				updateMetaNodeFailedCount++
				if updateMetaNodeFailedCount > 10 {
					log.LogErrorf("update meta node info failed count reach max retry count, task info: %v, src node: %v, err: %v",
						zoneCtrl.reBalanceTaskInfo(), srcMetaNode.nodeInfo.Addr, err)
					break
				}
				time.Sleep(time.Second * 30)
				continue
			}

			if err := srcMetaNode.doMigrate(); err != nil {
				log.LogInfof("migrate failed, task info: %v, node: %v, err: %v", zoneCtrl.reBalanceTaskInfo(),
					srcMetaNode.nodeInfo.Addr, err)
				return
			}
			time.Sleep(time.Second * 15)
		}
		srcMetaNode.isFinished = true
		log.LogInfof("doReBalance cluster:%v zoneName:%v srcMetaNode:%v finished", zoneCtrl.cluster, zoneCtrl.zoneName, srcMetaNode.nodeInfo.Addr)
		time.Sleep(time.Minute * 5)
	}
}

func (zoneCtrl *ZoneReBalanceController) refreshSrcMetaNodes() {
	ticker := time.NewTicker(time.Minute * 2)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			for index := 0; index < len(zoneCtrl.srcMetaNodes); index++ {
				err := zoneCtrl.srcMetaNodes[index].updateMetaNodeInfo()
				if err != nil {
					log.LogErrorf("refresh src meta node info failed, rebalance task: %v, refresh node: %s, err: %v",
						zoneCtrl.reBalanceTaskInfo(), zoneCtrl.srcMetaNodes[index].nodeInfo.Addr, err)
				}
			}
		case <-zoneCtrl.ctx.Done():
			log.LogInfof("rebalance task finished, task info: %v", zoneCtrl.reBalanceTaskInfo())
			return
		}
	}
}

func (zoneCtrl *ZoneReBalanceController) refreshDstMetaNodes() {
	ticker := time.NewTicker(time.Minute * 2)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			for index := 0; index < len(zoneCtrl.dstMetaNodes); index++ {
				nodeInfo, err := zoneCtrl.NodeAPI().GetMetaNode(zoneCtrl.dstMetaNodes[index].Addr)
				if err != nil {
					log.LogErrorf("refresh dst meta node info failed, rebalance task: %v, refresh node: %s, err: %v",
						zoneCtrl.reBalanceTaskInfo(), zoneCtrl.dstMetaNodes[index].Addr, err)
					continue
				}
				zoneCtrl.dstMetaNodes[index] = nodeInfo
			}
		case <-zoneCtrl.ctx.Done():
			log.LogInfof("rebalance task finished, task info: %v", zoneCtrl.reBalanceTaskInfo())
			return
		}
	}
}
