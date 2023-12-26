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

type Status int

// ZoneReBalanceController 用于控制每个Zone内数据的迁移
type ZoneReBalanceController struct {
	*master.MasterClient

	Id        uint64
	cluster   string
	zoneName  string
	srcNodes  []*DNReBalanceController // 待迁移的源机器
	dstNodes  []*proto.DataNodeInfo    // 目标机器
	highRatio float64                  // 源机器的阈值
	lowRatio  float64                  // 目标机器的阈值
	goalRatio float64                  // 迁移需达到的阈值
	ctx       context.Context          // context控制ReBalance程序终止
	cancel    context.CancelFunc
	status    Status
	mutex     sync.Mutex

	rw *ReBalanceWorker

	minWritableDPNum     int // 最小可写DP限制
	clusterMaxBatchCount int // 最大并发量
	dstIndex             int
	migrateLimitPerDisk  int // 每块磁盘每轮迁移dp数量上限，默认-1无上限

	createdAt time.Time
	updatedAt time.Time
}

func NewZoneReBalanceController(id uint64, clusterHost, zoneName string, highRatio, lowRatio, goalRatio float64, rw *ReBalanceWorker) (*ZoneReBalanceController, error) {
	if err := checkRatio(highRatio, lowRatio, goalRatio); err != nil {
		return nil, err
	}
	zoneCtrl := &ZoneReBalanceController{
		Id:                   id,
		cluster:              clusterHost,
		zoneName:             zoneName,
		highRatio:            highRatio,
		lowRatio:             lowRatio,
		goalRatio:            goalRatio,
		status:               StatusStop,
		minWritableDPNum:     defaultMinWritableDPNum,
		clusterMaxBatchCount: defaultClusterMaxBatchCount,
		migrateLimitPerDisk:  -1,
		rw:                   rw,
	}
	zoneCtrl.MasterClient = master.NewMasterClient([]string{zoneCtrl.cluster}, false)

	//if err := zoneCtrl.updateDataNodes(); err != nil {
	//	return zoneCtrl, err
	//}
	return zoneCtrl, nil
}

func (zoneCtrl *ZoneReBalanceController) SetCreatedUpdatedAt(createdAt, updatedAt time.Time) {
	zoneCtrl.createdAt = createdAt
	zoneCtrl.updatedAt = updatedAt
}

func (zoneCtrl *ZoneReBalanceController) UpdateRatio(highRatio, lowRatio, goalRatio float64) error {
	if err := checkRatio(highRatio, lowRatio, goalRatio); err != nil {
		return err
	}
	zoneCtrl.highRatio = highRatio
	zoneCtrl.lowRatio = lowRatio
	zoneCtrl.goalRatio = goalRatio
	return nil
}

// ReBalanceStart 开启ReBalance程序
func (zoneCtrl *ZoneReBalanceController) ReBalanceStart() error {
	zoneCtrl.mutex.Lock()
	defer zoneCtrl.mutex.Unlock()

	// 判断当前迁移状态
	if zoneCtrl.status != StatusStop {
		return fmt.Errorf("%v: expected Stop but found %v", ErrWrongStatus, getStatusStr(zoneCtrl.status))
	}

	// 更新源节点列表和目标节点列表
	err := zoneCtrl.updateDataNodes()
	if err != nil {
		return err
	}

	zoneCtrl.status = StatusRunning
	zoneCtrl.ctx, zoneCtrl.cancel = context.WithCancel(context.Background())

	log.LogInfof("start rebalance zone: %v", zoneCtrl.zoneName)
	go zoneCtrl.doReBalance()
	go zoneCtrl.refreshSrcNodes()
	go zoneCtrl.refreshDstNodes()
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

// SetMinWritableDPNum 设置最小可写DP限制
func (zoneCtrl *ZoneReBalanceController) SetMinWritableDPNum(minWritableDPNum int) {
	if minWritableDPNum <= 0 {
		return
	}
	zoneCtrl.minWritableDPNum = minWritableDPNum
}

// SetClusterMaxBatchCount 设置最大并发量
func (zoneCtrl *ZoneReBalanceController) SetClusterMaxBatchCount(clusterMaxBatchCount int) {
	zoneCtrl.clusterMaxBatchCount = clusterMaxBatchCount
}

func (zoneCtrl *ZoneReBalanceController) SetMigrateLimitPerDisk(limit int) {
	zoneCtrl.migrateLimitPerDisk = limit
	for _, node := range zoneCtrl.srcNodes {
		node.SetMigrateLimitPerDisk(limit)
	}
}

// 读取当前zone中的dataNode列表，根据使用率划分为srcNode和dstNode
// srcNode: 使用率高的源机器
// dstNode: 使用率低可作为迁移目标的目标机器
func (zoneCtrl *ZoneReBalanceController) updateDataNodes() error {
	zoneCtrl.srcNodes = make([]*DNReBalanceController, 0)
	zoneCtrl.dstNodes = make([]*proto.DataNodeInfo, 0)
	dataNodes := make(map[string]*proto.DataNodeInfo)
	lock := sync.Mutex{}

	dataNodeAddrs, err := getZoneDataNodesByClient(zoneCtrl.MasterClient, zoneCtrl.zoneName)
	if err != nil {
		return err
	}

	wg := sync.WaitGroup{}
	wg.Add(len(dataNodeAddrs))

	for _, addr := range dataNodeAddrs {
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
	zoneCtrl.updateDstNodes(dataNodes)
	err = zoneCtrl.updateSrcNodes(dataNodes)
	if err != nil {
		return err
	}
	return nil
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
		srcNode, err := NewDNReBalanceController(*dataNode, zoneCtrl.MasterClient, zoneCtrl.cluster, zoneCtrl.minWritableDPNum, zoneCtrl.clusterMaxBatchCount, zoneCtrl.migrateLimitPerDisk)
		if err != nil {
			log.LogErrorf("newDataNode error Zone: %v DataNode: %v %v", zoneCtrl.zoneName, addr, err.Error())
			continue
		}
		zoneCtrl.srcNodes = append(zoneCtrl.srcNodes, srcNode)
	}
	sort.Slice(zoneCtrl.srcNodes, func(i, j int) bool {
		return zoneCtrl.srcNodes[i].UsageRatio > zoneCtrl.srcNodes[j].UsageRatio
	})
	return nil
}

func (zoneCtrl *ZoneReBalanceController) updateDstNodes(dataNodes map[string]*proto.DataNodeInfo) {
	for _, dataNode := range dataNodes {
		if dataNode.UsageRatio < zoneCtrl.lowRatio {
			zoneCtrl.dstNodes = append(zoneCtrl.dstNodes, dataNode)
		}
		log.LogInfof("updateDstNodes dstNode:%v usageRatio:%v lowRatio:%v compare:%v", dataNode.Addr, dataNode.UsageRatio, zoneCtrl.lowRatio, dataNode.UsageRatio < zoneCtrl.lowRatio)
	}
	return
}

// 1. 从当前index开始往后遍历， 找到第一个符合条件的dstNode
// 2. 将index+1，避免重复选择一个dstNode
func (zoneCtrl *ZoneReBalanceController) selectDstNode(dpInfo *proto.DataPartitionInfo) (*proto.DataNodeInfo, error) {
	var node *proto.DataNodeInfo
	offset := 0
	for offset < len(zoneCtrl.dstNodes) {
		index := (zoneCtrl.dstIndex + offset) % len(zoneCtrl.dstNodes)
		node = zoneCtrl.dstNodes[index]
		if node.IsActive && node.UsageRatio < 0.8 && !utils.Contains(dpInfo.Hosts, node.Addr) {
			break
		}
		offset++
	}
	if offset >= len(zoneCtrl.dstNodes) {
		return nil, ErrNoSuitableDstNode
	}
	zoneCtrl.dstIndex++
	zoneCtrl.dstIndex = zoneCtrl.dstIndex % len(zoneCtrl.dstNodes)
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
func (zoneCtrl *ZoneReBalanceController) doReBalance() {
	defer func() {
		zoneCtrl.mutex.Lock()
		zoneCtrl.status = StatusStop
		err := zoneCtrl.rw.stopRebalanced(zoneCtrl.cluster, zoneCtrl.zoneName)
		if err != nil {
			log.LogErrorf("stop rebalanced cluster:%v zoneName：%v err:%v", zoneCtrl.cluster, zoneCtrl.zoneName, err)
		}
		zoneCtrl.mutex.Unlock()
	}()
	for _, srcNode := range zoneCtrl.srcNodes {
		err := srcNode.updateDataNode()
		if err != nil {
			log.LogErrorf("update dataNode error Zone: %v DataNode: %v %v", zoneCtrl.zoneName, srcNode.Addr, err.Error())
		}
		for srcNode.NeedReBalance(zoneCtrl.goalRatio) {
			select {
			case <-zoneCtrl.ctx.Done():
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
	})
	if err != nil {
		return err
	}
	log.LogInfof("DecommissionDataPartition host:%v zoneName:%v dpId:%v srcNode:%v dstNode:%v", zoneCtrl.cluster, zoneCtrl.zoneName, dpInfo.PartitionID, srcNode.Addr, dstNode.Addr)
	return nil
}

func (zoneCtrl *ZoneReBalanceController) refreshDstNodes() {
	ticker := time.NewTicker(time.Minute * 2)
	defer func() {
		ticker.Stop()
	}()
	for {
		log.LogDebugf("[refreshDstNodes] cluster:%v zoneName:%v", zoneCtrl.cluster, zoneCtrl.zoneName)
		select {
		case <-ticker.C:
			for _, node := range zoneCtrl.dstNodes {
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

func (zoneCtrl *ZoneReBalanceController) refreshSrcNodes() {
	ticker := time.NewTicker(time.Minute * 2)
	defer func() {
		ticker.Stop()
	}()
	for {
		log.LogDebugf("[refreshSrcNodes] cluster:%v zoneName:%v", zoneCtrl.cluster, zoneCtrl.zoneName)
		select {
		case <-ticker.C:
			for _, node := range zoneCtrl.srcNodes {
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
