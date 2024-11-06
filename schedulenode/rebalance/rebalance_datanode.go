package rebalance

import (
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/http_client"
	"github.com/cubefs/cubefs/util/log"
	"gorm.io/gorm/utils"
	"time"
)

// DNReBalanceController 用于控制每台机器(DataNode)上数据的迁移
type DNReBalanceController struct {
	zoneCtrl   *ZoneReBalanceController
	nodeInfo   *DataNodeStatsWithAddr
	dataClient *http_client.DataClient

	disks        map[string]*Disk
	migratedDp   map[uint64]*proto.DataPartitionInfo // 已执行迁移程序的dp
	migratedSize uint64                              // 迁移dp的总大小
	masterAddr   string

	minWritableDPNum    int // 最小可写dp限制
	migrateLimitPerDisk int
	maxMigrateDpCnt     int
	hasMigrateDpCnt     int
	isFinished          bool
}

func NewDNReBalanceController(zoneCtrl *ZoneReBalanceController, info *DataNodeStatsWithAddr, masterAddr string,
	minWritableDPNum, migrateLimitPerDisk int) (*DNReBalanceController, error) {
	dataNodeProfPort := zoneCtrl.rw.getDataNodePProfPort(masterAddr)
	dataClient := getDataHttpClient(info.Addr, dataNodeProfPort)
	dnCtrl := &DNReBalanceController{
		zoneCtrl:            zoneCtrl,
		nodeInfo:            info,
		dataClient:          dataClient,
		masterAddr:          masterAddr,
		minWritableDPNum:    minWritableDPNum,
		migrateLimitPerDisk: migrateLimitPerDisk,
	}
	if zoneCtrl.outMigRatio > 0 {
		dnCtrl.maxMigrateDpCnt = int(float64(len(dnCtrl.nodeInfo.PartitionReports))*zoneCtrl.outMigRatio)
	} else {
		dnCtrl.maxMigrateDpCnt = len(dnCtrl.nodeInfo.PartitionReports)
	}

	err := dnCtrl.updateDisks()
	if err != nil {
		return nil, err
	}

	err = dnCtrl.updateDps()
	if err != nil {
		return nil, err
	}
	dnCtrl.migratedDp = make(map[uint64]*proto.DataPartitionInfo)
	return dnCtrl, nil
}

// NeedReBalance 判断当前DataNode是否需要迁移，goalRatio为0，表示一直迁移
func (dnCtrl *DNReBalanceController) NeedReBalance(goalRatio float64) bool {
	usageRatioNeedRebalance := dnCtrl.Usage() > goalRatio
	dpMigCntNeedRebalance := dnCtrl.hasMigrateDpCnt < dnCtrl.maxMigrateDpCnt
	log.LogInfof("dataNode needReBalance addr(%v) usageRatioNeedRebalance(%v) usage(%v) goalRatio(%v), dpMigCntNeedRebalance(%v) hasMigrateDpCnt(%v) maxMigrateDpCnt(%v)",
		dnCtrl.nodeInfo.Addr, usageRatioNeedRebalance, dnCtrl.Usage(), goalRatio, dpMigCntNeedRebalance, dnCtrl.hasMigrateDpCnt, dnCtrl.maxMigrateDpCnt)
	return usageRatioNeedRebalance && dpMigCntNeedRebalance
}

func (dnCtrl *DNReBalanceController) selectDP(goalRatio float64) (*Disk, *proto.DataPartitionInfo, error) {
	if dnCtrl.Usage() < goalRatio {
		return nil, nil, ErrLessThanUsageRatio
	}
	for _, disk := range dnCtrl.disks {
		if disk.total == 0 {
			continue
		}
		if disk.Usage() < goalRatio {
			continue
		}
		dp, err := disk.SelectDP()
		if err != nil {
			log.LogWarnf("disk select dp error dataNode: %v disk: %v %v", dnCtrl.nodeInfo.Addr, disk.path, err.Error())
			continue
		}
		return disk, dp, nil
	}
	return nil, nil, ErrNoSuitablePartition
}

// 1. 从当前index开始往后遍历， 找到第一个符合条件的dstNode
// 2. 将index+1，避免重复选择一个dstNode
func (dnCtrl *DNReBalanceController) selectDstNode(dpInfo *proto.DataPartitionInfo) (*DataNodeStatsWithAddr, error) {
	var node *DataNodeStatsWithAddr
	offset := 0
	for offset < len(dnCtrl.zoneCtrl.dstDataNodes) {
		index := (dnCtrl.zoneCtrl.dstIndex + offset) % len(dnCtrl.zoneCtrl.dstDataNodes)
		node = dnCtrl.zoneCtrl.dstDataNodes[index]
		var goalRatio = dnCtrl.zoneCtrl.goalRatio
		if dnCtrl.zoneCtrl.goalRatio == 0 {
			goalRatio = 0.8
		}
		if node.UsageRatio < goalRatio && !utils.Contains(dpInfo.Hosts, node.Addr) {
			break
		}
		offset++
	}
	if offset >= len(dnCtrl.zoneCtrl.dstDataNodes) {
		log.LogWarnf("select dstNode error Zone: %v DataNode: %v %v", dnCtrl.zoneCtrl.zoneName, dnCtrl.nodeInfo.Addr, ErrNoSuitableDstNode)
		return nil, ErrNoSuitableDstNode
	}
	dnCtrl.zoneCtrl.dstIndex++
	dnCtrl.zoneCtrl.dstIndex = dnCtrl.zoneCtrl.dstIndex % len(dnCtrl.zoneCtrl.dstDataNodes)
	return node, nil
}

func (dnCtrl *DNReBalanceController) UpdateMigratedDP(disk *Disk, dpInfo *proto.DataPartitionInfo) {
	disk.UpdateMigratedDPSize(dpInfo)
	dnCtrl.migratedDp[dpInfo.PartitionID] = dpInfo
	for _, replica := range dpInfo.Replicas {
		if replica.Addr == dnCtrl.nodeInfo.Addr {
			dnCtrl.migratedSize += replica.Used
			break
		}
	}
}

// Usage 当前DataNode出去已迁移dp后的实际使用率（可能会多减，实际阈值没下降那么多）
func (dnCtrl *DNReBalanceController) Usage() float64 {
	log.LogDebugf("dnCtrl.Usage: used(%v) migrated(%v) total(%v)", dnCtrl.nodeInfo.Used, dnCtrl.migratedSize, dnCtrl.nodeInfo.Total)
	return float64(dnCtrl.nodeInfo.Used-dnCtrl.migratedSize) / float64(dnCtrl.nodeInfo.Total)
}

func (dnCtrl *DNReBalanceController) SetMigrateLimitPerDisk(limit int) {
	dnCtrl.migrateLimitPerDisk = limit
	for _, disk := range dnCtrl.disks {
		disk.SetMigrateLimit(limit)
	}
}

func (dnCtrl *DNReBalanceController) doMigrate(clusterDpCurrency int) (err error) {
	for clusterDpCurrency > 0 {
		select {
		case <-dnCtrl.zoneCtrl.ctx.Done():
			dnCtrl.zoneCtrl.SetIsManualStop(true)
			log.LogInfof("doMigrate stop: taskID(%v) cluster(%v) zone(%v) srcDataNode(%v)", dnCtrl.zoneCtrl.Id, dnCtrl.zoneCtrl.cluster, dnCtrl.zoneCtrl.zoneName, dnCtrl.nodeInfo.Addr)
			return
		default:
		}
		var (
			disk *Disk
			dpInfo *proto.DataPartitionInfo
			dstNode *DataNodeStatsWithAddr
		)
		// 选择srcNode中的一个可用dp
		disk, dpInfo, err = dnCtrl.selectDP(dnCtrl.zoneCtrl.goalRatio)
		if err != nil {
			// 当前srcNode已经没有可选的dp，break
			log.LogWarnf("dataNode select dp error Zone: %v DataNode: %v %v", dnCtrl.zoneCtrl.zoneName, dnCtrl.nodeInfo.Addr, err.Error())
			return
		}

		// 根据选择的dp指定一个目标机器
		dstNode, err = dnCtrl.selectDstNode(dpInfo)
		if err != nil {
			// 根据当前选定的dp找不到合适的dstNode，继续下一轮循环，选择新的dp
			continue
		}
		clusterDpCurrency--
		dnCtrl.hasMigrateDpCnt++
		// 执行迁移
		dnCtrl.migrate(disk, dpInfo, dstNode)
	}
	return
}

// 执行迁移
func (dnCtrl *DNReBalanceController) migrate(disk *Disk, dp *proto.DataPartitionInfo, dstNode *DataNodeStatsWithAddr) {
	var err error
	defer func() {
		msg := fmt.Sprintf("migrate dp: taskID(%v) cluster(%v) vol(%v) dp(%v) src(%v) dest(%v)",
			dnCtrl.zoneCtrl.Id, dnCtrl.zoneCtrl.cluster, dp.VolName, dp.PartitionID, dnCtrl.nodeInfo.Addr, dstNode.Addr)
		if err == nil {
			log.LogInfof("%s", msg)
		} else {
			log.LogErrorf("%s, err(%v)", msg, err)
		}
	}()

	if dstNode == nil {
		err = fmt.Errorf("doMigrate dpId:%v dstNode is nil", dp.PartitionID)
		return
	}
	dnCtrl.zoneCtrl.RecordMigratePartition(dp.PartitionID)
	needDeleteReplica := DeleteFlagForThreeReplica
	release := isRelease(dnCtrl.zoneCtrl.cluster)
	// 迁移两副本
	if dp.ReplicaNum == 2 {
		if release {
			//先加副本
			err = dnCtrl.zoneCtrl.releaseClient.AddDataReplica(dp.PartitionID, dstNode.Addr)
		} else {
			err = dnCtrl.zoneCtrl.masterClient.AdminAPI().AddDataReplica(dp.PartitionID, dstNode.Addr, proto.DefaultAddReplicaType)
		}
		// 添加到表中，后台任务去删副本
		needDeleteReplica = DeleteFlagForTwoReplica
	} else {
		if release {
			err = dnCtrl.zoneCtrl.releaseClient.DataPartitionOffline(dp.PartitionID, dnCtrl.nodeInfo.Addr, dstNode.Addr, dp.VolName)
		} else {
			err = dnCtrl.zoneCtrl.masterClient.AdminAPI().DecommissionDataPartition(dp.PartitionID, dnCtrl.nodeInfo.Addr, dstNode.Addr)
		}
	}
	if err != nil {
		return
	}
	oldUsage, oldDiskUsage := dnCtrl.Usage(), disk.Usage()
	dnCtrl.UpdateMigratedDP(disk, dp)
	newUsage, newDiskUsage := dnCtrl.Usage(), disk.Usage()

	for i := 0; i < 10; i++ {
		err = dnCtrl.zoneCtrl.rw.PutMigrateInfoToDB(&MigrateRecordTable{
			ClusterName:  dnCtrl.zoneCtrl.cluster,
			ZoneName:     dnCtrl.zoneCtrl.zoneName,
			RType:        RebalanceData,
			VolName:      dp.VolName,
			PartitionID:  dp.PartitionID,
			SrcAddr:      dnCtrl.nodeInfo.Addr,
			SrcDisk:      disk.path,
			DstAddr:      dstNode.Addr,
			OldUsage:     oldUsage,
			OldDiskUsage: oldDiskUsage,
			NewUsage:     newUsage,
			NewDiskUsage: newDiskUsage,
			TaskId:       dnCtrl.zoneCtrl.Id,
			TaskType:     dnCtrl.zoneCtrl.taskType,
			NeedDelete:   needDeleteReplica,
			CreatedAt:    time.Now(),
			UpdateAt:     time.Now(),
		})
		if err == nil {
			break
		}
	}
}

// 更新当前dataNode的信息
func (dnCtrl *DNReBalanceController) updateDataNode() error {
	node, err := dnCtrl.zoneCtrl.getCommonDataNodeInfo(dnCtrl.nodeInfo.Addr)
	if err != nil {
		return err
	}
	newNode := NewDataNodeStatsWithAddr(node, dnCtrl.nodeInfo.Addr)
	dnCtrl.nodeInfo = newNode

	err = dnCtrl.updateDisks()
	if err != nil {
		return err
	}

	err = dnCtrl.updateDps()
	if err != nil {
		return err
	}
	return nil
}

// 更新dataNode的disk列表
func (dnCtrl *DNReBalanceController) updateDisks() error {
	dnCtrl.disks = make(map[string]*Disk)
	diskReport, err := dnCtrl.dataClient.GetDisks()
	if err != nil {
		return fmt.Errorf("get disks error zoneName: %v dataNode: %v", dnCtrl.nodeInfo.ZoneName, dnCtrl.nodeInfo.Addr)
	}
	for _, diskInfo := range diskReport.Disks {
		if diskInfo.Total == 0 {
			continue
		}
		disk := NewDiskReBalanceController(dnCtrl.zoneCtrl, dnCtrl.dataClient, diskInfo, dnCtrl.masterAddr, dnCtrl.nodeInfo.Addr, dnCtrl.minWritableDPNum, dnCtrl.migrateLimitPerDisk,
			dnCtrl.zoneCtrl.masterClient, dnCtrl.zoneCtrl.releaseClient)
		dnCtrl.disks[diskInfo.Path] = disk
	}
	return nil
}

// 更新dataNode的data Partition
func (dnCtrl *DNReBalanceController) updateDps() error {
	dnCtrl.migratedSize = 0

	//persistenceDataPartitionsMap := make(map[uint64]struct{})
	//for _, dpID := range dnCtrl.nodeInfo.PersistenceDataPartitions {
	//	persistenceDataPartitionsMap[dpID] = struct{}{}
	//}

	// 将dp绑定对应disk
	dpReportsMap := make(map[uint64]*proto.PartitionReport)
	for _, dpReport := range dnCtrl.nodeInfo.PartitionReports {
		//if _, ok := persistenceDataPartitionsMap[dpReport.PartitionID]; !ok {
		//	continue
		//}
		if disk, ok := dnCtrl.disks[dpReport.DiskPath]; ok {
			disk.AddDP(dpReport)
		}
		dpReportsMap[dpReport.PartitionID] = dpReport
	}

	// 更新上轮次迁移的dp列表
	for id, dpInfo := range dnCtrl.migratedDp {
		var dpReport *proto.PartitionReport
		var disk *Disk
		var ok bool

		// 上轮次迁移节点已下线，删除节点
		if dpReport, ok = dpReportsMap[id]; !ok {
			delete(dnCtrl.migratedDp, id)
			continue
		}
		if disk, ok = dnCtrl.disks[dpReport.DiskPath]; !ok {
			delete(dnCtrl.migratedDp, id)
			continue
		}

		for _, replica := range dpInfo.Replicas {
			if replica.Addr == dnCtrl.nodeInfo.Addr {
				dnCtrl.migratedSize += replica.Used
				break
			}
		}
		disk.UpdateMigratedDPSize(dpInfo)
	}
	return nil
}
