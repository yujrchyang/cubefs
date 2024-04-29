package rebalance

import (
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util/log"
	"gorm.io/gorm/utils"
)

type MetaNodeReBalanceController struct {
	nodeInfo   *proto.MetaNodeInfo
	mc         *master.MasterClient
	zoneCtrl   *ZoneReBalanceController
	isFinished bool // 是否迁移完成

	alreadyMigrateFinishedPartitions map[uint64]bool
}

func NewMetaNodeReBalanceController(nodeInfo *proto.MetaNodeInfo, mc *master.MasterClient, zoneCtrl *ZoneReBalanceController) *MetaNodeReBalanceController {
	return &MetaNodeReBalanceController{
		nodeInfo: nodeInfo,
		mc:       mc,
		zoneCtrl: zoneCtrl,
	}
}

func (mnReBalanceCtrl *MetaNodeReBalanceController) updateMetaNodeInfo() (err error) {
	var nodeInfo *proto.MetaNodeInfo
	nodeInfo, err = mnReBalanceCtrl.mc.NodeAPI().GetMetaNode(mnReBalanceCtrl.nodeInfo.Addr)
	if err != nil {
		log.LogErrorf("update meta node info failed, node: %v, err: %v", mnReBalanceCtrl.nodeInfo.Addr, err)
		return
	}
	mnReBalanceCtrl.nodeInfo = nodeInfo
	return
}

func (mnReBalanceCtrl *MetaNodeReBalanceController) migrate(migratePartitionInfo *proto.MetaPartitionInfo, dstMetaNodeAddr string) {
	log.LogInfof("start do migrate, task info: %v, migrate partition id: %v, src node: %v",
		mnReBalanceCtrl.zoneCtrl.reBalanceTaskInfo(), migratePartitionInfo.PartitionID, mnReBalanceCtrl.nodeInfo.Addr)
	var dstStoreMode = proto.StoreModeDef
	for _, replica := range migratePartitionInfo.Replicas {
		if replica.Addr == mnReBalanceCtrl.nodeInfo.Addr {
			dstStoreMode = replica.StoreMode
			break
		}
	}

	if dstStoreMode == proto.StoreModeDef {
		log.LogErrorf("get store mode failed, task info: %v, migrate partition id: %v, src node: %v",
			mnReBalanceCtrl.zoneCtrl.reBalanceTaskInfo(), migratePartitionInfo.PartitionID, mnReBalanceCtrl.nodeInfo.Addr)
		return
	}

	if err := mnReBalanceCtrl.mc.AdminAPI().DecommissionMetaPartition(migratePartitionInfo.PartitionID, mnReBalanceCtrl.nodeInfo.Addr,
		dstMetaNodeAddr, int(dstStoreMode)); err != nil {
		log.LogErrorf("migrate partition failed, task info: %v, migrate partition id: %v, src node: %v, dst node: %v, err: %v",
			mnReBalanceCtrl.zoneCtrl.reBalanceTaskInfo(), migratePartitionInfo.PartitionID, mnReBalanceCtrl.nodeInfo.Addr, dstMetaNodeAddr, err)
	}

	err := mnReBalanceCtrl.zoneCtrl.rw.PutMigrateInfoToDB(&MigrateRecordTable{
		ClusterName: mnReBalanceCtrl.zoneCtrl.cluster,
		ZoneName:    mnReBalanceCtrl.zoneCtrl.zoneName,
		RType:       RebalanceMeta,
		VolName:     migratePartitionInfo.VolName,
		PartitionID: migratePartitionInfo.PartitionID,
		SrcAddr:     mnReBalanceCtrl.nodeInfo.Addr,
		DstAddr:     dstMetaNodeAddr,
		TaskId:      mnReBalanceCtrl.zoneCtrl.Id,
	})
	if err != nil {
		log.LogErrorf("put migrate record to data base failed, task info: %v, migrate partition id: %v, src node: %v, dst node: %v, err: %v",
			mnReBalanceCtrl.zoneCtrl.reBalanceTaskInfo(), migratePartitionInfo.PartitionID, mnReBalanceCtrl.nodeInfo.Addr, dstMetaNodeAddr, err)
	}
	log.LogInfof("migrate finish, task info: %v, migrate partition id: %v, src node: %v",
		mnReBalanceCtrl.zoneCtrl.reBalanceTaskInfo(), migratePartitionInfo.PartitionID, mnReBalanceCtrl.nodeInfo.Addr)
}

func canBeSelectedForMigrate(dstMetaNodeInfo *proto.MetaNodeInfo, goalRatio float64, migratePartitionHosts []string,
	dstMetaNodePartitionMaxCount int) bool {
	log.LogDebugf("dst node: %s, isActive: %v, metaPartitionCount: %v, toBeOffline: %v, toBeMigrate: %v,"+
		" srcPartitionHost: %v, ratio: %v", dstMetaNodeInfo.Addr, dstMetaNodeInfo.IsActive, dstMetaNodeInfo.MetaPartitionCount,
		dstMetaNodeInfo.ToBeOffline, dstMetaNodeInfo.ToBeMigrated, migratePartitionHosts, dstMetaNodeInfo.Ratio)
	if dstMetaNodeInfo.IsActive &&
		dstMetaNodeInfo.MetaPartitionCount < dstMetaNodePartitionMaxCount &&
		!dstMetaNodeInfo.ToBeOffline &&
		!dstMetaNodeInfo.ToBeMigrated &&
		!utils.Contains(migratePartitionHosts, dstMetaNodeInfo.Addr) {
		if goalRatio > 0 {
			return dstMetaNodeInfo.Ratio < goalRatio
		} else {
			return dstMetaNodeInfo.Ratio < 0.7
		}
	}
	return false
}

func (mnReBalanceCtrl *MetaNodeReBalanceController) selectDstMetaNodes(mpID uint64, hosts []string) (dstMetaNodeAddr string, err error) {
	offset := 0
	for offset < len(mnReBalanceCtrl.zoneCtrl.dstMetaNodes) {
		dstMetaNodeIndex := mnReBalanceCtrl.zoneCtrl.dstIndex % len(mnReBalanceCtrl.zoneCtrl.dstMetaNodes)
		dstMetaNode := mnReBalanceCtrl.zoneCtrl.dstMetaNodes[dstMetaNodeIndex]
		if canBeSelectedForMigrate(dstMetaNode, mnReBalanceCtrl.zoneCtrl.goalRatio, hosts,
			mnReBalanceCtrl.zoneCtrl.dstMetaNodeMaxPartitionCount) {
			dstMetaNodeAddr = dstMetaNode.Addr
			return
		}

		mnReBalanceCtrl.zoneCtrl.dstIndex++
		offset++
	}
	errMsg := fmt.Sprintf("no available dst meta node for migrate")
	err = fmt.Errorf(errMsg)
	log.LogErrorf("select dst meta node failed, task info: %v, mp id: %v, err: %v", mnReBalanceCtrl.zoneCtrl.reBalanceTaskInfo(), mpID, err)
	return
}

func (mnReBalanceCtrl *MetaNodeReBalanceController) selectMigratePartition() (migratePartitionInfo *proto.MetaPartitionInfo, err error) {
	for _, mpID := range mnReBalanceCtrl.nodeInfo.PersistenceMetaPartitions {
		if _, ok := mnReBalanceCtrl.alreadyMigrateFinishedPartitions[mpID]; ok {
			continue
		}

		mnReBalanceCtrl.alreadyMigrateFinishedPartitions[mpID] = true
		migratePartitionInfo, err = mnReBalanceCtrl.mc.ClientAPI().GetMetaPartition(mpID, "")
		if err != nil {
			log.LogErrorf("get meta partition info failed, task info: %v, mp id: %v, err: %v",
				mnReBalanceCtrl.zoneCtrl.reBalanceTaskInfo(), mpID, err)
			migratePartitionInfo = nil
			err = nil
			continue
		}
		if migratePartitionInfo.IsRecover {
			log.LogErrorf("select recovering meta partition, continue, task info: %v, mp id: %v",
				mnReBalanceCtrl.zoneCtrl.reBalanceTaskInfo(), mpID)
			continue
		}

		log.LogInfof("select migrate partition finish, task info: %v, mp id: %v",
			mnReBalanceCtrl.zoneCtrl.reBalanceTaskInfo(), mpID)
		return
	}
	err = fmt.Errorf("select migrate partition failed")
	return
}

func (mnReBalanceCtrl *MetaNodeReBalanceController) doMigrate() (err error) {
	var migratePartitionInfo *proto.MetaPartitionInfo
	migratePartitionInfo, err = mnReBalanceCtrl.selectMigratePartition()
	if err != nil {
		log.LogErrorf("no migrate partition be selected, task info: %v, src node: %v, err: %v",
			mnReBalanceCtrl.zoneCtrl.reBalanceTaskInfo(), mnReBalanceCtrl.nodeInfo.Addr, err)
		return
	}

	//select dst meta node
	var dstMetaNodeAddr string
	dstMetaNodeAddr, err = mnReBalanceCtrl.selectDstMetaNodes(migratePartitionInfo.PartitionID, migratePartitionInfo.Hosts)
	if err != nil {
		log.LogErrorf("select dst meta node failed, task info: %v, src node: %v, partition id: %v, err: %v",
			mnReBalanceCtrl.zoneCtrl.reBalanceTaskInfo(), mnReBalanceCtrl.nodeInfo.Addr, migratePartitionInfo.PartitionID, err)
		return
	}

	//do migrate
	mnReBalanceCtrl.migrate(migratePartitionInfo, dstMetaNodeAddr)
	return
}
