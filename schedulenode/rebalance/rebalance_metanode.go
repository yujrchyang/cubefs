package rebalance

import (
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"gorm.io/gorm/utils"
	"math"
	"sort"
	"time"
)

type MetaNodeReBalanceController struct {
	nodeInfo                         *proto.MetaNodeInfo
	zoneCtrl                         *ZoneReBalanceController
	isFinished                       bool
	inodeTotalCnt                    uint64
	sortedMetaPartitions             []*proto.MetaPartitionInfo // 按mp inode数排序 降序
	alreadyMigrateFinishedPartitions map[uint64]bool
	maxMigrateDpCnt     			int
	hasMigrateDpCnt     			int
}

func NewMetaNodeReBalanceController(nodeInfo *proto.MetaNodeInfo, zoneCtrl *ZoneReBalanceController) (ctrl *MetaNodeReBalanceController) {
	ctrl = &MetaNodeReBalanceController{
		nodeInfo:                         nodeInfo,
		zoneCtrl:                         zoneCtrl,
		alreadyMigrateFinishedPartitions: make(map[uint64]bool),
	}
	if zoneCtrl.outMigRatio > 0 {
		ctrl.maxMigrateDpCnt = int(float64(len(nodeInfo.PersistenceMetaPartitions)) * zoneCtrl.outMigRatio)
	} else {
		ctrl.maxMigrateDpCnt = len(nodeInfo.PersistenceMetaPartitions)
	}
	return ctrl
}

func (mnCtrl *MetaNodeReBalanceController) updateSortedMetaPartitions() (err error) {
	var (
		nodeInodeTotal uint64
		partitions     = make([]*proto.MetaPartitionInfo, 0)
	)
	for _, mpID := range mnCtrl.nodeInfo.PersistenceMetaPartitions {
		// 出错会使统计的inode总数变少 迁的会少 无法达到目标阈值
		var partition *proto.MetaPartitionInfo
		if mnCtrl.zoneCtrl.masterClient != nil {
			partition, err = mnCtrl.zoneCtrl.masterClient.ClientAPI().GetMetaPartition(mpID, "")
		} else if mnCtrl.zoneCtrl.releaseClient != nil {
			partition, err = mnCtrl.zoneCtrl.releaseClient.GetMetaPartition("", mpID)
		}
		if err != nil {
			log.LogWarnf("updateSortedMetaPartitions: get mp[%v] failed, err(%v)", mpID, err)
			continue
		}
		nodeInodeTotal += partition.InodeCount
		// recover状态不剔除
		partitions = append(partitions, partition)
	}
	if len(partitions) == 0 {
		return fmt.Errorf("no partitions for node(%s)", mnCtrl.nodeInfo.Addr)
	}
	sort.SliceStable(partitions, func(i, j int) bool {
		return partitions[i].InodeCount > partitions[j].InodeCount
	})
	mnCtrl.sortedMetaPartitions = partitions
	mnCtrl.updateInodeTotalCnt(nodeInodeTotal)
	log.LogInfof("updateSortedMetaPartitions success: node(%v) len(mp)=%v inodeTotalCnt(%v)", mnCtrl.nodeInfo.Addr,
		len(partitions), nodeInodeTotal)
	return nil
}

func (mnCtrl *MetaNodeReBalanceController) updateInodeTotalCnt(totalCount uint64) {
	mnCtrl.inodeTotalCnt = totalCount
}

func (mnCtrl *MetaNodeReBalanceController) getCanBeMigrateInodeCnt() uint64 {
	if mnCtrl.nodeInfo.Ratio <= 0 || mnCtrl.nodeInfo.Ratio <= mnCtrl.zoneCtrl.goalRatio {
		return 0
	}

	count := math.Ceil(float64(mnCtrl.inodeTotalCnt) * (1 - mnCtrl.zoneCtrl.goalRatio/mnCtrl.nodeInfo.Ratio))
	log.LogInfof("getCanBeMigrateInodeCnt: node(%v) total(%v) count(%v)", mnCtrl.nodeInfo.Addr, mnCtrl.inodeTotalCnt, uint64(count))
	return uint64(count)
}

func (mnCtrl *MetaNodeReBalanceController) NeedReBalance(goalRatio float64) bool {
	usageRatioNeedRebalance := mnCtrl.nodeInfo.Ratio > goalRatio
	mpMigCntNeedRebalance := mnCtrl.hasMigrateDpCnt < mnCtrl.maxMigrateDpCnt
	log.LogInfof("metaNode needReBalance addr(%v) usageRatioNeedRebalance(%v) usage(%v) goalRatio(%v), mpMigCntNeedRebalance(%v) hasMigrateDpCnt(%v) maxMigrateDpCnt(%v)",
		mnCtrl.nodeInfo.Addr, usageRatioNeedRebalance, mnCtrl.nodeInfo.Ratio, goalRatio, mpMigCntNeedRebalance, mnCtrl.hasMigrateDpCnt, mnCtrl.maxMigrateDpCnt)
	return usageRatioNeedRebalance && mpMigCntNeedRebalance
}

func (mnCtrl *MetaNodeReBalanceController) selectDstMetaNodes(mpID uint64, hosts []string) (dstMetaNodeAddr string, err error) {
	offset := 0
	for offset < len(mnCtrl.zoneCtrl.dstMetaNodes) {
		index := (mnCtrl.zoneCtrl.dstIndex + offset) % len(mnCtrl.zoneCtrl.dstMetaNodes)
		dstMetaNode := mnCtrl.zoneCtrl.dstMetaNodes[index]
		offset++
		if canBeSelectedForMigrate(dstMetaNode, mnCtrl.zoneCtrl.goalRatio, hosts, mnCtrl.zoneCtrl.dstMetaNodeMaxPartitionCount) {
			dstMetaNodeAddr = dstMetaNode.Addr
			mnCtrl.zoneCtrl.dstIndex += offset
			return
		}
	}
	err = ErrNoSuitableDstNode
	log.LogErrorf("select dst meta node failed, task info: %v, mp id: %v, err: %v", mnCtrl.zoneCtrl.String(), mpID, err)
	return
}

func canBeSelectedForMigrate(dstMetaNodeInfo *proto.MetaNodeInfo, goalRatio float64, migratePartitionHosts []string, dstMetaNodePartitionMaxCount int) (canBeSelect bool) {
	defer func() {
		log.LogDebugf("canBeSelectedForMigrate: dstNode(%v)-beSelected(%v) isActive(%v) toBeOffline(%v) toBeMigrate(%v) srcPartitionHost(%v) ratio(%v) mpCount(%v)",
			dstMetaNodeInfo.Addr, canBeSelect, dstMetaNodeInfo.IsActive, dstMetaNodeInfo.ToBeOffline, dstMetaNodeInfo.ToBeMigrated, migratePartitionHosts, dstMetaNodeInfo.Ratio, dstMetaNodeInfo.MetaPartitionCount)
	}()

	if dstMetaNodeInfo.IsActive &&
		dstMetaNodeInfo.MetaPartitionCount < dstMetaNodePartitionMaxCount &&
		!dstMetaNodeInfo.ToBeOffline &&
		!dstMetaNodeInfo.ToBeMigrated &&
		!utils.Contains(migratePartitionHosts, dstMetaNodeInfo.Addr) { // 迁移分片的host中没有选中的dst节点
		if goalRatio > 0 {
			canBeSelect = dstMetaNodeInfo.Ratio < goalRatio
		} else {
			canBeSelect = dstMetaNodeInfo.Ratio < 0.7
		}
	}
	return canBeSelect
}

func (mnCtrl *MetaNodeReBalanceController) selectMP() (metaPartition *proto.MetaPartitionInfo, err error) {
	for _, partition := range mnCtrl.sortedMetaPartitions {
		exist := mnCtrl.zoneCtrl.CheckMigVolumeExist(partition.VolName)
		if !exist {
			continue
		}
		mpID := partition.PartitionID
		if _, ok := mnCtrl.alreadyMigrateFinishedPartitions[mpID]; ok {
			continue
		}
		if mnCtrl.zoneCtrl.HasBeenMigratedTenMinutes(mpID) {
			continue
		}
		mnCtrl.alreadyMigrateFinishedPartitions[mpID] = true

		if mnCtrl.zoneCtrl.masterClient != nil {
			metaPartition, err = mnCtrl.zoneCtrl.masterClient.ClientAPI().GetMetaPartition(mpID, "")
		} else if mnCtrl.zoneCtrl.releaseClient != nil {
			metaPartition, err = mnCtrl.zoneCtrl.releaseClient.GetMetaPartition("", mpID)
			if metaPartition != nil {
				metaPartition.Hosts = metaPartition.PersistenceHosts
			}
		}
		if err != nil {
			metaPartition = nil
			err = nil
			continue
		}
		if metaPartition.IsRecover {
			continue
		}
		if metaPartition.ReplicaNum <= 2 {
			continue
		}
		if !utils.Contains(metaPartition.Hosts, mnCtrl.nodeInfo.Addr) {
			continue
		}

		var dstStoreMode = proto.StoreModeDef
		for _, replica := range metaPartition.Replicas {
			if replica.Addr == mnCtrl.nodeInfo.Addr {
				dstStoreMode = replica.StoreMode
				break
			}
		}
		if dstStoreMode == proto.StoreModeDef {
			continue
		}
		return
	}
	return nil, ErrNoSuitablePartition
}

// 里面不迁移了 但是跳不到下一个节点
func (mnCtrl *MetaNodeReBalanceController) doMigrate() error {
	canBeMigrateInodeCnt := mnCtrl.getCanBeMigrateInodeCnt()
	alreadyMigrateInodeCnt := uint64(0)

	for alreadyMigrateInodeCnt < canBeMigrateInodeCnt {
		inRecoveringMPMap, err := IsInRecoveringMoreThanMaxBatchCount(mnCtrl.zoneCtrl.masterClient, mnCtrl.zoneCtrl.releaseClient, mnCtrl.zoneCtrl.rType, mnCtrl.zoneCtrl.clusterMaxBatchCount)
		if err != nil {
			log.LogWarnf("doMigrate err:%v", err.Error())
			time.Sleep(defaultWaitClusterRecover)
			continue
		}
		clusterMPCurrency := mnCtrl.zoneCtrl.clusterMaxBatchCount - len(inRecoveringMPMap)
		if clusterMPCurrency > mnCtrl.maxMigrateDpCnt-mnCtrl.hasMigrateDpCnt {
			clusterMPCurrency = mnCtrl.maxMigrateDpCnt-mnCtrl.hasMigrateDpCnt
		}
		for clusterMPCurrency > 0 {
			select {
			case <-mnCtrl.zoneCtrl.ctx.Done():
				mnCtrl.zoneCtrl.SetIsManualStop(true)
				log.LogInfof("doMigrate stop: taskID(%v) cluster(%v) zone(%v) srcMetaNode(%v)", mnCtrl.zoneCtrl.Id, mnCtrl.zoneCtrl.cluster, mnCtrl.zoneCtrl.zoneName, mnCtrl.nodeInfo.Addr)
				return nil
			default:
			}

			if alreadyMigrateInodeCnt >= canBeMigrateInodeCnt {
				return ErrReachMaxInodeLimit
			}

			migratePartitionInfo, err := mnCtrl.selectMP()
			if err != nil {
				log.LogErrorf("doMigrate: mo metaPartition be selected, taskID(%v) srcMetaNode(%v) err(%v)", mnCtrl.zoneCtrl.String(), mnCtrl.nodeInfo.Addr, err)
				return err
			}

			dstMetaNodeAddr, err := mnCtrl.selectDstMetaNodes(migratePartitionInfo.PartitionID, migratePartitionInfo.Hosts)
			if err != nil {
				// only for this mp unsuitable, continue
				continue
			}
			clusterMPCurrency--
			mnCtrl.hasMigrateDpCnt++
			alreadyMigrateInodeCnt += migratePartitionInfo.InodeCount
			//do migrate
			mnCtrl.migrate(migratePartitionInfo, dstMetaNodeAddr)
		}
	}
	return nil
}

func (mnCtrl *MetaNodeReBalanceController) migrate(mp *proto.MetaPartitionInfo, destAddr string) (err error) {
	defer func() {
		msg := fmt.Sprintf("DecommissionMetaPartition: taskID(%v) mp(%v) src(%v) dest(%v)", mnCtrl.zoneCtrl.Id, mp.PartitionID, mnCtrl.nodeInfo.Addr, destAddr)
		if err == nil {
			log.LogInfof("%s", msg)
		} else {
			log.LogErrorf("%s, err(%v)", msg, err)
		}
	}()
	if mnCtrl.zoneCtrl.masterClient != nil {
		err = mnCtrl.zoneCtrl.masterClient.AdminAPI().DecommissionMetaPartition(mp.PartitionID, mnCtrl.nodeInfo.Addr, destAddr, int(mp.Replicas[0].StoreMode))
	}
	if mnCtrl.zoneCtrl.releaseClient != nil {
		err = mnCtrl.zoneCtrl.releaseClient.MetaPartitionOffline(mp.PartitionID, mnCtrl.nodeInfo.Addr, destAddr, mp.VolName)
	}
	mnCtrl.zoneCtrl.RecordMigratePartition(mp.PartitionID)
	if err != nil {
		return
	}

	err = mnCtrl.zoneCtrl.rw.PutMigrateInfoToDB(&MigrateRecordTable{
		ClusterName: mnCtrl.zoneCtrl.cluster,
		ZoneName:    mnCtrl.zoneCtrl.zoneName,
		RType:       RebalanceMeta,
		VolName:     mp.VolName,
		PartitionID: mp.PartitionID,
		SrcAddr:     mnCtrl.nodeInfo.Addr,
		DstAddr:     destAddr,
		TaskId:      mnCtrl.zoneCtrl.Id,
		CreatedAt:   time.Now(),
		UpdateAt:    time.Now(),
	})
	return
}
