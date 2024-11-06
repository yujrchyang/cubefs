package rebalance

import (
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/http_client"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util/log"
	"gorm.io/gorm/utils"
)

// Disk 记录每块Disk的DP、使用率
type Disk struct {
	zoneCtrl      *ZoneReBalanceController
	masterClient  *master.MasterClient
	releaseClient *releaseClient

	dataClient       *http_client.DataClient
	masterAddr       string
	nodeAddr         string // DataNode Addr
	path             string
	total            uint64 // 磁盘总空间
	used             uint64 // 已使用空间
	migratedSize     uint64 // 已迁移大小
	dpList           []*proto.PartitionReport
	dpIndex          int
	minWritableDPNum int
	migratedCount    int
	migrateLimit     int
}

func NewDiskReBalanceController(zoneCtrl *ZoneReBalanceController, dataClient *http_client.DataClient, disk *proto.DataNodeDiskInfo, masterAddr, nodeAddr string,
	minWritableDPNum, migrateLimitPerDisk int, masterClient *master.MasterClient, releaseClient *releaseClient) *Disk {
	return &Disk{
		zoneCtrl:         zoneCtrl,
		masterClient:     masterClient,
		releaseClient:    releaseClient,
		dataClient:       dataClient,
		masterAddr:       masterAddr,
		nodeAddr:         nodeAddr,
		path:             disk.Path,
		total:            disk.Total,
		used:             disk.Used,
		migratedSize:     0,
		dpList:           nil,
		minWritableDPNum: minWritableDPNum,
		migratedCount:    0,
		migrateLimit:     migrateLimitPerDisk,
	}
}

func (d *Disk) Usage() float64 {
	return float64(d.used-d.migratedSize) / float64(d.total)
}

// SelectDP 返回当前Disk中的一块DP供迁移
func (d *Disk) SelectDP() (dpInfo *proto.DataPartitionInfo, err error) {
	var ok bool
	if d.migrateLimit > -1 && d.migratedCount >= d.migrateLimit { // 设置每块磁盘迁移dp数量上限，-1代表无上限
		return nil, ErrNoSuitablePartition
	}

	for d.dpIndex < len(d.dpList) {
		dp := d.dpList[d.dpIndex]
		d.dpIndex++
		if exist := d.zoneCtrl.CheckMigVolumeExist(dp.VolName); !exist {
			continue
		}
		if ok, dpInfo = d.checkAvailable(dp); ok {
			break
		}
	}
	if d.dpIndex >= len(d.dpList) {
		return nil, ErrNoSuitablePartition
	}
	return dpInfo, nil
}

// AddDP 为当前disk添加一块dp
func (d *Disk) AddDP(dpID *proto.PartitionReport) {
	d.dpList = append(d.dpList, dpID)
}

func (d *Disk) SetMigrateLimit(limit int) {
	d.migrateLimit = limit
}

// UpdateMigratedDPSize 对dpInfo的迁移完成， 更新已迁移大小
func (d *Disk) UpdateMigratedDPSize(dpInfo *proto.DataPartitionInfo) {
	d.migratedCount++
	for _, replica := range dpInfo.Replicas {
		if replica.Addr == d.nodeAddr {
			d.migratedSize += replica.Used
			break
		}
	}
}

// 判断dataPartition是否可以选做迁移的Dp，并返回DataPartitionInfo
func (d *Disk) checkAvailable(dp *proto.PartitionReport) (bool, *proto.DataPartitionInfo) {
	var (
		dataPartition *proto.DataPartitionInfo
		err           error
	)
	// 检查10分钟内是否迁移过该dp
	if d.zoneCtrl.HasBeenMigratedTenMinutes(dp.PartitionID) {
		return false, nil
	}
	if d.releaseClient != nil {
		dataPartition, err = d.releaseClient.AdminGetDataPartition(dp.VolName, dp.PartitionID)
		if dataPartition != nil {
			dataPartition.Hosts = dataPartition.DbBackHosts_
		}
	}
	if d.masterClient != nil {
		dataPartition, err = d.masterClient.AdminAPI().GetDataPartition(dp.VolName, dp.PartitionID)
	}
	if err != nil {
		log.LogErrorf("GetDataPartition err:%v", err)
		return false, nil
	}
	log.LogInfof("checkAvailable: migNode(%v) disk(%s) dp(%v) status(%v) isRecover(%v) replicaNum(%v) hosts(%v)",
		d.nodeAddr, d.path, dp.PartitionID, dataPartition.Status, dataPartition.IsRecover, dataPartition.ReplicaNum, dataPartition.Hosts)
	// DP 正常
	if dataPartition.Status == proto.Unavailable {
		return false, nil
	}
	if dataPartition.IsRecover {
		return false, nil
	}
	//if d.masterClient != nil && dataPartition.ReplicaNum <= 2 { // 忽略非release集群2副本的dp
	//	return false, nil
	//}
	if dataPartition.ReplicaNum == 2 {
		if d.zoneCtrl.HasMigrateRecordForDp(dp.PartitionID) {
			log.LogInfof("checkAvailable: has records for 2-replica dp")
			return false, nil
		}
	}
	if len(dataPartition.Replicas) == 0 || dataPartition.Replicas[0].Used == 0 {
		log.LogInfof("checkAvailable: replica[0].Used = 0")
		return false, nil
	}

	liveReplicas := getLiveReplicas(dataPartition, 60*3)
	if len(liveReplicas) != len(dataPartition.Hosts) {
		log.LogWarnf("getDataPartitionFromDiskForMigration DP:%v liveReplicas:%v Hosts:%v", dataPartition.PartitionID, liveReplicas, dataPartition.Hosts)
		return false, nil
	}

	if !utils.Contains(dataPartition.Hosts, d.nodeAddr) {
		return false, nil
	}

	replicaNum := int(dataPartition.ReplicaNum)
	if len(dataPartition.Hosts) != replicaNum || len(dataPartition.Replicas) != replicaNum || len(dataPartition.MissingNodes) != 0 {
		return false, nil
	}

	// 先只选择非主备leader的副本执行迁移
	// 可能不需要这个限制条件了
	if dataPartition.Hosts[0] == d.nodeAddr {
		return false, nil
	}
	for _, replica := range dataPartition.Replicas {
		if replica.Addr == d.nodeAddr && replica.DiskPath != d.path {
			return false, nil
		}
	}
	//检测所有副本的raft status是否都正常
	if d.masterClient != nil {
		for _, host := range dataPartition.Hosts {
			stopped, err := checkRaftStatus(d.dataClient, dataPartition.PartitionID, host)
			if err != nil || stopped {
				log.LogWarnf("GetTargetReplicaRaftStatus partition:%v host:%v err:%v stopped:%v", dataPartition.PartitionID, host, err, stopped)
				return false, nil
			}
		}
	}

	// 判断vol中的可写dp数量是否大于最小可写dp数量
	var volView *proto.SimpleVolView
	if d.masterClient != nil {
		volView, err = d.masterClient.AdminAPI().GetVolumeSimpleInfo(dataPartition.VolName)
	} else {
		volView, err = d.releaseClient.GetVolumeSimpleInfo(dataPartition.VolName)
	}
	if err != nil {
		log.LogWarnf("getVolSimpleVolViewFromCache err:%v", err)
		return false, nil
	}
	log.LogInfof("checkAvailable: rwDpCount(%v) min-rwDpNum(%v) vol-min-rwDpNum(%v)", volView.RwDpCnt, d.minWritableDPNum, volView.MinWritableDPNum)
	if volView.RwDpCnt <= d.minWritableDPNum || volView.RwDpCnt <= volView.MinWritableDPNum {
		return false, nil
	}
	return true, dataPartition
}

func checkRaftStatus(dataHttpClient *http_client.DataClient, id uint64, host string) (stopped bool, err error) {
	raftStatus, err := dataHttpClient.GetRaftStatus(id)
	if err != nil {
		return
	}

	if raftStatus.ID != id {
		err = fmt.Errorf("GetRaftStatusFromDataNode err id not equal")
		return
	}
	if raftStatus.Applied == 0 {
		err = fmt.Errorf("GetRaftStatusFromDataNode Applied is 0")
		return
	}
	log.LogWarnf("GetRaftStatusFromDataNode dp:%v replica:%v Applied:%v", id, host, raftStatus.Applied)
	stopped = raftStatus.Stopped
	return
}
