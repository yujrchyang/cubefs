package master

import (
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"strings"
	"sync/atomic"
	"time"
)

func (c *Cluster) checkMigratedDataPartitionsRecoveryProgress() {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("checkMigratedDataPartitionsRecoveryProgress occurred panic,err[%v]", r)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"checkMigratedDataPartitionsRecoveryProgress occurred panic")
		}
	}()
	unrecoverPartitionIDs := make(map[string]uint64, 0)
	var passedTime int64
	unrecoverableDuration := atomic.LoadInt64(&c.cfg.UnrecoverableDuration)
	c.MigratedDataPartitionIds.Range(func(key, value interface{}) bool {
		if c.leaderHasChanged() {
			return false
		}
		partitionID := value.(uint64)
		partition, err := c.getDataPartitionByID(partitionID)
		if err != nil {
			unrecoverPartitionIDs[key.(string)] = partitionID
			return true
		}
		vol, err := c.getVol(partition.VolName)
		if err != nil {
			unrecoverPartitionIDs[key.(string)] = partitionID
			return true
		}
		if len(partition.Replicas) == 0 || len(partition.Replicas) < int(vol.dpReplicaNum) {
			return true
		}
		passedTime = time.Now().Unix() - partition.modifyTime
		if vol.Status == proto.VolStMarkDelete || (partition.isDataCatchUpInStrictMode() && partition.allReplicaHasRecovered() && passedTime > 2*defaultIntervalToCheckHeartbeat) {
			partition.RLock()
			if partition.isRecover {
				partition.isRecover = false
				c.syncUpdateDataPartition(partition)
			}
			partition.RUnlock()
			c.MigratedDataPartitionIds.Delete(key)
		} else {
			if passedTime > unrecoverableDuration {
				unrecoverPartitionIDs[key.(string)] = partitionID
			}
		}

		return true
	})
	if len(unrecoverPartitionIDs) != 0 {
		deletedDpIds := c.getHasDeletedDpIds(unrecoverPartitionIDs)
		for _, key := range deletedDpIds {
			c.MigratedDataPartitionIds.Delete(key)
			delete(unrecoverPartitionIDs, key)
		}
		if len(unrecoverPartitionIDs) == 0 {
			return
		}
		msg := fmt.Sprintf("action[checkMigratedDpRecoveryProgress] clusterID[%v],has[%v] has migrated more than %v hours,still not recovered,ids[%v]",
			c.Name, len(unrecoverPartitionIDs), unrecoverableDuration/60/60, unrecoverPartitionIDs)
		WarnBySpecialKey(gAlarmKeyMap[alarmKeyDpHasNotRecover], msg)
	}
}

func (c *Cluster) putMigratedDataPartitionIDs(replica *DataReplica, addr string, partitionID uint64) {
	var diskPath string
	if replica != nil {
		diskPath = replica.DiskPath
	}
	key := decommissionDataPartitionKey(addr, diskPath, partitionID)
	c.MigratedDataPartitionIds.Store(key, partitionID)
}

func decommissionDataPartitionKey(addr, diskPath string, partitionID uint64) string {
	return fmt.Sprintf("%s%v%s%v%v", addr, keySeparator, diskPath, keySeparator, partitionID)
}
func getAddrFromDecommissionDataPartitionKey(key string) string {
	return strings.Split(key, keySeparator)[0]
}

func (c *Cluster) putMigratedMetaPartitions(addr string, partitionID uint64) {
	key := decommissionMetaPartitionKey(addr, partitionID)
	c.MigratedMetaPartitionIds.Store(key, partitionID)
}

func decommissionMetaPartitionKey(addr string, partitionID uint64) string {
	return fmt.Sprintf("%v%v%v", addr, keySeparator, partitionID)
}
func getAddrFromDecommissionMetaPartitionKey(key string) string {
	return strings.Split(key, keySeparator)[0]
}

func (c *Cluster) checkMigratedMetaPartitionRecoveryProgress() {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("checkMigratedMetaPartitionRecoveryProgress occurred panic,err[%v]", r)
			WarnBySpecialKey(fmt.Sprintf("%v_%v_scheduling_job_panic", c.Name, ModuleName),
				"checkMigratedMetaPartitionRecoveryProgress occurred panic")
		}
	}()

	c.MigratedMetaPartitionIds.Range(func(key, value interface{}) bool {
		if c.leaderHasChanged() {
			return false
		}
		partitionID := value.(uint64)
		partition, err := c.getMetaPartitionByID(partitionID)
		if err != nil {
			return true
		}
		c.doLoadMetaPartition(partition)
		return true
	})

	var (
		dentryDiff  float64
		applyIDDiff float64
	)
	unrecoverMpIDs := make(map[string]uint64, 0)
	unrecoverableDuration := atomic.LoadInt64(&c.cfg.UnrecoverableDuration)
	c.MigratedMetaPartitionIds.Range(func(key, value interface{}) bool {
		if c.leaderHasChanged() {
			return false
		}
		partitionID := value.(uint64)
		partition, err := c.getMetaPartitionByID(partitionID)
		if err != nil {
			unrecoverMpIDs[key.(string)] = partitionID
			return true
		}
		vol, err := c.getVol(partition.volName)
		if err != nil {
			unrecoverMpIDs[key.(string)] = partitionID
			return true
		}
		if len(partition.Replicas) == 0 || len(partition.Replicas) < int(vol.mpReplicaNum) || len(partition.Recorders) < int(vol.mpRecorderNum) {
			return true
		}
		dentryDiff = partition.getMinusOfDentryCount()
		applyIDDiff = partition.getMinusOfApplyID()
		if vol.Status == proto.VolStMarkDelete || (dentryDiff == 0 && applyIDDiff == 0 && partition.allReplicaHasRecovered()) {
			partition.RLock()
			partition.IsRecover = false
			c.syncUpdateMetaPartition(partition)
			partition.RUnlock()
			c.MigratedMetaPartitionIds.Delete(key)
		} else {
			if time.Now().Unix()-partition.modifyTime > unrecoverableDuration {
				unrecoverMpIDs[key.(string)] = partitionID
			}
		}

		return true
	})
	if len(unrecoverMpIDs) != 0 {
		deletedMpIds := c.getHasDeletedMpIds(unrecoverMpIDs)
		for _, key := range deletedMpIds {
			c.MigratedMetaPartitionIds.Delete(key)
			delete(unrecoverMpIDs, key)
		}
		if len(unrecoverMpIDs) == 0 {
			return
		}
		msg := fmt.Sprintf("action[checkMetaPartitionRecoveryProgress] clusterID[%v],[%v] has migrated more than %v hours,still not recovered,ids[%v]",
			c.Name, len(unrecoverMpIDs), unrecoverableDuration/60/60, unrecoverMpIDs)
		WarnBySpecialKey(gAlarmKeyMap[alarmKeyMpHasNotRecover], msg)
	}
}
