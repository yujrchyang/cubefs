package cfs

import (
	"fmt"
	"github.com/cubefs/cubefs/schedulenode/common/cfs"
	"github.com/cubefs/cubefs/util/checktool"
	"github.com/cubefs/cubefs/util/log"
	"math"
	"sync"
	"time"
)

var warnUmp = true

func (s *ChubaoFSMonitor) checkDbbakDataPartition() {
	dpCheckStartTime := time.Now()
	log.LogInfof("checkDbbakDataPartition start")
	for _, host := range s.hosts {
		if host.isReleaseCluster {
			checkDbbakDataPartition(host, s.checkPeerConcurrency)
		}
	}
	log.LogInfof("checkDbbakDataPartition end, cost [%v]", time.Since(dpCheckStartTime))
}

func checkDbbakDataPartition(host *ClusterHost, concurrency int) {
	cv, err := getCluster(host)
	if err != nil {
		msg := fmt.Sprintf("get cluster info from %v failed,err:%v ", host.host, err)
		log.LogWarn(msg)
		return
	}
	dpCh := make(chan *DataPartitionResponse, 32)
	wg := sync.WaitGroup{}
	wg.Add(concurrency)
	// concurrent check partition
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case partition := <-dpCh:
					if partition == nil {
						return
					}
					// todo: check partition
					checkDpRecover(host, cv, partition)
				}
			}
		}()
	}
	// produce partition
	for _, vol := range cv.VolStat {
		var volInfo *SimpleVolView
		var vv *VolView
		volInfo, err = getVolSimpleView(vol.Name, host)
		if err != nil {
			log.LogErrorf("Domain[%v] vol[%v] get simple vol info failed, err:%v", host.host, vol.Name, err)
			continue
		}
		authKey := checktool.Md5(volInfo.Owner)
		vv, err = getDataPartitionsFromVolume(host, vol.Name, authKey)
		if err != nil {
			log.LogErrorf("Domain[%v] vol[%v] getDataPartitionsFromVolume failed, err:%v", host.host, vol.Name, err)
			continue
		}
		for _, dp := range vv.DataPartitions {
			dpCh <- dp
		}
	}
	close(dpCh)
	wg.Wait()
	log.LogInfof("Domain[%v] checkDbbakDataPartition finished", host.host)
}

func checkDpRecover(host *ClusterHost, cv *ClusterView, dpr *DataPartitionResponse) {
	dp, err := cfs.GetDataPartition(host.host, dpr.PartitionID, true)
	if err != nil {
		log.LogErrorf("action[checkDpRecover] err:%v", err)
		return
	}
	if !dp.IsRecover {
		return
	}
	if len(dp.Replicas) < dp.ReplicaNum {
		log.LogErrorf("action[checkDpRecover] dp[%v] may be missing replica[%v] replicaNum[%v]", dp.PartitionID, len(dp.Replicas), dp.ReplicaNum)
		return
	}

	// maybe dirty extent, just offline the leader
	if isBadPartition(cv, dpr.PartitionID) {
		log.LogWarnf("maybe dirty extent, recommend offline leader")
		return
	}

	// compare used size, if equal, reset recover status to false
	var minSize = uint64(math.MaxUint64)
	var maxSize uint64
	for _, r := range dp.Replicas {
		if r.UsedSize < minSize {
			minSize = r.UsedSize
		}
		if r.UsedSize > maxSize {
			maxSize = r.UsedSize
		}
	}

	host.tokenLock.Lock()
	defer host.tokenLock.Unlock()
	if host.tokenMap[resetDbBackRecoverToken] > 0 && maxSize == minSize {
		err = cfs.ResetDataPartitionRecover(host.host, dpr.PartitionID, true)
		if err != nil {
			log.LogErrorf("action[checkDpRecover] reset recover err:%v", err)
			return
		}

		host.tokenMap[resetDbBackRecoverToken] = host.tokenMap[resetDbBackRecoverToken] - 1
		log.LogWarnf("action[checkDpRecover] reset recover, dp:%v", dp.PartitionID)
		return
	}
	log.LogWarnf("action[checkDpRecover] maybe need reset recover, dp:%v, token:%v, maxSize:%v, minSize:%v", dp.PartitionID, host.tokenMap[resetDbBackRecoverToken], maxSize, minSize)
}

func isBadPartition(cv *ClusterView, partition uint64) bool {
	for _, dp := range cv.BadPartitionIDs {
		if dp.PartitionID == partition {
			return true
		}
	}
	return false
}
