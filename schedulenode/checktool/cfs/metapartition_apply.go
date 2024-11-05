package cfs

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/schedulenode/checktool/cfs/tcp_api"
	"github.com/cubefs/cubefs/sdk/http_client"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"math"
	"strings"
	"sync"
	"time"
)

func (s *ChubaoFSMonitor) CheckMetaPartitionApply() {
	log.LogInfof("CheckMetaPartitionApply start")
	wg := sync.WaitGroup{}
	for _, clusterHost := range s.hosts {
		wg.Add(1)
		go func(ch *ClusterHost) {
			defer wg.Done()
			checkMetaPartitionApply(ch)
		}(clusterHost)
	}
	wg.Wait()
	log.LogInfof("CheckMetaPartitionApply end")
}

func checkMetaPartitionApply(clusterHost *ClusterHost) {
	defer func() {
		if r := recover(); r != nil {
			msg := fmt.Sprintf("checkHostMetaPartition panic:%v", r)
			log.LogError(msg)
			fmt.Println(msg)
		}
	}()
	log.LogInfof("action[checkMetaPartitionApply] host[%v] check metaPartition apply begin", clusterHost)
	startTime := time.Now()
	volStats, _, err := getAllVolStat(clusterHost)
	if err != nil {
		log.LogErrorf("action[checkMetaPartitionApply] host[%v] get allVolStat occurred error, err:%v", clusterHost, err)
		return
	}
	for _, vss := range volStats {
		checkMetaPartitionByVol(vss.Name, clusterHost)
	}
	clusterHost.metaPartitionHolder.badMetaCheckCount++
	if clusterHost.metaPartitionHolder.badMetaCheckCount%3 == 0 {
		checkFailed := make([]uint64, 0)
		clusterHost.metaPartitionHolder.badMetaMapLock.RLock()
		for mp, count := range clusterHost.metaPartitionHolder.badMetaMap {
			if count == 3 {
				checkFailed = append(checkFailed, mp)
			}
		}
		clusterHost.metaPartitionHolder.badMetaMapLock.RUnlock()
		log.LogWarnf("action[checkMetaPartitionApply] host[%v] check failed meta partitions total[%v] detail[%v]", clusterHost.host, len(checkFailed), checkFailed)

		if len(checkFailed) > 0 {
			// 连续三次检查都失败的mp发送报警提示
			exporter.WarningBySpecialUMPKey(metaPartitionApplyFailedWarningKey, fmt.Sprintf("Domain[%v] check failed meta partition len[%v] details[%v] ", clusterHost.host, len(checkFailed), checkFailed))
		}
		// 清空map
		clusterHost.metaPartitionHolder.badMetaMapLock.Lock()
		clusterHost.metaPartitionHolder.badMetaMap = make(map[uint64]int, 0)
		clusterHost.metaPartitionHolder.badMetaMapLock.Unlock()
	}
	log.LogInfof("action[checkMetaPartitionApply] host[%v] check metaPartition apply end, cost [%v]", clusterHost.host, time.Since(startTime))
}

func checkMetaPartitionByVol(volName string, ch *ClusterHost) {
	var wg sync.WaitGroup
	mps, err := getMetaPartitionsFromVolume(volName, ch)
	if err != nil || mps == nil {
		log.LogWarnf("action[checkHostMetaPartition] get metaPartitions of volume failed, vol:%v", volName)
		return
	}
	// 6 并发
	mpChan := make(chan *MetaPartition, 6)
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func(w *sync.WaitGroup, mpCh chan *MetaPartition) {
			defer w.Done()
			for mp := range mpCh {
				retryCompareMetaPartition(ch, volName, mp.PartitionID)
				time.Sleep(time.Millisecond * 16)
			}
		}(&wg, mpChan)
	}
	for _, mp := range mps {
		mpChan <- mp
	}
	close(mpChan)
	wg.Wait()
}

const (
	applyIndex = iota
	dEntryIndex
	inoudeIndex
)

func retryCompareMetaPartition(ch *ClusterHost, volName string, partitionID uint64) {
	var (
		mp  *MetaPartition
		err error
	)
	defer func() {
		if err != nil {
			log.LogErrorf("action[retryCompareMetaPartition] failed, host[%v] vol[%v] bad partition id[%v], err:%v", ch.host, volName, partitionID, err)
			// 这类mp检查失败的报警原则是尽力而为，不需全报，应防止map过大产生过多内存或影响计算速度
			ch.metaPartitionHolder.badMetaMapLock.Lock()
			if _, ok := ch.metaPartitionHolder.badMetaMap[partitionID]; !ok && len(ch.metaPartitionHolder.badMetaMap) < 3000 {
				ch.metaPartitionHolder.badMetaMap[partitionID] = 1
			} else {
				ch.metaPartitionHolder.badMetaMap[partitionID] = ch.metaPartitionHolder.badMetaMap[partitionID] + 1
			}
			ch.metaPartitionHolder.badMetaMapLock.Unlock()
		}
	}()
	minReplicas := make([][]*tcp_api.MetaPartitionLoadResponse, 3)

	reqURL := fmt.Sprintf("http://%v/metaPartition/get?name=%v&id=%v", ch.host, volName, partitionID)
	data, err := doRequest(reqURL, ch.isReleaseCluster)
	if err != nil {
		return
	}
	mp = new(MetaPartition)
	if err = json.Unmarshal(data, mp); err != nil {
		return
	}
	log.LogDebugf("action[retryCompareMetaPartition] host[%v] vol[%v] partition[%v] isRecover[%v] mp[%v]", ch.host, volName, partitionID, mp.IsRecover, mp)
	if mp.IsRecover {
		return
	}
	var retry = 3
	var failedTime int
	// 连续检查 3 次，降低误报
	for i := 0; i < retry; i++ {
		if err = compareMetaPartition(ch.isReleaseCluster, minReplicas, mp, partitionID); err != nil {
			failedTime++
			continue
		}
		// 如果三类指标都没问题，则退出检查
		if len(minReplicas[applyIndex]) == 0 && len(minReplicas[dEntryIndex]) == 0 && len(minReplicas[inoudeIndex]) == 0 {
			return
		}
		time.Sleep(time.Second * 3)
	}
	if err != nil {
		return
	}
	// 如果检查失败超过一次，样本不足，可能误报
	if failedTime >= 2 {
		err = fmt.Errorf("failed too much times")
		return
	}

	// Apply至少命中两次
	if len(minReplicas[applyIndex]) > 2 {
		first := minReplicas[applyIndex][0]
		last := minReplicas[applyIndex][len(minReplicas[applyIndex])-1]
		// 两次检查apply相同且address相同，可能卡住
		if first.ApplyID == last.ApplyID && first.Addr == last.Addr {
			exporter.WarningBySpecialUMPKey(metaPartitionApplyWarningKey, fmt.Sprintf("Domain[%v] vol[%v] mp[%v] found different apply id, min apply addr[%v] min apply id[%v]", ch.host, volName, partitionID, first.Addr, first.ApplyID))
			return
		}
	}
	// dEntry连续命中并且addr相同，报警
	if len(minReplicas[dEntryIndex]) == retry {
		first := minReplicas[dEntryIndex][0]
		last := minReplicas[dEntryIndex][len(minReplicas[dEntryIndex])-1]
		if first.Addr == last.Addr {
			msg := fmt.Sprintf("Domain[%v] vol[%v] mp[%v] found different dentry, min addr[%v] min dEntry[%v]", ch.host, volName, partitionID, first.Addr, first.DentryCount)
			if isServerStartCompleted(first.Addr) {
				exporter.WarningBySpecialUMPKey(metaPartitionApplyWarningKey, msg)
			} else {
				log.LogWarnf("%v server not completed start", msg)
			}
		}
	}
	// inode连续命中并且addr相同，报警
	if len(minReplicas[inoudeIndex]) == retry {
		first := minReplicas[inoudeIndex][0]
		last := minReplicas[inoudeIndex][len(minReplicas[inoudeIndex])-1]
		if first.Addr == last.Addr {
			msg := fmt.Sprintf("Domain[%v] vol[%v] mp[%v] found different inode count, min addr[%v] min inode count[%v]", ch.host, volName, partitionID, first.Addr, first.InodeCount)
			if isServerStartCompleted(first.Addr) {
				exporter.WarningBySpecialUMPKey(metaPartitionApplyWarningKey, msg)
			} else {
				log.LogWarnf("%v server not completed start", msg)
			}
		}
	}
}

func isServerStartCompleted(tcpAddr string) bool {
	client := http_client.NewDataClient(fmt.Sprintf("%v:%v", strings.Split(tcpAddr, ":")[0], profPortMap[strings.Split(tcpAddr, ":")[1]]), false)
	stat, err := client.GetStatus()
	if err != nil {
		log.LogErrorf("ip[%v] get status failed, err:%v", tcpAddr, err)
		return false
	}
	return stat.StartComplete
}

func compareMetaPartition(dbbak bool, minReplicas [][]*tcp_api.MetaPartitionLoadResponse, mp *MetaPartition, partitionID uint64) (err error) {
	metaInfos := make(map[string]*tcp_api.MetaPartitionLoadResponse, len(mp.Replicas))
	for _, r := range mp.Replicas {
		var mpr *tcp_api.MetaPartitionLoadResponse
		for i := 0; i < 3; i++ {
			if mpr, err = tcp_api.LoadMetaPartition(dbbak, partitionID, r.Addr); err == nil {
				break
			}
			time.Sleep(time.Millisecond * 200)
		}
		if err != nil {
			return
		}
		metaInfos[r.Addr] = mpr
	}
	minApplied, appliedSame := compareLoadResponse(200, 0, func(mpr *tcp_api.MetaPartitionLoadResponse) uint64 { return mpr.ApplyID }, metaInfos)
	if !appliedSame {
		minReplicas[applyIndex] = append(minReplicas[applyIndex], minApplied)
		log.LogWarnf("action[checkMetaPartitionDiffInfo] apply same[%v] %v", appliedSame, minApplied)
	}
	minDentry, dentrySame := compareLoadResponse(200, 3, func(mpr *tcp_api.MetaPartitionLoadResponse) uint64 { return mpr.DentryCount }, metaInfos)
	if !dentrySame {
		minReplicas[dEntryIndex] = append(minReplicas[dEntryIndex], minDentry)
		log.LogWarnf("action[checkMetaPartitionDiffInfo] dEntry count same[%v] %v", dentrySame, minDentry)
	}
	minInode, inodeSame := compareLoadResponse(200, 3, func(mpr *tcp_api.MetaPartitionLoadResponse) uint64 { return mpr.InodeCount }, metaInfos)
	if !inodeSame {
		minReplicas[inoudeIndex] = append(minReplicas[inoudeIndex], minInode)
		log.LogWarnf("action[checkMetaPartitionDiffInfo] inode count same[%v] %v", inodeSame, minInode)
	}
	return
}

// percentDiff 如果为0，则不比较百分比差值
func compareLoadResponse(diff uint64, percentDiff uint64, getValue func(mpr *tcp_api.MetaPartitionLoadResponse) uint64, metaInfos map[string]*tcp_api.MetaPartitionLoadResponse) (*tcp_api.MetaPartitionLoadResponse, bool) {
	var minVReplica *tcp_api.MetaPartitionLoadResponse
	if len(metaInfos) < 2 {
		return nil, true
	}
	maxV := uint64(0)
	minV := uint64(math.MaxUint64)
	for _, metaInfo := range metaInfos {
		if getValue(metaInfo) > maxV {
			maxV = getValue(metaInfo)
		}
		if getValue(metaInfo) < minV {
			minV = getValue(metaInfo)
			minVReplica = metaInfo
		}
	}
	if maxV-minV > diff {
		return minVReplica, false
	}
	if maxV < minV {
		return minVReplica, false
	}
	if percentDiff > 0 && maxV > 0 && (maxV-minV)*uint64(100)/maxV > percentDiff {
		return minVReplica, false
	}
	return nil, true
}
