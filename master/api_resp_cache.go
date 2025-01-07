// Copyright 2018 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package master

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

func (c *Cluster) updateVolInfoResponseCache() (body []byte, err error) {

	var vol *Vol
	volsInfo := make([]*proto.VolInfo, 0)
	for _, name := range c.allVolNames() {
		if c.leaderHasChanged() {
			return nil, proto.ErrRaftLeaderHasChanged
		}
		if vol, err = c.getVol(name); err != nil {
			continue
		}
		stat := volStat(vol)
		volInfo := proto.NewVolInfo(vol.Name, vol.Owner, vol.createTime, vol.status(), stat.TotalSize, stat.UsedSize,
			vol.trashRemainingDays, vol.ChildFileMaxCount, vol.isSmart, vol.smartRules, vol.ForceROW, vol.compact(),
			vol.TrashCleanInterval, vol.enableToken, vol.enableWriteCache, vol.BatchDelInodeCnt, vol.DelInodeInterval,
			vol.CleanTrashDurationEachTime, vol.TrashCleanMaxCountEachTime, vol.EnableBitMapAllocator, vol.enableRemoveDupReq,
			vol.TruncateEKCountEveryTime, vol.DefaultStoreMode)
		volInfo.BitMapSnapFrozenHour = vol.BitMapSnapFrozenHour
		volInfo.FileTotalSize = stat.FileTotalSize
		volInfo.TrashUsedSize = stat.TrashUsedSize
		volInfo.EnableCheckDeleteEK = vol.EnableCheckDeleteEK
		volInfo.ReqRecordsReservedTime = vol.reqRecordReservedTime
		volInfo.ReqRecordMaxCount = vol.reqRecordMaxCount
		volInfo.PersistenceMode = vol.PersistenceMode
		volsInfo = append(volsInfo, volInfo)
	}
	reply := newSuccessHTTPReply(volsInfo)
	if body, err = json.Marshal(reply); err != nil {
		log.LogError(fmt.Sprintf("action[updateVolInfoResponseCache],err:%v", err.Error()))
		return nil, proto.ErrMarshalData
	}
	c.volsInfoCacheMutex.Lock()
	c.volsInfoRespCache = body
	c.volsInfoCacheMutex.Unlock()
	return
}

func (c *Cluster) getVolsResponseCache() []byte {
	c.volsInfoCacheMutex.RLock()
	defer c.volsInfoCacheMutex.RUnlock()
	return c.volsInfoRespCache
}

func (c *Cluster) clearVolsResponseCache() {
	c.volsInfoCacheMutex.Lock()
	defer c.volsInfoCacheMutex.Unlock()
	c.volsInfoRespCache = nil
}

func (c *Cluster) getServerLimitInfoRespCache(dataNodeZoneName string) (data []byte, err error) {
	value, ok := c.serverLimitInfoRespCache.Load(dataNodeZoneName)
	if !ok {
		return nil, proto.ErrLimitInfoIsNotCached
	}
	return value.([]byte), nil
}

func (c *Cluster) setServerLimitInfoRespCache(dataNodeZoneName string, data []byte) {
	c.serverLimitInfoRespCache.Store(dataNodeZoneName, data)
}

func (c *Cluster) updateServerLimitInfoRespCache() {

	limitInfo := c.buildLimitInfo("")
	repairTaskCount := atomic.LoadUint64(&c.cfg.DataNodeRepairTaskCount)
	ssdZoneRepairTaskCount := atomic.LoadUint64(&c.cfg.DataNodeRepairSSDZoneTaskCount)
	if ssdZoneRepairTaskCount == 0 {
		ssdZoneRepairTaskCount = defaultSSDZoneTaskLimit
	}
	c.zoneStatInfos.Range(func(key, value any) bool {
		zoneName, ok := key.(string)
		if !ok {
			return true
		}
		if zoneName != "" {
			if zoneTaskLimit, exist := c.cfg.DataNodeRepairTaskCountZoneLimit[zoneName]; exist {
				repairTaskCount = zoneTaskLimit
			} else if strings.Contains(zoneName, mediumSSD) {
				repairTaskCount = ssdZoneRepairTaskCount
			}
			limitInfo.DataNodeRepairTaskLimitOnDisk = repairTaskCount
		}
		reply := newSuccessHTTPReply(limitInfo)
		data, mErr := json.Marshal(reply)
		if mErr != nil {
			return true
		}
		c.setServerLimitInfoRespCache(zoneName, data)
		return true
	})

	// cache empty zone name
	reply := newSuccessHTTPReply(limitInfo)
	data, mErr := json.Marshal(reply)
	if mErr != nil {
		return
	}
	c.setServerLimitInfoRespCache("", data)
}

func (c *Cluster) buildLimitInfo(volName string) (cInfo *proto.LimitInfo) {
	batchCount := atomic.LoadUint64(&c.cfg.MetaNodeDeleteBatchCount)
	deleteLimitRate := atomic.LoadUint64(&c.cfg.DataNodeDeleteLimitRate)
	dumpWaterLevel := atomic.LoadUint64(&c.cfg.MetaNodeDumpWaterLevel)
	if dumpWaterLevel < defaultMetanodeDumpWaterLevel {
		dumpWaterLevel = defaultMetanodeDumpWaterLevel
	}
	repairTaskCount := atomic.LoadUint64(&c.cfg.DataNodeRepairTaskCount)
	ssdZoneRepairTaskCount := atomic.LoadUint64(&c.cfg.DataNodeRepairSSDZoneTaskCount)
	if ssdZoneRepairTaskCount == 0 {
		ssdZoneRepairTaskCount = defaultSSDZoneTaskLimit
	}
	clusterRepairTaskCount := repairTaskCount
	deleteSleepMs := atomic.LoadUint64(&c.cfg.MetaNodeDeleteWorkerSleepMs)
	metaNodeReadDirLimitNum := atomic.LoadUint64(&c.cfg.MetaNodeReadDirLimitNum)
	dataNodeFlushFDInterval := atomic.LoadUint32(&c.cfg.DataNodeFlushFDInterval)
	dataNodeFlushFDParallelismOnDisk := atomic.LoadUint64(&c.cfg.DataNodeFlushFDParallelismOnDisk)
	dataPartitionConsistencyMode := proto.ConsistencyModeFromInt32(atomic.LoadInt32(&c.cfg.DataPartitionConsistencyMode))
	persistenceMode := proto.PersistenceMode(atomic.LoadInt32(&c.cfg.PersistenceMode))
	monitorSummarySec := atomic.LoadUint64(&c.cfg.MonitorSummarySec)
	monitorReportSec := atomic.LoadUint64(&c.cfg.MonitorReportSec)
	metaRocksDBWalFileSize := atomic.LoadUint64(&c.cfg.MetaRockDBWalFileSize)
	metaRocksDBWalMemSize := atomic.LoadUint64(&c.cfg.MetaRocksWalMemSize)
	metaRocksDBLogSize := atomic.LoadUint64(&c.cfg.MetaRocksLogSize)
	metaRocksDBLogReservedTime := atomic.LoadUint64(&c.cfg.MetaRocksLogReservedTime)
	metaRocksDBLogReservedCnt := atomic.LoadUint64(&c.cfg.MetaRocksLogReservedCnt)
	metaRocksDBFlushWalInterval := atomic.LoadUint64(&c.cfg.MetaRocksFlushWalInterval)
	metaRocksDBWalTTL := atomic.LoadUint64(&c.cfg.MetaRocksWalTTL)
	metaRocksDBDisableFlush := atomic.LoadUint64(&c.cfg.MetaRocksDisableFlushFlag)
	metaDeleteEKRecordFilesMaxTotalSize := atomic.LoadUint64(&c.cfg.DeleteEKRecordFilesMaxSize)
	metaTrashCleanInterval := atomic.LoadUint64(&c.cfg.MetaTrashCleanInterval)
	metaRaftLogSize := atomic.LoadInt64(&c.cfg.MetaRaftLogSize)
	metaRaftLogCap := atomic.LoadInt64(&c.cfg.MetaRaftLogCap)
	normalExtentDeleteExpireTime := atomic.LoadUint64(&c.cfg.DataNodeNormalExtentDeleteExpire)
	trashCleanDuration := atomic.LoadInt32(&c.cfg.TrashCleanDurationEachTime)
	trashCleanMaxCount := atomic.LoadInt32(&c.cfg.TrashItemCleanMaxCountEachTime)
	dpTimeoutCntThreshold := atomic.LoadInt32(&c.cfg.DpTimeoutCntThreshold)
	clientReqRecordsReservedCount := atomic.LoadInt32(&c.cfg.ClientReqRecordsReservedCount)
	clientReqRecordsReservedMin := atomic.LoadInt32(&c.cfg.ClientReqRecordsReservedMin)
	c.cfg.reqRateLimitMapMutex.RLock()
	defer c.cfg.reqRateLimitMapMutex.RUnlock()

	topoFetchIntervalMin := atomic.LoadInt64(&c.cfg.TopologyFetchIntervalMin)
	topoForceFetchIntervalSec := atomic.LoadInt64(&c.cfg.TopologyForceFetchIntervalSec)
	zoneNetConnConfig := c.cfg.getZoneNetConnConfigMap()
	cInfo = &proto.LimitInfo{
		Cluster:                                c.Name,
		MetaNodeDeleteBatchCount:               batchCount,
		MetaNodeDeleteWorkerSleepMs:            deleteSleepMs,
		MetaNodeReadDirLimitNum:                metaNodeReadDirLimitNum,
		DataNodeDeleteLimitRate:                deleteLimitRate,
		DataNodeRepairTaskLimitOnDisk:          repairTaskCount,
		DataNodeRepairClusterTaskLimitOnDisk:   clusterRepairTaskCount,
		DataNodeRepairSSDZoneTaskLimitOnDisk:   ssdZoneRepairTaskCount,
		DataNodeFlushFDInterval:                dataNodeFlushFDInterval,
		DataNodeFlushFDParallelismOnDisk:       dataNodeFlushFDParallelismOnDisk,
		DataPartitionConsistencyMode:           dataPartitionConsistencyMode,
		PersistenceMode:                        persistenceMode,
		DataNodeNormalExtentDeleteExpire:       normalExtentDeleteExpireTime,
		DataNodeRepairTaskCountZoneLimit:       c.cfg.DataNodeRepairTaskCountZoneLimit,
		NetworkFlowRatio:                       c.cfg.NetworkFlowRatio,
		RateLimit:                              c.cfg.RateLimit,
		FlashNodeLimitMap:                      c.cfg.FlashNodeLimitMap,
		FlashNodeVolLimitMap:                   c.cfg.FlashNodeVolLimitMap,
		ClientReadVolRateLimitMap:              c.cfg.ClientReadVolRateLimitMap,
		ClientWriteVolRateLimitMap:             c.cfg.ClientWriteVolRateLimitMap,
		ClientVolOpRateLimit:                   c.cfg.ClientVolOpRateLimitMap[volName],
		ObjectNodeActionRateLimit:              c.cfg.ObjectNodeActionRateLimitMap[volName],
		DataNodeFixTinyDeleteRecordLimitOnDisk: c.dnFixTinyDeleteRecordLimit,
		MetaNodeDelEkVolRateLimitMap:           c.cfg.MetaNodeDelEKVolRateLimitMap,
		MetaNodeDelEkZoneRateLimitMap:          c.cfg.MetaNodeDelEKZoneRateLimitMap,
		MetaNodeDumpWaterLevel:                 dumpWaterLevel,
		MonitorSummarySec:                      monitorSummarySec,
		MonitorReportSec:                       monitorReportSec,
		RocksdbDiskUsageThreshold:              c.cfg.MetaNodeRocksdbDiskThreshold,
		MemModeRocksdbDiskUsageThreshold:       c.cfg.MetaNodeMemModeRocksdbDiskThreshold,
		RocksDBDiskReservedSpace:               c.cfg.RocksDBDiskReservedSpace,
		LogMaxSize:                             c.cfg.LogMaxSize,
		MetaRockDBWalFileSize:                  metaRocksDBWalFileSize,
		MetaRocksWalMemSize:                    metaRocksDBWalMemSize,
		MetaRocksLogSize:                       metaRocksDBLogSize,
		MetaRocksLogReservedTime:               metaRocksDBLogReservedTime,
		MetaRocksLogReservedCnt:                metaRocksDBLogReservedCnt,
		MetaRocksDisableFlushFlag:              metaRocksDBDisableFlush,
		MetaRocksFlushWalInterval:              metaRocksDBFlushWalInterval,
		MetaRocksWalTTL:                        metaRocksDBWalTTL,
		DeleteEKRecordFileMaxMB:                metaDeleteEKRecordFilesMaxTotalSize,
		MetaTrashCleanInterval:                 metaTrashCleanInterval,
		MetaRaftLogSize:                        metaRaftLogSize,
		MetaRaftCap:                            metaRaftLogCap,
		MetaSyncWALOnUnstableEnableState:       c.cfg.MetaSyncWALOnUnstableEnableState,
		DataSyncWALOnUnstableEnableState:       c.cfg.DataSyncWALOnUnstableEnableState,
		DisableStrictVolZone:                   c.cfg.DisableStrictVolZone,
		AutoUpdatePartitionReplicaNum:          c.cfg.AutoUpdatePartitionReplicaNum,
		BitMapAllocatorMaxUsedFactor:           c.cfg.BitMapAllocatorMaxUsedFactor,
		BitMapAllocatorMinFreeFactor:           c.cfg.BitMapAllocatorMinFreeFactor,
		TrashItemCleanMaxCountEachTime:         trashCleanMaxCount,
		TrashCleanDurationEachTime:             trashCleanDuration,
		DeleteMarkDelVolInterval:               c.cfg.DeleteMarkDelVolInterval,
		RemoteCacheBoostEnable:                 c.cfg.RemoteCacheBoostEnable,
		DpTimeoutCntThreshold:                  int(dpTimeoutCntThreshold),
		ClientReqRecordsReservedCount:          clientReqRecordsReservedCount,
		ClientReqRecordsReservedMin:            clientReqRecordsReservedMin,
		ClientReqRemoveDupFlag:                 c.cfg.ClientReqRemoveDup,
		RemoteReadConnTimeout:                  c.cfg.RemoteReadConnTimeoutMs,
		ZoneNetConnConfig:                      zoneNetConnConfig,
		MetaNodeDumpSnapCountByZone:            c.cfg.MetaNodeDumpSnapCountByZone,
		TopologyFetchIntervalMin:               topoFetchIntervalMin,
		TopologyForceFetchIntervalSec:          topoForceFetchIntervalSec,
		DataNodeDiskReservedRatio:              c.cfg.DataNodeDiskReservedRatio,
		DisableClusterCheckDeleteEK:            c.cfg.DisableClusterCheckDeleteEK,
		DelayMinutesReduceReplicaNum:           c.cfg.delayMinutesReduceReplicaNum,
	}
	return
}

func (c *Cluster) getVolLimitInfoRespCache(name string) (data []byte, err error) {
	vol, err := c.getVol(name)
	if err != nil {
		return nil, proto.ErrLimitInfoIsNotCached
	}
	vol.limitInfoRespCacheMutex.RLock()
	defer vol.limitInfoRespCacheMutex.RUnlock()
	return vol.limitInfoRespCache, nil
}

func (vol *Vol) setVolLimitInfoRespCache(data []byte) {
	vol.limitInfoRespCacheMutex.Lock()
	defer vol.limitInfoRespCacheMutex.Unlock()
	vol.limitInfoRespCache = data
}

func (c *Cluster) updateVolLimitInfoRespCache(vol *Vol) {
	if !c.mustUsedVolLimitInfoRespCache(vol.Name) {
		return
	}
	limitInfo := c.buildLimitInfo(vol.Name)
	reply := newSuccessHTTPReply(limitInfo)
	data, mErr := json.Marshal(reply)
	if mErr != nil {
		return
	}
	vol.setVolLimitInfoRespCache(data)
}

func (c *Cluster) mustUsedVolLimitInfoRespCache(volName string) bool {
	c.cfg.reqRateLimitMapMutex.RLock()
	defer c.cfg.reqRateLimitMapMutex.RUnlock()
	_, volMetaOpLimitExist := c.cfg.ClientVolOpRateLimitMap[volName]
	_, objectActionLimitExist := c.cfg.ObjectNodeActionRateLimitMap[volName]
	return volMetaOpLimitExist || objectActionLimitExist
}
