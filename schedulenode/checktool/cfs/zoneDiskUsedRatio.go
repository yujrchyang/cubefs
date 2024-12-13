package cfs

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/schedulenode/common/cfs"
	"github.com/cubefs/cubefs/util/checktool"
	"github.com/cubefs/cubefs/util/log"
	"strings"
	"sync"
	"time"
)

type AlarmInfo struct {
	shouldAlarm bool
	alarmMsg    string
}

type PackAlarmInfo struct {
	shouldAlarm    bool
	shouldTelAlarm bool
	nodeType       int
	zoneAlarmInfo  map[string]*AlarmInfo
}

func (s *ChubaoFSMonitor) scheduleToCheckZoneDiskUsedRatio() {
	s.checkZoneDiskUsedRatio()
	for {
		t := time.NewTimer(time.Duration(s.scheduleInterval) * time.Second)
		select {
		case <-s.ctx.Done():
			return
		case <-t.C:
			s.checkZoneDiskUsedRatio()
		}
	}
}

func (s *ChubaoFSMonitor) checkZoneDiskUsedRatio() {
	var wg sync.WaitGroup
	clusterDataNodeUsedRatioAlarmInfo := make(map[string]*PackAlarmInfo)
	clusterMetaNodeUsedRatioAlarmInfo := make(map[string]*PackAlarmInfo)
	clusterUsedRatioAlarmInfoLock := new(sync.RWMutex)
	for _, host := range s.hosts {
		wg.Add(1)
		go func(host *ClusterHost) {
			defer checktool.HandleCrash()
			defer wg.Done()
			if host.isReleaseCluster {
				// dbbak master /cluster/stat does not exist
				return
			}
			log.LogDebugf("checkZoneDiskUsedRatio [%v] begin", host)
			startTime := time.Now()
			csv, err := cfs.GetClusterStat(host.host, host.isReleaseCluster)
			if err != nil {
				_, ok := err.(*json.SyntaxError)
				if ok {
					return
				}
				log.LogErrorf("get cluster stat info from %v failed,err:%v", host.host, err)
				return
			}
			var dataNodeAlarmInfo, metaNodeAlarmInfo *PackAlarmInfo
			dataNodeAlarmInfo, metaNodeAlarmInfo = s.doCheckZoneDiskUsedRatio(csv, host)
			clusterUsedRatioAlarmInfoLock.Lock()
			clusterDataNodeUsedRatioAlarmInfo[host.host] = dataNodeAlarmInfo
			clusterMetaNodeUsedRatioAlarmInfo[host.host] = metaNodeAlarmInfo
			clusterUsedRatioAlarmInfoLock.Unlock()
			log.LogDebugf("checkZoneDiskUsedRatio [%v] end,cost[%v]", host, time.Since(startTime))
		}(host)
	}
	wg.Wait()
	s.alarmDataNode(clusterDataNodeUsedRatioAlarmInfo)
	s.alarmMataNode(clusterMetaNodeUsedRatioAlarmInfo)
}

func (s *ChubaoFSMonitor) alarmDataNode(clusterUsedRatioAlarmInfo map[string]*PackAlarmInfo) {
	finalAlarmMsg := strings.Builder{}
	shouldAlarm := false
	shouldTelAlarm := false
	for host, zoneAlarmInfo := range clusterUsedRatioAlarmInfo {
		if !zoneAlarmInfo.shouldAlarm {
			continue
		}
		if zoneAlarmInfo.shouldTelAlarm {
			shouldTelAlarm = true
		}
		finalAlarmMsg.WriteString(fmt.Sprintf("domain[%v] ", host))
		for _, alarmInfo := range zoneAlarmInfo.zoneAlarmInfo {
			if !alarmInfo.shouldAlarm {
				continue
			}
			finalAlarmMsg.WriteString(alarmInfo.alarmMsg)
			shouldAlarm = true
		}
	}
	alarmMsg := finalAlarmMsg.String()
	if shouldAlarm {
		if time.Since(s.lastZoneDataNodeDiskUsedRatioAlarmTime) > 30*time.Minute {
			warnBySpecialUmpKeyWithPrefix(UMPCFSNormalWarnKey, alarmMsg)
			s.lastZoneDataNodeDiskUsedRatioAlarmTime = time.Now()
		}
	}
	if shouldTelAlarm {
		if time.Since(s.lastZoneDataNodeDiskUsedRatioTelAlarm) > 60*time.Minute {
			warnBySpecialUmpKeyWithPrefix(UMPCFSZoneUsedRatioWarnKey, alarmMsg)
			s.lastZoneDataNodeDiskUsedRatioTelAlarm = time.Now()
		}
		if time.Since(s.lastZoneDataNodeDiskUsedRatioTelOpAlarm) > 30*time.Minute {
			warnBySpecialUmpKeyWithPrefix(UMPCFSZoneUsedRatioOPWarnKey, alarmMsg)
			s.lastZoneDataNodeDiskUsedRatioTelOpAlarm = time.Now()
		}
	}
}

func (s *ChubaoFSMonitor) alarmMataNode(clusterUsedRatioAlarmInfo map[string]*PackAlarmInfo) {
	finalAlarmMsg := strings.Builder{}
	shouldAlarm := false
	shouldTelAlarm := false
	for host, zoneAlarmInfo := range clusterUsedRatioAlarmInfo {
		if !zoneAlarmInfo.shouldAlarm {
			continue
		}
		if zoneAlarmInfo.shouldTelAlarm {
			shouldTelAlarm = true
		}
		finalAlarmMsg.WriteString(fmt.Sprintf("domain[%v] ", host))
		for _, alarmInfo := range zoneAlarmInfo.zoneAlarmInfo {
			if !alarmInfo.shouldAlarm {
				continue
			}
			finalAlarmMsg.WriteString(alarmInfo.alarmMsg)
			shouldAlarm = true
		}
	}
	alarmMsg := finalAlarmMsg.String()
	if shouldAlarm {
		if time.Since(s.lastZoneMetaNodeDiskUsedRatioAlarmTime) > 10*time.Minute {
			warnBySpecialUmpKeyWithPrefix(UMPCFSNormalWarnKey, alarmMsg)
			s.lastZoneMetaNodeDiskUsedRatioAlarmTime = time.Now()
		}
	}
	if shouldTelAlarm {
		if time.Since(s.lastZoneMetaNodeDiskUsedRatioTelAlarm) > 20*time.Minute {
			warnBySpecialUmpKeyWithPrefix(UMPCFSZoneUsedRatioWarnKey, alarmMsg)
			s.lastZoneMetaNodeDiskUsedRatioTelAlarm = time.Now()
		}
		if time.Since(s.lastZoneMetaNodeDiskUsedRatioTelOpAlarm) > 10*time.Minute {
			warnBySpecialUmpKeyWithPrefix(UMPCFSZoneUsedRatioOPWarnKey, alarmMsg)
			s.lastZoneMetaNodeDiskUsedRatioTelOpAlarm = time.Now()
		}
	}
}

func (s *ChubaoFSMonitor) doCheckZoneDiskUsedRatio(csv *cfs.ClusterStatInfoView, host *ClusterHost) (dataNodeAlarmInfo, metaNodeAlarmInfo *PackAlarmInfo) {
	dataNodeAlarmInfo = s.doCheckZoneDataNodeDiskUsedRatio(csv, host)
	metaNodeAlarmInfo = s.doCheckZoneMetaNodeDiskUsedRatio(csv, host)
	return
}

func (s *ChubaoFSMonitor) doCheckZoneDataNodeDiskUsedRatio(csv *cfs.ClusterStatInfoView, host *ClusterHost) (collectAlarmInfo *PackAlarmInfo) {
	collectAlarmInfo = &PackAlarmInfo{
		nodeType:      dataNodeType,
		zoneAlarmInfo: make(map[string]*AlarmInfo),
	}
	var usedRatioMin, usedRatioMax, usedRatioMinSSD, usedRatioMaxSSD float64
	usedRatioMin = s.dataNodeUsedRatioMinThresholdHDD
	usedRatioMax = s.dataNodeUsedRatioMinThresholdHDD + 0.05
	usedRatioMinSSD = s.dataNodeUsedRatioMinThresholdSSD
	usedRatioMaxSSD = s.dataNodeUsedRatioMinThresholdSSD + 0.05
	for zoneName, zoneStat := range csv.ZoneStatInfo {
		var normalAlarmRatio, telAlarmRatio float64
		if host.isSSDZone(zoneName) {
			normalAlarmRatio = usedRatioMinSSD
			telAlarmRatio = usedRatioMaxSSD
		} else {
			normalAlarmRatio = usedRatioMin
			telAlarmRatio = usedRatioMax
		}
		alarmInfo := &AlarmInfo{}
		zoneAlarmInfo := fmt.Sprintf("zone[%v]:", zoneName)
		if zoneStat.DataNodeStat.UsedRatio > normalAlarmRatio {
			collectAlarmInfo.shouldAlarm = true
			alarmInfo.shouldAlarm = true
			zoneAlarmInfo += fmt.Sprintf(" datanode usedRatio is now %v,", zoneStat.DataNodeStat.UsedRatio)
		}
		if zoneStat.DataNodeStat.UsedRatio >= telAlarmRatio {
			collectAlarmInfo.shouldTelAlarm = true
		}
		alarmInfo.alarmMsg = zoneAlarmInfo
		collectAlarmInfo.zoneAlarmInfo[zoneName] = alarmInfo
	}
	return
}

func (s *ChubaoFSMonitor) doCheckZoneMetaNodeDiskUsedRatio(csv *cfs.ClusterStatInfoView, host *ClusterHost) (collectAlarmInfo *PackAlarmInfo) {
	collectAlarmInfo = &PackAlarmInfo{
		nodeType:      metaNodeType,
		zoneAlarmInfo: make(map[string]*AlarmInfo),
	}
	var usedRatio, usedRatioSSD float64
	usedRatio = s.metaNodeUsedRatioMinThresholdHDD
	usedRatioSSD = s.metaNodeUsedRatioMinThresholdSSD
	for zoneName, zoneStat := range csv.ZoneStatInfo {
		var telAlarmRatio float64
		if host.isSSDZone(zoneName) {
			telAlarmRatio = usedRatioSSD
		} else {
			telAlarmRatio = usedRatio
		}
		alarmInfo := &AlarmInfo{}
		zoneAlarmInfo := fmt.Sprintf("zone[%v]:", zoneName)
		if zoneStat.MetaNodeStat.UsedRatio >= telAlarmRatio {
			collectAlarmInfo.shouldAlarm = true
			alarmInfo.shouldAlarm = true
			zoneAlarmInfo += fmt.Sprintf(" metanode usedRatio is now %v,\n", zoneStat.MetaNodeStat.UsedRatio)
			collectAlarmInfo.shouldTelAlarm = true
		}
		alarmInfo.alarmMsg = zoneAlarmInfo
		collectAlarmInfo.zoneAlarmInfo[zoneName] = alarmInfo
	}
	return
}

func (ch *ClusterHost) isSSDZone(zoneName string) (ok bool) {
	//elasticdb 集群 包含hdd的是hdd,其它为ssd, 即不包含hdd则为ssd
	//spark集群 包含ssd的是ssd，其它为hdd
	switch ch.host {
	case DomainMysql:
		return !strings.Contains(zoneName, "hdd")
	case DomainSpark:
		return strings.Contains(zoneName, "ssd") || strings.Contains(zoneName, "sfx")
	default:
		return false
	}
}
