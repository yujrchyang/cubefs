package data

import (
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/statistics"
	"sync/atomic"
)

func (client *ExtentClient) summaryMonitorData(deal func(data *statistics.MonitorData, vol, zone string, pid uint64)) {
	volume := client.dataWrapper.volName
	client.monitorStatistics.Range(func(key, value interface{}) bool {
		var (
			zone string
			ok   bool
		)
		if zone, ok = key.(string); !ok {
			client.monitorStatistics.Delete(key)
			return true
		}
		var dataList []*statistics.MonitorData
		if dataList, ok = value.([]*statistics.MonitorData); !ok {
			client.monitorStatistics.Delete(key)
			return true
		}
		for _, data := range dataList {
			deal(data, volume, zone, 0)
		}
		return true
	})
}

func (client *ExtentClient) updateMonitorData(zone string, action int, dataSize uint64) {
	v, found := client.monitorStatistics.Load(zone)
	if !found {
		v, _ = client.monitorStatistics.LoadOrStore(zone, statistics.InitMonitorData(statistics.ModelClient))
	}
	dataList, ok := v.([]*statistics.MonitorData)
	if !ok {
		client.monitorStatistics.Delete(zone)
		return
	}
	atomic.AddUint64(&dataList[action].Count, 1)
	atomic.AddUint64(&dataList[action].Size, dataSize)
}

func (s *Streamer) UpdateWrite(addr string, size uint64) {
	zone := s.client.dataWrapper.getHostZone(addr)
	if log.IsDebugEnabled() {
		log.LogDebugf("UpdateWrite: addr(%v) zone(%v)", addr, zone)
	}
	s.client.updateMonitorData(zone, proto.ActionClientWrite, size)
}

func (s *Streamer) UpdateOverWrite(addr string, size uint64) {
	zone := s.client.dataWrapper.getHostZone(addr)
	if log.IsDebugEnabled() {
		log.LogDebugf("UpdateOverWrite: addr(%v) zone(%v)", addr, zone)
	}
	s.client.updateMonitorData(zone, proto.ActionClientOverWrite, size)
}

func (s *Streamer) UpdateRead(addr string, size uint64) {
	zone := s.client.dataWrapper.getHostZone(addr)
	if log.IsDebugEnabled() {
		log.LogDebugf("UpdateRead: addr(%v) zone(%v)", addr, zone)
	}
	s.client.updateMonitorData(zone, proto.ActionClientRead, size)
}
