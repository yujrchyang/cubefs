package pingclient

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/util/iputil"
	"github.com/cubefs/cubefs/util/log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type RegionRankType uint8

func (t RegionRankType) String() string {
	switch t {
	case SameZoneRank:
		return "same-zone"
	case SameRegionRank:
		return "same-region"
	case CrossRegionRank:
		return "cross-region"
	case UnknownRegionRank:
		return "unknown-region"
	default:
	}
	return "undefined"
}

const (
	SameZoneRank RegionRankType = iota
	SameRegionRank
	CrossRegionRank
	UnknownRegionRank
)

const (
	pingTimeout       = 50 * time.Millisecond
	pingCount         = 3
	sameZoneTimeout   = 400 * time.Microsecond
	sameRegionTimeout = 2 * time.Millisecond
)

type PingClient struct {
	init      bool
	enable    atomic.Bool
	getIpList func() ([]string, error)
	ipPingMap map[string]RegionRankType
	closeCh   chan struct{}
	sync.RWMutex
}

// NewPingClient 注意，ipList成员为 "ip:port"
func NewPingClient(getIpList func() ([]string, error)) (client *PingClient) {
	client = &PingClient{
		getIpList: getIpList,
		closeCh:   make(chan struct{}, 1),
		ipPingMap: make(map[string]RegionRankType, 0),
	}
	http.HandleFunc("/ping/list", client.handleGetPingList)
	http.HandleFunc("/ping/set", client.handleSetPingEnable)
	return
}

func (client *PingClient) Start() (err error) {
	err = client.refreshIPList()
	if err != nil {
		return
	}
	go client.startScheduler()
	return
}

func (client *PingClient) Close() {
	client.closeCh <- struct{}{}
}

func (client *PingClient) SortByDistanceASC(hosts []string) {
	if !client.enable.Load() {
		return
	}
	client.RLock()
	defer client.RUnlock()
	sort.SliceStable(hosts, func(i, j int) bool {
		k, l := UnknownRegionRank, UnknownRegionRank
		if v, ok := client.ipPingMap[hosts[i]]; ok {
			k = v
		}
		if v, ok := client.ipPingMap[hosts[j]]; ok {
			l = v
		}
		return k < l
	})
}

func (client *PingClient) SortByDistanceDESC(hosts []string) {
	if !client.init || !client.enable.Load() {
		return
	}
	client.RLock()
	defer client.RUnlock()
	sort.SliceStable(hosts, func(i, j int) bool {
		k, l := UnknownRegionRank, UnknownRegionRank
		if v, ok := client.ipPingMap[hosts[i]]; ok {
			k = v
		}
		if v, ok := client.ipPingMap[hosts[j]]; ok {
			l = v
		}
		return k > l
	})
}

func (client *PingClient) ForceExecutePing() {
	client.executePing()
}

func (client *PingClient) SetPingEnable(enable bool) {
	client.enable.Store(enable)
}

func (client *PingClient) GetPingEnable() bool {
	return client.enable.Load()
}

func (client *PingClient) handleGetPingList(w http.ResponseWriter, r *http.Request) {
	data, err := json.Marshal(client.ipPingMap)
	if err != nil {
		w.Write([]byte(fmt.Sprintf("marshal failed:%v", err)))
		return
	}
	w.Write(data)
}

func (client *PingClient) handleSetPingEnable(w http.ResponseWriter, r *http.Request) {
	val := r.FormValue("enable")
	if val == "" {
		w.Write([]byte("parameter enable is empty"))
		return
	}
	enable, err := strconv.ParseBool(val)
	if err != nil {
		w.Write([]byte(fmt.Sprintf("parse enable failed:%v", err)))
		return
	}
	client.SetPingEnable(enable)
	w.Write([]byte(fmt.Sprintf("set ping enable to:%v success", enable)))
}

func (client *PingClient) startScheduler() {
	var err error
	updateIpTick := time.NewTicker(10 * time.Minute)
	pingTick := time.NewTicker(2 * time.Minute)
	defer pingTick.Stop()
	defer updateIpTick.Stop()
	for {
		select {
		case <-updateIpTick.C:
			if !client.enable.Load() {
				continue
			}
			err = client.refreshIPList()
			if err != nil {
				continue
			}
		case <-pingTick.C:
			client.executePing()
		case <-client.closeCh:
			return
		}
	}
}

func (client *PingClient) executePing() {
	var err error
	var avgTime time.Duration
	pingMap := make(map[string]RegionRankType, 0)

	client.RLock()
	for k, v := range client.ipPingMap {
		pingMap[k] = v
	}
	client.RUnlock()

	for host := range pingMap {
		avgTime, err = iputil.PingWithTimeout(strings.Split(host, ":")[0], pingCount, pingTimeout*pingCount)
		if err != nil {
			log.LogErrorf("ping host[%v] failed, err:%v", host, err)
			continue
		}
		log.LogDebugf("executePing: host(%v) ping time(%v) err(%v)", host, avgTime, err)
		switch {
		case avgTime < sameZoneTimeout:
			pingMap[host] = SameZoneRank
		case avgTime < sameRegionTimeout:
			pingMap[host] = SameRegionRank
		default:
			pingMap[host] = CrossRegionRank
		}
	}

	client.Lock()
	defer client.Unlock()
	for k, v := range pingMap {
		if _, ok := client.ipPingMap[k]; ok {
			client.ipPingMap[k] = v
		}
	}
	client.init = true
	return
}

func (client *PingClient) refreshIPList() (err error) {
	var hosts []string
	hosts, err = client.getIpList()
	if err != nil {
		log.LogErrorf("get hosts failed, err:%v", err)
		return
	}

	client.Lock()
	defer client.Unlock()
	newHostMap := make(map[string]RegionRankType, 0)
	for _, h := range hosts {
		if v, ok := client.ipPingMap[h]; ok {
			newHostMap[h] = v
		} else {
			newHostMap[h] = UnknownRegionRank
		}
	}
	client.ipPingMap = newHostMap
	return
}
