package common

import (
	"github.com/cubefs/cubefs/util/log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultUpdateInterval = 10
)

type hostPingElapsed struct {
	host    string
	elapsed time.Duration
}

type PingElapsedSortedHosts struct {
	sortedHosts  []*hostPingElapsed
	updateTSUnix int64      // Timestamp (unix second) of latest update.
	rankedHost   [][]string // 级别 和属于该级别的host
	hostErrCount sync.Map   // 周期内host累计失败次数

	getHosts      func() (hosts []string)
	getElapsed    func(host string) (elapsed time.Duration, ok bool)
	getRankedRule func() [][]time.Duration
}

func NewPingElapsedSortHosts(getHosts func() []string, getElapsed func(host string) (time.Duration, bool),
	getRankedRule func() [][]time.Duration) *PingElapsedSortedHosts {
	return &PingElapsedSortedHosts{
		getHosts:      getHosts,
		getElapsed:    getElapsed,
		getRankedRule: getRankedRule,
	}
}

func (h *PingElapsedSortedHosts) getSortedHosts() []string {
	sortedHosts := h.sortedHosts
	hosts := make([]string, 0, len(sortedHosts))
	for _, hostElapsed := range sortedHosts {
		hosts = append(hosts, hostElapsed.host)
	}
	if log.IsDebugEnabled() {
		log.LogDebugf("getSortedHosts: sortedHost(%v)", hosts)
	}
	return hosts
}

func (h *PingElapsedSortedHosts) update(getHosts func() []string, getElapsed func(host string) (time.Duration, bool)) []string {
	var hosts = getHosts()
	hostElapses := make([]*hostPingElapsed, 0, len(hosts))
	for _, host := range hosts {
		var hostElapsed *hostPingElapsed
		if elapsed, ok := getElapsed(host); ok {
			hostElapsed = &hostPingElapsed{host: host, elapsed: elapsed}
		} else {
			hostElapsed = &hostPingElapsed{host: host, elapsed: time.Duration(0)}
		}
		hostElapses = append(hostElapses, hostElapsed)
	}
	sort.SliceStable(hostElapses, func(i, j int) bool {
		return hostElapses[j].elapsed == 0 || hostElapses[i].elapsed < hostElapses[j].elapsed
	})
	sorted := make([]string, len(hostElapses))
	for i, hotElapsed := range hostElapses {
		sorted[i] = hotElapsed.host
	}

	h.sortedHosts = hostElapses
	if log.IsDebugEnabled() {
		log.LogDebugf("PingElapsedSortedHosts: update host: %v", sorted)
	}
	return sorted
}

func (h *PingElapsedSortedHosts) classifyHostsByPingRule(pingRules [][]time.Duration) (rankedHost [][]string) {
	if pingRules == nil {
		return
	}
	rankedHost = make([][]string, len(pingRules)+1)

	var chooseLevelHost = func(timeout time.Duration, pingRule []time.Duration) bool {
		for _, pingStep := range pingRule {
			if timeout > 0 && timeout <= pingStep {
				return true
			}
		}
		return false
	}
	// pingRules需要是升序，否则有问题
	sortedHosts := h.sortedHosts
	for _, host := range sortedHosts {
		if host.elapsed <= time.Duration(0) {
			rankedHost[len(pingRules)] = append(rankedHost[len(pingRules)], host.host)
			continue
		}
		placed := false
		for i, pingRule := range pingRules {
			satisfied := chooseLevelHost(host.elapsed, pingRule)
			if satisfied {
				rankedHost[i] = append(rankedHost[i], host.host)
				placed = true
				break
			}
		}
		if !placed {
			rankedHost[len(pingRules)] = append(rankedHost[len(pingRules)], host.host)
		}
	}

	h.rankedHost = rankedHost
	if log.IsDebugEnabled() {
		log.LogDebugf("RefreshRankedHost: classifyHostsByPingRule rankedHost(%v) by rule(%v)", h.rankedHost, pingRules)
	}
	return
}

func (h *PingElapsedSortedHosts) GetSortedHosts() []string {
	//if isNeedUpdate(h.updateTSUnix) {
	//	return h.update(h.getHosts, h.getElapsed)
	//}
	return h.getSortedHosts()
}

func (h *PingElapsedSortedHosts) GetRankedHost() (rankedHosts [][]string) {
	rankedHosts = h.rankedHost
	return
}

func (h *PingElapsedSortedHosts) StoreAndLoadHostErrCount(addr string) int {
	var errCount int
	if v, ok := h.hostErrCount.Load(addr); !ok {
		errCount = 0
	} else {
		errCount = v.(int)
	}
	h.hostErrCount.Store(addr, errCount+1)
	return errCount + 1
}

func (h *PingElapsedSortedHosts) RefreshSortedHost() {
	h.update(h.getHosts, h.getElapsed)
}

func (h *PingElapsedSortedHosts) RefreshRankedHost() {
	h.classifyHostsByPingRule(h.getRankedRule())
}

func (h *PingElapsedSortedHosts) RefreshHostErrMap() {
	// 清空map
	h.hostErrCount = sync.Map{}
}

func (h *PingElapsedSortedHosts) MoveToTailSortedHost(addr string) {
	reRangeHost := make([]*hostPingElapsed, 0)
	moved := false
	sortedHosts := h.sortedHosts
	for i, hostPing := range sortedHosts {
		if hostPing.host == addr {
			moved = true
			reRangeHost = append(sortedHosts[:i], sortedHosts[i+1:]...)
			reRangeHost = append(reRangeHost, hostPing)
			break
		}
	}
	if moved {
		if log.IsDebugEnabled() {
			log.LogDebugf("MoveToTailSortedHost: addr(%v) before(%v) after(%v)", addr, sortedHosts, reRangeHost)
		}
		h.sortedHosts = reRangeHost
	}
}

func (h *PingElapsedSortedHosts) MoveToLastRank(addr string) {
	rankedHosts := h.rankedHost
	if rankedHosts == nil {
		return
	}

	moved := false
	for rank, rankedHost := range rankedHosts {
		for i, host := range rankedHost {
			if host == addr {
				moved = true
				rankedHosts[rank] = append(rankedHost[:i], rankedHost[i+1:]...)
				break
			}
		}
		if moved {
			break
		}
	}

	lastRank := rankedHosts[len(rankedHosts)-1]
	lastRank = append(lastRank, addr)
	rankedHosts[len(rankedHosts)-1] = lastRank

	h.rankedHost = rankedHosts
}

func isNeedUpdate(ts int64) bool {
	return ts == 0 || time.Now().Unix()-ts > defaultUpdateInterval
}

func ParseRankedRuleStr(ruleStr string) [][]time.Duration {
	if strings.TrimSpace(ruleStr) == "" {
		return nil
	}
	levels := strings.Split(ruleStr, ",")
	rule := make([][]time.Duration, len(levels))
	for i, level := range levels {
		levelStep := strings.Split(level, "-")
		step := make([]time.Duration, len(levelStep))

		for j, s := range levelStep {
			v, err := strconv.ParseInt(s, 10, 64)
			if err == nil {
				step[j] = time.Duration(v) * time.Microsecond
			}
		}
		rule[i] = step
	}
	return rule
}
