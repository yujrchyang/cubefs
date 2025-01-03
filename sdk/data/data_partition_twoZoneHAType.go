package data

import (
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/common"
	"github.com/cubefs/cubefs/util/log"
)

const hostErrCountLimit = 5

// var pingRuleRegexp = regexp.MustCompile(`^(\d+(-\d+)*)(,(\d+(-\d+)*))*$`)
var pingRuleRegexp = regexp.MustCompile(`^(\d+(,?\d+(-\d+)*)*)*$`)

func (dp *DataPartition) getSameZoneReadHost() string {
	sameZoneHosts := dp.getSameZoneHostByPingElapsed()
	if len(sameZoneHosts) > 0 {
		err, host := dp.getEpochReadHost(sameZoneHosts)
		if err == nil {
			if log.IsDebugEnabled() {
				log.LogDebugf("getSameZoneReadHost: dp(%v) host(%v)", dp.PartitionID, host)
			}
			return host
		}
	}
	if log.IsDebugEnabled() {
		log.LogDebugf("getSameZoneReadHost: dp(%v) leader(%v)", dp.PartitionID, dp.GetLeaderAddr())
	}
	return dp.GetLeaderAddr()
}

func (dp *DataPartition) getSameZoneHostByPingElapsed() []string {
	if dp.pingElapsedSortedHosts != nil {
		rankedHost := dp.pingElapsedSortedHosts.GetRankedHost()
		if len(rankedHost) > 0 {
			return rankedHost[0]
		}
	}
	return nil
}

func (dp *DataPartition) sortHostsByPingElapsed() []string {
	if dp.pingElapsedSortedHosts == nil {
		dp.refreshHostPingElapsed()
	}
	return dp.pingElapsedSortedHosts.GetSortedHosts()
}

func (dp *DataPartition) refreshHostPingElapsed() {
	// 等于nil的情况: 初始化时，或者刚切换到该高可用类型, 或者新dp
	if dp.pingElapsedSortedHosts == nil {
		var getHosts = func() []string {
			return dp.Hosts
		}
		var getElapsed = func(host string) (time.Duration, bool) {
			delay, ok := dp.ClientWrapper.HostsDelay.Load(host)
			if !ok {
				return 0, false
			}
			return delay.(time.Duration), true
		}
		var rankedRule = func() [][]time.Duration {
			return dp.ClientWrapper.twoZoneHATypeRankedPing
		}
		dp.pingElapsedSortedHosts = common.NewPingElapsedSortHosts(getHosts, getElapsed, rankedRule)
	}
	dp.pingElapsedSortedHosts.RefreshSortedHost()
	dp.pingElapsedSortedHosts.RefreshRankedHost()
	dp.pingElapsedSortedHosts.RefreshHostErrMap()
	if log.IsDebugEnabled() {
		log.LogDebugf("refreshHostPingElapsed: dp(%v) pingRule(%s) sortedHost(%v) rankedHost(%v)", dp.PartitionID,
			dp.ClientWrapper.twoZoneHATypePingRule, dp.pingElapsedSortedHosts.GetSortedHosts(), dp.pingElapsedSortedHosts.GetRankedHost())
	}
}

func (dp *DataPartition) updateSameZoneHostRank(currAddr string) {
	moved := false
	errCount := dp.pingElapsedSortedHosts.StoreAndLoadHostErrCount(currAddr)
	if errCount >= hostErrCountLimit {
		dp.pingElapsedSortedHosts.MoveToLastRank(currAddr)
		moved = true
	}
	if log.IsDebugEnabled() {
		log.LogDebugf("updateSameZoneHostRank: host(%s) moved(%v)", currAddr, moved)
	}
}

func (w *Wrapper) IsSameZoneReadHAType() bool {
	return w.crossRegionHAType == proto.TwoZoneHATypeQuorum
}

func (w *Wrapper) getPingElapsedTh() time.Duration {
	if w.IsSameZoneReadHAType() {
		rule := w.twoZoneHATypePingRule
		if rule == "" {
			return sameZoneTimeout
		}
		sameZoneRule := strings.Split(rule, ",")[0]
		if sameZoneRule == "" {
			return sameZoneTimeout
		}
		step := strings.Split(sameZoneRule, "-")
		// 选同zone阶梯中最大的
		th, err := strconv.ParseInt(step[len(step)-1], 10, 64)
		if err != nil {
			return sameZoneTimeout
		}
		return time.Duration(th) * time.Microsecond
	}
	return 0
}

func (w *Wrapper) getHostZone(addr string) string {
	hostsStatus := w.HostsStatus
	if hs, ok := hostsStatus[addr]; ok {
		return hs.zone
	}
	return "unknown"
}

func validPingRule(rule string) bool {
	// ""不需要单独处理
	isValid := pingRuleRegexp.MatchString(rule)
	if log.IsDebugEnabled() {
		log.LogDebugf("validPingRule: rule(%v) isValid(%v)", rule, isValid)
	}
	return isValid
}
