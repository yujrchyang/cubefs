package meta

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/common"
	"github.com/cubefs/cubefs/util/log"
)

func (mw *MetaWrapper) sendReadToNearHost(ctx context.Context, mp *MetaPartition, req *proto.Packet) (resp *proto.Packet, err error) {
	sortedHost := mw.sortHostsByPingElapsed(mp)
	if len(sortedHost) == 0 {
		log.LogWarnf("sendReadToNearHost: mp(%v) sortedHost empty", mp.PartitionID)
		sortedHost = mp.Members
	}
	// 不允许转发，todo: leader会否报错
	req.SetFollowerReadMetaPkt()

	resp, _, err = mw.sendToHost(ctx, mp, req, sortedHost[0])
	if err == nil && !resp.ShouldRetry() || err == proto.ErrVolNotExists {
		if log.IsDebugEnabled() {
			log.LogDebugf("sendReadToNearHost: mp(%v) sorted[0](%v) success", mp.PartitionID, sortedHost[0])
		}
		return
	}
	mp.updateSameZoneHostRank(sortedHost[0])
	log.LogWarnf("sendReadToNearHost: sorted[0]=%v failed, err: %v, range host", sortedHost[0], err)

	start := time.Now()
	retryInterval := SendRetryInterval
	hostLen := len(sortedHost)
	for i := 1; i < SendRetryLimit; i++ {
		addr := sortedHost[i%hostLen]
		resp, _, err = mw.sendToHost(ctx, mp, req, addr)
		if err == nil && !resp.ShouldRetry() || err == proto.ErrVolNotExists {
			if log.IsDebugEnabled() {
				log.LogDebugf("retry[%v] success addr[%v]", i, addr)
			}
			return resp, err
		}
		if err == nil {
			log.LogWarnf("addr(%s) request should retry[%v]", addr, resp.GetResultMsg())
		}
		if time.Since(start) > SendTimeLimit {
			log.LogWarnf("sendToMetaPartition: retry timeout req(%v) mp(%v) time(%v)", req, mp, time.Since(start))
			break
		}
		time.Sleep(retryInterval)
		retryInterval += SendRetryInterval
	}

	log.LogWarnf("sendToMetaPartition: reach retry limit, err(%v) resp(%v)", err, resp)
	if err != nil || resp == nil {
		err = errors.New(fmt.Sprintf("err: %v, resp: %v", err, resp))
	}
	return
}

func (mw *MetaWrapper) IsSameZoneReadHAType() bool {
	return mw.crossRegionHAType == proto.TwoZoneHATypeQuorum
}

// 需要跟replaceOrInsert方法绑定在一起
func (mw *MetaWrapper) refreshHostPingElapsed(mp *MetaPartition) {
	if mp.pingElapsedSortedHosts == nil {
		var getHosts = func() []string {
			return ExcludeLearner(mp)
		}
		var getElapsed = func(host string) (time.Duration, bool) {
			delay, ok := mw.HostsDelay.Load(host)
			if !ok {
				return 0, false
			}
			return delay.(time.Duration), true
		}
		var rankedRule = func() [][]time.Duration { return nil }
		mp.pingElapsedSortedHosts = common.NewPingElapsedSortHosts(getHosts, getElapsed, rankedRule)
	}
	mp.pingElapsedSortedHosts.RefreshSortedHost()
	mp.pingElapsedSortedHosts.RefreshHostErrMap()
	if log.IsDebugEnabled() {
		log.LogDebugf("refreshHostPingElapsedP: mp(%v) sortedHost(%v)", mp.PartitionID, mp.pingElapsedSortedHosts.GetSortedHosts())
	}
}

func (mw *MetaWrapper) sortHostsByPingElapsed(mp *MetaPartition) []string {
	if mp.pingElapsedSortedHosts == nil {
		mw.refreshHostPingElapsed(mp)
	}
	return mp.pingElapsedSortedHosts.GetSortedHosts()
}

// todo: rename
func (mp *MetaPartition) updateSameZoneHostRank(currAddr string) {
	errCount := mp.pingElapsedSortedHosts.StoreAndLoadHostErrCount(currAddr)
	if errCount >= hostErrCountLimit {
		mp.pingElapsedSortedHosts.MoveToTailSortedHost(currAddr)
	}
	if log.IsDebugEnabled() {
		log.LogDebugf("updateSameZoneHostRank: host(%s)", currAddr)
	}
}
