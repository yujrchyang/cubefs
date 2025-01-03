package data

import (
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/common"
	"github.com/stretchr/testify/assert"
)

func initTestWrapper() (*Wrapper, error) {
	dataWrapper, err := NewDataPartitionWrapper(ltptestVolume, ltptestMaster, Normal, 0, 0, nil)
	if err != nil {
		return nil, err
	}

	dataWrapper.HostsDelay.Store(ip1, 100*time.Microsecond)
	dataWrapper.HostsDelay.Store(ip2, 200*time.Microsecond)
	dataWrapper.HostsDelay.Store(ip3, 300*time.Microsecond)
	dataWrapper.HostsDelay.Store(ip4, 400*time.Microsecond)
	dataWrapper.HostsDelay.Store(ip5, 500*time.Microsecond)

	return dataWrapper, nil
}

func Test_getSameZoneReadHost(t *testing.T) {
	dataWrapper, err := initTestWrapper()
	assert.Nil(t, err, "NewDataWrapper failed")

	dp := &DataPartition{
		DataPartitionResponse: proto.DataPartitionResponse{Hosts: []string{ip3, ip1, ip4, ip2, ip5}, LeaderAddr: proto.NewAtomicString(ip3)},
		ClientWrapper:         dataWrapper,
	}

	testCases := []struct {
		name          string
		pingRule      string
		rankedPing    [][]time.Duration
		sameZoneHosts []string
	}{
		{
			name:          "empty rule",
			pingRule:      "",
			rankedPing:    nil,
			sameZoneHosts: nil,
		},
		{
			name:     "default rule",
			pingRule: defaultTwoZoneHTypePingRule,
			rankedPing: [][]time.Duration{
				{400 * time.Microsecond},
			},
			sameZoneHosts: []string{ip1, ip2, ip3, ip4},
		},
		{
			name:     "all in sameZone",
			pingRule: "500",
			rankedPing: [][]time.Duration{
				{500 * time.Microsecond},
			},
			sameZoneHosts: []string{ip1, ip2, ip3, ip4, ip5},
		},
		{
			name:     "empty sameZone",
			pingRule: "90",
			rankedPing: [][]time.Duration{
				{90 * time.Microsecond},
			},
			sameZoneHosts: nil,
		},
		{
			name:     "one in sameZone",
			pingRule: "90-100",
			rankedPing: [][]time.Duration{
				{90 * time.Microsecond, 100 * time.Microsecond},
			},
			sameZoneHosts: []string{ip1},
		},
		{
			name:     "all in sameZone(step)",
			pingRule: "100-500,600",
			rankedPing: [][]time.Duration{
				{100 * time.Microsecond, 500 * time.Microsecond},
				{600 * time.Microsecond},
			},
			sameZoneHosts: []string{ip1, ip2, ip3, ip4, ip5},
		},
	}
	for _, tt := range testCases {
		rankedPing := common.ParseRankedRuleStr(tt.pingRule)
		assert.Equal(t, tt.rankedPing, rankedPing, "%v rankedRule failed", tt.name)
		dataWrapper.twoZoneHATypeRankedPing = tt.rankedPing

		dp.refreshHostPingElapsed()
		sameZoneHosts := dp.getSameZoneHostByPingElapsed()
		assert.Equal(t, tt.sameZoneHosts, sameZoneHosts, "%v rankedHost failed", tt.name)

		if tt.name == "one in sameZone" {
			for i := 0; i < hostErrCountLimit; i++ {
				dp.updateSameZoneHostRank(ip1)
			}
			sameZoneHosts = dp.getSameZoneHostByPingElapsed()
			assert.Equal(t, 0, len(sameZoneHosts), "sameZoneHost should be empty")

			readHost := dp.getSameZoneReadHost()
			assert.Equal(t, ip3, readHost, "readHost should be leader")

			dp.refreshHostPingElapsed()
			sameZoneHosts = dp.getSameZoneHostByPingElapsed()
			assert.Equal(t, tt.sameZoneHosts, sameZoneHosts, "after refresh failed: %v", tt.name)

			readHost = dp.getSameZoneReadHost()
			assert.Equal(t, tt.sameZoneHosts[0], readHost, "readHost after refresh: %v", tt.name)
		}
	}
}

func Test_sortHostsByPingElapsed(t *testing.T) {
	dataWrapper, err := initTestWrapper()
	assert.Nil(t, err, "NewDataWrapper failed")

	dp := &DataPartition{
		DataPartitionResponse: proto.DataPartitionResponse{Hosts: []string{ip1, ip2, ip3, ip4, ip5}, LeaderAddr: proto.NewAtomicString(ip3)},
		ClientWrapper:         dataWrapper,
	}

	sortedHost := dp.sortHostsByPingElapsed()
	assert.Equal(t, len(dp.Hosts), len(sortedHost))

	expectSorted := []string{ip1, ip2, ip3, ip4, ip5}
	for i, expect := range expectSorted {
		assert.Equal(t, expect, sortedHost[i])
	}
}
