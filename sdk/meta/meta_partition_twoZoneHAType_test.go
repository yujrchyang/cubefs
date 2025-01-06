package meta

import (
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm/utils"
	"testing"
	"time"
)

var (
	ip1 = "192.168.0.31:17210"
	ip2 = "192.168.0.32:17210"
	ip3 = "192.168.0.33:17210"
	ip4 = "192.168.0.34:17210"
	ip5 = "192.168.0.35:17210"
)

func Test_refreshHostPingElapsed(t *testing.T) {
	assert.NotNil(t, mw, "new testMetaWrapper failed")

	mw.HostsDelay.Store(ip1, 100*time.Microsecond)
	mw.HostsDelay.Store(ip2, 200*time.Microsecond)
	mw.HostsDelay.Store(ip3, 300*time.Microsecond)
	mw.HostsDelay.Store(ip4, 400*time.Microsecond)
	mw.HostsDelay.Store(ip5, 500*time.Microsecond)

	mp := &MetaPartition{
		Members:  []string{ip1, ip2, ip3, ip4, ip5},
		Learners: []string{ip3, ip4}, //ExcludeLearner
	}

	mw.refreshHostPingElapsed(mp)
	sortedHost := mw.sortHostsByPingElapsed(mp)
	assert.NotNil(t, sortedHost, "empty sortedHost")
	assert.Equal(t, ip1, sortedHost[0], "sorted host failed")
	// not contain learner
	if utils.Contains(sortedHost, ip3) || utils.Contains(sortedHost, ip4) {
		t.Fatalf("Test_refreshHostPingElapsed: unexpected learner in sortedHost.")
	}
	// record err and move to tail
	for i := 0; i < hostErrCountLimit; i++ {
		mp.updateSameZoneHostRank(ip1)
	}
	sortedHost = mw.sortHostsByPingElapsed(mp)
	assert.True(t, sortedHost[0] == ip2 && sortedHost[len(sortedHost)-1] == ip1, "updateHostRank failed")
	// refresh
	//time.Sleep(10 * time.Second)
	mw.refreshHostPingElapsed(mp)
	sortedHost = mw.sortHostsByPingElapsed(mp)
	assert.True(t, sortedHost[0] == ip1 && sortedHost[len(sortedHost)-1] == ip5, "refresh failed")
}
