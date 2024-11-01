package cfs

import (
	"github.com/cubefs/cubefs/util/log"
	"testing"
)

func TestCheckDbBackPeer(t *testing.T) {
	initTestLog("storagebot")
	defer func() {
		log.LogFlush()
	}()
	warnUmp = false
	host := newClusterHost("test.dbbak.jd.local", true)
	checkDbbakDataPartition(host, 4)
}
