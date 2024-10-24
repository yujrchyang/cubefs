package cfs

import (
	"github.com/cubefs/cubefs/schedulenode/checktool/cfs/multi_email"
	"github.com/cubefs/cubefs/util/log"
	"testing"
)

func TestCheckAvailableTinyExtent(t *testing.T) {
	t.Skipf("skip TestCheckAvailableTinyExtent")
	initTestLog("storagebot")
	defer func() {
		log.LogFlush()
	}()
	multi_email.InitMultiMail(25, "mx.jd.local", "storage-sre@jd.com", "storage-sre", "******", []string{"xuxihao3@jd.com"})
	host := newClusterHost("test.chubaofs.jd.local")
	checkTinyExtentsByVol(host, []string{"testvol"})
}
