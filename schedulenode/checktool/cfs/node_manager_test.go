package cfs

import (
	"fmt"
	"github.com/cubefs/cubefs/util/log"
	"github.com/stretchr/testify/assert"
	"os"
	"path"
	"testing"
)

func TestCheckNodeSet(t *testing.T) {
	tv := &TopologyView{
		Zones: []*ZoneView{
			{
				Name: "zone1",
				NodeSet: map[uint64]*nodeSetView{
					1: {
						DataNodeLen: 1,
						MetaNodeLen: 20,
					},
					2: {
						DataNodeLen: 20,
						MetaNodeLen: 25,
					},
				},
			},
			{
				Name: "zone1",
				NodeSet: map[uint64]*nodeSetView{
					1: {
						DataNodeLen: 0,
						MetaNodeLen: 0,
					},
					2: {
						DataNodeLen: 200,
						MetaNodeLen: 200,
					},
				},
			},
			{
				Name: "zone1",
				NodeSet: map[uint64]*nodeSetView{
					1: {
						DataNodeLen: 200,
						MetaNodeLen: 200,
					},
					2: {
						DataNodeLen: 231,
						MetaNodeLen: 233,
					},
				},
			},
		},
	}
	badDataSets, badMetaSets := checkNodeSetLen(tv, "test.cluster")
	assert.Equal(t, 2, len(badDataSets))
	assert.Equal(t, 1, len(badMetaSets))
}

func initTestLog(module string) {
	logdir := path.Join(os.TempDir(), fmt.Sprintf("test_%v", module))
	os.RemoveAll(logdir)
	os.MkdirAll(logdir, 0666)
	log.InitLog(logdir, module, log.DebugLevel, nil)
}

func TestCheckNodeAlive(t *testing.T) {
	initTestLog("checknode")
	defer func() {
		log.LogFlush()
	}()
	host := newClusterHost("test.chubaofs.jd.local")
	host.isReleaseCluster = false
	cv, err := getCluster(host)
	if err != nil {
		return
	}
	for i := 0; i < 4; i++ {
		inactive := cv.confirmCheckMetaNodeAlive(host, false)
		t.Logf("check-%v inactive metanodes: %v\n", i, len(inactive))
	}
	for i := 0; i < 4; i++ {
		inactive := cv.confirmCheckDataNodeAlive(host, false)
		t.Logf("check-%v inactive datanodes: %v\n", i, len(inactive))
	}
}
