package cfs

import (
	"context"
	"fmt"
	"github.com/cubefs/cubefs/util/log"
	"github.com/stretchr/testify/assert"
	"os"
	"path"
	"testing"
	"time"
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
	initEnv(DevelopmentEnv)
	logdir := path.Join(os.TempDir(), fmt.Sprintf("test_%v", module))
	os.RemoveAll(logdir)
	os.MkdirAll(logdir, 0666)
	log.InitLog(logdir, module, log.DebugLevel, nil)
}

func TestCheckNodeAlive(t *testing.T) {
	//t.Skipf("skip checkNodeAlive")
	initTestLog("storagebot")
	defer func() {
		log.LogFlush()
	}()
	s := NewChubaoFSMonitor(context.Background())
	s.integerMap[cfgKeyHDDDiskOfflineThreshold] = 10
	s.integerMap[cfgKeySSDDiskOfflineThreshold] = 10

	t.Run("spark", func(t *testing.T) {
		host := newClusterHost("test.chubaofs.jd.local", false)
		host.offlineDataNodeTokenPool = newTokenPool(time.Hour, 1)
		for i := 0; i < 20; i++ {
			cv, err := getCluster(host)
			if err != nil {
				return
			}
			cv.checkDataNodeAlive(host, s)
			t.Logf("check datanode alive")
			time.Sleep(time.Minute)
		}
	})

	t.Run("dbbak", func(t *testing.T) {
		host := newClusterHost("test.dbbak.jd.local", true)
		host.offlineDataNodeTokenPool = newTokenPool(time.Hour, 1)
		for i := 0; i < 20; i++ {
			cv, err := getCluster(host)
			if err != nil {
				return
			}
			cv.checkDataNodeAlive(host, s)
			t.Logf("check datanode alive")
			time.Sleep(time.Minute)
		}
	})
}

func TestGetDiskMap(t *testing.T) {
	//t.Skipf("skip TestGetDiskMap")
	initTestLog("storagebot")
	defer func() {
		log.LogFlush()
	}()
	t.Run("dbbak", func(t *testing.T) {
		host := newClusterHost("dbbak.jd.local", true)
		dataNodeView, err := getDataNode(host, "1.1.1.1:6000")
		if !assert.NoError(t, err) {
			return
		}
		disks := getDisks(host, dataNodeView)
		t.Logf("disks:%v", disks)
	})

	t.Run("spark", func(t *testing.T) {
		host := newClusterHost("cn.chubaofs.jd.local", false)
		dataNodeView, err := getDataNode(host, "1.1.1.1:6000")
		if !assert.NoError(t, err) {
			return
		}
		disks := getDisks(host, dataNodeView)
		t.Logf("disks:%v", disks)
	})

}

func TestConfirmCheckNodeAlive(t *testing.T) {
	initTestLog("storagebot")
	defer func() {
		log.LogFlush()
	}()
	host := newClusterHost("test.chubaofs.jd.local", false)
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

func TestCheckHeartbeatStuck(t *testing.T) {
	initTestLog("storagebot")
	defer func() {
		log.LogFlush()
	}()

	t.Run("spark", func(t *testing.T) {
		host := newClusterHost("test.chubaofs.jd.local")
		host.isReleaseCluster = false
		cv, err := getCluster(host)
		if err != nil {
			t.Errorf(err.Error())
			return
		}
		cv.checkMetaNodeStuckHeartbeat(host, false)
	})

	t.Run("dbbak", func(t *testing.T) {
		host := newClusterHost("test.dbbak.jd.local")
		host.isReleaseCluster = true
		cv, err := getCluster(host)
		if err != nil {
			t.Errorf(err.Error())
			return
		}
		cv.checkMetaNodeStuckHeartbeat(host, false)
	})
}

func TestGetClusterByMasterNodes(t *testing.T) {
	initTestLog("storagebot")
	defer func() {
		log.LogFlush()
	}()
	host := newClusterHost("test.chubaofs.jd.local", false)
	host.masterNodes = []string{
		"1.1.1.1:80",
		"",
	}
	host.isReleaseCluster = false
	cv, err := getClusterByMasterNodes(host)
	if !assert.NoError(t, err) {
		return
	}
	assert.NotZero(t, len(cv.DataNodes))
}
