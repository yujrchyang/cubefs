package cfs

import (
	"github.com/cubefs/cubefs/util/checktool"
	"github.com/cubefs/cubefs/util/log"
	"testing"
)

func TestCheckIsMissReplicaAllInactiveNodes(t *testing.T) {
	mn3 := "mn3"
	ch := new(ClusterHost)
	ch.inactiveNodesForCheckVol = make(map[string]bool)

	mp := &MetaPartition{Hosts: []string{"mn1", "mn2", mn3}}
	mp.Replicas = append(mp.Replicas, &MetaReplica{Addr: "mn1"})
	mp.Replicas = append(mp.Replicas, &MetaReplica{Addr: "mn2"})
	missHosts := mp.getMissReplicas()
	if len(missHosts) == 0 || missHosts[0] != mn3 {
		t.Errorf("missHosts expect %v but get:%v", mn3, missHosts)
	}
	if ch.missReplicaIsInactiveNodes(mp) != false {
		t.Errorf("expcet false but get true")
	}
	ch.inactiveNodesForCheckVol[mn3] = true
	if ch.missReplicaIsInactiveNodes(mp) != true {
		t.Errorf("expcet true but get false")
	}

	mp.Replicas = append(mp.Replicas, &MetaReplica{Addr: mn3})
	missHosts = mp.getMissReplicas()
	if len(missHosts) != 0 {
		t.Errorf("missHosts expect 0 but get:%v", missHosts)
	}
}

func TestCheckCoreVol(t *testing.T) {
	t.Skipf("skip test")
	initTestLog("storagebot")
	defer func() {
		log.LogFlush()
	}()
	ch := new(ClusterHost)
	ch.isReleaseCluster = true
	ch.host = "xxx.xxx.xxx"
	importantVolMap = map[string]map[string]*volThreshold{
		ch.host: {
			"offline_logs": {
				maxInodeCount: 900000000,
				minRWDpCount:  20000,
			},
		},
	}
	checktool.DebugMod = true
	checkCoreVols(ch)
}
