package cfs

import (
	"github.com/cubefs/cubefs/schedulenode/checktool/cfs/tcp_api"
	"github.com/cubefs/cubefs/util/log"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMetaPartitionApply(t *testing.T) {
	//t.Skipf("skip")
	initTestLog("storagebot")
	defer func() {
		log.LogFlush()
	}()
	t.Run("dbbak", func(t *testing.T) {
		host := newClusterHost("")
		host.isReleaseCluster = true
		checkMetaPartitionApply(host)
	})
	t.Run("spark", func(t *testing.T) {
		host := newClusterHost("")
		checkMetaPartitionApply(host)
	})
}

func TestCompareMetaLoadInfo(t *testing.T) {
	metaInfos := make(map[string]*tcp_api.MetaPartitionLoadResponse, 0)
	metaInfos["node1"] = &tcp_api.MetaPartitionLoadResponse{
		PartitionID: 1,
		ApplyID:     1000,
	}
	metaInfos["node2"] = &tcp_api.MetaPartitionLoadResponse{
		PartitionID: 1,
		ApplyID:     10000,
	}
	metaInfos["node3"] = &tcp_api.MetaPartitionLoadResponse{
		PartitionID: 1,
		ApplyID:     1000,
	}
	var minReplica *tcp_api.MetaPartitionLoadResponse
	minReplica, same := compareLoadResponse(200, 0, func(mpr *tcp_api.MetaPartitionLoadResponse) uint64 { return mpr.ApplyID }, metaInfos)
	assert.False(t, same)
	assert.Equal(t, uint64(1000), minReplica.ApplyID)

	metaInfos["node2"] = &tcp_api.MetaPartitionLoadResponse{
		PartitionID: 1,
		ApplyID:     1000,
	}

	minReplica, same = compareLoadResponse(200, 0, func(mpr *tcp_api.MetaPartitionLoadResponse) uint64 { return mpr.ApplyID }, metaInfos)
	assert.True(t, same)
}
