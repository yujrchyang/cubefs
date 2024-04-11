package datanode

import (
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/raftstore"
)

type dataPartitionCfg struct {
	VolName       string              `json:"vol_name"`
	ClusterID     string              `json:"cluster_id"`
	PartitionID   uint64              `json:"partition_id"`
	PartitionSize int                 `json:"partition_size"`
	Peers         []proto.Peer        `json:"peers"`
	ReplicaNum    int                 `json:"replica_num"`
	Hosts         []string            `json:"hosts"`
	Learners      []proto.Learner     `json:"learners"`
	NodeID        uint64              `json:"-"`
	RaftStore     raftstore.RaftStore `json:"-"`
	CreationType  int                 `json:"-"`

	VolHAType proto.CrossRegionHAType `json:"vol_ha_type"`

	Mode proto.ConsistencyMode `json:"-"`
}