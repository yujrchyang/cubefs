package datanode

import (
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/errors"
	"reflect"
	"strings"
)

type DataPartitionMetadata struct {
	VolumeID                string
	PartitionID             uint64
	PartitionSize           int
	CreateTime              string
	Peers                   []proto.Peer
	Hosts                   []string
	Learners                []proto.Learner
	ReplicaNum              int
	DataPartitionCreateType int
	LastTruncateID          uint64
	VolumeHAType            proto.CrossRegionHAType
	ConsistencyMode         proto.ConsistencyMode

	// 该BOOL值表示Partition是否已经就绪，该值默认值为false，
	// 新创建的DP成员为默认值，表示未完成第一次Raft恢复，Raft未就绪。
	// 当第一次快照或者有应用日志行为时，该值被置为true并需要持久化该信息。
	// 当发生快照应用(Apply Snapshot)行为时，该值为true。该DP需要关闭并进行报警。
	IsCatchUp            bool
	NeedServerFaultCheck bool
}

func (md *DataPartitionMetadata) Equals(other *DataPartitionMetadata) bool {
	return (md == nil && other == nil) ||
		(md != nil && other != nil && md.VolumeID == other.VolumeID &&
			md.PartitionID == other.PartitionID &&
			md.PartitionSize == other.PartitionSize &&
			reflect.DeepEqual(md.Peers, other.Peers) &&
			reflect.DeepEqual(md.Hosts, other.Hosts) &&
			reflect.DeepEqual(md.Learners, other.Learners) &&
			md.ReplicaNum == other.ReplicaNum &&
			md.DataPartitionCreateType == other.DataPartitionCreateType &&
			md.LastTruncateID == other.LastTruncateID &&
			md.VolumeHAType == other.VolumeHAType) &&
			md.IsCatchUp == other.IsCatchUp &&
			md.NeedServerFaultCheck == other.NeedServerFaultCheck &&
			md.ConsistencyMode == other.ConsistencyMode
}

func (md *DataPartitionMetadata) Validate() (err error) {
	md.VolumeID = strings.TrimSpace(md.VolumeID)
	if len(md.VolumeID) == 0 || md.PartitionID == 0 || md.PartitionSize == 0 {
		err = errors.New("illegal data partition metadata")
		return
	}
	return
}