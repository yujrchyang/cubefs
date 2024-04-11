package datanode

import (
	"github.com/cubefs/cubefs/datanode/riskdata"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/storage"
	"github.com/tiglabs/raft"
)

type DataPartitionViewInfo struct {
	VolName              string                    `json:"volName"`
	ID                   uint64                    `json:"id"`
	Size                 int                       `json:"size"`
	Used                 int                       `json:"used"`
	Status               int                       `json:"status"`
	Path                 string                    `json:"path"`
	Files                []storage.ExtentInfoBlock `json:"extents"`
	FileCount            int                       `json:"fileCount"`
	Replicas             []string                  `json:"replicas"`
	TinyDeleteRecordSize int64                     `json:"tinyDeleteRecordSize"`
	RaftStatus           *raft.Status              `json:"raftStatus"`
	Peers                []proto.Peer              `json:"peers"`
	Learners             []proto.Learner           `json:"learners"`
	IsFinishLoad         bool                      `json:"isFinishLoad"`
	IsRecover            bool                      `json:"isRecover"`
	BaseExtentID         uint64                    `json:"baseExtentID"`
	RiskFixerStatus      *riskdata.FixerStatus     `json:"riskFixerStatus"`
}
