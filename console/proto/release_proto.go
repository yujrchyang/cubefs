package proto

import (
	"github.com/cubefs/cubefs/proto"
	"time"
)

const (
	APICodeGetCluster        uint8 = 0x01
	APICodeGetVol            uint8 = 0x02
	APICodeGetMetaPartitions uint8 = 0x03
	APICodeListVols          uint8 = 0x04
	APICodeGetDataPartitions uint8 = 0x05
)

type ClusterView struct {
	Name                           string
	LeaderAddr                     string
	CompactStatus                  bool
	DisableAutoAlloc               bool
	DpRecoverPool                  int32
	MpRecoverPool                  int32
	BadDiskAutoOffline             bool
	Applied                        uint64
	MaxDataPartitionID             uint64
	MaxMetaNodeID                  uint64
	MaxMetaPartitionID             uint64
	DataNodeStat                   *DataNodeSpaceStat
	MetaNodeStat                   *MetaNodeSpaceStat
	VolStat                        []*VolSpaceStat
	MetaNodes                      []MetaNodeView
	DataNodes                      []DataNodeView
	BadPartitionIDs                []BadPartitionView
	BadMetaPartitionIDs            []BadPartitionView
	RepairCrossPodPartitionIDs     []BadPartitionView
	RepairCrossPodMetaPartitionIDs []BadPartitionView
	ToBeOfflineDpChanCount         int
	DataNodeBadDisks               []DataNodeBadDisksView
	BandwidthLimit                 uint64
}

type DataNodeSpaceStat struct {
	TotalGB    uint64
	UsedGB     uint64
	IncreaseGB int64
	UsedRatio  string
}

type MetaNodeSpaceStat struct {
	TotalGB    uint64
	UsedGB     uint64
	IncreaseGB int64
	UsedRatio  string
}

type VolSpaceStat struct {
	Name      string
	TotalGB   uint64
	UsedGB    uint64
	UsedRatio string
}

type DataNodeView struct {
	Addr   string
	Status bool
}

type MetaNodeView struct {
	ID     uint64
	Addr   string
	Status bool
}

type BadPartitionView struct {
	Path        string
	PartitionID uint64
}

type DataNodeBadDisksView struct {
	Addr        string
	BadDiskPath []string
}

type LimitInfo struct {
	Cluster string

	MetaNodeReqRateLimit             uint64
	MetaNodeReqOpRateLimitMap        map[uint8]uint64
	DataNodeReqZoneRateLimitMap      map[string]uint64
	DataNodeReqZoneOpRateLimitMap    map[string]map[uint8]uint64
	DataNodeReqZoneVolOpRateLimitMap map[string]map[string]map[uint8]uint64
	DataNodeReqVolPartRateLimitMap   map[string]uint64
	DataNodeReqVolOpPartRateLimitMap map[string]map[uint8]uint64
	ReadDirLimitNum                  uint64
	NetworkFlowRatio                 map[string]uint64
	// map[module]map[zone:|vol:]map[op]AllLimitGroup
	RateLimit                    map[string]map[string]map[int]proto.AllLimitGroup
	ClientReadVolRateLimitMap    map[string]uint64
	ClientWriteVolRateLimitMap   map[string]uint64
	ClientVolOpRateLimit         map[uint8]int64 // less than 0: no limit; equal 0: disable op
	DataNodeRepairLimitOnDisk    uint64
	BitMapAllocatorMaxUsedFactor float64
	BitMapAllocatorMinFreeFactor float64

	MonitorSummarySec             uint64
	MonitorReportSec              uint64
	TopologyFetchIntervalMin      int64
	TopologyForceFetchIntervalSec int64
	ApiReqBwRateLimitMap          map[uint8]int64
}

type DataNode struct {
	MaxDiskAvailWeight        uint64 `json:"MaxDiskAvailWeight"`
	CreatedVolWeights         uint64
	RemainWeightsForCreateVol uint64
	Total                     uint64 `json:"TotalWeight"`
	Used                      uint64 `json:"UsedWeight"`
	Available                 uint64
	RackName                  string `json:"Rack"`
	Addr                      string
	PodName                   string
	ReportTime                time.Time
	Ratio                     float64
	SelectCount               uint64
	Carry                     float64
	DataPartitionCount        uint32
	ToBeOffline               bool
	PersistenceDataPartitions []uint64
	IsActive                  bool `json:"-"`
}

type MetaNode struct {
	ID                        uint64
	Addr                      string
	PodName                   string
	IsActive                  bool
	RackName                  string `json:"Rack"`
	MaxMemAvailWeight         uint64 `json:"MaxMemAvailWeight"`
	Total                     uint64 `json:"TotalWeight"`
	Used                      uint64 `json:"UsedWeight"`
	Ratio                     float64
	SelectCount               uint64
	Carry                     float64
	Threshold                 float32
	ReportTime                time.Time
	MetaPartitionCount        uint32
	PersistenceMetaPartitions []uint64
	ProfPort                  string
	Version                   string
}

type MetaPartition struct {
	PartitionID      uint64
	VolName          string
	Start            uint64
	End              uint64
	MaxInodeID       uint64
	InodeCount       uint64
	DentryCount      uint64
	IsManual         bool
	Replicas         []*MetaReplica
	ReplicaNum       uint8
	Status           int8
	IsRecover        bool
	PersistenceHosts []string
	Peers            []proto.Peer
	MissNodes        map[string]int64
	OfflinePeerID    uint64
	LoadResponse     []*LoadMetaPartitionMetricResponse
}

// 只需要ip 和 leader(leader放在首位)
type MetaReplica struct {
	Addr        string
	MaxInodeID  uint64
	InodeCount  uint64
	DentryCount uint64
	ReportTime  int64
	Status      int8
	IsLeader    bool
}

type LoadMetaPartitionMetricResponse struct {
	Start       uint64
	End         uint64
	Status      uint8
	Result      string
	PartitionID uint64
	DoCompare   bool
	ApplyID     uint64
	MaxInode    uint64
	DentryCount uint64
	Addr        string
	InodeCount  uint64
}

type DataPartition struct {
	PartitionID      uint64
	LastLoadTime     int64
	ReplicaNum       uint8
	Status           int8
	IsRecover        bool
	IsManual         bool
	Replicas         []*DataReplica
	PartitionType    string
	PersistenceHosts []string
	VolName          string
	MissNodes        map[string]int64
	FileMissReplica  map[string]int64
}

// 不支持raft 没有leader信息
type DataReplica struct {
	Addr                    string
	ReportTime              int64
	FileCount               uint32
	Status                  int8
	LoadPartitionIsResponse bool
	Total                   uint64 `json:"TotalSize"`
	Used                    uint64 `json:"UsedSize"`
	NeedCompare             bool
	DiskPath                string
}

type VolView struct {
	ID                        uint64
	Name                      string
	Owner                     string
	DpReplicaNum              uint8
	MpReplicaNum              uint8
	Status                    uint8
	Capacity                  uint64 // GB
	MinWritableDPNum          uint64
	MinWritableMPNum          uint64
	RwMpCnt                   int
	RwDpCnt                   int
	MpCnt                     int
	DpCnt                     int
	DentryCount               uint64
	InodeCount                uint64
	AvailSpaceAllocated       uint64 //GB
	CrossPod                  bool
	AutoRepairCrossPod        bool
	EnableToken               bool
	Tokens                    map[string]*Token
	CrossPodNeedRepairDpCount int
	CrossPodNeedRepairMpCount int
	EnableBitmapAllocator     bool
	BitMapSnapFrozenHour      int64
	BatchDelInodeCnt          int32
	ConnConfig                *ConnConfig
}

type Token struct {
	TokenType int8
	Value     string
	VolName   string
}

type ConnConfig struct {
	ReadConnTimeout  time.Duration `json:"ReadTimeoutNs"`
	WriteConnTimeout time.Duration `json:"WriteTimeoutNs"`
}

type DataPartitionsView struct {
	PartitionID   uint64
	Status        int8
	ReplicaNum    uint8
	PartitionType string
	Hosts         []string
}

type DataPartitionsResp struct {
	DataPartitions []*DataPartitionsView
}

type MetaPartitionView struct {
	PartitionID uint64
	Start       uint64
	End         uint64
	Members     []string
	LeaderAddr  string
	Status      int8
	InodeCount  uint64
	DentryCount uint64
	MaxInodeID  uint64
	IsRecover   bool
}
