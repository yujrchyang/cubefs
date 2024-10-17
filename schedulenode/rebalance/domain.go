package rebalance

import (
	"github.com/cubefs/cubefs/proto"
	"time"
)

type ClusterConfig struct {
	ClusterName  string   `json:"clusterName"`
	MasterAddrs  []string `json:"mastersAddr"`
	MetaProfPort uint16   `json:"mnProfPort"`
	DataProfPort uint16   `json:"dnProfPort"`
	IsDBBack     bool     `json:"isDBBack"`
}

type DiskView struct {
	Path          string
	Total         uint64 // 磁盘总空间
	Used          uint64 // 已使用空间
	MigratedSize  uint64 // 已迁移大小
	MigratedCount int
	MigrateLimit  int
}

type NodeUsageInfo struct {
	Addr       string     `json:"addr"`
	UsageRatio float64    `json:"usage_ratio"`
	Disk       []DiskView `json:"disks,omitempty"`
}

type ReBalanceInfo struct {
	RebalancedInfoTable
	SrcNodesUsageRatio []NodeUsageInfo // todo: remove，不关心的信息
	DstNodesUsageRatio []NodeUsageInfo
}

type RebalanceNodeInfo struct {
	Addr          string
	IsFinish      bool
	TotalCount    int // 待迁移总数(估)
	MigratedCount int // 已迁移
}

type RebalanceStatusInfo struct {
	SrcNodesInfo []*RebalanceNodeInfo
	DstNodesInfo []*RebalanceNodeInfo
	Status       Status // 任务状态
}

type DbBakNodeView struct {
	ID     uint64 `json:"-"`
	Addr   string
	Status bool
}

type BadPartitionView struct {
	Path         string
	PartitionIDs []uint64
}

type ClusterView struct {
	MetaNodes           []DbBakNodeView
	DataNodes           []DbBakNodeView
	BadPartitionIDs     []BadPartitionView
	BadMetaPartitionIDs []BadPartitionView
}

type DbBakDataNodeInfo struct {
	Addr                      string
	Ratio                     float64
	DataPartitionCount        uint32
	ToBeOffline               bool
	PersistenceDataPartitions []uint64
	ReportTime                time.Time
}

type DataNodeStatsWithAddr struct {
	*proto.DataNodeStats
	Addr       string
	UsageRatio float64 // 节点使用率 压缩盘特殊处理
}

func NewDataNodeStatsWithAddr(nodeStats *proto.DataNodeStats, addr string) *DataNodeStatsWithAddr {
	return &DataNodeStatsWithAddr{
		DataNodeStats: nodeStats,
		Addr:          addr,
		UsageRatio:    convertActualUsageRatio(nodeStats),
	}
}
