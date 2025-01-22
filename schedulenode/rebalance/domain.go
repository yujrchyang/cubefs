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
	Addr           string     `json:"addr"`
	UsageRatio     float64    `json:"usage_ratio"`
	PartitionCount int        `json:"partition_count"`
	Disk           []DiskView `json:"disks,omitempty"`
}

type ReBalanceInfo struct {
	RebalancedInfoTable
	SrcNodesUsageRatio []NodeUsageInfo // todo: remove，不关心的信息
	DstNodesUsageRatio []NodeUsageInfo
}

type RebalanceNodeInfo struct {
	Addr          string
	IsFinish      bool
	TotalCount    int     // 待迁移总数(估)
	MigratedCount int     // 已迁移
	UsageRatio    float64 // 节点使用率
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
	PartitionID  uint64
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

type VolMigrateRequest struct {
	Cluster         string
	SrcZone         string
	DstZone         string
	ClusterCurrency int
	VolCurrency     int
	VolDpCurrency   int
	WaitSecond      int
}

type VolMigrateInfo struct {
	Name                  string // 卷名
	Status                Status // 已完成/进行中/已终止
	UpdateTime            string // 完成(或更新)时间
	MigratePartitionCount int    // 迁移完成的分片个数(0: 1.删卷了)
	TotalCount            int    // 需要迁移分片数，预估（vol分片数，不一定都满足迁移条件）
}

// 任务状态：***
// 卷名   已迁移/dp总数 状态  更新时间   -- 完成的排在前面
type VolMigrateTaskStatus struct {
	Status          int               // 任务状态
	VolMigrateInfos []*VolMigrateInfo // 所有vol的迁移情况，已完成的顺序在前面
}
