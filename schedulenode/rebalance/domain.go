package rebalance

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
