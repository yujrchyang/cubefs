package rebalance

type DiskView struct {
	Path             string
	Total            uint64 // 磁盘总空间
	Used             uint64 // 已使用空间
	MigratedSize     uint64 // 已迁移大小
	MigratedCount    int
	MigrateLimit     int
}

type NodeUsageInfo struct {
	Addr       string     `json:"addr"`
	UsageRatio float64    `json:"usage_ratio"`
	Disk       []DiskView `json:"disks,omitempty"`
}

type ReBalanceInfo struct {
	RebalancedInfoTable
	SrcNodesUsageRatio []NodeUsageInfo
	DstNodesUsageRatio []NodeUsageInfo
}
