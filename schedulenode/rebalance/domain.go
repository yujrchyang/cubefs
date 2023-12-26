package rebalance

type DiskView struct {
	Path             string
	Total            uint64 // 磁盘总空间
	Used             uint64 // 已使用空间
	MigratedSize     uint64 // 已迁移大小
	MigratedCount    int
	MigrateLimit     int
}

type DstDataNode struct {
	Addr string
	UsageRatio float64
}

type SrcDataNode struct {
	Addr       string
	UsageRatio float64
	Disk       []DiskView
}

type ReBalanceInfo struct {
	RebalancedInfoTable
	SrcNodesUsageRatio []SrcDataNode
	DstNodesUsageRatio []DstDataNode
}
