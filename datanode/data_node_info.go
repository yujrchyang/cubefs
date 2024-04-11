package datanode

type DataNodeInfo struct {
	Addr                      string
	PersistenceDataPartitions []uint64
}
