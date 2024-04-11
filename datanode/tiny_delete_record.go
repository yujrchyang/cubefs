package datanode

type TinyDeleteRecord struct {
	extentID uint64
	offset   uint64
	size     uint64
}

type TinyDeleteRecordArr []TinyDeleteRecord