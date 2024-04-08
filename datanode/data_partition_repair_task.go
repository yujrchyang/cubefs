package datanode

import "github.com/cubefs/cubefs/storage"

// DataPartitionRepairTask defines the reapir task for the data partition.
type DataPartitionRepairTask struct {
	TaskType                       uint8
	addr                           string
	extents                        map[uint64]storage.ExtentInfoBlock
	ExtentsToBeCreated             []storage.ExtentInfoBlock
	ExtentsToBeRepaired            []storage.ExtentInfoBlock
	ExtentsToBeDeleted             []storage.ExtentInfoBlock
	ExtentsToBeRepairedSource      map[uint64]string
	BaseExtentID                   uint64
	LeaderTinyDeleteRecordFileSize int64
	LeaderAddr                     string
}

func NewDataPartitionRepairTask(address string, extentFiles []storage.ExtentInfoBlock, baseExtentID uint64, tinyDeleteRecordFileSize int64, source, leaderAddr string) (task *DataPartitionRepairTask) {
	task = &DataPartitionRepairTask{
		addr:                           address,
		extents:                        make(map[uint64]storage.ExtentInfoBlock, 0),
		ExtentsToBeCreated:             make([]storage.ExtentInfoBlock, 0),
		ExtentsToBeRepaired:            make([]storage.ExtentInfoBlock, 0),
		ExtentsToBeRepairedSource:      make(map[uint64]string, 0),
		LeaderTinyDeleteRecordFileSize: tinyDeleteRecordFileSize,
		LeaderAddr:                     leaderAddr,
		BaseExtentID:                   baseExtentID,
	}
	for _, extentFile := range extentFiles {
		task.extents[extentFile[storage.FileID]] = extentFile
	}

	return
}
