package datanode

import "github.com/cubefs/cubefs/storage"

type DataPartitionValidateCRCTask struct {
	TaskType   uint8
	addr       string
	extents    map[uint64]storage.ExtentInfoBlock
	LeaderAddr string
	Source     string
}

func NewDataPartitionValidateCRCTask(extentFiles []storage.ExtentInfoBlock, source, leaderAddr string) (task *DataPartitionValidateCRCTask) {
	task = &DataPartitionValidateCRCTask{
		extents:    make(map[uint64]storage.ExtentInfoBlock, len(extentFiles)),
		LeaderAddr: leaderAddr,
		Source:     source,
	}
	for _, extentFile := range extentFiles {
		task.extents[extentFile[storage.FileID]] = extentFile
	}
	return
}
