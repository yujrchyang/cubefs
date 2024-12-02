package tinyblck

import (
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/storage"
	"github.com/cubefs/cubefs/util/unit"
	"go.uber.org/atomic"
	"time"
)

const (
	DefaultCheckInterval     = time.Minute * 1440 // 24h
	DefaultSafeCleanInterval = 60            //1 min
	DefaultTaskConcurrency   = 5
	DefaultInodeConcurrency  = 10
	defaultParallelMPCount   = 20
	DefaultMailToMember      = "lizhenzhen36@jd.com"
	DefaultAlarmErps         = "lizhenzhen36"
	DefTinyEKCheckRatio      = 0.5 //tiny extents的检查需要注意，所以只针对前50%的数据进行检测
	DefReservedSize          = 16 * unit.MB
	DefMaxCheckSize          = 1*unit.TB
	DefBitSetCap             = 32*unit.MB //1TB对应32MB
)

var (
	parallelMpCnt = atomic.NewInt32(defaultParallelMPCount)
	parallelInodeCnt = atomic.NewInt32(DefaultInodeConcurrency)
)

type DataPartitionView struct {
	VolName      string                    `json:"volName"`
	ID           uint64                    `json:"id"`
	Files        []storage.ExtentInfoBlock `json:"extents"`
	FileCount    int                       `json:"fileCount"`
	IsFinishLoad bool                      `json:"isFinishLoad"`
}

type ExtentInfo struct {
	DataPartitionID uint64
	ExtentID        uint64
	ExtentOffset    uint64
	Size            uint32
}

type ExtentHolesInfo struct {
	Holes           []*proto.TinyExtentHole `json:"holes"`
	ExtentSize      uint64                  `json:"-"`
	ExtentAvaliSize uint64                  `json:"extentAvaliSize"`
	ExtentBlocks    int64                   `json:"blockNum"`
}