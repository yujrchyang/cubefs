package datanode

import "github.com/cubefs/cubefs/util/unit"

type DiskConfig struct {
	GetReservedRatio         GetReservedRatioFunc
	MaxErrCnt                int
	MaxFDLimit               uint64     // 触发强制FD淘汰策略的阈值
	ForceFDEvictRatio        unit.Ratio // 强制FD淘汰比例
	FixTinyDeleteRecordLimit uint64
}
