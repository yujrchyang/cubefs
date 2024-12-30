package proto

import (
	"encoding/json"
	"github.com/cubefs/cubefs/proto"
	"time"
)

var RebalanceModule = []string{"data", "meta"}
var RebalanceStatus = []string{"已完成", "迁移中", "停止"} // code和顺序有关
var RebalanceTaskType = []string{"zone自动均衡", "节点迁移"}

const (
	_ int = iota
	ZoneAutoRebalance
	NodeMigrate
	VolMigrate
)

type ReBalanceInfoTable struct {
	ID                           uint64    `json:"ID"`
	Cluster                      string    `json:"Cluster"`
	Host                         string    `json:"Host"`
	ZoneName                     string    `json:"ZoneName"`
	VolName                      string    `json:"VolName"`
	RType                        int       `json:"RType"` // 0-data 1-meta
	TaskType                     int       `json:"TaskType"`
	Status                       int       `json:"Status"`
	MaxBatchCount                int       `json:"MaxBatchCount"`
	HighRatio                    float64   `json:"HighRatio"`
	LowRatio                     float64   `json:"LowRatio"`
	GoalRatio                    float64   `json:"GoalRatio"`
	MigrateLimitPerDisk          int       `json:"MigrateLimitPerDisk"`
	DstMetaNodePartitionMaxCount int       `json:"DstMetaNodePartitionMaxCount"`
	SrcNodes                     string    `json:"SrcNodes"`
	DstNodes                     string    `json:"DstNodes"`
	OutMigRatio                  float64   `json:"OutMigRatio"`
	CreatedAt                    time.Time `json:"CreatedAt"`
	UpdatedAt                    time.Time `json:"UpdatedAt"`
	DstZone                      string    `json:"DstZone"`
	VolBatchCount                int       `json:"VolBatchCount"`
	PartitionBatchCount          int       `json:"PartitionBatchCount"`
	RoundInterval                int       `json:"RoundInterval"`
}

// 前端做format
type Disk struct {
	Path          string
	Total         uint64 // 磁盘总空间 字节
	Used          uint64 // 已使用
	MigratedSize  uint64 // 已迁移
	MigratedCount int    // 迁移并发度
	MigrateLimit  int    // 最多迁出dp数/盘
}

type NodeUsageInfo struct {
	Addr       string
	UsageRatio float64
	Disk       []*Disk
}

type ReBalanceInfo struct {
	ReBalanceInfoTable
	SrcNodesUsageRatio []*NodeUsageInfo
	DstNodesUsageRatio []*NodeUsageInfo
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
	Status       int // 任务状态
}

type MigrateRecord struct {
	ID           int64
	ClusterName  string
	ZoneName     string
	RType        int
	VolName      string
	PartitionID  uint64
	SrcAddr      string
	SrcDisk      string
	DstAddr      string
	OldUsage     float64
	NewUsage     float64
	OldDiskUsage float64
	NewDiskUsage float64
	TaskId       uint64
	CreatedAt    time.Time
}

type RebalanceInfoView struct {
	TaskID             uint64
	Cluster            string
	Zone               string
	Module             string // 0-data 1-meta
	VolName            string
	Status             int     // 1-已完成 2-迁移中 3-停止
	TaskType           int     // 1-zone自动迁移 2-节点迁移
	HighRatio          float64 // 保留4小数
	LowRatio           float64
	GoalRatio          float64
	Concurrency        int
	LimitDPonDisk      int
	LimitMPonDstNode   int
	SrcNodes           string
	DstNodes           string
	OutMigRatio        float64 // 节点迁移迁出比例
	CreatedAt          string
	UpdatedAt          string
	SrcNodesUsageRatio []*NodeUsageInfo
	DstNodesUsageRatio []*NodeUsageInfo
}

type RebalanceListResp struct {
	Total int
	Data  []*RebalanceInfoView
}

type MigrateRecordListResp struct {
	Total int
	Data  []*MigrateRecord
}

type NodeUsageRatio struct {
	AvgUsageRatio   float64
	AvgPartitionCnt int64
	Nodes           []*NodeUsage
}

type NodeUsage struct {
	Addr           string  `json:"addr"`
	PartitionCount int     `json:"partition_count"`
	UsedRatio      float64 `json:"usage_ratio"`
}

type TaskResponse struct {
	Code int             `json:"Code"`
	Msg  string          `json:"Msg"`
	Data json.RawMessage `json:"Data"`
}

type PageResponse struct {
	Code       int             `json:"Code"`
	Msg        string          `json:"Msg"`
	TotalCount int             `json:"TotalCount"`
	Data       json.RawMessage `json:"Data"`
}

// compact 的结构
type CompactVolListResp struct {
	Total int
	Data  []*proto.DataMigVolume
}

type VolMigrateListResp struct {
	Total int
	Data  []*VolMigrateView
}

type VolMigrateView struct {
	TaskID                uint64
	Cluster               string
	Volumes               []string
	Module                string
	Status                int
	SrcZone               string
	DstZone               string
	ClusterConcurrency    int
	VolConcurrency        int
	PartitionConcurrency  int
	WaitSeconds           int
	DstDataNodeUsageRatio float64
	DstMetaPartitionLimit int
	CreatedAt             string
	UpdatedAt             string
}

type VolMigrateInfo struct {
	Name                  string // 卷名
	Status                int    // 已完成/进行中/已终止
	UpdateTime            string // 完成(或更新)时间
	MigratePartitionCount int    // 迁移完成的分片个数(0: 1.删卷了)
	TotalCount            int    // 需要迁移分片数，预估（vol分片数，不一定都满足迁移条件）
}

type VolMigrateTaskStatus struct {
	Status          int               // 任务状态: 1-stop 2-running 3-terminate
	VolMigrateInfos []*VolMigrateInfo // 所有vol的迁移情况，已完成的顺序在前面
}
