package rebalance

import (
	"fmt"
	"time"
)

const (
	RBStart        = "/rebalance/start"
	RBStop         = "/rebalance/stop"
	RBStatus       = "/rebalance/status"
	RBReset        = "/rebalance/reset"
	RBInfo         = "/rebalance/info"
	RBResetControl = "/rebalance/resetControl"
	RBList         = "/rebalance/list"
	RBRecordsQuery = "/rebalance/queryRecords"
)

const (
	ZoneUsageRatio = "/zone/UsageRatio"
)

const (
	ParamId                           = "id"
	ParamCluster                      = "cluster"
	ParamZoneName                     = "zoneName"
	ParamStatus                       = "status"
	ParamHighRatio                    = "highRatio"
	ParamLowRatio                     = "lowRatio"
	ParamGoalRatio                    = "goalRatio"
	ParamClusterMaxBatchCount         = "maxBatchCount"
	ParamMigrateLimitPerDisk          = "migrateLimit"
	ParamDstMetaNodePartitionMaxCount = "dstMetaNodeMaxPartitionCount"
	ParamSrcNodesList                 = "srcNodes"
	ParamDstNodesList                 = "dstNodes"

	ParamPage          = "page"
	ParamPageSize      = "pageSize"
	ParamRebalanceType = "type"
	ParamVolName       = "volume"
	ParamPid           = "pid"
	ParamSrcHost       = "src"
	ParamDstHost       = "dst"
	ParamQueryDate     = "date"
	ParamQueryTaskId   = "taskId"
	ParamTaskType      = "taskType"
)

type Status int

const (
	_ Status = iota
	StatusStop
	StatusRunning
	StatusTerminating
)

const (
	defaultMinWritableDPNum             = 2
	defaultClusterMaxBatchCount         = 50
	defaultMigrateLimitPerDisk          = 10
	defaultInterval                     = time.Minute * 5
	defaultDstMetaNodePartitionMaxCount = 10000
)

const (
	ReadOnly    = 1
	ReadWrite   = 2
	Unavailable = -1
)

type RebalanceType int

const (
	RebalanceData RebalanceType = iota
	RebalanceMeta
	MaxRebalanceType
)

func (t RebalanceType) String() string {
	switch t {
	case RebalanceData:
		return "data"
	case RebalanceMeta:
		return "meta"
	default:
		return fmt.Sprintf("unknown_type_%d", t)
	}
}

func ConvertRebalanceTypeStr(rTypeStr string) (rType RebalanceType, err error) {
	switch rTypeStr {
	case "data":
		rType = RebalanceData
	case "meta":
		rType = RebalanceMeta
	default:
		err = fmt.Errorf("error rebalance type: %v", rTypeStr)
	}
	return
}

type TaskType int

const (
	_ TaskType = iota
	ZoneAutoReBalance
	NodesMigrate
	MaxTaskType
)

func (t TaskType) String() string {
	switch t {
	case ZoneAutoReBalance:
		return "ZoneAutoRebalance"
	case NodesMigrate:
		return "NodesMigrate"
	default:
		return fmt.Sprintf("unknown_type_%d", t)
	}
}

func ConvertRebalanceTaskTypeStr(rTypeStr string) (tType TaskType, err error) {
	switch rTypeStr {
	case "ZoneAutoRebalance":
		tType = ZoneAutoReBalance
	case "NodesMigrate":
		tType = NodesMigrate
	default:
		err = fmt.Errorf("error rebalance task type: %v", rTypeStr)
	}
	return
}
