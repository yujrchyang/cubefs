package rebalance

import (
	"fmt"
	"time"
)

const (
	deleteWarnKey = "rebalance_two_replica_dp_wrong_status"
)

const (
	defaultMinWritableDPNum             = 2
	defaultClusterMaxBatchCount         = 50
	defaultMigrateLimitPerDisk          = 10
	defaultWaitClusterRecover           = time.Second * 30
	defaultMigNodeInterval              = time.Minute * 1
	defaultDstMetaNodePartitionMaxCount = 10000
	defaultRefreshNodeInterval          = time.Minute * 2
	defaultDeletedTaskInterval          = time.Minute * 10
	defaultDstDatanodeUsage             = 0.8
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

	VolMigrateCreate       = "/volMig/create"
	VolMigrateResetControl = "/volMig/resetCtrl"
	VolMigrateStop         = "/volMig/stop"
	VolMigrateStatus       = "/volMig/status"
	VolMigrateQueryRecords = "/volMig/queryRecords"

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
	ParamMigVolumesList               = "volNames"
	ParamOutMigRatio                  = "outMigRatio"

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

	ParamSrcZone              = "srcZone"
	ParamDstZone              = "dstZone"
	ParamClusterConcurrency   = "clusterConcurrency"
	ParamVolConcurrency       = "volConcurrency"
	ParamPartitionConcurrency = "partitionConcurrency"
	ParamWaitSecond           = "waitSecond"
)

type Status int

const (
	_ Status = iota
	StatusStop
	StatusRunning
	StatusTerminating
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
	VolsMigrate
	MaxTaskType
)

func (t TaskType) String() string {
	switch t {
	case ZoneAutoReBalance:
		return "ZoneAutoRebalance"
	case NodesMigrate:
		return "NodesMigrate"
	case VolsMigrate:
		return "VolsMigrate"
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
	case "VolsMigrate":
		tType = VolsMigrate
	default:
		err = fmt.Errorf("error rebalance task type: %v", rTypeStr)
	}
	return
}

const (
	FinishDeleteDefault = 0
	FinishDeleteSuccess = 1
	FinishDeleteFailed  = -1
)

const (
	DeleteFlagForThreeReplica int = iota
	DeleteFlagForTwoReplica
)

const (
	MaxMigrateVolumeNum = 100
)
