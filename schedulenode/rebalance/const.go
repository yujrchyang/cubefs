package rebalance

import "time"

const (
	RBStart        = "/rebalance/start"
	RBStop         = "/rebalance/stop"
	RBStatus       = "/rebalance/status"
	RBReset        = "/rebalance/reset"
	RBInfo         = "/rebalance/info"
	RBResetControl = "/rebalance/resetControl"
	RBList         = "/rebalance/list"
)

const (
	ZoneUsageRatio = "/zone/UsageRatio"
)

const (
	ParamId                   = "id"
	ParamCluster              = "cluster"
	ParamZoneName             = "zoneName"
	ParamStatus               = "status"
	ParamHighRatio            = "highRatio"
	ParamLowRatio             = "lowRatio"
	ParamGoalRatio            = "goalRatio"
	ParamClusterMaxBatchCount = "maxBatchCount"
	ParamMigrateLimitPerDisk  = "migrateLimit"
	ParamPage                 = "page"
	ParamPageSize             = "pageSize"
)

const (
	_ Status = iota
	StatusStop
	StatusRunning
	StatusTerminating
)

const (
	defaultMinWritableDPNum     = 2
	defaultClusterMaxBatchCount = 50
	defaultMigrateLimitPerDisk  = 10
	defaultInterval             = time.Minute * 5
)

const (
	ReadOnly    = 1
	ReadWrite   = 2
	Unavailable = -1
)
