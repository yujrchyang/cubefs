package proto

import (
	"fmt"
	"github.com/cubefs/cubefs/proto"
)

const (
	BasicTypeUintPrefix  = "uint"
	BasicTypeIntPrefix   = "int"
	BasicTypeFloatPrefix = "float"
	BasicTypeBool        = "bool"
	BasicTypeString      = "string"

	EmptyZoneVolFlag = "_"
)

var CliModuleList = []*CliModule{
	ClusterModule, MetaNodeModule, DataNodeModule, FlashNodeModule, EcModule,
	RateLimitModule, NetworkModule, VolumeModule, BatchModule, KeyValueModule, FileMigrateModule,
}

const (
	ClusterModuleType = iota
	VolumeModuleType
	MetaNodeModuleType
	DataNodeModuleType
	FlashNodeModuleType
	EcModuleType
	RateLimitModuleType
	NetworkModuleType
	BatchModuleType
	KeyValueModuleType
	FileMigrateModuleType
)

var ClusterModule = &CliModule{
	ModuleType: ClusterModuleType,
	Module:     "cluster集群配置",
}
var MetaNodeModule = &CliModule{
	ModuleType: MetaNodeModuleType,
	Module:     "metanode配置",
}
var DataNodeModule = &CliModule{
	ModuleType: DataNodeModuleType,
	Module:     "datanode配置",
}
var FlashNodeModule = &CliModule{
	ModuleType: FlashNodeModuleType,
	Module:     "flashnode配置",
}
var EcModule = &CliModule{
	ModuleType: EcModuleType,
	Module:     "ec配置",
}
var RateLimitModule = &CliModule{
	ModuleType: RateLimitModuleType,
	Module:     "限速配置",
}
var NetworkModule = &CliModule{
	ModuleType: NetworkModuleType,
	Module:     "网络超时配置",
}
var VolumeModule = &CliModule{
	ModuleType: VolumeModuleType,
	Module:     "volume配置",
}
var BatchModule = &CliModule{
	ModuleType: BatchModuleType,
	Module:     "批量设置",
}
var KeyValueModule = &CliModule{
	ModuleType: KeyValueModuleType,
	Module:     "key-value自定义",
}
var FileMigrateModule = &CliModule{
	ModuleType: FileMigrateModuleType,
	Module:     "文件迁移",
}

func GetModule(t int) string {
	switch t {
	case ClusterModuleType:
		return ClusterModule.Module
	case MetaNodeModuleType:
		return MetaNodeModule.Module
	case DataNodeModuleType:
		return DataNodeModule.Module
	case EcModuleType:
		return EcModule.Module
	case RateLimitModuleType:
		return RateLimitModule.Module
	case NetworkModuleType:
		return NetworkModule.Module
	case VolumeModuleType:
		return VolumeModule.Module
	case FlashNodeModuleType:
		return FlashNodeModule.Module
	case BatchModuleType:
		return BatchModule.Module
	case KeyValueModuleType:
		return KeyValueModule.Module
	case FileMigrateModuleType:
		return FileMigrateModule.Module
	}
	return ""
}

const (
	_                  = iota
	OpSetClientPkgAddr = iota
	OpSetRemoteCacheBoostEnableStatus
	OpSetNodeState
	OpSetNodeSetCapacity
	OpAutoMergeNodeSet
	OpMonitorSummarySecond
	OpLogMaxMB
	OpTopologyManager
	OpSetClusterFreeze
	OpSetClusterPersistMode
	// metanode
	OpSetThreshold
	OpSetRocksDBDiskThreshold
	OpSetMemModeRocksDBDiskThreshold
	OpMetaNodeDeleteBatchCount
	OpMetaNodeDeleteWorkerSleepMs
	OpMetaNodeReadDirLimitNum
	OpDeleteEKRecordFileMaxMB
	OpMetaClientRequestConf
	OpMetaRocksDBConf
	OpSetMetaRaftConfig
	OpSetTrashCleanConfig
	OpSetMetaBitMapAllocator
	OpMetaNodeDumpWaterLevel
	OpMetaNodeDumpSnapCountByZone
	// ec
	OpSetEcConfig
	// datanode
	OpDataNodeDeleteRateLimit
	OpDataNodeFixTinyDeleteRecordLimit
	OpDataNodeNormalExtentDeleteExpire
	OpDataNodeFlushFDInterval
	OpDataSyncWALOnUnstableEnableState
	OpDataNodeFlushFDParallelismOnDisk
	OpDataPartitionConsistencyMode
	OpDataNodeDiskReservedRatio
	OpDataNodeRepairTaskCount
	OpDataNodeExtentRepairTask
	OpDataNodeDisableBlacklist
	OpDataNodeTrashKeepTime
	OpBatchSetDataNodeSettings
	// flashnode
	OpFlashNodeReadTimeoutUs
	OpFlashNodeDisableStack
	//  限速
	OpClientReadVolRateLimit
	OpClientWriteVolRateLimit
	OpClientVolOpRateLimit
	OpFlashNodeZoneRate
	OpFlashNodeVolRate
	OpObjectNodeActionRateLimit
	OpDatanodeRateLimit
	OpMetanodeRateLimit
	OpFlashnodeRateLimit
	OpApiReqBwRateLimit
	OpSetBandwidthLimiter
	// 网络
	OpRemoteReadConnTimeoutMs
	OpReadConnTimeoutMs
	OpWriteConnTimeoutMs
	OpNetworkFlowRatio
	// volume
	OpSetVolume
	OpSetVolumeRelease
	OpSetVolRemoteCache
	OpSetVolSmart
	OpSetVolFollowerRead
	OpSetVolTrash
	OpSetVolAuthentication
	OpSetVolMeta
	OpSetVolumeConnConfig
	OpSetVolMinRWPartition
	OpVolAddDp
	OpVolAddMp
	OpVolAddDpRelease
	OpVolSetChildFileMaxCount
	OpVolS3ActionRateLimit
	OpVolClearTrash
	// todo: 编辑卷 高可用类型 支持新的类型
	OpSetVolumePersistMode
	// 批量
	OpBatchSetVolForceROW
	OpBatchSetVolWriteCache
	OpBatchSetVolDpSelector
	OpBatchSetVolConnConfig
	OpBatchSetVolInodeReuse
	OpBatchSetVolReplicaNum
	OpBatchSetVolMpReplicaNum
	OpBatchSetVolFollowerReadCfg
	OpBatchSetVolReqRemoveDup
	OpBatchSetJSSVolumeMetaTag
	OpBatchSetVolPersistMode
	// key-value 查数据库表
	// 文件迁移
	OpMigrationConfigList
)

const (
	isList         = true
	releaseSupport = true
	sparkSupport   = true
)

var (
	MigrationConfigList              = NewCliOperation(OpMigrationConfigList, "迁移配置列表", isList, !releaseSupport, sparkSupport)
	BatchSetVolPersistMode           = NewCliOperation(OpBatchSetVolPersistMode, "vol属性-persist mode", isList, !releaseSupport, sparkSupport)
	BatchSetVolReqRemoveDup          = NewCliOperation(OpBatchSetVolReqRemoveDup, "vol属性-元数据请求去重", isList, !releaseSupport, sparkSupport)
	BatchSetJSSVolumeMetaTag         = NewCliOperation(OpBatchSetJSSVolumeMetaTag, "vol属性-jss卷元数据标记", isList, !releaseSupport, sparkSupport)
	BatchSetVolFollowerReadCfg       = NewCliOperation(OpBatchSetVolFollowerReadCfg, "vol属性-从读配置", isList, !releaseSupport, sparkSupport)
	BatchSetVolMpReplicaNum          = NewCliOperation(OpBatchSetVolMpReplicaNum, "vol属性-mp副本数", isList, !releaseSupport, sparkSupport)
	BatchSetVolReplicaNum            = NewCliOperation(OpBatchSetVolReplicaNum, "vol属性-dp副本数", isList, !releaseSupport, sparkSupport)
	BatchSetVolForceROW              = NewCliOperation(OpBatchSetVolForceROW, "vol属性-ForceROW", isList, !releaseSupport, sparkSupport)
	BatchSetVolWriteCache            = NewCliOperation(OpBatchSetVolWriteCache, "vol属性-writeCache", isList, !releaseSupport, sparkSupport)
	BatchSetVolDpSelector            = NewCliOperation(OpBatchSetVolDpSelector, "vol属性-dpSelector", isList, !releaseSupport, sparkSupport)
	BatchSetVolConnConfig            = NewCliOperation(OpBatchSetVolConnConfig, "vol属性-读写超时时间", isList, releaseSupport, sparkSupport)
	BatchSetVolInodeReuse            = NewCliOperation(OpBatchSetVolInodeReuse, "vol属性-inode复用", isList, releaseSupport, sparkSupport)
	TopologyManager                  = NewCliOperation(OpTopologyManager, "topology管理", !isList, releaseSupport, sparkSupport)
	VolS3ActionRateLimit             = NewCliOperation(OpVolS3ActionRateLimit, "vol s3操作限速", isList, !releaseSupport, sparkSupport)
	VolSetChildFileMaxCount          = NewCliOperation(OpVolSetChildFileMaxCount, "设置子文件数上限", !isList, !releaseSupport, sparkSupport)
	VolAddDp                         = NewCliOperation(OpVolAddDp, "add dp", !isList, !releaseSupport, sparkSupport)
	VolAddDpRelease                  = NewCliOperation(OpVolAddDpRelease, "add dp", !isList, releaseSupport, !sparkSupport)
	VolAddMp                         = NewCliOperation(OpVolAddMp, "add mp", !isList, releaseSupport, sparkSupport)
	SetVolMinRWPartition             = NewCliOperation(OpSetVolMinRWPartition, "设置min RW dp/mp数", !isList, !releaseSupport, sparkSupport)
	SetClientPkgAddr                 = NewCliOperation(OpSetClientPkgAddr, "客户端热升级地址", !isList, !releaseSupport, sparkSupport)
	SetRemoteCacheBoostEnableStatus  = NewCliOperation(OpSetRemoteCacheBoostEnableStatus, "设置分布式缓存开关", !isList, !releaseSupport, sparkSupport)
	SetEcConfig                      = NewCliOperation(OpSetEcConfig, "更新ec config", !isList, !releaseSupport, sparkSupport)
	SetNodeState                     = NewCliOperation(OpSetNodeState, "设置node State", !isList, !releaseSupport, sparkSupport)
	SetNodeSetCapacity               = NewCliOperation(OpSetNodeSetCapacity, "设置nodeSet capacity", !isList, !releaseSupport, sparkSupport)
	SetAutoMergeNodeSet              = NewCliOperation(OpAutoMergeNodeSet, "启动autoMerge nodeSet", !isList, releaseSupport, sparkSupport)
	SetClusterFreeze                 = NewCliOperation(OpSetClusterFreeze, "设置cluster freeze", !isList, !releaseSupport, sparkSupport)
	SetMonitorSummarySecond          = NewCliOperation(OpMonitorSummarySecond, "monitor 收集/上报间隔(s)", !isList, releaseSupport, sparkSupport)
	SetLogMaxMB                      = NewCliOperation(OpLogMaxMB, "log文件最大size", !isList, !releaseSupport, sparkSupport)
	SetClusterPersistMode            = NewCliOperation(OpSetClusterPersistMode, "设置persist mode", !isList, !releaseSupport, sparkSupport)
	SetThreshold                     = NewCliOperation(OpSetThreshold, "设置metanode内存阈值", !isList, releaseSupport, sparkSupport)
	SetRocksDBDiskThreshold          = NewCliOperation(OpSetRocksDBDiskThreshold, "设置metanode rocksdb磁盘阈值", !isList, !releaseSupport, sparkSupport)
	SetMemModeRocksDBDiskThreshold   = NewCliOperation(OpSetMemModeRocksDBDiskThreshold, "设置mem模式metanode rocksdb磁盘阈值", !isList, !releaseSupport, sparkSupport)
	MetaNodeDumpWaterLevel           = NewCliOperation(OpMetaNodeDumpWaterLevel, "设置dump snapshot水位", !isList, !releaseSupport, sparkSupport)
	MetaNodeDumpSnapCountByZone      = NewCliOperation(OpMetaNodeDumpSnapCountByZone, "设置dump snapshot count", isList, !releaseSupport, sparkSupport)
	SetMetaRocksDBConf               = NewCliOperation(OpMetaRocksDBConf, "设置rocksDB配置项 - 多项", !isList, !releaseSupport, sparkSupport)
	SetMetaBitMapAllocator           = NewCliOperation(OpSetMetaBitMapAllocator, "设置BitMap分配器 - 多项", !isList, releaseSupport, sparkSupport)
	SetTrashCleanConfig              = NewCliOperation(OpSetTrashCleanConfig, "trash清理配置 - 多项", !isList, !releaseSupport, sparkSupport)
	SetMetaRaftConfig                = NewCliOperation(OpSetMetaRaftConfig, "raft配置项 - 多项", !isList, !releaseSupport, sparkSupport)
	MetaClientRequestConf            = NewCliOperation(OpMetaClientRequestConf, "处理客户端请求配置 - 多项", !isList, !releaseSupport, sparkSupport)
	MetaNodeDeleteBatchCount         = NewCliOperation(OpMetaNodeDeleteBatchCount, "设置batchDelete count", !isList, !releaseSupport, sparkSupport)
	MetaNodeDeleteWorkerSleepMs      = NewCliOperation(OpMetaNodeDeleteWorkerSleepMs, "设置delete worker sleep时间", !isList, !releaseSupport, sparkSupport)
	MetaNodeReadDirLimitNum          = NewCliOperation(OpMetaNodeReadDirLimitNum, "设置readdir limit", !isList, releaseSupport, sparkSupport)
	DeleteEKRecordFileMaxMB          = NewCliOperation(OpDeleteEKRecordFileMaxMB, "设置delEK record文件大小", !isList, !releaseSupport, sparkSupport)
	DataNodeNormalExtentDeleteExpire = NewCliOperation(OpDataNodeNormalExtentDeleteExpire, "设置normal ext删除记录过期时间", !isList, !releaseSupport, sparkSupport)
	DataNodeFixTinyDeleteRecordLimit = NewCliOperation(OpDataNodeFixTinyDeleteRecordLimit, "设置修复tiny ext删除记录限速", !isList, !releaseSupport, sparkSupport)
	DataNodeMarkDeleteRateLimit      = NewCliOperation(OpDataNodeDeleteRateLimit, "datanode mark delete限速", !isList, !releaseSupport, sparkSupport)
	DataNodeFlushFDInterval          = NewCliOperation(OpDataNodeFlushFDInterval, "设置flush FD间隔", !isList, !releaseSupport, sparkSupport)
	DataNodeFlushFDParallelismOnDisk = NewCliOperation(OpDataNodeFlushFDParallelismOnDisk, "设置flush FD并发度/盘", !isList, !releaseSupport, sparkSupport)
	DataSyncWALOnUnstableEnableState = NewCliOperation(OpDataSyncWALOnUnstableEnableState, "设置SyncWALFlag", !isList, !releaseSupport, sparkSupport)
	DataPartitionConsistencyMode     = NewCliOperation(OpDataPartitionConsistencyMode, "设置dp consistency mode", !isList, !releaseSupport, sparkSupport)
	DataNodeDiskReservedRatio        = NewCliOperation(OpDataNodeDiskReservedRatio, "设置磁盘保留阈值", !isList, !releaseSupport, sparkSupport)
	DataNodeRepairTaskCount          = NewCliOperation(OpDataNodeRepairTaskCount, "设置repair task count", !isList, releaseSupport, !sparkSupport)
	DataNodeExtentRepairTask         = NewCliOperation(OpDataNodeExtentRepairTask, "设置extent修复任务", isList, !releaseSupport, sparkSupport)
	DataNodeDisableBlacklist         = NewCliOperation(OpDataNodeDisableBlacklist, "禁用连接池黑名单", !isList, !releaseSupport, sparkSupport)
	DataNodeTrashKeepTime            = NewCliOperation(OpDataNodeTrashKeepTime, "设置trash保留时间", !isList, !releaseSupport, sparkSupport)
	BatchSetDataNodeSettings         = NewCliOperation(OpBatchSetDataNodeSettings, "节点-setSettings", isList, !releaseSupport, sparkSupport)
	ClientReadVolRateLimit           = NewCliOperation(OpClientReadVolRateLimit, "客户端 vol读请求限速", isList, releaseSupport, sparkSupport)
	ClientWriteVolRateLimit          = NewCliOperation(OpClientWriteVolRateLimit, "客户端 vol写请求限速", isList, releaseSupport, sparkSupport)
	ClientVolOpRateLimit             = NewCliOperation(OpClientVolOpRateLimit, "客户端 vol请求op限速(除读写外)", isList, !releaseSupport, sparkSupport)
	ObjectNodeActionRateLimit        = NewCliOperation(OpObjectNodeActionRateLimit, "S3 请求action限速", isList, !releaseSupport, sparkSupport)
	FlashNodeZoneRate                = NewCliOperation(OpFlashNodeZoneRate, "flashnode 按zone限速", isList, !releaseSupport, sparkSupport)
	FlashNodeVolRate                 = NewCliOperation(OpFlashNodeVolRate, "flashnode 按vol限速", isList, !releaseSupport, sparkSupport)
	FlashNodeReadTimeoutUs           = NewCliOperation(OpFlashNodeReadTimeoutUs, "flashnode 读超时时间", !isList, !releaseSupport, sparkSupport)
	FlashNodeDisableStack            = NewCliOperation(OpFlashNodeDisableStack, "flashnode 区间读", !isList, !releaseSupport, sparkSupport)
	DatanodeRateLimit                = NewCliOperation(OpDatanodeRateLimit, "datanode ratelimit限速", isList, releaseSupport, sparkSupport)
	MetanodeRateLimit                = NewCliOperation(OpMetanodeRateLimit, "metanode ratelimit限速", isList, releaseSupport, sparkSupport)
	FlashnodeRateLimit               = NewCliOperation(OpFlashnodeRateLimit, "flashnode ratelimit限速", isList, !releaseSupport, sparkSupport)
	ApiReqBwRateLimit                = NewCliOperation(OpApiReqBwRateLimit, "api请求带宽限速", isList, releaseSupport, sparkSupport)
	SetBandwidthLimiter              = NewCliOperation(OpSetBandwidthLimiter, "带宽限速", !isList, releaseSupport, sparkSupport)
	RemoteReadConnTimeoutMs          = NewCliOperation(OpRemoteReadConnTimeoutMs, "缓存链路读超时(ms)", !isList, !releaseSupport, sparkSupport)
	ReadConnTimeoutMs                = NewCliOperation(OpReadConnTimeoutMs, "读超时(ms)", isList, !releaseSupport, sparkSupport)
	WriteConnTimeoutMs               = NewCliOperation(OpWriteConnTimeoutMs, "写超时(ms)", isList, !releaseSupport, sparkSupport)
	NetworkFlowRatio                 = NewCliOperation(OpNetworkFlowRatio, "网络流量百分比", isList, releaseSupport, sparkSupport)
	SetVolume                        = NewCliOperation(OpSetVolume, "编辑卷", !isList, !releaseSupport, sparkSupport)
	SetVolumeRelease                 = NewCliOperation(OpSetVolumeRelease, "编辑卷", !isList, releaseSupport, !sparkSupport)
	SetVolRemoteCache                = NewCliOperation(OpSetVolRemoteCache, "编辑卷-分布式缓存", !isList, !releaseSupport, sparkSupport)
	SetVolSmart                      = NewCliOperation(OpSetVolSmart, "编辑卷-智能vol", !isList, !releaseSupport, sparkSupport)
	SetVolFollowerRead               = NewCliOperation(OpSetVolFollowerRead, "编辑卷-从读", !isList, !releaseSupport, sparkSupport)
	SetVolTrash                      = NewCliOperation(OpSetVolTrash, "编辑卷-回收站", !isList, !releaseSupport, sparkSupport)
	SetVolAuthentication             = NewCliOperation(OpSetVolAuthentication, "编辑卷-鉴权", !isList, !releaseSupport, sparkSupport)
	SetVolMeta                       = NewCliOperation(OpSetVolMeta, "编辑卷-meta相关", !isList, !releaseSupport, sparkSupport)
	SetVolumeConnConfig              = NewCliOperation(OpSetVolumeConnConfig, "编辑卷-connConfig", !isList, !releaseSupport, sparkSupport)
	SetVolumePersistMode             = NewCliOperation(OpSetVolumePersistMode, "编辑卷-persist mode", !isList, !releaseSupport, sparkSupport)
	ClearVolumeTrash                 = NewCliOperation(OpVolClearTrash, "清理回收站数据", !isList, !releaseSupport, sparkSupport)
)

var CliOperationMap = map[int][]*CliOperation{
	ClusterModuleType: {
		SetClientPkgAddr,
		SetRemoteCacheBoostEnableStatus,
		SetNodeState,
		SetNodeSetCapacity,
		SetMonitorSummarySecond,
		SetLogMaxMB,
		TopologyManager,
		SetAutoMergeNodeSet,
		SetClusterFreeze,
		SetClusterPersistMode,
	},
	VolumeModuleType: {
		SetVolume,
		SetVolRemoteCache,
		SetVolumeConnConfig,
		SetVolSmart,
		SetVolFollowerRead,
		SetVolTrash,
		SetVolumeRelease,
		SetVolMeta,
		SetVolAuthentication,
		ClientVolOpRateLimit,
		SetVolMinRWPartition,
		VolAddDp,
		VolAddMp,
		VolAddDpRelease,
		VolSetChildFileMaxCount,
		VolS3ActionRateLimit,
		SetVolumePersistMode,
		ClearVolumeTrash,
	},
	MetaNodeModuleType: {
		SetThreshold,
		SetRocksDBDiskThreshold,
		SetMemModeRocksDBDiskThreshold,
		MetaNodeDumpWaterLevel,
		MetaNodeDumpSnapCountByZone,
		SetMetaRocksDBConf,
		SetMetaBitMapAllocator,
		SetTrashCleanConfig,
		SetMetaRaftConfig,
		MetaClientRequestConf,
		MetaNodeDeleteBatchCount,
		MetaNodeDeleteWorkerSleepMs,
		MetaNodeReadDirLimitNum,
		DeleteEKRecordFileMaxMB,
	},
	DataNodeModuleType: {
		DataNodeFlushFDInterval,
		DataSyncWALOnUnstableEnableState,
		DataNodeFlushFDParallelismOnDisk,
		DataNodeNormalExtentDeleteExpire,
		DataNodeFixTinyDeleteRecordLimit,
		DataPartitionConsistencyMode,
		DataNodeDiskReservedRatio,
		DataNodeRepairTaskCount,
		DataNodeExtentRepairTask,
		DataNodeDisableBlacklist,
		DataNodeTrashKeepTime,
		BatchSetDataNodeSettings,
	},
	FlashNodeModuleType: {
		FlashNodeReadTimeoutUs,
		FlashNodeDisableStack,
	},
	EcModuleType: {
		SetEcConfig,
	},
	RateLimitModuleType: {
		DatanodeRateLimit,
		MetanodeRateLimit,
		FlashnodeRateLimit,
		DataNodeMarkDeleteRateLimit,
		ClientReadVolRateLimit,
		ClientWriteVolRateLimit,
		ObjectNodeActionRateLimit,
		FlashNodeZoneRate,
		FlashNodeVolRate,
		ApiReqBwRateLimit,
		SetBandwidthLimiter,
	},
	NetworkModuleType: {
		RemoteReadConnTimeoutMs,
		ReadConnTimeoutMs,
		WriteConnTimeoutMs,
		NetworkFlowRatio,
	},
	BatchModuleType: {
		BatchSetVolForceROW,
		BatchSetVolInodeReuse,
		BatchSetVolConnConfig,
		BatchSetVolWriteCache,
		BatchSetVolDpSelector,
		BatchSetVolReplicaNum,
		BatchSetVolMpReplicaNum,
		BatchSetVolFollowerReadCfg,
		BatchSetVolReqRemoveDup,
		BatchSetJSSVolumeMetaTag,
		BatchSetVolPersistMode,
	},
	FileMigrateModuleType: {
		MigrationConfigList,
	},
}

var CliOperations = map[int][]CliValueMetric{
	// valueName是key 使用proto中定义或者master/const中定义的 保持一致
	OpSetClientPkgAddr: {
		CliValueMetric{"addr", "Set url for download client package used for client hot-upgrade.", "", "", manualSetValueForm(InputBox), 0},
	},
	OpSetRemoteCacheBoostEnableStatus: {
		CliValueMetric{proto.RemoteCacheBoostEnableKey, "Set cluster remoteCacheBoostStatus to ON/OFF", "", "", manualSetValueForm(Slider), 0},
	},
	OpSetEcConfig: {
		CliValueMetric{"ecScrubEnable", "Enable ec scrub", "", "", manualSetValueForm(Slider), 0},
		CliValueMetric{"ecScrubPeriod", "Specify ec scrub period unit: min", "", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{"ecDiskConcurrentExtents", "Specify every disk concurrent scrub extents", "", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{"maxCodecConcurrent", "Specify every codecNode concurrent migrate dp", "", "", manualSetValueForm(InputBox), 0},
	},
	// 没有回显的值 直接设置
	OpSetNodeState: {
		CliValueMetric{"addrList", "使用“;”分隔节点ip", "string", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{"nodeType", "Should be one of dataNode/metaNode/all", "string", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{"zoneName", "", "string", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{"state", "offline state, true or false", "string", "", manualSetValueForm(InputBox), 0},
	},
	OpSetNodeSetCapacity: {
		CliValueMetric{proto.NodeSetCapacityKey, "", "", "", manualSetValueForm(InputBox), 0},
	},
	OpAutoMergeNodeSet: {
		CliValueMetric{"enable", "", "", "", manualSetValueForm(Slider), 0},
	},
	OpSetClusterFreeze: {
		CliValueMetric{"enable", "", "", "", manualSetValueForm(Slider), 0},
	},
	OpMonitorSummarySecond: {
		CliValueMetric{"monitorSummarySec", "summary seconds for monitor, sec >= 5", "", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{"monitorReportSec", "report seconds for monitor, sec >= 5", "", "", manualSetValueForm(InputBox), 0},
	},
	OpLogMaxMB: {
		CliValueMetric{proto.LogMaxMB, "log max MB", "", "", manualSetValueForm(InputBox), 0},
	},
	OpTopologyManager: {
		CliValueMetric{proto.TopologyFetchIntervalMinKey, "topologyManager force fetch interval, unit: second", "", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{proto.TopologyForceFetchIntervalSecKey, "topology fetch interval, unit: min", "", "", manualSetValueForm(InputBox), 0},
	},
	OpSetClusterPersistMode: {
		CliValueMetric{proto.PersistenceModeKey, "0-未指定, 1-WriteBack(写PageCache不强制落盘), 2:WriteThrough(写穿, 强制落盘)", "", "", manualSetValueForm(DropDown), 0},
	},
	OpSetThreshold: {
		CliValueMetric{"threshold", "Set memory threshold of metanodes, 0.00~1.00", "", "", manualSetValueForm(InputBox), 0},
	},
	OpSetRocksDBDiskThreshold: {
		CliValueMetric{"threshold", "Set RocksDB Disk threshold of metanodes, 0.00~1.00", "", "", manualSetValueForm(InputBox), 0},
	},
	OpSetMemModeRocksDBDiskThreshold: {
		CliValueMetric{"threshold", "Set RocksDB Disk threshold of mem mode metanodes, 0.00~1.00", "", "", manualSetValueForm(InputBox), 0},
	},
	OpMetaNodeDumpWaterLevel: {
		CliValueMetric{"metaNodeDumpWaterLevel", "metanode dump snapshot water level", "", "", manualSetValueForm(InputBox), 0},
	},
	OpMetaNodeDumpSnapCountByZone: {
		CliValueMetric{"zoneName", " acts as default", "", "", manualSetValueForm(DropDown), 0},
		CliValueMetric{proto.MetaNodeDumpSnapCountKey, "metanode dump snapshot count", "", "", manualSetValueForm(InputBox), 0},
	},
	OpMetaRocksDBConf: {
		CliValueMetric{proto.RocksDBDiskReservedSpaceKey, "设置rocksDB磁盘预留空间(MB)", "", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{proto.MetaRockDBWalFileMaxMB, "设置rocksDB配置: wal_size_limit_mb, unit:MB, default:10MB", "", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{proto.MetaRocksDBWalMemMaxMB, "设置rocksDB配置: max_total_wal_size, unit:MB, default:3MB", "", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{proto.MetaRocksDBLogMaxMB, "设置rocksDB配置: max_log_file_size, unit:MB, default:1MB", "", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{proto.MetaRocksLogReservedDay, "设置rocksDB配置: log_file_time_to_roll, unit:Day, default:3day", "", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{proto.MetaRocksLogReservedCnt, "设置rocksDB配置: keep_log_file_num, default:3", "", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{proto.MetaRocksDisableFlushWalKey, "设置rocksDB配置: flush wal flag, 0:enable, 1:disable, default:0", "", "", manualSetValueForm(Slider), 0},
		CliValueMetric{proto.MetaRocksWalFlushIntervalKey, "设置rocksDB配置: flush wal interval, unit:min, default:30min", "", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{proto.MetaRocksWalTTLKey, "设置rocksDB配置: wal_ttl_seconds, unit:second, default:60s", "", "", manualSetValueForm(InputBox), 0},
	},
	OpSetMetaBitMapAllocator: {
		CliValueMetric{proto.AllocatorMaxUsedFactorKey, "float64, bit map allocator max used factor for available", "", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{proto.AllocatorMinFreeFactorKey, "float64, bit map allocator min free factor for available", "", "", manualSetValueForm(InputBox), 0},
	},
	OpSetTrashCleanConfig: {
		CliValueMetric{proto.MetaTrashCleanIntervalKey, "clean del inode interval, unit:min", "", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{proto.TrashCleanDurationKey, "trash clean max duration for each time, unit:min", "", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{proto.TrashItemCleanMaxCountKey, "trash clean max count for each time", "", "", manualSetValueForm(InputBox), 0},
	},
	OpSetMetaRaftConfig: {
		CliValueMetric{proto.MetaRaftLogSizeKey, "meta node raft log size, unit:MB", "", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{proto.MetaRaftLogCapKey, "meta node raft log cap, unit:MB", "", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{proto.MetaSyncWalEnableStateKey, "metaSyncWALFlag", "", "", manualSetValueForm(Slider), 0},
	},
	OpMetaClientRequestConf: {
		CliValueMetric{proto.ClientReqRemoveDupFlagKey, "client req remove dup flag", "", "", manualSetValueForm(Slider), 0},
		CliValueMetric{proto.ClientReqRecordReservedMinKey, "client req records reserved min", "", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{proto.ClientReqRecordReservedCntKey, "client req records reserved count", "", "", manualSetValueForm(InputBox), 0},
	},
	OpMetaNodeDeleteBatchCount: {
		CliValueMetric{"batchCount", "MetaNodeDeleteBatchCount", "", "", manualSetValueForm(InputBox), 0},
	},
	OpMetaNodeDeleteWorkerSleepMs: {
		CliValueMetric{"deleteWorkerSleepMs", "delete worker sleep time, unit: ms", "", "", manualSetValueForm(InputBox), 0},
	},
	OpMetaNodeReadDirLimitNum: {
		CliValueMetric{"metaNodeReadDirLimit", "readdir limit count", "", "", manualSetValueForm(InputBox), 0},
	},
	OpDeleteEKRecordFileMaxMB: {
		CliValueMetric{proto.MetaDelEKRecordFileMaxMB, "meta node delete ek record file max MB", "", "", manualSetValueForm(InputBox), 0},
	},
	OpDataNodeFlushFDInterval: {
		CliValueMetric{"dataNodeFlushFDInterval", "time interval for flushing WAL and open FDs on DataNode, unit is second", "", "", manualSetValueForm(InputBox), 0},
	},
	OpDataNodeFlushFDParallelismOnDisk: {
		CliValueMetric{"dataNodeFlushFDParallelismOnDisk", "parallelism for flushing WAL and open FDs on DataNode per disk", "", "", manualSetValueForm(InputBox), 0},
	},
	OpDataSyncWALOnUnstableEnableState: {
		CliValueMetric{proto.DataSyncWalEnableStateKey, "dataSyncWALFlag", "", "", manualSetValueForm(Slider), 0},
	},
	OpDataNodeNormalExtentDeleteExpire: {
		CliValueMetric{"normalExtentDeleteExpire", "datanode normal extent delete record expire time(second, >=600)", "", "", manualSetValueForm(InputBox), 0},
	},
	OpDataNodeFixTinyDeleteRecordLimit: {
		CliValueMetric{"fixTinyDeleteRecordKey", "data node fix tiny delete record limit", "", "", manualSetValueForm(InputBox), 0},
	},
	OpDataPartitionConsistencyMode: {
		// 增加module参数, 否则接口会认为只有这一个参数
		CliValueMetric{"dataPartitionConsistencyMode", "cluster consistency mode for data partitions, 0-Standard 1-Strict", "", "", manualSetValueForm(InputBox), 0},
	},
	OpDataNodeDiskReservedRatio: {
		// 增加module参数, 否则接口会认为只有这一个参数
		CliValueMetric{proto.DataNodeDiskReservedRatioKey, "data node disk reserved ratio, greater than or equal 0", "", "", manualSetValueForm(InputBox), 0},
	},
	OpDataNodeRepairTaskCount: {
		CliValueMetric{"dataNodeRepairTaskCount", "", "", "", manualSetValueForm(InputBox), 0},
	},
	OpDataNodeExtentRepairTask: {
		CliValueMetric{"partitionID", "dpId", "", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{"extentID", "", "extID，多个用-分隔", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{"host", "datanode节点地址，无需端口", "", "", manualSetValueForm(InputBox), 0},
	},
	OpDataNodeDisableBlacklist: {
		CliValueMetric{"dataNodeDisableBlacklist", "是否禁用DataNode连接池开关", "", "", manualSetValueForm(Slider), 0},
	},
	OpDataNodeTrashKeepTime: {
		CliValueMetric{"dataNodeTrashKeepTimeSec", "DataNode Trash保留时间(秒)(>=-1, -1表示关闭)", "", "", manualSetValueForm(InputBox), 0},
	},
	OpBatchSetDataNodeSettings: {
		// 开关设置成string类型，可以区分出填了还是没填
		CliValueMetric{"hosts", "节点地址(多个,分隔)", "", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{"disableBlackList", "是否禁用黑名单(true/false)", "", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{"disableAutoDeleteTrash", "是否禁用Trash自动删除(true/false)", "", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{"trashKeepTimeSec", "Trash保留时间(秒)(>=-1, -1表示关闭)", "", "", manualSetValueForm(InputBox), 0},
	},
	// modul(请求接口的时候别忘了) zone vol opcode rateLimitIndex rateLimit
	OpDatanodeRateLimit: {
		CliValueMetric{"zoneName", " acts as default", "", "", manualSetValueForm(DropDown), 1},
		CliValueMetric{"volume", " acts as default", "", "", manualSetValueForm(InputBox), 1},
		CliValueMetric{"opcode", "", "", "", manualSetValueForm(DropDown), 0},
		CliValueMetric{proto.RateLimitIndexKey, "rate limit index, 0: timeout(ns); 1-3: count, in bytes, out bytes; 4-6: per disk; 7-9: per partition", "", "", manualSetValueForm(DropDown), 0},
		CliValueMetric{proto.RateLimitKey, "", "", "", manualSetValueForm(InputBox), 0},
	},
	OpMetanodeRateLimit: {
		CliValueMetric{"zoneName", " acts as default", "", "", manualSetValueForm(DropDown), 1},
		CliValueMetric{"volume", " acts as default", "", "", manualSetValueForm(InputBox), 1},
		CliValueMetric{"opcode", "", "", "", manualSetValueForm(DropDown), 0},
		CliValueMetric{proto.RateLimitIndexKey, "rate limit index, 0: timeout(ns); 1-3: count, in bytes, out bytes; 4-6: per disk; 7-9: per partition", "", "", manualSetValueForm(DropDown), 0},
		CliValueMetric{proto.RateLimitKey, "", "", "", manualSetValueForm(InputBox), 0},
	},
	OpFlashnodeRateLimit: {
		CliValueMetric{"zoneName", " acts as default", "", "", manualSetValueForm(DropDown), 1},
		CliValueMetric{"volume", " acts as default", "", "", manualSetValueForm(InputBox), 1},
		CliValueMetric{"opcode", "", "", "", manualSetValueForm(DropDown), 0},
		CliValueMetric{proto.RateLimitIndexKey, "rate limit index, 0: timeout(ns); 1-3: count, in bytes, out bytes; 4-6: per disk; 7-9: per partition", "", "", manualSetValueForm(DropDown), 0},
		CliValueMetric{proto.RateLimitKey, "", "", "", manualSetValueForm(InputBox), 0},
	},
	OpApiReqBwRateLimit: {
		CliValueMetric{"opcode", "", "", "", manualSetValueForm(DropDown), 0},
		CliValueMetric{"apiReqBwRate", "rateLimit value", "", "", manualSetValueForm(InputBox), 0},
	},
	OpSetBandwidthLimiter: {
		CliValueMetric{"bw", "bandwidth ratelimit value, unit: byte", "", "", manualSetValueForm(InputBox), 0},
	},
	OpDataNodeDeleteRateLimit: {
		CliValueMetric{proto.DataNodeMarkDeleteRateKey, "data node mark delete request rate limit", "", "", manualSetValueForm(InputBox), 0},
	},
	OpClientReadVolRateLimit: {
		CliValueMetric{"volume", "", "", "", manualSetValueForm(MultiSelect), 1},
		CliValueMetric{"clientReadVolRate", "", "", "", manualSetValueForm(InputBox), 0},
	},
	OpClientWriteVolRateLimit: {
		CliValueMetric{"volume", "", "", "", manualSetValueForm(MultiSelect), 1},
		CliValueMetric{"clientWriteVolRate", "", "", "", manualSetValueForm(InputBox), 0},
	},
	OpClientVolOpRateLimit: {
		// 没在限速中 在vol配置中
		CliValueMetric{"opcode", "", "", "", manualSetValueForm(DropDown), 0},
		CliValueMetric{"clientVolOpRate", "", "", "", manualSetValueForm(InputBox), 0},
	},
	OpObjectNodeActionRateLimit: {
		CliValueMetric{"volume", "", "", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{"action", "object node action", "", "", manualSetValueForm(DropDown), 0},
		CliValueMetric{"objectVolActionRate", "", "", "", manualSetValueForm(InputBox), 0},
	},
	OpFlashNodeZoneRate: {
		CliValueMetric{"zoneName", "if zone is empty, set rate limit for all zones", "", "", manualSetValueForm(DropDown), 0},
		CliValueMetric{proto.FlashNodeRateKey, "flash node cache read request rate limit", "", "", manualSetValueForm(InputBox), 0},
	},
	OpFlashNodeVolRate: {
		CliValueMetric{"zoneName", "if zone is empty, set rate limit for all zones", "", "", manualSetValueForm(DropDown), 1},
		CliValueMetric{"volume", "", "", "", manualSetValueForm(InputBox), 1},
		CliValueMetric{proto.FlashNodeVolRateKey, "flash node cache read request rate limit for a volume", "", "", manualSetValueForm(InputBox), 0},
	},
	OpFlashNodeReadTimeoutUs: {
		CliValueMetric{"flashNodeReadTimeoutUs", "读超时，单位us(>=500)", "", "", manualSetValueForm(InputBox), 0},
	},
	OpFlashNodeDisableStack: {
		CliValueMetric{"flashNodeDisableStack", "禁用FlashNode区间读", "", "", manualSetValueForm(Slider), 0},
	},
	OpRemoteReadConnTimeoutMs: {
		CliValueMetric{proto.RemoteReadConnTimeoutKey, "缓存客户端读超时时间(ms)", "", "", manualSetValueForm(InputBox), 0},
	},
	OpReadConnTimeoutMs: {
		CliValueMetric{"zoneName", "if zone is empty, set readTimeout for all zones", "", "", manualSetValueForm(DropDown), 0},
		CliValueMetric{proto.ReadConnTimeoutMsKey, "客户端读超时时间(ms)", "", "", manualSetValueForm(InputBox), 0},
	},
	OpWriteConnTimeoutMs: {
		CliValueMetric{"zoneName", "if zone is empty, set writeTimeout for all zones", "", "", manualSetValueForm(DropDown), 0},
		CliValueMetric{proto.WriteConnTimeoutMsKey, "客户端写超时时间(ms)", "", "", manualSetValueForm(InputBox), 0},
	},
	OpNetworkFlowRatio: {
		CliValueMetric{"module", "role of cluster, like master", "", "", manualSetValueForm(DropDown), 0},
		CliValueMetric{proto.NetworkFlowRatioKey, "network flow ratio percent: [0, 100]", "", "", manualSetValueForm(InputBox), 0},
	},
	OpSetVolume: {
		CliValueMetric{"volWriteMutex", "", "bool", "", manualSetValueForm(Slider), 0},
		CliValueMetric{"forceROW", "", "bool", "", manualSetValueForm(Slider), 0},
		CliValueMetric{"writeCache", "", "bool", "", manualSetValueForm(Slider), 0},
		CliValueMetric{"autoRepair", "", "bool", "", manualSetValueForm(Slider), 0},
		CliValueMetric{"zoneName", "", "string", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{"capacity", "", "", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{"replicaNum", "", "", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{"mpReplicaNum", "", "", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{"crossRegion", "Set cross region high available type(0 for default, 1 for quorum)", "", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{"ekExpireSec", "int64\nSpecify the expiration second of the extent cache (-1 means never expires)", "", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{"compactTag", "string\nSpecify volume compact", "", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{"umpCollectWay", "Set ump collect way(0-unknown 1-file 2-jmtp client)", "", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{"dpSelectorName", "default/kfaster", "string", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{"dpSelectorParm", "请输入(0, 100)之间的数", "string", "", manualSetValueForm(InputBox), 0},
	},
	OpSetVolumeConnConfig: {
		CliValueMetric{"readConnTimeout", "int64\nSet client read connection timeout, unit: ms", "", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{"writeConnTimeout", "int64\nSet client write connection timeout, unit: ms", "", "", manualSetValueForm(InputBox), 0},
	},
	OpSetVolAuthentication: {
		CliValueMetric{"authenticate", "Enable authenticate", "bool", "", manualSetValueForm(Slider), 0},
		CliValueMetric{"enableToken", "ReadOnly/ReadWrite token validation for fuse client", "bool", "", manualSetValueForm(Slider), 0},
		CliValueMetric{"bucketPolicy", "Set bucket access policy for S3(0 for private 1 for public-read)", "", "", manualSetValueForm(InputBox), 0},
	},
	OpSetVolMeta: {
		CliValueMetric{proto.EnableBitMapAllocatorKey, "", "bool", "", manualSetValueForm(Slider), 0},
		CliValueMetric{proto.BitMapSnapFrozenHour, "int64,延迟分配时间，单位: hour", "", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{proto.VolRemoveDupFlagKey, "客户端请求幂等操作开关", "bool", "", manualSetValueForm(Slider), 0},
		CliValueMetric{proto.ReqRecordReservedTimeKey, "客户端请求保留时间", "", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{proto.ReqRecordMaxCountKey, "客户端请求保留最大条数", "", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{"storeMode", "Specify volume default store mode [1:Mem, 2:Rocks]", "", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{"metaLayout", "Specify volume meta layout num1,num2 [num1:rocksdb mp percent, num2:rocksdb replicas percent]", "", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{"batchDelInodeCnt", "", "", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{"delInodeInterval", "Specify del inode interval, unit:ms", "", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{proto.MetaNodeTruncateEKCountKey, "int64\ntruncate EK count every time when del inode", "", "", manualSetValueForm(InputBox), 0},
	},
	OpSetVolRemoteCache: {
		CliValueMetric{"remoteCacheBoostEnable", "卷缓存加速开关", "bool", "", manualSetValueForm(Slider), 0},
		CliValueMetric{"remoteCacheAutoPrepare", "缓存预热开关", "bool", "", manualSetValueForm(Slider), 0},
		CliValueMetric{"remoteCacheBoostPath", "设置加速路径，多个路径','分隔", "string", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{"remoteCacheTTL", "单位：秒", "string", "", manualSetValueForm(InputBox), 0},
	},
	OpSetVolSmart: {
		CliValueMetric{"smart", "", "bool", "", manualSetValueForm(Slider), 0},
		CliValueMetric{"smartRules", "Specify volume smart rules separate by ','", "string", "", manualSetValueForm(InputBox), 0},
	},
	OpSetVolFollowerRead: {
		CliValueMetric{"followerRead", "从节点读", "bool", "", manualSetValueForm(Slider), 0},
		CliValueMetric{"hostDelayInterval", "Specify host delay update interval [unit: s]", "", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{"follReadHostWeight", "assign weight for the lowest delay host when enable FollowerRead,(0,100)", "", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{"nearRead", "", "bool", "", manualSetValueForm(Slider), 0},
	},
	OpSetVolTrash: {
		CliValueMetric{"trashRemainingDays", "", "", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{proto.TrashCleanDurationKey, "Trash clean duration, unit:min", "", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{proto.MetaTrashCleanIntervalKey, "specify trash clean interval, unit:min", "", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{proto.TrashItemCleanMaxCountKey, "Trash clean max count", "", "", manualSetValueForm(InputBox), 0},
	},
	OpVolSetChildFileMaxCount: {
		CliValueMetric{proto.ChildFileMaxCountKey, "值类型：uint32", "", "", manualSetValueForm(InputBox), 0},
	},
	OpSetVolMinRWPartition: {
		CliValueMetric{"minWritableDp", "", "", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{"minWritableMp", "", "", "", manualSetValueForm(InputBox), 0},
	},
	OpVolAddDp: {
		//CliValueMetric{"dpCount", "显示值，修改不生效", "", "", manualSetValueForm(InputBox), 0},
		//CliValueMetric{"rwDpCount", "显示值，修改不生效", "", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{"count", "需要增加的dp数量", "", "", manualSetValueForm(InputBox), 0},
	},
	OpVolAddDpRelease: {
		//CliValueMetric{"dpCount", "显示值，修改不生效", "", "", manualSetValueForm(InputBox), 0},
		//CliValueMetric{"rwDpCount", "显示值，修改不生效", "", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{"count", "需要增加的dp数量", "", "", manualSetValueForm(InputBox), 0},
		//CliValueMetric{"type", "partition类型：tiny/extent", "", "", manualSetValueForm(InputBox), 0},
	},
	OpVolAddMp: {
		//CliValueMetric{"mpCount", "显示值，修改不生效", "", "", manualSetValueForm(InputBox), 0},
		//CliValueMetric{"rwMpCount", "显示值，修改不生效", "", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{"start", "inodeStart", "", "", manualSetValueForm(InputBox), 0},
	},
	OpSetVolumeRelease: {
		CliValueMetric{"capacity", "", "", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{"minWritableDp", "", "", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{"minWritableMp", "", "", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{"batchDelInodeCnt", "", "", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{"readConnTimeout", "单位：ms，请设置>=1000的值", "", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{"writeConnTimeout", "单位：ms", "", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{"enableToken", "", "bool", "", manualSetValueForm(Slider), 0},
		CliValueMetric{"crossPod", "", "bool", "", manualSetValueForm(Slider), 0},
		CliValueMetric{"autoRepair", "", "bool", "", manualSetValueForm(Slider), 0},
		CliValueMetric{proto.EnableBitMapAllocatorKey, "", "bool", "", manualSetValueForm(Slider), 0},
		CliValueMetric{proto.BitMapSnapFrozenHour, "int64,延迟分配时间，单位: hour", "", "", manualSetValueForm(InputBox), 0},
	},
	OpVolS3ActionRateLimit: {
		CliValueMetric{"action", "object node action", "", "", manualSetValueForm(DropDown), 0},
		CliValueMetric{"objectVolActionRate", "", "", "", manualSetValueForm(InputBox), 0},
	},
	OpVolClearTrash: {
		CliValueMetric{"doCleanTrash", "是否清理回收站", "", "", manualSetValueForm(Slider), 0},
	},
	OpSetVolumePersistMode: {
		CliValueMetric{proto.PersistenceModeKey, "0-未指定, 1-WriteBack(写PageCache不强制落盘), 2:WriteThrough(写穿, 强制落盘)", "", "", manualSetValueForm(DropDown), 0},
	},
	OpBatchSetVolForceROW: {
		CliValueMetric{"volume", "", "", "", manualSetValueForm(MultiSelect), 0},
		CliValueMetric{"forceROW", "", "", "", manualSetValueForm(Slider), 0},
	},
	OpBatchSetVolWriteCache: {
		CliValueMetric{"volume", "", "", "", manualSetValueForm(MultiSelect), 0},
		CliValueMetric{"writeCache", "", "", "", manualSetValueForm(Slider), 0},
	},
	OpBatchSetVolInodeReuse: {
		CliValueMetric{"volume", "", "", "", manualSetValueForm(MultiSelect), 0},
		CliValueMetric{proto.EnableBitMapAllocatorKey, "", "bool", "", manualSetValueForm(Slider), 0},
		// todo: 延迟分配时间
	},
	OpBatchSetVolConnConfig: {
		CliValueMetric{"volume", "", "", "", manualSetValueForm(MultiSelect), 0},
		CliValueMetric{"readConnTimeout", "int64\nSet client read connection timeout, unit: ms", "", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{"writeConnTimeout", "int64\nSet client write connection timeout, unit: ms", "", "", manualSetValueForm(InputBox), 0},
	},
	OpBatchSetVolDpSelector: {
		CliValueMetric{"volume", "", "string", "", manualSetValueForm(MultiSelect), 0},
		CliValueMetric{"dpSelectorName", "default/kfaster", "string", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{"dpSelectorParm", "请输入(0, 100)之间的数", "", "", manualSetValueForm(InputBox), 0},
	},
	OpBatchSetVolReplicaNum: {
		CliValueMetric{"volume", "", "", "", manualSetValueForm(MultiSelect), 0},
		CliValueMetric{"replicaNum", "请输入副本数", "", "", manualSetValueForm(InputBox), 0},
	},
	OpBatchSetVolMpReplicaNum: {
		CliValueMetric{"volume", "", "", "", manualSetValueForm(MultiSelect), 0},
		CliValueMetric{"mpReplicaNum", "请输入副本数", "", "", manualSetValueForm(InputBox), 0},
	},
	OpBatchSetVolFollowerReadCfg: {
		CliValueMetric{"volume", "", "", "", manualSetValueForm(MultiSelect), 0},
		CliValueMetric{"hostDelayInterval", "统计host时延的周期[unit: s](>0表示该功能启用，0关闭该功能)", "", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{"follReadHostWeight", "延迟最低的host分流权重(0,100)", "", "", manualSetValueForm(InputBox), 0},
	},
	OpBatchSetVolReqRemoveDup: {
		CliValueMetric{"volume", "", "", "", manualSetValueForm(MultiSelect), 0},
		CliValueMetric{proto.VolRemoveDupFlagKey, "客户端请求幂等操作开关", "bool", "", manualSetValueForm(Slider), 0},
		CliValueMetric{proto.ReqRecordReservedTimeKey, "客户端请求保留时间", "", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{proto.ReqRecordMaxCountKey, "客户端请求保留最大条数", "", "", manualSetValueForm(InputBox), 0},
	},
	OpBatchSetJSSVolumeMetaTag: {
		CliValueMetric{"volume", "", "", "", manualSetValueForm(MultiSelect), 0},
		CliValueMetric{proto.MetaOutKey, "对象存储vol元数据标志", "bool", "", manualSetValueForm(Slider), 0},
	},
	OpBatchSetVolPersistMode: {
		CliValueMetric{"volume", "", "", "", manualSetValueForm(MultiSelect), 1},
		CliValueMetric{proto.PersistenceModeKey, "vol持久化模式：0-未指定, 1-WriteBack(写PageCache不强制落盘), 2:WriteThrough(写穿, 强制落盘)", "", "", manualSetValueForm(DropDown), 0},
	},
	OpMigrationConfigList: {
		CliValueMetric{"volume", "卷名(可多选)", "string", "", manualSetValueForm(MultiSelect), 1},
		CliValueMetric{"smart", "是否开启冷热分离", "bool", "", manualSetValueForm(Slider), 0},
		CliValueMetric{"compact", "是否开启碎片化整理", "bool", "", manualSetValueForm(Slider), 0},
		CliValueMetric{"migrationBack", "冷热分离是否回迁", "bool", "", manualSetValueForm(Slider), 0},
		CliValueMetric{"smartRules", "迁移规则:inodeAccessTime:timestamp:1726827474:hdd\n" + "inodeAccessTime:sec:60:hdd\n" + "inodeAccessTime:days:10:hdd", "string", "", manualSetValueForm(InputBox), 0},
		CliValueMetric{"hddDirs", "指定目录下文件迁移,/ 表示根目录下所有文件都迁移\n/a/(.*\\.log$) 表示a目录下以.log结尾的文件迁移\n1.括号中的是正则匹配表达式，匹配文件名\n2.可以配置多个以英文逗号隔开",
			"string", "", manualSetValueForm(InputBox), 0},
	},
}

func GetOperationShortMsg(operationCode int) string {
	switch operationCode {
	case OpSetClientPkgAddr:
		return SetClientPkgAddr.Short
	case OpSetRemoteCacheBoostEnableStatus:
		return SetRemoteCacheBoostEnableStatus.Short
	case OpAutoMergeNodeSet:
		return SetAutoMergeNodeSet.Short
	case OpSetClusterFreeze:
		return SetClusterFreeze.Short
	case OpLogMaxMB:
		return SetLogMaxMB.Short
	case OpSetClusterPersistMode:
		return SetClusterPersistMode.Short
	case OpSetThreshold:
		return SetThreshold.Short
	case OpSetRocksDBDiskThreshold:
		return SetRocksDBDiskThreshold.Short
	case OpSetMemModeRocksDBDiskThreshold:
		return SetMemModeRocksDBDiskThreshold.Short
	case OpMetaNodeDumpWaterLevel:
		return MetaNodeDumpWaterLevel.Short
	case OpMetaNodeDumpSnapCountByZone:
		return MetaNodeDumpSnapCountByZone.Short
	case OpMetaRocksDBConf:
		return SetMetaRocksDBConf.Short
	case OpSetMetaBitMapAllocator:
		return SetMetaBitMapAllocator.Short
	case OpSetTrashCleanConfig:
		return SetTrashCleanConfig.Short
	case OpSetMetaRaftConfig:
		return SetMetaRaftConfig.Short
	case OpMetaClientRequestConf:
		return MetaClientRequestConf.Short
	case OpMetaNodeDeleteBatchCount:
		return MetaNodeDeleteBatchCount.Short
	case OpMetaNodeDeleteWorkerSleepMs:
		return MetaNodeDeleteWorkerSleepMs.Short
	case OpMetaNodeReadDirLimitNum:
		return MetaNodeReadDirLimitNum.Short
	case OpDeleteEKRecordFileMaxMB:
		return DeleteEKRecordFileMaxMB.Short
	case OpSetEcConfig:
		return SetEcConfig.Short
	case OpSetNodeState:
		return SetNodeState.Short
	case OpSetNodeSetCapacity:
		return SetNodeSetCapacity.Short
	case OpMonitorSummarySecond:
		return SetMonitorSummarySecond.Short
	case OpDataNodeFlushFDInterval:
		return DataNodeFlushFDInterval.Short
	case OpDataSyncWALOnUnstableEnableState:
		return DataSyncWALOnUnstableEnableState.Short
	case OpDataNodeFlushFDParallelismOnDisk:
		return DataNodeFlushFDParallelismOnDisk.Short
	case OpDataPartitionConsistencyMode:
		return DataPartitionConsistencyMode.Short
	case OpDataNodeDiskReservedRatio:
		return DataNodeDiskReservedRatio.Short
	case OpDataNodeFixTinyDeleteRecordLimit:
		return DataNodeFixTinyDeleteRecordLimit.Short
	case OpDataNodeDeleteRateLimit:
		return DataNodeMarkDeleteRateLimit.Short
	case OpDataNodeNormalExtentDeleteExpire:
		return DataNodeNormalExtentDeleteExpire.Short
	case OpClientReadVolRateLimit:
		return ClientReadVolRateLimit.Short
	case OpClientWriteVolRateLimit:
		return ClientWriteVolRateLimit.Short
	case OpClientVolOpRateLimit:
		return ClientVolOpRateLimit.Short
	case OpObjectNodeActionRateLimit:
		return ObjectNodeActionRateLimit.Short
	case OpFlashNodeZoneRate:
		return FlashNodeZoneRate.Short
	case OpFlashNodeVolRate:
		return FlashNodeVolRate.Short
	case OpDatanodeRateLimit:
		return DatanodeRateLimit.Short
	case OpMetanodeRateLimit:
		return MetanodeRateLimit.Short
	case OpFlashnodeRateLimit:
		return FlashnodeRateLimit.Short
	case OpApiReqBwRateLimit:
		return ApiReqBwRateLimit.Short
	case OpSetBandwidthLimiter:
		return SetBandwidthLimiter.Short
	case OpRemoteReadConnTimeoutMs:
		return RemoteReadConnTimeoutMs.Short
	case OpReadConnTimeoutMs:
		return ReadConnTimeoutMs.Short
	case OpWriteConnTimeoutMs:
		return WriteConnTimeoutMs.Short
	case OpNetworkFlowRatio:
		return NetworkFlowRatio.Short
	case OpSetVolume:
		return SetVolume.Short
	case OpSetVolumeRelease:
		return SetVolumeRelease.Short
	case OpSetVolRemoteCache:
		return SetVolRemoteCache.Short
	case OpSetVolTrash:
		return SetVolTrash.Short
	case OpSetVolSmart:
		return SetVolSmart.Short
	case OpSetVolFollowerRead:
		return SetVolFollowerRead.Short
	case OpSetVolAuthentication:
		return SetVolAuthentication.Short
	case OpSetVolMeta:
		return SetVolMeta.Short
	case OpDataNodeRepairTaskCount:
		return DataNodeRepairTaskCount.Short
	case OpDataNodeExtentRepairTask:
		return DataNodeExtentRepairTask.Short
	case OpDataNodeDisableBlacklist:
		return DataNodeDisableBlacklist.Short
	case OpDataNodeTrashKeepTime:
		return DataNodeTrashKeepTime.Short
	case OpBatchSetDataNodeSettings:
		return BatchSetDataNodeSettings.Short
	case OpSetVolMinRWPartition:
		return SetVolMinRWPartition.Short
	case OpSetVolumeConnConfig:
		return SetVolumeConnConfig.Short
	case OpVolAddDp:
		return VolAddDp.Short
	case OpVolAddMp:
		return VolAddMp.Short
	case OpVolAddDpRelease:
		return VolAddDpRelease.Short
	case OpVolSetChildFileMaxCount:
		return VolSetChildFileMaxCount.Short
	case OpVolS3ActionRateLimit:
		return VolS3ActionRateLimit.Short
	case OpVolClearTrash:
		return ClearVolumeTrash.Short
	case OpSetVolumePersistMode:
		return SetVolumePersistMode.Short
	case OpTopologyManager:
		return TopologyManager.Short
	case OpBatchSetVolForceROW:
		return BatchSetVolForceROW.Short
	case OpBatchSetVolInodeReuse:
		return BatchSetVolInodeReuse.Short
	case OpBatchSetVolConnConfig:
		return BatchSetVolConnConfig.Short
	case OpBatchSetVolWriteCache:
		return BatchSetVolWriteCache.Short
	case OpBatchSetVolDpSelector:
		return BatchSetVolDpSelector.Short
	case OpBatchSetVolReplicaNum:
		return BatchSetVolReplicaNum.Short
	case OpBatchSetVolMpReplicaNum:
		return BatchSetVolMpReplicaNum.Short
	case OpBatchSetVolFollowerReadCfg:
		return BatchSetVolFollowerReadCfg.Short
	case OpBatchSetVolReqRemoveDup:
		return BatchSetVolReqRemoveDup.Short
	case OpBatchSetJSSVolumeMetaTag:
		return BatchSetJSSVolumeMetaTag.Short
	case OpBatchSetVolPersistMode:
		return BatchSetVolPersistMode.Short
	case OpMigrationConfigList:
		return MigrationConfigList.Short
	case OpFlashNodeReadTimeoutUs:
		return FlashNodeReadTimeoutUs.Short
	case OpFlashNodeDisableStack:
		return FlashNodeDisableStack.Short
	default:
	}
	return fmt.Sprintf("undefined operation:%v", operationCode)
}

var MetaRatelimitOpList = []uint8{
	proto.OpMetaCreateInode,
	proto.OpMetaInodeGet,
	proto.OpMetaInodeGetV2,
	proto.OpMetaBatchInodeGet,
	proto.OpMetaBatchUnlinkInode,
	proto.OpMetaBatchEvictInode,

	proto.OpMetaCreateDentry,
	proto.OpMetaBatchDeleteDentry,
	proto.OpMetaExtentsAdd,
	proto.OpMetaBatchExtentsAdd,
	proto.OpMetaExtentsList,
	proto.OpMetaReadDir,
	proto.OpMetaLookup,
	proto.OpMetaBatchGetXAttr,

	proto.OpListMultiparts,
	proto.OpMetaGetCmpInode,
	proto.OpMetaInodeMergeEks,

	proto.OpMetaGetDeletedInode,
	proto.OpMetaBatchGetDeletedInode,
	proto.OpMetaRecoverDeletedDentry,
	proto.OpMetaBatchRecoverDeletedDentry,
	proto.OpMetaRecoverDeletedInode,
	proto.OpMetaBatchRecoverDeletedInode,
	proto.OpMetaBatchCleanDeletedDentry,
	proto.OpMetaBatchCleanDeletedInode,
}
var ReleaseMetaRatelimitOpList = []uint8{
	proto.OpMetaCreateInode,
	proto.OpMetaUnlinkInode,
	proto.OpMetaCreateDentry,
	proto.OpMetaDeleteDentry,
	proto.OpMetaOpen,
	proto.OpMetaLookup,
	proto.OpMetaReadDir,
	proto.OpMetaInodeGet,
	proto.OpMetaBatchInodeGet,
	proto.OpMetaExtentsAdd,
	proto.OpMetaExtentsDel,
	proto.OpMetaExtentsList,
	proto.OpMetaUpdateDentry,
	proto.OpMetaTruncate,
	proto.OpMetaLinkInode,
	proto.OpMetaEvictInode,
}

var DataRatelimitOpList = []uint8{
	proto.OpCreateExtent,
	proto.OpMarkDelete,
	proto.OpWrite,
	proto.OpStreamRead,
	proto.OpStreamFollowerRead,
	proto.OpRandomWrite,
	proto.OpExtentRepairRead,
	proto.OpTinyExtentRepairRead,
	proto.OpBatchDeleteExtent,
}
var DataRatelimitOpList_ext = []int{
	proto.OpExtentRepairWrite_,
	proto.OpFlushDelete_,
	proto.OpExtentRepairWriteToApplyTempFile_,
	proto.OpExtentRepairWriteByPolicy_,
	proto.OpExtentRepairReadToRollback_,
	proto.OpExtentRepairReadToComputeCrc_,
	proto.OpExtentReadToGetCrc_,
	proto.OpFetchDataPartitionView_,
	proto.OpFixIssueFragments_,
	proto.OpPlaybackTinyDelete_,
	proto.OpStreamFollowerReadSrcFlashNode_,
	proto.OpRecoverTrashExtent_,
}

const (
	OpNormalExtentRepairRead uint8 = 0x10
	OpTinyExtentRepairRead   uint8 = 0x11
)

var ReleaseDataRatelimitOpList = []uint8{
	proto.OpMarkDelete,
	proto.OpWrite,
	proto.OpRead,
	proto.OpStreamRead,
	OpNormalExtentRepairRead,
	OpTinyExtentRepairRead,
}
var ReleaseDataRatelimitOpList_ext = []int{
	proto.OpExtentRepairWrite_,
	proto.OpFlushDelete_,
}

var FlashRatelimitOpList = []uint8{
	proto.OpCacheRead,
	proto.OpCachePrepare,
}

var ClientRatelimitOpList = []uint8{
	proto.OpMetaCreateInode,
	proto.OpMetaUnlinkInode,
	proto.OpMetaBatchUnlinkInode,
	proto.OpMetaEvictInode,
	proto.OpMetaBatchEvictInode,
	proto.OpMetaCreateDentry,
	proto.OpMetaDeleteDentry,
	proto.OpMetaBatchDeleteDentry,
	proto.OpMetaLookup,
	proto.OpMetaInodeGetV2,
	proto.OpMetaBatchInodeGet,
	proto.OpMetaReadDir,
	proto.OpMetaExtentsInsert,
	proto.OpMetaExtentsList,
	proto.OpMetaTruncate,
	proto.OpMetaLinkInode,
	proto.OpMetaSetattr,
	proto.OpMetaDeleteInode,
	proto.OpMetaGetDeletedInode,
	proto.OpMetaBatchGetDeletedInode,
}

var ApiReqBwRateLimitOpList = []uint8{
	APICodeGetCluster,
	APICodeGetVol,
	APICodeGetMetaPartitions,
	APICodeListVols,
	APICodeGetDataPartitions,
}
var ReleaseApiReqBwRateLimitOpList = []uint8{
	APICodeGetCluster,
	APICodeGetVol,
	APICodeGetMetaPartitions,
	APICodeListVols,
}

type RatelimitIndexType int

const (
	IndexTimeout RatelimitIndexType = iota
	IndexTotalCount
	IndexTotalInBytes
	IndexTotalOutBytes
	IndexDiskCount
	IndexDiskInBytes
	IndexDiskOutBytes
	IndexPartitionCount
	IndexPartitionInBytes
	IndexPartitionOutBytes
	IndexConcurrency
	IndexTypeMax
)

func GetRatelimitIndexMsg(indexType RatelimitIndexType) string {
	var (
		m string
	)
	switch indexType {
	case IndexTimeout:
		// 这个时间单位。。。。。谁会设置纳秒。。。
		m = "timeout(ns)"
	case IndexTotalCount:
		m = "count"
	case IndexTotalInBytes:
		m = "inBytes"
	case IndexTotalOutBytes:
		m = "outBytes"
	case IndexDiskCount:
		m = "countPerDisk"
	case IndexDiskInBytes:
		m = "inBytesPerDisk"
	case IndexDiskOutBytes:
		m = "outBytesPerDisk"
	case IndexPartitionCount:
		m = "countPerPartition"
	case IndexPartitionInBytes:
		m = "inBytesPerPartition"
	case IndexPartitionOutBytes:
		m = "outBytesPerPartition"
	case IndexConcurrency:
		m = "concurrency"
	}
	return m
}

func GetOpMsg(op uint8) string {
	switch op {
	case OpNormalExtentRepairRead:
		return "OpNormalExtentRepairRead"
	case OpTinyExtentRepairRead:
		return "OpTinyExtentRepairRead"
	default:
		return proto.GetOpMsg(op)
	}
}

func GetApiOpMsg(op uint8) string {
	switch op {
	case APICodeGetCluster:
		return "APICodeGetCluster"
	case APICodeGetVol:
		return "APICodeGetVol"
	case APICodeGetMetaPartitions:
		return "APICodeGetMetaPartitions"
	case APICodeListVols:
		return "APICodeListVols"
	case APICodeGetDataPartitions:
		return "APICodeGetDataPartitions"
	default:
		return ""
	}
}
