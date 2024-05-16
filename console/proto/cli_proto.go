package proto

import (
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"math"
	"reflect"
	"strconv"
	"strings"
)

const (
	BasicTypeUintPrefix  = "uint"
	BasicTypeIntPrefix   = "int"
	BasicTypeFloatPrefix = "float"
	BasicTypeBool        = "bool"
	BasicTypeString      = "string"

	EmptyZoneVolFlag = "_"
)

type CliModule struct {
	ModuleType int
	Module     string
}

var CliModuleList = []*CliModule{
	ClusterModule, MetaNodeModule, DataNodeModule, EcModule,
	RateLimitModule, NetworkModule, VolumeModule, BatchModule, KeyValueModule,
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
var FlashNodeModule = &CliModule{
	ModuleType: FlashNodeModuleType,
	Module:     "flashnode配置",
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
	}
	return ""
}

type CliOperation struct {
	OpType         int // 在module下唯一即可
	Short          string
	Desc           string
	IsList         int // setConfig or setConfigList
	ReleaseSupport bool
	SparkSupport   bool
}

func NewCliOperation(opcode int, displayMsg string, isList bool, releaseSupport, sparkSupport bool) *CliOperation {
	cliOp := &CliOperation{
		OpType:         opcode,
		Short:          displayMsg,
		ReleaseSupport: releaseSupport,
		SparkSupport:   sparkSupport,
	}
	if isList {
		cliOp.IsList = 1
	}
	return cliOp
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
	// 批量
	OpBatchSetVolForceROW
	OpBatchSetVolWriteCache
	OpBatchSetVolDpSelector
	OpBatchSetVolConnConfig
	OpBatchSetVolInodeReuse
	OpBatchSetVolReplicaNum
	OpBatchSetVolFollowerReadCfg
	// key-value 查数据库表
)

const (
	isList         = true
	releaseSupport = true
	sparkSupport   = true
)

var (
	BatchSetVolFollowerReadCfg       = NewCliOperation(OpBatchSetVolFollowerReadCfg, "vol属性-从读配置", isList, !releaseSupport, sparkSupport)
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
	ClientReadVolRateLimit           = NewCliOperation(OpClientReadVolRateLimit, "客户端 vol读请求限速", isList, releaseSupport, sparkSupport)
	ClientWriteVolRateLimit          = NewCliOperation(OpClientWriteVolRateLimit, "客户端 vol写请求限速", isList, releaseSupport, sparkSupport)
	ClientVolOpRateLimit             = NewCliOperation(OpClientVolOpRateLimit, "客户端 vol请求op限速(除读写外)", isList, !releaseSupport, sparkSupport)
	ObjectNodeActionRateLimit        = NewCliOperation(OpObjectNodeActionRateLimit, "S3 请求action限速", isList, !releaseSupport, sparkSupport)
	FlashNodeZoneRate                = NewCliOperation(OpFlashNodeZoneRate, "flashnode 按zone限速", isList, !releaseSupport, sparkSupport)
	FlashNodeVolRate                 = NewCliOperation(OpFlashNodeVolRate, "flashnode 按vol限速", isList, !releaseSupport, sparkSupport)
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
	},
	//FlashNodeModuleType: {},
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
		BatchSetVolFollowerReadCfg,
	},
}

var CliOperations = map[int][]CliValueMetric{
	// valueName是key 使用proto中定义或者master/const中定义的 保持一致
	OpSetClientPkgAddr: {
		CliValueMetric{"addr", "Set url for download client package used for client hot-upgrade.", "", "", manualSetValueForm(InputBox)},
	},
	OpSetRemoteCacheBoostEnableStatus: {
		CliValueMetric{proto.RemoteCacheBoostEnableKey, "Set cluster remoteCacheBoostStatus to ON/OFF", "", "", manualSetValueForm(Slider)},
	},
	OpSetEcConfig: {
		CliValueMetric{"ecScrubEnable", "Enable ec scrub", "", "", manualSetValueForm(Slider)},
		CliValueMetric{"ecScrubPeriod", "Specify ec scrub period unit: min", "", "", manualSetValueForm(InputBox)},
		CliValueMetric{"ecDiskConcurrentExtents", "Specify every disk concurrent scrub extents", "", "", manualSetValueForm(InputBox)},
		CliValueMetric{"maxCodecConcurrent", "Specify every codecNode concurrent migrate dp", "", "", manualSetValueForm(InputBox)},
	},
	// 没有回显的值 直接设置
	OpSetNodeState: {
		CliValueMetric{"addrList", "使用“;”分隔节点ip", "string", "", manualSetValueForm(InputBox)},
		CliValueMetric{"nodeType", "Should be one of dataNode/metaNode/all", "string", "", manualSetValueForm(InputBox)},
		CliValueMetric{"zoneName", "", "string", "", manualSetValueForm(InputBox)},
		CliValueMetric{"state", "offline state, true or false", "string", "", manualSetValueForm(InputBox)},
	},
	OpSetNodeSetCapacity: {
		CliValueMetric{proto.NodeSetCapacityKey, "", "", "", manualSetValueForm(InputBox)},
	},
	OpAutoMergeNodeSet: {
		CliValueMetric{"enable", "", "", "", manualSetValueForm(Slider)},
	},
	OpSetClusterFreeze: {
		CliValueMetric{"enable", "", "", "", manualSetValueForm(Slider)},
	},
	OpMonitorSummarySecond: {
		CliValueMetric{"monitorSummarySec", "summary seconds for monitor, sec >= 5", "", "", manualSetValueForm(InputBox)},
		CliValueMetric{"monitorReportSec", "report seconds for monitor, sec >= 5", "", "", manualSetValueForm(InputBox)},
	},
	OpLogMaxMB: {
		CliValueMetric{proto.LogMaxMB, "log max MB", "", "", manualSetValueForm(InputBox)},
	},
	OpTopologyManager: {
		CliValueMetric{proto.TopologyFetchIntervalMinKey, "topologyManager force fetch interval, unit: second", "", "", manualSetValueForm(InputBox)},
		CliValueMetric{proto.TopologyForceFetchIntervalSecKey, "topology fetch interval, unit: min", "", "", manualSetValueForm(InputBox)},
	},
	OpSetThreshold: {
		CliValueMetric{"threshold", "Set memory threshold of metanodes, 0.00~1.00", "", "", manualSetValueForm(InputBox)},
	},
	OpSetRocksDBDiskThreshold: {
		CliValueMetric{"threshold", "Set RocksDB Disk threshold of metanodes, 0.00~1.00", "", "", manualSetValueForm(InputBox)},
	},
	OpSetMemModeRocksDBDiskThreshold: {
		CliValueMetric{"threshold", "Set RocksDB Disk threshold of mem mode metanodes, 0.00~1.00", "", "", manualSetValueForm(InputBox)},
	},
	OpMetaNodeDumpWaterLevel: {
		CliValueMetric{"metaNodeDumpWaterLevel", "metanode dump snapshot water level", "", "", manualSetValueForm(InputBox)},
	},
	OpMetaNodeDumpSnapCountByZone: {
		CliValueMetric{"zoneName", " acts as default", "", "", manualSetValueForm(DropDown)},
		CliValueMetric{proto.MetaNodeDumpSnapCountKey, "metanode dump snapshot count", "", "", manualSetValueForm(InputBox)},
	},
	OpMetaRocksDBConf: {
		CliValueMetric{proto.RocksDBDiskReservedSpaceKey, "设置rocksDB磁盘预留空间(MB)", "", "", manualSetValueForm(InputBox)},
		CliValueMetric{proto.MetaRockDBWalFileMaxMB, "设置rocksDB配置: wal_size_limit_mb, unit:MB, default:10MB", "", "", manualSetValueForm(InputBox)},
		CliValueMetric{proto.MetaRocksDBWalMemMaxMB, "设置rocksDB配置: max_total_wal_size, unit:MB, default:3MB", "", "", manualSetValueForm(InputBox)},
		CliValueMetric{proto.MetaRocksDBLogMaxMB, "设置rocksDB配置: max_log_file_size, unit:MB, default:1MB", "", "", manualSetValueForm(InputBox)},
		CliValueMetric{proto.MetaRocksLogReservedDay, "设置rocksDB配置: log_file_time_to_roll, unit:Day, default:3day", "", "", manualSetValueForm(InputBox)},
		CliValueMetric{proto.MetaRocksLogReservedCnt, "设置rocksDB配置: keep_log_file_num, default:3", "", "", manualSetValueForm(InputBox)},
		CliValueMetric{proto.MetaRocksDisableFlushWalKey, "设置rocksDB配置: flush wal flag, 0:enable, 1:disable, default:0", "", "", manualSetValueForm(Slider)},
		CliValueMetric{proto.MetaRocksWalFlushIntervalKey, "设置rocksDB配置: flush wal interval, unit:min, default:30min", "", "", manualSetValueForm(InputBox)},
		CliValueMetric{proto.MetaRocksWalTTLKey, "设置rocksDB配置: wal_ttl_seconds, unit:second, default:60s", "", "", manualSetValueForm(InputBox)},
	},
	OpSetMetaBitMapAllocator: {
		CliValueMetric{proto.AllocatorMaxUsedFactorKey, "float64, bit map allocator max used factor for available", "", "", manualSetValueForm(InputBox)},
		CliValueMetric{proto.AllocatorMinFreeFactorKey, "float64, bit map allocator min free factor for available", "", "", manualSetValueForm(InputBox)},
	},
	OpSetTrashCleanConfig: {
		CliValueMetric{proto.MetaTrashCleanIntervalKey, "clean del inode interval, unit:min", "", "", manualSetValueForm(InputBox)},
		CliValueMetric{proto.TrashCleanDurationKey, "trash clean max duration for each time, unit:min", "", "", manualSetValueForm(InputBox)},
		CliValueMetric{proto.TrashItemCleanMaxCountKey, "trash clean max count for each time", "", "", manualSetValueForm(InputBox)},
	},
	OpSetMetaRaftConfig: {
		CliValueMetric{proto.MetaRaftLogSizeKey, "meta node raft log size, unit:MB", "", "", manualSetValueForm(InputBox)},
		CliValueMetric{proto.MetaRaftLogCapKey, "meta node raft log cap, unit:MB", "", "", manualSetValueForm(InputBox)},
		CliValueMetric{proto.MetaSyncWalEnableStateKey, "metaSyncWALFlag", "", "", manualSetValueForm(Slider)},
	},
	OpMetaClientRequestConf: {
		CliValueMetric{proto.ClientReqRemoveDupFlagKey, "client req remove dup flag", "", "", manualSetValueForm(Slider)},
		CliValueMetric{proto.ClientReqRecordReservedMinKey, "client req records reserved min", "", "", manualSetValueForm(InputBox)},
		CliValueMetric{proto.ClientReqRecordReservedCntKey, "client req records reserved count", "", "", manualSetValueForm(InputBox)},
	},
	OpMetaNodeDeleteBatchCount: {
		CliValueMetric{"batchCount", "MetaNodeDeleteBatchCount", "", "", manualSetValueForm(InputBox)},
	},
	OpMetaNodeDeleteWorkerSleepMs: {
		CliValueMetric{"deleteWorkerSleepMs", "delete worker sleep time, unit: ms", "", "", manualSetValueForm(InputBox)},
	},
	OpMetaNodeReadDirLimitNum: {
		CliValueMetric{"metaNodeReadDirLimit", "readdir limit count", "", "", manualSetValueForm(InputBox)},
	},
	OpDeleteEKRecordFileMaxMB: {
		CliValueMetric{proto.MetaDelEKRecordFileMaxMB, "meta node delete ek record file max MB", "", "", manualSetValueForm(InputBox)},
	},
	OpDataNodeFlushFDInterval: {
		CliValueMetric{"dataNodeFlushFDInterval", "time interval for flushing WAL and open FDs on DataNode, unit is second", "", "", manualSetValueForm(InputBox)},
	},
	OpDataNodeFlushFDParallelismOnDisk: {
		CliValueMetric{"dataNodeFlushFDParallelismOnDisk", "parallelism for flushing WAL and open FDs on DataNode per disk", "", "", manualSetValueForm(InputBox)},
	},
	OpDataSyncWALOnUnstableEnableState: {
		CliValueMetric{proto.DataSyncWalEnableStateKey, "dataSyncWALFlag", "", "", manualSetValueForm(Slider)},
	},
	OpDataNodeNormalExtentDeleteExpire: {
		CliValueMetric{"normalExtentDeleteExpire", "datanode normal extent delete record expire time(second, >=600)", "", "", manualSetValueForm(InputBox)},
	},
	OpDataNodeFixTinyDeleteRecordLimit: {
		CliValueMetric{"fixTinyDeleteRecordKey", "data node fix tiny delete record limit", "", "", manualSetValueForm(InputBox)},
	},
	OpDataPartitionConsistencyMode: {
		// 增加module参数, 否则接口会认为只有这一个参数
		CliValueMetric{"dataPartitionConsistencyMode", "cluster consistency mode for data partitions, 0-Standard 1-Strict", "", "", manualSetValueForm(InputBox)},
	},
	OpDataNodeDiskReservedRatio: {
		// 增加module参数, 否则接口会认为只有这一个参数
		CliValueMetric{proto.DataNodeDiskReservedRatioKey, "data node disk reserved ratio, greater than or equal 0", "", "", manualSetValueForm(InputBox)},
	},
	OpDataNodeRepairTaskCount: {
		CliValueMetric{"dataNodeRepairTaskCount", "", "", "", manualSetValueForm(InputBox)},
	},
	// modul(请求接口的时候别忘了) zone vol opcode rateLimitIndex rateLimit
	OpDatanodeRateLimit: {
		CliValueMetric{"zoneName", " acts as default", "", "", manualSetValueForm(DropDown)},
		CliValueMetric{"volume", " acts as default", "", "", manualSetValueForm(InputBox)},
		CliValueMetric{"opcode", "", "", "", manualSetValueForm(DropDown)},
		CliValueMetric{proto.RateLimitIndexKey, "rate limit index, 0: timeout(ns); 1-3: count, in bytes, out bytes; 4-6: per disk; 7-9: per partition", "", "", manualSetValueForm(DropDown)},
		CliValueMetric{proto.RateLimitKey, "", "", "", manualSetValueForm(InputBox)},
	},
	OpMetanodeRateLimit: {
		CliValueMetric{"zoneName", " acts as default", "", "", manualSetValueForm(DropDown)},
		CliValueMetric{"volume", " acts as default", "", "", manualSetValueForm(InputBox)},
		CliValueMetric{"opcode", "", "", "", manualSetValueForm(DropDown)},
		CliValueMetric{proto.RateLimitIndexKey, "rate limit index, 0: timeout(ns); 1-3: count, in bytes, out bytes; 4-6: per disk; 7-9: per partition", "", "", manualSetValueForm(DropDown)},
		CliValueMetric{proto.RateLimitKey, "", "", "", manualSetValueForm(InputBox)},
	},
	OpFlashnodeRateLimit: {
		CliValueMetric{"zoneName", " acts as default", "", "", manualSetValueForm(DropDown)},
		CliValueMetric{"volume", " acts as default", "", "", manualSetValueForm(InputBox)},
		CliValueMetric{"opcode", "", "", "", manualSetValueForm(DropDown)},
		CliValueMetric{proto.RateLimitIndexKey, "rate limit index, 0: timeout(ns); 1-3: count, in bytes, out bytes; 4-6: per disk; 7-9: per partition", "", "", manualSetValueForm(DropDown)},
		CliValueMetric{proto.RateLimitKey, "", "", "", manualSetValueForm(InputBox)},
	},
	OpApiReqBwRateLimit: {
		CliValueMetric{"opcode", "", "", "", manualSetValueForm(DropDown)},
		CliValueMetric{"apiReqBwRate", "rateLimit value", "", "", manualSetValueForm(InputBox)},
	},
	OpSetBandwidthLimiter: {
		CliValueMetric{"bw", "bandwidth ratelimit value, unit: byte", "", "", manualSetValueForm(InputBox)},
	},
	OpDataNodeDeleteRateLimit: {
		CliValueMetric{proto.DataNodeMarkDeleteRateKey, "data node mark delete request rate limit", "", "", manualSetValueForm(InputBox)},
	},
	OpClientReadVolRateLimit: {
		CliValueMetric{"volume", "", "", "", manualSetValueForm(MultiSelect)},
		CliValueMetric{"clientReadVolRate", "", "", "", manualSetValueForm(InputBox)},
	},
	OpClientWriteVolRateLimit: {
		CliValueMetric{"volume", "", "", "", manualSetValueForm(MultiSelect)},
		CliValueMetric{"clientWriteVolRate", "", "", "", manualSetValueForm(InputBox)},
	},
	OpClientVolOpRateLimit: {
		// 没在限速中 在vol配置中
		CliValueMetric{"opcode", "", "", "", manualSetValueForm(DropDown)},
		CliValueMetric{"clientVolOpRate", "", "", "", manualSetValueForm(InputBox)},
	},
	OpObjectNodeActionRateLimit: {
		CliValueMetric{"volume", "", "", "", manualSetValueForm(InputBox)},
		CliValueMetric{"action", "object node action", "", "", manualSetValueForm(DropDown)},
		CliValueMetric{"objectVolActionRate", "", "", "", manualSetValueForm(InputBox)},
	},
	OpFlashNodeZoneRate: {
		CliValueMetric{"zoneName", "if zone is empty, set rate limit for all zones", "", "", manualSetValueForm(DropDown)},
		CliValueMetric{proto.FlashNodeRateKey, "flash node cache read request rate limit", "", "", manualSetValueForm(InputBox)},
	},
	OpFlashNodeVolRate: {
		CliValueMetric{"zoneName", "if zone is empty, set rate limit for all zones", "", "", manualSetValueForm(DropDown)},
		CliValueMetric{"volume", "", "", "", manualSetValueForm(InputBox)},
		CliValueMetric{proto.FlashNodeVolRateKey, "flash node cache read request rate limit for a volume", "", "", manualSetValueForm(InputBox)},
	},
	OpRemoteReadConnTimeoutMs: {
		CliValueMetric{proto.RemoteReadConnTimeoutKey, "缓存客户端读超时时间(ms)", "", "", manualSetValueForm(InputBox)},
	},
	OpReadConnTimeoutMs: {
		CliValueMetric{"zoneName", "if zone is empty, set readTimeout for all zones", "", "", manualSetValueForm(DropDown)},
		CliValueMetric{proto.ReadConnTimeoutMsKey, "客户端读超时时间(ms)", "", "", manualSetValueForm(InputBox)},
	},
	OpWriteConnTimeoutMs: {
		CliValueMetric{"zoneName", "if zone is empty, set writeTimeout for all zones", "", "", manualSetValueForm(DropDown)},
		CliValueMetric{proto.WriteConnTimeoutMsKey, "客户端写超时时间(ms)", "", "", manualSetValueForm(InputBox)},
	},
	OpNetworkFlowRatio: {
		CliValueMetric{"module", "role of cluster, like master", "", "", manualSetValueForm(DropDown)},
		CliValueMetric{proto.NetworkFlowRatioKey, "network flow ratio percent: [0, 100]", "", "", manualSetValueForm(InputBox)},
	},
	OpSetVolume: {
		CliValueMetric{"volWriteMutex", "", "bool", "", manualSetValueForm(Slider)},
		CliValueMetric{"forceROW", "", "bool", "", manualSetValueForm(Slider)},
		CliValueMetric{"writeCache", "", "bool", "", manualSetValueForm(Slider)},
		CliValueMetric{"autoRepair", "", "bool", "", manualSetValueForm(Slider)},
		CliValueMetric{"zoneName", "", "string", "", manualSetValueForm(InputBox)},
		CliValueMetric{"capacity", "", "", "", manualSetValueForm(InputBox)},
		CliValueMetric{"replicaNum", "", "", "", manualSetValueForm(InputBox)},
		CliValueMetric{"mpReplicaNum", "", "", "", manualSetValueForm(InputBox)},
		CliValueMetric{"crossRegion", "Set cross region high available type(0 for default, 1 for quorum)", "", "", manualSetValueForm(InputBox)},
		CliValueMetric{"ekExpireSec", "int64\nSpecify the expiration second of the extent cache (-1 means never expires)", "", "", manualSetValueForm(InputBox)},
		CliValueMetric{"compactTag", "string\nSpecify volume compact", "", "", manualSetValueForm(InputBox)},
		CliValueMetric{"umpCollectWay", "Set ump collect way(0-unknown 1-file 2-jmtp client)", "", "", manualSetValueForm(InputBox)},
		CliValueMetric{"dpSelectorName", "default/kfaster", "string", "", manualSetValueForm(InputBox)},
		CliValueMetric{"dpSelectorParm", "请输入(0, 100)之间的数", "string", "", manualSetValueForm(InputBox)},
	},
	OpSetVolumeConnConfig: {
		CliValueMetric{"readConnTimeout", "int64\nSet client read connection timeout, unit: ms", "", "", manualSetValueForm(InputBox)},
		CliValueMetric{"writeConnTimeout", "int64\nSet client write connection timeout, unit: ms", "", "", manualSetValueForm(InputBox)},
	},
	OpSetVolAuthentication: {
		CliValueMetric{"authenticate", "Enable authenticate", "bool", "", manualSetValueForm(Slider)},
		CliValueMetric{"enableToken", "ReadOnly/ReadWrite token validation for fuse client", "bool", "", manualSetValueForm(Slider)},
		CliValueMetric{"bucketPolicy", "Set bucket access policy for S3(0 for private 1 for public-read)", "", "", manualSetValueForm(InputBox)},
	},
	OpSetVolMeta: {
		CliValueMetric{proto.EnableBitMapAllocatorKey, "", "bool", "", manualSetValueForm(Slider)},
		CliValueMetric{proto.BitMapSnapFrozenHour, "int64,延迟分配时间，单位: hour", "", "", manualSetValueForm(InputBox)},
		CliValueMetric{proto.VolRemoveDupFlagKey, "", "bool", "", manualSetValueForm(Slider)},
		CliValueMetric{"storeMode", "Specify volume default store mode [1:Mem, 2:Rocks]", "", "", manualSetValueForm(InputBox)},
		CliValueMetric{"metaLayout", "Specify volume meta layout num1,num2 [num1:rocksdb mp percent, num2:rocksdb replicas percent]", "", "", manualSetValueForm(InputBox)},
		CliValueMetric{"batchDelInodeCnt", "", "", "", manualSetValueForm(InputBox)},
		CliValueMetric{"delInodeInterval", "Specify del inode interval, unit:ms", "", "", manualSetValueForm(InputBox)},
		CliValueMetric{proto.MetaNodeTruncateEKCountKey, "int64\ntruncate EK count every time when del inode", "", "", manualSetValueForm(InputBox)},
	},
	OpSetVolRemoteCache: {
		CliValueMetric{"remoteCacheBoostEnable", "", "bool", "", manualSetValueForm(Slider)},
		CliValueMetric{"remoteCacheAutoPrepare", "", "bool", "", manualSetValueForm(Slider)},
		CliValueMetric{"remoteCacheBoostPath", "设置加速路径，多个路径','分隔", "string", "", manualSetValueForm(InputBox)},
		CliValueMetric{"remoteCacheTTL", "", "string", "", manualSetValueForm(InputBox)},
	},
	OpSetVolSmart: {
		CliValueMetric{"smart", "", "bool", "", manualSetValueForm(Slider)},
		CliValueMetric{"smartRules", "Specify volume smart rules separate by ','", "string", "", manualSetValueForm(InputBox)},
	},
	OpSetVolFollowerRead: {
		CliValueMetric{"followerRead", "从节点读", "bool", "", manualSetValueForm(Slider)},
		CliValueMetric{"hostDelayInterval", "Specify host delay update interval [unit: s]", "", "", manualSetValueForm(InputBox)},
		CliValueMetric{"follReadHostWeight", "assign weight for the lowest delay host when enable FollowerRead,(0,100)", "", "", manualSetValueForm(InputBox)},
		CliValueMetric{"nearRead", "", "bool", "", manualSetValueForm(Slider)},
	},
	OpSetVolTrash: {
		CliValueMetric{"trashRemainingDays", "", "", "", manualSetValueForm(InputBox)},
		CliValueMetric{proto.TrashCleanDurationKey, "Trash clean duration, unit:min", "", "", manualSetValueForm(InputBox)},
		CliValueMetric{proto.MetaTrashCleanIntervalKey, "specify trash clean interval, unit:min", "", "", manualSetValueForm(InputBox)},
		CliValueMetric{proto.TrashItemCleanMaxCountKey, "Trash clean max count", "", "", manualSetValueForm(InputBox)},
	},
	OpVolSetChildFileMaxCount: {
		CliValueMetric{proto.ChildFileMaxCountKey, "值类型：uint32", "", "", manualSetValueForm(InputBox)},
	},
	OpSetVolMinRWPartition: {
		CliValueMetric{"minWritableDp", "", "", "", manualSetValueForm(InputBox)},
		CliValueMetric{"minWritableMp", "", "", "", manualSetValueForm(InputBox)},
	},
	OpVolAddDp: {
		//CliValueMetric{"dpCount", "显示值，修改不生效", "", "", manualSetValueForm(InputBox)},
		//CliValueMetric{"rwDpCount", "显示值，修改不生效", "", "", manualSetValueForm(InputBox)},
		CliValueMetric{"count", "需要增加的dp数量", "", "", manualSetValueForm(InputBox)},
	},
	OpVolAddDpRelease: {
		//CliValueMetric{"dpCount", "显示值，修改不生效", "", "", manualSetValueForm(InputBox)},
		//CliValueMetric{"rwDpCount", "显示值，修改不生效", "", "", manualSetValueForm(InputBox)},
		CliValueMetric{"count", "需要增加的dp数量", "", "", manualSetValueForm(InputBox)},
		//CliValueMetric{"type", "partition类型：tiny/extent", "", "", manualSetValueForm(InputBox)},
	},
	OpVolAddMp: {
		//CliValueMetric{"mpCount", "显示值，修改不生效", "", "", manualSetValueForm(InputBox)},
		//CliValueMetric{"rwMpCount", "显示值，修改不生效", "", "", manualSetValueForm(InputBox)},
		CliValueMetric{"start", "inodeStart", "", "", manualSetValueForm(InputBox)},
	},
	OpSetVolumeRelease: {
		CliValueMetric{"capacity", "", "", "", manualSetValueForm(InputBox)},
		CliValueMetric{"minWritableDp", "", "", "", manualSetValueForm(InputBox)},
		CliValueMetric{"minWritableMp", "", "", "", manualSetValueForm(InputBox)},
		CliValueMetric{"batchDelInodeCnt", "", "", "", manualSetValueForm(InputBox)},
		CliValueMetric{"readConnTimeout", "单位：ms，请设置>=1000的值", "", "", manualSetValueForm(InputBox)},
		CliValueMetric{"writeConnTimeout", "单位：ms", "", "", manualSetValueForm(InputBox)},
		CliValueMetric{"enableToken", "", "bool", "", manualSetValueForm(Slider)},
		CliValueMetric{"crossPod", "", "bool", "", manualSetValueForm(Slider)},
		CliValueMetric{"autoRepair", "", "bool", "", manualSetValueForm(Slider)},
		CliValueMetric{proto.EnableBitMapAllocatorKey, "", "bool", "", manualSetValueForm(Slider)},
		CliValueMetric{proto.BitMapSnapFrozenHour, "int64,延迟分配时间，单位: hour", "", "", manualSetValueForm(InputBox)},
	},
	OpVolS3ActionRateLimit: {
		CliValueMetric{"action", "object node action", "", "", manualSetValueForm(DropDown)},
		CliValueMetric{"objectVolActionRate", "", "", "", manualSetValueForm(InputBox)},
	},
	OpVolClearTrash: {
		CliValueMetric{"doCleanTrash", "是否清理回收站", "", "", manualSetValueForm(Slider)},
	},
	OpBatchSetVolForceROW: {
		CliValueMetric{"volume", "", "", "", manualSetValueForm(MultiSelect)},
		CliValueMetric{"forceROW", "", "", "", manualSetValueForm(Slider)},
	},
	OpBatchSetVolWriteCache: {
		CliValueMetric{"volume", "", "", "", manualSetValueForm(MultiSelect)},
		CliValueMetric{"writeCache", "", "", "", manualSetValueForm(Slider)},
	},
	OpBatchSetVolInodeReuse: {
		CliValueMetric{"volume", "", "", "", manualSetValueForm(MultiSelect)},
		CliValueMetric{proto.EnableBitMapAllocatorKey, "", "bool", "", manualSetValueForm(Slider)},
		// todo: 延迟分配时间
	},
	OpBatchSetVolConnConfig: {
		CliValueMetric{"volume", "", "", "", manualSetValueForm(MultiSelect)},
		CliValueMetric{"readConnTimeout", "int64\nSet client read connection timeout, unit: ms", "", "", manualSetValueForm(InputBox)},
		CliValueMetric{"writeConnTimeout", "int64\nSet client write connection timeout, unit: ms", "", "", manualSetValueForm(InputBox)},
	},
	OpBatchSetVolDpSelector: {
		CliValueMetric{"volume", "", "string", "", manualSetValueForm(MultiSelect)},
		CliValueMetric{"dpSelectorName", "default/kfaster", "string", "", manualSetValueForm(InputBox)},
		CliValueMetric{"dpSelectorParm", "请输入(0, 100)之间的数", "", "", manualSetValueForm(InputBox)},
	},
	OpBatchSetVolReplicaNum: {
		CliValueMetric{"volume", "", "", "", manualSetValueForm(MultiSelect)},
		CliValueMetric{"replicaNum", "请输入副本数", "", "", manualSetValueForm(InputBox)},
	},
	OpBatchSetVolFollowerReadCfg: {
		CliValueMetric{"volume", "", "", "", manualSetValueForm(MultiSelect)},
		CliValueMetric{"hostDelayInterval", "统计host时延的周期[unit: s](>0表示该功能启用，0关闭该功能)", "", "", manualSetValueForm(InputBox)},
		CliValueMetric{"follReadHostWeight", "延迟最低的host分流权重(0,100)", "", "", manualSetValueForm(InputBox)},
	},
	//OpSetRateLimitKeyValue: {
	//	CliValueMetric{"key", "master接口支持的key", "string", "", manualSetValueForm(InputBox)},
	//	CliValueMetric{"value", "", "string", "", manualSetValueForm(InputBox)},
	//},
	//OpUpdateVolumeKeyValue: {
	//	CliValueMetric{"key", "master接口支持的key", "string", "", manualSetValueForm(InputBox)},
	//	CliValueMetric{"value", "", "string", "", manualSetValueForm(InputBox)},
	//},
	//OpCreateVolumeKeyValue: {
	//	CliValueMetric{"key", "master接口支持的key", "string", "", manualSetValueForm(InputBox)},
	//	CliValueMetric{"value", "", "string", "", manualSetValueForm(InputBox)},
	//},
	//OpSetDiskUsageKeyValue: {
	//	CliValueMetric{"key", "master接口支持的key", "string", "", manualSetValueForm(InputBox)},
	//	CliValueMetric{"value", "", "string", "", manualSetValueForm(InputBox)},
	//},
}

func GetKeyValueBasicMetric() []*CliValueMetric {
	baseMetric := make([]*CliValueMetric, 0)
	keyMetric := &CliValueMetric{"key", "master接口支持的key，请依照提示类型输入value", "string", "", manualSetValueForm(DropDown)}
	valueMetric := &CliValueMetric{"value", "", "string", "", manualSetValueForm(InputBox)}
	baseMetric = append(baseMetric, keyMetric, valueMetric)
	return baseMetric
}

func GetOpShortMsg(operationCode int) string {
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
	case OpBatchSetVolFollowerReadCfg:
		return BatchSetVolFollowerReadCfg.Short
	default:
	}
	return fmt.Sprintf("undefined operation:%v", operationCode)
}

func GetCliOperationBaseMetrics(operation int) []CliValueMetric {
	return CliOperations[operation]
}

type CliValueMetric struct {
	ValueName string `json:"name"`
	ValueDesc string `json:"-"`
	ValueType string `json:"type"`
	Value     string `json:"value"`
	ValueForm string `json:"-"`
}

func (metric *CliValueMetric) String() string {
	if metric == nil {
		return ""
	}
	return fmt.Sprintf("%s=%s", metric.ValueName, metric.Value)
}

func (metric *CliValueMetric) SetValueName(name string) {
	metric.ValueName = name
}

func (metric *CliValueMetric) SetValueDesc(desc string) {
	metric.ValueDesc = desc
}

func (metric *CliValueMetric) SetValueType(vtype string) {
	metric.ValueType = vtype
}

func (metric *CliValueMetric) SetValue(v string) {
	metric.Value = v
}

type valueForm int

const (
	Slider valueForm = iota
	InputBox
	DropDown
	MultiSelect
)

func manualSetValueForm(form valueForm) string {
	return strconv.Itoa(int(form))
}

func FormatOperationNilData(operation int, paramTypes ...string) []*CliValueMetric {
	metrics := CliOperations[operation]
	if len(paramTypes) != len(metrics) {
		log.LogErrorf("FormatOperationNilData failed: op[%v] expect %v args but %v", GetOpShortMsg(operation), len(metrics), len(paramTypes))
		return nil
	}
	result := make([]*CliValueMetric, 0, len(metrics))
	for index, paramType := range paramTypes {
		metric := metrics[index]
		metric.SetValueType(paramType)

		result = append(result, &metric)
	}
	log.LogDebugf("FormatOperationNilData: %v:%v metrics:%v", operation, GetOpShortMsg(operation), result)
	return result
}

func FormatArgsToValueMetrics(operation int, args ...interface{}) []*CliValueMetric {
	// args长度及顺序和op中metrics必须一一对应
	metrics := CliOperations[operation]
	if len(args) != len(metrics) {
		log.LogErrorf("FormatArgsToValueMetrics failed: op[%v] expect %v args but %v", GetOpShortMsg(operation), len(metrics), len(args))
		return nil
	}
	results := make([]*CliValueMetric, 0, len(args))
	for index, arg := range args {
		metric := metrics[index]

		value := reflect.ValueOf(arg)
		typeof := value.Type().String()
		metric.SetValueType(typeof)

		if strings.Contains(typeof, BasicTypeUintPrefix) {
			metric.SetValue(strconv.FormatUint(value.Uint(), 10))
		} else if strings.Contains(typeof, BasicTypeIntPrefix) {
			metric.SetValue(strconv.FormatInt(value.Int(), 10))
		}
		if strings.Contains(typeof, BasicTypeFloatPrefix) {
			metric.SetValue(strconv.FormatFloat(value.Float(), 'f', 2, 64))
		}
		if strings.Contains(typeof, BasicTypeBool) {
			metric.SetValue(strconv.FormatBool(value.Bool()))
		}
		if strings.Contains(typeof, BasicTypeString) {
			metric.SetValue(value.String())
		}
		results = append(results, &metric)
	}
	if log.IsDebugEnabled() {
		log.LogDebugf("FormatArgsToValueMetrics: %v:%v, metrics:%v", operation, GetOpShortMsg(operation), results)
	}
	return results
}

func ParseValueMetricsToArgs(operation int, metrics []*CliValueMetric) (map[string]interface{}, error) {
	if len(metrics) == 0 {
		return nil, fmt.Errorf("empty metrics, operation: %v:%v", operation, GetOpShortMsg(operation))
	}
	expectArgsNum := len(CliOperations[operation])
	if expectArgsNum == 0 {
		return nil, fmt.Errorf("undefined operation code: %v:%v", operation, GetOpShortMsg(operation))
	}
	if len(metrics) != expectArgsNum {
		return nil, fmt.Errorf("args count is inconsistent, except:actural= %v:%v, operation: %v:%v",
			expectArgsNum, len(metrics), operation, GetOpShortMsg(operation))
	}
	args := make(map[string]interface{})
	for _, metric := range metrics {
		value := metric.Value
		vType := metric.ValueType

		var (
			arg interface{}
			err error
		)
		if strings.Contains(vType, BasicTypeUintPrefix) {
			bitsLen := strings.TrimPrefix(vType, BasicTypeUintPrefix)
			bits, _ := strconv.Atoi(bitsLen)
			arg, err = strconv.ParseUint(value, 10, bits)
		} else if strings.Contains(vType, BasicTypeIntPrefix) {
			bitsLen := strings.TrimPrefix(vType, BasicTypeIntPrefix)
			bits, _ := strconv.Atoi(bitsLen)
			arg, err = strconv.ParseInt(value, 10, bits)
		}
		if strings.Contains(vType, BasicTypeFloatPrefix) {
			bitsLen := strings.TrimPrefix(vType, BasicTypeFloatPrefix)
			bits, _ := strconv.Atoi(bitsLen)
			var f float64
			f, err = strconv.ParseFloat(value, bits)
			if err == nil {
				arg = math.Trunc(math.Round(f*100)) / 100
			}
		}
		if strings.Contains(vType, BasicTypeBool) {
			arg, err = strconv.ParseBool(value)
		}
		if strings.Contains(vType, BasicTypeString) {
			arg = value
		}

		if arg == nil || err != nil {
			return nil, fmt.Errorf("ParseValueMetricsToArgs: parse arg failed: %v, args(%v) err(%v)", metric, arg, err)
		}
		args[metric.ValueName] = arg
	}
	log.LogInfof("ParseValueMetricsToArgs: %v:%v, args:%v", operation, GetOpShortMsg(operation), args)
	return args, nil
}

func ParseValueMetricsToParams(operation int, metrics []*CliValueMetric) (params map[string]string, isEmpty bool, err error) {
	if len(metrics) != len(CliOperations[operation]) {
		err = fmt.Errorf("args count is inconsistent, except:%v actural:%v, operation:%v",
			len(CliOperations[operation]), len(metrics), GetOpShortMsg(operation))
		return
	}
	if checkEmptyMetric(metrics) {
		isEmpty = true
		return
	}
	params = make(map[string]string)
	for _, metric := range metrics {
		params[metric.ValueName] = metric.Value
	}
	return
}

func ParseKeyValueParams(operation int, metrics [][]*CliValueMetric) (params map[string]string, err error) {
	params = make(map[string]string)
	for _, metric := range metrics {
		if checkKeyValueEmpty(metric) {
			continue
		}
		var (
			key   string
			value string
		)
		for _, entry := range metric {
			entry.ValueName = strings.TrimSpace(entry.ValueName)
			entry.Value = strings.TrimSpace(entry.Value)
			switch entry.ValueName {
			case "key":
				key = entry.Value
			case "value":
				value = entry.Value
			}
		}
		params[key] = value
	}
	if len(params) == 0 {
		return nil, fmt.Errorf("empty key-value list")
	}
	return params, nil
}

func checkKeyValueEmpty(metrics []*CliValueMetric) bool {
	for _, metric := range metrics {
		if metric.Value == "" {
			return true
		}
	}
	return false
}

func checkEmptyMetric(metrics []*CliValueMetric) bool {
	for _, metric := range metrics {
		if metric.Value != "" {
			return false
		}
	}
	return true
}

// 非批量模块支持批量的的value
func IsMultiSelectValue(operation int) bool {
	switch operation {
	case OpClientReadVolRateLimit, OpClientWriteVolRateLimit:
		return true
	default:
		return false
	}
}

type CliOpMetric struct {
	OpCode string
	OpMsg  string
}

func NewCliOpMetric(code, msg string) *CliOpMetric {
	return &CliOpMetric{
		OpCode: code,
		OpMsg:  msg,
	}
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
