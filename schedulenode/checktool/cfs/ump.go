package cfs

import (
	"github.com/cubefs/cubefs/util/checktool"
	"github.com/cubefs/cubefs/util/log"
)

const (
	UMPCFSNormalWarnKeyConst = "Storage-Bot.cfs"
)

var (
	dongDongAlarmApp string
	dongDongAlarmGid int
)
var (
	umpKeyStorageBotPrefix               string
	UMPCFSNormalWarnKey                  = "cfs"
	UMPCFSZoneUsedRatioWarnKey           = "cfs.zone.used.ratio"
	UMPCFSZoneUsedRatioOPWarnKey         = "cfs.zone.used.ratio.op"
	UMPCFSRaftlogBackWarnKey             = "chubaofs.raft.log.backup"
	UMPCFSClusterUsedRatio               = "chubaofs.cluster.used.ratio"
	UMPCFSClusterConnRefused             = "chubaofs.cluster.connection.refused"
	UMPKeyInactiveNodes                  = "chubaofs.inactive.nodes"
	UMPKeyMetaPartitionNoLeader          = "chubaofs.meta.partition.no.leader"
	UMPKeyDataPartitionLoadFailed        = "chubaofs.data.partition.load.failed"
	UMPKeyMetaPartitionPeerInconsistency = "chubaofs.meta.partition.peer.inconsistency"
	UMPKeyDataPartitionPeerInconsistency = "chubaofs.data.partition.peer.inconsistency"
	UMPKeyMetaNodeDiskSpace              = "chubaofs.meta.node.disk.space"
	UMPKeyMetaNodeDiskRatio              = "chubaofs.meta.node.disk.ratio"
	UMPKeyMasterLbPodStatus              = "chubaofs.master.lb.pod.status"
	UMPKeyClusterConfigCheck             = "chubaofs.cluster.config"
	UMPCFSNodeRestartWarnKey             = "cfs.restart.node"
	UMPCFSInactiveNodeWarnKey            = "cfs.inactive.node"
	UMPCFSZoneWriteAbilityWarnKey        = "cfs.zone.writeability.ratio"
	UMPCFSInodeCountDiffWarnKey          = "cfs.inode.count.diff"
	UMPCFSRapidMemIncreaseWarnKey        = "cfs.rapid.mem.increase"
	UMPCFSMySqlMemWarnKey                = "cfs.mysql.mem"
	UMPCFSSparkFixPartitionKey           = "cfs.fix_bad_replica"
	UMPCFSSparkFlashNodeVersionKey       = "cfs.flashnode.version"
	UMPCFSBadDiskWarnKey                 = "cfs.bad.disk"
	UMPCFSMasterMetaCompareKey           = "cfs.master.rocksdb.compare"
	UMPCFSNodeSetNumKey                  = "cfs.nodeset.num"
	UMPCFSNodeTinyExtentCheckKey         = "cfs.tiny.extent.check"
	UMPCFSCoreVolWarnKey                 = "cfs.core.vol"
	UMPCFSMysqlBadDiskKey                = "cfs.mysql.bad.disk"
	UMPCFSMysqlInactiveNodeKey           = "cfs.mysql.inactive.node"
	UMPMetaPartitionApplyKey             = "cfs.meta.apply"
	UMPMetaPartitionApplyFailedKey       = "cfs.meta.apply.failed"
	UmpHBaseCapBeyondLevelKey            = "hbase.cap.beyond.level"
	UMPCFSNLInactiveNodeKey              = "cfs.nl.inactive.node"
	UMPKeyStuckNodes                     = "chubaofs.stuck.nodes"
	UMPKeyOfflineFailed                  = "chubaofs.offline.failed"
)

const (
	umpKeyMysqlClientPrefix = "mysql_kbpclient_"
	umpKeySparkClientPrefix = "spark_client_"
	umpKeyWarningSufix      = "warning"
	umpKeyFatalSufix        = "fatal"
)

func ResetUmpKeyPrefix(keyPrefix string) {
	umpKeyStorageBotPrefix = keyPrefix
}

func isClientKey(key string) bool {
	return key == umpKeyMysqlClientPrefix+umpKeyFatalSufix || key == umpKeySparkClientPrefix+umpKeyFatalSufix
}

func isNormalUmpKey(umpKey string) bool {
	return umpKey == UMPCFSNormalWarnKeyConst
}

func warnBySpecialUmpKeyWithPrefix(key, alarmMsg string) {
	var realKey string
	if umpKeyStorageBotPrefix == "" {
		log.LogErrorf("Storage Bot ump key prefix is empty, key:%v msg:%v", key, alarmMsg)
		return
	}

	// todo 以后客户端监控能不能改成带prefix的？否则测试环境不好适配
	if isProEnv() && isClientKey(key) {
		realKey = key
	} else {
		realKey = umpKeyStorageBotPrefix + "." + key
	}

	// 线上重要key直接推送到运维群
	// 测试环境key全部推送到测试群
	if isNormalUmpKey(realKey) || isDevEnv() {
		checktool.WarnByDongDongAlarmToTargetGid(dongDongAlarmGid, dongDongAlarmApp, realKey, alarmMsg)
	} else {
		checktool.WarnBySpecialUmpKey(realKey, alarmMsg)
	}
}
