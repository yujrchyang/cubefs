package migration

import (
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/sdk/meta"
)

type Task interface {
	UpdateStatisticsInfo(info MigrateRecord)
	GetInodeMigDirection(inodeInfo *proto.InodeInfo) (migDir MigrateDirection, err error)
	GetInodeInfoMaxTime(inodeInfo *proto.InodeInfo) (err error)
	GetTaskType() proto.WorkerType
	GetTaskId() uint64
	GetMasterClient() *master.MasterClient
	GetProfPort() string
	GetMpInfo() *meta.MetaPartition
	GetLocalIp() string
	GetVol() *VolumeInfo
	GetMpId() uint64
	GetMpLeader() string
	GetRawTask() *proto.Task
}
