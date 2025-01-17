package migration

import (
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/sdk/meta"
)

type MigTask interface {
	UpdateStatisticsInfo(info MigRecord)
	GetInodeMigDirection(inodeInfo *proto.InodeInfo) (migDir MigDirection, err error)
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
