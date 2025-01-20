package intraMigration

import (
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/schedulenode/migration"
	"github.com/cubefs/cubefs/sdk/data"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
	"time"
)

const (
	clusterName   = "chubaofs01"
	ltptestVolume = "ltptest"
	ltptestMaster = "192.168.0.11:17010,192.168.0.12:17010,192.168.0.13:17010"
)

var (
	masterClient = master.NewMasterClient(strings.Split(ltptestMaster, ","), false)
	migTask *MigrateTask
	vol  *migration.VolumeInfo
)

func init() {
	vol = &migration.VolumeInfo{
		GetMigrationConfig: func(cluster, volName string) (volumeConfig proto.MigrationConfig) {
			return proto.MigrationConfig{
				Region:    region,
				Endpoint:  endPoint,
				AccessKey: accessKey,
				SecretKey: secretKey,
				Bucket:    bucketName,
				VolId:     volId,
			}
		},
	}
	vol.ClusterName = clusterName
	vol.Name = ltptestVolume
	nodes := strings.Split(ltptestMaster, ",")
	err := vol.Init(nodes, data.Normal)
	if err != nil {
		panic(fmt.Sprintf("vol.Init, err:%v", err))
	}
}

var task = &proto.Task{
	TaskId:     0,
	TaskType:   proto.WorkerTypeInodeMigration,
	Cluster:    clusterName,
	VolName:    "volumeName",
	DpId:       0,
	MpId:       1,
	WorkerAddr: "127.0.0.1:17321",
}

func getMpInfo(t *testing.T) {
	time.Sleep(time.Minute * 2)
	vol := &migration.VolumeInfo{
		GetMigrationConfig: func(cluster, volName string) (volumeConfig proto.MigrationConfig) {
			return proto.MigrationConfig{
				Region:    region,
				Endpoint:  endPoint,
				AccessKey: accessKey,
				SecretKey: secretKey,
				Bucket:    bucketName,
				VolId:     volId,
			}
		},
	}
	migTask = NewMigrateTask("127.0.0.1", task, masterClient, vol)
	err := migTask.SetMpInfo()
	if err != nil {
		assert.FailNowf(t, err.Error(), "SetMpInfo err(%v)", err)
	}
	assert.Equal(t, migTask.stage, GetMNProfPort)
}

func TestGetProfPort(t *testing.T) {
	getMpInfo(t)
	err := migTask.SetProfPort()
	if err != nil {
		assert.FailNowf(t, err.Error(), "SetProfPort err(%v)", err)
	}
	assert.Equal(t, migTask.stage, ListAllIno)
}


type LayerPolicyMeta struct {
	TimeType     int8
	TimeValue    int64
	TargetMedium proto.MediumType
}

func TestSetInodeMigDirection(t *testing.T) {
	layerPolicyMetas := []LayerPolicyMeta{
		{
			TimeType:     proto.InodeAccessTimeTypeSec,
			TimeValue:    15,
			TargetMedium: proto.MediumHDD,
		},
		{
			TimeType:     proto.InodeAccessTimeTypeDays,
			TimeValue:    15,
			TargetMedium: proto.MediumHDD,
		},
		{
			TimeType:     proto.InodeAccessTimeTypeTimestamp,
			TimeValue:    15,
			TargetMedium: proto.MediumHDD,
		},
		{
			TimeType:     proto.InodeAccessTimeTypeSec,
			TimeValue:    15,
			TargetMedium: proto.MediumS3,
		},
		{
			TimeType:     proto.InodeAccessTimeTypeDays,
			TimeValue:    15,
			TargetMedium: proto.MediumS3,
		},
		{
			TimeType:     proto.InodeAccessTimeTypeTimestamp,
			TimeValue:    15,
			TargetMedium: proto.MediumS3,
		},
	}
	for _, policyMeta := range layerPolicyMetas {
		vol.GetLayerPolicies = func(cluster, volName string) (layerPolicies []interface{}, exist bool) {
			exist = true
			lp := &proto.LayerPolicyInodeATime{
				TimeType:     policyMeta.TimeType,
				TimeValue:    policyMeta.TimeValue,
				TargetMedium: policyMeta.TargetMedium,
			}
			layerPolicies = append(layerPolicies, lp)
			return
		}
		mpOperation := &MigrateTask{
			vol:  vol,
			mpId: 1,
		}
		inodeInfo := &proto.InodeInfo{
			AccessTime: proto.CubeFSTime(0),
			ModifyTime: proto.CubeFSTime(0),
		}
		var (
			migDir migration.MigrateDirection
			err    error
		)
		if migDir, err = mpOperation.GetInodeMigDirection(inodeInfo); err != nil {
			assert.FailNow(t, err.Error())
		}
		if policyMeta.TargetMedium == proto.MediumHDD {
			assert.Equal(t, migration.SSDToHDDFileMigrate, migDir)
		}
		if policyMeta.TargetMedium == proto.MediumS3 {
			assert.Equal(t, migration.S3FileMigrate, migDir)
		}
		inodeInfo = &proto.InodeInfo{
			AccessTime: proto.CubeFSTime(time.Now().Unix()),
			ModifyTime: proto.CubeFSTime(time.Now().Unix()),
		}
		if migDir, err = mpOperation.GetInodeMigDirection(inodeInfo); err != nil {
			assert.FailNow(t, err.Error())
		}
		if policyMeta.TargetMedium == proto.MediumHDD {
			assert.Equal(t, migration.HDDToSSDFileMigrate, migDir)
		}
		if policyMeta.TargetMedium == proto.MediumS3 {
			assert.Equal(t, migration.ReverseS3FileMigrate, migDir)
		}
	}
}