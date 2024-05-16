package migration

import (
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
	"time"
)

var (
	masterClient = master.NewMasterClient(strings.Split(ltptestMaster, ","), false)
)
var migTask *MigrateTask

var task = &proto.Task{
	TaskId:     0,
	TaskType:   proto.WorkerTypeInodeMigration,
	Cluster:    clusterName,
	VolName:    "volumeName",
	DpId:       0,
	MpId:       1,
	WorkerAddr: "127.0.0.1:17321",
}

func TestGetMpInfo(t *testing.T) {
	time.Sleep(time.Minute * 2)
	migTask = NewMigrateTask(task, masterClient, vol)
	err := migTask.GetMpInfo()
	if err != nil {
		assert.FailNowf(t, err.Error(), "GetMpInfo err(%v)", err)
	}
	assert.Equal(t, migTask.stage, GetMNProfPort)
}

func TestGetProfPort(t *testing.T) {
	TestGetMpInfo(t)
	err := migTask.GetProfPort()
	if err != nil {
		assert.FailNowf(t, err.Error(), "GetProfPort err(%v)", err)
	}
	assert.Equal(t, migTask.stage, ListAllIno)
}

func TestMpIsRecover(t *testing.T) {
	migTask = NewMigrateTask(task, masterClient, vol)
	assert.False(t, migTask.isRecover(), "")
}
