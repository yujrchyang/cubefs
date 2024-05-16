package migration

import (
	"github.com/cubefs/cubefs/sdk/data"
	"strings"
	"testing"
)

const (
	ClusterName   = "chubaofs01"
	LtptestVolume = "ltptest"
	LtptestMaster = "192.168.0.11:17010,192.168.0.12:17010,192.168.0.13:17010"
)

var volume *VolumeInfo

func TestNewVolumeInfo(t *testing.T) {
	var err error
	mcc := &ControlConfig{InodeCheckStep: 100, InodeConcurrent: 10, MinEkLen: 2, MinInodeSize: 1, MaxEkAvgSize: 32}
	if volume, err = NewVolumeInfo(ClusterName, LtptestVolume, strings.Split(LtptestMaster, ","), mcc, data.Normal, nil, nil); err != nil {
		t.Fatalf("NewVolumeInfo should not hava err, but err:%v", err)
	}
}

func TestReleaseResource(t *testing.T) {
	TestNewVolumeInfo(t)
	volume.ReleaseResource()
}

func TestIsRunning(t *testing.T) {
	TestNewVolumeInfo(t)
	running := volume.IsRunning()
	if !running {
		t.Fatalf("volume IsRunning expect:%v, actual:%v", running, !running)
	}
}
