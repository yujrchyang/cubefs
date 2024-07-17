package data

import (
	"strings"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	masterSDK "github.com/cubefs/cubefs/sdk/master"
	"github.com/stretchr/testify/assert"
)

const (
	ltptestVolume  = "ltptest"
	ltptestMaster  = "192.168.0.11:17010,192.168.0.12:17010,192.168.0.13:17010"
	ltptestAuthKey = "0e20229116d5a9a4a9e876806b514a85"
)

func TestWrapper_getDataPartitionFromMaster(t *testing.T) {
	dataWrapper, err := NewDataPartitionWrapper(ltptestVolume, strings.Split(ltptestMaster, ","), Normal)
	if err != nil {
		t.Fatalf("NewDataPartitionWrapper failed, err %v", err)
	}

	dataWrapper.InitFollowerRead(true)
	dataWrapper.SetNearRead(true)

	close(dataWrapper.stopC)

	var validPids []uint64
	dataWrapper.partitions.Range(func(key, value interface{}) bool {
		pid := key.(uint64)
		validPids = append(validPids, pid)
		return true
	})
	if len(validPids) == 0 {
		t.Fatalf("no valid data partition for test")
	}

	var invalidPid uint64
	for _, pid := range validPids {
		if invalidPid <= pid {
			invalidPid = pid + 1
		}
		if err = dataWrapper.getDataPartitionFromMaster(pid); err != nil {
			t.Fatalf("getDataPartitionFromMaster failed, pid %v, err %v", pid, err)
		}
	}

	oldValue := MasterNoCacheAPIRetryTimeout
	MasterNoCacheAPIRetryTimeout = 10 * time.Second
	if err = dataWrapper.getDataPartitionFromMaster(invalidPid); err == nil {
		t.Fatalf("getDataPartitionFromMaster use invalidPid %v, expect failed but success", invalidPid)
	}
	MasterNoCacheAPIRetryTimeout = oldValue
}

func TestWrapper_updateUmpKeyPrefix(t *testing.T) {
	_, ec, _ := creatHelper(t)
	assert.Equal(t, ltptestVolume, ec.UmpKeyPrefix())
	keyPrefix := "test"
	mc := masterSDK.NewMasterClient(strings.Split(ltptestMaster, ","), false)
	mc.AdminAPI().UpdateVolumeWithMap(ltptestVolume, ltptestAuthKey, map[string]string{proto.UmpKeyPrefixKey: keyPrefix})
	ec.dataWrapper.updateSimpleVolView()
	assert.Equal(t, keyPrefix, ec.UmpKeyPrefix())
	mc.AdminAPI().UpdateVolumeWithMap(ltptestVolume, ltptestAuthKey, map[string]string{proto.UmpKeyPrefixKey: ""})
	ec.dataWrapper.updateSimpleVolView()
	assert.Equal(t, ltptestVolume, ec.UmpKeyPrefix())
}
