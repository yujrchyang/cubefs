package data

import (
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/assert"
)

func TestWrapper_getDataPartitionFromMaster(t *testing.T) {
	dataWrapper, err := NewDataPartitionWrapper(ltptestVolume, ltptestMaster, Normal)
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
	assert.Equal(t, ltptestVolume, ec.UmpKeyPrefix())
	keyPrefix := "test"
	mc.AdminAPI().UpdateVolumeWithMap(ltptestVolume, calcAuthKey(ltptestVolume), map[string]string{proto.UmpKeyPrefixKey: keyPrefix})
	ec.dataWrapper.updateSimpleVolView()
	assert.Equal(t, keyPrefix, ec.UmpKeyPrefix())
	mc.AdminAPI().UpdateVolumeWithMap(ltptestVolume, calcAuthKey(ltptestVolume), map[string]string{proto.UmpKeyPrefixKey: ""})
	ec.dataWrapper.updateSimpleVolView()
	assert.Equal(t, ltptestVolume, ec.UmpKeyPrefix())
}
