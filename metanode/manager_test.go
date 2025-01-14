package metanode

import (
	"github.com/stretchr/testify/assert"
	"math/rand"
	"os"
	"path"
	"strconv"
	"testing"
	"time"
)

func TestMetadataManager_getMetaPartitionsLastExpiredInfoCase01(t *testing.T) {
	metaDataDir := "./test_restore_partitions/meta"
	raftDir := "./test_restore_partitions/raft"
	_ = os.RemoveAll(metaDataDir)
	_ = os.RemoveAll(raftDir)
	err := os.MkdirAll(metaDataDir, 0777)
	if err != nil {
		t.Errorf("mkdir %s failed: %v", metaDataDir, err)
		return
	}

	err = os.MkdirAll(raftDir, 0777)
	if err != nil {
		t.Errorf("mkdir %s failed: %v", raftDir, err)
		return
	}
	defer func() {
		_ = os.RemoveAll(metaDataDir)
		_ = os.RemoveAll(raftDir)
	}()
	metaNode := &MetaNode{
		nodeId:            1,
		metadataDir:       metaDataDir,
		raftDir:           raftDir,
		localAddr:         "127.0.0.1",
		clusterId:         "test",
		zoneName:          "default",
	}
	metaManager := &metadataManager{
		nodeId:                1,
		zoneName:              "default",
		rootDir:               metaDataDir,
		metaNode:              metaNode,
	}
	metaNode.metadataManager = metaManager

	expectLastExpiredInfo := make(map[uint64]*RestorePartitionInfo, 0)
	rand.Seed(time.Now().UnixNano())
	//mkdir expired dir
	for index := uint64(1); index < 100; index++ {
		partitionIDStr := strconv.FormatUint(index, 10)
		if index%5 == 0 {
			curTime := time.Now()
			expiredDirCount := rand.Intn(5)
			if expiredDirCount == 0 {
				continue
			}
			for i := 1; i <= expiredDirCount; i++ {
				expiredTimeStamp := curTime.Add(time.Duration(-i*10)*time.Minute).Unix()
				partitionMetaDataDir := path.Join(metaDataDir, ExpiredPartitionPrefix+"partition_" + partitionIDStr+"_"+strconv.FormatInt(expiredTimeStamp, 10))
				partitionRaftDir := path.Join(raftDir, ExpiredPartitionPrefix+partitionIDStr+"_"+strconv.FormatInt(expiredTimeStamp, 10))
				err = os.Mkdir(partitionMetaDataDir, 0777)
				assert.Empty(t, err)
				err = os.Mkdir(partitionRaftDir, 0777)
				assert.Empty(t, err)
				if i == 1 {
					expectLastExpiredInfo[index] = &RestorePartitionInfo{
						PartitionID:          index,
						LastExpiredTimeStamp: expiredTimeStamp,
						MetaDataDir:          partitionMetaDataDir,
						RaftDir:              partitionRaftDir,
					}
				}
			}
			continue
		}

		err = os.Mkdir(path.Join(metaDataDir, partitionPrefix+ partitionIDStr), 0777)
		assert.Empty(t, err)
		err = os.Mkdir(path.Join(raftDir, partitionIDStr), 0777)
		assert.Empty(t, err)
	}

	restorePartitions, _, err := metaManager.getMetaPartitionsLastExpiredInfo(true, nil)
	assert.Empty(t, err)
	assert.Equal(t, expectLastExpiredInfo, restorePartitions)
}

func TestMetadataManager_getMetaPartitionsLastExpiredInfoCase02(t *testing.T) {
	metaDataDir := "./test_restore_partitions/meta"
	raftDir := "./test_restore_partitions/raft"
	_ = os.RemoveAll(metaDataDir)
	_ = os.RemoveAll(raftDir)
	err := os.MkdirAll(metaDataDir, 0777)
	if err != nil {
		t.Errorf("mkdir %s failed: %v", metaDataDir, err)
		return
	}

	err = os.MkdirAll(raftDir, 0777)
	if err != nil {
		t.Errorf("mkdir %s failed: %v", raftDir, err)
		return
	}
	defer func() {
		_ = os.RemoveAll(metaDataDir)
		_ = os.RemoveAll(raftDir)
	}()
	metaNode := &MetaNode{
		nodeId:            1,
		metadataDir:       metaDataDir,
		raftDir:           raftDir,
		localAddr:         "127.0.0.1",
		clusterId:         "test",
		zoneName:          "default",
	}
	metaManager := &metadataManager{
		nodeId:                1,
		zoneName:              "default",
		rootDir:               metaDataDir,
		metaNode:              metaNode,
	}
	metaNode.metadataManager = metaManager

	rand.Seed(time.Now().UnixNano())
	//mkdir expired dir
	for index := uint64(1); index < 100; index++ {
		partitionIDStr := strconv.FormatUint(index, 10)
		if index%5 == 0 {
			curTime := time.Now()
			var expiredDirCount int
			if index == 5 {
				expiredDirCount = 1
			} else {
				expiredDirCount = rand.Intn(5)
			}
			if expiredDirCount == 0 {
				continue
			}
			for i := 1; i <= expiredDirCount; i++ {
				expiredTimeStamp := curTime.Add(time.Duration(-i*10)*time.Minute).Unix()
				partitionMetaDataDir := path.Join(metaDataDir, ExpiredPartitionPrefix+"partition_" + partitionIDStr+"_"+strconv.FormatInt(expiredTimeStamp, 10))
				partitionRaftDir := path.Join(raftDir, ExpiredPartitionPrefix+partitionIDStr+"_"+strconv.FormatInt(expiredTimeStamp, 10))
				err = os.Mkdir(partitionMetaDataDir, 0777)
				assert.Empty(t, err)
				err = os.Mkdir(partitionRaftDir, 0777)
				assert.Empty(t, err)
			}
			if index == 5 {
				err = os.Mkdir(path.Join(metaDataDir, partitionPrefix+ partitionIDStr), 0777)
				assert.Empty(t, err)
			}
			continue
		}

		err = os.Mkdir(path.Join(metaDataDir, partitionPrefix+ partitionIDStr), 0777)
		assert.Empty(t, err)
		err = os.Mkdir(path.Join(raftDir, partitionIDStr), 0777)
		assert.Empty(t, err)
	}

	var failedMPs []uint64
	var restorePartitions map[uint64]*RestorePartitionInfo
	restorePartitions, failedMPs, err = metaManager.getMetaPartitionsLastExpiredInfo(false, map[uint64]bool{5:true})
	assert.Empty(t, err)
	assert.Equal(t, 0, len(restorePartitions))
	assert.Equal(t, []uint64{5}, failedMPs)
}

func TestMetadataManager_getMetaPartitionsLastExpiredInfoCase03(t *testing.T) {
	metaDataDir := "./test_restore_partitions/meta"
	raftDir := "./test_restore_partitions/raft"
	_ = os.RemoveAll(metaDataDir)
	_ = os.RemoveAll(raftDir)
	err := os.MkdirAll(metaDataDir, 0777)
	if err != nil {
		t.Errorf("mkdir %s failed: %v", metaDataDir, err)
		return
	}

	err = os.MkdirAll(raftDir, 0777)
	if err != nil {
		t.Errorf("mkdir %s failed: %v", raftDir, err)
		return
	}
	defer func() {
		_ = os.RemoveAll(metaDataDir)
		_ = os.RemoveAll(raftDir)
	}()
	metaNode := &MetaNode{
		nodeId:            1,
		metadataDir:       metaDataDir,
		raftDir:           raftDir,
		localAddr:         "127.0.0.1",
		clusterId:         "test",
		zoneName:          "default",
	}
	metaManager := &metadataManager{
		nodeId:                1,
		zoneName:              "default",
		rootDir:               metaDataDir,
		metaNode:              metaNode,
	}
	metaNode.metadataManager = metaManager

	expectLastExpiredInfo := make(map[uint64]*RestorePartitionInfo, 0)
	rand.Seed(time.Now().UnixNano())
	//mkdir expired dir
	for index := uint64(1); index < 100; index++ {
		partitionIDStr := strconv.FormatUint(index, 10)
		if index%5 == 0 {
			curTime := time.Now()
			var expiredDirCount int
			if index == 50 {
				expiredDirCount = 1
			} else {
				expiredDirCount = rand.Intn(5)
			}
			if expiredDirCount == 0 {
				continue
			}
			for i := 1; i <= expiredDirCount; i++ {
				expiredTimeStamp := curTime.Add(time.Duration(-i*10)*time.Minute).Unix()
				partitionMetaDataDir := path.Join(metaDataDir, ExpiredPartitionPrefix+"partition_" + partitionIDStr+"_"+strconv.FormatInt(expiredTimeStamp, 10))
				partitionRaftDir := path.Join(raftDir, ExpiredPartitionPrefix+partitionIDStr+"_"+strconv.FormatInt(expiredTimeStamp, 10))
				if i == 1 && index == 50 {
					partitionRaftDir = path.Join(raftDir, partitionIDStr)
				}

				err = os.Mkdir(partitionMetaDataDir, 0777)
				assert.Empty(t, err)
				err = os.Mkdir(partitionRaftDir, 0777)
				assert.Empty(t, err)
				if i == 1 {
					expectLastExpiredInfo[index] = &RestorePartitionInfo{
						PartitionID:          index,
						LastExpiredTimeStamp: expiredTimeStamp,
						MetaDataDir:          partitionMetaDataDir,
						RaftDir:              partitionRaftDir,
					}
					if index == 50 {
						expectLastExpiredInfo[index].RaftDir = ""
					}
				}
			}
			continue
		}

		err = os.Mkdir(path.Join(metaDataDir, partitionPrefix+ partitionIDStr), 0777)
		assert.Empty(t, err)
		err = os.Mkdir(path.Join(raftDir, partitionIDStr), 0777)
		assert.Empty(t, err)
	}

	var failedMPs []uint64
	var restorePartitions map[uint64]*RestorePartitionInfo
	restorePartitions, failedMPs, err = metaManager.getMetaPartitionsLastExpiredInfo(true, nil)
	assert.Empty(t, err)
	assert.Equal(t, expectLastExpiredInfo, restorePartitions)
	assert.Equal(t, []uint64{50}, failedMPs)
}

func TestMetadataManager_getMetaPartitionsLastExpiredInfoCase04(t *testing.T) {
	metaDataDir := "./test_restore_partitions/meta"
	raftDir := "./test_restore_partitions/raft"
	_ = os.RemoveAll(metaDataDir)
	_ = os.RemoveAll(raftDir)
	err := os.MkdirAll(metaDataDir, 0777)
	if err != nil {
		t.Errorf("mkdir %s failed: %v", metaDataDir, err)
		return
	}

	err = os.MkdirAll(raftDir, 0777)
	if err != nil {
		t.Errorf("mkdir %s failed: %v", raftDir, err)
		return
	}
	defer func() {
		_ = os.RemoveAll(metaDataDir)
		_ = os.RemoveAll(raftDir)
	}()
	metaNode := &MetaNode{
		nodeId:            1,
		metadataDir:       metaDataDir,
		raftDir:           raftDir,
		localAddr:         "127.0.0.1",
		clusterId:         "test",
		zoneName:          "default",
	}
	metaManager := &metadataManager{
		nodeId:                1,
		zoneName:              "default",
		rootDir:               metaDataDir,
		metaNode:              metaNode,
	}
	metaNode.metadataManager = metaManager

	expectLastExpiredInfo := make(map[uint64]*RestorePartitionInfo, 0)
	rand.Seed(time.Now().UnixNano())
	//mkdir expired dir
	for index := uint64(1); index < 100; index++ {
		partitionIDStr := strconv.FormatUint(index, 10)
		if index%5 == 0 {
			curTime := time.Now()
			var expiredDirCount int
			if index == 50 {
				expiredDirCount = 1
			} else {
				expiredDirCount = rand.Intn(5)
			}
			if expiredDirCount == 0 {
				continue
			}
			for i := 1; i <= expiredDirCount; i++ {
				expiredTimeStamp := curTime.Add(time.Duration(-i*10)*time.Minute).Unix()
				partitionMetaDataDir := path.Join(metaDataDir, ExpiredPartitionPrefix+"partition_" + partitionIDStr+"_"+strconv.FormatInt(expiredTimeStamp, 10))
				partitionRaftDir := path.Join(raftDir, ExpiredPartitionPrefix+partitionIDStr+"_"+strconv.FormatInt(expiredTimeStamp, 10))
				if i == 1 && index == 50 {
					partitionRaftDir = path.Join(raftDir, ExpiredPartitionPrefix+partitionIDStr+"_"+strconv.FormatInt(expiredTimeStamp+120, 10))
				}

				err = os.Mkdir(partitionMetaDataDir, 0777)
				assert.Empty(t, err)
				err = os.Mkdir(partitionRaftDir, 0777)
				assert.Empty(t, err)
				if i == 1 {
					expectLastExpiredInfo[index] = &RestorePartitionInfo{
						PartitionID:          index,
						LastExpiredTimeStamp: expiredTimeStamp,
						MetaDataDir:          partitionMetaDataDir,
						RaftDir:              partitionRaftDir,
					}
					if index == 50 {
						expectLastExpiredInfo[index].RaftDir = ""
					}
				}
			}
			continue
		}

		err = os.Mkdir(path.Join(metaDataDir, partitionPrefix+ partitionIDStr), 0777)
		assert.Empty(t, err)
		err = os.Mkdir(path.Join(raftDir, partitionIDStr), 0777)
		assert.Empty(t, err)
	}

	var failedMPs []uint64
	var restorePartitions map[uint64]*RestorePartitionInfo
	restorePartitions, failedMPs, err = metaManager.getMetaPartitionsLastExpiredInfo(true, nil)
	assert.Empty(t, err)
	assert.Equal(t, expectLastExpiredInfo, restorePartitions)
	assert.Equal(t, []uint64{50}, failedMPs)
}

func TestMetadataManager_getMetaPartitionsLastExpiredInfoCase05(t *testing.T) {
	metaDataDir := "./test_restore_partitions/meta"
	raftDir := "./test_restore_partitions/raft"
	_ = os.RemoveAll(metaDataDir)
	_ = os.RemoveAll(raftDir)
	err := os.MkdirAll(metaDataDir, 0777)
	if err != nil {
		t.Errorf("mkdir %s failed: %v", metaDataDir, err)
		return
	}

	err = os.MkdirAll(raftDir, 0777)
	if err != nil {
		t.Errorf("mkdir %s failed: %v", raftDir, err)
		return
	}
	defer func() {
		_ = os.RemoveAll(metaDataDir)
		_ = os.RemoveAll(raftDir)
	}()
	metaNode := &MetaNode{
		nodeId:            1,
		metadataDir:       metaDataDir,
		raftDir:           raftDir,
		localAddr:         "127.0.0.1",
		clusterId:         "test",
		zoneName:          "default",
	}
	metaManager := &metadataManager{
		nodeId:                1,
		zoneName:              "default",
		rootDir:               metaDataDir,
		metaNode:              metaNode,
	}
	metaNode.metadataManager = metaManager

	expectLastExpiredInfo := make(map[uint64]*RestorePartitionInfo, 0)
	rand.Seed(time.Now().UnixNano())
	//mkdir expired dir
	for index := uint64(1); index < 100; index++ {
		partitionIDStr := strconv.FormatUint(index, 10)
		if index%5 == 0 {
			curTime := time.Now()
			expiredDirCount := rand.Intn(5)
			if expiredDirCount == 0 {
				continue
			}
			for i := 1; i <= expiredDirCount; i++ {
				expiredTimeStamp := curTime.Add(time.Duration(-i*10)*time.Minute).Unix()
				partitionMetaDataDir := path.Join(metaDataDir, ExpiredPartitionPrefix+"partition_" + partitionIDStr+"_"+strconv.FormatInt(expiredTimeStamp, 10))
				partitionRaftDir := path.Join(raftDir, ExpiredPartitionPrefix+partitionIDStr+"_"+strconv.FormatInt(expiredTimeStamp+2, 10))
				err = os.Mkdir(partitionMetaDataDir, 0777)
				assert.Empty(t, err)
				err = os.Mkdir(partitionRaftDir, 0777)
				assert.Empty(t, err)
				if i == 1 {
					expectLastExpiredInfo[index] = &RestorePartitionInfo{
						PartitionID:          index,
						LastExpiredTimeStamp: expiredTimeStamp,
						MetaDataDir:          partitionMetaDataDir,
						RaftDir:              partitionRaftDir,
					}
				}
			}
			continue
		}

		err = os.Mkdir(path.Join(metaDataDir, partitionPrefix+ partitionIDStr), 0777)
		assert.Empty(t, err)
		err = os.Mkdir(path.Join(raftDir, partitionIDStr), 0777)
		assert.Empty(t, err)
	}

	var restorePartitions map[uint64]*RestorePartitionInfo
	restorePartitions, _, err = metaManager.getMetaPartitionsLastExpiredInfo(true, nil)
	assert.Empty(t, err)
	assert.Equal(t, expectLastExpiredInfo, restorePartitions)
}