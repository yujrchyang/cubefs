package metanode

import (
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"
)

func batchCreateInode(mp *metaPartition, count int) {
	reqCreateInode := &proto.CreateInodeRequest{
		PartitionID: mp.config.PartitionId,
		Gid:         0,
		Uid:         0,
		Mode:        470,
	}
	resp := &Packet{}
	for index := 0; index < count; index++ {
		_ = mp.CreateInode(reqCreateInode, resp)
	}
}

func checkSnapshotApplyID(t *testing.T, mp *metaPartition, expectApplyID uint64) {
	if mp.HasMemStore() {
		mpForCheckResult := new(metaPartition)
		mpForCheckResult.config = mp.config
		if err := mpForCheckResult.loadApplyIDAndCursor(); err != nil {
			t.Logf("load apply id by check mp failed: %v", err)
			t.FailNow()
		}
		assert.Equal(t, expectApplyID,  mpForCheckResult.applyID, fmt.Sprintf("mem mode expect applyID: %v, but is %v", expectApplyID, mpForCheckResult.applyID))
	} else {
		assert.Equal(t, mp.applyID,  mp.inodeTree.GetApplyID(), fmt.Sprintf("rocksdb mode expect applyID: %v, but is %v", mp.applyID, mp.inodeTree.GetApplyID()))
	}
}

func innerTestStoreSchedule(t *testing.T, partitionStoreMode proto.StoreMode) {
	rand.Seed(time.Now().UnixNano())
	var mp *metaPartition
	var err error
	mp, err = mockMetaPartitionWithStartStoreSchedule(1, 1, partitionStoreMode, "./test_store_schedule", ApplyMock)
	if mp == nil {
		fmt.Printf("new mock meta partition failed\n")
		t.FailNow()
	}

	defer func() {
		releaseMetaPartition(mp)
	}()

	maxInode := genInode(t, mp, 10000)
	if maxInode <= 0 {
		fmt.Printf("error max inode id:%v\n", maxInode)
		t.FailNow()
	}
	genDentry(t, mp, 10000, maxInode)
	mp.reqRecords = InitRequestRecords(genBatchRequestInfo(128, false))
	mp.config.Cursor = maxInode
	err = mp.store(&storeMsg{
		command:    opFSMStoreTick,
		applyIndex: mp.applyID,
		snap:       NewSnapshot(mp),
		reqTree:    mp.reqRecords.ReqBTreeSnap(),
	})
	if err != nil {
		t.Logf("store snapshot failed: %v", err)
		t.FailNow()
	}

	mp.storeChan <- &storeMsg{
		command:    opFSMStoreTick,
		applyIndex: mp.applyID - 100,
		snap:       NewSnapshot(mp),
		reqTree:    mp.reqRecords.ReqBTreeSnap(),
	}

	expectApplyID := mp.applyID
	atomic.StoreInt64(&mp.lastDumpTime, time.Now().Add(time.Minute*(-5)).Unix())
	time.Sleep(intervalDumpSnap*2)
	fmt.Printf("start check apply id, scene: curIndex > msg.applyID\n")
	checkSnapshotApplyID(t, mp, expectApplyID)

	batchCreateInode(mp, 120)

	assert.Equal(t, mp.raftPartition.AppliedIndex(), mp.applyID, fmt.Sprintf("apply expect equal, " +
		"raftPartitionApplyID: %v, mpApplyID: %v", mp.raftPartition.AppliedIndex(), mp.applyID))

	expectApplyID = mp.applyID
	mp.storeChan <- &storeMsg{
		command:    opFSMStoreTick,
		applyIndex: mp.applyID,
		snap:       NewSnapshot(mp),
		reqTree:    mp.reqRecords.ReqBTreeSnap(),
	}
	atomic.StoreInt64(&mp.lastDumpTime, time.Now().Add(time.Minute*(-5)).Unix())
	time.Sleep(intervalDumpSnap*2)
	fmt.Printf("start check apply id, scene: curIndex < msg.applyID\n")
	checkSnapshotApplyID(t, mp, expectApplyID)

	for retryCnt := 0; retryCnt < 3; retryCnt++ {
		batchCreateInode(mp, 20)
		mp.storeChan <- &storeMsg{
			command:    opFSMStoreTick,
			applyIndex: mp.applyID,
			snap:       NewSnapshot(mp),
			reqTree:    mp.reqRecords.ReqBTreeSnap(),
		}
	}

	expectApplyID = mp.applyID
	mp.storeChan <- &storeMsg{
		command:    opFSMStoreTick,
		applyIndex: mp.applyID - 10,
		snap:       NewSnapshot(mp),
		reqTree:    mp.reqRecords.ReqBTreeSnap(),
	}

	atomic.StoreInt64(&mp.lastDumpTime, time.Now().Add(time.Minute*(-5)).Unix())
	time.Sleep(intervalDumpSnap*2)
	fmt.Printf("start check apply id, scene: curIndex < msg.applyID\n")
	checkSnapshotApplyID(t, mp, expectApplyID)
}

func TestMetaPartition_StoreSchedule(t *testing.T) {
	innerTestStoreSchedule(t, proto.StoreModeMem)
	innerTestStoreSchedule(t, proto.StoreModeRocksDb)
}