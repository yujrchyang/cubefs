package datanode

import (
	"context"
	"fmt"
	"github.com/cubefs/cubefs/raftstore"
	"github.com/cubefs/cubefs/util/log"
	"github.com/stretchr/testify/assert"
	"github.com/tiglabs/raft/logger"
	raftlog "github.com/tiglabs/raft/util/log"
	"math/rand"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cubefs/cubefs/datanode/mock"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/testutil"
)

func TestSpaceManager_CreatePartition(t *testing.T) {
	var testPath = testutil.InitTempTestPath(t)
	defer testPath.Cleanup()

	var err error

	var diskPath = path.Join(testPath.Path(), "disk")
	if err = os.MkdirAll(diskPath, os.ModePerm); err != nil {
		t.Fatalf("Make disk path %v failed: %v", diskPath, err)
	}

	var space = NewSpaceManager(&fakeNode.DataNode)
	space.SetClusterID("test")
	space.SetRaftStore(mock.NewMockRaftStore())
	if err = space.LoadDisk(path.Join(testPath.Path(), "disk"), nil); err != nil {
		t.Fatalf("Load disk %v failed: %v", diskPath, err)
	}

	const (
		numOfInitPartitions     = 5
		numOfAccessWorker       = 10
		numOfIncreasePartitions = 5
	)

	var nextPartitionID uint64 = 1
	var initPartitionIDs = make([]uint64, 0, numOfInitPartitions)
	for i := 1; i <= numOfInitPartitions; i++ {
		if _, err = space.CreatePartition(&proto.CreateDataPartitionRequest{
			PartitionId:   nextPartitionID,
			PartitionSize: 1024 * 1024,
			VolumeId:      "test_volume",
		}); err != nil {
			t.Fatalf("Create partition failed: %v", err)
		}
		initPartitionIDs = append(initPartitionIDs, uint64(i))
		nextPartitionID++
	}

	var (
		ctx, cancel          = context.WithCancel(context.Background())
		accessWorkerLaunchWG = new(sync.WaitGroup)
		accessWorkerFinishWG = new(sync.WaitGroup)
		accessCount          int64
	)

	accessWorkerLaunchWG.Add(1)
	for i := 0; i < numOfAccessWorker; i++ {
		accessWorkerFinishWG.Add(1)
		go func() {
			defer accessWorkerFinishWG.Done()
			accessWorkerLaunchWG.Wait()
			var r = rand.New(rand.NewSource(time.Now().UnixNano()))
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				var pid = initPartitionIDs[r.Intn(len(initPartitionIDs))]
				space.Partition(pid)
				atomic.AddInt64(&accessCount, 1)
			}
		}()
	}

	var startTime = time.Now()
	accessWorkerLaunchWG.Done()
	for i := 0; i < numOfIncreasePartitions; i++ {
		if _, err = space.CreatePartition(&proto.CreateDataPartitionRequest{
			PartitionId:   nextPartitionID,
			PartitionSize: 1024 * 1024,
			VolumeId:      "test_volume",
		}); err != nil {
			t.Fatalf("Create partition failed: %v", err)
		}
		nextPartitionID++
	}

	cancel()
	accessWorkerFinishWG.Wait()
	var creationElapsed = time.Now().Sub(startTime)

	t.Logf("Statistics: creation elapsed %v, access count: %v", creationElapsed, atomic.LoadInt64(&accessCount))
}
func newRaftLogger(dir string) {

	raftLogPath := path.Join(dir, "logs")
	_, err := os.Stat(raftLogPath)
	if err != nil {
		if pathErr, ok := err.(*os.PathError); ok {
			if os.IsNotExist(pathErr) {
				os.MkdirAll(raftLogPath, 0755)
			}
		}
	}

	raftLog, err := raftlog.NewLog(raftLogPath, "raft", "debug")
	if err != nil {
		fmt.Println("Fatal: failed to start the baud storage daemon - ", err)
		return
	}
	logger.SetLogger(raftLog)
	return
}

func TestReloadExpiredDataPartitions(t *testing.T) {
	raftPath := "/tmp/raft"
	newRaftLogger(raftPath)
	var testPath = testutil.InitTempTestPath(t)
	//log.InitLog(testPath.Path(), "dp", log.DebugLevel, nil)
	//defer log.LogFlush()

	defer testPath.Cleanup()
	var err error
	diskNames := []string{
		"testDisk",
		"testDisk2",
	}
	diskDirs := []string{
		path.Join(testPath.Path(), "testDisk"),
		path.Join(testPath.Path(), "testDisk2"),
	}
	dps := []uint64{1, 2, 3, 4, 5, 6}
	for _, d := range diskNames {
		if err = os.MkdirAll(path.Join(testPath.Path(), d), os.ModePerm); err != nil {
			t.Fatalf("Make disk path %v failed: %v", d, err)
		}
	}
	nodeID := uint64(1)
	var space = NewSpaceManager(&fakeNode.DataNode)
	space.SetClusterID("test")
	raftConf := &raftstore.Config{
		NodeID:            nodeID,
		RaftPath:          raftPath,
		TickInterval:      raftstore.DefaultTickInterval,
		IPAddr:            LocalIP,
		HeartbeatPort:     9098,
		ReplicaPort:       9097,
		NumOfLogsToRetain: DefaultRaftLogsToRetain,
	}
	var raftStore raftstore.RaftStore
	raftStore, err = raftstore.NewRaftStore(raftConf)
	if err != nil {
		log.LogErrorf("action[startRaftServer] cannot start raft server err(%v)", err)
		return
	}
	space.SetRaftStore(raftStore)
	for _, d := range diskNames {
		if err = space.LoadDisk(path.Join(testPath.Path(), d), nil); err != nil {
			t.Fatalf("Load disk %v failed: %v", d, err)
		}
	}

	var newCreateDpReq = func(id uint64, volName string) (req *proto.CreateDataPartitionRequest) {
		return &proto.CreateDataPartitionRequest{
			PartitionId:   id,
			PartitionSize: 1024 * 1024,
			VolumeId:      volName,
			ReplicaNum:    1,
			Members: []proto.Peer{
				{
					1,
					fmt.Sprintf("127.0.0.1:%v", fakeNode.DataNode.port),
					proto.PeerNormal,
				},
			},
			Hosts: []string{
				fmt.Sprintf("127.0.0.1:%v", fakeNode.DataNode.port),
			},
			CreateType: proto.NormalCreateDataPartition,
		}
	}
	// create dp
	for _, id := range dps {
		_, err = space.CreatePartition(newCreateDpReq(id, "test_vol"))
		if err != nil {
			t.Fatalf("Create partition failed: %v", err)
		}
	}
	time.Sleep(time.Second)
	for _, id := range dps {
		dp := space.Partition(id)
		status := dp.raftPartition.Status()
		assert.NotNil(t, status)
		if !assert.Equal(t, false, status.Stopped) {
			return
		}
	}
	t.Run("reload_dp_by_id", func(t *testing.T) {
		// expire dp
		space.ExpiredPartition(dps[0])
		//restore dp
		failedDisks, failedDps, successDps := space.RestoreExpiredPartitions(false, map[uint64]bool{dps[0]: true})
		assert.Equal(t, 0, len(failedDisks))
		assert.Equal(t, 0, len(failedDps))
		assert.Equal(t, 1, len(successDps))
	})
	t.Run("reload_dp", func(t *testing.T) {
		// expire dp
		space.ExpiredPartition(dps[1])
		//restore dp
		failedDisks, failedDps, successDps := space.RestoreExpiredPartitions(true, make(map[uint64]bool, 0))
		assert.Equal(t, 0, len(failedDisks))
		assert.Equal(t, 0, len(failedDps))
		assert.Equal(t, 1, len(successDps))
	})
	var expireWithoutTimestamp = func(dpid uint64) {
		dp := space.Partition(dpid)
		if dp == nil {
			return
		}
		space.partitionMutex.Lock()
		delete(space.partitions, dpid)
		space.partitionMutex.Unlock()
		dp.Stop()
		dp.Disk().DetachDataPartition(dp)
		var currentPath = path.Clean(dp.path)
		var newPath = path.Join(path.Dir(currentPath), ExpiredPartitionPrefix+path.Base(currentPath))
		if err = os.Rename(currentPath, newPath); err != nil {
			log.LogErrorf("ExpiredPartition: mark expired partition fail: volume(%v) partitionID(%v) path(%v) newPath(%v) err(%v)",
				dp.volumeID,
				dp.partitionID,
				dp.path,
				newPath,
				err)
			return
		}
	}

	var verifyExpired = func(t *testing.T, expect int, paths []string) {
		count := 0
		for _, p := range paths {
			var entries []os.DirEntry
			entries, err = os.ReadDir(p)
			if err != nil {
				t.Fatalf("read dir failed: %v", err)
			}
			for _, entry := range entries {
				if !entry.IsDir() {
					continue
				}
				if strings.Contains(entry.Name(), "expire") {
					count++
				}
			}
		}
		if !assert.Equal(t, expect, count) {
			return
		}
	}
	t.Run("reload_dp_without_timestamp", func(t *testing.T) {
		// expire dp
		expireWithoutTimestamp(dps[2])
		//restore dp
		failedDisks, failedDps, successDps := space.RestoreExpiredPartitions(true, make(map[uint64]bool, 0))
		assert.Equal(t, 0, len(failedDisks))
		assert.Equal(t, 0, len(failedDps))
		assert.Equal(t, 1, len(successDps))
		// verify vol name
		assert.Equal(t, "test_vol", space.Partition(dps[2]).volumeID)
		verifyExpired(t, 0, diskDirs)
	})

	t.Run("reload_dp_with_wal", func(t *testing.T) {
		// expire dp
		dp := space.Partition(dps[3])
		dp.raftPartition.Expired()
		space.ExpiredPartition(dps[3])
		//restore dp
		failedDisks, failedDps, successDps := space.RestoreExpiredPartitions(true, make(map[uint64]bool, 0))
		assert.Equal(t, 0, len(failedDisks))
		assert.Equal(t, 0, len(failedDps))
		assert.Equal(t, 1, len(successDps))
		var entries []os.DirEntry
		entries, err = os.ReadDir(dp.path)
		for _, entry := range entries {
			if !entry.IsDir() {
				continue
			}
			if strings.Contains(entry.Name(), "expire") {
				t.Errorf("partition:%v invalid wal dir: %v", dps[3], entry.Name())
			}
		}
	})

	verifyExpired(t, 0, diskDirs)

	t.Run("reload_multi_dp", func(t *testing.T) {
		// expire old dp
		space.ExpiredPartition(dps[4])
		// expire dp without timestamp
		newVol := "test_vol_1"
		_, err = space.CreatePartition(newCreateDpReq(dps[4], newVol))
		if err != nil {
			t.Fatalf("Create partition failed: %v", err)
		}
		time.Sleep(time.Second)
		expireWithoutTimestamp(dps[4])
		// expire dp with latest timestamp
		newVol2 := "test_vol_2"
		_, err = space.CreatePartition(newCreateDpReq(dps[4], newVol2))
		if err != nil {
			t.Fatalf("Create partition failed: %v", err)
		}
		time.Sleep(time.Second)
		space.ExpiredPartition(dps[4])

		verifyExpired(t, 3, diskDirs)

		// restore dp
		failedDisks, failedDps, successDps := space.RestoreExpiredPartitions(true, make(map[uint64]bool))
		assert.Equal(t, 0, len(failedDisks))
		assert.Equal(t, 0, len(failedDps))
		assert.Equal(t, 1, len(successDps))
		// verify vol name
		assert.Equal(t, newVol2, space.Partition(dps[4]).volumeID)
	})

	t.Run("reload_dp_already_exist", func(t *testing.T) {
		assert.GreaterOrEqual(t, len(space.disks), 2)
		disks := make(map[string]int)
		for i := 0; i < 4; i++ {
			_, _ = space.CreatePartition(newCreateDpReq(dps[5], "test_vol"))
			dp := space.Partition(dps[5])
			time.Sleep(time.Second * 2)
			t.Logf("create partition(%v) on disk(%v) allocated(%v) total disk(%v)", dp.partitionID, dp.disk.Path, dp.disk.Allocated, len(space.disks))
			space.ExpiredPartition(dps[5])
			dp.disk.Allocated += 1024 * 1024
			disks[dp.disk.Path] += 1
		}
		for d, count := range disks {
			for i := 0; i < count; i++ {
				space.disks[d].Allocated -= 1024 * 1024
			}
		}

		failedMap := make(map[uint64]int)
		successMap := make(map[uint64]int)
		_, failed, success := space.RestoreExpiredPartitions(false, map[uint64]bool{dps[5]: true})
		for _, f := range failed {
			failedMap[f] = failedMap[f] + 1
		}
		for _, f := range success {
			successMap[f] = successMap[f] + 1
		}
		assert.Equal(t, 0, len(failedMap))
		assert.Equal(t, 1, len(successMap))

		failedMap = make(map[uint64]int)
		successMap = make(map[uint64]int)
		_, failed, success = space.RestoreExpiredPartitions(false, map[uint64]bool{dps[5]: true})
		for _, f := range failed {
			failedMap[f] = failedMap[f] + 1
		}
		for _, f := range success {
			successMap[f] = successMap[f] + 1
		}
		assert.Equal(t, 1, len(failedMap))
		assert.Equal(t, 0, len(successMap))
	})
}
