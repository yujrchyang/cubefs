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
	defer log.LogFlush()
	var testPath = testutil.InitTempTestPath(t)
	defer testPath.Cleanup()
	var err error
	diskName := "testDisk"
	dps := []uint64{1, 2, 3}
	var diskPath = path.Join(testPath.Path(), diskName)
	if err = os.MkdirAll(diskPath, os.ModePerm); err != nil {
		t.Fatalf("Make disk path %v failed: %v", diskPath, err)
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
	if err = space.LoadDisk(path.Join(testPath.Path(), diskName), nil); err != nil {
		t.Fatalf("Load disk %v failed: %v", diskPath, err)
	}
	// create dp
	for _, id := range dps {
		_, err = space.CreatePartition(&proto.CreateDataPartitionRequest{
			PartitionId:   id,
			PartitionSize: 1024 * 1024,
			VolumeId:      "test_volume",
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
		})
		if err != nil {
			t.Fatalf("Create partition failed: %v", err)
		}
	}
	time.Sleep(time.Second * 5)
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
	t.Run("reload_dp_with_wal", func(t *testing.T) {
		// expire dp
		dp := space.Partition(dps[2])
		dp.raftPartition.Expired()
		space.ExpiredPartition(dps[2])
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
				t.Errorf("partition:%v invalid wal dir: %v", dps[2], entry.Name())
			}
		}
	})
	var entries []os.DirEntry
	entries, err = os.ReadDir(diskPath)
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		if strings.Contains(entry.Name(), "expire") {
			t.Errorf("invalid partition path: %v", entry.Name())
		}
	}
}
