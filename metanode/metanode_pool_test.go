package metanode

import (
	"fmt"
	se "github.com/cubefs/cubefs/util/sortedextent"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"sync"
	"testing"
	"time"
)

var nullInode = &Inode{
	Extents: se.NewSortedExtents(),
}
func init() {
	rand.Seed(time.Now().UnixMilli())
}

func fillInodeInfo(inode *Inode) {
	curTime := Now.GetCurrentTime()
	createTimeSinceMinute := rand.Intn(60)
	inode.CreateTime = curTime.Add(time.Minute * time.Duration(-createTimeSinceMinute)).Unix()
	inode.AccessTime = curTime.Unix()
	inode.ModifyTime = curTime.Unix()
	inode.Size = rand.Uint64()
	inode.Gid = rand.Uint32()
	inode.Uid = rand.Uint32()
	inode.NLink = uint32(rand.Intn(10000))
	inode.Generation = uint64(rand.Intn(10000))
	inode.LinkTarget = []byte(fmt.Sprintf("test_inode_pool_%v", inode.Inode))
}

func testInodePoolBatchGetAndPut(t *testing.T, goroutineIndex int) {
	//循环2000次
	for index := 0; index < 2000; index++ {
		inodeCount := rand.Intn(1000)
		batchInodes := inodePool.BatchGet(inodeCount)
		for i := 0; i < inodeCount; i++ {
			assert.Equal(t, batchInodes[i], nullInode)
			fillInodeInfo(batchInodes[i])
		}
		for i := 0; i < inodeCount; i++ {
			inodePool.Put(batchInodes[i])
		}
	}
}

func testInodePoolGetAndBatchPut(t *testing.T, goroutineIndex int) {
	//循环2000次
	for index := 0; index < 2000; index++ {
		inodeCount := rand.Intn(1000)
		batchInodes := make(InodeBatch, 0)
		for cnt := 0; cnt < inodeCount; cnt++ {
			inode := inodePool.Get()
			assert.Equal(t, inode, nullInode)
			inode.Inode = uint64(goroutineIndex * index)
			fillInodeInfo(inode)
			batchInodes = append(batchInodes, inode)
		}
		inodePool.BatchPut(batchInodes)
	}
}

func testInodePoolBatchGetAndBatchPut(t *testing.T, goroutineIndex int) {
	//循环2000次
	for index := 0; index < 2000; index++ {
		inodeCount := rand.Intn(1000)
		batchInodes := inodePool.BatchGet(inodeCount)
		for i := 0; i < inodeCount; i++ {
			assert.Equal(t, batchInodes[i], nullInode)
			fillInodeInfo(batchInodes[i])
		}
		inodePool.BatchPut(batchInodes)
	}
}

func testInodePoolGetAndPut(t *testing.T, goroutineIndex int) {
	for i := 0; i < 10000; i++ {
		inode := inodePool.Get()
		//check inode get
		assert.Equal(t, inode, nullInode)
		inode.Inode = uint64(goroutineIndex * i)
		fillInodeInfo(inode)
		inodePool.Put(inode)
	}
}

func TestInodePool(t *testing.T) {
	nullInode.Reset()
	var wg sync.WaitGroup
	goroutineCount := 20
	wg.Add(goroutineCount)
	for index := 1; index <= goroutineCount; index++ {
		go func(index int) {
			defer wg.Done()
			if index%4 == 0 {
				testInodePoolGetAndPut(t, index)
			} else if index%4 == 1 {
				testInodePoolBatchGetAndPut(t, index)
			} else if index%4 == 2 {
				testInodePoolGetAndBatchPut(t, index)
			} else {
				testInodePoolBatchGetAndBatchPut(t, index)
			}
		}(index)
	}
	wg.Wait()
}
