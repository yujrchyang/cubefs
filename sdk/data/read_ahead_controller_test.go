package data

import (
	"container/list"
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/unit"
	"github.com/stretchr/testify/assert"
)

func TestReadAhead_Controller(t *testing.T) {
	mw, ec, err := creatExtentClient()
	assert.NoErrorf(t, err, "init extent client err")
	defer func() {
		ec.Close(context.Background())
		log.LogFlush()
	}()

	var (
		inodeInfo *proto.InodeInfo
		read      int
		hole      bool
	)
	ec.dataWrapper.readAheadController, err = NewReadAheadController(ec.dataWrapper, 4096, DefaultReadAheadWindowMB*unit.MB)
	assert.NoErrorf(t, err, "init read ahead controller err")

	mw.Delete_ll(context.Background(), 1, "TestReadAhead", false)
	inodeInfo, err = mw.Create_ll(context.Background(), 1, "TestReadAhead", 0644, 0, 0, nil)
	assert.NoErrorf(t, err, "create file TestReadAhead err")

	err = ec.OpenStream(inodeInfo.Inode, false, false)
	assert.NoErrorf(t, err, "open streamer of inode(%v)", inodeInfo.Inode)
	s := ec.GetStreamer(inodeInfo.Inode)
	assert.NotNilf(t, s, "get streamer of inode(%v)", inodeInfo.Inode)

	size := 128 * unit.KB
	for offset := uint64(0); offset < 10*unit.MB+100; offset += uint64(size) {
		data := randTestData(size)
		_, _, err = ec.Write(context.Background(), inodeInfo.Inode, offset, data, false)
		assert.NoErrorf(t, err, "write file ino(%v) offset(%v) size(%v) err", inodeInfo.Inode, offset, size)
	}

	readOffset := uint64(10)
	readSize := uint64(1024)
	readData := make([]byte, readSize)
	read, hole, err = ec.dataWrapper.readAheadController.ReadFromBlocks(s, readData, readOffset, readSize)
	assert.Zerof(t, read, "first read ahead size")
	assert.Falsef(t, hole, "read ahead hole")
	assert.Errorf(t, err, "first read ahead should fail")
	// 等待预读
	time.Sleep(1 * time.Second)
	assert.Equalf(t, int64(31), ec.dataWrapper.readAheadController.blockCount, "read ahead block count")
	assert.Equalf(t, int(31), ec.dataWrapper.readAheadController.blockLruList.Len(), "read ahead lru list length")
	for offset := uint64(128 * unit.KB); offset < 10*unit.MB+100; offset += uint64(size) {
		data := make([]byte, size)
		read, hole, err = ec.dataWrapper.readAheadController.ReadFromBlocks(s, data, offset, uint64(size))
		assert.Equalf(t, size, read, "read ahead ino(%v) offset(%v) size(%v)", inodeInfo.Inode, offset, size)
		assert.Falsef(t, hole, "read ahead hole, ino(%v) offset(%v) size(%v)", inodeInfo.Inode, offset, size)
		assert.NoErrorf(t, err, "read ahead ino(%v) offset(%v) size(%v)", inodeInfo.Inode, offset, size)
		time.Sleep(1 * time.Millisecond)
	}

	assert.Zerof(t, ec.dataWrapper.readAheadController.blockCount, "read ahead block count after expire")
	assert.Zerof(t, ec.dataWrapper.readAheadController.blockLruList.Len(), "read ahead lru list length after expire")

	err = ec.CloseStream(context.Background(), inodeInfo.Inode)
	assert.NoErrorf(t, err, "close streamer of inode(%v)", inodeInfo.Inode)
	err = ec.EvictStream(context.Background(), inodeInfo.Inode)
	assert.NoErrorf(t, err, "evict streamer of inode(%v)", inodeInfo.Inode)
}

func TestReadAhead_ReadAndWrite(t *testing.T) {
	// create inode
	mw, ec, err := creatExtentClient()
	assert.NoErrorf(t, err, "init extent client err")
	defer func() {
		ec.Close(context.Background())
		assert.Equalf(t, ec.GetReadAheadController().blockCount, int64(ec.GetReadAheadController().blockLruList.Len()), "block lru list len")
		log.LogFlush()
	}()

	ec.autoFlush = true
	ec.SetEnableWriteCache(true)
	ec.tinySize = unit.DefaultTinySizeLimit
	ec.dataWrapper.followerRead = false
	ec.dataWrapper.readAheadController, err = NewReadAheadController(ec.dataWrapper, 64, DefaultReadAheadWindowMB*unit.MB)
	assert.NoErrorf(t, err, "init read ahead controller err")

	mw.Delete_ll(context.Background(), 1, "TestReadAhead_ReadAndWrite", false)
	var inodeInfo *proto.InodeInfo
	inodeInfo, err = mw.Create_ll(context.Background(), 1, "TestReadAhead_ReadAndWrite", 0644, 0, 0, nil)
	assert.NoErrorf(t, err, "create file 'TestReadAhead_ReadAndWrite' err")

	err = ec.OpenStream(inodeInfo.Inode, false, false)
	assert.NoErrorf(t, err, "open stream of inode(%v)", inodeInfo.Inode)

	localPath := "/tmp/TestReadAhead_ReadAndWrite"
	localFile, _ := os.Create(localPath)

	timestamp := time.Now().Unix()
	rand.Seed(timestamp)
	fmt.Println("time: ", timestamp)
	fileSize := int64(700 * unit.MB)
	for i := 0; i < 10000; i++ {
		wOffset, wSize := randOffset(fileSize)
		//fmt.Printf("write offset: %v size: %v\n", wOffset, wSize)
		if err = writeLocalAndCFS(localFile, ec, inodeInfo.Inode, wOffset, wSize); err != nil {
			panic(err)
		}
		rOffset, rSize := randOffset(fileSize)
		//fmt.Printf("read offset: %v size: %v\n", rOffset, rSize)
		if err = verifyLocalAndCFS(localFile, ec, inodeInfo.Inode, rOffset, rSize); err != nil {
			log.LogFlush()
			panic(err)
		}
		if i%3000 == 0 {
			truncateSize := fileSize/2 + rand.Int63n(fileSize/2)
			if err = truncateLocalAndCFS(localFile, ec, inodeInfo.Inode, truncateSize); err != nil {
				log.LogFlush()
				panic(err)
			}
			rOffset, rSize := randOffset(fileSize)
			//fmt.Printf("read offset: %v size: %v\n", rOffset, rSize)
			if err = verifyLocalAndCFS(localFile, ec, inodeInfo.Inode, rOffset, rSize); err != nil {
				log.LogFlush()
				panic(err)
			}
		}
		assert.LessOrEqualf(t, int64(ec.GetReadAheadController().blockLruList.Len()), ec.GetReadAheadController().blockCntThreshold, "block should be less than threshold")
	}
	cfsSize, _, _ := ec.FileSize(inodeInfo.Inode)
	localInfo, _ := localFile.Stat()
	assert.Equal(t, uint64(localInfo.Size()), cfsSize, "file size")
	verifySize := 1024 * 1024
	for off := int64(0); off < int64(cfsSize); off += int64(verifySize) {
		if err = verifyLocalAndCFS(localFile, ec, inodeInfo.Inode, off, verifySize); err != nil {
			log.LogFlush()
			panic(err)
		}
	}
	assert.LessOrEqualf(t, ec.GetReadAheadController().blockCount, ec.GetReadAheadController().blockCntThreshold, "block should be less than threshold")

	s := ec.GetStreamer(inodeInfo.Inode)
	err = ec.Flush(context.Background(), inodeInfo.Inode)
	assert.NoErrorf(t, err, "flush inode(%v) fail", inodeInfo.Inode)
	err = ec.CloseStream(context.Background(), inodeInfo.Inode)
	assert.NoErrorf(t, err, "close streamer of inode(%v)", inodeInfo.Inode)
	err = ec.EvictStream(context.Background(), inodeInfo.Inode)
	assert.NoErrorf(t, err, "evict streamer of inode(%v)", inodeInfo.Inode)
	localFile.Close()

	blockCount := 0
	s.readAheadBlocks.Range(func(key, value interface{}) bool {
		blockCount++
		return true
	})
	assert.Zerof(t, blockCount, "streamer block count should be 0")
	//time.Sleep(6 * time.Second)
	//assert.Zerof(t, ec.GetReadAheadController().blockCount, "read ahead block count should be 0")
	//assert.Zerof(t, ec.GetReadAheadController().blockLruList.Len(), "read ahead lru list length should be 0")
}

func TestReadAhead_ReadFromBlocks(t *testing.T) {
	defer func() {
		log.LogFlush()
	}()
	tests := []struct {
		name                 string
		extentSize           uint64
		dataTotalSize        int
		readOffset           uint64
		readSize             uint64
		storeBlocks          []*ReadAheadBlock
		expectedErr          bool
		expectedRead         int
		expectedRemainBlocks []uint64
	}{
		{
			name:          "test01",
			extentSize:    10 * unit.MB,
			dataTotalSize: 100 * unit.KB,
			readOffset:    0,
			readSize:      100 * unit.KB,
			storeBlocks: []*ReadAheadBlock{
				{startFileOffset: 0, endFileOffset: 128 * unit.KB, actualDataSize: 100 * unit.KB, data: make([]byte, 128*unit.KB)},
			},
			expectedRemainBlocks: []uint64{128 * unit.KB},
			expectedErr:          false,
			expectedRead:         100 * unit.KB,
		},
		{
			name:          "test02",
			extentSize:    10 * unit.MB,
			dataTotalSize: 1 * unit.MB,
			readOffset:    128*unit.KB + 100,
			readSize:      1 * unit.KB,
			storeBlocks: []*ReadAheadBlock{
				{startFileOffset: 0, endFileOffset: 128 * unit.KB, actualDataSize: 100 * unit.KB, data: make([]byte, 128*unit.KB)},
				{startFileOffset: 128 * unit.KB, endFileOffset: 2 * 128 * unit.KB, actualDataSize: 100 * unit.KB, data: make([]byte, 128*unit.KB)},
			},
			expectedRemainBlocks: []uint64{0, 128 * unit.KB},
			expectedErr:          false,
			expectedRead:         1 * unit.KB,
		},
		{
			name:          "test03",
			extentSize:    10 * unit.MB,
			dataTotalSize: 1 * unit.MB,
			readOffset:    128*unit.KB + 100,
			readSize:      100*unit.KB - 100,
			storeBlocks: []*ReadAheadBlock{
				{startFileOffset: 0, endFileOffset: 128 * unit.KB, actualDataSize: 100 * unit.KB, data: make([]byte, 128*unit.KB)},
				{startFileOffset: 128 * unit.KB, endFileOffset: 2 * 128 * unit.KB, actualDataSize: 100 * unit.KB, data: make([]byte, 128*unit.KB)},
			},
			expectedRemainBlocks: []uint64{0},
			expectedErr:          false,
			expectedRead:         100*unit.KB - 100,
		},
		{
			name:          "test04",
			extentSize:    10 * unit.MB,
			dataTotalSize: 1 * unit.MB,
			readOffset:    128 * unit.KB,
			readSize:      128 * unit.KB,
			storeBlocks: []*ReadAheadBlock{
				{startFileOffset: 0, endFileOffset: 128 * unit.KB, actualDataSize: 100 * unit.KB, data: make([]byte, 128*unit.KB)},
				{startFileOffset: 128 * unit.KB, endFileOffset: 2 * 128 * unit.KB, actualDataSize: 128 * unit.KB, data: make([]byte, 128*unit.KB)},
				{startFileOffset: 200 * unit.KB, endFileOffset: 300 * unit.KB, actualDataSize: 50 * unit.KB, data: make([]byte, 128*unit.KB)},
				{startFileOffset: 2 * 128 * unit.KB, endFileOffset: 3 * 128 * unit.KB, actualDataSize: 100*unit.KB + 100, data: make([]byte, 128*unit.KB)},
				{startFileOffset: 4 * 128 * unit.KB, endFileOffset: 5 * 128 * unit.KB, actualDataSize: 1024, data: make([]byte, 128*unit.KB), hasHole: true},
			},
			expectedRemainBlocks: []uint64{0, 200 * unit.KB, 2 * 128 * unit.KB, 4 * 128 * unit.KB},
			expectedErr:          false,
			expectedRead:         128 * unit.KB,
		},
		{
			name:          "test05",
			extentSize:    10 * unit.MB,
			dataTotalSize: 1 * unit.MB,
			readOffset:    200 * unit.KB,
			readSize:      100 * unit.KB,
			storeBlocks: []*ReadAheadBlock{
				{startFileOffset: 0, endFileOffset: 128 * unit.KB, actualDataSize: 100 * unit.KB, data: make([]byte, 128*unit.KB)},
				{startFileOffset: 128 * unit.KB, endFileOffset: 2 * 128 * unit.KB, actualDataSize: 128 * unit.KB, data: make([]byte, 128*unit.KB)},
				{startFileOffset: 200 * unit.KB, endFileOffset: 300 * unit.KB, actualDataSize: 50 * unit.KB, data: make([]byte, 128*unit.KB)},
				{startFileOffset: 2 * 128 * unit.KB, endFileOffset: 3 * 128 * unit.KB, actualDataSize: 100*unit.KB + 100, data: make([]byte, 128*unit.KB)},
				{startFileOffset: 4 * 128 * unit.KB, endFileOffset: 5 * 128 * unit.KB, actualDataSize: 1024, data: make([]byte, 128*unit.KB), hasHole: true},
			},
			expectedRemainBlocks: []uint64{0, 200 * unit.KB, 2 * 128 * unit.KB, 4 * 128 * unit.KB},
			expectedErr:          false,
			expectedRead:         100 * unit.KB,
		},
		{
			name:          "test06",
			extentSize:    10 * unit.MB,
			dataTotalSize: 1 * unit.MB,
			readOffset:    128 * unit.KB,
			readSize:      3 * 128 * unit.KB,
			storeBlocks: []*ReadAheadBlock{
				{startFileOffset: 0, endFileOffset: 128 * unit.KB, actualDataSize: 100 * unit.KB, data: make([]byte, 128*unit.KB)},
				{startFileOffset: 128 * unit.KB, endFileOffset: 2 * 128 * unit.KB, actualDataSize: 128 * unit.KB, data: make([]byte, 128*unit.KB)},
				{startFileOffset: 200 * unit.KB, endFileOffset: 300 * unit.KB, actualDataSize: 50 * unit.KB, data: make([]byte, 128*unit.KB)},
				{startFileOffset: 2 * 128 * unit.KB, endFileOffset: 3 * 128 * unit.KB, actualDataSize: 100*unit.KB + 100, data: make([]byte, 128*unit.KB)},
				{startFileOffset: 4 * 128 * unit.KB, endFileOffset: 5 * 128 * unit.KB, actualDataSize: 1024, data: make([]byte, 128*unit.KB), hasHole: true},
			},
			expectedRemainBlocks: []uint64{0, 200 * unit.KB, 4 * 128 * unit.KB},
			expectedErr:          true,
			expectedRead:         128*unit.KB + 100*unit.KB + 100,
		},
		{
			name:          "test07",
			extentSize:    10 * unit.MB,
			dataTotalSize: 1 * unit.MB,
			readOffset:    2 * 128 * unit.KB,
			readSize:      129 * unit.KB,
			storeBlocks: []*ReadAheadBlock{
				{startFileOffset: 0, endFileOffset: 128 * unit.KB, actualDataSize: 100 * unit.KB, data: make([]byte, 128*unit.KB)},
				{startFileOffset: 128 * unit.KB, endFileOffset: 2 * 128 * unit.KB, actualDataSize: 128 * unit.KB, data: make([]byte, 128*unit.KB)},
				{startFileOffset: 200 * unit.KB, endFileOffset: 300 * unit.KB, actualDataSize: 50 * unit.KB, data: make([]byte, 128*unit.KB)},
				{startFileOffset: 2 * 128 * unit.KB, endFileOffset: 3 * 128 * unit.KB, actualDataSize: 100*unit.KB + 100, data: make([]byte, 128*unit.KB)},
				{startFileOffset: 4 * 128 * unit.KB, endFileOffset: 5 * 128 * unit.KB, actualDataSize: 1024, data: make([]byte, 128*unit.KB)},
			},
			expectedRemainBlocks: []uint64{0, 128 * unit.KB, 200 * unit.KB, 4 * 128 * unit.KB},
			expectedErr:          true,
			expectedRead:         100*unit.KB + 100,
		},
		{
			name:          "test08",
			extentSize:    10 * unit.MB,
			dataTotalSize: 1 * unit.MB,
			readOffset:    0,
			readSize:      2 * 128 * unit.KB,
			storeBlocks: []*ReadAheadBlock{
				{startFileOffset: 0, endFileOffset: 128 * unit.KB, actualDataSize: 128 * unit.KB, data: make([]byte, 128*unit.KB)},
				{startFileOffset: 128 * unit.KB, endFileOffset: 2 * 128 * unit.KB, actualDataSize: 128 * unit.KB, data: make([]byte, 128*unit.KB)},
				{startFileOffset: 200 * unit.KB, endFileOffset: 300 * unit.KB, actualDataSize: 50 * unit.KB, data: make([]byte, 128*unit.KB)},
				{startFileOffset: 2 * 128 * unit.KB, endFileOffset: 3 * 128 * unit.KB, actualDataSize: 100*unit.KB + 100, data: make([]byte, 128*unit.KB)},
				{startFileOffset: 4 * 128 * unit.KB, endFileOffset: 5 * 128 * unit.KB, actualDataSize: 1024, data: make([]byte, 128*unit.KB)},
			},
			expectedRemainBlocks: []uint64{200 * unit.KB, 2 * 128 * unit.KB, 4 * 128 * unit.KB},
			expectedErr:          false,
			expectedRead:         2 * 128 * unit.KB,
		},
		{
			name:          "test09",
			extentSize:    10 * unit.MB,
			dataTotalSize: 1 * unit.MB,
			readOffset:    0,
			readSize:      2*128*unit.KB + 120*unit.KB,
			storeBlocks: []*ReadAheadBlock{
				{startFileOffset: 0, endFileOffset: 128 * unit.KB, actualDataSize: 128 * unit.KB, data: make([]byte, 128*unit.KB)},
				{startFileOffset: 128 * unit.KB, endFileOffset: 2 * 128 * unit.KB, actualDataSize: 128 * unit.KB, data: make([]byte, 128*unit.KB)},
				{startFileOffset: 200 * unit.KB, endFileOffset: 300 * unit.KB, actualDataSize: 50 * unit.KB, data: make([]byte, 128*unit.KB)},
				{startFileOffset: 2 * 128 * unit.KB, endFileOffset: 3 * 128 * unit.KB, actualDataSize: 100*unit.KB + 100, data: make([]byte, 128*unit.KB)},
				{startFileOffset: 4 * 128 * unit.KB, endFileOffset: 5 * 128 * unit.KB, actualDataSize: 1024, data: make([]byte, 128*unit.KB)},
			},
			expectedRemainBlocks: []uint64{200 * unit.KB, 4 * 128 * unit.KB},
			expectedErr:          false,
			expectedRead:         2*128*unit.KB + 100*unit.KB + 100,
		},
		{
			name:          "test10",
			extentSize:    10 * unit.MB,
			dataTotalSize: 1 * unit.MB,
			readOffset:    100,
			readSize:      2*128*unit.KB + 100*unit.KB,
			storeBlocks: []*ReadAheadBlock{
				{startFileOffset: 0, endFileOffset: 128 * unit.KB, actualDataSize: 128 * unit.KB, data: make([]byte, 128*unit.KB)},
				{startFileOffset: 128 * unit.KB, endFileOffset: 2 * 128 * unit.KB, actualDataSize: 128 * unit.KB, data: make([]byte, 128*unit.KB)},
				{startFileOffset: 200 * unit.KB, endFileOffset: 300 * unit.KB, actualDataSize: 50 * unit.KB, data: make([]byte, 128*unit.KB)},
				{startFileOffset: 2 * 128 * unit.KB, endFileOffset: 3 * 128 * unit.KB, actualDataSize: 100*unit.KB + 100, data: make([]byte, 128*unit.KB)},
				{startFileOffset: 4 * 128 * unit.KB, endFileOffset: 5 * 128 * unit.KB, actualDataSize: 1024, data: make([]byte, 128*unit.KB)},
			},
			expectedRemainBlocks: []uint64{200 * unit.KB, 4 * 128 * unit.KB},
			expectedErr:          false,
			expectedRead:         2*128*unit.KB + 100*unit.KB,
		},
		{
			name:          "test11",
			extentSize:    4 * unit.MB,
			dataTotalSize: 1 * unit.MB,
			readOffset:    10,
			readSize:      4*128*unit.KB - 10,
			storeBlocks: []*ReadAheadBlock{
				{startFileOffset: 0, endFileOffset: 128 * unit.KB, actualDataSize: 128 * unit.KB, data: make([]byte, 128*unit.KB)},
				{startFileOffset: 128 * unit.KB, endFileOffset: 2 * 128 * unit.KB, actualDataSize: 128 * unit.KB, data: make([]byte, 128*unit.KB)},
				{startFileOffset: 200 * unit.KB, endFileOffset: 300 * unit.KB, actualDataSize: 50 * unit.KB, data: make([]byte, 128*unit.KB)},
				{startFileOffset: 2 * 128 * unit.KB, endFileOffset: 3 * 128 * unit.KB, actualDataSize: 128 * unit.KB, data: make([]byte, 128*unit.KB)},
				{startFileOffset: 4 * 128 * unit.KB, endFileOffset: 5 * 128 * unit.KB, actualDataSize: 1024, data: make([]byte, 128*unit.KB)},
			},
			expectedRemainBlocks: []uint64{200 * unit.KB, 4 * 128 * unit.KB},
			expectedErr:          true,
			expectedRead:         3*128*unit.KB - 10,
		},
		{
			name:          "test12",
			extentSize:    3 * unit.MB,
			dataTotalSize: 1 * unit.MB,
			readOffset:    10,
			readSize:      4*128*unit.KB - 10,
			storeBlocks: []*ReadAheadBlock{
				{startFileOffset: 0, endFileOffset: 128 * unit.KB, actualDataSize: 128 * unit.KB, data: make([]byte, 128*unit.KB)},
				{startFileOffset: 128 * unit.KB, endFileOffset: 2 * 128 * unit.KB, actualDataSize: 128 * unit.KB, data: make([]byte, 128*unit.KB)},
			},
			expectedRemainBlocks: []uint64{0, 128 * unit.KB},
			expectedErr:          false,
			expectedRead:         0,
		},
		{
			name:          "test13",
			extentSize:    10 * unit.MB,
			dataTotalSize: 1 * unit.MB,
			readOffset:    0,
			readSize:      2 * 128 * unit.KB,
			storeBlocks: []*ReadAheadBlock{
				{startFileOffset: 0, endFileOffset: 128 * unit.KB, actualDataSize: 128 * unit.KB, data: make([]byte, 128*unit.KB)},
			},
			expectedRemainBlocks: []uint64{128 * unit.KB},
			expectedErr:          true,
			expectedRead:         128 * unit.KB,
		},
		{
			name:          "test14",
			extentSize:    10 * unit.MB,
			dataTotalSize: 1 * unit.MB,
			readOffset:    3*128*unit.KB - 100,
			readSize:      2,
			storeBlocks: []*ReadAheadBlock{
				{startFileOffset: 0, endFileOffset: 128 * unit.KB, actualDataSize: 128 * unit.KB, data: make([]byte, 128*unit.KB)},
				{startFileOffset: 128 * unit.KB, endFileOffset: 2 * 128 * unit.KB, actualDataSize: 128 * unit.KB, data: make([]byte, 128*unit.KB)},
				{startFileOffset: 200 * unit.KB, endFileOffset: 300 * unit.KB, actualDataSize: 50 * unit.KB, data: make([]byte, 128*unit.KB)},
				{startFileOffset: 2 * 128 * unit.KB, endFileOffset: 3 * 128 * unit.KB, actualDataSize: 100*unit.KB + 100, data: make([]byte, 128*unit.KB)},
				{startFileOffset: 4 * 128 * unit.KB, endFileOffset: 5 * 128 * unit.KB, actualDataSize: 1024, data: make([]byte, 128*unit.KB)},
			},
			expectedRemainBlocks: []uint64{0, 128 * unit.KB, 200 * unit.KB, 2 * 128 * unit.KB, 4 * 128 * unit.KB},
			expectedErr:          true,
			expectedRead:         0,
		},
		{
			name:          "test15",
			extentSize:    10 * unit.MB,
			dataTotalSize: 1 * unit.MB,
			readOffset:    128*unit.KB + 100,
			readSize:      2 * 128 * unit.KB,
			storeBlocks: []*ReadAheadBlock{
				{startFileOffset: 0, endFileOffset: 128 * unit.KB, actualDataSize: 128 * unit.KB, data: make([]byte, 128*unit.KB)},
				{startFileOffset: 128 * unit.KB, endFileOffset: 2 * 128 * unit.KB, actualDataSize: 128 * unit.KB, data: make([]byte, 128*unit.KB)},
				{startFileOffset: 200 * unit.KB, endFileOffset: 300 * unit.KB, actualDataSize: 50 * unit.KB, data: make([]byte, 128*unit.KB)},
				{startFileOffset: 2 * 128 * unit.KB, endFileOffset: 3 * 128 * unit.KB, actualDataSize: 100*unit.KB + 100, data: make([]byte, 128*unit.KB)},
				{startFileOffset: 3 * 128 * unit.KB, endFileOffset: 4 * 128 * unit.KB, actualDataSize: 128 * unit.KB, data: make([]byte, 128*unit.KB)},
				{startFileOffset: 4 * 128 * unit.KB, endFileOffset: 5 * 128 * unit.KB, actualDataSize: 1024, data: make([]byte, 128*unit.KB)},
			},
			expectedRemainBlocks: []uint64{0, 200 * unit.KB, 3 * 128 * unit.KB, 4 * 128 * unit.KB},
			expectedErr:          true,
			expectedRead:         128*unit.KB + 100*unit.KB,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inodeID := uint64(2)
			s := &Streamer{inode: inodeID, extents: NewExtentCache(inodeID)}
			s.extents.SetSize(tt.extentSize, false)
			actualData := randTestData(tt.dataTotalSize)
			for _, block := range tt.storeBlocks {
				block.inodeID = inodeID
				copy(block.data[:block.actualDataSize], actualData[block.startFileOffset:(block.startFileOffset+block.actualDataSize)])
				s.readAheadBlocks.Store(block.startFileOffset, block)
			}
			c := &ReadAheadController{windowSize: 2 * unit.MB, blockCntThreshold: 1024}
			readData := make([]byte, tt.readSize)
			read, hole, err := c.ReadFromBlocks(s, readData, tt.readOffset, tt.readSize)
			assert.Equalf(t, tt.expectedRead, read, "read size from blocks")
			assert.Falsef(t, hole, "data have no hole")
			assert.Equalf(t, tt.expectedErr, err != nil, "err(%v) should exist(%v)", err, tt.expectedErr)
			for i := 0; i < read; i++ {
				assert.Equalf(t, actualData[tt.readOffset+uint64(i)], readData[i], "data inconsistent at offset(%v) read from(%v)", i, tt.readOffset)
			}
			remainCnt := 0
			s.readAheadBlocks.Range(func(key, value interface{}) bool {
				remainCnt++
				assert.Containsf(t, tt.expectedRemainBlocks, key, "block should be remained")
				return true
			})
			assert.Equalf(t, len(tt.expectedRemainBlocks), remainCnt, "remain blocks count")
		})
	}
}

func TestReadAhead_EvictAfterTruncate(t *testing.T) {
	// create inode
	mw, ec, err := creatExtentClient()
	assert.NoErrorf(t, err, "init extent client err")
	defer func() {
		ec.Close(context.Background())
		log.LogFlush()
	}()

	var (
		inodeInfo *proto.InodeInfo
		read      int
		hole      bool
	)
	ec.dataWrapper.readAheadController, err = NewReadAheadController(ec.dataWrapper, 4096, DefaultReadAheadWindowMB*unit.MB)
	assert.NoErrorf(t, err, "init read ahead controller err")

	mw.Delete_ll(context.Background(), 1, "TestReadAhead_EvictAfterTruncate", false)
	inodeInfo, err = mw.Create_ll(context.Background(), 1, "TestReadAhead_EvictAfterTruncate", 0644, 0, 0, nil)
	assert.NoErrorf(t, err, "create file TestReadAhead_EvictAfterTruncate err")

	err = ec.OpenStream(inodeInfo.Inode, false, false)
	assert.NoErrorf(t, err, "open streamer of inode(%v)", inodeInfo.Inode)
	s := ec.GetStreamer(inodeInfo.Inode)
	assert.NotNilf(t, s, "get streamer of inode(%v)", inodeInfo.Inode)

	tests := []struct {
		name                  string
		truncateSize          uint64
		firstEvictBlockOffset uint64
		expectedBlockCount    int
	}{
		{
			name:                  "test01",
			truncateSize:          0,
			firstEvictBlockOffset: 0,
			expectedBlockCount:    0,
		},
		{
			name:                  "test02",
			truncateSize:          1234,
			firstEvictBlockOffset: 0,
			expectedBlockCount:    0,
		},
		{
			name:                  "test03",
			truncateSize:          128 * unit.KB,
			firstEvictBlockOffset: 128 * unit.KB,
			expectedBlockCount:    0,
		},
		{
			name:                  "test04",
			truncateSize:          128*unit.KB + 1234,
			firstEvictBlockOffset: 128 * unit.KB,
			expectedBlockCount:    0,
		},
		{
			name:                  "test05",
			truncateSize:          2 * 128 * unit.KB,
			firstEvictBlockOffset: 2 * 128 * unit.KB,
			expectedBlockCount:    1,
		},
		{
			name:                  "test06",
			truncateSize:          4*unit.MB - 1234,
			firstEvictBlockOffset: 4*unit.MB - 128*unit.KB,
			expectedBlockCount:    30,
		},
		{
			name:                  "test07",
			truncateSize:          4 * unit.MB,
			firstEvictBlockOffset: 4 * unit.MB,
			expectedBlockCount:    31,
		},
		{
			name:                  "test08",
			truncateSize:          5*unit.MB - 1234,
			firstEvictBlockOffset: 4 * unit.MB,
			expectedBlockCount:    31,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// write
			size := 128 * unit.KB
			for offset := uint64(0); offset < 4*unit.MB+100; offset += uint64(size) {
				data := randTestData(size)
				_, _, err = ec.Write(context.Background(), inodeInfo.Inode, offset, data, false)
				assert.NoErrorf(t, err, "write file ino(%v) offset(%v) size(%v) err", inodeInfo.Inode, offset, size)
			}
			// read
			readOffset := uint64(10)
			readSize := int(1024)
			readData := make([]byte, readSize)
			read, hole, err = ec.Read(context.Background(), inodeInfo.Inode, readData, readOffset, readSize)
			assert.Equalf(t, readSize, read, "first read size")
			assert.Falsef(t, hole, "read ahead hole")
			assert.NoErrorf(t, err, "first read should success")
			// 等待预读
			time.Sleep(500 * time.Millisecond)
			assert.Equalf(t, int64(31), ec.dataWrapper.readAheadController.blockCount, "read ahead block count")
			assert.Equalf(t, int(31), ec.dataWrapper.readAheadController.blockLruList.Len(), "read ahead lru list length")

			err = ec.Truncate(context.Background(), inodeInfo.Inode, 0, tt.truncateSize)
			assert.NoErrorf(t, err, "truncate to size(%v)", tt.truncateSize)

			for startOffset := uint64(128 * unit.KB); startOffset < tt.firstEvictBlockOffset; startOffset += readAheadBlockSize {
				value, ok := s.readAheadBlocks.Load(startOffset)
				assert.Truef(t, ok, "block(%v) at offset(%v) should be reserved before truncate size(%v)", value, startOffset, tt.truncateSize)
				assert.NotZerof(t, value.(*ReadAheadBlock).actualDataSize, "block(%v) at offset(%v) should have data but actual size is 0", value, startOffset)
			}
			for startOffset := tt.firstEvictBlockOffset; startOffset < 4*unit.MB+100; startOffset += readAheadBlockSize {
				value, ok := s.readAheadBlocks.Load(startOffset)
				assert.Falsef(t, ok, "block(%v) at offset(%v) should be evicted before truncate size(%v)", value, startOffset, tt.truncateSize)
			}
			assert.Equalf(t, int64(tt.expectedBlockCount), ec.dataWrapper.readAheadController.blockCount, "read ahead block count")
			assert.Equalf(t, tt.expectedBlockCount, ec.dataWrapper.readAheadController.blockLruList.Len(), "read ahead lru list length")
		})
	}

}

func TestReadAhead_EvictBlocksAtOffset(t *testing.T) {
	defer func() {
		log.LogFlush()
	}()

	tests := []struct{
		name			string
		writeOff		uint64
		writeSize		int
		storeBlocks		[]*ReadAheadBlock
		expectBlocks	[]uint64
	}{
		{
			name:      "test01",
			writeOff:  0,
			writeSize: 1024,
			storeBlocks: []*ReadAheadBlock{
				{startFileOffset: 0, actualDataSize: 1024},
				{startFileOffset: 20 * unit.KB, actualDataSize: 1024},
				{startFileOffset: 100 * unit.KB, actualDataSize: 1024},
			},
			expectBlocks: []uint64{20 * unit.KB, 100 * unit.KB},
		},
		{
			name:      "test02",
			writeOff:  0,
			writeSize: 128 * unit.KB,
			storeBlocks: []*ReadAheadBlock{
				{startFileOffset: 0, actualDataSize: 128 * unit.KB},
				{startFileOffset: 128 * unit.KB, actualDataSize: 128 * unit.KB},
				{startFileOffset: 256 * unit.KB, actualDataSize: 128 * unit.KB},
			},
			expectBlocks: []uint64{128 * unit.KB, 256 * unit.KB},
		},
		{
			name:      "test03",
			writeOff:  0,
			writeSize: 129 * unit.KB,
			storeBlocks: []*ReadAheadBlock{
				{startFileOffset: 0, actualDataSize: 120 * unit.KB},
				{startFileOffset: 128 * unit.KB, actualDataSize: 200 * unit.KB},
				{startFileOffset: 256 * unit.KB, actualDataSize: 128 * unit.KB},
			},
			expectBlocks: []uint64{256 * unit.KB},
		},
		{
			name:      "test04",
			writeOff:  0,
			writeSize: 260 * unit.KB,
			storeBlocks: []*ReadAheadBlock{
				{startFileOffset: 0, actualDataSize: 120 * unit.KB},
				{startFileOffset: 128 * unit.KB, actualDataSize: 200 * unit.KB},
				{startFileOffset: 256 * unit.KB, actualDataSize: 1 * unit.KB},
			},
			expectBlocks: []uint64{},
		},
		{
			name:      "test05",
			writeOff:  1024,
			writeSize: 1 * unit.KB,
			storeBlocks: []*ReadAheadBlock{
				{startFileOffset: 0, actualDataSize: 120 * unit.KB},
				{startFileOffset: 128 * unit.KB, actualDataSize: 200 * unit.KB},
				{startFileOffset: 256 * unit.KB, actualDataSize: 1 * unit.KB},
			},
			expectBlocks: []uint64{128 * unit.KB, 256 * unit.KB},
		},
		{
			name:      "test06",
			writeOff:  1024,
			writeSize: 128*unit.KB - 1024,
			storeBlocks: []*ReadAheadBlock{
				{startFileOffset: 0, actualDataSize: 128 * unit.KB},
				{startFileOffset: 128 * unit.KB, actualDataSize: 200 * unit.KB},
				{startFileOffset: 256 * unit.KB, actualDataSize: 1 * unit.KB},
			},
			expectBlocks: []uint64{128 * unit.KB, 256 * unit.KB},
		},
		{
			name:      "test07",
			writeOff:  1024,
			writeSize: 128 * unit.KB,
			storeBlocks: []*ReadAheadBlock{
				{startFileOffset: 128 * unit.KB, actualDataSize: 128 * unit.KB},
				{startFileOffset: 256 * unit.KB, actualDataSize: 1 * unit.KB},
			},
			expectBlocks: []uint64{256 * unit.KB},
		},
		{
			name:      "test08",
			writeOff:  1024,
			writeSize: 256 * unit.KB,
			storeBlocks: []*ReadAheadBlock{
				{startFileOffset: 0 * unit.KB, actualDataSize: 300 * unit.KB},
				{startFileOffset: 0 * unit.KB, actualDataSize: 200 * unit.KB},
				{startFileOffset: 0 * unit.KB, actualDataSize: 257 * unit.KB},
				{startFileOffset: 256 * unit.KB, actualDataSize: 1 * unit.KB},
				{startFileOffset: 512 * unit.KB, actualDataSize: 128 * unit.KB},
			},
			expectBlocks: []uint64{512 * unit.KB},
		},
		{
			name:      "test09",
			writeOff:  128 * unit.KB,
			writeSize: 256,
			storeBlocks: []*ReadAheadBlock{
				{startFileOffset: 0 * unit.KB, actualDataSize: 64 * unit.KB},
				{startFileOffset: 128 * unit.KB, actualDataSize: 500 * unit.KB},
				{startFileOffset: 256 * unit.KB, actualDataSize: 1 * unit.KB},
				{startFileOffset: 512 * unit.KB, actualDataSize: 128 * unit.KB},
			},
			expectBlocks: []uint64{0, 256 * unit.KB, 512 * unit.KB},
		},
		{
			name:      "test10",
			writeOff:  128 * unit.KB,
			writeSize: 128 * unit.KB,
			storeBlocks: []*ReadAheadBlock{
				{startFileOffset: 0 * unit.KB, actualDataSize: 128 * unit.KB},
				{startFileOffset: 128 * unit.KB, actualDataSize: 1 * unit.KB},
				{startFileOffset: 256 * unit.KB, actualDataSize: 1 * unit.KB},
				{startFileOffset: 512 * unit.KB, actualDataSize: 128 * unit.KB},
			},
			expectBlocks: []uint64{0, 256 * unit.KB, 512 * unit.KB},
		},
		{
			name:      "test11",
			writeOff:  128 * unit.KB,
			writeSize: 256 * unit.KB,
			storeBlocks: []*ReadAheadBlock{
				{startFileOffset: 0 * unit.KB, actualDataSize: 128 * unit.KB},
				{startFileOffset: 120 * unit.KB, actualDataSize: 10 * unit.KB},
				{startFileOffset: 200 * unit.KB, actualDataSize: 500 * unit.KB},
				{startFileOffset: 384 * unit.KB, actualDataSize: 1 * unit.KB},
				{startFileOffset: 512 * unit.KB, actualDataSize: 128 * unit.KB},
			},
			expectBlocks: []uint64{0, 384 * unit.KB, 512 * unit.KB},
		},
		{
			name:      "test12",
			writeOff:  128*unit.KB + 1024,
			writeSize: 256 * unit.KB,
			storeBlocks: []*ReadAheadBlock{
				{startFileOffset: 0 * unit.KB, actualDataSize: 128 * unit.KB},
				{startFileOffset: 128 * unit.KB, actualDataSize: 10 * unit.KB},
				{startFileOffset: 150 * unit.KB, actualDataSize: 10 * unit.KB},
				{startFileOffset: 200 * unit.KB, actualDataSize: 500 * unit.KB},
				{startFileOffset: 384 * unit.KB, actualDataSize: 1 * unit.KB},
				{startFileOffset: 512 * unit.KB, actualDataSize: 128 * unit.KB},
			},
			expectBlocks: []uint64{0, 512 * unit.KB},
		},
	}
	w := &Wrapper{getStreamerFunc: func(inode uint64) *Streamer {
		return &Streamer{}
	}}
	controller, err := NewReadAheadController(w, 1024, DefaultReadAheadWindowMB)
	assert.NoErrorf(t, err, "init read ahead controller")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Streamer{}
			for _, block := range tt.storeBlocks {
				s.readAheadBlocks.Store(block.startFileOffset, block)
			}
			controller.EvictBlocksAtOffset(s, tt.writeOff, tt.writeSize)
			count := 0
			s.readAheadBlocks.Range(func(key, value interface{}) bool {
				count++
				assert.Containsf(t, tt.expectBlocks, key.(uint64), "blocks should contain")
				return true
			})
			assert.Equalf(t, len(tt.expectBlocks), count, "expect blocks count")
		})
	}
}

func TestReadAhead_prepareReadAhead(t *testing.T) {
	defer func() {
		log.LogFlush()
	}()

	tests := []struct {
		name               string
		windowSize         uint64
		inodeSize          uint64
		readOff            uint64
		expectPrepare      bool
		expectPrepareBlock *ReadAheadBlock
	}{
		{
			name:               "test01",
			windowSize:         DefaultReadAheadWindowMB * unit.MB,
			inodeSize:          1 * unit.MB,
			readOff:            0,
			expectPrepare:      false,
			expectPrepareBlock: nil,
		},
		{
			name:               "test02",
			windowSize:         DefaultReadAheadWindowMB * unit.MB,
			inodeSize:          DefaultReadAheadWindowMB * unit.MB,
			readOff:            1000,
			expectPrepare:      false,
			expectPrepareBlock: nil,
		},
		{
			name:               "test03",
			windowSize:         DefaultReadAheadWindowMB * unit.MB,
			inodeSize:          3*unit.MB + 1234,
			readOff:            1234,
			expectPrepare:      true,
			expectPrepareBlock: &ReadAheadBlock{startFileOffset: 128 * unit.KB, endFileOffset: 3*unit.MB + 1234},
		},
		{
			name:               "test04",
			windowSize:         DefaultReadAheadWindowMB * unit.MB,
			inodeSize:          3*unit.MB + 1234,
			readOff:            128 * unit.KB,
			expectPrepare:      true,
			expectPrepareBlock: &ReadAheadBlock{startFileOffset: 128 * unit.KB, endFileOffset: 3*unit.MB + 1234},
		},
		{
			name:               "test05",
			windowSize:         DefaultReadAheadWindowMB * unit.MB,
			inodeSize:          4*unit.MB + 1234,
			readOff:            DefaultReadAheadWindowMB * unit.MB,
			expectPrepare:      true,
			expectPrepareBlock: &ReadAheadBlock{startFileOffset: 2 * DefaultReadAheadWindowMB * unit.MB, endFileOffset: 4*unit.MB + 1234},
		},
		{
			name:               "test06",
			windowSize:         DefaultReadAheadWindowMB * unit.MB,
			inodeSize:          3*unit.MB + 1234,
			readOff:            DefaultReadAheadWindowMB * unit.MB,
			expectPrepare:      false,
			expectPrepareBlock: nil,
		},
		{
			name:               "test07",
			windowSize:         DefaultReadAheadWindowMB * unit.MB,
			inodeSize:          3*unit.MB + 1234,
			readOff:            4 * unit.MB,
			expectPrepare:      false,
			expectPrepareBlock: nil,
		},
		{
			name:               "test08",
			windowSize:         DefaultReadAheadWindowMB * unit.MB,
			inodeSize:          3*unit.MB + 1234,
			readOff:            4 * unit.MB,
			expectPrepare:      false,
			expectPrepareBlock: nil,
		},
		{
			name:               "test09",
			windowSize:         DefaultReadAheadWindowMB * unit.MB,
			inodeSize:          2 * DefaultReadAheadWindowMB * unit.MB,
			readOff:            DefaultReadAheadWindowMB*unit.MB + 1234,
			expectPrepare:      false,
			expectPrepareBlock: nil,
		},
		{
			name:               "test10",
			windowSize:         DefaultReadAheadWindowMB * unit.MB,
			inodeSize:          5 * unit.MB,
			readOff:            DefaultReadAheadWindowMB*unit.MB - 1234,
			expectPrepare:      true,
			expectPrepareBlock: &ReadAheadBlock{startFileOffset: 128 * unit.KB, endFileOffset: 2 * DefaultReadAheadWindowMB * unit.MB},
		},
		{
			name:               "test11",
			windowSize:         DefaultReadAheadWindowMB * unit.MB,
			inodeSize:          5*unit.MB + 123,
			readOff:            DefaultReadAheadWindowMB*unit.MB + 1234,
			expectPrepare:      true,
			expectPrepareBlock: &ReadAheadBlock{startFileOffset: 2 * DefaultReadAheadWindowMB * unit.MB, endFileOffset: 5*unit.MB + 123},
		},
		{
			name:               "test12",
			windowSize:         DefaultReadAheadWindowMB * unit.MB,
			inodeSize:          DefaultReadAheadWindowMB * unit.MB,
			readOff:            DefaultReadAheadWindowMB * unit.MB,
			expectPrepare:      false,
			expectPrepareBlock: nil,
		},
		{
			name:               "test13",
			windowSize:         DefaultReadAheadWindowMB * unit.MB,
			inodeSize:          6 * unit.MB,
			readOff:            DefaultReadAheadWindowMB * unit.MB,
			expectPrepare:      true,
			expectPrepareBlock: &ReadAheadBlock{startFileOffset: 2 * DefaultReadAheadWindowMB * unit.MB, endFileOffset: 3 * DefaultReadAheadWindowMB * unit.MB},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			controller := &ReadAheadController{windowSize: tt.windowSize, prepareChan: make(chan *ReadAheadBlock, 1024)}
			s := &Streamer{inode: 2, extents: NewExtentCache(2)}
			s.extents.SetSize(tt.inodeSize, true)
			preparing := controller.prepareReadAhead(s, tt.readOff)
			assert.Equalf(t, tt.expectPrepare, preparing, "trigger prepare blocks")
			if preparing {
				assert.Equalf(t, 1, len(controller.prepareChan), "prepare channel length should be 1")
				prepareBlock := <-controller.prepareChan
				assert.Equalf(t, s.inode, prepareBlock.inodeID, "inode of prepare block")
				assert.Equalf(t, tt.expectPrepareBlock.startFileOffset, prepareBlock.startFileOffset, "start offset of prepare block")
				assert.Equalf(t, tt.expectPrepareBlock.endFileOffset, prepareBlock.endFileOffset, "end offset of prepare block")
			} else {
				assert.Zerof(t, len(controller.prepareChan), "prepare channel length should be 0")
			}
		})
	}
}

func TestReadAhead_splitReadAheadBlock(t *testing.T) {
	defer func() {
		log.LogFlush()
	}()

	tests := []struct {
		name              string
		prepareBlock      *ReadAheadBlock
		expectSplitBlocks []*ReadAheadBlock
	}{
		{
			name:         "test01",
			prepareBlock: &ReadAheadBlock{inodeID: 5, startFileOffset: 128 * unit.KB, endFileOffset: 1 * unit.MB},
			expectSplitBlocks: []*ReadAheadBlock{
				{inodeID: 5, startFileOffset: 128 * unit.KB, endFileOffset: 2 * 128 * unit.KB},
				{inodeID: 5, startFileOffset: 2 * 128 * unit.KB, endFileOffset: 3 * 128 * unit.KB},
				{inodeID: 5, startFileOffset: 3 * 128 * unit.KB, endFileOffset: 4 * 128 * unit.KB},
				{inodeID: 5, startFileOffset: 4 * 128 * unit.KB, endFileOffset: 5 * 128 * unit.KB},
				{inodeID: 5, startFileOffset: 5 * 128 * unit.KB, endFileOffset: 6 * 128 * unit.KB},
				{inodeID: 5, startFileOffset: 6 * 128 * unit.KB, endFileOffset: 7 * 128 * unit.KB},
				{inodeID: 5, startFileOffset: 7 * 128 * unit.KB, endFileOffset: 8 * 128 * unit.KB},
			},
		},
		{
			name:         "test02",
			prepareBlock: &ReadAheadBlock{inodeID: 5, startFileOffset: 0, endFileOffset: 128*unit.KB - 1000},
			expectSplitBlocks: []*ReadAheadBlock{
				{inodeID: 5, startFileOffset: 0, endFileOffset: 128*unit.KB - 1000},
			},
		},
		{
			name:         "test03",
			prepareBlock: &ReadAheadBlock{inodeID: 5, startFileOffset: 0, endFileOffset: 128 * unit.KB},
			expectSplitBlocks: []*ReadAheadBlock{
				{inodeID: 5, startFileOffset: 0, endFileOffset: 128 * unit.KB},
			},
		},
		{
			name:         "test04",
			prepareBlock: &ReadAheadBlock{inodeID: 5, startFileOffset: 1000, endFileOffset: 128*unit.KB + 2000},
			expectSplitBlocks: []*ReadAheadBlock{
				{inodeID: 5, startFileOffset: 1000, endFileOffset: 128*unit.KB + 1000},
				{inodeID: 5, startFileOffset: 128*unit.KB + 1000, endFileOffset: 128*unit.KB + 2000},
			},
		},
		{
			name:         "test05",
			prepareBlock: &ReadAheadBlock{inodeID: 5, startFileOffset: 1000, endFileOffset: 128 * unit.KB},
			expectSplitBlocks: []*ReadAheadBlock{
				{inodeID: 5, startFileOffset: 1000, endFileOffset: 128 * unit.KB},
			},
		},
		{
			name:         "test06",
			prepareBlock: &ReadAheadBlock{inodeID: 5, startFileOffset: 128 * unit.KB, endFileOffset: 129 * unit.KB},
			expectSplitBlocks: []*ReadAheadBlock{
				{inodeID: 5, startFileOffset: 128 * unit.KB, endFileOffset: 129 * unit.KB},
			},
		},
		{
			name:         "test07",
			prepareBlock: &ReadAheadBlock{inodeID: 5, startFileOffset: 130 * unit.KB, endFileOffset: 400 * unit.KB},
			expectSplitBlocks: []*ReadAheadBlock{
				{inodeID: 5, startFileOffset: 130 * unit.KB, endFileOffset: 258 * unit.KB},
				{inodeID: 5, startFileOffset: 258 * unit.KB, endFileOffset: 386 * unit.KB},
				{inodeID: 5, startFileOffset: 386 * unit.KB, endFileOffset: 400 * unit.KB},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			controller := &ReadAheadController{blockChan: make(chan *ReadAheadBlock, 1024)}
			controller.splitReadAheadBlock(tt.prepareBlock)
			assert.Equalf(t, len(tt.expectSplitBlocks), len(controller.blockChan), "block count in channel")
			for i := 0; i < len(tt.expectSplitBlocks); i++ {
				block := <-controller.blockChan
				assert.Equalf(t, tt.expectSplitBlocks[i].inodeID, block.inodeID, "block inode")
				assert.Equalf(t, tt.expectSplitBlocks[i].startFileOffset, block.startFileOffset, "block start offset")
				assert.Equalf(t, tt.expectSplitBlocks[i].endFileOffset, block.endFileOffset, "block end offset")
			}
		})
	}
}

func TestReadAhead_doReadAhead(t *testing.T) {
	// create inode
	mw, ec, err := creatExtentClient()
	assert.NoErrorf(t, err, "init extent client err")
	defer func() {
		ec.Close(context.Background())
		log.LogFlush()
	}()

	var (
		inodeInfo *proto.InodeInfo
	)
	ec.dataWrapper.readAheadController, err = NewReadAheadController(ec.dataWrapper, 4096, DefaultReadAheadWindowMB*unit.MB)
	assert.NoErrorf(t, err, "init read ahead controller err")

	mw.Delete_ll(context.Background(), 1, "TestReadAhead_doReadAhead", false)
	inodeInfo, err = mw.Create_ll(context.Background(), 1, "TestReadAhead_doReadAhead", 0644, 0, 0, nil)
	assert.NoErrorf(t, err, "create file TestReadAhead_doReadAhead err")

	err = ec.OpenStream(inodeInfo.Inode, false, false)
	assert.NoErrorf(t, err, "open streamer of inode(%v)", inodeInfo.Inode)
	s := ec.GetStreamer(inodeInfo.Inode)
	assert.NotNilf(t, s, "get streamer of inode(%v)", inodeInfo.Inode)

	size := 3*128*unit.KB + 1234
	fileData := randTestData(size)
	_, _, err = ec.Write(context.Background(), inodeInfo.Inode, 0, fileData, false)
	assert.NoErrorf(t, err, "write file ino(%v) offset(%v) size(%v) err", inodeInfo.Inode, 0, size)

	tests := []struct {
		name          string
		readBlock     *ReadAheadBlock
		expectedBlock *ReadAheadBlock
		expectedErr   bool
	}{
		{
			name:      "test01",
			readBlock: &ReadAheadBlock{startFileOffset: 0, endFileOffset: 128 * unit.KB},
			expectedBlock: &ReadAheadBlock{
				startFileOffset: 0,
				endFileOffset:   128 * unit.KB,
				actualDataSize:  128 * unit.KB,
				data:            make([]byte, 128*unit.KB),
			},
			expectedErr: false,
		},
		{
			name:      "test02",
			readBlock: &ReadAheadBlock{startFileOffset: 4 * 128 * unit.KB, endFileOffset: 5 * 128 * unit.KB},
			expectedBlock: &ReadAheadBlock{
				startFileOffset: 4 * 128 * unit.KB,
				endFileOffset:   5 * 128 * unit.KB,
				actualDataSize:  0,
			},
			expectedErr: true,
		},
		{
			name:      "test03",
			readBlock: &ReadAheadBlock{startFileOffset: 3 * 128 * unit.KB, endFileOffset: 4 * 128 * unit.KB},
			expectedBlock: &ReadAheadBlock{
				startFileOffset: 3 * 128 * unit.KB,
				endFileOffset:   4 * 128 * unit.KB,
				actualDataSize:  1234,
				data:            make([]byte, 128*unit.KB),
			},
			expectedErr: false,
		},
		{
			name:      "test04",
			readBlock: &ReadAheadBlock{startFileOffset: 2*128*unit.KB + 1234, endFileOffset: 3*128*unit.KB + 1234},
			expectedBlock: &ReadAheadBlock{
				startFileOffset: 2*128*unit.KB + 1234,
				endFileOffset:   3*128*unit.KB + 1234,
				actualDataSize:  128 * unit.KB,
				data:            make([]byte, 128*unit.KB),
			},
			expectedErr: false,
		},
		{
			name:      "test05",
			readBlock: &ReadAheadBlock{startFileOffset: 3*128*unit.KB + 1234, endFileOffset: 4*128*unit.KB + 1234},
			expectedBlock: &ReadAheadBlock{
				startFileOffset: 3*128*unit.KB + 1234,
				endFileOffset:   4*128*unit.KB + 1234,
				actualDataSize:  0,
			},
			expectedErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			controller := &ReadAheadController{blockLruList: list.New()}
			err := controller.doReadAhead(context.Background(), s, tt.readBlock)
			assert.Equalf(t, tt.expectedErr, err != nil, "read ahead err(%v) should exist(%v)", err, tt.expectedErr)
			assert.Equalf(t, tt.expectedBlock.startFileOffset, tt.readBlock.startFileOffset, "block start offset")
			assert.Equalf(t, tt.expectedBlock.endFileOffset, tt.readBlock.endFileOffset, "block end offset")
			assert.Equalf(t, tt.expectedBlock.actualDataSize, tt.readBlock.actualDataSize, "block actual size")
			assert.Equalf(t, len(tt.expectedBlock.data), len(tt.readBlock.data), "block data size")
			for i := uint64(0); i < tt.expectedBlock.actualDataSize; i++ {
				assert.Equalf(t, fileData[tt.readBlock.startFileOffset+i], tt.readBlock.data[i], "file data at offset(%v) from block(%v)", i, tt.readBlock)
			}
		})
	}
}

type readAheadConfigRule struct {
	source         string
	memMB          int64
	windowMB       int64
	expectMemMB    int64
	expectWindowMB int64
	isErr          bool
}

func TestReadAhead_Config(t *testing.T) {
	defer func() {
		log.LogFlush()
	}()

	Remote_Config := "remote"
	Local_Config := "local"
	tests := []struct {
		name        string
		configRules []*readAheadConfigRule
	}{
		{
			name: "test01",
			configRules: []*readAheadConfigRule{
				{source: Local_Config, memMB: 0, windowMB: 0, expectMemMB: 0, expectWindowMB: 0, isErr: false},                                 // 无操作
				{source: Local_Config, memMB: -1, windowMB: -1, expectMemMB: 0, expectWindowMB: 0, isErr: false},                               // 无操作
				{source: Local_Config, memMB: 0, windowMB: 0, expectMemMB: 0, expectWindowMB: 0, isErr: false},                                 // 无操作
				{source: Local_Config, memMB: 0, windowMB: 1, expectMemMB: 0, expectWindowMB: 0, isErr: false},                                 // 无操作
				{source: Local_Config, memMB: 10, windowMB: 0, expectMemMB: 10, expectWindowMB: int64(DefaultReadAheadWindowMB), isErr: false}, // 初始化
				{source: Remote_Config, memMB: 5, windowMB: 1, expectMemMB: 10, expectWindowMB: 1, isErr: false},                               // local优先
				{source: Local_Config, memMB: -1, windowMB: -1, expectMemMB: 5, expectWindowMB: 1, isErr: false},                               // local取消设置，改用remote设置值
				{source: Remote_Config, memMB: 0, windowMB: -1, expectMemMB: 5, expectWindowMB: int64(DefaultReadAheadWindowMB), isErr: false}, // remote window取消，设置为默认值
				{source: Remote_Config, memMB: -1, windowMB: 5, expectMemMB: 0, expectWindowMB: 5, isErr: false},                               // 关闭预读
			},
		},
		{
			name: "test02",
			configRules: []*readAheadConfigRule{
				{source: Local_Config, memMB: 10, windowMB: 1, expectMemMB: 10, expectWindowMB: 1, isErr: false},
				{source: Local_Config, memMB: 0, windowMB: 1, expectMemMB: 10, expectWindowMB: 1, isErr: false},
				{source: Local_Config, memMB: 5, windowMB: 0, expectMemMB: 5, expectWindowMB: 1, isErr: false},
				{source: Local_Config, memMB: -1, windowMB: -1, expectMemMB: 0, expectWindowMB: int64(DefaultReadAheadWindowMB), isErr: false}, // 取消
				{source: Local_Config, memMB: 10, windowMB: -1, expectMemMB: 10, expectWindowMB: int64(DefaultReadAheadWindowMB), isErr: false},
				{source: Local_Config, memMB: -1, windowMB: 10, expectMemMB: 0, expectWindowMB: 10, isErr: false}, // 取消
				{source: Local_Config, memMB: 10, windowMB: 0, expectMemMB: 10, expectWindowMB: 10, isErr: false},
				{source: Remote_Config, memMB: 5, windowMB: 1, expectMemMB: 10, expectWindowMB: 10, isErr: false},
				{source: Local_Config, memMB: 0, windowMB: -1, expectMemMB: 10, expectWindowMB: 1, isErr: false}, // local取消window设置，改用remote设置值
			},
		},
		{ // 非法值
			name: "test03",
			configRules: []*readAheadConfigRule{
				{source: Remote_Config, memMB: 5, windowMB: 0, expectMemMB: 5, expectWindowMB: int64(DefaultReadAheadWindowMB), isErr: false},
				{source: Local_Config, memMB: 102400, windowMB: -1, expectMemMB: 5, expectWindowMB: int64(DefaultReadAheadWindowMB), isErr: true},
				{source: Local_Config, memMB: 102400, windowMB: 0, expectMemMB: 5, expectWindowMB: int64(DefaultReadAheadWindowMB), isErr: true},
				{source: Local_Config, memMB: 102400, windowMB: 1, expectMemMB: 5, expectWindowMB: int64(DefaultReadAheadWindowMB), isErr: true},
				{source: Local_Config, memMB: -1, windowMB: 1024, expectMemMB: 5, expectWindowMB: int64(DefaultReadAheadWindowMB), isErr: true},
				{source: Local_Config, memMB: 0, windowMB: 1024, expectMemMB: 5, expectWindowMB: int64(DefaultReadAheadWindowMB), isErr: true},
				{source: Local_Config, memMB: 1, windowMB: 1024, expectMemMB: 5, expectWindowMB: int64(DefaultReadAheadWindowMB), isErr: true},
				{source: Local_Config, memMB: 102400, windowMB: 1024, expectMemMB: 5, expectWindowMB: int64(DefaultReadAheadWindowMB), isErr: true},
				{source: Remote_Config, memMB: 102400, windowMB: -1, expectMemMB: 5, expectWindowMB: int64(DefaultReadAheadWindowMB), isErr: true},
				{source: Remote_Config, memMB: 102400, windowMB: 0, expectMemMB: 5, expectWindowMB: int64(DefaultReadAheadWindowMB), isErr: true},
				{source: Remote_Config, memMB: 102400, windowMB: 1, expectMemMB: 5, expectWindowMB: int64(DefaultReadAheadWindowMB), isErr: true},
				{source: Remote_Config, memMB: -1, windowMB: 1024, expectMemMB: 5, expectWindowMB: int64(DefaultReadAheadWindowMB), isErr: true},
				{source: Remote_Config, memMB: 0, windowMB: 1024, expectMemMB: 5, expectWindowMB: int64(DefaultReadAheadWindowMB), isErr: true},
				{source: Remote_Config, memMB: 1, windowMB: 1024, expectMemMB: 5, expectWindowMB: int64(DefaultReadAheadWindowMB), isErr: true},
				{source: Remote_Config, memMB: 102400, windowMB: 1024, expectMemMB: 5, expectWindowMB: int64(DefaultReadAheadWindowMB), isErr: true},
			},
		},
	}
	for _, tt := range tests {
		lastLocalMemMB, lastLocalWinMB := int64(0), int64(0)
		lastRemoteMemMB, lastRemoteWinMB := int64(0), int64(0)
		t.Run(tt.name, func(t *testing.T) {
			wrapper := &Wrapper{volName: "test", getStreamerFunc: func(inode uint64) *Streamer {
				return &Streamer{inode: inode}
			}}
			for _, rule := range tt.configRules {
				var err error
				if rule.source == Local_Config {
					err = wrapper.updateReadAheadLocalConfig(rule.memMB, rule.windowMB)
				}
				if rule.source == Remote_Config {
					err = wrapper.updateReadAheadRemoteConfig(rule.memMB, rule.windowMB)
				}
				if rule.isErr {
					assert.Errorf(t, err, "config rule(%v) should fail", rule)
				} else {
					assert.NoErrorf(t, err, "config rule(%v) should success", rule)
					if wrapper.readAheadController != nil && rule.source == Local_Config {
						if rule.memMB != 0 {
							lastLocalMemMB = rule.memMB
						}
						if rule.windowMB != 0 {
							lastLocalWinMB = rule.windowMB
						}
					} else if wrapper.readAheadController != nil && rule.source == Remote_Config {
						lastRemoteMemMB = rule.memMB
						lastRemoteWinMB = rule.windowMB
					}
				}
				assert.Equalf(t, rule.expectMemMB, wrapper.readAheadController.getMemoryMB(), "expect memMB after rule(%v)", rule)
				assert.Equalf(t, rule.expectWindowMB, wrapper.readAheadController.getWindowMB(), "expect windowMB after rule(%v)", rule)

				if wrapper.readAheadController != nil {
					assert.Equalf(t, lastLocalMemMB, wrapper.readAheadController.localMemMB, "expect local memMB after rule(%v)", rule)
					assert.Equalf(t, lastLocalWinMB, wrapper.readAheadController.localWindowMB, "expect local windowMB after rule(%v)", rule)
					assert.Equalf(t, lastRemoteMemMB, wrapper.readAheadController.remoteMemMB, "expect remote memMB after rule(%v)", rule)
					assert.Equalf(t, lastRemoteWinMB, wrapper.readAheadController.remoteWindowMB, "expect remote windowMB after rule(%v)", rule)
				}

			}
		})
	}

}
