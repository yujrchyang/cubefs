package storage

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"github.com/cubefs/cubefs/util/log"
	"github.com/stretchr/testify/assert"
	"hash/crc32"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"regexp"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/async"
	"github.com/cubefs/cubefs/util/testutil"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

const (
	testInode     = 1024
	cacheCapacity = 10
)

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func createFile(name string, data []byte) (err error) {
	os.RemoveAll(name)
	fp, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE, 0777)
	if err != nil {
		return
	}
	fp.Write(data)
	fp.Close()
	return
}

func removeFile(name string) {
	os.RemoveAll(name)
}

func computeMd5(data []byte) string {
	h := md5.New()
	h.Write(data)
	return hex.EncodeToString(h.Sum(nil))
}

func compareMd5(s *ExtentStore, extent, offset, size uint64, expectMD5 string) (err error) {
	actualMD5, err := s.ComputeMd5Sum(extent, offset, size)
	if err != nil {
		return fmt.Errorf("ComputeMd5Sum failed on extent(%v),"+
			"offset(%v),size(%v) expect(%v) actual(%v) err(%v)", extent, offset, size, expectMD5, actualMD5, err)
	}
	if actualMD5 != expectMD5 {
		return fmt.Errorf("ComputeMd5Sum failed on extent(%v),"+
			"offset(%v),size(%v) expect(%v) actual(%v) err(%v)", extent, offset, size, expectMD5, actualMD5, err)
	}
	return nil
}

func TestExtentStore_ComputeMd5Sum(t *testing.T) {
	allData := []byte(RandStringRunes(100 * 1024))
	allMd5 := computeMd5(allData)
	dataPath := "/tmp"
	extentID := 3675
	err := createFile(path.Join(dataPath, strconv.Itoa(extentID)), allData)
	if err != nil {
		t.Logf("createFile failed %v", err)
		t.FailNow()
	}
	s := new(ExtentStore)
	s.dataPath = dataPath
	err = compareMd5(s, uint64(extentID), 0, 0, allMd5)
	if err != nil {
		t.Logf("compareMd5 failed %v", err)
		t.FailNow()
	}

	for i := 0; i < 100; i++ {
		data := allData[i*1024 : (i+1)*1024]
		expectMD5 := computeMd5(data)
		err = compareMd5(s, uint64(extentID), uint64(i*1024), 1024, expectMD5)
		if err != nil {
			t.Logf("compareMd5 failed %v", err)
			t.FailNow()
		}
	}
	removeFile(path.Join(dataPath, strconv.Itoa(extentID)))

}

func TestExtentStore_PlaybackTinyDelete(t *testing.T) {
	const (
		testStoreSize          int    = 128849018880 // 120GB
		testStoreCacheCapacity int    = 1
		testPartitionID        uint64 = 1
		testTinyExtentID       uint64 = 1
		testTinyFileCount      int    = 1024
		testTinyFileSize       int    = PageSize
	)
	var baseTestPath = testutil.InitTempTestPath(t)
	t.Log(baseTestPath.Path())
	defer func() {
		baseTestPath.Cleanup()
	}()

	var (
		err error
	)

	var store *ExtentStore
	if store, err = NewExtentStore(baseTestPath.Path(), testPartitionID, testStoreSize, testStoreCacheCapacity, nil, false, IOInterceptors{}); err != nil {
		t.Fatalf("init test store failed: %v", err)
	}
	// 准备小文件数据，向TinyExtent 1写入1024个小文件数据, 每个小文件size为1024.
	var (
		tinyFileData    = make([]byte, testTinyFileSize)
		testFileDataCrc = crc32.ChecksumIEEE(tinyFileData[:testTinyFileSize])
		holes           = make([]*proto.TinyExtentHole, 0)
	)
	for i := 0; i < testTinyFileCount; i++ {
		off := int64(i * PageSize)
		size := int64(testTinyFileSize)
		if err = store.Write(testTinyExtentID, off, size,
			tinyFileData[:testTinyFileSize], testFileDataCrc, Append, false); err != nil {
			t.Fatalf("prepare tiny data [index: %v, off: %v, size: %v] failed: %v", i, off, size, err)
		}
		if i%2 == 0 {
			continue
		}
		if err = store.RecordTinyDelete(testTinyExtentID, off, size); err != nil {
			t.Fatalf("write tiny delete record [index: %v, off: %v, size: %v] failed: %v", i, off, size, err)
		}
		holes = append(holes, &proto.TinyExtentHole{
			Offset: uint64(off),
			Size:   uint64(size),
		})
	}
	// 执行TinyDelete回放
	if err = store.PlaybackTinyDelete(0); err != nil {
		t.Fatalf("playback tiny delete failed: %v", err)
	}
	// 验证回放后测试TinyExtent的洞是否可预期一致
	var (
		testTinyExtent *Extent
		newOffset      int64
	)
	if testTinyExtent, err = store.extentWithHeaderByExtentID(testTinyExtentID); err != nil {
		t.Fatalf("load test tiny extent %v failed: %v", testTinyExtentID, err)
	}
	for _, hole := range holes {
		newOffset, _, err = testTinyExtent.tinyExtentAvaliOffset(int64(hole.Offset))
		if err != nil && strings.Contains(err.Error(), syscall.ENXIO.Error()) {
			newOffset = testTinyExtent.dataSize
			err = nil
		}
		if err != nil {
			t.Fatalf("check tiny extent avali offset failed: %v", err)
		}
		if hole.Offset+hole.Size != uint64(newOffset) {
			t.Fatalf("punch hole record [offset: %v, size: %v] not applied to extent.", hole.Offset, hole.Size)
		}
	}
}

func TestExtentStore_UsageOnConcurrentModification(t *testing.T) {
	var testPath = testutil.InitTempTestPath(t)
	defer testPath.Cleanup()

	const (
		partitionID       uint64 = 1
		storageSize              = 1 * 1024 * 1024
		cacheCapacity            = 10
		writeWorkers             = 10
		dataSizePreWrite         = 16
		executionDuration        = 30 * time.Second
	)

	var testPartitionPath = path.Join(testPath.Path(), fmt.Sprintf("datapartition_%d", partitionID))
	var storage *ExtentStore
	var err error
	if storage, err = NewExtentStore(testPartitionPath, partitionID, storageSize, cacheCapacity,
		func(event CacheEvent, e *Extent) {}, true, IOInterceptors{}); err != nil {
		t.Fatalf("Create extent store failed: %v", err)
		return
	}

	storage.Load()
	for {
		if storage.IsFinishLoad() {
			break
		}
		time.Sleep(time.Second)
	}

	var futures = make(map[string]*async.Future, 0)
	var dataSize = int64(dataSizePreWrite)
	var data = make([]byte, dataSize)
	var dataCRC = crc32.ChecksumIEEE(data)
	var ctx, cancel = context.WithCancel(context.Background())

	var handleWorkerPanic async.PanicHandler = func(i interface{}) {
		t.Fatalf("unexpected panic orrcurred: %v\nCallStack:\n%v", i, string(debug.Stack()))
	}

	// Start workers to write extents
	for i := 0; i < writeWorkers; i++ {
		var future = async.NewFuture()
		var worker async.ParamWorkerFunc = func(args ...interface{}) {
			var (
				future     = args[0].(*async.Future)
				ctx        = args[1].(context.Context)
				cancelFunc = args[2].(context.CancelFunc)
				err        error
			)
			defer func() {
				if err != nil {
					cancelFunc()
				}
				future.Respond(nil, err)
			}()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				var extentID uint64
				if extentID, err = storage.NextExtentID(); err != nil {
					return
				}
				if err = storage.Create(extentID, 0, true); err != nil {
					return
				}
				var offset int64 = 0
				for offset+int64(dataSize) <= 64*1024*1024 {
					select {
					case <-ctx.Done():
						return
					default:
					}
					if err = storage.Write(extentID, offset, dataSize, data, dataCRC, Append, false); err != nil {
						err = nil
						break
					}
					offset += dataSize
				}
			}
		}
		async.ParamWorker(worker, handleWorkerPanic).RunWith(future, ctx, cancel)
		futures[fmt.Sprintf("WriteWorker-%d", i)] = future
	}

	const (
		deleteMarker       = 0
		trashMarker        = 1
		trashRecoverMarker = 2
	)

	// Start extents delete worker
	{
		var future = async.NewFuture()
		var worker async.ParamWorkerFunc = func(args ...interface{}) {
			var (
				future     = args[0].(*async.Future)
				ctx        = args[1].(context.Context)
				cancelFunc = args[2].(context.CancelFunc)
				err        error
			)
			defer func() {
				if err != nil {
					cancelFunc()
				}
				future.Respond(nil, err)
			}()
			var batchDeleteTicker = time.NewTicker(1 * time.Second)
			for {
				select {
				case <-batchDeleteTicker.C:
				case <-ctx.Done():
					return
				}
				var eibs []ExtentInfoBlock
				if eibs, err = storage.GetAllWatermarks(proto.NormalExtentType, nil); err != nil {
					return
				}
				if _, err = storage.GetAllExtentInfoWithByteArr(ExtentFilterForValidateCRC()); err != nil {
					return
				}
				for _, eib := range eibs {
					if eib[FileID]%3 != deleteMarker {
						continue
					}
					_ = storage.MarkDelete(SingleMarker(0, eib[FileID], 0, 0))
				}
			}
		}
		async.ParamWorker(worker, handleWorkerPanic).RunWith(future, ctx, cancel)
		futures["DeleteWorker"] = future
	}

	// Start extents trash worker
	{
		var future = async.NewFuture()
		var worker async.ParamWorkerFunc = func(args ...interface{}) {
			var (
				future     = args[0].(*async.Future)
				ctx        = args[1].(context.Context)
				cancelFunc = args[2].(context.CancelFunc)
				err        error
			)
			defer func() {
				if err != nil {
					cancelFunc()
				}
				future.Respond(nil, err)
			}()
			var batchTrashTicker = time.NewTicker(1 * time.Second)
			for {
				select {
				case <-batchTrashTicker.C:
				case <-ctx.Done():
					return
				}
				var eibs []ExtentInfoBlock
				if eibs, err = storage.GetAllWatermarks(proto.NormalExtentType, nil); err != nil {
					return
				}
				if _, err = storage.GetAllExtentInfoWithByteArr(ExtentFilterForValidateCRC()); err != nil {
					return
				}
				for _, eib := range eibs {
					if eib[FileID]%3 != trashMarker {
						continue
					}
					_ = storage.TrashExtent(eib[FileID], 0, 0)
				}
			}
		}
		async.ParamWorker(worker, handleWorkerPanic).RunWith(future, ctx, cancel)
		futures["TrashWorker"] = future
	}

	// Start extents trash and recover worker
	{
		var future = async.NewFuture()
		var worker async.ParamWorkerFunc = func(args ...interface{}) {
			var (
				future     = args[0].(*async.Future)
				ctx        = args[1].(context.Context)
				cancelFunc = args[2].(context.CancelFunc)
				err        error
			)
			defer func() {
				if err != nil {
					cancelFunc()
				}
				future.Respond(nil, err)
			}()
			var batchTrashRecoverTicker = time.NewTicker(1 * time.Second)
			for {
				select {
				case <-batchTrashRecoverTicker.C:
				case <-ctx.Done():
					return
				}
				var eibs []ExtentInfoBlock
				if eibs, err = storage.GetAllWatermarks(proto.NormalExtentType, nil); err != nil {
					return
				}
				if _, err = storage.GetAllExtentInfoWithByteArr(ExtentFilterForValidateCRC()); err != nil {
					return
				}
				for _, eib := range eibs {
					if eib[FileID]%3 != trashRecoverMarker {
						continue
					}
					_ = storage.TrashExtent(eib[FileID], 0, 0)
					time.Sleep(time.Millisecond * 200)
					storage.RecoverTrashExtent(eib[FileID])
				}
			}
		}
		async.ParamWorker(worker, handleWorkerPanic).RunWith(future, ctx, cancel)
		futures["TrashRecoverWorker"] = future
	}

	// Start control worker
	{
		var future = async.NewFuture()
		var worker async.ParamWorkerFunc = func(args ...interface{}) {
			var (
				future     = args[0].(*async.Future)
				ctx        = args[1].(context.Context)
				cancelFunc = args[2].(context.CancelFunc)
			)
			defer func() {
				future.Respond(nil, nil)
			}()
			var startTime = time.Now()
			var stopTimer = time.NewTimer(executionDuration)
			var displayTicker = time.NewTicker(time.Second * 10)
			for {
				select {
				case <-stopTimer.C:
					t.Logf("Execution finish.")
					cancelFunc()
					return
				case <-displayTicker.C:
					t.Logf("Execution time: %.0fs.", time.Now().Sub(startTime).Seconds())
				case <-ctx.Done():
					t.Logf("Execution aborted.")
					return
				}
			}
		}
		async.ParamWorker(worker, handleWorkerPanic).RunWith(future, ctx, cancel)
		futures["ControlWorker"] = future
	}

	for name, future := range futures {
		if _, err = future.Response(); err != nil {
			t.Fatalf("%v respond error: %v", name, err)
		}
	}
	_, _, _ = storage.FlushDelete(NewFuncInterceptor(nil, nil), 0)

	// 结果检查
	{
		// 计算本地文件系统中实际Size总和
		var actualNormalExtentTotalUsed int64
		var files []os.FileInfo
		var extentFileRegexp = regexp.MustCompile("^(\\d)+$")
		if files, err = ioutil.ReadDir(testPartitionPath); err != nil {
			t.Fatalf("Stat test partition path %v failed, error message: %v", testPartitionPath, err)
		}
		for _, file := range files {
			if extentFileRegexp.Match([]byte(file.Name())) {
				actualNormalExtentTotalUsed += file.Size()
			}
		}

		var eibs []ExtentInfoBlock
		if eibs, err = storage.GetAllWatermarks(proto.AllExtentType, nil); err != nil {
			t.Fatalf("Get extent info from storage failed, error message: %v", err)
		}
		var eibTotalUsed int64
		for _, eib := range eibs {
			eibTotalUsed += int64(eib[Size])
		}

		var storeUsedSize = storage.GetStoreUsedSize() // 存储引擎统计的使用Size总和

		if !((actualNormalExtentTotalUsed == eibTotalUsed) && (eibTotalUsed == storeUsedSize)) {
			t.Fatalf("Used size validation failed, actual total used %v, store used size %v, extent info total used %v", actualNormalExtentTotalUsed, storeUsedSize, eibTotalUsed)
		}
	}

	storage.Close()
}

func initExtentStorage(t *testing.T, extentSize int64, storePath string, extentNum int) (storage *ExtentStore, extents []uint64, err error) {
	const (
		partitionID      uint64 = 1
		storageSize             = 128 * 1024 * 1024
		dataSizePreWrite        = 16
	)
	var testPartitionPath = path.Join(storePath, fmt.Sprintf("datapartition_%d", partitionID))
	if storage, err = NewExtentStore(testPartitionPath, partitionID, storageSize, cacheCapacity,
		func(event CacheEvent, e *Extent) {}, true, IOInterceptors{}); err != nil {
		t.Fatalf("Create extent store failed: %v", err)
		return
	}

	storage.Load()
	for {
		if storage.IsFinishLoad() {
			break
		}
		time.Sleep(time.Second)
	}

	var dataSize = int64(dataSizePreWrite)
	var data = make([]byte, dataSize)
	var dataCRC = crc32.ChecksumIEEE(data)
	extents = make([]uint64, 0)
	for i := 0; i < extentNum; i++ {
		extentID, _ := storage.NextExtentID()
		_ = storage.Create(extentID, uint64(testInode), true)
		var offset int64 = 0
		for offset+dataSize <= extentSize {
			if err = storage.Write(extentID, offset, dataSize, data, dataCRC, Append, false); err != nil {
				err = nil
				break
			}
			offset += dataSize
		}
		extents = append(extents, extentID)
	}
	// calculate extent size
	assert.Equal(t, uint64(extentSize)*uint64(len(extents)), storage.infoStore.NormalUsed())
	return storage, extents, nil
}

func TestTrashExtent(t *testing.T) {
	var testPath = testutil.InitTempTestPath(t)
	defer testPath.Cleanup()
	_, err := log.InitLog(path.Join(os.TempDir(), t.Name(), "logs"), "datanode_test", log.DebugLevel, nil)
	if err != nil {
		t.Errorf("Init log failed: %v", err)
		return
	}
	defer log.LogFlush()
	tStart := time.Now()
	extentSize := int64(4 * 1024 * 1024)
	storage, extents, err := initExtentStorage(t, extentSize, testPath.Path(), 100)
	if err != nil {
		t.Errorf("Init extent store failed: %v", err)
		return
	}
	defer storage.Close()

	// using wrong inode number
	for _, extentID := range extents {
		assert.Error(t, storage.TrashExtent(extentID, uint64(testInode+10), uint32(extentSize)))
	}

	// mv extent to trash dir
	for _, extentID := range extents {
		assert.NoError(t, storage.TrashExtent(extentID, uint64(testInode), uint32(extentSize)))
	}
	trashExtents := 0
	storage.trashExtents.Range(func(k, v interface{}) bool {
		trashExtents++
		return true
	})
	assert.Equal(t, len(extents), trashExtents)
	// calculate extent size
	assert.Equal(t, uint64(0), storage.infoStore.NormalUsed())

	t.Log("starting mark delete trash with long keep time")
	// delete trash less than keep time
	storage.BatchDeleteTrashExtents(uint64(time.Since(tStart).Seconds()) + uint64(60*60))
	recentDelete := 0
	storage.recentDeletedExtents.Range(func(k, v interface{}) bool {
		recentDelete++
		return true
	})
	t.Log("verify recent delete")
	assert.Equal(t, 0, recentDelete)
	assert.Equal(t, uint64(0), storage.infoStore.NormalUsed())

	// sleep 2 sec waiting trash expire
	time.Sleep(time.Second * 2)

	t.Log("starting mark delete trash with short keep time")
	// delete trash
	storage.BatchDeleteTrashExtents(1)
	recentDelete = 0
	storage.recentDeletedExtents.Range(func(k, v interface{}) bool {
		recentDelete++
		return true
	})
	t.Log("verify recent delete")
	assert.Equal(t, len(extents), recentDelete)
	assert.Equal(t, uint64(0), storage.infoStore.NormalUsed())

	t.Log("starting flush delete")
	deleted, remain, err := storage.FlushDelete(NewFuncInterceptor(nil, nil), 128)
	assert.NoError(t, err)
	assert.Equal(t, len(extents), deleted)
	assert.Equal(t, 0, remain)
	var files []os.DirEntry
	files, err = os.ReadDir(path.Join(storage.dataPath, ExtentTrashDirName))
	assert.NoError(t, err)
	assert.Equal(t, 0, len(files))
	assert.Equal(t, uint64(0), storage.infoStore.NormalUsed())
}

func TestRecoverTrashExtents(t *testing.T) {
	var testPath = testutil.InitTempTestPath(t)
	defer testPath.Cleanup()
	_, err := log.InitLog(path.Join(os.TempDir(), t.Name(), "logs"), "datanode_test", log.DebugLevel, nil)
	if err != nil {
		t.Errorf("Init log failed: %v", err)
		return
	}
	defer log.LogFlush()
	extentSize := int64(4 * 1024 * 1024)
	storage, extents, err := initExtentStorage(t, extentSize, testPath.Path(), 100)
	if err != nil {
		t.Errorf("Init extent store failed: %v", err)
		return
	}

	// mv extent to trash dir
	for _, extentID := range extents {
		assert.NoError(t, storage.TrashExtent(extentID, uint64(testInode), uint32(extentSize)))
	}
	trashExtents := 0
	storage.trashExtents.Range(func(k, v interface{}) bool {
		trashExtents++
		return true
	})
	assert.Equal(t, len(extents), trashExtents)
	assert.Equal(t, uint64(0), storage.infoStore.NormalUsed())

	t.Log("starting recover trash extents")
	recovered := 0
	for _, extentID := range extents {
		if storage.RecoverTrashExtent(extentID) {
			recovered++
		}
	}
	assert.Equal(t, len(extents), recovered)
	assert.Equal(t, uint64(extentSize)*uint64(len(extents)), storage.infoStore.NormalUsed())

	trashExtents = 0
	storage.trashExtents.Range(func(k, v interface{}) bool {
		trashExtents++
		return true
	})
	assert.Equal(t, 0, trashExtents)
	for _, extentID := range extents {
		var e *Extent
		e, err = storage.extentWithHeaderByExtentID(extentID)
		assert.NoError(t, err)
		assert.NotNil(t, e)
	}

	var files []os.DirEntry
	files, err = os.ReadDir(path.Join(storage.dataPath, ExtentTrashDirName))
	assert.NoError(t, err)
	assert.Equal(t, 0, len(files))
}

func TestLoadTrashExtents(t *testing.T) {
	var testPath = testutil.InitTempTestPath(t)
	defer testPath.Cleanup()
	_, err := log.InitLog(path.Join(os.TempDir(), t.Name(), "logs"), "datanode_test", log.DebugLevel, nil)
	if err != nil {
		t.Errorf("Init log failed: %v", err)
		return
	}
	defer log.LogFlush()
	extentSize := int64(1 * 1024 * 1024)
	storage, extents, err := initExtentStorage(t, extentSize, testPath.Path(), 128)
	if err != nil {
		t.Errorf("Init extent store failed: %v", err)
		return
	}
	testPartitionPath := storage.dataPath
	// mv extent to trash dir
	for _, extentID := range extents {
		assert.NoError(t, storage.TrashExtent(extentID, uint64(testInode), uint32(extentSize)))
	}
	trashExtents := 0
	storage.trashExtents.Range(func(k, v interface{}) bool {
		trashExtents++
		return true
	})
	assert.Equal(t, len(extents), trashExtents)
	assert.Equal(t, uint64(0), storage.infoStore.NormalUsed())
	storage.Close()

	var newStorage *ExtentStore
	if newStorage, err = NewExtentStore(testPartitionPath, storage.partitionID, storage.storeSize, cacheCapacity,
		func(event CacheEvent, e *Extent) {}, false, IOInterceptors{}); err != nil {
		t.Fatalf("Create extent store failed: %v", err)
		return
	}
	newStorage.Load()
	for {
		if storage.IsFinishLoad() {
			break
		}
		time.Sleep(time.Second)
	}
	assert.Equal(t, uint64(0), newStorage.infoStore.NormalUsed())

	t.Log("starting recover trash extents")
	recovered := 0
	for _, extentID := range extents {
		if storage.RecoverTrashExtent(extentID) {
			recovered++
		}
	}
	assert.Equal(t, len(extents), recovered)
	assert.Equal(t, uint64(extentSize)*uint64(len(extents)), storage.infoStore.NormalUsed())
}

func TestConcurrencyTrashExtents(t *testing.T) {
	var testPath = testutil.InitTempTestPath(t)
	defer testPath.Cleanup()
	_, err := log.InitLog(path.Join(os.TempDir(), t.Name(), "logs"), "datanode_test", log.DebugLevel, nil)
	if err != nil {
		t.Errorf("Init log failed: %v", err)
		return
	}
	defer log.LogFlush()

	extentSize := int64(4 * 1024)
	const (
		partitionID      uint64 = 1
		storageSize             = 128 * 1024 * 1024
		dataSizePreWrite        = 16
	)
	var storage *ExtentStore
	var testPartitionPath = path.Join(testPath.Path(), fmt.Sprintf("datapartition_%d", partitionID))

	if storage, err = NewExtentStore(testPartitionPath, partitionID, storageSize, cacheCapacity,
		func(event CacheEvent, e *Extent) {}, true, IOInterceptors{}); err != nil {
		t.Fatalf("Create extent store failed: %v", err)
		return
	}

	storage.Load()
	for {
		if storage.IsFinishLoad() {
			break
		}
		time.Sleep(time.Second)
	}

	var dataSize = int64(dataSizePreWrite)
	var data = make([]byte, dataSize)
	var dataCRC = crc32.ChecksumIEEE(data)
	// 100% trash 产生, 50% trash 最终删除, 50% trash 最终恢复
	trashCh1 := make(chan uint64, 1024)
	trashCh2 := make(chan uint64, 1024)
	recoverCh1 := make(chan uint64, 102400)
	recoverCh2 := make(chan uint64, 102400)
	wg := sync.WaitGroup{}
	wg.Add(3)
	testTimer := time.NewTimer(time.Second * 30)
	defer testTimer.Stop()
	stopCh := make(chan struct{}, 1)

	go func() {
		for {
			select {
			case <-testTimer.C:
				close(stopCh)
				return
			}
		}
	}()

	// goroutine-1: create extent
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopCh:
				return
			default:
				extentID, _ := storage.NextExtentID()
				_ = storage.Create(extentID, uint64(testInode), true)
				var offset int64 = 0
				for offset+dataSize <= extentSize {
					if err = storage.Write(extentID, offset, dataSize, data, dataCRC, Append, false); err != nil {
						err = nil
						break
					}
					offset += dataSize
				}
				trashCh1 <- extentID
				trashCh2 <- extentID
				if extentID%2 == 0 {
					recoverCh1 <- extentID
					recoverCh2 <- extentID
				}
			}
		}

	}()

	// goroutine-2: trash worker-1
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopCh:
				return
			case extent := <-trashCh1:
				if extent == 0 {
					continue
				}
				err = storage.TrashExtent(extent, testInode, uint32(extentSize))
				assert.NoError(t, err)
			}
		}
	}()

	// goroutine-3: trash worker-2
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopCh:
				return
			case extent := <-trashCh2:
				if extent == 0 {
					continue
				}
				err = storage.TrashExtent(extent, testInode, uint32(extentSize))
				assert.NoError(t, err)
			}
		}
	}()

	wg.Wait()
	verifyExtentStoreSize(t, testPartitionPath, storage)

	stopCh = make(chan struct{}, 1)
	testTimer.Reset(time.Second * 10)
	go func() {
		for {
			select {
			case <-testTimer.C:
				close(stopCh)
				return
			}
		}
	}()

	wg.Add(2)
	// goroutine-4:  trash recover worker-1
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopCh:
				return
			case extent := <-recoverCh1:
				if extent == 0 {
					continue
				}
				storage.RecoverTrashExtent(extent)
			}
		}
	}()

	// goroutine-5: trash recover worker-2
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopCh:
				return
			case extent := <-recoverCh2:
				if extent == 0 {
					continue
				}
				storage.RecoverTrashExtent(extent)
			}
		}
	}()

	wg.Add(2)
	// goroutine-6: trash delete worker-1
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopCh:
				return
			default:
				time.Sleep(time.Second)
				storage.BatchDeleteTrashExtents(1)
				var deleted, remain int
				deleted, remain, err = storage.FlushDelete(nil, 512)
				assert.NoError(t, err)
				t.Logf("flushdelete extent store, deleted(%v) remain(%v)", deleted, remain)
			}
		}
	}()

	// goroutine-7: trash delete worker-2
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopCh:
				return
			default:
				time.Sleep(time.Second)
				storage.BatchDeleteTrashExtents(1)
				var deleted, remain int
				deleted, remain, err = storage.FlushDelete(nil, 512)
				assert.NoError(t, err)
				t.Logf("flushdelete extent store, deleted(%v) remain(%v)", deleted, remain)
			}
		}
	}()
	wg.Wait()
	close(trashCh1)
	close(trashCh2)
	close(recoverCh1)
	close(recoverCh2)
	verifyExtentStoreSize(t, testPartitionPath, storage)
}

// 验证普通删除和Trash删除并发的场景
func TestBatchDeleteAndTrashDeleteConcurrency(t *testing.T) {
	var testPath = testutil.InitTempTestPath(t)
	defer testPath.Cleanup()
	_, err := log.InitLog(path.Join(os.TempDir(), t.Name(), "logs"), "datanode_test", log.DebugLevel, nil)
	if err != nil {
		t.Errorf("Init log failed: %v", err)
		return
	}
	defer log.LogFlush()

	extentSize := int64(4 * 1024)
	const (
		partitionID      uint64 = 1
		storageSize             = 128 * 1024 * 1024
		dataSizePreWrite        = 16
	)
	var storage *ExtentStore
	var testPartitionPath = path.Join(testPath.Path(), fmt.Sprintf("datapartition_%d", partitionID))

	if storage, err = NewExtentStore(testPartitionPath, partitionID, storageSize, cacheCapacity,
		func(event CacheEvent, e *Extent) {}, true, IOInterceptors{}); err != nil {
		t.Fatalf("Create extent store failed: %v", err)
		return
	}

	storage.Load()
	for {
		if storage.IsFinishLoad() {
			break
		}
		time.Sleep(time.Second)
	}

	var dataSize = int64(dataSizePreWrite)
	var data = make([]byte, dataSize)
	var dataCRC = crc32.ChecksumIEEE(data)
	trashCh := make(chan uint64, 1024)
	deleteCh := make(chan uint64, 1024)
	wg := sync.WaitGroup{}
	wg.Add(4)
	testTimer := time.NewTimer(time.Second * 30)
	defer testTimer.Stop()
	stopCh := make(chan struct{}, 1)

	go func() {
		for {
			select {
			case <-testTimer.C:
				close(stopCh)
				return
			}
		}
	}()

	// goroutine-1: create extent
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopCh:
				return
			default:
				extentID, _ := storage.NextExtentID()
				_ = storage.Create(extentID, uint64(testInode), true)
				var offset int64 = 0
				for offset+dataSize <= extentSize {
					if err = storage.Write(extentID, offset, dataSize, data, dataCRC, Append, false); err != nil {
						err = nil
						break
					}
					offset += dataSize
				}
				trashCh <- extentID
				deleteCh <- extentID
			}
		}
	}()

	// goroutine-2: trash worker
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopCh:
				return
			case extent := <-trashCh:
				if extent == 0 {
					continue
				}
				err = storage.TrashExtent(extent, testInode, uint32(extentSize))
				assert.NoError(t, err)
			}
		}
	}()

	// goroutine-3: delete worker
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopCh:
				return
			case extent := <-deleteCh:
				if extent == 0 {
					continue
				}
				err = storage.MarkDelete(SingleMarker(testInode, extent, 0, extentSize))
				assert.NoError(t, err)
			}
		}
	}()

	// goroutine-4: flush delete worker
	var remain int
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopCh:
				return
			default:
				time.Sleep(time.Second)
				var deleted int
				deleted, remain, err = storage.FlushDelete(nil, 512)
				assert.NoError(t, err)
				t.Logf("flushdelete extent store, deleted(%v) remain(%v)", deleted, remain)
			}
		}
	}()
	wg.Wait()
	close(deleteCh)
	close(trashCh)
	storage.FlushDelete(nil, 0)
	verifyExtentStoreSize(t, testPartitionPath, storage)
}

func verifyExtentStoreSize(t *testing.T, extentStorePath string, storage *ExtentStore) {
	// 1-文件实际size累加和
	realSize := int64(0)
	dirs, err := os.ReadDir(extentStorePath)
	assert.NoError(t, err)
	for _, dir := range dirs {
		if _, ok := storage.ExtentID(dir.Name()); !ok {
			continue
		}
		var stat os.FileInfo
		stat, err = os.Stat(path.Join(extentStorePath, dir.Name()))
		if !assert.NoError(t, err) {
			continue
		}
		realSize += stat.Size()
	}

	// 2-ExtentInfoBlockSize累加和
	extentInfoBlockSize := int64(0)
	extents, _ := storage.GetAllWatermarks(proto.NormalExtentType, nil)
	for _, extent := range extents {
		extentInfoBlockSize += int64(extent[Size])
	}
	// 3-storage.GetStoreUsedSize()程序计数器累加和
	storageUsedSize := storage.GetStoreUsedSize()
	assert.Equal(t, realSize, storageUsedSize)
	assert.Equal(t, extentInfoBlockSize, storageUsedSize)
}
