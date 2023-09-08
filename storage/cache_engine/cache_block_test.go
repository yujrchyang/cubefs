// Copyright 2018 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package cache_engine

import (
	"context"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/tmpfs"
	"github.com/cubefs/cubefs/util/unit"
	"github.com/stretchr/testify/assert"
	"math"
	"math/rand"
	"sync"
	"testing"
	"time"
)

var letterRunes = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randTestData(size int) (data []byte) {
	rand.Seed(time.Now().UnixNano())
	b := make([]byte, size)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return b
}

func umountTestTmpfs() error {
	return tmpfs.Umount(testTmpFS)
}

func TestWriteCacheBlock(t *testing.T) {
	assert.Nil(t, initTestTmpfs(200*unit.MB))
	defer func() {
		assert.Nil(t, umountTestTmpfs())
	}()
	testWriteSingleFile(t)
	testWriteSingleFileError(t)
	testWriteCacheBlockFull(t)
	testWriteMultiCacheBlock(t, newCacheBlockWithDiffInode)
	testWriteMultiCacheBlock(t, newCacheBlockWithDiffVolume)
}

func testWriteSingleFile(t *testing.T) {
	cacheBlock := NewCacheBlock(testTmpFS, t.Name(), 1, 1024, 112456871, proto.CACHE_BLOCK_SIZE, nil)
	assert.Nil(t, cacheBlock.initCacheStore())
	defer func() {
		assert.Nil(t, cacheBlock.Delete())
	}()
	bytes := randTestData(1024)
	assert.Nil(t, cacheBlock.WriteAt(bytes, int64(0), 1024))
	t.Logf("testWriteSingleFile, test:%s cacheBlock.datasize:%d", t.Name(), cacheBlock.usedSize)
}

func testWriteSingleFileError(t *testing.T) {
	cacheBlock := NewCacheBlock(testTmpFS, t.Name(), 1, 1024, 112456871, proto.CACHE_BLOCK_SIZE, nil)
	assert.Nil(t, cacheBlock.initCacheStore())
	defer func() {
		assert.Nil(t, cacheBlock.Delete())
	}()
	bytes := randTestData(1024)
	assert.Nil(t, cacheBlock.WriteAt(bytes, int64(0), 1024))
	assert.NotNil(t, cacheBlock.WriteAt(bytes, proto.CACHE_BLOCK_SIZE, 1024))
	t.Logf("testWriteSingleFileError, test:%s cacheBlock.datasize:%d", t.Name(), cacheBlock.usedSize)
}

func testWriteCacheBlockFull(t *testing.T) {
	var err error
	cacheBlock := NewCacheBlock(testTmpFS, t.Name(), 1, 1024, 112456871, proto.CACHE_BLOCK_SIZE, nil)
	assert.Nil(t, cacheBlock.initCacheStore())
	defer func() {
		assert.Nil(t, cacheBlock.Delete())
	}()
	bytes := randTestData(1024)
	var offset int64
	for {
		err = cacheBlock.WriteAt(bytes, offset, 1024)
		if err != nil {
			break
		}
		offset += 1024
		if offset/1024%1024 == 0 {
			t.Logf("testWriteCacheBlockFull, offset:%d cacheBlock.datasize:%d", offset, cacheBlock.usedSize)
		}
	}
	assert.GreaterOrEqual(t, offset+1024, int64(proto.CACHE_BLOCK_SIZE))
}

func newCacheBlockWithDiffInode(volume string, index int, allocSize uint64) (cacheBlock *CacheBlock, err error) {
	cacheBlock = NewCacheBlock(testTmpFS, volume, uint64(index), 1024, 112456871, allocSize, nil)
	err = cacheBlock.initCacheStore()
	return
}

func newCacheBlockWithDiffVolume(volume string, index int, allocSize uint64) (cacheBlock *CacheBlock, err error) {
	newVolume := fmt.Sprintf("%s_%d", volume, index)
	cacheBlock = NewCacheBlock(testTmpFS, newVolume, 1, 1024, 112456871, allocSize, nil)
	err = cacheBlock.initCacheStore()
	return
}

func testWriteMultiCacheBlock(t *testing.T, newMultiCacheFunc func(volume string, index int, allocSize uint64) (*CacheBlock, error)) {
	count := 100
	wg := sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			var err error
			volume := fmt.Sprintf("%s_%d", t.Name(), index)
			cacheBlock, err := newMultiCacheFunc(volume, 1, proto.CACHE_BLOCK_SIZE)
			if err != nil {
				t.Errorf("testWriteMultiCacheBlock, err:%v", err)
				return
			}
			defer func() {
				assert.Nil(t, cacheBlock.Delete())
			}()
			bytes := randTestData(1024)
			var offset int64
			for j := 0; j < count; j++ {
				err = cacheBlock.WriteAt(bytes, offset, 1024)
				if err != nil {
					break
				}
				offset += 1024
				time.Sleep(time.Millisecond * 100)
				if j%50 == 0 {
					t.Logf("testWriteMultiCacheBlock, volume:%v, write count:%v, cacheBlock.datasize:%d", volume, j, cacheBlock.usedSize)
				}
			}
			assert.GreaterOrEqual(t, offset+1024, int64(1024*count))
		}(i)
	}
	wg.Wait()
}

func TestCacheBlockTmpfsStore(t *testing.T) {
	assert.Nil(t, initTestTmpfs(200*unit.MB))
	defer func() {
		assert.Nil(t, umountTestTmpfs())
	}()
	cacheBlock := NewCacheBlock(testTmpFS, t.Name(), 1, 1024, 2568748711, proto.CACHE_BLOCK_SIZE, nil)
	assert.Nil(t, cacheBlock.initCacheStore())
	defer func() {
		assert.Nil(t, cacheBlock.Delete())
	}()
	cacheBlockReadWrite(t, cacheBlock)
	return
}

func TestCacheBlockMemoryStore(t *testing.T) {
	cacheBlock := NewCacheBlock(testTmpFS, t.Name(), 1, 1024, 2568748711, proto.CACHE_BLOCK_SIZE, nil)
	assert.Nil(t, cacheBlock.initCacheStore())
	defer func() {
		assert.Nil(t, cacheBlock.Delete())
	}()
	cacheBlockReadWrite(t, cacheBlock)
	return
}

func TestParallelOperation(t *testing.T) {
	for i := 0; i < 1; i++ {
		testParallelOperation(t)
	}
}

func testParallelOperation(t *testing.T) {
	assert.Nil(t, initTestTmpfs(200*unit.MB))
	defer func() {
		assert.Nil(t, umountTestTmpfs())
	}()

	cacheBlock := NewCacheBlock(testTmpFS, t.Name(), 1, 1024, 112456871, proto.CACHE_BLOCK_SIZE, nil)
	assert.Nil(t, cacheBlock.initCacheStore())
	stopCh := make(chan struct{}, 1)

	//delete func
	go func() {
		time.Sleep(time.Second * 5)
		assert.Nil(t, cacheBlock.Delete())
	}()
	cacheBlock.markReady()
	//read func
	go func() {
		ticker := time.NewTicker(time.Millisecond * 50)
		for {
			select {
			case <-stopCh:
				break
			case <-ticker.C:
				bytesRead := make([]byte, 1024)
				rand.Seed(time.Now().Unix())
				offset := rand.Intn(int(cacheBlock.allocSize))
				cacheBlock.Read(context.Background(), bytesRead, int64(offset), 1024)
			}
		}
	}()

	//write func
	go func() {
		bytes := randTestData(1024)
		offset := int64(0)
		ticker := time.NewTicker(time.Millisecond * 50)
		for {
			select {
			case <-stopCh:
				break
			case <-ticker.C:
				err := cacheBlock.WriteAt(bytes, offset, 1024)
				if err != nil {
					break
				}
				offset += 1024
			}
		}
	}()
	time.Sleep(time.Second * 10)
	close(stopCh)
}

func cacheBlockReadWrite(t *testing.T, cacheBlock *CacheBlock) {
	dataLen := 1024
	offsetIndex := 0
	sizeIndex := 1
	sources := [][]int64{
		{
			0,
			int64(dataLen),
		},
		{
			int64(dataLen) * 2,
			int64(dataLen),
		},
		{
			int64(dataLen) * 3,
			int64(dataLen),
		},
	}
	lackSource := []int64{
		int64(dataLen),
		int64(dataLen),
	}
	writeBytes := randTestData(dataLen)
	for _, s := range sources {
		assert.Nil(t, cacheBlock.WriteAt(writeBytes, s[offsetIndex]%proto.CACHE_BLOCK_SIZE, s[sizeIndex]))
	}
	bytesRead := make([]byte, dataLen)

	t.Run("read_timeout", func(t *testing.T) {
		timeoutCtx, cancel := context.WithTimeout(context.Background(), proto.ReadCacheTimeoutMs*time.Millisecond)
		defer cancel()
		_, _, err := cacheBlock.Read(timeoutCtx, bytesRead, lackSource[offsetIndex], lackSource[sizeIndex])
		if !assert.Error(t, err) {
			return
		}
		if !assert.Equal(t, err.Error(), context.DeadlineExceeded.Error()) {
			return
		}
	})

	t.Run("range_data_ready", func(t *testing.T) {
		_, _, err := cacheBlock.Read(context.Background(), bytesRead, sources[0][offsetIndex], sources[0][sizeIndex])
		if !assert.NoError(t, err) {
			return
		}
		for i := 0; i < dataLen; i++ {
			if !assert.Equal(t, writeBytes[i], bytesRead[i]) {
				return
			}
		}
	})

	t.Run("all_data_ready", func(t *testing.T) {
		cacheBlock.markReady()
		_, _, err := cacheBlock.Read(context.Background(), bytesRead, lackSource[offsetIndex], lackSource[sizeIndex])
		if !assert.NoError(t, err) {
			return
		}
		for i := 0; i < dataLen; i++ {
			if !assert.Equal(t, uint8(0), bytesRead[i]) {
				return
			}
		}
	})

	t.Run("read_offset_usedsize_0", func(t *testing.T) {
		offset := int64(0)
		cacheBlock.usedSize = 0
		_, _, err := cacheBlock.Read(context.Background(), bytesRead, offset, int64(dataLen))
		assert.Error(t, err)
	})

	t.Run("read_offset_gt_usedsize", func(t *testing.T) {
		offset := int64(dataLen + 1)
		cacheBlock.usedSize = int64(dataLen)
		_, _, err := cacheBlock.Read(context.Background(), bytesRead, offset, int64(dataLen))
		assert.Error(t, err)
	})

}

func TestComputeAllocSize(t *testing.T) {
	cases1 := []*proto.DataSource{
		{
			FileOffset: 0,
			Size_:      100,
		},
		{
			FileOffset: 1024,
			Size_:      100,
		},
	}
	alloc, err := computeAllocSize(cases1)
	assert.ErrorContains(t, err, SparseFileError.Error())

	cases2 := []*proto.DataSource{
		{
			FileOffset: proto.CACHE_BLOCK_SIZE + 53,
			Size_:      43,
		},
	}
	alloc, err = computeAllocSize(cases2)
	assert.ErrorContains(t, err, SparseFileError.Error())

	cases3 := []*proto.DataSource{
		{
			FileOffset: proto.CACHE_BLOCK_SIZE,
			Size_:      43,
		},
	}
	alloc, err = computeAllocSize(cases3)
	assert.NoError(t, err)
	assert.Equal(t, uint64(proto.PageSize), alloc)

	cases4 := []*proto.DataSource{
		{
			FileOffset: 0,
			Size_:      1024,
		},
		{
			FileOffset: 1024,
			Size_:      4096,
		},
		{
			FileOffset: 5120,
			Size_:      4096,
		},
	}
	alloc, err = computeAllocSize(cases4)
	assert.NoError(t, err)
	assert.Equal(t, uint64(3*proto.PageSize), alloc)

	cases5 := []*proto.DataSource{
		{
			FileOffset: proto.CACHE_BLOCK_SIZE * 128,
			Size_:      1024,
		},
		{
			FileOffset: proto.CACHE_BLOCK_SIZE*128 + 1024,
			Size_:      4096,
		},
		{
			FileOffset: proto.CACHE_BLOCK_SIZE*128 + 5120,
			Size_:      4096,
		},
	}
	alloc, err = computeAllocSize(cases5)
	assert.NoError(t, err)
	assert.Equal(t, uint64(3*proto.PageSize), alloc)

	alloc, err = computeAllocSize(generateSparseSources(1024))
	assert.ErrorContains(t, err, SparseFileError.Error())

	alloc, err = computeAllocSize(generateSources(1024))
	assert.NoError(t, err)
	assert.Equal(t, uint64(proto.PageSize), alloc)

	alloc, err = computeAllocSize(generateSources(4 * 1024))
	assert.NoError(t, err)
	assert.Equal(t, uint64(4*1024), alloc)

	alloc, err = computeAllocSize(generateSources(128 * 1024))
	assert.NoError(t, err)
	assert.Equal(t, uint64(128*1024), alloc)

	alloc, err = computeAllocSize(generateSparseSources(128 * 1024))
	assert.ErrorContains(t, err, SparseFileError.Error())

	alloc, err = computeAllocSize(generateSources(1024 * 1024))
	assert.NoError(t, err)
	assert.Equal(t, uint64(1024*1024), alloc)

}

func generateSources(fileSize int64) (sources []*proto.DataSource) {
	sources = make([]*proto.DataSource, 0)
	var bufSlice []int
	if fileSize > proto.PageSize {
		bufSlice = []int{1, 4, 16, 64, 128, 512, 1024, proto.PageSize, 16 * 1024, 64 * 1024, 128 * 1024, 256 * 1024, 512 * 1024, proto.CACHE_BLOCK_SIZE}
	} else {
		bufSlice = []int{1, 4, 16, 64, 128}
	}
	var offset int64
	//init test data
	for {
		if offset >= fileSize {
			break
		}
		rand.New(rand.NewSource(time.Now().UnixNano()))
		index := rand.Intn(len(bufSlice))
		if int64(bufSlice[index]) >= fileSize {
			continue
		}
		size := int(math.Min(float64(fileSize-offset), float64(bufSlice[index])))
		sources = append(sources, &proto.DataSource{
			Size_:      uint64(size),
			FileOffset: uint64(offset),
		})
		offset += int64(size)
	}
	return
}

func generateSparseSources(fileSize int64) (sources []*proto.DataSource) {
	for i := 0; i < 1000; i++ {
		originSources := generateSources(fileSize)
		sources = make([]*proto.DataSource, 0)
		for j, s := range originSources {
			if j%2 == 0 {
				sources = append(sources, s)
			}
		}
		if len(sources) >= 2 {
			break
		}
	}
	if len(sources) < 2 {
		return make([]*proto.DataSource, 0)
	}
	return
}
