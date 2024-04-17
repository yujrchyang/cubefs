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
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/stackmerge"
	"hash/crc32"
	"math"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type CacheBlock struct {
	store       Store
	volume      string
	inode       uint64
	fixedOffset uint64
	version     uint32
	rootPath    string
	filePath    string
	modifyTime  int64
	usedSize    int64 //usedSize是缓存文件的真实Size
	allocSize   int64 //allocSize是为了避免并发回源导致tmpfs满，而预先将所有source按照4K对齐后的Size之和，是逻辑值
	sizeLock    sync.RWMutex
	blockKey    string
	readSource  ReadExtentData
	initOnce    sync.Once

	stacks    *stackmerge.StackList
	ready     atomic.Bool
	closeOnce sync.Once
	readyCh   chan struct{}
	closeCh   chan struct{}
	sync.Mutex
}

// NewCacheBlock create and returns a new extent instance.
func NewCacheBlock(path string, volume string, inode, fixedOffset uint64, version uint32, allocSize uint64, reader ReadExtentData) (cb *CacheBlock) {
	cb = new(CacheBlock)
	cb.volume = volume
	cb.inode = inode
	cb.fixedOffset = fixedOffset
	cb.version = version
	cb.blockKey = GenCacheBlockKey(volume, inode, fixedOffset, version)
	cb.updateAllocSize(int64(allocSize))
	cb.filePath = path + "/" + cb.blockKey
	cb.rootPath = path
	cb.readSource = reader
	cb.stacks = stackmerge.NewStackList()
	cb.readyCh = make(chan struct{}, 1)
	cb.closeCh = make(chan struct{}, 1)
	return
}

func (cb *CacheBlock) String() string {
	return fmt.Sprintf("volume(%v) inode(%v) offset(%v) version(%v)", cb.volume, cb.inode, cb.fixedOffset, cb.version)
}

// Close this extent and release FD.
func (cb *CacheBlock) Close() {
	defer func() {
		if r := recover(); r != nil {
			log.LogErrorf("key(%v) recover on close:%v", cb.blockKey, r)
		}
	}()
	cb.closeOnce.Do(func() {
		if cb.closeCh != nil {
			close(cb.closeCh)
		}
		if cb.store != nil {
			cb.store.Close()
		}
	})
}

func (cb *CacheBlock) Delete() (err error) {
	var exist bool
	exist, err = cb.Exist()
	if err != nil {
		return err
	}
	if !exist {
		return
	}
	cb.Close()
	err = os.Remove(cb.filePath)
	if err != nil {
		return err
	}
	return
}

func (cb *CacheBlock) Exist() (exsit bool, err error) {
	_, err = os.Stat(cb.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// WriteAt writes data to an cacheBlock, it is allowed to write at anywhere for sparse file
func (cb *CacheBlock) WriteAt(data []byte, offset, size int64) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("write panic: %v, blockKey:%v dataLen:%v offset:%v size:%v", r, cb.blockKey, len(data), offset, size)
		}
	}()
	if err = cb.checkWriteOffsetAndSize(offset, size); err != nil {
		return
	}
	if _, err = cb.store.WriteAt(data[:size], offset); err != nil {
		return
	}
	cb.maybeUpdateUsedSize(offset + size)
	if err = cb.putStack(offset, size); err != nil {
		return
	}
	return
}

// Read reads data from an extent.
func (cb *CacheBlock) Read(data []byte, offset, size int64) (crc uint32, err error) {
	if cb.getUsedSize() == 0 || offset >= cb.getAllocSize() || offset >= cb.getUsedSize() {
		return 0, fmt.Errorf("invalid read, offset:%d, size:%v, allocSize:%d, usedSize:%d, stacks:%v", offset, size, cb.getAllocSize(), cb.getUsedSize(), cb.stacks)
	}
	readSize := int64(math.Min(float64(cb.getUsedSize()-offset), float64(size)))
	if log.IsDebugEnabled() {
		log.LogDebugf("action[Read] read cache block:%v, offset:%d, allocSize:%d, usedSize:%d", cb.blockKey, offset, cb.allocSize, cb.usedSize)
	}
	if _, err = cb.store.ReadAt(data[:readSize], offset); err != nil {
		return
	}
	crc = crc32.ChecksumIEEE(data)
	return
}

func (cb *CacheBlock) checkWriteOffsetAndSize(offset, size int64) error {
	if offset+size > cb.getAllocSize() {
		return NewParameterMismatchErr(fmt.Sprintf("invalid write, offset=%v size=%v allocSize:%d", offset, size, cb.getAllocSize()))
	}
	if offset >= cb.getAllocSize() || size == 0 {
		return NewParameterMismatchErr(fmt.Sprintf("invalid write, offset=%v size=%v allocSize:%d", offset, size, cb.getAllocSize()))
	}
	return nil
}

func (cb *CacheBlock) initCacheStore() (err error) {
	err = os.Mkdir(cb.rootPath+"/"+cb.volume, 0666)
	if err != nil {
		if !os.IsExist(err) {
			return
		}
		err = nil
	}
	_, err = os.Stat(cb.filePath)
	if err == nil {
		os.Remove(cb.filePath)
		log.LogWarnf("an old version cache block:%v found, remove it", cb.filePath)
	}
	cb.store, err = NewFileStore(cb.filePath)
	if err != nil {
		return fmt.Errorf("open file err: %v", err)
	}
	cb.maybeUpdateUsedSize(0)
	if log.IsDebugEnabled() {
		log.LogDebugf("init cache block(%s) to tmpfs", cb.blockKey)
	}
	return
}

func (cb *CacheBlock) Init(sources []*proto.DataSource, engine *CacheEngine) {
	var err error
	metric := exporter.NewModuleTPUs("InitBlock")
	defer func() {
		if err != nil {
			engine.deleteCacheBlock(cb.blockKey)
		}
		metric.Set(err)
	}()
	//parallel read source data
	sourceTaskCh := make(chan *proto.DataSource, 100)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wg := sync.WaitGroup{}
	for i := 0; i < int(math.Min(float64(8), float64(len(sources)))); i++ {
		wg.Add(1)
		go func() {
			var e error
			defer func() {
				if e != nil {
					cancel()
				}
				wg.Done()
			}()
			e = cb.prepareSource(ctx, sourceTaskCh)
		}()
	}
	var stop bool
	var sb = strings.Builder{}
	for idx, s := range sources {
		select {
		case sourceTaskCh <- s:
			sb.WriteString(fmt.Sprintf("  sourceIndex(%d) dp(%v) extent(%v) offset(%v) size(%v) fileOffset(%v) hosts(%v)\n", idx, s.PartitionID, s.ExtentID, s.ExtentOffset, s.Size_, s.FileOffset, strings.Join(s.Hosts, ",")))
		case <-ctx.Done():
			stop = true
		}
		if stop {
			break
		}
	}
	close(sourceTaskCh)
	wg.Wait()
	if err = ctx.Err(); err != nil {
		return
	}
	if log.IsInfoEnabled() {
		log.LogInfof("action[Init], cache block:%v, sources_len:%v, sources:\n%v", cb.blockKey, len(sources), sb.String())
	}
	cb.notifyReady()
	return
}

func (cb *CacheBlock) prepareSource(ctx context.Context, taskCh chan *proto.DataSource) (err error) {
	for {
		select {
		case <-ctx.Done():
			return
		case task := <-taskCh:
			if task == nil {
				return
			}
			tStart := time.Now()
			if _, err = cb.readSource(task, cb.WriteAt, func() {
				cb.stacks = stackmerge.NewStackList()
			}); err != nil {
				log.LogErrorf("action[prepareSource] cache block(%s), dp:%d, extent:%d, ExtentOffset:%v, FileOffset:%d, size:%v, readSource err:%v", cb.blockKey, task.PartitionID, task.ExtentID, task.ExtentOffset, task.FileOffset, task.Size_, err)
				return
			}

			if log.IsDebugEnabled() {
				log.LogDebugf("action[prepareSource] cache block(%s), dp:%d, extent:%d, ExtentOffset:%v, FileOffset:%d, size:%v, end, cost[%v]", cb.blockKey, task.PartitionID, task.ExtentID, task.ExtentOffset, task.FileOffset, task.Size_, time.Since(tStart))
			}
		}
	}
}

// Wait better waiting no more than 4ms
func (cb *CacheBlock) Wait(ctx context.Context) (err error) {
	select {
	case <-cb.readyCh:
		log.LogInfof("action[Wait] cache block(%s) is ready", cb.blockKey)
	case <-cb.closeCh:
		err = CacheClosedError
	case <-ctx.Done():
		err = ctx.Err()
	}
	return
}

func (cb *CacheBlock) IsReady(offset, size uint64) bool {
	if cb.ready.Load() {
		return true
	}
	if gEnableStack.Load() && cb.stacks.IsCover(offset, offset+size) {
		if log.IsDebugEnabled() {
			log.LogDebugf("action[IsReady] cache block(%s) stacks{%v} cover range[%v, %v]", cb.blockKey, cb.stacks, offset, offset+size)
		}
		return true
	}
	return false
}

func (cb *CacheBlock) putStack(offset, size int64) (err error) {
	if !gEnableStack.Load() {
		return
	}
	_, err = cb.stacks.Put(&stackmerge.Stack{
		uint64(offset),
		uint64(offset + size),
	})
	return
}

func (cb *CacheBlock) notifyReady() {
	cb.ready.Store(true)
	close(cb.readyCh)
}

// compute alloc size
func computeAllocSize(sources []*proto.DataSource) (alloc uint64, err error) {
	if len(sources) == 0 {
		err = EmptySourcesError
		return
	}
	var sum uint64
	for _, s := range sources {
		off := s.CacheBlockOffset()
		if off+s.Size_ > alloc {
			alloc = off + s.Size_
		}
		sum += s.Size_
	}
	if sum != alloc {
		err = SparseFileError
		return
	}
	if alloc%proto.PageSize != 0 {
		alloc += proto.PageSize - alloc%proto.PageSize
	}
	return
}

func (cb *CacheBlock) getUsedSize() int64 {
	cb.sizeLock.RLock()
	defer cb.sizeLock.RUnlock()
	return cb.usedSize
}

func (cb *CacheBlock) maybeUpdateUsedSize(size int64) {
	cb.sizeLock.Lock()
	defer cb.sizeLock.Unlock()
	cb.modifyTime = time.Now().Unix()
	if cb.usedSize < size {
		log.LogDebugf("maybeUpdateUsedSize, cache block:%v, old:%v, new:%v", cb.blockKey, cb.usedSize, size)
		cb.usedSize = size
	}
}

func (cb *CacheBlock) getAllocSize() int64 {
	cb.sizeLock.RLock()
	defer cb.sizeLock.RUnlock()
	return cb.allocSize
}

func (cb *CacheBlock) updateAllocSize(size int64) {
	cb.sizeLock.Lock()
	defer cb.sizeLock.Unlock()
	cb.allocSize = size
}
