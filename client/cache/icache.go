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

package cache

import (
	"container/list"
	"context"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
)

const (
	// MinInodeCacheEvictNum is used in the foreground eviction.
	// When clearing the inodes from the cache, it stops as soon as 10 inodes have been evicted.
	MinInodeCacheEvictNum = 10
	// MaxInodeCacheEvictNum is used in the back ground. We can evict 200000 inodes at max.
	MaxInodeCacheEvictNum = 200000

	BgEvictionInterval = 2 * time.Minute
)

// InodeCache defines the structure of the inode cache.
type InodeCache struct {
	sync.RWMutex
	cache              map[uint64]*list.Element
	lruList            *list.List
	oldExpiration      time.Duration
	expiration         time.Duration
	maxElements        int
	bgEvictionInterval time.Duration
	useCache           bool
	stopC              chan struct{}
	wg                 sync.WaitGroup

	parent     map[uint64]uint64
	parentLock sync.RWMutex
}

// NewInodeCache returns a new inode cache.
func NewInodeCache(exp time.Duration, maxElements int, bgEvictionInterval time.Duration, useCache bool, parent map[uint64]uint64) *InodeCache {
	ic := &InodeCache{
		cache:              make(map[uint64]*list.Element),
		lruList:            list.New(),
		oldExpiration:      exp,
		expiration:         exp,
		maxElements:        maxElements,
		bgEvictionInterval: bgEvictionInterval,
		useCache:           useCache,
		stopC:              make(chan struct{}),
		parent:             parent,
	}
	if parent == nil {
		ic.parent = make(map[uint64]uint64)
	}
	if useCache {
		ic.wg.Add(1)
		go ic.backgroundEviction()
	}
	return ic
}

// Put puts the given inode info into the inode cache.
func (ic *InodeCache) Put(info *proto.InodeInfo) {
	if !ic.useCache {
		return
	}

	ic.Lock()
	old, ok := ic.cache[info.Inode]
	if ok {
		ic.lruList.Remove(old)
		delete(ic.cache, info.Inode)
	}

	if ic.lruList.Len() >= ic.maxElements {
		ic.evict(true)
	}

	info.SetCacheTime(time.Now().Unix())
	element := ic.lruList.PushFront(info)
	ic.cache[info.Inode] = element
	ic.Unlock()
}

// Get returns the inode info based on the given inode number.
func (ic *InodeCache) Get(ctx context.Context, ino uint64) *proto.InodeInfo {
	if !ic.useCache {
		return nil
	}

	ic.RLock()
	element, ok := ic.cache[ino]
	if !ok {
		ic.RUnlock()
		return nil
	}

	info := element.Value.(*proto.InodeInfo)
	if ic.inodeExpired(info) {
		ic.RUnlock()
		return nil
	}
	ic.RUnlock()
	return info
}

// Delete deletes the inode info based on the given inode number.
func (ic *InodeCache) Delete(ctx context.Context, ino uint64) {
	//log.LogDebugf("InodeCache Delete: ino(%v)", ino)
	if !ic.useCache {
		return
	}

	ic.Lock()
	element, ok := ic.cache[ino]
	if ok {
		ic.lruList.Remove(element)
		delete(ic.cache, ino)
	}
	ic.Unlock()
}

func (ic *InodeCache) Clear() {
	ic.Lock()
	defer ic.Unlock()
	for k := range ic.cache {
		delete(ic.cache, k)
	}
	ic.cache = make(map[uint64]*list.Element)
	ic.lruList.Init()
}

// Foreground eviction cares more about the speed.
// Background eviction evicts all expired items from the cache.
// The caller should grab the WRITE lock of the inode cache.
func (ic *InodeCache) evict(foreground bool) {
	var count int

	for i := 0; i < MinInodeCacheEvictNum; i++ {
		element := ic.lruList.Back()
		if element == nil {
			return
		}

		// For background eviction, if all expired items have been evicted, just return
		// But for foreground eviction, we need to evict at least MinInodeCacheEvictNum inodes.
		// The foreground eviction, does not need to care if the inode has expired or not.
		info := element.Value.(*proto.InodeInfo)
		if !foreground && !ic.inodeExpired(info) {
			return
		}

		ic.lruList.Remove(element)
		delete(ic.cache, info.Inode)
		count++
	}

	// For background eviction, we need to continue evict all expired items from the cache
	if foreground {
		return
	}

	for i := 0; i < MaxInodeCacheEvictNum; i++ {
		element := ic.lruList.Back()
		if element == nil {
			break
		}
		info := element.Value.(*proto.InodeInfo)
		if !ic.inodeExpired(info) {
			break
		}
		ic.lruList.Remove(element)
		delete(ic.cache, info.Inode)
		count++
	}

	// shrink the map manually to reduce memory consumption
	if len(ic.cache) == 0 {
		ic.cache = make(map[uint64]*list.Element)
	}
}

func (ic *InodeCache) backgroundEviction() {
	defer ic.wg.Done()
	t := time.NewTicker(ic.bgEvictionInterval)
	defer t.Stop()
	for {
		select {
		case <-ic.stopC:
			return
		case <-t.C:
			//log.LogInfof("InodeCache: start BG evict")
			//start := time.Now()
			ic.Lock()
			ic.evict(false)
			ic.Unlock()
			//elapsed := time.Since(start)
			//log.LogInfof("InodeCache: done BG evict, cost (%v)ns", elapsed.Nanoseconds())
		}
	}
}

func (ic *InodeCache) Stop() {
	close(ic.stopC)
	ic.wg.Wait()
}

func (ic *InodeCache) inodeExpired(info *proto.InodeInfo) bool {
	expire := int64(ic.expiration / time.Second)
	flock := getFlock(info)
	if flock != nil {
		expire = int64(flock.WaitTime)
	}
	return time.Now().Unix() >= info.CacheTime()+expire
}

func getFlock(info *proto.InodeInfo) (flock *proto.XAttrFlock) {
	xattrs := info.XAttrs()
	if xattrs == nil {
		return
	}
	for _, xattr := range *xattrs {
		if xattr.Name != proto.XATTR_FLOCK {
			continue
		}
		tmpFlock, ok := xattr.Value.(proto.XAttrFlock)
		if !ok || (tmpFlock.ValidTime > 0 && time.Now().Unix() > int64(tmpFlock.ValidTime)) {
			continue
		}
		flock = &tmpFlock
	}
	return
}

func (ic *InodeCache) Parent() map[uint64]uint64 {
	return ic.parent
}

func (ic *InodeCache) PutParent(ino uint64, parentIno uint64) {
	ic.parentLock.Lock()
	ic.parent[ino] = parentIno
	ic.parentLock.Unlock()
}

func (ic *InodeCache) GetParent(ino uint64) (parentIno uint64, ok bool) {
	ic.parentLock.RLock()
	parentIno, ok = ic.parent[ino]
	ic.parentLock.RUnlock()
	return
}

func (ic *InodeCache) DeleteParent(ino uint64) {
	ic.parentLock.Lock()
	delete(ic.parent, ino)
	ic.parentLock.Unlock()
}
