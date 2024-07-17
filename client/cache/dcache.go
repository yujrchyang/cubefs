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
	"sync"
	"time"
)

// DentryCache defines the dentry cache.
type dentry struct {
	ino   uint64
	typ  uint32
}
type DentryCache struct {
	sync.RWMutex
	cache          map[string]dentry
	expiration     int64
	dentryValidSec uint32
	useCache       bool
}

// NewDentryCache returns a new dentry cache.
func NewDentryCache(dentryValidSec uint32, useCache bool) *DentryCache {
	return &DentryCache{
		cache:          make(map[string]dentry),
		expiration:     time.Now().Unix() + int64(dentryValidSec),
		dentryValidSec: dentryValidSec,
		useCache:       useCache,
	}
}

// Put puts an item into the cache.
func (dc *DentryCache) Put(name string, ino uint64, typ uint32) {
	if dc == nil || !dc.useCache {
		return
	}
	dc.Lock()
	defer dc.Unlock()
	dc.cache[name] = dentry{ino: ino, typ: typ}
	dc.expiration = time.Now().Unix() + int64(dc.dentryValidSec)
}

// Get gets the item from the cache based on the given key.
func (dc *DentryCache) Get(name string) (uint64, uint32, bool) {
	if dc == nil || !dc.useCache {
		return 0, 0, false
	}

	dc.RLock()
	if dc.expiration < time.Now().Unix() {
		dc.RUnlock()
		dc.Lock()
		dc.cache = make(map[string]dentry)
		dc.Unlock()
		return 0, 0, false
	}
	d, ok := dc.cache[name]
	dc.RUnlock()
	if !ok {
		return 0, 0, false
	}
	return d.ino, d.typ, true
}

// Delete deletes the item based on the given key.
func (dc *DentryCache) Delete(name string) {
	if dc == nil || !dc.useCache {
		return
	}
	dc.Lock()
	defer dc.Unlock()
	delete(dc.cache, name)
}

// Count gets the count of cache items.
func (dc *DentryCache) Count() int {
	if dc == nil || !dc.useCache {
		return 0
	}
	dc.RLock()
	defer dc.RUnlock()

	return len(dc.cache)
}

func (dc *DentryCache) IsEmpty() bool {
	if dc == nil || !dc.useCache {
		return true
	}
	dc.RLock()
	defer dc.RUnlock()

	return len(dc.cache) == 0
}

func (dc *DentryCache) IsExpired() bool {
	if dc == nil || !dc.useCache {
		return false
	}
	dc.RLock()
	defer dc.RUnlock()

	return dc.expiration < time.Now().Unix()
}

func (dc *DentryCache) Expiration() int64 {
	if dc == nil || !dc.useCache {
		return time.Now().Unix()
	}
	dc.RLock()
	defer dc.RUnlock()

	return dc.expiration
}

func (dc *DentryCache) ResetExpiration(dentryValidSec uint32) {
	if dc == nil || !dc.useCache {
		return
	}
	dc.Lock()
	defer dc.Unlock()

	dc.expiration = time.Now().Unix() + int64(dentryValidSec)
	dc.dentryValidSec = dentryValidSec
}
