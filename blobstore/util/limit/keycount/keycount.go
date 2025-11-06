// Copyright 2022 The CubeFS Authors.
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

package keycount

import (
	"context"
	"sync"

	"github.com/cubefs/cubefs/blobstore/util/limit"
)

/*
 * 按 key 限制并发数，每个 key 最多允许 n 个并发 holder
 */

// 非阻塞限流器，若已达到上限，则返回 ErrLimited
// 适合轻量、快速失败场景
type keyCountLimit struct {
	mutex   sync.Mutex
	limit   uint32                 // 每个 key 的最大并发数
	current map[interface{}]uint32 // 当前每个 key 的活跃计数
}

// New returns limiter with concurrent n by everyone key
// 创建一个每个 key 最多 n 并发的限流器
func New(n int) limit.ResettableLimiter {
	return &keyCountLimit{
		limit:   uint32(n),
		current: make(map[interface{}]uint32),
	}
}

// 返回所有 key 的总和
func (l *keyCountLimit) Running() int {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	all := uint32(0)
	for _, v := range l.current {
		all += v
	}
	return int(all)
}

func (l *keyCountLimit) Acquire(keys ...interface{}) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	// 遍历所有的 key，如果都达到上限了，则返回 ErrLimited
	for _, key := range keys {
		n := l.current[key]
		if n >= l.limit {
			return limit.ErrLimited
		}
	}
	// 自增
	for _, key := range keys {
		l.current[key]++
	}

	return nil
}

// 只做了简单 ctx 检查然后调用 Acquire，所以实际仍是非阻塞的
func (l *keyCountLimit) AcquireWithContext(ctx context.Context, keys ...interface{}) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	return l.Acquire(keys...)
}

func (l *keyCountLimit) Release(keys ...interface{}) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	for _, key := range keys {
		n, ok := l.current[key]
		// key 不存在或释放未获取的 key，panic
		if !ok || n == 0 {
			panic("released by 0")
		}
		// key 释放到 0 时删除这个 key
		if n == 1 {
			delete(l.current, key)
		} else {
			l.current[key]--
		}
	}
}

// 重新设置上限数量
func (l *keyCountLimit) Reset(n int) {
	l.mutex.Lock()
	l.limit = uint32(n)
	l.mutex.Unlock()
}

type blocker struct {
	ref   int32         // 引用计数（有多少 acquire 在用这个 blocker）
	ready chan struct{} // 带缓冲的信号量通道
}

func newBlocker(n int) *blocker {
	s := &blocker{
		ref:   0,
		ready: make(chan struct{}, n),
	}
	// 初始化时发送 n 个消息，表示 n 个令牌已就绪
	for i := 0; i < n; i++ {
		s.ready <- struct{}{}
	}
	return s
}

// 从通道中获取一个消息（令牌），如果没有消息则代表令牌已经取完，会阻塞等待
func (s *blocker) acquire() {
	<-s.ready
}

func (s *blocker) acquireWithCtx(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	select {
	case <-s.ready:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

// 释放即向通道中发送一个消息，在其上阻塞的任务会立即返回
func (s *blocker) release() {
	s.ready <- struct{}{}
}

// 自增通道引用计数，acquire 时调用
func (s *blocker) addRef() {
	s.ref++
}

// 自减通道引用计数，release 时调用，等于 0 时会释放这个 blocker
func (s *blocker) subRef() int32 {
	s.ref--
	return s.ref
}

// 阻塞限流器，若资源不足，会等待知道可用或 context 超时
// 适用于需要“最终获取资源”的场景
// 每个 key 对应一个 带缓冲 channel 的信号量（blocker）
type blockingKeyCountLimit struct {
	lock   sync.RWMutex             // 读写锁
	limit  int                      // 容量
	keyMap map[interface{}]*blocker // 容量为 limit 的 channel，每个 token 代表一个可用许可
}

// NewBlockingKeyCountLimit returns blocking limiter
//
//	with concurrent n by everyone key
func NewBlockingKeyCountLimit(n int) limit.Limiter {
	return &blockingKeyCountLimit{
		limit:  n,
		keyMap: make(map[interface{}]*blocker),
	}
}

func (l *blockingKeyCountLimit) Running() int {
	// 加读锁
	l.lock.RLock()
	defer l.lock.RUnlock()
	// 遍历统计总数
	all := 0
	for _, v := range l.keyMap {
		all += l.limit - len(v.ready)
	}
	return all
}

func (l *blockingKeyCountLimit) Acquire(keys ...interface{}) error {
	if len(keys) == 0 {
		return limit.ErrLimited
	}
	kls := make([]*blocker, 0, len(keys))

	// 加写锁
	l.lock.Lock()
	// 先检查 key 是否存在，不存在则创建
	for _, key := range keys {
		kl, ok := l.keyMap[key]
		if !ok {
			kl = newBlocker(l.limit)
			l.keyMap[key] = kl
		}
		kl.addRef()
		kls = append(kls, kl)
	}
	l.lock.Unlock()

	// 仅获取传入的 key 的令牌
	for _, kl := range kls {
		kl.acquire()
	}
	return nil
}

func (l *blockingKeyCountLimit) AcquireWithContext(ctx context.Context, keys ...interface{}) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if len(keys) == 0 {
		return limit.ErrLimited
	}
	kls := make([]*blocker, 0, len(keys))
	l.lock.Lock()
	for _, key := range keys {
		kl, ok := l.keyMap[key]
		if !ok {
			kl = newBlocker(l.limit)
			l.keyMap[key] = kl
		}
		kl.addRef()
		kls = append(kls, kl)
	}
	l.lock.Unlock()

	for idx, kl := range kls {
		if err := kl.acquireWithCtx(ctx); err != nil {
			l.Release(keys[:idx]...)
			return err
		}
	}
	return nil
}

func (l *blockingKeyCountLimit) Release(keys ...interface{}) {
	kls := make([]*blocker, 0, len(keys))
	l.lock.Lock()
	for _, key := range keys {
		kl, ok := l.keyMap[key]
		// key 不存在，panic
		if !ok {
			l.lock.Unlock()
			panic("key not in map. Possible reason: Release without Acquire.")
		}
		// 释放与获取不匹配，panic
		ref := kl.subRef()
		if ref < 0 {
			l.lock.Unlock()
			panic("internal error: refs < 0")
		}
		// 所有占用都已释放，删除对应 key
		// 需要注意的是：如果有在等待的任务，其在阻塞前会自增 ref
		if ref == 0 {
			delete(l.keyMap, key)
		}
		kls = append(kls, kl)
	}
	l.lock.Unlock()
	for _, kl := range kls {
		kl.release()
	}
}
