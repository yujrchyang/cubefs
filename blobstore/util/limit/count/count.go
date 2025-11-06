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

package count

import (
	"context"
	"sync/atomic"

	"github.com/cubefs/cubefs/blobstore/util/limit"
)

/*
 * 不区分 key，而是对整个限流器做全局并发控制，即总量限流
 */

const minusOne = ^uint32(0) // 等价于 -1 的无符号表示（即 0xFFFFFFFF）

type countLimit struct {
	limit   uint32 // 最大并发数
	current uint32 // 当前活跃数（原子操作）
}

// New returns limiter with concurrent n
func New(n int) limit.Limiter {
	return &countLimit{limit: uint32(n)}
}

func (l *countLimit) Running() int {
	return int(atomic.LoadUint32(&l.current))
}

func (l *countLimit) Acquire(keys ...interface{}) error {
	// 原子加一，如果超过了限制再减一
	if atomic.AddUint32(&l.current, 1) > l.limit {
		atomic.AddUint32(&l.current, minusOne)
		return limit.ErrLimited
	}
	return nil
}

func (l *countLimit) AcquireWithContext(ctx context.Context, keys ...interface{}) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	return l.Acquire(keys...)
}

// 原子加一
func (l *countLimit) Release(keys ...interface{}) {
	atomic.AddUint32(&l.current, minusOne)
}

type blockingCountLimit struct {
	ch chan struct{} // 容量为 n 的信号量通道
}

// NewBlockingCount returns limiter with concurrent n
// Blocking acquire if no available concurrence
func NewBlockingCount(n int) limit.Limiter {
	ch := make(chan struct{}, n)
	// 预填充 n 个 token
	for i := 0; i < n; i++ {
		ch <- struct{}{}
	}
	return &blockingCountLimit{ch: ch}
}

func (l *blockingCountLimit) Running() int {
	return cap(l.ch) - len(l.ch)
}

// 阻塞直到拿到 token
func (l *blockingCountLimit) Acquire(keys ...interface{}) error {
	<-l.ch
	return nil
}

func (l *blockingCountLimit) AcquireWithContext(ctx context.Context, keys ...interface{}) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	select {
	case <-l.ch:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func (l *blockingCountLimit) Release(keys ...interface{}) {
	l.ch <- struct{}{}
}
