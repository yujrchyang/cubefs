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

package limit

import (
	"context"
	"errors"
)

// ErrLimited limited error for non-blocking
// 非阻塞场景下失败的错误，当资源不足且调用者不希望阻塞时，返回此错误
var ErrLimited = errors.New("limit exceeded")

// Limiter to limit all by key
type Limiter interface {
	// Running returns how many holder are running
	// return -1 if u donot want to implement this
	// 返回当前正在运行（已 acquire 未 release）的 holder 数量
	// 可选的监控/调试接口，非核心功能，不想实现可返回 -1
	Running() int

	// Acquire by this keys, returns error if no available resource
	// Panic if key is unhashable type necessarily
	// 阻塞或非阻塞地申请资源
	// keys ...interface{} 表示支持任意类型 key
	// 需要注意的是 key 必须是可哈希的，即可以作为 map 的 key
	Acquire(keys ...interface{}) error

	// AcquireWithContext exit acquire limit via ctx
	// 支持上下文取消（如超时、手动 cancel），更安全的 acquire 方式
	AcquireWithContext(ctx context.Context, keys ...interface{}) error

	// Release this keys holder
	// Panic if not acquire yet necessarily
	// Panic if key is unhashable type necessarily
	// 释放之前通过 acquire 获取的资源
	Release(keys ...interface{})
}

// ResettableLimiter resetable limiter
// 在 Limiter 基础上增加动态调整容量的能力
type ResettableLimiter interface {
	Limiter

	// Reset the available resource
	// 将可用资源总数重置为 n，适用于运行时动态调整限流阈值的场景（如根据负载自动扩缩容）
	Reset(n int)
}
