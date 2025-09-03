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

package closer

import (
	"sync"
)

// Closer is the interface for object that can release its resources.
// 提供了一个极简、并发安全的“关闭信号”抽象层，
// 让任何对象都能一次性、优雅地释放资源，并对外广播“我已经关了”
type Closer interface {
	// Close release all resources holded by the object.
	// 触发一次性资源释放
	Close()
	// Done returns a channel that's closed when object was closed.
	// 返回一个只读 channel，关闭后立即可读，用于广播“已关闭”
	Done() <-chan struct{}
}

// Close release all resources holded by the object.
// 如果传入的对象实现了 Closer，就调用他的 Close()，否则什么都不做
func Close(obj interface{}) {
	if obj == nil {
		return
	}
	if c, ok := obj.(Closer); ok {
		c.Close()
	}
}

// New returns a closer.
func New() Closer {
	return &closer{ch: make(chan struct{})}
}

type closer struct {
	once sync.Once     // 保证 close() 只执行一次（并发安全）
	ch   chan struct{} // 关闭后所有监听者立即收到 done 信号
}

func (c *closer) Close() {
	c.once.Do(func() {
		close(c.ch)
	})
}

func (c *closer) Done() <-chan struct{} {
	return c.ch
}
