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

package core

/**
 * 基于逻辑时间戳的同步屏障（synchronization barrier）机制，用于需要强一致性语义或顺序可见性保证的场景
 * 核心思想：
 *   1. 使用一个全局递增的逻辑时间戳（timestamp）为每个请求（request）打上唯一序号
 *   2. 所有请求通过 Begin() 注册，通过 End() 表示完成
 *   3. 调用 Synchronize() 会阻塞，直到所有 时间戳 <= 当前时间戳 的请求都已完成
 *   4. 利用 sync.Cond 实现高效等待/通知
 */

import (
	"container/list"
	"sync"
	"sync/atomic"
)

type Request struct {
	item      interface{}
	timestamp uint64
}

type ConsistencyController struct {
	lock      sync.Mutex
	cond      *sync.Cond
	reqs      list.List
	timestamp uint64
}

// 为请求分配时间戳，并将其插入到链表尾部
func (cc *ConsistencyController) Begin(item interface{}) (elem *list.Element) {
	cc.lock.Lock()
	defer cc.lock.Unlock()

	curtime := atomic.AddUint64(&cc.timestamp, 1)

	req := Request{
		item:      item,
		timestamp: curtime,
	}

	elem = cc.reqs.PushBack(req)

	return elem
}

// 请求完成后从链表中删除并广播
func (cc *ConsistencyController) End(elem *list.Element) {
	cc.lock.Lock()
	defer cc.lock.Unlock()

	cc.reqs.Remove(elem)
	cc.cond.Broadcast()
}

// 在条件变量中等待
func (cc *ConsistencyController) synchronize(anchor uint64) {
	cc.lock.Lock()
	defer cc.lock.Unlock()

loopCheck:
	for e := cc.reqs.Front(); e != nil; {
		req := e.Value.(Request)
		if req.timestamp <= anchor {
			cc.cond.Wait()
			goto loopCheck
		}
		break
	}
}

func (cc *ConsistencyController) CurrentTime() uint64 {
	cc.lock.Lock()
	timestamp := atomic.LoadUint64(&cc.timestamp)
	cc.lock.Unlock()
	return timestamp
}

func (cc *ConsistencyController) Synchronize() uint64 {
	timestamp := cc.CurrentTime()
	cc.synchronize(timestamp)
	return timestamp
}

func NewConsistencyController() (cc *ConsistencyController) {
	cc = &ConsistencyController{}
	cc.cond = sync.NewCond(&cc.lock)
	return cc
}
