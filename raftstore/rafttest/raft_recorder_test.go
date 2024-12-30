// Copyright 2018 The tiglabs raft Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/tiglabs/raft"
	"github.com/tiglabs/raft/proto"
)

const (
	F1_complete_1	= "F1_complete_1"
	F2_complete_1   = "F2_complete_1"
	F2_complete_2   = "F2_complete_2"
)

var raftTestCasesForRecorder = map[string]raftTestRules{
	F1_complete_1: {
		/* e.g.
			 L    F    F	R    R
		li  100  100  50   50	100
		ci  100	 100  50   50   100
		ai	100	 100  50   50   100
		*/
		name: F1_complete_1,
		msgFilter: func(msg *proto.Message) bool {
			if msg.Type == proto.ReqMsgAppend && (msg.To == 3 || msg.To == 4) && msg.Index > 50 {
				return true
			}
			return false
		},
	},
	F2_complete_1: {
		/* e.g.
			 L    F    F	R    R
		li  100   50   50  100	100
		ci  100	  50   50  100  100
		ai	100	  50   50  100  100
		*/
		name: F2_complete_1,
		msgFilter: func(msg *proto.Message) bool {
			if msg.Type == proto.ReqMsgAppend && (msg.To == 2 || msg.To == 3) && msg.Index > 50 {
				return true
			}
			return false
		},
	},
	F2_complete_2: {
		/* e.g.
			 L    F    F	R    R
		li  200   50   50  150	200
		ci  200	  50   50  150  200
		ai	200	  50   50  150  200
		*/
		name: F2_complete_2,
		msgFilter: func(msg *proto.Message) bool {
			if msg.Type == proto.ReqMsgAppend && (msg.To == 2 || msg.To == 3) && msg.Index > 50 {
				return true
			}
			if msg.Type == proto.ReqMsgAppend && msg.To == 4 && msg.Index > 100 {
				return true
			}
			return false
		},
	},
}

func TestRecorder(t *testing.T) {
	leaderDownTests := []string{
		F2_complete_1,
		F2_complete_2,
	}
	for _, name := range leaderDownTests{
		tt := raftTestCasesForRecorder[name]
		t.Run(tt.name, func(t *testing.T) {
			recorder_filterMsgs_leaderDown(t, tt.name, tt.msgFilter)
		})
	}
	followerDownTests := []string{
		F1_complete_1,
		F2_complete_1,
		F2_complete_2,
	}
	for _, name := range followerDownTests{
		tt := raftTestCasesForRecorder[name]
		t.Run(tt.name, func(t *testing.T) {
			recorder_filterMsgs_2nodeDown(t, tt.name, tt.msgFilter)
		})
	}
}

func recorder_filterMsgs_2nodeDown(t *testing.T, name string, msgFilter raft.MsgFilterFunc) {
	putDataStep := 200
	servers := initTestServerWithMsgFilter(recorderPeers, true, true, 1, raft.StandardMode, msgFilter)
	f, w := getLogFile("", fmt.Sprintf("TestRecorder_2nodeDown_%v.log", name))
	defer func() {
		w.Flush()
		f.Close()
		// end
		for _, s := range servers {
			s.raft.Stop()
		}
	}()

	leadServer := tryToLeader(1, servers, w)
	dataLen := verifyRestoreValue(servers, leadServer, w)
	printStatus(servers, w)

	var (
		futs		map[int]*raft.Future
		err			error
		wg      	sync.WaitGroup
	)

	wg.Add(1)
	go func(startIndex int) {
		defer wg.Done()
		_, futs, err = leadServer.putDataAsync(1, startIndex, putDataStep, w)
	}(dataLen)
	wg.Wait()

	dataLen += putDataStep
	if err != nil {
		output("put data err: %v", err)
		msg := fmt.Sprintf("put data maxKey(%v) err(%v) future len(%v).", dataLen-1, err, len(futs))
		printLog(w, msg)
		t.Fatalf(msg)
	}
	printStatus(servers, w)

	var (
		newServers  []*testServer
		downServers []*testServer
	)
	for i, server := range servers {
		if i < 2 {
			downServers = append(downServers, server)
			server.raft.Stop()
			printLog(w, fmt.Sprintf("stop raft server(%v)", server.nodeID))
			continue
		}
		newServers = append(newServers, server)
	}
	servers = newServers
	for _, s := range servers {
		s.raft.SetMsgFilterFunc(1, raft.DefaultNoMsgFilter)
	}
	waitForApply(servers, 1, w)
	// start down server
	for _, s := range downServers {
		_, servers = startServer(recorderPeers, servers, s, w)
	}
	waitForApply(servers, 1, w)
	// todo 看看日志条数有没有+1，没有的话再提交数据
	// check data
	err = verifyStrictRestoreValue(dataLen, servers, w)
	assert.Equal(t, nil, err, "verify data is inconsistent")
}

func recorder_filterMsgs_leaderDown(t *testing.T, name string, msgFilter raft.MsgFilterFunc) {
	putDataStep := 200

	servers := initTestServerWithMsgFilter(recorderPeers, true, true, 1, raft.StandardMode, msgFilter)
	f, w := getLogFile("", fmt.Sprintf("TestRecorder_LeaderDown_%v.log", name))
	defer func() {
		w.Flush()
		f.Close()
		// end
		for _, s := range servers {
			s.raft.Stop()
		}
	}()

	leadServer := tryToLeader(1, servers, w)
	dataLen := verifyRestoreValue(servers, leadServer, w)
	printStatus(servers, w)

	var (
		futs		map[int]*raft.Future
		err			error
		wg      	sync.WaitGroup
		downServer	*testServer
	)

	wg.Add(1)
	go func(startIndex int) {
		defer wg.Done()
		_, futs, err = leadServer.putDataAsync(1, startIndex, putDataStep, w)
	}(dataLen)
	wg.Wait()

	dataLen += putDataStep
	if err != nil {
		output("put data err: %v", err)
		msg := fmt.Sprintf("put data maxKey(%v) err(%v) future len(%v).", dataLen-1, err, len(futs))
		printLog(w, msg)
		t.Fatalf(msg)
	}
	printStatus(servers, w)
	// stop leader
	downServer, leadServer, servers = stopLeader(servers, w, true)
	for _, s := range servers {
		s.raft.SetMsgFilterFunc(1, raft.DefaultNoMsgFilter)
	}
	waitForApply(servers, 1, w)
	// start down server
	_, servers = startServer(recorderPeers, servers, downServer, w)
	applyIndex := waitForApply(servers, 1, w)
	// check data，减去2条选举产生的空白日志
	err = verifyStrictRestoreValue(int(applyIndex-2), servers, w)
	assert.Equal(t, nil, err, "verify data is inconsistent")
}

func TestRecorder_truncate(t *testing.T) {
	putDataStep := 200
	msgFilter := func(msg *proto.Message) bool {
		if msg.Type == proto.ReqMsgAppend && msg.To == 3 && msg.Index > 50 {
			return true
		}
		return false
	}
	servers := initTestServerWithMsgFilter(recorderPeers, true, true, 1, raft.StandardMode, msgFilter)
	f, w := getLogFile("","TestRecorder_truncate.log")
	defer func() {
		w.Flush()
		f.Close()
		// end
		for _, s := range servers {
			s.raft.Stop()
		}
	}()
	leadServer := tryToLeader(1, servers, w)
	printStatus(servers, w)

	var (
		futs	map[int]*raft.Future
		err		error
		wg  	sync.WaitGroup
	)
	dataLen := 0
	wg.Add(1)
	go func(startIndex int) {
		defer wg.Done()
		_, futs, err = leadServer.putDataAsync(1, startIndex, putDataStep, w)
	}(dataLen)
	dataLen += putDataStep
	wg.Wait()
	if err != nil {
		output("put data err: %v", err)
		msg := fmt.Sprintf("put data maxKey(%v) err(%v) future len(%v).", dataLen-1, err, len(futs))
		printLog(w, msg)
		t.Fatalf(msg)
	}
	printStatus(servers, w)

	var truncateServer *testServer
	actualTruncIndex :=	uint64(dataLen)
	for _, s := range servers {
		if truncateServer == nil && s.raft.Status(1).State == "StateRecorder" {
			truncateServer = s
		}
		if s.raft.Status(1).Applied < actualTruncIndex {
			actualTruncIndex = s.raft.Status(1).Applied
		}
	}
	truncateIndex := truncateServer.raft.CommittedIndex(1) - 1
	truncateServer.raft.Truncate(1, actualTruncIndex)
	time.Sleep(1*time.Second)
	fi, _ := truncateServer.store[1].FirstIndex()
	// 由于nodeID=3落后，因此只能truncate到actualTruncIndex
	assert.Equal(t, actualTruncIndex+1, fi, "get first index for actual truncate index")

	for _, s := range servers {
		s.raft.SetMsgFilterFunc(1, raft.DefaultNoMsgFilter)
	}
	waitForApply(servers, 1, w)
	truncateServer.raft.Truncate(1, truncateIndex)
	time.Sleep(1*time.Second)
	fi, _ = truncateServer.store[1].FirstIndex()
	// 取消nodeID=3的append限制后，可以truncate到指定的truncateIndex
	assert.Equal(t, truncateIndex+1, fi, "get first index")

	wg.Add(1)
	go func(startIndex int) {
		defer wg.Done()
		_, futs, err = leadServer.putDataAsync(1, startIndex, putDataStep, w)
	}(dataLen)
	dataLen += putDataStep
	wg.Wait()
	waitForApply(servers, 1, w)

	// check data
	err = verifyStrictRestoreValue(dataLen, servers, w)
	assert.Equal(t, nil, err, "verify data is inconsistent")
}