// Copyright 2015 The etcd Authors
// Modified work copyright 2018 The tiglabs Authors.
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

package raft

import (
	"context"
	"fmt"

	"github.com/tiglabs/raft/logger"
	"github.com/tiglabs/raft/proto"
)

func (r *raftFsm) becomeCandidate() {
	if r.state == stateLeader {
		panic(AppPanicError(fmt.Sprintf("[raft->becomeCandidate][%v] invalid transition [leader -> candidate].", r.id)))
	}
	if r.maybeChangeState(UnstableState) && logger.IsEnableDebug() {
		logger.Debug("raft[%v] change rist state to %v cause become candidate", r.id, UnstableState)
	}

	r.step = stepCandidate
	r.reset(r.term+1, 0, false, false)
	r.tick = r.tickElection
	r.vote = r.config.NodeID
	r.state = stateCandidate

	if logger.IsEnableDebug() {
		logger.Debug("raft[%v] became candidate at term %d.", r.id, r.term)
	}
}

func stepCandidate(r *raftFsm, m *proto.Message) {
	switch m.Type {
	case proto.LocalMsgProp:
		if logger.IsEnableDebug() {
			logger.Debug("raft[%v] no leader at term %d; dropping proposal.", r.id, r.term)
		}
		proto.ReturnMessage(m)
		return

	case proto.ReqMsgAppend:
		r.becomeFollower(m.Ctx(), r.term, m.From)
		r.handleAppendEntries(m)
		proto.ReturnMessage(m)
		return

	case proto.ReqMsgHeartBeat:
		r.becomeFollower(m.Ctx(), r.term, m.From)
		return

	case proto.ReqMsgElectAck:
		r.becomeFollower(m.Ctx(), r.term, m.From)
		nmsg := proto.GetMessage()
		nmsg.Type = proto.RespMsgElectAck
		nmsg.To = m.From
		nmsg.SetCtx(m.Ctx())
		r.send(nmsg)
		proto.ReturnMessage(m)
		return

	case proto.ReqMsgVote:
		if logger.IsEnableDebug() {
			logger.Debug("raft[%v] [logterm: %d, index: %d, vote: %v] rejected vote from %v [logterm: %d, index: %d] at term %d.", r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.vote, m.From, m.LogTerm, m.Index, r.term)
		}
		nmsg := proto.GetMessage()
		nmsg.Type = proto.RespMsgVote
		nmsg.To = m.From
		nmsg.Reject = true
		nmsg.SetCtx(m.Ctx())
		r.send(nmsg)
		proto.ReturnMessage(m)
		return

	case proto.RespMsgVote:
		r.maybeUpdateReplica(m.From, m.Index, m.Commit)
		gr := r.poll(m.From, !m.Reject, m.Index, m.LogTerm)
		quorum := r.quorum()
		if logger.IsEnableDebug() {
			logger.Debug("raft[%v] [quorum:%d] has received %d votes and %d vote rejections.", r.id, quorum, gr, len(r.votes)-gr)
		}
		if !r.isCommitReady() {
			gr = len(r.votes) - quorum
		}
		switch quorum {
		case gr:
			if r.needToCompleteEntry(m.Ctx()) {
				proto.ReturnMessage(m)
				return
			}
			if r.config.LeaseCheck {
				r.becomeElectionAck()
			} else {
				r.becomeLeader()
				r.bcastAppend(m.Ctx())
			}
		case len(r.votes) - gr:
			r.becomeFollower(m.Ctx(), r.term, NoLeader)
		}
		proto.ReturnMessage(m)
		return

	case proto.RespCompleteEntry:
		if r.needCompleteEntryTo != 0 {
			r.handleCompleteEntry(m)
		}
		proto.ReturnMessage(m)
		return
		
	}
}
func (r *raftFsm) isCommitReady() bool {
	if r.raftLog.committed < r.startCommit {
		if logger.IsEnableWarn() {
			logger.Warn("raft[%v] cannot campaign at term %d since current raftLog commit %d is less than start commit %d.", r.id, r.term, r.raftLog.committed, r.startCommit)
		}
		return false
	}
	return true
}

func (r *raftFsm) campaign(force bool) {
	r.becomeCandidate()

	li, lt := r.raftLog.lastIndexAndTerm()
	if r.isCommitReady() && r.quorum() == r.poll(r.config.NodeID, true, li, lt) {
		if r.config.LeaseCheck {
			r.becomeElectionAck()
		} else {
			r.becomeLeader()
		}
		return
	}

	if r.needCompleteEntryTo > 0 {
		return
	}

	for id := range r.replicas {
		if id == r.config.NodeID || r.replicas[id].isLearner {
			continue
		}
		if logger.IsEnableDebug() {
			logger.Debug("raft[%v] [logterm: %d, index: %d] [commited:%v,applied:%v] sent vote request to %v at term %d.", r.id, lt, li,r.raftLog.committed,r.raftLog.applied, id, r.term)
		}

		m := proto.GetMessage()
		m.To = id
		m.Type = proto.ReqMsgVote
		m.ForceVote = force
		m.Index = li
		m.LogTerm = lt
		m.Commit = r.raftLog.committed
		r.send(m)
	}
}

func (r *raftFsm) poll(id uint64, vote bool, li, lt uint64) (granted int) {
	if logger.IsEnableDebug() {
		if vote {
			logger.Debug("raft[%v] received vote from %v at term %d.", r.id, id, r.term)
		} else {
			logger.Debug("raft[%v] received vote rejection from %v at term %d.", r.id, id, r.term)
		}
	}
	if _, exist := r.replicas[id]; exist {
		if vInfo, ok := r.votes[id]; !ok {
			r.votes[id] = &voterInfo{
				vote:      vote,
				lastIndex: li,
				lastTerm:  lt,
			}
		} else {
			vInfo.lastIndex = li
			vInfo.lastTerm = lt
		}
	}

	for _, vv := range r.votes {
		if vv.vote {
			granted++
		}
	}
	return granted
}

func (r *raftFsm) needToCompleteEntry(ctx context.Context) bool {
	var maxId, maxLastIndex, maxLastTerm uint64
	for id, vv := range r.votes {
		if vv.vote && ( (vv.lastTerm > maxLastTerm) || ((vv.lastTerm == maxLastTerm) && (vv.lastIndex > maxLastIndex)) ) {
			maxId = id
			maxLastIndex = vv.lastIndex
			maxLastTerm = vv.lastTerm
		}
	}
	li, lt := r.raftLog.lastIndexAndTerm()
	if maxId != 0 && maxId != r.config.NodeID && ((maxLastTerm > lt) || (maxLastIndex > li)) {
		if logger.IsEnableDebug() {
			logger.Debug("ID[%v] raft[%v] needToCompleteEntry from ID[%v] in voters[%v]", r.config.NodeID, r.id, maxId, r.votes)
		}
		pr, ok := r.replicas[maxId]
		if ok && pr.peer.IsRecorder() {
			r.needCompleteEntryTo = maxLastIndex
			nmsg := proto.GetMessage()
			nmsg.Type = proto.ReqCompleteEntry
			nmsg.To = maxId
			nmsg.Index, nmsg.LogTerm = r.raftLog.lastIndexAndTerm()
			nmsg.Commit = r.raftLog.committed
			nmsg.SetCtx(ctx)
			r.send(nmsg)
		}
		// 如果最高水位者既不是自己，也不是recorder或者不在副本内，说明此次选举无效，等待下一轮选举
		return true
	}
	return false
}

func (r *raftFsm) handleCompleteEntry(m *proto.Message) {
	if logger.IsEnableDebug() {
		logger.Debug("ID[%v] raft[%v] [logterm: %d, index: %d, commit: %d] handle RespCompleteEntry [%v], needCompleteEntryTo[%v]",
			r.config.NodeID, r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.raftLog.committed, m.ToString(), r.needCompleteEntryTo)
	}
	if m.Reject {
		// reject说明日志已经truncate，等待下一轮选举(是否需要尝试其它节点?)
		r.needCompleteEntryTo = 0
		return
	}
	if mlastIndex, ok := r.raftLog.maybeAppend(r.config.NodeID, r.id, false, m.Index, m.LogTerm, m.Commit, m.Entries...); ok {
		mlastLogTerm, errt := r.raftLog.term(mlastIndex)
		if errt == nil && mlastIndex < r.needCompleteEntryTo {
			nmsg := proto.GetMessage()
			nmsg.Type = proto.ReqCompleteEntry
			nmsg.To = m.From
			nmsg.Index = mlastIndex
			nmsg.LogTerm = mlastLogTerm
			nmsg.Commit = r.raftLog.committed
			nmsg.SetCtx(m.Ctx())
			r.send(nmsg)
			if logger.IsEnableDebug() {
				logger.Debug("ID[%v] raft[%v] [logterm: %d, index: %d, commit: %d] handle RespCompleteEntry [%v], needCompleteEntryTo[%v]",
					r.config.NodeID, r.id, mlastLogTerm, mlastIndex, r.raftLog.committed, m.ToString(), r.needCompleteEntryTo)
			}
		} else {
			if logger.IsEnableDebug() {
				logger.Debug("ID[%v] raft[%v] has completeEntry to [logterm: %d, index: %d, commit: %d, errt: %v] from msg [%v], needCompleteEntryTo[%v]",
					r.config.NodeID, r.id, mlastLogTerm, mlastIndex, r.raftLog.committed, errt, m.ToString(), r.needCompleteEntryTo)
			}
			r.needCompleteEntryTo = 0
		}
	} else {
		if logger.IsEnableDebug() {
			logger.Debug("ID[%v] raft[%v] [logterm: %d, index: %d, commit: %d] rejected message completeEntry [logterm: %d, index: %d, commit: %d] from %v",
				r.config.NodeID, r.id, r.raftLog.zeroTermOnErrCompacted(r.raftLog.term(m.Index)), m.Index, r.raftLog.committed, m.LogTerm, m.Index, m.Commit, m.From)
		}
		r.needCompleteEntryTo = 0
	}
}