package raft

import (
	"context"
	"fmt"
	"math"

	"github.com/tiglabs/raft/logger"
	"github.com/tiglabs/raft/proto"
)

func (r *raftFsm) becomeRecorder(ctx context.Context, term, lead uint64) {
	//if r.maybeChangeState(UnstableState) && logger.IsEnableDebug() {
	//	logger.Debug("raft[%v] change rist state to %v cause become recorder", r.id, UnstableState)
	//}
	r.step = stepRecorder
	r.reset(term, 0, false, false)
	r.tick = r.tickRecord
	r.leader = lead
	r.state = stateRecorder
	r.needCompleteEntryTo = 0
	if logger.IsEnableDebug() {
		logger.Debug("ID[%v] raft[%v] became recorder at term[%d] leader[%d].", r.config.NodeID, r.id, r.term, r.leader)
	}
}

func stepRecorder(r *raftFsm, m *proto.Message) {
	switch m.Type {
	case proto.LocalMsgProp:
		if r.leader == NoLeader {
			if logger.IsEnableWarn() {
				logger.Warn("raft[%v] no leader at term %d; dropping proposal.", r.id, r.term)
			}
			return
		}
		m.To = r.leader
		r.send(m)
		return

	case proto.ReqMsgAppend:
		r.electionElapsed = 0
		r.leader = m.From
		r.handleAppendEntries(m)
		proto.ReturnMessage(m)
		return

	case proto.ReqMsgHeartBeat:
		r.electionElapsed = 0
		r.leader = m.From
		if entry, exist := m.HeartbeatContext.Get(r.id); exist {
			var newState = StableState
			if entry.IsUnstable {
				newState = UnstableState
			}
			if r.maybeChangeState(newState) {
				if logger.IsEnableDebug() {
					logger.Debug("raft[%v] recv risk state change to [%v] from leader [%v].", r.id, newState, m.From)
				}
			}
		}
		return

	case proto.ReqMsgElectAck:
		r.electionElapsed = 0
		r.leader = m.From
		nmsg := proto.GetMessage()
		nmsg.Type = proto.RespMsgElectAck
		nmsg.To = m.From
		nmsg.Index = r.raftLog.lastIndex()
		nmsg.Commit = r.raftLog.committed
		nmsg.SetCtx(m.Ctx())
		r.send(nmsg)
		proto.ReturnMessage(m)
		return

	case proto.ReqCheckQuorum:
		// TODO: remove this
		if logger.IsEnableDebug() {
			logger.Debug("raft[%d] recv check quorum from %d, index=%d", r.id, m.From, m.Index)
		}
		r.electionElapsed = 0
		r.leader = m.From
		nmsg := proto.GetMessage()
		nmsg.Type = proto.RespCheckQuorum
		nmsg.Index = m.Index
		nmsg.To = m.From
		nmsg.SetCtx(m.Ctx())
		r.send(nmsg)
		proto.ReturnMessage(m)
		return

	case proto.ReqMsgVote:
		fpri, lpri := uint16(math.MaxUint16), uint16(0)
		if pr, ok := r.replicas[m.From]; ok {
			fpri = pr.peer.Priority
		}
		if pr, ok := r.replicas[r.config.NodeID]; ok {
			lpri = pr.peer.Priority
		}
		//  logTerm	|	F1	|	F2	|	F3	|	R1	|	R2
		//		4	|		|	99	|		|	99	|
		//		5	|		|		|	99'	|		|	99'
		//		6	|		|	100	|		|	100	|
		// 假设以上场景 F1和F2 宕机，需要 R1和R2 给 F3 投票，此时不能采用正常的投票判断条件(isUpToDate)，而需要比较激进的：只要本轮没投过票给其它节点，就可以投给当前节点
		// 但该策略有可能导致在正常无宕机的情况下，扰乱投票秩序，造成非必要的日志补全消息，需要上线后观察是否有影响
		if (!r.config.LeaseCheck || r.leader == NoLeader) && (r.vote == NoLeader || r.vote == m.From) && (fpri >= lpri) {
			r.electionElapsed = 0
			if logger.IsEnableDebug() {
				logger.Debug("ID[%v] raft[%v] [logterm: %d, index: %d, vote: %v startCommit: %d] voted for %v [logterm: %d, index: %d] at term %d.",
					r.config.NodeID, r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.vote, r.startCommit, m.From, m.LogTerm, m.Index, r.term)
			}
			r.vote = m.From
			nmsg := proto.GetMessage()
			nmsg.Type = proto.RespMsgVote
			nmsg.To = m.From
			nmsg.Index, nmsg.LogTerm = r.raftLog.lastIndexAndTerm()
			nmsg.Commit = r.raftLog.committed
			nmsg.SetCtx(m.Ctx())
			r.send(nmsg)
		} else {
			if logger.IsEnableDebug() {
				logger.Debug("ID[%v] raft[%v] [logterm: %d, index: %d, vote: %v startCommit: %d] rejected vote from %v [logterm: %d, index: %d] at term %d.",
					r.config.NodeID, r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.vote, r.startCommit, m.From, m.LogTerm, m.Index, r.term)
			}
			nmsg := proto.GetMessage()
			nmsg.Type = proto.RespMsgVote
			nmsg.To = m.From
			nmsg.Reject = true
			nmsg.SetCtx(m.Ctx())
			r.send(nmsg)
		}
		proto.ReturnMessage(m)
		return

	case proto.LeaseMsgTimeout:
		proto.ReturnMessage(m)
		return

	case proto.ReqCompleteEntry:
		nmsg := proto.GetMessage()
		nmsg.Type = proto.RespCompleteEntry
		nmsg.To = m.From
		nmsg.Commit = r.raftLog.committed
		nmsg.SetCtx(m.Ctx())
		if r.raftLog.matchTerm(m.Index, m.LogTerm) {
			nmsg.Index = m.Index
		} else {
			// 如果日志不匹配，从对方的commit水位开始复制
			nmsg.Index = m.Commit
		}
		logTerm, errt := r.raftLog.term(nmsg.Index)
		ents, erre := r.raftLog.entries(nmsg.Index+1, r.config.MaxSizePerMsg)
		if errt != nil || erre != nil || len(ents) == 0 {
			errMsg := fmt.Sprintf("ID[%v] [raft->stepRecorder][%v] unexpected errt(%v) erre(%v) getting entries len(%v) from index(%v), from nodeID(%v).",
				r.config.NodeID, r.id, errt, erre, len(ents), nmsg.Index+1, m.From)
			logger.Error(errMsg)
			nmsg.Reject = true	// reject 表示日志已经truncate 或者 index > lastIndex
		} else {
			nmsg.Entries = append(nmsg.Entries, ents...)
			nmsg.LogTerm = logTerm
		}
		if logger.IsEnableDebug() {
			logger.Debug("ID[%v] raft[%v] [logterm: %d, index: %d] reply [%v] for ReqCompleteEntry [%v] at term %d.",
				r.config.NodeID, r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), nmsg.ToString(), m.ToString(), r.term)
		}
		r.send(nmsg)
		proto.ReturnMessage(m)
		return
	}
}

func (r *raftFsm) tickRecord() {
	// do nothing?
}

func (r *raftFsm) bcastGetApplyIndex(truncateIndex uint64) {
	r.appliedIndexes = make(map[uint64]uint64)
	r.appliedIndexes[r.config.NodeID] = truncateIndex
	for id := range r.replicas {
		if id == r.config.NodeID {
			continue
		}
		m := proto.GetMessage()
		m.Type = proto.ReqMsgGetApplyIndex
		m.To = id
		//if logger.IsEnableDebug() {
		//	logger.Debug("ID[%v] raft[%v] check ApplyIndex from[%v] for truncate index[%v]", r.config.NodeID, r.id, id, truncateIndex)
		//}
		r.send(m)
	}
}

func (r *raftFsm) maybeTruncate(m *proto.Message, truncatec chan uint64) {
	defer func() {
		proto.ReturnMessage(m)
	}()

	if r.state != stateRecorder {
		return
	}
	r.appliedIndexes[m.From] = m.Index
	if len(r.appliedIndexes) == len(r.replicas) {
		truncateTo := r.appliedIndexes[r.config.NodeID]
		for _, idx := range r.appliedIndexes {
			if idx < truncateTo {
				truncateTo = idx
			}
		}
		if logger.IsEnableDebug() {
			logger.Debug("ID[%v] raft[%v] truncate to[%v] by appliedIndexes[%v]", r.config.NodeID, r.id, truncateTo, r.appliedIndexes)
		}
		select {
		case truncatec <- truncateTo:
		default:
		}
	}
}