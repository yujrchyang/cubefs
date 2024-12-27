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

package metanode

import (
	"fmt"

	"net"
	"strings"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

const (
	ForceClosedConnect = true
	NoClosedConnect    = false
	ProxyReadTimeoutSec  = 2 // Seconds of read timout
	ProxyWriteTimeoutSec = 2
)

// The proxy is used during the leader change. When a leader of a partition changes, the proxy forwards the request to
// the new leader.
func (m *metadataManager) serveProxy(conn net.Conn, mp MetaPartition,
	p *Packet, req interface{}) (ok bool) {
	p.proxyStartTimestamp = time.Now().Unix()
	defer func() {
		p.proxyFinishTimestamp = time.Now().Unix()
	}()

	if p.IsReadMetaPkt() && p.IsFollowerReadMetaPkt() {
		return m.serveProxyFollowerReadPacket(conn, mp, p)
	} else {
		return m.serveProxyLeaderPacket(conn, mp, p, req)
	}
}

func (m *metadataManager) serveProxyLeaderPacket(conn net.Conn, mp MetaPartition, p *Packet, req interface{}) (ok bool){
	var (
		leaderAddr      string
		oldLeaderAddr   string
		err             error
		reqID           = p.ReqID
		reqOp           = p.Opcode
		needTryToLeader = false
	)
	if leaderAddr, ok = mp.IsLeader(); ok {
		if p.IsReadMetaPkt() && mp.IsRaftHang(){
			//disk error, do nothing
			time.Sleep(time.Second * RaftHangTimeOut)
			err = fmt.Errorf("mp[%d] leader raft disk is fault, try anthoer host again", mp.GetBaseConfig().PartitionId)
			p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
			goto end
		}
		return
	}

	if leaderAddr == "" {
		err = ErrNoLeader
		p.PacketErrorWithBody(proto.OpAgain, []byte(err.Error()))
		goto end
	}

	oldLeaderAddr = leaderAddr
	p.ResetPackageData(req)
	if err = m.forwardPacket(leaderAddr, p); err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		if strings.Contains(err.Error(), "i/o timeout") {
			// leader no response, retry and set try to leader flag true
			needTryToLeader = true
		}
		goto end
	}

	if reqID != p.ReqID || reqOp != p.Opcode {
		log.LogErrorf("serveProxy: send and received packet mismatch: mp(%v) req(%v_%v) resp(%v_%v)",
			mp.GetBaseConfig().PartitionId, reqID, reqOp, p.ReqID, p.Opcode)
	}
end:
	if err != nil {
		log.LogErrorf("serveProxy: mp: %v, req: %d - %v, err: %s", mp.GetBaseConfig().PartitionId, p.GetReqID(),
			p.GetOpMsg(), err.Error())
	}
	log.LogDebugf("serveProxy: mp: %v, req: %d - %v, resp: %v", mp.GetBaseConfig().PartitionId, p.GetReqID(), p.GetOpMsg(),
		p.GetResultMsg())

	leaderAddr, _ = mp.IsLeader()
	if leaderAddr == oldLeaderAddr && needTryToLeader && p.ShallTryToLeader() {
		p.PacketErrorWithBody(proto.OpErr, []byte(fmt.Sprintf("proxy to leader[%s] err:%v, try to leader", leaderAddr, err)))
		log.LogErrorf("mp[%v] leader(%s) is not response, now try to elect to be leader", mp.GetBaseConfig().PartitionId, leaderAddr)
		_ = mp.TryToLeader(mp.GetBaseConfig().PartitionId)
	}

	ok = false
	m.respondToClient(conn, p)
	return
}

func (m *metadataManager) serveProxyFollowerReadPacket(conn net.Conn, mp MetaPartition, p *Packet) (ok bool){
	if len(mp.GetBaseConfig().Recorders) == 0 {
		//recorder disabled, no need do read consistent by self,  do follower read
		log.LogDebugf("read from follower: mp(%v), p(%v), arg(%v)", mp.GetBaseConfig().PartitionId, p, p.Arg)
		ok = true
		return
	}

	//already is forward packet, do follower read
	if p.IsForwardFollowerReadMetaPkt() {
		log.LogDebugf("forward follower read packet: mp(%v), p(%v), arg(%v)",
			mp.GetBaseConfig().PartitionId, p, p.Arg)
		ok = true
		return
	}

	//if enable recorder, meta node do read consistent by self
	partition := mp.(*metaPartition)
	isSelf, targetHosts, err := partition.getTargetHostsForReadConsistent()
	if isSelf {
		//do follower read
		log.LogDebugf("do follower read on current node, packet: mp(%v), p(%v), arg(%v)",
			mp.GetBaseConfig().PartitionId, p, p.Arg)
		ok = true
		return
	}

	//read from other host
	defer func() {
		ok = false
		m.respondToClient(conn, p)
	}()
	if err != nil {
		log.LogErrorf("get target hosts failed, packet: mp(%v), p(%v), arg(%v), err(%v)",
			mp.GetBaseConfig().PartitionId, p, p.Arg, err)
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
		return
	}

	if len(targetHosts) == 0 {
		log.LogErrorf("no reachable host for follower read, packet: mp(%v), p(%v), arg(%v)",
			mp.GetBaseConfig().PartitionId, p, p.Arg)
		p.PacketErrorWithBody(proto.OpErr, []byte("no reachable host"))
		return
	}

	//forward to target hosts
	p.ResetPktDataForFollowerReadForward()
	for _, host := range targetHosts {
		err = m.forwardPacket(host, p)
		if err == nil {
			return
		}
		log.LogErrorf("forward failed mp(%v) addr(%v) reqID(%v) err(%v), try next host",
			partition.config.PartitionId, host, p.ReqID, err)
	}

	if err != nil {
		p.PacketErrorWithBody(proto.OpErr, []byte(err.Error()))
	}
	return
}

func (m *metadataManager) forwardPacket(addr string, p *Packet) (err error) {
	var mConn *net.TCPConn
	if mConn, err = m.connPool.GetConnect(addr); err != nil {
		return
	}

	defer func() {
		if err != nil {
			m.connPool.PutConnect(mConn, ForceClosedConnect)
		} else {
			m.connPool.PutConnect(mConn, NoClosedConnect)
		}
	}()

	// forward
	if err = p.WriteToConn(mConn, ProxyWriteTimeoutSec); err != nil {
		log.LogErrorf("write to %s failed, packet: mp(%v), p(%v), arg(%v)", addr, p.PartitionID, p, p.Arg)
		return
	}

	if err = p.ReadFromConn(mConn, ProxyReadTimeoutSec); err != nil {
		log.LogErrorf("read from %s failed, packet: mp(%v), p(%v), arg(%v)", addr, p.PartitionID, p, p.Arg)
		return
	}
	return
}
