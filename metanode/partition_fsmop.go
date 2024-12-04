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
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/util/errors"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

func (mp *metaPartition) initInode(ino *Inode) {
	for {

		time.Sleep(10 * time.Nanosecond)
		select {
		case <-mp.stopC:
			return
		default:
			// check first root inode
			if ok, _ := mp.hasInode(ino.Inode); ok {
				return
			}
			if !mp.raftPartition.IsRaftLeader() {
				continue
			}
			data, err := ino.Marshal()
			if err != nil {
				log.LogFatalf("[initInode] marshal: %s", err.Error())
			}

			ctx := context.Background()
			// put first root inode
			resp, err := mp.submit(ctx, opFSMCreateInode, "", data, nil)
			if err != nil {
				log.LogFatalf("[initInode] raft sync: %s", err.Error())
			}
			p := NewPacket(ctx)
			p.ResultCode = resp.(uint8)
			log.LogDebugf("[initInode] raft sync: response status = %v.",
				p.GetResultMsg())
			return
		}
	}
}

// Not implemented.
func (mp *metaPartition) decommissionPartition() (err error) {
	return
}

func (mp *metaPartition) fsmConfigHasChanged() bool{
	needStore := false

	if mp.CreationType != mp.config.CreationType {
		needStore = true
		mp.config.CreationType = mp.CreationType
	}

	globalConfInfo := getGlobalConfNodeInfo()

	if globalConfInfo.rocksWalFileSize != 0 && globalConfInfo.rocksWalFileSize != mp.config.RocksWalFileSize {
		needStore = true
		mp.config.RocksWalFileSize = globalConfInfo.rocksWalFileSize
	}

	if globalConfInfo.rocksWalMemSize != 0 && globalConfInfo.rocksWalMemSize != mp.config.RocksWalMemSize {
		needStore = true
		mp.config.RocksWalMemSize = globalConfInfo.rocksWalMemSize
	}

	if globalConfInfo.rocksLogSize != 0 && globalConfInfo.rocksLogSize != mp.config.RocksLogFileSize {
		needStore = true
		mp.config.RocksLogFileSize = globalConfInfo.rocksLogSize
	}

	if globalConfInfo.rocksLogReservedTime != 0 && globalConfInfo.rocksLogReservedTime != mp.config.RocksLogReversedTime {
		needStore = true
		mp.config.RocksLogReversedTime = globalConfInfo.rocksLogReservedTime
	}

	if globalConfInfo.rocksLogReservedCnt != 0 && globalConfInfo.rocksLogReservedCnt != mp.config.RocksLogReVersedCnt {
		needStore = true
		mp.config.RocksLogReVersedCnt = globalConfInfo.rocksLogReservedCnt
	}

	if globalConfInfo.rocksWalTTL != 0 && globalConfInfo.rocksWalTTL != mp.config.RocksWalTTL {
		needStore = true
		mp.config.RocksWalTTL = globalConfInfo.rocksWalTTL
	}
	return needStore
}

func (mp *metaPartition) fsmStoreConfig() {
	if mp.fsmConfigHasChanged() {
		_ = mp.PersistMetadata()
	}
}

func (mp *metaPartition) fsmUpdatePartition(end uint64) (status uint8, err error) {
	status = proto.OpOk
	oldEnd := atomic.LoadUint64(&mp.config.End)
	atomic.StoreUint64(&mp.config.End, end)
	defer func() {
		if err != nil {
			log.LogErrorf("fsmUpdatePartition failed:%v", err.Error())
			atomic.StoreUint64(&mp.config.End, oldEnd)
			status = proto.OpDiskErr
		}
	}()
	if err = mp.config.checkMeta(); err != nil {
		return
	}

	if err = mp.config.persist(); err != nil {
		err = errors.NewErrorf("[persistConf] config persist->%s", err.Error())
		return
	}
	return
}

func (mp *metaPartition) confAddNode(req *proto.AddMetaPartitionRaftMemberRequest, index uint64) (updated bool, err error) {
	var (
		heartbeatPort int
		replicaPort   int
	)
	if heartbeatPort, replicaPort, err = mp.getRaftPort(); err != nil {
		return
	}

	hasPeer := false
	for _, peer := range mp.config.Peers {
		if peer.ID == req.AddPeer.ID {
			hasPeer = true
			break
		}
	}
	updated = !hasPeer
	if !updated {
		return
	}
	mp.config.Peers = append(mp.config.Peers, req.AddPeer)
	addr := strings.Split(req.AddPeer.Addr, ":")[0]
	mp.config.RaftStore.AddNodeWithPort(req.AddPeer.ID, addr, heartbeatPort, replicaPort)
	return
}

func (mp *metaPartition) confRemoveNode(req *proto.RemoveMetaPartitionRaftMemberRequest,
	index uint64) (updated bool, err error) {
	canRemoveSelf := true
	if mp.config.NodeId == req.RemovePeer.ID {
		if canRemoveSelf, err = mp.canRemoveSelf(); err != nil {
			return
		}
	}
	peerIndex := -1
	data, _ := json.Marshal(req)
	log.LogInfof("Start RemoveRaftNode  PartitionID(%v) nodeID(%v)  do RaftLog (%v) ",
		req.PartitionId, mp.config.NodeId, string(data))
	for i, peer := range mp.config.Peers {
		if peer.ID == req.RemovePeer.ID && peer.Type == proto.PeerNormal {
			updated = true
			peerIndex = i
			break
		}
	}
	if !updated {
		log.LogInfof("NoUpdate RemoveRaftNode  PartitionID(%v) nodeID(%v)  do RaftLog (%v) ",
			req.PartitionId, mp.config.NodeId, string(data))
		return
	}
	mp.config.Peers = append(mp.config.Peers[:peerIndex], mp.config.Peers[peerIndex+1:]...)
	learnerIndex := -1
	for i, learner := range mp.config.Learners {
		if learner.ID == req.RemovePeer.ID {
			learnerIndex = i
			break
		}
	}
	if learnerIndex != -1 {
		mp.config.Learners = append(mp.config.Learners[:learnerIndex], mp.config.Learners[learnerIndex+1:]...)
	}
	if mp.config.NodeId == req.RemovePeer.ID && canRemoveSelf {
		//if req.ReserveResource {
		//	mp.raftPartition.Stop()
		//} else {
		//	mp.ExpiredRaft()
		//}
		_ = mp.ExpiredRaft()
		_ = mp.manager.expiredPartition(mp.GetBaseConfig().PartitionId)
		updated = false
	}
	log.LogInfof("Finish RemoveRaftNode  PartitionID(%v) nodeID(%v)  do RaftLog (%v) ",
		req.PartitionId, mp.config.NodeId, string(data))
	return
}

func (mp *metaPartition) confAddRecorder(req *proto.AddMetaPartitionRaftRecorderRequest, index uint64) (updated bool, err error) {
	var (
		heartbeatPort int
		replicaPort   int
	)
	if heartbeatPort, replicaPort, err = mp.getRaftPort(); err != nil {
		return
	}

	hasPeer, hasRecorder := false, false
	for _, peer := range mp.config.Peers {
		if peer.ID == req.AddRecorder.ID {
			hasPeer = true
			break
		}
	}
	for _, recorder := range mp.config.Recorders {
		if recorder == req.AddRecorder.Addr {
			hasRecorder = true
		}
	}
	updated = !hasPeer
	if !updated {
		return
	}
	if !hasPeer {
		mp.config.Peers = append(mp.config.Peers, req.AddRecorder)
	}
	if !hasRecorder {
		mp.config.Recorders = append(mp.config.Recorders, req.AddRecorder.Addr)
	}
	addr := strings.Split(req.AddRecorder.Addr, ":")[0]
	mp.config.RaftStore.AddNodeWithPort(req.AddRecorder.ID, addr, heartbeatPort, replicaPort)
	return
}

func (mp *metaPartition) confRemoveRecorder(req *proto.RemoveMetaPartitionRaftRecorderRequest, index uint64) (updated bool, err error) {
	peerIndex, recorderIndex := -1, -1
	data, _ := json.Marshal(req)
	if mp.config.NodeId == req.RemoveRecorder.ID {
		// 假如是本节点曾经是recorder副本，下线后又重新在本节点分配了mp副本，如果是从头回放日志的话，会走到这里，不能返错，否则无法启动，暂时先打印error日志观察情况
		log.LogErrorf("RemoveRaftRecorder PartitionID(%v) nodeID(%v) remove recorder of self ID RaftLog(%v) ", req.PartitionId, mp.config.NodeId, string(data))
	}
	log.LogInfof("Start RemoveRaftRecorder  PartitionID(%v) nodeID(%v)  do RaftLog (%v) ",
		req.PartitionId, mp.config.NodeId, string(data))
	for i, peer := range mp.config.Peers {
		if peer.ID == req.RemoveRecorder.ID && peer.Type == proto.PeerRecorder {
			updated = true
			peerIndex = i
			break
		}
	}
	for i, recorder := range mp.config.Recorders {
		if recorder == req.RemoveRecorder.Addr {
			updated = true
			recorderIndex = i
			break
		}
	}
	if !updated {
		log.LogInfof("NoUpdate RemoveRaftRecorder  PartitionID(%v) nodeID(%v)  do RaftLog (%v) ",
			req.PartitionId, mp.config.NodeId, string(data))
		return
	}
	if peerIndex != -1 {
		mp.config.Peers = append(mp.config.Peers[:peerIndex], mp.config.Peers[peerIndex+1:]...)
	}
	if recorderIndex != -1 {
		mp.config.Recorders = append(mp.config.Recorders[:recorderIndex], mp.config.Recorders[recorderIndex+1:]...)
	}
	log.LogInfof("Finish RemoveRaftRecorder  PartitionID(%v) nodeID(%v)  do RaftLog (%v) ",
		req.PartitionId, mp.config.NodeId, string(data))
	return
}

func (mp *metaPartition) ApplyResetMember(req *proto.ResetMetaPartitionRaftMemberRequest) {
	var (
		newPeers          []proto.Peer
		newLearners       []proto.Learner
		newRecorders	  []string
	)
	data, _ := json.Marshal(req)
	log.LogInfof("Start ResetRaftNode  PartitionID(%v) nodeID(%v)  do RaftLog (%v) ",
		req.PartitionId, mp.config.NodeId, string(data))

	isNewPeer := func(peerID uint64) bool {
		for _, peer := range req.NewPeers {
			if peerID == peer.ID {
				return true
			}
		}
		return false
	}

	newPeers = make([]proto.Peer, 0, len(req.NewPeers))
	newLearners = make([]proto.Learner, 0, len(req.NewPeers))
	newRecorders = make([]string, 0, len(req.NewPeers))
	for _, p := range mp.config.Peers {
		if isNewPeer(p.ID) {
			newPeers = append(newPeers, p)
		}
	}
	for _, l := range mp.config.Learners {
		if isNewPeer(l.ID) {
			newLearners = append(newLearners, l)
		}
	}
	for _, peer := range newPeers {
		if peer.IsRecorder() {
			newRecorders = append(newRecorders, peer.Addr)
		}
	}

	mp.config.Peers = newPeers
	mp.config.Learners = newLearners
	mp.config.Recorders = newRecorders

	log.LogInfof("Finish ResetRaftNode  PartitionID(%v) nodeID(%v)  do RaftLog (%v) ",
		req.PartitionId, mp.config.NodeId, string(data))
	return
}

func (mp *metaPartition) confAddLearner(req *proto.AddMetaPartitionRaftLearnerRequest, index uint64) (updated bool, err error) {
	var (
		heartbeatPort int
		replicaPort   int
	)
	if heartbeatPort, replicaPort, err = mp.getRaftPort(); err != nil {
		return
	}

	addPeer := false
	for _, peer := range mp.config.Peers {
		if peer.ID == req.AddLearner.ID {
			addPeer = true
			break
		}
	}
	if !addPeer {
		peer := proto.Peer{ID: req.AddLearner.ID, Addr: req.AddLearner.Addr}
		mp.config.Peers = append(mp.config.Peers, peer)
	}

	addLearner := false
	for _, learner := range mp.config.Learners {
		if learner.ID == req.AddLearner.ID {
			addLearner = true
			break
		}
	}
	if !addLearner {
		mp.config.Learners = append(mp.config.Learners, req.AddLearner)
	}
	updated = !addPeer || !addLearner
	if !updated {
		return
	}
	addr := strings.Split(req.AddLearner.Addr, ":")[0]
	mp.config.RaftStore.AddNodeWithPort(req.AddLearner.ID, addr, heartbeatPort, replicaPort)
	return
}

func (mp *metaPartition) confPromoteLearner(req *proto.PromoteMetaPartitionRaftLearnerRequest, index uint64) (updated bool, err error) {
	var promoteIndex int
	for i, learner := range mp.config.Learners {
		if learner.ID == req.PromoteLearner.ID {
			updated = true
			promoteIndex = i
			break
		}
	}
	if updated {
		mp.config.Learners = append(mp.config.Learners[:promoteIndex], mp.config.Learners[promoteIndex+1:]...)
	}
	return
}

func (mp *metaPartition) confUpdateNode(req *proto.MetaPartitionDecommissionRequest,
	index uint64) (updated bool, err error) {
	return
}

func (mp *metaPartition) delOldExtentFile(buf []byte) (err error) {
	return
}

func (mp *metaPartition) setExtentDeleteFileCursor(buf []byte) (err error) {
	str := string(buf)
	var (
		fileName string
		cursor   int64
	)
	_, err = fmt.Sscanf(str, "%s %d", &fileName, &cursor)
	if err != nil {
		return
	}
	fp, err := os.OpenFile(path.Join(mp.config.RootDir, fileName), os.O_CREATE|os.O_RDWR,
		0644)
	if err != nil {
		log.LogErrorf("[setExtentDeleteFileCursor] openFile %s failed: %s",
			fileName, err.Error())
		return
	}
	if err = binary.Write(fp, binary.BigEndian, cursor); err != nil {
		log.LogErrorf("[setExtentDeleteFileCursor] write file %s cursor"+
			" failed: %s", fileName, err.Error())
	}
	// TODO Unhandled errors
	fp.Close()
	return
}

func (mp *metaPartition) CanRemoveRaftMember(peer proto.Peer) error {
	for _, learner := range mp.config.Learners {
		if peer.ID == learner.ID && peer.Addr == learner.Addr {
			return nil
		}
	}
	downReplicas := mp.config.RaftStore.RaftServer().GetDownReplicas(mp.config.PartitionId)
	hasExsit := false
	for _, p := range mp.config.Peers {
		if p.ID == peer.ID {
			hasExsit = true
			break
		}
	}
	if !hasExsit {
		return nil
	}

	hasDownReplicasExcludePeer := make([]uint64, 0)
	for _, nodeID := range downReplicas {
		if nodeID.NodeID == peer.ID {
			continue
		}
		hasDownReplicasExcludePeer = append(hasDownReplicasExcludePeer, nodeID.NodeID)
	}

	sumReplicas := len(mp.config.Peers) - len(mp.config.Learners)
	if sumReplicas%2 == 1 {
		if sumReplicas-len(hasDownReplicasExcludePeer) > (sumReplicas/2 + 1) {
			return nil
		}
	} else {
		if sumReplicas-len(hasDownReplicasExcludePeer) >= (sumReplicas/2 + 1) {
			return nil
		}
	}

	return fmt.Errorf("downReplicas(%v) too much,so donnot offline (%v)", downReplicas, peer)
}

func (mp *metaPartition) IsEquareCreateMetaPartitionRequst(request *proto.CreateMetaPartitionRequest) (err error) {
	if len(mp.config.Peers) != len(request.Members) {
		return fmt.Errorf("exsit unavali Partition(%v) partitionHosts(%v) requestHosts(%v)", mp.config.PartitionId, mp.config.Peers, request.Members)
	}
	if mp.config.Start != request.Start || mp.config.End != request.End {
		return fmt.Errorf("exsit unavali Partition(%v) range(%v-%v) requestRange(%v-%v)", mp.config.PartitionId, mp.config.Start, mp.config.End, request.Start, request.End)
	}
	for index, peer := range mp.config.Peers {
		requestPeer := request.Members[index]
		if !requestPeer.IsEqual(peer) {
			return fmt.Errorf("exsit unavali Partition(%v) partitionHosts(%v) requestHosts(%v)", mp.config.PartitionId, mp.config.Peers, request.Members)
		}
	}
	if mp.config.VolName != request.VolName {
		return fmt.Errorf("exsit unavali Partition(%v) VolName(%v) requestVolName(%v)", mp.config.PartitionId, mp.config.VolName, request.VolName)
	}
	if len(mp.config.Learners) != len(request.Learners) {
		return fmt.Errorf("exsit unavali Partition(%v) partitionLearners(%v) requestLearners(%v)", mp.config.PartitionId, mp.config.Learners, request.Learners)
	}
	if len(mp.config.Recorders) != len(request.Recorders) {
		return fmt.Errorf("exsit unavali Partition(%v) partitionRecorders(%v) requestRecorders(%v)", mp.config.PartitionId, mp.config.Recorders, request.Recorders)
	}

	return
}

func (mp *metaPartition) evictExpiredRequestRecords(dbWriteHandle interface{}, evictTimestamp int64) {
	var err error
	mp.reqRecords.EvictByTime(evictTimestamp)
	if mp.HasRocksDBStore() {
		startKey, endKey := []byte{byte(ReqRecordsTable)}, []byte{byte(ReqRecordsTable + 1)}
		dbSnap := mp.db.OpenSnap()
		if dbSnap == nil{
			log.LogErrorf("evictExpiredRequestRecords genSnap failed, partitionID(%v), error:%v",
				mp.config.PartitionId, err)
			return
		}
		defer mp.db.ReleaseSnap(dbSnap)

		if err = mp.db.RangeWithSnap(startKey, endKey, dbSnap, func(k, v []byte) (bool, error) {
			if len(k) != requestInfoRocksDBKeyLen {
				return false, fmt.Errorf("error key len:%v", len(k))
			}
			if int64(binary.BigEndian.Uint64(k[1:requestInfoRocksDBKeyLen])) > evictTimestamp {
				return false, nil
			}
			if err = mp.db.DelItemToBatch(dbWriteHandle, k); err != nil {
				return false, err
			}
			return true, nil
		}); err != nil {
			log.LogErrorf("evictExpiredRequestRecords addDeleteItem to batchWriteHandle failed, partitionID(%v), error:%v",
				mp.config.PartitionId, err)
		}
	}
	log.LogDebugf("evictExpiredRequestRecords, evict timestamp:%v", evictTimestamp)
	return
}

func (mp *metaPartition) fsmCorrectInodesAndDelInodesTotalSize(req *proto.CorrectMPInodesAndDelInodesTotalSizeReq) {
	if !contains(req.NeedCorrectHosts, mp.manager.metaNode.localAddr + ":" + mp.manager.metaNode.listen) {
		log.LogDebugf("fsmCorrectInodesAndDelInodesTotalSize, partition(%v) in local node no need correct", mp.config.PartitionId)
		return
	}

	delInodesOldTotalSize := mp.inodeDeletedTree.GetDelInodesTotalSize()
	inodesOldTotalSize    := mp.inodeTree.GetInodesTotalSize()
	snap := NewSnapshot(mp)

	go func() {
		var (
			wg                 sync.WaitGroup
			inodesTotalSize    uint64
			delInodesTotalSize uint64
		)
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = snap.Range(InodeType, func(item interface{}) (bool, error) {
				ino := item.(*Inode)
				inodesTotalSize += ino.Size
				return true, nil
			})
		}()

		go func() {
			defer wg.Done()
			_ = snap.Range(DelInodeType, func(item interface{}) (bool, error) {
				delIno := item.(*DeletedINode)
				delInodesTotalSize += delIno.Size
				return true, nil
			})
		}()
		wg.Wait()

		if inodesTotalSize >= inodesOldTotalSize {
			mp.updateInodesTotalSize(inodesTotalSize - inodesOldTotalSize, 0)
		} else {
			mp.updateInodesTotalSize(0, inodesOldTotalSize - inodesTotalSize)
		}
		log.LogDebugf("fsmCorrectInodesAndDelInodesTotalSize, correct inodes total size partitionID: %v, changeSize: %v",
			mp.config.PartitionId, inodesTotalSize - inodesOldTotalSize)

		if delInodesTotalSize > delInodesOldTotalSize {
			mp.updateDelInodesTotalSize(delInodesTotalSize - delInodesOldTotalSize, 0)
		} else {
			mp.updateDelInodesTotalSize(0, delInodesOldTotalSize - delInodesTotalSize)
		}
		log.LogDebugf("fsmCorrectInodesAndDelInodesTotalSize, correct delInodes total size partitionID: %v, changeSize: %v",
			mp.config.PartitionId, delInodesTotalSize - delInodesOldTotalSize)
	}()
}