package metanode

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path"
	"runtime/debug"
	"strconv"
	"sync"
	"time"

	"github.com/cubefs/cubefs/util/topology"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/raftstore"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"github.com/tiglabs/raft"
	raftproto "github.com/tiglabs/raft/proto"
)

const (
	RecorderPrefix        = "recorder_"
	ExpiredRecorderPrefix = "expired_"
)

type metaRecorder struct {
	recorder    *raftstore.Recorder
	partitionID uint64
	status      int8
	isRecover   bool
	manager     *metadataManager
	stopC       chan bool
	stopOnce    sync.Once
	topoManager *topology.TopologyManager

	persistMutex	sync.Mutex
}

func NewMetaRecorder(cfg *raftstore.RecorderConfig, m *metadataManager) (*metaRecorder, error) {
	dataPath := path.Join(m.rootDir, fmt.Sprintf("%s%d", RecorderPrefix, cfg.PartitionID))
	recorder, err := raftstore.NewRaftRecorder(dataPath, "", cfg)
	if err != nil {
		return nil, err
	}
	mr := &metaRecorder{
		partitionID: recorder.Config().PartitionID,
		recorder:    recorder,
		manager:     m,
		topoManager: m.metaNode.topoManager,
		stopC:       make(chan bool, 0),
	}
	return mr, nil
}

func (mr *metaRecorder) String() string {
	if mr == nil {
		return ""
	}
	return fmt.Sprintf("Vol(%v)ID(%v)Peers(%v)Learners(%v)Recorders(%v)",
		mr.Recorder().VolName(), mr.partitionID, mr.Recorder().GetPeers(), mr.Recorder().GetLearners(), mr.Recorder().GetRecorders())
}

func (mr *metaRecorder) Recorder() *raftstore.Recorder {
	return mr.recorder
}

func (mr *metaRecorder) persist() error {
	mr.persistMutex.Lock()
	defer mr.persistMutex.Unlock()

	return mr.Recorder().Persist()
}

func (mr *metaRecorder) start() (err error) {
	defer func() {
		if err == nil {
			return
		}
		mr.Stop(false)
	}()
	fsm := &raftstore.FunctionalPartitionFsm{
		ApplyFunc:              mr.Recorder().HandleApply,
		ApplyMemberChangeFunc:  mr.ApplyMemberChange,
		SnapshotFunc:           mr.Recorder().HandleRaftSnapshot,
		ApplySnapshotFunc:      mr.Recorder().HandleRaftApplySnapshot,
		AskRollbackFunc:        nil,
		HandleFatalEventFunc:   mr.HandleFatalEvent,
		HandleLeaderChangeFunc: nil,
	}
	if err = mr.Recorder().StartRaft(fsm); err != nil {
		return err
	}
	mr.isRecover = true

	go mr.startRecorderWorker()
	go mr.checkRecoverAfterStart()

	return nil
}

func (mr *metaRecorder) startRecorderWorker() {
	defer func() {
		if r := recover(); r != nil {
			log.LogCriticalf("recorder worker panic(%v) stack: %v", r, string(debug.Stack()))
			msg := fmt.Sprintf("vol(%v) mp(%v) recorderNode(%v) worker occur panic(%v)",
				mr.Recorder().VolName(), mr.Recorder().PartitionID(), mr.Recorder().NodeID(), r)
			exporter.WarningAppendKey(raftstore.RecorderCriticalUmpKey, msg)
		}
	}()
	persistTicker := time.NewTicker(2 * time.Minute)
	updateConfigTicker := time.NewTicker(intervalToUpdateVolTrashExpires)
	defer func() {
		updateConfigTicker.Stop()
		persistTicker.Stop()
	}()

	ctx := context.Background()
	failTruncateCount := 0
	log.LogInfof("recorder(%v) start recorder worker", mr.partitionID)
	for {
		select {
		case <-mr.stopC:
			log.LogInfof("recorder(%v) stop recorder worker", mr.partitionID)
			return

		case <-persistTicker.C:
			applyIndex, err := mr.Recorder().PersistApplyIndex()
			if err != nil {
				log.LogWarnf("recorder(%v) persist apply index(%v) err(%v)", mr.partitionID, applyIndex, err)
				continue
			}
			var truncateIndex uint64
			truncateIndex, err = mr.GetMinTruncateIndex(ctx)
			if err != nil {
				failTruncateCount++
				log.LogWarnf("recorder(%v) get truncate index err(%v) failCnt(%v)", mr.partitionID, err, failTruncateCount)
				if failTruncateCount%10 == 0 {
					msg := fmt.Sprintf("vol(%v) mp(%v) recorderNode(%v) truncate fail cnt(%v) err(%v)",
						mr.Recorder().VolName(), mr.Recorder().PartitionID(), mr.Recorder().NodeID(), failTruncateCount, err)
					exporter.WarningAppendKey(raftstore.RecorderCriticalUmpKey, msg)
				}
				continue
			}
			failTruncateCount = 0
			if truncateIndex == 0 {
				continue
			}
			mr.Recorder().RaftPartition().Truncate(truncateIndex)
			log.LogInfof("recorder(%v) persist apply index to(%v) and truncate WAL to index(%v)", mr.partitionID, applyIndex, truncateIndex)

		case <-updateConfigTicker.C:
			volTopo := mr.topoManager.GetVolume(mr.Recorder().VolName())
			conf := volTopo.Config()
			if conf == nil {
				continue
			}
			// 对集群级别和Volume级别PersistenceMode设置进行合并，优先使用集群级别的设置
			persistenceMode := nodeInfo.PersistenceMode
			if persistenceMode == proto.PersistenceMode_Nil {
				persistenceMode = conf.GetPersistenceMode()
			}
			sync := persistenceMode == proto.PersistenceMode_WriteThrough
			changed := mr.Recorder().WalSync() != sync || mr.Recorder().WALSyncRotate() != sync
			mr.Recorder().SetWALSync(persistenceMode == proto.PersistenceMode_WriteThrough)
			mr.Recorder().SetWALSyncRotate(persistenceMode == proto.PersistenceMode_WriteThrough)
			if changed {
				if err := mr.persist(); err != nil {
					log.LogErrorf("recorder(%v) persist recorder config failed: %v", mr.partitionID, err)
				}
			}
		}
	}
}

func (mr *metaRecorder) checkRecoverAfterStart() {
	defer func() {
		if r := recover(); r != nil {
			log.LogCriticalf("recorder checkRecoverAfterStart panic(%v) stack: %v", r, string(debug.Stack()))
			msg := fmt.Sprintf("vol(%v) mp(%v) recorderNode(%v) checkRecoverAfterStart occur panic(%v)",
				mr.Recorder().VolName(), mr.Recorder().PartitionID(), mr.Recorder().NodeID(), r)
			exporter.WarningAppendKey(raftstore.RecorderCriticalUmpKey, msg)
		}
	}()

	ticker := time.NewTicker(time.Second * 5)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			leaderAddr := mr.Recorder().GetRaftLeader()
			if leaderAddr == "" {
				log.LogErrorf("CheckRecoverAfterStart mr(%v) no leader", mr)
				continue
			}
			leaderApplyID, err := mr.GetLeaderRaftApplyID(leaderAddr)
			if err != nil {
				log.LogErrorf("CheckRecoverAfterStart mr(%v) get leader raft apply id failed: %v", mr, err)
				continue
			}
			applyID := mr.Recorder().GetApplyID()
			if applyID < leaderApplyID {
				log.LogErrorf("CheckRecoverAfterStart mr(%v) apply id(leader:%v, current node:%v)", mr, leaderApplyID, applyID)
				continue
			}
			mr.isRecover = false
			log.LogInfof("CheckRecoverAfterStart mr(%v) recover finish, applyID(leader:%v, current node:%v)", mr, leaderApplyID, applyID)
			return
		case <-mr.stopC:
			return
		}
	}
}

func (mr *metaRecorder) Stop(needPersist bool) {
	mr.stopOnce.Do(func() {
		if mr.stopC != nil {
			close(mr.stopC)
		}
		mr.Recorder().StopRaft()
		if needPersist {
			if err := mr.persist(); err != nil {
				log.LogErrorf("persist recorder(%v) failed when stop: %v", mr.partitionID, err)
			}
		}
		mr.manager.detachRecorder(mr.partitionID)
		return
	})
}

func (mr *metaRecorder) Expired() (err error) {
	mr.Stop(true)
	currentPath := path.Clean(mr.Recorder().MetaPath())
	var newPath = path.Join(path.Dir(currentPath),
		ExpiredRecorderPrefix+path.Base(currentPath)+"_"+strconv.FormatInt(time.Now().Unix(), 10))

	if err := os.Rename(currentPath, newPath); err != nil {
		log.LogErrorf("mark expired recorder fail: recorder(%v) path(%v) newPath(%v) err(%v)", mr.partitionID, currentPath, newPath, err)
		return err
	}
	log.LogInfof("mark expired recorder: recorder(%v) path(%v) newPath(%v)", mr.partitionID, currentPath, newPath)
	return nil
}

func (mr *metaRecorder) HandleFatalEvent(err *raft.FatalError) {
	// Panic while fatal event happen.
	exporter.WarningCritical(fmt.Sprintf("action[HandleFatalEvent] err[%v].", err))
	log.LogFatalf("action[HandleFatalEvent] err[%v].", err)
	panic(err.Err)
}

func (mr *metaRecorder) ApplyMemberChange(confChange *raftproto.ConfChange, index uint64) (resp interface{}, err error) {
	var (
		updated bool
	)
	defer func() {
		if err == nil {
			mr.Recorder().ApplyTo(index)
			mr.Recorder().PersistApplyIndex()
		}
	}()
	switch confChange.Type {
	case raftproto.ConfAddNode:
		req := &proto.AddMetaPartitionRaftMemberRequest{}
		if err = json.Unmarshal(confChange.Context, req); err != nil {
			return
		}
		updated, err = mr.confAddNode(req, index)
	case raftproto.ConfRemoveNode:
		req := &proto.RemoveMetaPartitionRaftMemberRequest{}
		if err = json.Unmarshal(confChange.Context, req); err != nil {
			return
		}
		updated, err = mr.confRemoveNode(req, index)
	case raftproto.ConfUpdateNode:
		//updated, err = mp.confUpdateNode(req, index)
	case raftproto.ConfAddLearner:
		req := &proto.AddMetaPartitionRaftLearnerRequest{}
		if err = json.Unmarshal(confChange.Context, req); err != nil {
			return
		}
		updated, err = mr.confAddLearner(req, index)
	case raftproto.ConfPromoteLearner:
		req := &proto.PromoteMetaPartitionRaftLearnerRequest{}
		if err = json.Unmarshal(confChange.Context, req); err != nil {
			return
		}
		updated, err = mr.confPromoteLearner(req, index)
	case raftproto.ConfAddRecorder:
		req := &proto.AddMetaPartitionRaftRecorderRequest{}
		if err = json.Unmarshal(confChange.Context, req); err != nil {
			return
		}
		updated, err = mr.confAddRecorder(req, index)
	case raftproto.ConfRemoveRecorder:
		req := &proto.RemoveMetaPartitionRaftRecorderRequest{}
		if err = json.Unmarshal(confChange.Context, req); err != nil {
			return
		}
		updated, err = mr.confRemoveRecorder(req, index)
	}
	if err != nil {
		return
	}
	if updated {
		if err = mr.persist(); err != nil {
			log.LogErrorf("action[ApplyMemberChange] pid[%v] err[%v].", mr.partitionID, err)
			return
		}
	}
	return
}

func (mr *metaRecorder) confAddNode(req *proto.AddMetaPartitionRaftMemberRequest, index uint64) (updated bool, err error) {
	updated, err = mr.Recorder().ApplyAddNode(req.PartitionId, req.AddPeer, index)
	return
}

func (mr *metaRecorder) confRemoveNode(req *proto.RemoveMetaPartitionRaftMemberRequest, index uint64) (updated bool, err error) {
	updated, err = mr.Recorder().ApplyRemoveNode(req.PartitionId, req.RemovePeer, index)
	return
}

func (mr *metaRecorder) confAddLearner(req *proto.AddMetaPartitionRaftLearnerRequest, index uint64) (updated bool, err error) {
	updated, err = mr.Recorder().ApplyAddLearner(req.PartitionId, req.AddLearner, index)
	return
}

func (mr *metaRecorder) confPromoteLearner(req *proto.PromoteMetaPartitionRaftLearnerRequest, index uint64) (updated bool, err error) {
	updated, err = mr.Recorder().ApplyPromoteLearner(req.PartitionId, req.PromoteLearner, index)
	return
}

func (mr *metaRecorder) confAddRecorder(req *proto.AddMetaPartitionRaftRecorderRequest, index uint64) (updated bool, err error) {
	updated, err = mr.Recorder().ApplyAddRecorder(req.PartitionId, req.AddRecorder, index)
	return
}

func (mr *metaRecorder) confRemoveRecorder(req *proto.RemoveMetaPartitionRaftRecorderRequest, index uint64) (updated bool, err error) {
	removeSelf := false
	if removeSelf, updated, err = mr.Recorder().ApplyRemoveRecorder(mr.canRemoveSelf, req.PartitionId, req.RemoveRecorder, index); err != nil {
		return
	}

	if removeSelf {
		_ = mr.Recorder().ExpiredRaft()
		_ = mr.manager.expiredRecorder(mr.Recorder().Config().PartitionID)
		updated = false
	}
	log.LogInfof("Finish confRemoveRecorder Recorder(%v) nodeID(%v)  do RaftLog remove peer(%v) ",
		req.PartitionId, mr.Recorder().Config().NodeID, req.RemoveRecorder)
	return
}

func (mr *metaRecorder) canRemoveSelf(cfg *raftstore.RecorderConfig) (canRemove bool, err error) {
	var partition *proto.MetaPartitionInfo
	if partition, err = masterClient.ClientAPI().GetMetaPartition(cfg.PartitionID, cfg.VolName); err != nil {
		log.LogErrorf("action[canRemoveSelf] err[%v]", err)
		return
	}
	canRemove = false
	var existInPeers bool
	for _, peer := range partition.Peers {
		if cfg.NodeID == peer.ID {
			existInPeers = true
		}
	}
	if !existInPeers {
		canRemove = true
		return
	}
	if cfg.NodeID == partition.OfflinePeerID {
		canRemove = true
		return
	}
	return
}

func (mr *metaRecorder) ResetRaftMember(req *proto.ResetMetaRecorderRaftMemberRequest) (err error) {
	var reqData []byte
	if reqData, err = json.Marshal(req); err != nil {
		err = fmt.Errorf("json marshal err(%v)", err)
		return
	}
	log.LogInfof("Start ResetRaftMember: Recorder(%v) nodeID(%v) do RaftLog(%v) ", req.PartitionId, mr.Recorder().NodeID(), string(reqData))
	update := false
	if update, err = mr.Recorder().ResetRaftMember(req.NewPeers); err != nil {
		log.LogErrorf("recorder(%v) reset raft member to (%v) failed: (%v)", mr.partitionID, req.NewPeers, err)
		return
	}
	log.LogInfof("Finish ResetRaftMember: Recorder(%v) nodeID(%v) newPeers(%v) do RaftLog(%v) ",
		req.PartitionId, mr.Recorder().NodeID(), mr.Recorder().GetPeers(), string(reqData))
	if update {
		if err = mr.persist(); err != nil {
			return
		}
	}
	return
}

func (mr *metaRecorder) GetMinTruncateIndex(ctx context.Context) (uint64, error) {
	return mr.Recorder().GetMinTruncateIndex(ctx, mr.GetRemoteTruncateIndex)
}

func (mr *metaRecorder) GetRemoteTruncateIndex(ctx context.Context, target string) (truncateIndex uint64, err error) {
	packet := NewPacketToGetTruncateIndex(ctx, mr.partitionID)
	if packet, err = mr.sendTcpPacket(packet, target); err != nil {
		return
	}
	truncateIndex = binary.BigEndian.Uint64(packet.Data[:8])
	return
}

func (mr *metaRecorder) GetLeaderRaftApplyID(target string) (applyID uint64, err error) {
	packet := NewPacketToGetApplyID(context.Background(), mr.partitionID, false)
	if packet, err = mr.sendTcpPacket(packet, target); err != nil {
		return
	}
	applyID = binary.BigEndian.Uint64(packet.Data[:8])
	return
}

func (mr *metaRecorder) sendTcpPacket(packet *Packet, target string) (resp *Packet, err error) {
	var conn *net.TCPConn
	defer func() {
		if err != nil {
			mr.manager.connPool.PutConnect(conn, ForceClosedConnect)
		} else {
			mr.manager.connPool.PutConnect(conn, NoClosedConnect)
		}
	}()
	conn, err = mr.manager.connPool.GetConnect(target)
	if err != nil {
		return
	}
	if err = packet.WriteToConn(conn, proto.WriteDeadlineTime); err != nil {
		return
	}
	if err = packet.ReadFromConn(conn, proto.ReadDeadlineTime); err != nil {
		return
	}
	if packet.ResultCode != proto.OpOk {
		err = fmt.Errorf("result from(%v) with code 0x%x: %v", target, packet.ResultCode, packet.GetRespData())
		return
	}
	return packet, nil
}

func (mr *metaRecorder) IsEqualCreateMetaRecorderRequest(request *proto.CreateMetaRecorderRequest) (err error) {
	if mr.Recorder().IsEqualCreateRecorderRequest(request.VolName, request.Members, request.Learners, request.Recorders) {
		return nil
	}
	return fmt.Errorf("existed recorder(%v) but request(%v)", mr, request)
}

func (mr *metaRecorder) UpdateStatus() {
	if addr := mr.Recorder().GetRaftLeader(); addr == "" {
		mr.status = proto.Unavailable
	} else {
		mr.status = proto.ReadWrite
	}
}

func (mr *metaRecorder) ResponseLoadMetaPartition(p *Packet) error {
	resp := &proto.MetaPartitionLoadResponse{
		PartitionID: mr.partitionID,
		DoCompare:   true,
	}
	resp.ApplyID = mr.Recorder().GetApplyID()
	data, err := json.Marshal(resp)
	if err != nil {
		return fmt.Errorf("load recorder json marshal err: %v", err)
	}
	p.PacketOkWithBody(data)
	return nil
}