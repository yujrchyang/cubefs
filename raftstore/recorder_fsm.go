package raftstore

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/async"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/unit"
	raftproto "github.com/tiglabs/raft/proto"
)

const (
	MetadataFileName     	= "META"
	TempMetadataFileName	= ".meta"
	ApplyIndexFileName      = "APPLY"
	TempApplyIndexFile      = ".apply"

	defRaftLogSize        	= 8 * unit.MB

	RecorderCriticalUmpKey	= "recorder_critical"
)

type sortedPeers []proto.Peer

func (sp sortedPeers) Len() int {
	return len(sp)
}

func (sp sortedPeers) Less(i, j int) bool {
	return sp[i].ID < sp[j].ID
}

func (sp sortedPeers) Swap(i, j int) {
	sp[i], sp[j] = sp[j], sp[i]
}

type Recorder struct {
	metaPath 		string
	walPath			string
	config   		*RecorderConfig
	raftPartition	Partition
	applyID			uint64
	persistApplyID	uint64
}

type RecorderConfig struct {
	VolName     string          `json:"VolName"`
	PartitionID	uint64          `json:"PartitionID"`
	Peers       []proto.Peer    `json:"Peers"`
	Learners    []proto.Learner	`json:"Learners"`
	Recorders	[]string		`json:"recorders"`
	CreateTime	string			`json:"CreateTime"`
	ClusterID	string			`json:"-"`
	NodeID		uint64			`json:"-"`
	RaftStore   RaftStore 		`json:"-"`

	sync.RWMutex
}

func LoadRecorderConfig(clusterID, recorderDir string, nodeID uint64, rs RaftStore) (cfg *RecorderConfig, err error) {
	var (
		metaFileData	[]byte
	)
	if metaFileData, err = ioutil.ReadFile(path.Join(recorderDir, MetadataFileName)); err != nil {
		return
	}
	cfg = &RecorderConfig{}
	if err = json.Unmarshal(metaFileData, cfg); err != nil {
		return
	}
	cfg.ClusterID = clusterID
	cfg.NodeID = nodeID
	cfg.RaftStore = rs
	if err = cfg.CheckValidate(); err != nil {
		return
	}

	log.LogInfof("load recorder config(%s) from dir(%v)", string(metaFileData), recorderDir)
	return
}

func (cfg *RecorderConfig) CheckValidate() (err error) {
	cfg.VolName = strings.TrimSpace(cfg.VolName)
	if len(cfg.VolName) == 0 {
		err = fmt.Errorf("invalid volume name: %v", cfg.VolName)
		return
	}
	if cfg.PartitionID <= 0 {
		err = fmt.Errorf("invalid partition ID: %v", cfg.PartitionID)
		return
	}
	if len(cfg.Peers) <= 0 {
		err = fmt.Errorf("invalid peers: %v", cfg.Peers)
		return
	}
	if len(cfg.Recorders) <= 0 {
		err = fmt.Errorf("invalid recorders: %v", cfg.Recorders)
		return
	}
	for _, p := range cfg.Peers {
		if p.ID == cfg.NodeID && !p.IsRecorder() {
			err = fmt.Errorf("mismatch peer(%v) peers: %v", p, cfg.Peers)
			return
		}
		if p.IsRecorder() && !contains(cfg.Recorders, p.Addr) {
			err = fmt.Errorf("mismatch peer(%v) recorders: %v", p, cfg.Recorders)
			return
		}
		if !p.IsRecorder() && contains(cfg.Recorders, p.Addr) {
			err = fmt.Errorf("mismatch peer(%v) recorders: %v", p, cfg.Recorders)
			return
		}
	}
	return
}

func NewRaftRecorder(metaPath, walPath string, cfg *RecorderConfig) (r *Recorder, err error) {
	if err = os.MkdirAll(metaPath, 0755); err != nil {
		return nil, fmt.Errorf("mkdir[%v] err[%v]", metaPath, err)
	}
	r = &Recorder{
		metaPath: metaPath,
		walPath:  walPath,
		config:   cfg,
	}
	return
}

func (r *Recorder) RaftPartition() Partition {
	return r.raftPartition
}

func (r *Recorder) VolName() string {
	return r.config.VolName
}

func (r *Recorder) ClusterName() string {
	return r.config.ClusterID
}

func (r *Recorder) PartitionID() uint64 {
	return r.config.PartitionID
}

func (r *Recorder) NodeID() uint64 {
	return r.config.NodeID
}

func (r *Recorder) MetaPath() string {
	return r.metaPath
}

func (r *Recorder) WalPath() string {
	return r.walPath
}

func (r *Recorder) Config() *RecorderConfig {
	return r.config
}

func (r *Recorder) ApplyTo(index uint64) {
	atomic.StoreUint64(&r.applyID, index)
}

func (r *Recorder) GetApplyID() uint64 {
	return atomic.LoadUint64(&r.applyID)
}

func (r *Recorder) Persist() (err error) {
	// todo 加锁
	if _, err = r.PersistApplyIndex(); err != nil {
		return
	}

	originFileName := path.Join(r.metaPath, MetadataFileName)
	tempFileName := path.Join(r.metaPath, TempMetadataFileName)

	sp := sortedPeers(r.GetPeers())
	sort.Sort(sp)

	meta := &RecorderConfig{
		VolName:     r.config.VolName,
		PartitionID: r.config.PartitionID,
		Peers:       r.GetPeers(),
		Learners:    r.GetLearners(),
		Recorders: 	 r.GetRecorders(),
		CreateTime:  r.config.CreateTime,
		NodeID: 	 r.NodeID(),
	}
	if err = meta.CheckValidate(); err != nil {
		msg := fmt.Sprintf("cluster(%v) vol(%v) mp(%v) recorderNode(%v) config invalid(%v), peers(%v) recorders(%v)",
			r.ClusterName(), r.VolName(), r.PartitionID(), r.NodeID(), err, r.GetPeers(), r.GetRecorders())
		exporter.WarningAppendKey(RecorderCriticalUmpKey, msg)
		return
	}

	var newData []byte
	if newData, err = json.Marshal(meta); err != nil {
		return
	}
	var tempFile *os.File
	if tempFile, err = os.OpenFile(tempFileName, os.O_CREATE|os.O_RDWR|os.O_TRUNC|os.O_APPEND, 0666); err != nil {
		return
	}
	defer func() {
		_ = tempFile.Close()
		if err != nil {
			_ = os.Remove(tempFileName)
		}
	}()
	if _, err = tempFile.Write(newData); err != nil {
		return
	}
	if err = tempFile.Sync(); err != nil {
		return
	}
	if err = os.Rename(tempFileName, originFileName); err != nil {
		return
	}
	log.LogInfof("RaftRecorder PersistMetadata recorderID(%v) data(%v)", r.config.PartitionID, string(newData))
	return
}

func (r *Recorder) HandleApply(command []byte, index uint64) (interface{}, error) {
	r.ApplyTo(index)
	return nil, nil
}

func (r *Recorder) HandleRaftSnapshot(recoverNode uint64, isRecorder bool) (raftproto.Snapshot, error) {
	msg := fmt.Sprintf("cluster(%v) vol(%v) mp(%v) recorderNode(%v) become leader and trigger snapshot from node(%v) by mistake, peers(%v)",
		r.ClusterName(), r.VolName(), r.PartitionID(), r.NodeID(), recoverNode, r.GetPeers())
	exporter.WarningAppendKey(RecorderCriticalUmpKey, msg)
	return nil, fmt.Errorf("unsupported snapshot")
}

func (r *Recorder) HandleRaftApplySnapshot(peers []raftproto.Peer, iterator raftproto.SnapIterator, snapV uint32) (err error) {
	// Recorder needn't snapshot.
	log.LogInfof("Recorder(%v) ApplySnapshot from(%v)", r.config.PartitionID, r.raftPartition.CommittedIndex())
	for {
		if _, err = iterator.Next(); err != nil {
			if err != io.EOF {
				log.LogError(fmt.Sprintf("action[ApplySnapshot] Recorder(%v) ApplySnapshot from(%v) failed,err:%v",
					r.config.PartitionID, r.raftPartition.CommittedIndex(), err.Error()))
				return
			}
			return nil
		}
	}
}

func (r *Recorder) StartRaft(fsm *FunctionalPartitionFsm) (err error) {
	var (
		heartbeatPort int
		replicaPort   int
		peers         []PeerAddress
		learners      []raftproto.Learner
	)

	if heartbeatPort, replicaPort, err = r.config.RaftStore.RaftPort(); err != nil {
		return
	}
	for _, peer := range r.GetPeers() {
		addr := strings.Split(peer.Addr, ":")[0]
		rp := PeerAddress{
			Peer: raftproto.Peer{
				ID: 	peer.ID,
				Type:	raftproto.PeerType(peer.Type),
			},
			Address:       addr,
			HeartbeatPort: heartbeatPort,
			ReplicaPort:   replicaPort,
		}
		peers = append(peers, rp)
	}
	for _, learner := range r.GetLearners() {
		addLearner := raftproto.Learner{
			ID:         learner.ID,
			PromConfig: &raftproto.PromoteConfig{AutoPromote: learner.PmConfig.AutoProm, PromThreshold: learner.PmConfig.PromThreshold},
		}
		learners = append(learners, addLearner)
	}
	log.LogInfof("start recorder(%v) raft peers: %s path: %s", r.config.PartitionID, peers, r.metaPath)

	if r.applyID, err = r.loadApplyIndex(); err != nil {
		log.LogWarnf("recorderID(%v) loadApplyIndex err(%v)", r.config.PartitionID, err)
		return
	}
	pc := &PartitionConfig{
		ID:                 r.config.PartitionID,
		Peers:              peers,
		Learners:           learners,
		SM:                 fsm,
		WalPath:            r.walPath,
		StartCommit:        0,
		GetStartIndex: 		func(firstIndex, lastIndex uint64) (startIndex uint64) { return r.applyID },
		WALContinuityCheck: false,
		WALContinuityFix:   false,
		Mode:               proto.StandardMode,
		StorageListener: 	nil,
	}
	r.raftPartition = r.config.RaftStore.CreatePartition(pc)
	r.raftPartition.SetWALFileSize(defRaftLogSize)

	if err = r.raftPartition.Start(); err != nil {
		return
	}

	return
}

func (r *Recorder) StopRaft() {
	if r.raftPartition != nil {
		if log.IsInfoEnabled() {
			log.LogInfof("Recorder(%v) stop raft partition", r.config.PartitionID)
		}
		_ = r.raftPartition.Stop()
	}
}

func (r *Recorder) ExpiredRaft() (err error) {
	if r.raftPartition == nil {
		return fmt.Errorf("recorder [%d] is not start", r.Config().PartitionID)
	}
	err = r.raftPartition.Expired()
	return
}

func (r *Recorder) ApplyAddNode(partitionID uint64, addPeer proto.Peer, index uint64) (isUpdated bool, err error) {
	curPeers := r.GetPeers()
	for _, peer := range curPeers {
		if peer.ID == addPeer.ID {
			log.LogInfof("recorder addRaftNode: recorderID(%v) nodeID(%v) index(%v) existed peer(%v) ", partitionID, r.config.NodeID, index, addPeer)
			return
		}
	}

	var (
		heartbeatPort int
		replicaPort   int
	)
	if heartbeatPort, replicaPort, err = r.config.RaftStore.RaftPort(); err != nil {
		return
	}
	log.LogInfof("recorder addRaftNode: recorderID(%v) nodeID(%v) index(%v) peer(%v) ", partitionID, r.config.NodeID, index, addPeer)
	isUpdated = true
	r.SetPeers(append(curPeers, addPeer))
	addr := strings.Split(addPeer.Addr, ":")[0]
	r.config.RaftStore.AddNodeWithPort(addPeer.ID, addr, heartbeatPort, replicaPort)
	return
}

func (r *Recorder) ApplyRemoveNode(partitionID uint64, removePeer proto.Peer, index uint64) (isUpdated bool, err error) {
	peerIndex, learnerIndex := -1, -1
	isUpdated = false
	if r.config.NodeID == removePeer.ID {
		// 假如是本节点曾经是mp副本，下线后又重新在本节点分配了recorder副本，如果是从头回放日志的话，会走到这里。不能返错，否则无法启动，暂时先打印error日志观察情况
		log.LogErrorf("Recorder PartitionID(%v) nodeID(%v) remove replica of self ID (%v) ", partitionID, r.config.NodeID, removePeer)
	}

	log.LogInfof("Recorder RemoveRaftNode PartitionID(%v) nodeID(%v) removePeer(%v) ", partitionID, r.config.NodeID, removePeer)
	curPeers := r.GetPeers()
	for i, peer := range curPeers {
		if peer.ID == removePeer.ID && peer.Type == proto.PeerNormal {
			peerIndex = i
			isUpdated = true
			break
		}
	}
	curLearners := r.GetLearners()
	for i, learner := range curLearners {
		if learner.ID == removePeer.ID && learner.Addr == removePeer.Addr {
			learnerIndex = i
			isUpdated = true
			break
		}
	}
	if !isUpdated {
		log.LogInfof("Recorder NoUpdate RemoveRaftNode PartitionID(%v) nodeID(%v) removePeer(%v) ", partitionID, r.config.NodeID, removePeer)
		return
	}
	if peerIndex != -1 {
		r.SetPeers(append(curPeers[:peerIndex], curPeers[peerIndex+1:]...))
	}
	if learnerIndex != -1 {
		r.SetLearners(append(curLearners[:learnerIndex], curLearners[learnerIndex+1:]...))
	}
	return
}

func (r *Recorder) ApplyAddLearner(partitionID uint64, addLearner proto.Learner, index uint64) (isUpdated bool, err error) {
	var (
		heartbeatPort int
		replicaPort   int
	)
	if heartbeatPort, replicaPort, err = r.config.RaftStore.RaftPort(); err != nil {
		return
	}

	existedPeer := false
	curPeers := r.GetPeers()
	for _, peer := range curPeers {
		if peer.ID == addLearner.ID {
			existedPeer = true
			break
		}
	}
	isUpdated = !existedPeer
	if !isUpdated {
		return
	}
	if !existedPeer {
		peer := proto.Peer{ID: addLearner.ID, Addr: addLearner.Addr}
		r.SetPeers(append(curPeers, peer))
	}

	existedLearner := false
	curLearners := r.GetLearners()
	for _, learner := range curLearners {
		if learner.ID == addLearner.ID {
			existedLearner = true
			break
		}
	}
	if !existedLearner {
		r.SetLearners(append(curLearners, addLearner))
	}
	addr := strings.Split(addLearner.Addr, ":")[0]
	r.config.RaftStore.AddNodeWithPort(addLearner.ID, addr, heartbeatPort, replicaPort)
	log.LogInfof("recorderID(%v) nodeID(%v) index(%v) addLearner(%v) ", partitionID, r.config.NodeID, index, addLearner)
	return
}

func (r *Recorder) ApplyPromoteLearner(partitionID uint64, promoteLearner proto.Learner, index uint64) (isUpdated bool, err error) {
	var promoteIndex int
	curLearners := r.GetLearners()
	for i, learner := range curLearners {
		if learner.ID == promoteLearner.ID {
			isUpdated = true
			promoteIndex = i
			break
		}
	}
	if isUpdated {
		r.SetLearners(append(curLearners[:promoteIndex], curLearners[promoteIndex+1:]...))
		log.LogInfof("recorderID(%v) nodeID(%v) index(%v) promoteLearner(%v), new learners(%v) ",
			partitionID, r.config.NodeID, index, promoteLearner, curLearners)
	}
	return
}

func (r *Recorder) ApplyAddRecorder(partitionID uint64, addRecorder proto.Peer, index uint64) (isUpdated bool, err error) {
	var peerExisted, recorderExisted bool
	curPeers := r.GetPeers()
	for _, peer := range curPeers {
		if peer.ID == addRecorder.ID {
			peerExisted = true
		}
	}
	curRecorders := r.GetRecorders()
	for _, recorder := range curRecorders {
		if recorder == addRecorder.Addr {
			recorderExisted = true
		}
	}
	if peerExisted {
		log.LogInfof("recorder ApplyAddRecorder: recorderID(%v) nodeID(%v) index(%v) existed peer(%v) ", partitionID, r.config.NodeID, index, addRecorder)
		return
	}

	var (
		heartbeatPort int
		replicaPort   int
	)
	if heartbeatPort, replicaPort, err = r.config.RaftStore.RaftPort(); err != nil {
		return
	}
	log.LogInfof("recorder addRaftNode: recorderID(%v) nodeID(%v) index(%v) peer(%v) ", partitionID, r.config.NodeID, index, addRecorder)
	isUpdated = true
	if !peerExisted {
		r.SetPeers(append(curPeers, addRecorder))
	}
	if !recorderExisted {
		r.SetRecorders(append(curRecorders, addRecorder.Addr))
	}
	addr := strings.Split(addRecorder.Addr, ":")[0]
	r.config.RaftStore.AddNodeWithPort(addRecorder.ID, addr, heartbeatPort, replicaPort)
	return
}

func (r *Recorder) ApplyRemoveRecorder(canRemoveSelf func(cfg *RecorderConfig) (bool, error),
	partitionID uint64, removePeer proto.Peer, index uint64) (removeSelf, isUpdated bool, err error) {
	if r.config.NodeID == removePeer.ID {
		if removeSelf, err = canRemoveSelf(r.Config()); err != nil {
			return
		}
	}

	peerIndex, recorderIndex := -1, -1
	isUpdated = false
	log.LogInfof("Recorder start RemoveRaftNode PartitionID(%v) nodeID(%v) removePeer(%v) ", partitionID, r.config.NodeID, removePeer)
	curPeers := r.GetPeers()
	for i, peer := range curPeers {
		if peer.ID == removePeer.ID && peer.Type == proto.PeerRecorder {
			peerIndex = i
			isUpdated = true
			break
		}
	}
	curRecorders := r.GetRecorders()
	for i, recorder := range curRecorders {
		if recorder == removePeer.Addr {
			recorderIndex = i
			isUpdated = true
			break
		}
	}
	if !isUpdated {
		log.LogInfof("Recorder NoUpdate RemoveRaftNode PartitionID(%v) nodeID(%v) removePeer(%v) ", partitionID, r.config.NodeID, removePeer)
		return
	}

	if peerIndex != -1 {
		r.SetPeers(append(curPeers[:peerIndex], curPeers[peerIndex+1:]...))
	}
	if recorderIndex != -1 {
		r.SetRecorders(append(curRecorders[:recorderIndex], curRecorders[recorderIndex+1:]...))
	}
	return
}

func (r *Recorder) ResetRaftMember(newPeers []proto.Peer) (update bool, err error) {
	for _, peer := range newPeers {
		if !r.HasPeer(peer) {
			err = fmt.Errorf("peer(%v) not exist", peer)
			return
		}
	}
	if len(newPeers) >= len(r.GetPeers()) {
		log.LogInfof("NoUpdate for ResetRaftMember: Recorder(%v) nodeID(%v) newPeers(%v) ", r.config.PartitionID, r.NodeID(), newPeers)
		return
	}

	update = true
	var (
		peers 			[]raftproto.Peer
		newLearners		= make([]proto.Learner, 0, len(newPeers))
		newRecorders	= make([]string, 0, len(newPeers))
	)
	for _, peer := range newPeers {
		peers = append(peers, raftproto.Peer{ID: peer.ID, Type: raftproto.PeerType(peer.Type)})
	}
	if err = r.resetRaftInterMember(peers, nil); err != nil {
		return
	}

	isNewPeer := func(peerID uint64) bool {
		for _, peer := range newPeers {
			if peerID == peer.ID {
				return true
			}
		}
		return false
	}
	for _, l := range r.GetLearners() {
		if isNewPeer(l.ID) {
			newLearners = append(newLearners, l)
		}
	}
	for _, peer := range newPeers {
		if peer.IsRecorder() {
			newRecorders = append(newRecorders, peer.Addr)
		}
	}
	r.SetPeers(newPeers)
	r.SetLearners(newLearners)
	r.SetRecorders(newRecorders)
	return
}

func (r *Recorder) resetRaftInterMember(peers []raftproto.Peer, context []byte) (err error) {
	if r.raftPartition == nil {
		return fmt.Errorf("raft instance not ready")
	}
	err = r.raftPartition.ResetMember(peers, nil, context)
	return
}

func (r *Recorder) GetRaftLeader() (addr string) {
	if r.raftPartition == nil {
		return
	}
	leaderID, _ := r.raftPartition.LeaderTerm()
	if leaderID == 0 {
		return
	}
	if leaderID == r.config.NodeID {
		msg := fmt.Sprintf("cluster(%v) vol(%v) mp(%v) recorderNode(%v) become leader by mistake, peers(%v)",
			r.ClusterName(), r.VolName(), r.PartitionID(), r.NodeID(), r.GetPeers())
		exporter.WarningAppendKey(RecorderCriticalUmpKey, msg)
		return
	}
	for _, peer := range r.GetPeers() {
		if leaderID == peer.ID {
			addr = peer.Addr
			return
		}
	}
	return
}

func (r *Recorder) GetPeers() []proto.Peer {
	r.config.RLock()
	defer r.config.RUnlock()

	return r.config.Peers
}

func (r *Recorder) GetLearners() []proto.Learner {
	r.config.RLock()
	defer r.config.RUnlock()

	return r.config.Learners
}

func (r *Recorder) GetRecorders() []string {
	r.config.RLock()
	defer r.config.RUnlock()

	return r.config.Recorders
}

func (r *Recorder) SetPeers(peers []proto.Peer) {
	r.config.Lock()
	defer r.config.Unlock()

	r.config.Peers = peers
}

func (r *Recorder) SetLearners(learners []proto.Learner) {
	r.config.Lock()
	defer r.config.Unlock()

	r.config.Learners = learners
}

func (r *Recorder) SetRecorders(recorders []string) {
	r.config.Lock()
	defer r.config.Unlock()

	r.config.Recorders = recorders
}

func (r *Recorder) HasPeer(peer proto.Peer) bool {
	for _, p := range r.GetPeers() {
		if p.IsEqual(peer) {
			return true
		}
	}
	return false
}

func (r *Recorder) loadApplyIndex() (index uint64, err error) {
	filename := path.Join(r.metaPath, ApplyIndexFileName)
	var data []byte
	data, err = ioutil.ReadFile(filename)
	if err != nil {
		if os.IsNotExist(err) {
			err = nil
			return
		}
		err = fmt.Errorf("[loadApplyIndex] OpenFile: %s", err.Error())
		return
	}
	if len(data) == 0 {
		err = fmt.Errorf("[loadApplyIndex]: ApplyIndex is empty")
		return
	}
	if _, err = fmt.Sscanf(string(data), "%d", &index); err != nil {
		err = fmt.Errorf("[loadApplyID] ReadApplyID: %s", err.Error())
		return
	}
	return
}

func (r *Recorder) PersistApplyIndex() (index uint64, err error) {
	index = atomic.LoadUint64(&r.applyID)
	if index == atomic.LoadUint64(&r.persistApplyID) {
		return
	}
	originFileName := path.Join(r.metaPath, ApplyIndexFileName)
	tempFileName := path.Join(r.metaPath, TempApplyIndexFile)
	var tempFile *os.File
	if tempFile, err = os.OpenFile(tempFileName, os.O_CREATE|os.O_RDWR|os.O_TRUNC|os.O_APPEND, 0666); err != nil {
		return
	}
	defer func() {
		_ = tempFile.Close()
		if err != nil {
			_ = os.Remove(tempFileName)
		} else {
			atomic.StoreUint64(&r.persistApplyID, index)
		}
	}()
	if _, err = tempFile.WriteString(fmt.Sprintf("%d", index)); err != nil {
		return
	}
	if err = tempFile.Sync(); err != nil {
		return
	}
	if err = os.Rename(tempFileName, originFileName); err != nil {
		return
	}
	return
}

func (r *Recorder) GetMinTruncateIndex(ctx context.Context, getRemoteTruncateIndex func(ctx context.Context, targetHost string) (uint64, error)) (minTruncateIndex uint64, err error) {
	truncateIndexes := make(map[string]uint64)
	futures := make(map[string]*async.Future) 	// host -> future
	for _, peer := range r.GetPeers() {
		if peer.IsRecorder() {
			continue
		}
		var future = async.NewFuture()
		go func(future *async.Future, curAddr string) {
			truncateIndex, err := getRemoteTruncateIndex(ctx, curAddr)
			future.Respond(truncateIndex, err)
		}(future, peer.Addr)
		futures[peer.Addr] = future
	}

	minTruncateIndex = r.raftPartition.AppliedIndex()
	for host, future := range futures {
		resp, respErr := future.Response()
		if respErr != nil {
			log.LogWarnf("recorder(%v) get truncate index from remote(%v) failed: %v, responds(%v)",
				r.config.PartitionID, host, respErr, truncateIndexes)
			err = respErr
			return
		}
		truncateIndex := resp.(uint64)
		truncateIndexes[host] = truncateIndex
		if truncateIndex < minTruncateIndex {
			minTruncateIndex = truncateIndex
		}
	}

	if log.IsDebugEnabled() {
		log.LogDebugf("recorder(%v) get truncate index from all replications, responds(%v)", r.config.PartitionID, truncateIndexes)
	}
	return
}

func (r *Recorder) IsEqualCreateRecorderRequest(volName string, peers []proto.Peer, learners []proto.Learner, recorders []string) bool {
	if len(r.GetPeers()) != len(peers) {
		return false
	}
	sp := sortedPeers(r.GetPeers())
	sort.Sort(sp)
	requestSP := sortedPeers(peers)
	sort.Sort(requestSP)
	for index, peer := range r.GetPeers() {
		requestPeer := peers[index]
		if !requestPeer.IsEqual(peer) {
			return false
		}
	}
	if len(r.GetLearners()) != len(learners) {
		return false
	}
	if len(r.GetRecorders()) != len(recorders) {
		return false
	}
	if r.VolName() != volName {
		return false
	}
	return true
}

func contains(arr []string, element string) (ok bool) {
	if arr == nil || len(arr) == 0 {
		return
	}

	for _, e := range arr {
		if e == element {
			ok = true
			break
		}
	}
	return
}