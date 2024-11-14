package raftstore

import (
	"reflect"
	"testing"

	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/assert"
	"github.com/tiglabs/raft"
)

func TestApplyMemberChange(t *testing.T)  {
	peer1 := proto.Peer{ID: 1, Addr: "127.0.0.1", Type: proto.PeerNormal}
	testPeerNormal := proto.Peer{ID: 2, Addr: "127.0.0.2", Type: proto.PeerNormal}
	testPeerRecorder := proto.Peer{ID: 2, Addr: "127.0.0.2", Type: proto.PeerRecorder}
	testCases := []struct{
		name			string
		peers			[]proto.Peer
		recorders		[]string
		resultPeers		[][]proto.Peer	// 0: addNode, 1: removeNode, 2: addRecorder, 3: removeRecorder
		resultRecorders	[][]string		// 0: addNode, 1: removeNode, 2: addRecorder, 3: removeRecorder
		resultUpdate	[]bool			// 0: addNode, 1: removeNode, 2: addRecorder, 3: removeRecorder
	} {
		{
			"no peer",
			[]proto.Peer{peer1},
			[]string{},
			[][]proto.Peer{
				{peer1, testPeerNormal},
				{peer1},
				{peer1, testPeerRecorder},
				{peer1},
			},
			[][]string{
				{},
				{},
				{testPeerRecorder.Addr},
				{},
			},
			[]bool{true, false, true, false},
		},
		{
			"recorder peer",
			[]proto.Peer{peer1, testPeerRecorder},
			[]string{testPeerRecorder.Addr},
			[][]proto.Peer{
				{peer1, testPeerRecorder},
				{peer1, testPeerRecorder},
				{peer1, testPeerRecorder},
				{peer1},
			},
			[][]string{
				{testPeerRecorder.Addr},
				{testPeerRecorder.Addr},
				{testPeerRecorder.Addr},
				{},
			},
			[]bool{false, false, false, true},
		},
		{
			"normal peer",
			[]proto.Peer{peer1, testPeerNormal},
			[]string{},
			[][]proto.Peer{
				{peer1, testPeerNormal},
				{peer1},
				{peer1, testPeerNormal},
				{peer1, testPeerNormal},
			},
			[][]string{
				{},
				{},
				{},
				{},
			},
			[]bool{false, true, false, false},
		},
	}
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			// add node
			r := initRecorder(tt.peers, tt.recorders)
			isUpdate, err := r.ApplyAddNode(1, testPeerNormal, 1)
			assertResult(t, r, isUpdate, tt.resultUpdate[0], err, tt.name, tt.resultPeers[0], tt.resultRecorders[0])
			// remove node
			r = initRecorder(tt.peers, tt.recorders)
			isUpdate, err = r.ApplyRemoveNode(1, testPeerNormal, 1)
			assertResult(t, r, isUpdate, tt.resultUpdate[1], err, tt.name, tt.resultPeers[1], tt.resultRecorders[1])
			// add recorder
			r = initRecorder(tt.peers, tt.recorders)
			isUpdate, err = r.ApplyAddRecorder(1, testPeerRecorder, 1)
			assertResult(t, r, isUpdate, tt.resultUpdate[2], err, tt.name, tt.resultPeers[2], tt.resultRecorders[2])
			// remove recorder
			r = initRecorder(tt.peers, tt.recorders)
			canRemoveSelf := func(cfg *RecorderConfig) (bool, error) {
				return true, nil
			}
			_, isUpdate, err = r.ApplyRemoveRecorder(canRemoveSelf, 1, testPeerRecorder, 1)
			assertResult(t, r, isUpdate, tt.resultUpdate[3], err, tt.name, tt.resultPeers[3], tt.resultRecorders[3])
		})
	}
}

func initRecorder(peers []proto.Peer, recorders []string) *Recorder {
	r := &Recorder{config: &RecorderConfig{
		Peers: make([]proto.Peer, len(peers)),
		Recorders: make([]string, len(recorders)),
		RaftStore: NewMockRaftStore(),
	}}
	copy(r.config.Peers, peers)
	copy(r.config.Recorders, recorders)
	return r
}

func assertResult(t *testing.T, r *Recorder, isUpdate, expectUpdate bool, err error, name string, expectPeers []proto.Peer, expectRecorders []string) {
	assert.NoErrorf(t, err, "test(%v) add node err", name)
	assert.Equalf(t, expectUpdate, isUpdate, "test(%v) add node update", name)
	assert.Truef(t, reflect.DeepEqual(expectPeers, r.GetPeers()), "test(%v) add node peers", name)
	assert.Truef(t, reflect.DeepEqual(expectRecorders, r.GetRecorders()), "test(%v) add node recorders", name)
}

type mockRaftStore struct {
	cfg *raft.Config
}

func (m mockRaftStore) CreatePartition(cfg *PartitionConfig) Partition {
	return nil
}

func (m mockRaftStore) Stop() {
}

func (m mockRaftStore) RaftConfig() *raft.Config {
	return m.cfg
}

func (m mockRaftStore) RaftStatus(raftID uint64) (raftStatus *raft.Status) {
	return nil
}

func (m mockRaftStore) AddNodeWithPort(nodeID uint64, addr string, heartbeat int, replicate int) {
}

func (m mockRaftStore) DeleteNode(nodeID uint64) {
}

func (m mockRaftStore) RaftServer() *raft.RaftServer {
	return nil
}

func (m mockRaftStore) SetSyncWALOnUnstable(enable bool) {
	m.cfg.SyncWALOnUnstable = enable
}

func (m mockRaftStore) IsSyncWALOnUnstable() (enabled bool) {
	return m.cfg.SyncWALOnUnstable
}

func (m mockRaftStore) RaftPath() string {
	return ""
}

func (m mockRaftStore) RaftPort() (heartbeat, replica int, err error) {
	return
}

func NewMockRaftStore() RaftStore {
	return &mockRaftStore{}
}
