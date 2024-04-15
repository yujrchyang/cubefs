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

package datanode

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"net"
	"os"
	"path"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/sdk/http_client"
	"github.com/cubefs/cubefs/util/async"
	"github.com/cubefs/cubefs/util/infra"
	"golang.org/x/time/rate"

	"github.com/cubefs/cubefs/util/topology"

	"github.com/cubefs/cubefs/datanode/riskdata"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/raftstore"
	"github.com/cubefs/cubefs/repl"
	"github.com/cubefs/cubefs/storage"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/holder"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/multirate"
	"github.com/cubefs/cubefs/util/statistics"
	"github.com/cubefs/cubefs/util/unit"
	"github.com/tiglabs/raft"
	raftProto "github.com/tiglabs/raft/proto"
	"github.com/tiglabs/raft/storage/wal"
)

const (
	DataPartitionPrefix           = "datapartition"
	DataPartitionMetadataFileName = "META"
	TempMetadataFileName          = ".meta"
	ApplyIndexFile                = "APPLY"
	TempApplyIndexFile            = ".apply"
	TimeLayout                    = "2006-01-02 15:04:05"
)

type FaultOccurredCheckLevel uint8

const (
	CheckNothing FaultOccurredCheckLevel = iota // default value, no need fault occurred check or check finished
	// CheckQuorumCommitID never persist
	CheckQuorumCommitID // fetch commit with quorum in fault occurred check
	CheckAllCommitID    // fetch commit with all in fault occurred check
)

type DataPartition struct {
	clusterID       string
	volumeID        string
	partitionID     uint64
	partitionStatus int
	partitionSize   int
	replicas        []string // addresses of the replicas
	replicasLock    sync.RWMutex
	disk            *Disk
	isReplLeader    bool
	isRaftLeader    bool
	path            string
	used            int
	extentStore     *storage.ExtentStore
	raftPartition   raftstore.Partition
	config          *dataPartitionCfg

	isCatchUp             bool
	needServerFaultCheck  bool
	serverFaultCheckLevel FaultOccurredCheckLevel
	applyStatus           *WALApplyStatus

	repairPropC              chan struct{}
	updateVolInfoPropC       chan struct{}
	latestPropUpdateReplicas int64 // 记录最近一次申请更新Replicas信息的时间戳，单位为秒

	stopOnce  sync.Once
	stopRaftC chan uint64
	stopC     chan bool

	intervalToUpdateReplicas      int64 // interval to ask the master for updating the replica information
	snapshot                      []*proto.File
	snapshotMutex                 sync.RWMutex
	intervalToUpdatePartitionSize int64
	loadExtentHeaderStatus        int
	FullSyncTinyDeleteTime        int64
	lastSyncTinyDeleteTime        int64
	DataPartitionCreateType       int
	monitorData                   []*statistics.MonitorData
	topologyManager               *topology.TopologyManager
	persistSync                   chan struct{}

	inRepairExtents  map[uint64]struct{}
	inRepairExtentMu sync.Mutex

	persistedApplied  uint64
	persistedMetadata *DataPartitionMetadata

	actionHolder *holder.ActionHolder
	dataFixer    *riskdata.Fixer
}

func (dp *DataPartition) ID() uint64 {
	return dp.partitionID
}

func (dp *DataPartition) AllocateExtentID() (id uint64, err error) {
	id, err = dp.extentStore.NextExtentID()
	return
}

func (dp *DataPartition) IsEquareCreateDataPartitionRequst(request *proto.CreateDataPartitionRequest) (err error) {
	if len(dp.config.Peers) != len(request.Members) {
		return fmt.Errorf("Exsit unavali Partition(%v) partitionHosts(%v) requestHosts(%v)", dp.partitionID, dp.config.Peers, request.Members)
	}
	for index, host := range dp.config.Hosts {
		requestHost := request.Hosts[index]
		if host != requestHost {
			return fmt.Errorf("Exsit unavali Partition(%v) partitionHosts(%v) requestHosts(%v)", dp.partitionID, dp.config.Hosts, request.Hosts)
		}
	}
	sp := sortedPeers(dp.config.Peers)
	sort.Sort(sp)
	requestSP := sortedPeers(request.Members)
	sort.Sort(requestSP)
	for index, peer := range dp.config.Peers {
		requestPeer := request.Members[index]
		if requestPeer.ID != peer.ID || requestPeer.Addr != peer.Addr {
			return fmt.Errorf("Exist unavali Partition(%v) partitionHosts(%v) requestHosts(%v)", dp.partitionID, dp.config.Peers, request.Members)
		}
	}
	for index, learner := range dp.config.Learners {
		requestLearner := request.Learners[index]
		if requestLearner.ID != learner.ID || requestLearner.Addr != learner.Addr {
			return fmt.Errorf("Exist unavali Partition(%v) partitionLearners(%v) requestLearners(%v)", dp.partitionID, dp.config.Learners, request.Learners)
		}
	}
	if dp.config.VolName != request.VolumeId {
		return fmt.Errorf("Exist unavali Partition(%v) VolName(%v) requestVolName(%v)", dp.partitionID, dp.config.VolName, request.VolumeId)
	}
	return
}

func (dp *DataPartition) initIssueProcessor(latestFlushTimeUnix int64) (err error) {
	var fragments []*riskdata.Fragment
	if dp.needServerFaultCheck {
		if fragments, err = dp.scanIssueFragments(latestFlushTimeUnix); err != nil {
			return
		}
	}
	var getRemotes riskdata.GetRemotesFunc = func() []string {
		var replicas = dp.getReplicaClone()
		var remotes = make([]string, 0, len(replicas)-1)
		for _, replica := range replicas {
			if !dp.IsLocalAddress(replica) {
				remotes = append(remotes, replica)
			}
		}
		return remotes
	}
	var getHAType riskdata.GetHATypeFunc = func() proto.CrossRegionHAType {
		return dp.config.VolHAType
	}
	if dp.dataFixer, err = riskdata.NewFixer(dp.partitionID, dp.path, dp.extentStore, getRemotes, getHAType, fragments, gConnPool, dp.disk.Path, dp.limit); err != nil {
		return
	}
	return
}

func (dp *DataPartition) CheckRisk(extentID, offset, size uint64) bool {
	return dp.dataFixer.FindOverlap(extentID, offset, size)
}

func (dp *DataPartition) maybeUpdateFaultOccurredCheckLevel() {
	if maybeServerFaultOccurred {
		dp.setNeedFaultCheck(true)
		_ = dp.persistMetaDataOnly()
	}
}

func newDataPartition(dpCfg *dataPartitionCfg, disk *Disk, isCreatePartition bool, fetchtopoManager *topology.TopologyManager, interceptors storage.IOInterceptors) (dp *DataPartition, err error) {
	partitionID := dpCfg.PartitionID
	dataPath := path.Join(disk.Path, fmt.Sprintf(DataPartitionPrefix+"_%v_%v", partitionID, dpCfg.PartitionSize))
	partition := &DataPartition{
		volumeID:                dpCfg.VolName,
		clusterID:               dpCfg.ClusterID,
		partitionID:             partitionID,
		disk:                    disk,
		path:                    dataPath,
		partitionSize:           dpCfg.PartitionSize,
		replicas:                make([]string, 0),
		repairPropC:             make(chan struct{}, 1),
		updateVolInfoPropC:      make(chan struct{}, 1),
		stopC:                   make(chan bool, 0),
		stopRaftC:               make(chan uint64, 0),
		snapshot:                make([]*proto.File, 0),
		partitionStatus:         proto.ReadWrite,
		config:                  dpCfg,
		DataPartitionCreateType: dpCfg.CreationType,
		monitorData:             statistics.InitMonitorData(statistics.ModelDataNode),
		persistSync:             make(chan struct{}, 1),
		inRepairExtents:         make(map[uint64]struct{}),
		topologyManager:         fetchtopoManager,
		applyStatus:             NewWALApplyStatus(),
		actionHolder:            holder.NewActionHolder(),
	}
	partition.replicasInit()

	var cacheListener storage.CacheListener = func(event storage.CacheEvent, e *storage.Extent) {
		switch event {
		case storage.CacheEvent_Add:
			disk.IncreaseFDCount()
		case storage.CacheEvent_Evict:
			disk.DecreaseFDCount()
		}
	}

	partition.extentStore, err = storage.NewExtentStore(partition.path, dpCfg.PartitionID, dpCfg.PartitionSize, CacheCapacityPerPartition, cacheListener, isCreatePartition, interceptors)
	if err != nil {
		return
	}

	rand.Seed(time.Now().UnixNano())
	partition.FullSyncTinyDeleteTime = time.Now().Unix() + rand.Int63n(3600*24)
	partition.lastSyncTinyDeleteTime = partition.FullSyncTinyDeleteTime
	dp = partition
	return
}

func (dp *DataPartition) RaftStatus() *raftstore.PartitionStatus {
	if dp.raftPartition != nil {
		return dp.raftPartition.Status()
	}
	return nil
}

func (dp *DataPartition) RaftHardState() (hs raftProto.HardState, err error) {
	hs, err = dp.tryLoadRaftHardStateFromDisk()
	return
}

func (dp *DataPartition) tryLoadRaftHardStateFromDisk() (hs raftProto.HardState, err error) {
	var walPath = path.Join(dp.path, "wal_"+strconv.FormatUint(dp.partitionID, 10))
	var metaFile *wal.MetaFile
	if metaFile, hs, _, err = wal.OpenMetaFile(walPath); err != nil {
		return
	}
	_ = metaFile.Close()
	return
}

func (dp *DataPartition) Start() (err error) {
	go func() {
		go dp.statusUpdateScheduler(context.Background())
		if dp.DataPartitionCreateType == proto.DecommissionedCreateDataPartition {
			dp.startRaftAfterRepair()
			return
		}
		dp.startRaftAsync()
	}()
	return
}

func (dp *DataPartition) RiskFixer() *riskdata.Fixer {
	return dp.dataFixer
}

func (dp *DataPartition) replicasInit() {
	replicas := make([]string, 0)
	if dp.config.Hosts == nil {
		return
	}
	for _, host := range dp.config.Hosts {
		replicas = append(replicas, host)
	}
	dp.replicasLock.Lock()
	dp.replicas = replicas
	dp.replicasLock.Unlock()
	if dp.config.Hosts != nil && len(dp.config.Hosts) >= 1 {
		leaderAddr := strings.Split(dp.config.Hosts[0], ":")
		if len(leaderAddr) == 2 && strings.TrimSpace(leaderAddr[0]) == LocalIP {
			dp.isReplLeader = true
		}
	}
}

func (dp *DataPartition) GetExtentCount() int {
	return dp.extentStore.GetExtentCount()
}

func (dp *DataPartition) Path() string {
	return dp.path
}

// IsRaftLeader tells if the given address belongs to the raft leader.
func (dp *DataPartition) IsRaftLeader() (addr string, ok bool) {
	if dp.raftPartition == nil {
		return
	}
	leaderID, _ := dp.raftPartition.LeaderTerm()
	if leaderID == 0 {
		return
	}
	ok = leaderID == dp.config.NodeID
	for _, peer := range dp.config.Peers {
		if leaderID == peer.ID {
			addr = peer.Addr
			return
		}
	}
	return
}

func (dp *DataPartition) IsRaftStarted() bool {
	return dp.raftPartition != nil
}

func (dp *DataPartition) IsLocalAddress(addr string) bool {
	var addrID uint64
	if dp.config == nil {
		return false
	}
	for _, peer := range dp.config.Peers {
		if addr == peer.Addr {
			addrID = peer.ID
			break
		}
	}
	if addrID == dp.config.NodeID {
		return true
	}
	return false
}

func (dp *DataPartition) IsRandomWriteDisabled() (disabled bool) {
	disabled = dp.config.VolHAType == proto.CrossRegionHATypeQuorum
	return
}

func (dp *DataPartition) IsRaftLearner() bool {
	for _, learner := range dp.config.Learners {
		if learner.ID == dp.config.NodeID {
			return true
		}
	}
	return false
}

func (dp *DataPartition) getReplicaClone() (newReplicas []string) {
	dp.replicasLock.RLock()
	defer dp.replicasLock.RUnlock()
	newReplicas = make([]string, len(dp.replicas))
	copy(newReplicas, dp.replicas)
	return
}

func (dp *DataPartition) IsExistReplica(addr string) bool {
	dp.replicasLock.RLock()
	defer dp.replicasLock.RUnlock()
	for _, host := range dp.replicas {
		if host == addr {
			return true
		}
	}
	return false
}

func (dp *DataPartition) IsExistLearner(tarLearner proto.Learner) bool {
	dp.replicasLock.RLock()
	defer dp.replicasLock.RUnlock()
	for _, learner := range dp.config.Learners {
		if learner.Addr == tarLearner.Addr && learner.ID == tarLearner.ID {
			return true
		}
	}
	return false
}

func (dp *DataPartition) ReloadSnapshot() {
	files, err := dp.extentStore.SnapShot()
	if err != nil {
		return
	}
	dp.snapshotMutex.Lock()
	for _, f := range dp.snapshot {
		storage.PutSnapShotFileToPool(f)
	}
	dp.snapshot = files
	dp.snapshotMutex.Unlock()
}

// Snapshot returns the snapshot of the data partition.
func (dp *DataPartition) SnapShot() (files []*proto.File) {
	dp.snapshotMutex.RLock()
	defer dp.snapshotMutex.RUnlock()

	return dp.snapshot
}

// Stop close the store and the raft store.
func (dp *DataPartition) Stop() {
	dp.stopOnce.Do(func() {
		if dp.stopC != nil {
			close(dp.stopC)
		}
		// Close the store and raftstore.
		dp.dataFixer.Stop()
		dp.extentStore.Close()
		dp.stopRaft()
		if err := dp.persist(nil, false); err != nil {
			log.LogErrorf("persist partition [%v] failed when stop: %v", dp.partitionID, err)
		}
	})
	return
}

func (dp *DataPartition) Delete() {
	if dp == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			mesg := fmt.Sprintf("DataPartition(%v) Delete panic(%v)", dp.partitionID, r)
			log.LogWarnf(mesg)
		}
	}()
	dp.Stop()
	dp.Disk().DetachDataPartition(dp)
	if dp.raftPartition != nil {
		_ = dp.raftPartition.Delete()
	} else {
		log.LogWarnf("action[Delete] raft instance not ready! dp:%v", dp.config.PartitionID)
	}
	_ = os.RemoveAll(dp.Path())
}

func (dp *DataPartition) MarkDelete(marker storage.Marker) (err error) {
	err = dp.extentStore.MarkDelete(marker)
	return
}

func (dp *DataPartition) FlushDelete(limit int) (deleted, remain int, err error) {

	const (
		exporterOp            = "FlushDelete"
		ctxKeyExporterTp byte = 0x0
		ctxKeySreTp      byte = 0x01
	)

	var (
		monitorData                    = dp.monitorData[proto.ActionFlushDelete]
		before      storage.BeforeFunc = func() (ctx context.Context, err error) {
			ctx = context.Background()
			err = dp.limit(ctx, proto.OpFlushDelete_, 0, multirate.FlowDisk)
			if err != nil {
				return
			}
			ctx = context.WithValue(ctx, ctxKeyExporterTp, exporter.NewModuleTP(exporterOp))
			ctx = context.WithValue(ctx, ctxKeySreTp, monitorData.BeforeTp())
			return
		}
		after storage.AfterFunc = func(ctx context.Context, n int64, err error) {
			if tp, is := ctx.Value(ctxKeyExporterTp).(exporter.TP); is {
				tp.Set(err)
			}
			if tp, is := ctx.Value(ctxKeySreTp).(*statistics.TpObject); is {
				tp.AfterTp(0)
			}
		}
	)
	deleted, remain, err = dp.extentStore.FlushDelete(storage.NewFuncInterceptor(before, after), limit)
	return
}

func (dp *DataPartition) Expired() {
	if dp == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			mesg := fmt.Sprintf("DataPartition(%v) Expired panic(%v)", dp.partitionID, r)
			log.LogWarnf(mesg)
		}
	}()

	dp.Stop()
	dp.Disk().DetachDataPartition(dp)
	var currentPath = path.Clean(dp.path)
	var newPath = path.Join(path.Dir(currentPath),
		ExpiredPartitionPrefix+path.Base(currentPath)+"_"+strconv.FormatInt(time.Now().Unix(), 10))
	if err := os.Rename(currentPath, newPath); err != nil {
		log.LogErrorf("ExpiredPartition: mark expired partition fail: volume(%v) partitionID(%v) path(%v) newPath(%v) err(%v)",
			dp.volumeID,
			dp.partitionID,
			dp.path,
			newPath,
			err)
		return
	}
	log.LogInfof("ExpiredPartition: mark expired partition: volume(%v) partitionID(%v) path(%v) newPath(%v)",
		dp.volumeID,
		dp.partitionID,
		dp.path,
		newPath)
}

// Disk returns the disk instance.
func (dp *DataPartition) Disk() *Disk {
	return dp.disk
}

func (dp *DataPartition) CheckWritable() error {
	if dp.Disk().Status == proto.Unavailable {
		return storage.BrokenDiskError
	}
	if dp.used > dp.partitionSize*2 || dp.Disk().Status == proto.ReadOnly {
		return storage.NoSpaceError
	}
	return nil
}

const (
	MinDiskSpace = 10 * 1024 * 1024 * 1024
)

func (dp *DataPartition) IsRejectRandomWrite() bool {
	return dp.Disk().Available < MinDiskSpace
}

// Status returns the partition status.
func (dp *DataPartition) Status() int {
	return dp.partitionStatus
}

// Size returns the partition size.
func (dp *DataPartition) Size() int {
	return dp.partitionSize
}

func (dp *DataPartition) SetUnAvailable() {
	dp.partitionStatus = proto.Unavailable
}

// Used returns the used space.
func (dp *DataPartition) Used() int {
	return dp.used
}

// Available returns the available space.
func (dp *DataPartition) Available() int {
	return dp.partitionSize - dp.used
}

func (dp *DataPartition) ForceLoadHeader() {
	dp.loadExtentHeaderStatus = FinishLoadDataPartitionExtentHeader
}

func (dp *DataPartition) proposeRepair() {
	select {
	case dp.repairPropC <- struct{}{}:
	default:
	}
}

func (dp *DataPartition) proposeUpdateVolumeInfo() {
	select {
	case dp.updateVolInfoPropC <- struct{}{}:
	default:
	}
}

func (dp *DataPartition) statusUpdateScheduler(ctx context.Context) {
	rand := rand.New(rand.NewSource(time.Now().UnixNano()))
	repairTimer := time.NewTimer(time.Minute + time.Duration(rand.Intn(120))*time.Second)
	validateCRCTimer := time.NewTimer(DefaultIntervalDataPartitionValidateCRC)
	retryUpdateVolInfoTimer := time.NewTimer(0)
	retryUpdateVolInfoTimer.Stop()
	persistDpLastUpdateTimer := time.NewTimer(time.Hour) //for persist dp lastUpdateTime
	var index int
	for {

		select {
		case <-dp.stopC:
			repairTimer.Stop()
			validateCRCTimer.Stop()
			return

		case <-dp.repairPropC:
			repairTimer.Stop()
			log.LogDebugf("partition(%v) execute manual data repair for all extent", dp.partitionID)
			dp.ExtentStore().MoveAllToBrokenTinyExtentC(proto.TinyExtentCount)
			dp.runRepair(ctx, proto.TinyExtentType)
			dp.runRepair(ctx, proto.NormalExtentType)
			repairTimer.Reset(time.Minute)
		case <-repairTimer.C:
			index++
			dp.statusUpdate()
			if index >= math.MaxUint32 {
				index = 0
			}
			if err := dp.updateReplicas(); err != nil {
				log.LogWarnf("DP[%v] update replicas failed: %v", dp.partitionID, err)
				repairTimer.Reset(time.Minute)
				continue
			}
			if index%2 == 0 {
				dp.runRepair(ctx, proto.TinyExtentType)
			} else {
				dp.runRepair(ctx, proto.NormalExtentType)
			}
			repairTimer.Reset(time.Minute)
		case <-validateCRCTimer.C:
			dp.runValidateCRC(ctx)
			validateCRCTimer.Reset(DefaultIntervalDataPartitionValidateCRC)
		case <-dp.updateVolInfoPropC:
			if err := dp.updateVolumeInfoFromMaster(); err != nil {
				retryUpdateVolInfoTimer.Reset(time.Minute)
			}
		case <-retryUpdateVolInfoTimer.C:
			if err := dp.updateVolumeInfoFromMaster(); err != nil {
				retryUpdateVolInfoTimer.Reset(time.Minute)
			}
		case <-persistDpLastUpdateTimer.C:
			_ = dp.persistMetaDataOnly()
			persistDpLastUpdateTimer.Reset(time.Hour)
		}
	}
}

func (dp *DataPartition) updateVolumeInfoFromMaster() (err error) {
	var simpleVolView *proto.SimpleVolView
	if simpleVolView, err = MasterClient.AdminAPI().GetVolumeSimpleInfo(dp.volumeID); err != nil {
		return
	}
	// Process CrossRegionHAType
	var changed bool
	if dp.config.VolHAType != simpleVolView.CrossRegionHAType {
		dp.config.VolHAType = simpleVolView.CrossRegionHAType
		changed = true
	}
	if dp.config.ReplicaNum != int(simpleVolView.DpReplicaNum) {
		dp.config.ReplicaNum = int(simpleVolView.DpReplicaNum)
		changed = true
	}
	if changed {
		if err = dp.persistMetaDataOnly(); err != nil {
			return
		}
	}
	return
}

func (dp *DataPartition) statusUpdate() {
	status := proto.ReadWrite
	dp.computeUsage()

	if dp.used >= dp.partitionSize {
		status = proto.ReadOnly
	}
	if dp.extentStore.GetExtentCount() >= storage.MaxExtentCount {
		status = proto.ReadOnly
	}
	if dp.Status() == proto.Unavailable {
		status = proto.Unavailable
	}

	dp.partitionStatus = int(math.Min(float64(status), float64(dp.disk.Status)))
}

func (dp *DataPartition) computeUsage() {
	if time.Now().Unix()-dp.intervalToUpdatePartitionSize < IntervalToUpdatePartitionSize {
		return
	}
	dp.used = int(dp.ExtentStore().GetStoreUsedSize())
	dp.intervalToUpdatePartitionSize = time.Now().Unix()
}

func (dp *DataPartition) ExtentStore() *storage.ExtentStore {
	return dp.extentStore
}

func (dp *DataPartition) checkIsPartitionError(err error) (partitionError bool) {
	if err == nil {
		return
	}
	if IsDiskErr(err) {
		mesg := fmt.Sprintf("disk path %v error on %v", dp.Path(), LocalIP)
		exporter.Warning(mesg)
		log.LogErrorf(mesg)
		dp.SetUnAvailable()
		dp.stopRaft()
		dp.statusUpdate()
		partitionError = true
	}
	return
}

// String returns the string format of the data partition information.
func (dp *DataPartition) String() (m string) {
	return fmt.Sprintf(DataPartitionPrefix+"_%v_%v", dp.partitionID, dp.partitionSize)
}

// runRepair launches the repair of extents.
func (dp *DataPartition) runRepair(ctx context.Context, extentType uint8) {

	/*	if dp.partitionStatus == proto.Unavailable {
		return
	}*/

	if !dp.isReplLeader {
		return
	}
	if dp.extentStore.BrokenTinyExtentCnt() == 0 {
		dp.extentStore.MoveAllToBrokenTinyExtentC(MinTinyExtentsToRepair)
	}
	dp.repair(ctx, extentType)
}

func (dp *DataPartition) updateReplicas() (err error) {
	var isLeader bool
	replicas := make([]string, 0)
	dp.backendRefreshCacheView()
	partition, err := dp.getCacheView()
	if err != nil {
		return
	}
	for _, host := range partition.Hosts {
		replicas = append(replicas, host)
	}
	if partition.Hosts != nil && len(partition.Hosts) >= 1 {
		leaderAddr := strings.Split(partition.Hosts[0], ":")
		if len(leaderAddr) == 2 && strings.TrimSpace(leaderAddr[0]) == LocalIP {
			isLeader = true
		}
	}
	dp.replicasLock.Lock()
	defer dp.replicasLock.Unlock()
	if !dp.compareReplicas(dp.replicas, replicas) {
		log.LogInfof("action[updateReplicas] partition(%v) replicas changed from(%v) to(%v).",
			dp.partitionID, dp.replicas, replicas)
	}
	dp.isReplLeader = isLeader
	dp.replicas = replicas
	dp.intervalToUpdateReplicas = time.Now().Unix()
	log.LogInfof(fmt.Sprintf("ActionUpdateReplicationHosts partiton(%v)", dp.partitionID))

	return
}

// Compare the fetched replica with the local one.
func (dp *DataPartition) compareReplicas(v1, v2 []string) (equals bool) {
	equals = true
	if len(v1) == len(v2) {
		for i := 0; i < len(v1); i++ {
			if v1[i] != v2[i] {
				equals = false
				return
			}
		}
		equals = true
		return
	}
	equals = false
	return
}

func (dp *DataPartition) Load() (response *proto.LoadDataPartitionResponse) {
	response = &proto.LoadDataPartitionResponse{}
	response.PartitionId = uint64(dp.partitionID)
	response.PartitionStatus = dp.partitionStatus
	response.Used = uint64(dp.Used())

	if dp.loadExtentHeaderStatus != FinishLoadDataPartitionExtentHeader {
		response.PartitionSnapshot = make([]*proto.File, 0)
	} else {
		response.PartitionSnapshot = dp.SnapShot()
	}
	return
}

func (dp *DataPartition) doStreamFixTinyDeleteRecord(ctx context.Context, repairTask *DataPartitionRepairTask, isFullSync bool) {
	var (
		originLocalTinyDeleteSize int64
		localTinyDeleteFileSize   int64
		err                       error
		conn                      *net.TCPConn
		isRealSync                bool
	)

	if !dp.Disk().canFinTinyDeleteRecord() {
		return
	}
	defer func() {
		dp.Disk().finishFixTinyDeleteRecord()
	}()
	log.LogInfof(ActionSyncTinyDeleteRecord+" start PartitionID(%v) localTinyDeleteFileSize(%v) leaderTinyDeleteFileSize(%v) "+
		"leaderAddr(%v) ,lastSyncTinyDeleteTime(%v) currentTime(%v) fullSyncTinyDeleteTime(%v) isFullSync(%v)",
		dp.partitionID, localTinyDeleteFileSize, repairTask.LeaderTinyDeleteRecordFileSize, repairTask.LeaderAddr,
		dp.lastSyncTinyDeleteTime, time.Now().Unix(), dp.FullSyncTinyDeleteTime, isFullSync)

	defer func() {
		log.LogInfof(ActionSyncTinyDeleteRecord+" end PartitionID(%v) originLocalTinyDeleteSize(%v) localTinyDeleteFileSize(%v) leaderTinyDeleteFileSize(%v) leaderAddr(%v) "+
			"err(%v), lastSyncTinyDeleteTime(%v) currentTime(%v) fullSyncTinyDeleteTime(%v) isFullSync(%v) isRealSync(%v)\",",
			dp.partitionID, originLocalTinyDeleteSize, localTinyDeleteFileSize, repairTask.LeaderTinyDeleteRecordFileSize, repairTask.LeaderAddr, err,
			dp.lastSyncTinyDeleteTime, time.Now().Unix(), dp.FullSyncTinyDeleteTime, isFullSync, isRealSync)
	}()
	if dp.DataPartitionCreateType != proto.DecommissionedCreateDataPartition && !isFullSync && time.Now().Unix()-dp.lastSyncTinyDeleteTime < MinSyncTinyDeleteTime {
		return
	}
	var release = dp.extentStore.LockFlushDelete()
	defer release()

	if isFullSync {
		dp.FullSyncTinyDeleteTime = time.Now().Unix()
		err = dp.extentStore.DropTinyDeleteRecord()
		if err != nil {
			return
		}
	}
	if localTinyDeleteFileSize, err = dp.extentStore.LoadTinyDeleteFileOffset(); err != nil {
		return
	}
	if localTinyDeleteFileSize >= repairTask.LeaderTinyDeleteRecordFileSize {
		return
	}
	originLocalTinyDeleteSize = localTinyDeleteFileSize
	isRealSync = true
	dp.lastSyncTinyDeleteTime = time.Now().Unix()
	p := repl.NewPacketToReadTinyDeleteRecord(ctx, dp.partitionID, localTinyDeleteFileSize)
	if conn, err = gConnPool.GetConnect(repairTask.LeaderAddr); err != nil {
		return
	}
	defer gConnPool.PutConnect(conn, true)
	if err = p.WriteToConn(conn, proto.WriteDeadlineTime); err != nil {
		return
	}
	store := dp.extentStore
	start := time.Now().Unix()
	defer func() {
		err = dp.ExtentStore().PlaybackTinyDelete(originLocalTinyDeleteSize)
	}()
	for localTinyDeleteFileSize < repairTask.LeaderTinyDeleteRecordFileSize {
		if localTinyDeleteFileSize >= repairTask.LeaderTinyDeleteRecordFileSize {
			return
		}
		if err = p.ReadFromConn(conn, proto.ReadDeadlineTime); err != nil {
			return
		}
		if p.IsErrPacket() {
			logContent := fmt.Sprintf("action[doStreamFixTinyDeleteRecord] %v.",
				p.LogMessage(p.GetOpMsg(), conn.RemoteAddr().String(), start, fmt.Errorf(string(p.Data[:p.Size]))))
			err = fmt.Errorf(logContent)
			return
		}
		if p.CRC != crc32.ChecksumIEEE(p.Data[:p.Size]) {
			err = fmt.Errorf("crc not match")
			return
		}
		if p.Size%storage.DeleteTinyRecordSize != 0 {
			err = fmt.Errorf("unavali size")
			return
		}
		var index int
		var allTinyDeleteRecordsArr [proto.TinyExtentCount + 1]TinyDeleteRecordArr
		for currTinyExtentID := proto.TinyExtentStartID; currTinyExtentID < proto.TinyExtentStartID+proto.TinyExtentCount; currTinyExtentID++ {
			allTinyDeleteRecordsArr[currTinyExtentID] = make([]TinyDeleteRecord, 0)
		}

		for (index+1)*storage.DeleteTinyRecordSize <= int(p.Size) {
			record := p.Data[index*storage.DeleteTinyRecordSize : (index+1)*storage.DeleteTinyRecordSize]
			extentID, offset, size := storage.UnMarshalTinyExtent(record)
			localTinyDeleteFileSize += storage.DeleteTinyRecordSize
			index++
			if !proto.IsTinyExtent(extentID) {
				continue
			}
			dr := TinyDeleteRecord{
				extentID: extentID,
				offset:   offset,
				size:     size,
			}
			allTinyDeleteRecordsArr[extentID] = append(allTinyDeleteRecordsArr[extentID], dr)
		}
		for currTinyExtentID := proto.TinyExtentStartID; currTinyExtentID < proto.TinyExtentStartID+proto.TinyExtentCount; currTinyExtentID++ {
			currentDeleteRecords := allTinyDeleteRecordsArr[currTinyExtentID]
			for _, dr := range currentDeleteRecords {
				if dr.extentID != uint64(currTinyExtentID) {
					continue
				}
				if !proto.IsTinyExtent(dr.extentID) {
					continue
				}
				store.PersistTinyDeleteRecord(dr.extentID, int64(dr.offset), int64(dr.size))
			}
		}
	}
}

// ChangeRaftMember is a wrapper function of changing the raft member.
func (dp *DataPartition) ChangeRaftMember(changeType raftProto.ConfChangeType, peer raftProto.Peer, context []byte) (resp interface{}, err error) {
	if log.IsWarnEnabled() {
		log.LogWarnf("DP %v: change raft member: type %v, peer %v", dp.partitionID, changeType, peer)
	}
	resp, err = dp.raftPartition.ChangeMember(changeType, peer, context)
	return
}

func (dp *DataPartition) canRemoveSelf() (canRemove bool, err error) {
	var currentPeers []proto.Peer
	var offlinePeerID uint64
	for i := 0; i < 2; i++ {
		if offlinePeerID, currentPeers, err = dp.topologyManager.GetPartitionRaftPeerFromMaster(dp.volumeID, dp.partitionID); err == nil {
			break
		}
	}
	if err != nil {
		log.LogErrorf("action[canRemoveSelf] err(%v)", err)
		return
	}
	canRemove = false
	var existInPeers bool
	for _, peer := range currentPeers {
		if dp.config.NodeID == peer.ID {
			existInPeers = true
		}
	}
	if !existInPeers {
		canRemove = true
		return
	}
	if dp.config.NodeID == offlinePeerID {
		canRemove = true
		return
	}
	return
}

func (dp *DataPartition) SyncReplicaHosts(replicas []string) {
	if len(replicas) == 0 {
		return
	}
	var leader bool // Whether current instance is the leader member.
	if len(replicas) >= 1 {
		leaderAddr := replicas[0]
		leaderAddrParts := strings.Split(leaderAddr, ":")
		if len(leaderAddrParts) == 2 && strings.TrimSpace(leaderAddrParts[0]) == LocalIP {
			leader = true
		}
	}
	dp.replicasLock.Lock()
	dp.isReplLeader = leader
	dp.replicas = replicas
	dp.intervalToUpdateReplicas = time.Now().Unix()
	dp.replicasLock.Unlock()
	log.LogInfof("partition(%v) synchronized replica hosts from master [replicas:(%v), leader: %v]",
		dp.partitionID, strings.Join(replicas, ","), leader)
	if leader {
		dp.proposeRepair()
	}
}

// ResetRaftMember is a wrapper function of changing the raft member.
func (dp *DataPartition) ResetRaftMember(peers []raftProto.Peer, context []byte) (err error) {
	if dp.raftPartition == nil {
		return fmt.Errorf("raft instance not ready")
	}
	err = dp.raftPartition.ResetMember(peers, nil, context)
	return
}

func (dp *DataPartition) EvictExpiredFileDescriptor() {
	dp.extentStore.EvictExpiredCache()
}

func (dp *DataPartition) ForceEvictFileDescriptor(ratio unit.Ratio) {
	dp.extentStore.ForceEvictCache(ratio)
}

func (dp *DataPartition) EvictExpiredExtentDeleteCache(expireTime int64) {
	if expireTime == 0 {
		expireTime = DefaultNormalExtentDeleteExpireTime
	}
	dp.extentStore.EvictExpiredNormalExtentDeleteCache(expireTime)
}

func (dp *DataPartition) getTinyExtentHoleInfo(extent uint64) (result interface{}, err error) {
	holes, extentAvaliSize, err := dp.ExtentStore().TinyExtentHolesAndAvaliSize(extent, 0)
	if err != nil {
		return
	}

	blocks, _ := dp.ExtentStore().GetRealBlockCnt(extent)
	result = &struct {
		Holes           []*proto.TinyExtentHole `json:"holes"`
		ExtentAvaliSize uint64                  `json:"extentAvaliSize"`
		ExtentBlocks    int64                   `json:"blockNum"`
	}{
		Holes:           holes,
		ExtentAvaliSize: extentAvaliSize,
		ExtentBlocks:    blocks,
	}
	return
}

func (dp *DataPartition) getDataPartitionInfo() (dpInfo *DataPartitionViewInfo, err error) {
	var (
		tinyDeleteRecordSize int64
	)
	if tinyDeleteRecordSize, err = dp.ExtentStore().LoadTinyDeleteFileOffset(); err != nil {
		err = fmt.Errorf("load tiny delete file offset fail: %v", err)
		return
	}
	var raftStatus *raft.Status
	if dp.raftPartition != nil {
		raftStatus = dp.raftPartition.Status()
	}
	dpInfo = &DataPartitionViewInfo{
		VolName:              dp.volumeID,
		ID:                   dp.partitionID,
		Size:                 dp.Size(),
		Used:                 dp.Used(),
		Status:               dp.Status(),
		Path:                 dp.Path(),
		Replicas:             dp.getReplicaClone(),
		TinyDeleteRecordSize: tinyDeleteRecordSize,
		RaftStatus:           raftStatus,
		Peers:                dp.config.Peers,
		Learners:             dp.config.Learners,
		IsFinishLoad:         dp.ExtentStore().IsFinishLoad(),
		IsRecover:            dp.DataPartitionCreateType == proto.DecommissionedCreateDataPartition,
		BaseExtentID:         dp.ExtentStore().GetBaseExtentID(),
		RiskFixerStatus: func() *riskdata.FixerStatus {
			if dp.dataFixer != nil {
				return dp.dataFixer.Status()
			}
			return nil
		}(),
	}
	return
}

func (dp *DataPartition) setFaultOccurredCheckLevel(checkCorruptLevel FaultOccurredCheckLevel) {
	dp.serverFaultCheckLevel = checkCorruptLevel
}

func (dp *DataPartition) ChangeCreateType(createType int) (err error) {
	if dp.DataPartitionCreateType != createType {
		dp.DataPartitionCreateType = createType
		err = dp.persistMetaDataOnly()
		return
	}
	return
}

func (dp *DataPartition) scanIssueFragments(latestFlushTimeUnix int64) (fragments []*riskdata.Fragment, err error) {
	if latestFlushTimeUnix == 0 {
		return
	}
	// 触发所有Extent必要元信息的加载或等待异步加载结束以在接下来的处理可以获得存储引擎中所有Extent的准确元信息。
	dp.extentStore.Load()

	var latestFlushTime = time.Unix(latestFlushTimeUnix, 0)
	var safetyTime = latestFlushTime.Add(-time.Second)
	// 对存储引擎中的所有数据块进行过滤，将有数据(Size > 0)且修改时间晚于最近一次Flush的Extent过滤出来进行接下来的检查和修复。
	dp.extentStore.WalkExtentsInfo(func(info *storage.ExtentInfoBlock) {
		if log.IsDebugEnabled() {
			log.LogDebugf("scanIssueFragments Partition(%v)_Extent(%v)_ModifyTime(%v), safetyTime(%v)", dp.partitionID, info[storage.FileID], info[storage.ModifyTime], safetyTime)
		}
		if info[storage.Size] > 0 && time.Unix(int64(info[storage.ModifyTime]), 0).After(safetyTime) {
			var (
				extentID   = info[storage.FileID]
				extentSize = info[storage.Size]

				fragOffset uint64 = 0
				fragSize          = extentSize
			)
			if proto.IsTinyExtent(extentID) {
				var err error
				if extentSize, err = dp.extentStore.TinyExtentGetFinfoSize(extentID); err != nil {
					if log.IsWarnEnabled() {
						log.LogWarnf("Partition(%v) can not get file info size for tiny Extent(%v): %v", dp.partitionID, extentID, err)
						return
					}
				}
				if extentSize > 128*unit.MB {
					fragOffset = extentSize - 128*unit.MB
				}
				// 按512个字节对齐
				if fragOffset%512 != 0 {
					fragOffset = (fragOffset / 512) * 512
				}
				fragSize = extentSize - fragOffset
			}
			// 切成最大16MB的段
			for subFragOffset := fragOffset; subFragOffset < extentSize; {
				subFragSize := uint64(math.Min(float64(16*unit.MB), float64((fragOffset+fragSize)-subFragOffset)))
				fragments = append(fragments, &riskdata.Fragment{
					ExtentID: extentID,
					Offset:   subFragOffset,
					Size:     subFragSize,
				})
				subFragOffset += subFragSize
			}

		}
	})
	return
}

func convertCheckCorruptLevel(l uint64) (FaultOccurredCheckLevel, error) {
	switch l {
	case 0:
		return CheckNothing, nil
	case 1:
		return CheckQuorumCommitID, nil
	case 2:
		return CheckAllCommitID, nil
	default:
		return CheckNothing, fmt.Errorf("invalid param")
	}
}

func (dp *DataPartition) limit(ctx context.Context, op int, size uint32, bandType string) (err error) {
	if dp == nil {
		return ErrPartitionNil
	}
	prBuilder := multirate.NewPropertiesBuilder().SetOp(strconv.Itoa(op)).SetVol(dp.volumeID).SetDisk(dp.disk.Path)
	stBuilder := multirate.NewStatBuilder().SetCount(1)

	switch op {
	case int(proto.OpWrite), int(proto.OpSyncWrite), int(proto.OpRandomWrite), int(proto.OpSyncRandomWrite), proto.OpExtentRepairWrite_, proto.OpExtentRepairWriteToApplyTempFile_:
		prBuilder.SetBandType(bandType)
		stBuilder.SetInBytes(int(size))
	case int(proto.OpStreamRead), int(proto.OpRead), int(proto.OpStreamFollowerRead), int(proto.OpTinyExtentRepairRead), int(proto.OpTinyExtentAvaliRead),
		int(proto.OpExtentRepairRead), proto.OpExtentRepairReadToRollback_, proto.OpExtentRepairReadToComputeCrc_, proto.OpExtentReadToGetCrc_:
		prBuilder.SetBandType(bandType).Properties()
		stBuilder.SetOutBytes(int(size)).Stat()
	default:
	}
	err = multirate.WaitNUseDefaultTimeout(ctx, prBuilder.Properties(), stBuilder.Stat())
	if err != nil {
		err = errors.Trace(err, proto.RateLimit)
	}
	return
}

func (dp *DataPartition) backendRefreshCacheView() {
	if dp.topologyManager == nil {
		return
	}
	dp.topologyManager.FetchDataPartitionView(dp.volumeID, dp.partitionID)
}

func (dp *DataPartition) getCacheView() (dataPartition *topology.DataPartition, err error) {
	if dp.topologyManager == nil {
		return nil, fmt.Errorf("topo manager is nil")
	}
	return dp.topologyManager.GetPartition(dp.volumeID, dp.partitionID)
}

// partition op by raft
func (dp *DataPartition) checkWriteErrs(errMsg string) (ignore bool) {
	// file has been deleted when applying the raft log
	if strings.Contains(errMsg, storage.ExtentHasBeenDeletedError.Error()) || strings.Contains(errMsg, proto.ExtentNotFoundError.Error()) {
		return true
	}
	return false
}

// CheckLeader checks if itself is the leader during read
func (dp *DataPartition) CheckLeader() (addr string, err error) {
	addr, ok := dp.IsRaftLeader()
	if !ok {
		err = raft.ErrNotLeader
		return
	}
	return
}

const (
	ForceRepair   = true
	NoForceRepair = false
)

func (dp *DataPartition) repairDataOnRandomWriteFromHost(extentID uint64, fromOffset, size uint64, host string) (err error) {
	remoteExtentInfo := storage.ExtentInfoBlock{}
	remoteExtentInfo[storage.FileID] = extentID
	remoteExtentInfo[storage.Size] = fromOffset + size
	err = dp.streamRepairExtent(nil, remoteExtentInfo, host, ForceRepair)
	log.LogWarnf("repairDataFromHost extentID(%v) fromOffset(%v) size(%v) result(%v)", dp.applyRepairKey(int(extentID)), fromOffset, size, err)
	return err
}

func (dp *DataPartition) repairDataOnRandomWrite(extentID uint64, fromOffset, size uint64) (err error) {
	hosts := dp.getReplicaClone()
	addr, _ := dp.IsRaftLeader()
	if addr != "" {
		err = dp.repairDataOnRandomWriteFromHost(extentID, fromOffset, size, addr)
		if err == nil {
			return
		}
	}
	for _, h := range hosts {
		if h == addr {
			continue
		}
		err = dp.repairDataOnRandomWriteFromHost(extentID, fromOffset, size, h)
		if err == nil {
			return
		}
	}
	return
}

func (dp *DataPartition) checkDeleteOnAllHosts(extentId uint64) bool {
	var err error
	defer func() {
		if err != nil {
			log.LogErrorf("checkDeleteOnAllHosts, partition:%v, extent:%v, error:%v", dp.partitionID, extentId, err)
		}
	}()
	hosts := dp.getReplicaClone()
	if dp.disk == nil || dp.disk.space == nil || dp.disk.space.dataNode == nil {
		return false
	}
	localExtentSize, err := dp.ExtentStore().LoadExtentWaterMark(extentId)
	if err != nil {
		return false
	}
	profPort := dp.disk.space.dataNode.httpPort
	notFoundErrCount := 0
	for _, h := range hosts {
		if dp.IsLocalAddress(h) {
			continue
		}
		httpAddr := fmt.Sprintf("%v:%v", strings.Split(h, ":")[0], profPort)
		dataClient := http_client.NewDataClient(httpAddr, false)
		var extentBlock *proto.ExtentInfoBlock
		for i := 0; i < 3; i++ {
			extentBlock, err = dataClient.GetExtentInfo(dp.partitionID, extentId)
			if err == nil || strings.Contains(err.Error(), "e extent") && strings.Contains(err.Error(), "not exist") {
				break
			}
		}
		if err == nil && extentBlock[proto.ExtentInfoSize] <= uint64(localExtentSize) {
			notFoundErrCount++
			continue
		}
		if err != nil && strings.Contains(err.Error(), "e extent") && strings.Contains(err.Error(), "not exist") {
			notFoundErrCount++
			err = nil
		}
	}
	if notFoundErrCount == len(hosts)-1 {
		return true
	}
	return false
}

// ApplyRandomWrite random write apply
func (dp *DataPartition) ApplyRandomWrite(opItem *rndWrtOpItem, raftApplyID uint64) (resp interface{}, err error) {
	start := time.Now().UnixMicro()
	toObject := dp.monitorData[proto.ActionOverWrite].BeforeTp()
	defer func() {
		toObject.AfterTp(uint64(opItem.size))
		if err == nil {
			resp = proto.OpOk
			if log.IsWriteEnabled() {
				log.LogWritef("[ApplyRandomWrite] "+
					"ApplyID(%v) Partition(%v)_Extent(%v)_"+
					"ExtentOffset(%v)_Size(%v)_CRC(%v) cost(%v)us",
					raftApplyID, dp.partitionID, opItem.extentID,
					opItem.offset, opItem.size, opItem.crc, time.Now().UnixMicro()-start)
			}
		} else {
			msg := fmt.Sprintf("[ApplyRandomWrite] "+
				"ApplyID(%v) Partition(%v)_Extent(%v)_"+
				"ExtentOffset(%v)_Size(%v)_CRC(%v)  Failed Result(%v) cost(%v)us",
				raftApplyID, dp.partitionID, opItem.extentID,
				opItem.offset, opItem.size, opItem.crc, err.Error(), time.Now().UnixMicro()-start)
			exporter.Warning(msg)
			resp = proto.OpDiskErr
			log.LogErrorf(msg)
		}
	}()
	for i := 0; i < 2; i++ {
		err = dp.ExtentStore().Write(nil, opItem.extentID, opItem.offset, opItem.size, opItem.data, opItem.crc, storage.RandomWriteType, opItem.opcode == proto.OpSyncRandomWrite)
		if err == nil {
			break
		}
		if dp.checkIsPartitionError(err) {
			return
		}
		if strings.Contains(err.Error(), storage.IllegalOverWriteError) {
			err = dp.repairDataOnRandomWrite(opItem.extentID, uint64(opItem.offset), uint64(opItem.size))
			if err == nil {
				continue
			}
			if dp.checkDeleteOnAllHosts(opItem.extentID) {
				log.LogErrorf("[ApplyRandomWrite] ApplyID(%v) Partition(%v)_Extent(%v)_ExtentOffset(%v)_Size(%v) extent deleted in all other hosts and ignore error, retry(%v)", raftApplyID, dp.partitionID, opItem.extentID, opItem.offset, opItem.size, i)
				err = nil
				break
			}
			continue
		}
		if strings.Contains(err.Error(), proto.ExtentNotFoundError.Error()) {
			log.LogErrorf("[ApplyRandomWrite] ApplyID(%v) Partition(%v)_Extent(%v)_ExtentOffset(%v)_Size(%v) apply err(%v) retry(%v)", raftApplyID, dp.partitionID, opItem.extentID, opItem.offset, opItem.size, err, i)
			err = nil
			return
		}
		log.LogErrorf("[ApplyRandomWrite] ApplyID(%v) Partition(%v)_Extent(%v)_ExtentOffset(%v)_Size(%v) apply err(%v) retry(%v)", raftApplyID, dp.partitionID, opItem.extentID, opItem.offset, opItem.size, err, i)
	}
	return
}

// RandomWriteSubmit submits the proposal to raft.
func (dp *DataPartition) RandomWriteSubmit(pkg *repl.Packet) (err error) {
	if !dp.ExtentStore().IsExists(pkg.ExtentID) || dp.ExtentStore().IsDeleted(pkg.ExtentID) {
		err = proto.ExtentNotFoundError
		return
	}
	err = dp.ExtentStore().CheckIsAvaliRandomWrite(pkg.ExtentID, pkg.ExtentOffset, int64(pkg.Size))
	if err != nil {
		return err
	}

	var cmd = NewRandomWriteCommand(pkg.Opcode, pkg.ExtentID, pkg.ExtentOffset, int64(pkg.Size), pkg.Data[:pkg.Size], pkg.CRC)
	if err = dp.submitToRaft(cmd); err != nil {
		return
	}

	pkg.PacketOkReply()

	if log.IsDebugEnabled() {
		log.LogDebugf("[RandomWrite] SubmitRaft: %v", pkg.GetUniqueLogId())
	}

	return
}

// RandomWriteSubmit submits the proposal to raft.
func (dp *DataPartition) RandomWriteSubmitV3(pkg *repl.Packet) (err error) {
	err = dp.ExtentStore().CheckIsAvaliRandomWrite(pkg.ExtentID, pkg.ExtentOffset, int64(pkg.Size))
	if err != nil {
		return err
	}
	//if len(pkg.Data)<int(pkg.Size)+proto.RandomWriteRaftLogV3HeaderSize{
	//	err=fmt.Errorf("unavali len(pkg.Data)(%v) ,pkg.Size(%v)," +
	//		"RandomWriteRaftLogV3HeaderSize(%v)",len(pkg.Data),pkg.Size,proto.RandomWriteRaftLogV3HeaderSize)
	//	return
	//}
	var cmd []byte
	cmd, err = MarshalRandWriteRaftLogV3(pkg.Opcode, pkg.ExtentID, pkg.ExtentOffset, int64(pkg.Size), pkg.Data[0:int(pkg.Size)+proto.RandomWriteRaftLogV3HeaderSize], pkg.CRC)
	if err != nil {
		return
	}
	if err = dp.submitToRaft(cmd); err != nil {
		return
	}
	pkg.PacketOkReply()
	if log.IsDebugEnabled() {
		log.LogDebugf("[RandomWrite] SubmitRaft: %v", pkg.GetUniqueLogId())
	}

	return
}

// partition persist
type PersistFlag int

func (dp *DataPartition) lockPersist() (release func()) {
	dp.persistSync <- struct{}{}
	release = func() {
		<-dp.persistSync
	}
	return
}

// Deprecated: Flush is deprecated, use Flusher instead.
func (dp *DataPartition) Flush() (err error) {
	return dp.persist(nil, true)
}

func (dp *DataPartition) Flusher() infra.Flusher {
	var (
		status       = dp.applyStatus.Snap()
		storeFlusher = dp.extentStore.Flusher()
	)
	return infra.NewFuncFlusher(
		func(opsLimiter, bpsLimiter *rate.Limiter) error {
			var release = dp.lockPersist()
			defer release()

			var err error
			if err = storeFlusher.Flush(opsLimiter, bpsLimiter); err != nil {
				return err
			}
			if dp.raftPartition != nil {
				if err = dp.raftPartition.FlushWAL(false); err != nil {
					return err
				}
			}
			if opsLimiter != nil {
				_ = opsLimiter.Wait(context.Background())
			}
			if err = dp.persistAppliedID(status); err != nil {
				return err
			}

			if opsLimiter != nil {
				_ = opsLimiter.Wait(context.Background())
			}
			if err = dp.persistMetadata(status); err != nil {
				return err
			}
			return nil
		},
		func() int {
			return storeFlusher.Count() + 2
		})
}

// Persist 方法会执行以下操作:
// 1. Sync所有打开的文件句柄
// 2. Sync Raft WAL以及HardState信息
// 3. 持久化Applied Index水位信息
// 4. 持久化DP的META信息, 主要用于持久化和Applied Index对应的LastTruncateID。
// 若status参数为nil，则会使用调用该方法时WALApplyStatus状态

func (dp *DataPartition) persist(status *WALApplyStatus, useFlushExtentsRateLimiter bool) (err error) {
	var release = dp.lockPersist()
	defer release()

	if status == nil {
		status = dp.applyStatus.Snap()
	}
	var flushExtentsLimitRater *rate.Limiter
	if useFlushExtentsRateLimiter {
		flushExtentsLimitRater = newFlushBpsLimiter(atomic.LoadUint64(&dp.disk.forceFlushFDParallelism), dp.disk.isSSDMediaType())
	}
	_ = dp.forceFlushAllFD(flushExtentsLimitRater)

	if dp.raftPartition != nil {
		if err = dp.raftPartition.FlushWAL(false); err != nil {
			return
		}
	}

	if err = dp.persistAppliedID(status); err != nil {
		return
	}

	if err = dp.persistMetadata(status); err != nil {
		return
	}

	return
}

// persistMetaDataOnly 仅持久化DP的META信息(不对LastTruncatedID信息进行变更)
func (dp *DataPartition) persistMetaDataOnly() (err error) {
	var release = dp.lockPersist()
	defer release()

	if err = dp.persistMetadata(nil); err != nil {
		return
	}
	return
}

func (dp *DataPartition) persistAppliedID(snap *WALApplyStatus) (err error) {

	var (
		originalApplyIndex uint64
		newAppliedIndex    = snap.Applied()
	)

	if newAppliedIndex == 0 || newAppliedIndex <= dp.persistedApplied {
		return
	}

	var originalFilename = path.Join(dp.Path(), ApplyIndexFile)
	if originalFileData, readErr := ioutil.ReadFile(originalFilename); readErr == nil {
		_, _ = fmt.Sscanf(string(originalFileData), "%d", &originalApplyIndex)
	}

	if newAppliedIndex <= originalApplyIndex {
		return
	}

	tmpFilename := path.Join(dp.Path(), TempApplyIndexFile)
	tmpFile, err := os.OpenFile(tmpFilename, os.O_RDWR|os.O_APPEND|os.O_TRUNC|os.O_CREATE, 0755)
	if err != nil {
		return
	}
	defer func() {
		_ = tmpFile.Close()
		_ = os.Remove(tmpFilename)
	}()
	if _, err = tmpFile.WriteString(fmt.Sprintf("%d", newAppliedIndex)); err != nil {
		return
	}
	if err = tmpFile.Sync(); err != nil {
		return
	}
	err = os.Rename(tmpFilename, path.Join(dp.Path(), ApplyIndexFile))
	log.LogInfof("partition[%v] persisted appliedID [prev: %v, new: %v]", dp.partitionID, dp.persistedApplied, newAppliedIndex)
	dp.persistedApplied = newAppliedIndex
	return
}

// PersistMetadata persists the file metadata on the disk.
// 若snap参数为nil，则不会修改META文件中的LastTruncateID信息。
func (dp *DataPartition) persistMetadata(snap *WALApplyStatus) (err error) {

	originFileName := path.Join(dp.path, DataPartitionMetadataFileName)
	tempFileName := path.Join(dp.path, TempMetadataFileName)

	var metadata = new(DataPartitionMetadata)

	sp := sortedPeers(dp.config.Peers)
	sort.Sort(sp)
	metadata.VolumeID = dp.config.VolName
	metadata.PartitionID = dp.config.PartitionID
	metadata.PartitionSize = dp.config.PartitionSize
	metadata.ReplicaNum = dp.config.ReplicaNum
	metadata.Peers = dp.config.Peers
	metadata.Hosts = dp.config.Hosts
	metadata.Learners = dp.config.Learners
	metadata.DataPartitionCreateType = dp.DataPartitionCreateType
	metadata.VolumeHAType = dp.config.VolHAType
	metadata.IsCatchUp = dp.isCatchUp
	metadata.NeedServerFaultCheck = dp.needServerFaultCheck
	metadata.ConsistencyMode = dp.config.Mode

	if dp.persistedMetadata != nil {
		metadata.CreateTime = dp.persistedMetadata.CreateTime
	}
	if metadata.CreateTime == "" {
		metadata.CreateTime = time.Now().Format(TimeLayout)
	}

	if snap != nil && snap.Truncated() > metadata.LastTruncateID {
		metadata.LastTruncateID = snap.Truncated()
	} else if dp.persistedMetadata != nil {
		metadata.LastTruncateID = dp.persistedMetadata.LastTruncateID
	}
	if dp.persistedMetadata != nil && dp.persistedMetadata.Equals(metadata) {
		return
	}

	var newData []byte
	if newData, err = json.Marshal(metadata); err != nil {
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
	dp.persistedMetadata = metadata
	log.LogInfof("PersistMetadata DataPartition(%v) data(%v)", dp.partitionID, string(newData))
	return
}

// Deprecated: forceFlushAllFD is deprecated, please use Flusher instead.
func (dp *DataPartition) forceFlushAllFD(limiter *rate.Limiter) (error error) {
	return dp.extentStore.Flush(limiter)
}

func (dp *DataPartition) raftPort() (heartbeat, replica int, err error) {
	raftConfig := dp.config.RaftStore.RaftConfig()
	heartbeatAddrSplits := strings.Split(raftConfig.HeartbeatAddr, ":")
	replicaAddrSplits := strings.Split(raftConfig.ReplicateAddr, ":")
	if len(heartbeatAddrSplits) != 2 {
		err = errors.New("illegal heartbeat address")
		return
	}
	if len(replicaAddrSplits) != 2 {
		err = errors.New("illegal replica address")
		return
	}
	heartbeat, err = strconv.Atoi(heartbeatAddrSplits[1])
	if err != nil {
		return
	}
	replica, err = strconv.Atoi(replicaAddrSplits[1])
	if err != nil {
		return
	}
	return
}

// startRaft start raft instance when data partition start or restore.
func (dp *DataPartition) startRaft() (err error) {
	var (
		heartbeatPort int
		replicaPort   int
		peers         []raftstore.PeerAddress
		learners      []raftProto.Learner
	)
	defer func() {
		if r := recover(); r != nil {
			mesg := fmt.Sprintf("startRaft(%v)  Raft Panic(%v)", dp.partitionID, r)
			panic(mesg)
		}
	}()

	if heartbeatPort, replicaPort, err = dp.raftPort(); err != nil {
		return
	}
	for _, peer := range dp.config.Peers {
		addr := strings.Split(peer.Addr, ":")[0]
		rp := raftstore.PeerAddress{
			Peer: raftProto.Peer{
				ID: peer.ID,
			},
			Address:       addr,
			HeartbeatPort: heartbeatPort,
			ReplicaPort:   replicaPort,
		}
		peers = append(peers, rp)
	}
	for _, learner := range dp.config.Learners {
		addLearner := raftProto.Learner{
			ID:         learner.ID,
			PromConfig: &raftProto.PromoteConfig{AutoPromote: learner.PmConfig.AutoProm, PromThreshold: learner.PmConfig.PromThreshold},
		}
		learners = append(learners, addLearner)
	}
	log.LogDebugf("start partition(%v) raft peers: %s path: %s",
		dp.partitionID, peers, dp.path)

	var getStartIndex raftstore.GetStartIndexFunc = func(firstIndex, lastIndex uint64) (startIndex uint64) {
		// Compute index for raft recover
		var applied = dp.applyStatus.Applied()
		defer func() {
			log.LogWarnf("partition(%v) computed start index [startIndex: %v, applied: %v, firstIndex: %v, lastIndex: %v]",
				dp.partitionID, startIndex, applied, firstIndex, lastIndex)
		}()
		if applied >= firstIndex && applied-firstIndex > RaftLogRecoverInAdvance {
			startIndex = applied - RaftLogRecoverInAdvance
			return
		}
		startIndex = firstIndex
		return
	}

	var maxCommitID uint64
	if dp.isNeedFaultCheck() {
		if maxCommitID, err = dp.getMaxCommitID(context.Background()); err != nil {
			return
		}
	}

	var fsm = &raftstore.FunctionalPartitionFsm{
		ApplyFunc:              dp.handleRaftApply,
		ApplyMemberChangeFunc:  dp.handleRaftApplyMemberChange,
		SnapshotFunc:           dp.handleRaftSnapshot,
		AskRollbackFunc:        dp.handleRaftAskRollback,
		ApplySnapshotFunc:      dp.handleRaftApplySnapshot,
		HandleFatalEventFunc:   dp.handleRaftFatalEvent,
		HandleLeaderChangeFunc: dp.handleRaftLeaderChange,
	}

	pc := &raftstore.PartitionConfig{
		ID:                 dp.partitionID,
		Peers:              peers,
		Learners:           learners,
		SM:                 fsm,
		WalPath:            dp.path,
		StartCommit:        maxCommitID,
		GetStartIndex:      getStartIndex,
		WALContinuityCheck: dp.isNeedFaultCheck(),
		WALContinuityFix:   dp.isNeedFaultCheck(),
		Mode:               dp.config.Mode,
		StorageListener: raftstore.NewStorageListenerBuilder().
			ListenStoredEntry(dp.listenStoredRaftLogEntry).
			Build(),
	}
	dp.raftPartition = dp.config.RaftStore.CreatePartition(pc)

	if err = dp.raftPartition.Start(); err != nil {
		return
	}
	go dp.startRaftWorker()
	return
}

func (dp *DataPartition) getMaxCommitID(ctx context.Context) (maxID uint64, err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("getMaxCommitID, partition:%v, error:%v", dp.partitionID, err)
		}
	}()
	minReply := dp.getServerFaultCheckQuorum()
	if minReply == 0 {
		log.LogDebugf("start partition(%v), skip get max commit id", dp.partitionID)
		return
	}
	allRemoteCommitID, replyNum := dp.getRemoteReplicaCommitID(ctx)
	if replyNum < minReply {
		err = fmt.Errorf("reply num(%d) not enough to minReply(%d)", replyNum, minReply)
		return
	}
	maxID, _ = dp.findMaxID(allRemoteCommitID)
	log.LogInfof("start partition(%v), maxCommitID(%v)", dp.partitionID, maxID)
	return
}

func (dp *DataPartition) stopRaft() {
	if dp.raftPartition != nil {
		if log.IsInfoEnabled() {
			log.LogInfof("DP(%v) stop raft partition", dp.partitionID)
		}
		_ = dp.raftPartition.Stop()
	}
	return
}

func (dp *DataPartition) CanRemoveRaftMember(peer proto.Peer) error {
	for _, learner := range dp.config.Learners {
		if learner.ID == peer.ID && learner.Addr == peer.Addr {
			return nil
		}
	}
	downReplicas := dp.config.RaftStore.RaftServer().GetDownReplicas(dp.partitionID)
	hasExsit := false
	for _, p := range dp.config.Peers {
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

	sumReplicas := len(dp.config.Peers) - len(dp.config.Learners)
	if sumReplicas%2 == 1 {
		if sumReplicas-len(hasDownReplicasExcludePeer) > (sumReplicas/2 + 1) {
			return nil
		}
	} else {
		if sumReplicas-len(hasDownReplicasExcludePeer) >= (sumReplicas/2 + 1) {
			return nil
		}
	}

	return fmt.Errorf("hasDownReplicasExcludePeer(%v) too much,so donnot offline(%v)", downReplicas, peer)
}

// startRaftWorker starts the task schedule as follows:
// 1. write the raft applied id into disk.
// 2. collect the applied ids from raft members.
// 3. based on the minimum applied id to cutoff and delete the saved raft log in order to free the disk space.
func (dp *DataPartition) startRaftWorker() {
	raftLogTruncateScheduleTimer := time.NewTimer(time.Second * 1)

	log.LogDebugf("[startSchedule] hello DataPartition schedule")

	for {
		select {
		case <-dp.stopC:
			log.LogDebugf("[startSchedule] stop partition(%v)", dp.partitionID)
			raftLogTruncateScheduleTimer.Stop()
			return

		case extentID := <-dp.stopRaftC:
			dp.stopRaft()
			log.LogErrorf("action[ExtentRepair] stop raft partition(%v)_%v", dp.partitionID, extentID)

		case <-raftLogTruncateScheduleTimer.C:
			dp.scheduleRaftWALTruncate(context.Background())
			raftLogTruncateScheduleTimer.Reset(time.Minute * 1)

		}
	}
}

// startRaftAfterRepair starts the raft after repairing a partition.
// It can only happens after all the extent files are repaired by the leader.
// When the repair is finished, the local dp.partitionSize is same as the leader's dp.partitionSize.
// The repair task can be done in statusUpdateScheduler->runRepair.
func (dp *DataPartition) startRaftAfterRepair() {
	var (
		initPartitionSize, initMaxExtentID uint64
		err                                error
	)
	timer := time.NewTimer(0)
	for {
		select {
		case <-timer.C:
			err = nil
			if dp.isReplLeader { // primary does not need to wait repair
				dp.DataPartitionCreateType = proto.NormalCreateDataPartition
				if err = dp.persist(nil, false); err != nil {
					log.LogErrorf("Partition(%v) persist metadata failed and try after 5s: %v", dp.partitionID, err)
					timer.Reset(5 * time.Second)
					continue
				}
				if err := dp.startRaft(); err != nil {
					log.LogErrorf("PartitionID(%v) leader start raft err(%v).", dp.partitionID, err)
					timer.Reset(5 * time.Second)
					continue
				}
				log.LogDebugf("PartitionID(%v) leader started.", dp.partitionID)
				return
			}
			ctx := context.Background()
			// wait for dp.replicas to be updated
			if replicas := dp.getReplicaClone(); len(replicas) == 0 {
				log.LogErrorf("action[startRaftAfterRepair] partition(%v) replicas is nil.", dp.partitionID)
				timer.Reset(5 * time.Second)
				continue
			}
			if initMaxExtentID == 0 || initPartitionSize == 0 {
				initMaxExtentID, initPartitionSize, err = dp.getLeaderMaxExtentIDAndPartitionSize(ctx)
				if err != nil {
					log.LogErrorf("PartitionID(%v) get MaxExtentID  err(%v)", dp.partitionID, err)
					timer.Reset(5 * time.Second)
					continue
				}
			}

			if dp.isPartitionSizeRecover(ctx, initMaxExtentID, initPartitionSize) {
				timer.Reset(5 * time.Second)
				continue
			}

			// start raft
			dp.DataPartitionCreateType = proto.NormalCreateDataPartition
			if err = dp.persist(nil, false); err != nil {
				log.LogErrorf("Partition(%v) persist metadata failed and try after 5s: %v", dp.partitionID, err)
				timer.Reset(5 * time.Second)
				continue
			}
			if err := dp.startRaft(); err != nil {
				log.LogErrorf("PartitionID(%v) start raft err(%v). Retry after 5s.", dp.partitionID, err)
				timer.Reset(5 * time.Second)
				continue
			}
			log.LogInfof("PartitionID(%v) raft started.", dp.partitionID)
			return
		case <-dp.stopC:
			timer.Stop()
			return
		}
	}
}

func (dp *DataPartition) isPartitionSizeRecover(ctx context.Context, initMaxExtentID, initPartitionSize uint64) bool {
	// get the partition size from the primary and compare it with the loparal one
	currLeaderPartitionSize, err := dp.getLeaderPartitionSize(ctx, initMaxExtentID)
	if err != nil {
		log.LogErrorf("PartitionID(%v) get leader size err(%v)", dp.partitionID, err)
		return true
	}
	if currLeaderPartitionSize < initPartitionSize {
		initPartitionSize = currLeaderPartitionSize
	}
	localSize := dp.extentStore.StoreSizeExtentID(initMaxExtentID)

	log.LogInfof("startRaftAfterRepair PartitionID(%v) initMaxExtentID(%v) initPartitionSize(%v) currLeaderPartitionSize(%v)"+
		"localSize(%v)", dp.partitionID, initMaxExtentID, initPartitionSize, currLeaderPartitionSize, localSize)

	if initPartitionSize > localSize {
		log.LogErrorf("PartitionID(%v) partition data  wait snapshot recover, leader(%v) local(%v)", dp.partitionID, initPartitionSize, localSize)
		return true
	}
	return false
}

// startRaftAsync dp instance can start without raft, this enables remote request to get the dp basic info
func (dp *DataPartition) startRaftAsync() {
	var err error
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			if err = dp.startRaft(); err != nil {
				log.LogErrorf("partition(%v) start raft failed: %v", dp.partitionID, err)
				timer.Reset(5 * time.Second)
				continue
			}
			return
		case <-dp.stopC:
			return
		}
	}
}

// Add a raft node.
func (dp *DataPartition) addRaftNode(req *proto.AddDataPartitionRaftMemberRequest, index uint64) (isUpdated bool, err error) {
	var (
		heartbeatPort int
		replicaPort   int
	)
	if heartbeatPort, replicaPort, err = dp.raftPort(); err != nil {
		return
	}

	found := false
	for _, peer := range dp.config.Peers {
		if peer.ID == req.AddPeer.ID {
			found = true
			break
		}
	}
	isUpdated = !found
	if !isUpdated {
		return
	}
	data, _ := json.Marshal(req)
	log.LogInfof("addRaftNode: remove self: partitionID(%v) nodeID(%v) index(%v) data(%v) ",
		req.PartitionId, dp.config.NodeID, index, string(data))
	dp.config.Peers = append(dp.config.Peers, req.AddPeer)
	dp.config.Hosts = append(dp.config.Hosts, req.AddPeer.Addr)
	dp.replicasLock.Lock()
	dp.replicas = make([]string, len(dp.config.Hosts))
	copy(dp.replicas, dp.config.Hosts)
	dp.replicasLock.Unlock()
	addr := strings.Split(req.AddPeer.Addr, ":")[0]
	dp.config.RaftStore.AddNodeWithPort(req.AddPeer.ID, addr, heartbeatPort, replicaPort)
	return
}

// Delete a raft node.
func (dp *DataPartition) removeRaftNode(req *proto.RemoveDataPartitionRaftMemberRequest, index uint64) (isUpdated bool, err error) {
	canRemoveSelf := true
	if dp.config.NodeID == req.RemovePeer.ID {
		if canRemoveSelf, err = dp.canRemoveSelf(); err != nil {
			return
		}
	}

	peerIndex := -1
	data, _ := json.Marshal(req)
	isUpdated = false
	log.LogInfof("Start RemoveRaftNode  PartitionID(%v) nodeID(%v)  do RaftLog(%v) ",
		req.PartitionId, dp.config.NodeID, string(data))
	for i, peer := range dp.config.Peers {
		if peer.ID == req.RemovePeer.ID {
			peerIndex = i
			isUpdated = true
			break
		}
	}
	if !isUpdated {
		log.LogInfof("NoUpdate RemoveRaftNode  PartitionID(%v) nodeID(%v)  do RaftLog(%v) ",
			req.PartitionId, dp.config.NodeID, string(data))
		return
	}
	hostIndex := -1
	for index, host := range dp.config.Hosts {
		if host == req.RemovePeer.Addr {
			hostIndex = index
			break
		}
	}
	if hostIndex != -1 {
		dp.config.Hosts = append(dp.config.Hosts[:hostIndex], dp.config.Hosts[hostIndex+1:]...)
	}

	dp.replicasLock.Lock()
	dp.replicas = make([]string, len(dp.config.Hosts))
	copy(dp.replicas, dp.config.Hosts)
	dp.replicasLock.Unlock()

	dp.config.Peers = append(dp.config.Peers[:peerIndex], dp.config.Peers[peerIndex+1:]...)
	learnerIndex := -1
	for i, learner := range dp.config.Learners {
		if learner.ID == req.RemovePeer.ID && learner.Addr == req.RemovePeer.Addr {
			learnerIndex = i
			break
		}
	}
	if learnerIndex != -1 {
		dp.config.Learners = append(dp.config.Learners[:learnerIndex], dp.config.Learners[learnerIndex+1:]...)
	}
	if dp.config.NodeID == req.RemovePeer.ID && canRemoveSelf {
		if req.ReserveResource {
			dp.Disk().space.DeletePartitionFromCache(dp.partitionID)
		} else {
			dp.raftPartition.Expired()
			dp.Disk().space.ExpiredPartition(dp.partitionID)
		}
		isUpdated = false
	}
	log.LogInfof("Fininsh RemoveRaftNode  PartitionID(%v) nodeID(%v)  do RaftLog(%v) ",
		req.PartitionId, dp.config.NodeID, string(data))

	return
}

// Reset a raft node.
func (dp *DataPartition) resetRaftNode(req *proto.ResetDataPartitionRaftMemberRequest) (isUpdated bool, err error) {
	var (
		newHostIndexes    []int
		newPeerIndexes    []int
		newLearnerIndexes []int
		newHosts          []string
		newPeers          []proto.Peer
		newLearners       []proto.Learner
	)
	data, _ := json.Marshal(req)
	isUpdated = true
	log.LogInfof("Start ResetRaftNode  PartitionID(%v) nodeID(%v)  do RaftLog(%v) ",
		req.PartitionId, dp.config.NodeID, string(data))

	if len(req.NewPeers) >= len(dp.config.Peers) {
		log.LogInfof("NoUpdate ResetRaftNode  PartitionID(%v) nodeID(%v)  do RaftLog(%v) ",
			req.PartitionId, dp.config.NodeID, string(data))
		return
	}
	for _, peer := range req.NewPeers {
		flag := false
		for index, p := range dp.config.Peers {
			if peer.ID == p.ID {
				flag = true
				newPeerIndexes = append(newPeerIndexes, index)
				break
			}
		}
		if !flag {
			isUpdated = false
			log.LogInfof("ResetRaftNode must be old node, PartitionID(%v) nodeID(%v)  do RaftLog(%v) ",
				req.PartitionId, dp.config.NodeID, string(data))
			return
		}
	}
	for _, peer := range req.NewPeers {
		flag := false
		for index, host := range dp.config.Hosts {
			if peer.Addr == host {
				flag = true
				newHostIndexes = append(newHostIndexes, index)
				break
			}
		}
		if !flag {
			isUpdated = false
			log.LogInfof("ResetRaftNode must be old node, PartitionID(%v) nodeID(%v) OldHosts(%v)  do RaftLog(%v) ",
				req.PartitionId, dp.config.NodeID, dp.config.Hosts, string(data))
			return
		}
	}
	for _, peer := range req.NewPeers {
		for index, l := range dp.config.Learners {
			if peer.ID == l.ID {
				newLearnerIndexes = append(newLearnerIndexes, index)
				break
			}
		}
	}

	var peers []raftProto.Peer
	for _, peer := range req.NewPeers {
		peers = append(peers, raftProto.Peer{ID: peer.ID})
	}
	if err = dp.ResetRaftMember(peers, nil); err != nil {
		log.LogErrorf("partition[%v] reset raft member to %v failed: %v", dp.partitionID, peers, err)
		return
	}

	newHosts = make([]string, len(newHostIndexes))
	newPeers = make([]proto.Peer, len(newPeerIndexes))
	newLearners = make([]proto.Learner, len(newLearnerIndexes))
	dp.replicasLock.Lock()
	sort.Ints(newHostIndexes)
	for i, index := range newHostIndexes {
		newHosts[i] = dp.config.Hosts[index]
	}
	dp.config.Hosts = newHosts

	sort.Ints(newPeerIndexes)
	for i, index := range newPeerIndexes {
		newPeers[i] = dp.config.Peers[index]
	}
	dp.config.Peers = newPeers

	sort.Ints(newLearnerIndexes)
	for i, index := range newLearnerIndexes {
		newLearners[i] = dp.config.Learners[index]
	}
	dp.config.Learners = newLearners

	dp.replicas = make([]string, len(dp.config.Hosts))
	copy(dp.replicas, dp.config.Hosts)
	dp.replicasLock.Unlock()
	log.LogInfof("Finish ResetRaftNode  PartitionID(%v) nodeID(%v) newHosts(%v)  do RaftLog(%v) ",
		req.PartitionId, dp.config.NodeID, newHosts, string(data))

	release := dp.lockPersist()
	defer release()
	if err = dp.persistMetadata(nil); err != nil {
		return
	}
	return
}

// Add a raft learner.
func (dp *DataPartition) addRaftLearner(req *proto.AddDataPartitionRaftLearnerRequest, index uint64) (isUpdated bool, err error) {
	var (
		heartbeatPort int
		replicaPort   int
	)
	if heartbeatPort, replicaPort, err = dp.raftPort(); err != nil {
		return
	}

	addPeer := false
	for _, peer := range dp.config.Peers {
		if peer.ID == req.AddLearner.ID {
			addPeer = true
			break
		}
	}
	if !addPeer {
		peer := proto.Peer{ID: req.AddLearner.ID, Addr: req.AddLearner.Addr}
		dp.config.Peers = append(dp.config.Peers, peer)
		dp.config.Hosts = append(dp.config.Hosts, peer.Addr)
	}

	addLearner := false
	for _, learner := range dp.config.Learners {
		if learner.ID == req.AddLearner.ID {
			addLearner = true
			break
		}
	}
	if !addLearner {
		dp.config.Learners = append(dp.config.Learners, req.AddLearner)
	}
	isUpdated = !addPeer || !addLearner
	if !isUpdated {
		return
	}
	log.LogInfof("addRaftLearner: partitionID(%v) nodeID(%v) index(%v) data(%v) ",
		req.PartitionId, dp.config.NodeID, index, req)
	dp.replicasLock.Lock()
	dp.replicas = make([]string, len(dp.config.Hosts))
	copy(dp.replicas, dp.config.Hosts)
	dp.replicasLock.Unlock()
	addr := strings.Split(req.AddLearner.Addr, ":")[0]
	dp.config.RaftStore.AddNodeWithPort(req.AddLearner.ID, addr, heartbeatPort, replicaPort)
	return
}

// Promote a raft learner.
func (dp *DataPartition) promoteRaftLearner(req *proto.PromoteDataPartitionRaftLearnerRequest, index uint64) (isUpdated bool, err error) {
	var promoteIndex int
	for i, learner := range dp.config.Learners {
		if learner.ID == req.PromoteLearner.ID {
			isUpdated = true
			promoteIndex = i
			break
		}
	}
	if isUpdated {
		dp.config.Learners = append(dp.config.Learners[:promoteIndex], dp.config.Learners[promoteIndex+1:]...)
		log.LogInfof("promoteRaftLearner: partitionID(%v) nodeID(%v) index(%v) data(%v), new learners(%v) ",
			req.PartitionId, dp.config.NodeID, index, req, dp.config.Learners)
	}
	return
}

// Update a raft node.
func (dp *DataPartition) updateRaftNode(req *proto.DataPartitionDecommissionRequest, index uint64) (updated bool, err error) {
	log.LogDebugf("[updateRaftNode]: not support.")
	return
}

// LoadAppliedID loads the applied IDs to the memory.
func (dp *DataPartition) LoadAppliedID() (applied uint64, err error) {
	filename := path.Join(dp.Path(), ApplyIndexFile)
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		if os.IsNotExist(err) {
			err = nil
			return
		}
		err = errors.NewErrorf("[loadApplyIndex] OpenFile: %s", err.Error())
		return
	}
	if len(data) == 0 {
		err = errors.NewErrorf("[loadApplyIndex]: ApplyIndex is empty")
		return
	}
	if _, err = fmt.Sscanf(string(data), "%d", &applied); err != nil {
		err = errors.NewErrorf("[loadApplyID] ReadApplyID: %s", err.Error())
		return
	}
	return
}

func (dp *DataPartition) TruncateRaftWAL(id uint64) (err error) {
	if dp.raftPartition != nil {
		if _, success := dp.applyStatus.AdvanceTruncated(id); success {
			if err = dp.persist(nil, true); err != nil {
				log.LogErrorf("partition[%v] persisted metadata before truncate raft WAL failed: %v", dp.partitionID, id)
				return
			}
			dp.raftPartition.Truncate(id)
			if log.IsInfoEnabled() {
				log.LogInfof("partition[%v] advance Raft WAL truncate to [%v]", dp.partitionID, id)
			}
		}
	}
	return
}

func (dp *DataPartition) GetAppliedID() (id uint64) {
	return dp.applyStatus.Applied()
}

func (dp *DataPartition) GetPersistedAppliedID() (id uint64) {
	return dp.persistedApplied
}

func (dp *DataPartition) SetConsistencyMode(mode proto.ConsistencyMode) {
	if mode == proto.StrictMode && dp.config.ReplicaNum < StrictModeMinReplicaNum {
		mode = proto.StandardMode
	}
	if current := dp.config.Mode; current != mode {
		dp.config.Mode = mode
		if dp.raftPartition != nil {
			dp.raftPartition.SetConsistencyMode(mode)
		}
		if log.IsInfoEnabled() {
			log.LogInfof("SetConsistencyMode: Partition(%v) consistency mode changes [%v -> %v]", dp.partitionID, current, mode)
		}
		if err := dp.persistMetaDataOnly(); err != nil {
			log.LogErrorf("SetConsistencyMode: Partition(%v) persist metadata failed: %v", dp.partitionID, err)
		}
	}
}

func (dp *DataPartition) GetConsistencyMode() proto.ConsistencyMode {
	if dp.raftPartition != nil {
		return dp.raftPartition.GetConsistencyMode()
	}
	return dp.config.Mode
}

func (dp *DataPartition) findMinID(allIDs map[string]uint64) (minID uint64, host string) {
	minID = math.MaxUint64
	for k, v := range allIDs {
		if v < minID {
			minID = v
			host = k
		}
	}
	return minID, host
}

func (dp *DataPartition) findMaxID(allIDs map[string]uint64) (maxID uint64, host string) {
	for k, v := range allIDs {
		if v > maxID {
			maxID = v
			host = k
		}
	}
	return maxID, host
}

// Get the partition size from the leader.
func (dp *DataPartition) getLeaderPartitionSize(ctx context.Context, maxExtentID uint64) (size uint64, err error) {
	var (
		conn *net.TCPConn
	)

	p := NewPacketToGetPartitionSize(ctx, dp.partitionID)
	p.ExtentID = maxExtentID
	replicas := dp.getReplicaClone()
	if len(replicas) == 0 {
		err = errors.Trace(err, " partition(%v) get LeaderHost failed ", dp.partitionID)
		return
	}
	target := replicas[0]
	conn, err = gConnPool.GetConnect(target) //get remote connect
	if err != nil {
		err = errors.Trace(err, " partition(%v) get host(%v) connect", dp.partitionID, target)
		return
	}
	defer gConnPool.PutConnect(conn, true)
	err = p.WriteToConn(conn, proto.WriteDeadlineTime) // write command to the remote host
	if err != nil {
		err = errors.Trace(err, "partition(%v) write to host(%v)", dp.partitionID, target)
		return
	}
	err = p.ReadFromConn(conn, 60)
	if err != nil {
		err = errors.Trace(err, "partition(%v) read from host(%v)", dp.partitionID, target)
		return
	}

	if p.ResultCode != proto.OpOk {
		err = errors.Trace(err, "partition(%v) result code not ok(%v) from host(%v)", dp.partitionID, p.ResultCode, target)
		return
	}
	size = binary.BigEndian.Uint64(p.Data[0:8])
	log.LogInfof("partition(%v) MaxExtentID(%v) size(%v)", dp.partitionID, maxExtentID, size)

	return
}

// Get the MaxExtentID partition  from the leader.
func (dp *DataPartition) getLeaderMaxExtentIDAndPartitionSize(ctx context.Context) (maxExtentID, PartitionSize uint64, err error) {
	var (
		conn *net.TCPConn
	)

	p := NewPacketToGetMaxExtentIDAndPartitionSIze(ctx, dp.partitionID)
	replicas := dp.getReplicaClone()
	if len(replicas) == 0 {
		err = errors.Trace(err, " partition(%v) get Leader failed ", dp.partitionID)
		return
	}
	target := replicas[0]
	conn, err = gConnPool.GetConnect(target) //get remote connect
	if err != nil {
		err = errors.Trace(err, " partition(%v) get host(%v) connect", dp.partitionID, target)
		return
	}
	defer gConnPool.PutConnect(conn, true)
	err = p.WriteToConn(conn, proto.WriteDeadlineTime) // write command to the remote host
	if err != nil {
		err = errors.Trace(err, "partition(%v) write to host(%v)", dp.partitionID, target)
		return
	}
	err = p.ReadFromConn(conn, 60)
	if err != nil {
		err = errors.Trace(err, "partition(%v) read from host(%v)", dp.partitionID, target)
		return
	}

	if p.ResultCode != proto.OpOk {
		err = errors.Trace(err, "partition(%v) result code not ok(%v) from host(%v)", dp.partitionID, p.ResultCode, target)
		return
	}
	maxExtentID = binary.BigEndian.Uint64(p.Data[0:8])
	PartitionSize = binary.BigEndian.Uint64(p.Data[8:16])

	log.LogInfof("partition(%v) maxExtentID(%v) PartitionSize(%v) on leader", dp.partitionID, maxExtentID, PartitionSize)

	return
}

func (dp *DataPartition) broadcastTruncateRaftWAL(ctx context.Context, truncateID uint64) (err error) {
	replicas := dp.getReplicaClone()
	if len(replicas) == 0 {
		err = errors.Trace(err, " partition(%v) get replicas failed,replicas is nil. ", dp.partitionID)
		log.LogErrorf(err.Error())
		return
	}
	var wg = new(sync.WaitGroup)
	for i := 0; i < len(replicas); i++ {
		target := replicas[i]
		if dp.IsLocalAddress(target) {
			dp.TruncateRaftWAL(truncateID)
			continue
		}
		wg.Add(1)
		go func(target string) {
			defer wg.Done()
			p := NewPacketToBroadcastMinAppliedID(ctx, dp.partitionID, truncateID)
			var conn *net.TCPConn
			conn, err = gConnPool.GetConnect(target)
			if err != nil {
				return
			}
			defer gConnPool.PutConnect(conn, true)
			err = p.WriteToConn(conn, proto.WriteDeadlineTime)
			if err != nil {
				return
			}
			err = p.ReadFromConn(conn, 60)
			if err != nil {
				return
			}
			gConnPool.PutConnect(conn, true)
			if log.IsInfoEnabled() {
				log.LogInfof("partition[%v] broadcast raft WAL next truncate ID [%v] to replica[%v]", dp.partitionID, truncateID, target)
			}
		}(target)
	}
	wg.Wait()
	return
}

// Get all replica commit ids
func (dp *DataPartition) getRemoteReplicaCommitID(ctx context.Context) (commitIDMap map[string]uint64, replyNum uint8) {
	hosts := dp.getReplicaClone()
	if len(hosts) == 0 {
		log.LogErrorf("action[getRemoteReplicaCommitID] partition(%v) replicas is nil.", dp.partitionID)
		return
	}
	commitIDMap = make(map[string]uint64, len(hosts))
	errSlice := make(map[string]error)
	var (
		wg   sync.WaitGroup
		lock sync.Mutex
	)
	for _, host := range hosts {
		if dp.IsLocalAddress(host) {
			continue
		}
		wg.Add(1)
		go func(curAddr string) {
			var commitID uint64
			var err error
			commitID, err = dp.getRemoteCommitID(curAddr)
			if commitID == 0 {
				log.LogDebugf("action[getRemoteReplicaCommitID] partition(%v) replicaHost(%v) commitID=0",
					dp.partitionID, curAddr)
			}
			ok := false
			lock.Lock()
			defer lock.Unlock()
			if err != nil {
				log.LogErrorf("action[getRemoteReplicaCommitID] partition(%v) failed, err:%v", dp.partitionID, err)
				errSlice[curAddr] = err
			} else {
				commitIDMap[curAddr] = commitID
				ok = true
			}
			log.LogDebugf("action[getRemoteReplicaCommitID]: get commit id[%v] ok[%v] from host[%v], pid[%v]", commitID, ok, curAddr, dp.partitionID)
			wg.Done()
		}(host)
	}
	wg.Wait()
	replyNum = uint8(len(hosts) - 1 - len(errSlice))
	log.LogDebugf("action[getRemoteReplicaCommitID]: get commit id from hosts[%v], pid[%v]", hosts, dp.partitionID)
	return
}

// Get all replica applied ids
func (dp *DataPartition) getPersistedAppliedIDFromReplicas(ctx context.Context, hosts []string, timeoutNs, readTimeoutNs int64) (appliedIDs map[string]uint64) {
	if len(hosts) == 0 {
		log.LogErrorf("action[getAllReplicaAppliedID] partition(%v) replicas is nil.", dp.partitionID)
		return
	}
	appliedIDs = make(map[string]uint64)
	var futures = make(map[string]*async.Future) // host -> future
	for _, host := range hosts {
		if dp.IsLocalAddress(host) {
			appliedIDs[host] = dp.persistedApplied
			continue
		}
		var future = async.NewFuture()
		go func(future *async.Future, curAddr string) {
			var appliedID uint64
			var err error
			appliedID, err = dp.getRemotePersistedAppliedID(ctx, curAddr, timeoutNs, readTimeoutNs)
			if err == nil {
				future.Respond(appliedID, nil)
				return
			}
			if !strings.Contains(err.Error(), repl.ErrorUnknownOp.Error()) {
				future.Respond(nil, err)
				return
			}
			// TODO: 以下为版本过度而设计的兼容逻辑，由于获取PersistedAppliedID为新增接口，为避免在灰度过程中无法正常进行WAL的Truncate调度。 请在下一个版本移除。
			if log.IsWarnEnabled() {
				log.LogWarnf("partition[%v] get persisted applied ID failed cause remote [%v] respond result code [%v], try to get current applied ID.",
					dp.partitionID, curAddr, repl.ErrorUnknownOp.Error())
			}
			appliedID, err = dp.getRemoteAppliedID(ctx, curAddr, timeoutNs, readTimeoutNs)
			if err != nil {
				future.Respond(nil, err)
				return
			}
			future.Respond(appliedID, nil)
		}(future, host)
		futures[host] = future
	}

	for host, future := range futures {
		resp, err := future.Response()
		if err != nil {
			log.LogWarnf("partition[%v] get applied ID from remote[%v] failed: %v", dp.partitionID, host, err)
			continue
		}
		applied := resp.(uint64)
		appliedIDs[host] = applied
		if log.IsDebugEnabled() {
			log.LogDebugf("partition[%v]: get applied ID [%v] from remote[%v] ", dp.partitionID, host, applied)
		}
	}

	if log.IsDebugEnabled() {
		log.LogDebugf("partition[%v] get applied ID from all replications, hosts[%v], respond[%v]", dp.partitionID, hosts, appliedIDs)
	}
	return
}

// Get target members' commit id
func (dp *DataPartition) getRemoteCommitID(target string) (commitID uint64, err error) {
	if dp.disk == nil || dp.disk.space == nil || dp.disk.space.dataNode == nil {
		err = fmt.Errorf("action[getRemoteCommitID] data node[%v] not ready", target)
		return
	}
	profPort := dp.disk.space.dataNode.httpPort
	httpAddr := fmt.Sprintf("%v:%v", strings.Split(target, ":")[0], profPort)
	dataClient := http_client.NewDataClient(httpAddr, false)
	var hardState proto.HardState
	hardState, err = dataClient.GetPartitionRaftHardState(dp.partitionID)
	if err != nil {
		err = fmt.Errorf("action[getRemoteCommitID] datanode[%v] get partition failed, err:%v", target, err)
		return
	}
	commitID = hardState.Commit
	log.LogDebugf("[getRemoteCommitID] partition(%v) remoteCommitID(%v)", dp.partitionID, commitID)
	return
}

func (dp *DataPartition) getLocalAppliedID() {

}

// Get target members' applied id
func (dp *DataPartition) getRemoteAppliedID(ctx context.Context, target string, timeoutNs, readTimeoutNs int64) (appliedID uint64, err error) {
	p := NewPacketToGetAppliedID(ctx, dp.partitionID)
	if err = dp.sendTcpPacket(target, p, timeoutNs, readTimeoutNs); err != nil {
		return
	}
	appliedID = binary.BigEndian.Uint64(p.Data[0:8])
	log.LogDebugf("[getRemoteAppliedID] partition(%v) remoteAppliedID(%v)", dp.partitionID, appliedID)
	return
}

// Get target members' persisted applied id
func (dp *DataPartition) getRemotePersistedAppliedID(ctx context.Context, target string, timeoutNs, readTimeoutNs int64) (appliedID uint64, err error) {
	p := NewPacketToGetPersistedAppliedID(ctx, dp.partitionID)
	if err = dp.sendTcpPacket(target, p, timeoutNs, readTimeoutNs); err != nil {
		return
	}
	appliedID = binary.BigEndian.Uint64(p.Data[0:8])
	log.LogDebugf("[getRemotePersistedAppliedID] partition(%v) remoteAppliedID(%v)", dp.partitionID, appliedID)
	return
}

func (dp *DataPartition) sendTcpPacket(target string, p *repl.Packet, timeout, readTimeout int64) (err error) {
	var conn *net.TCPConn
	start := time.Now().UnixNano()
	defer func() {
		if err != nil {
			err = fmt.Errorf(p.LogMessage(p.GetOpMsg(), target, start, err))
			log.LogErrorf(err.Error())
		}
	}()

	conn, err = gConnPool.GetConnect(target)
	if err != nil {
		return
	}
	defer gConnPool.PutConnect(conn, true)
	err = p.WriteToConnNs(conn, timeout) // write command to the remote host
	if err != nil {
		return
	}
	err = p.ReadFromConnNs(conn, readTimeout)
	if err != nil {
		return
	}
	if p.ResultCode != proto.OpOk {
		err = errors.NewErrorf("partition(%v) result code not ok(%v) from host(%v)", dp.partitionID, p.ResultCode, target)
		return
	}
	log.LogDebugf("[sendTcpPacket] partition(%v)", dp.partitionID)
	return
}

// Get all members' applied ids and find the minimum one
func (dp *DataPartition) scheduleRaftWALTruncate(ctx context.Context) {
	var minPersistedAppliedID uint64

	if !dp.IsRaftStarted() {
		return
	}

	// Get the applied id by the leader
	_, isLeader := dp.IsRaftLeader()
	if !isLeader {
		return
	}

	var replicas = dp.getReplicaClone()

	// Check replicas number
	var replicaNum = dp.config.ReplicaNum
	if len(replicas) != replicaNum {
		if log.IsDebugEnabled() {
			log.LogDebugf("partition[%v] skips schedule raft WAL truncate cause replicas number illegal",
				dp.partitionID)
		}
		return
	}
	var raftStatus = dp.raftPartition.Status()
	if len(raftStatus.Replicas) != replicaNum {
		if log.IsDebugEnabled() {
			log.LogDebugf("partition[%v] skips schedule raft WAL truncate cause replicas number illegal",
				dp.partitionID)
		}
		return
	}
	for id, replica := range raftStatus.Replicas {
		if strings.Contains(replica.State, "ReplicaStateSnapshot") || replica.Match < raftStatus.Log.FirstIndex {
			if log.IsDebugEnabled() {
				log.LogDebugf("partition[%v] skips schedule raft WAL truncate cause found replica %v[%v] not in ready state",
					dp.partitionID, id, replica)
			}
			return
		}
	}

	var persistedAppliedIDs = dp.getPersistedAppliedIDFromReplicas(ctx, replicas, proto.WriteDeadlineTime*1e9, 60*1e9)
	if len(persistedAppliedIDs) == 0 {
		log.LogDebugf("[scheduleRaftWALTruncate] PartitionID(%v) Get appliedId failed!", dp.partitionID)
		return
	}
	if len(persistedAppliedIDs) == len(replicas) { // update dp.minPersistedAppliedID when every member had replied
		minPersistedAppliedID, _ = dp.findMinID(persistedAppliedIDs)
		if log.IsInfoEnabled() {
			log.LogInfof("partition[%v] computed min persisted applied ID [%v] from replication group [%v]",
				dp.partitionID, minPersistedAppliedID, persistedAppliedIDs)
		}
		dp.broadcastTruncateRaftWAL(ctx, minPersistedAppliedID)
	}
	return
}

func (dp *DataPartition) isNeedFaultCheck() bool {
	return dp.needServerFaultCheck
}

func (dp *DataPartition) setNeedFaultCheck(need bool) {
	dp.needServerFaultCheck = need
}

func (dp *DataPartition) getServerFaultCheckQuorum() uint8 {
	switch dp.serverFaultCheckLevel {
	case CheckAllCommitID:
		return uint8(len(dp.replicas)) - 1
	case CheckQuorumCommitID:
		return uint8(len(dp.replicas) / 2)
	default:
		return 0
	}
}

func (dp *DataPartition) listenStoredRaftLogEntry(entry *raftProto.Entry) {
	var command []byte = nil
	switch entry.Type {
	case raftProto.EntryNormal:
		if len(entry.Data) > 0 {
			command = entry.Data
		}

	case raftProto.EntryRollback:
		rollback := new(raftProto.Rollback)
		rollback.Decode(entry.Data)
		if len(rollback.Data) > 0 {
			command = rollback.Data
		}

	default:
	}

	if command != nil && len(command) > 0 {
		if opItem, err := UnmarshalRandWriteRaftLog(entry.Data, false); err == nil && opItem.opcode == proto.OpRandomWrite {
			dp.actionHolder.Register(entry.Index, &extentAction{
				extentID: opItem.extentID,
				offset:   opItem.offset,
				size:     opItem.size,
			})
		}
	}
}

func (dp *DataPartition) checkAndWaitForPendingActionApplied(extentID uint64, offset, size int64) (err error) {
	var ctx, _ = context.WithTimeout(context.Background(), time.Second*3)
	err = dp.actionHolder.Wait(ctx, &extentAction{
		extentID: extentID,
		offset:   offset,
		size:     size,
	})
	return
}

// validate crc task
func (dp *DataPartition) runValidateCRC(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			msg := fmt.Sprintf("DataPartition(%v) runValidateCRC panic(%v)", dp.partitionID, r)
			log.LogWarnf(msg)
		}
	}()
	if dp.partitionStatus == proto.Unavailable {
		return
	}
	if !dp.isReplLeader {
		return
	}

	start := time.Now().UnixNano()
	log.LogInfof("action[runValidateCRC] partition(%v) start.", dp.partitionID)
	replicas := dp.getReplicaClone()
	if len(replicas) == 0 {
		log.LogErrorf("action[runValidateCRC] partition(%v) replicas is nil.", dp.partitionID)
		return
	}

	validateCRCTasks := make([]*DataPartitionValidateCRCTask, len(replicas))
	err := dp.buildDataPartitionValidateCRCTask(ctx, validateCRCTasks, replicas)
	if err != nil {
		log.LogErrorf("action[runValidateCRC] partition(%v) err(%v).", dp.partitionID, err)
		if isGetConnectError(err) || isConnectionRefusedFailure(err) || isIOTimeoutFailure(err) || isPartitionRecoverFailure(err) {
			return
		}
		dpCrcInfo := proto.DataPartitionExtentCrcInfo{
			PartitionID:               dp.partitionID,
			IsBuildValidateCRCTaskErr: true,
			ErrMsg:                    err.Error(),
		}
		if err = MasterClient.NodeAPI().DataNodeValidateCRCReport(&dpCrcInfo); err != nil {
			log.LogErrorf("report DataPartition Validate CRC result failed,PartitionID(%v) err:%v", dp.partitionID, err)
			return
		}
		return
	}

	dp.validateCRC(validateCRCTasks)
	end := time.Now().UnixNano()
	log.LogWarnf("action[runValidateCRC] partition(%v) finish cost[%vms].", dp.partitionID, (end-start)/int64(time.Millisecond))
}

func isGetConnectError(err error) bool {
	if strings.Contains(err.Error(), errorGetConnectMsg) {
		return true
	}
	return false
}

func isConnectionRefusedFailure(err error) bool {
	if strings.Contains(strings.ToLower(err.Error()), strings.ToLower(errorConnRefusedMsg)) {
		return true
	}
	return false
}

func isIOTimeoutFailure(err error) bool {
	if strings.Contains(strings.ToLower(err.Error()), strings.ToLower(errorIOTimeoutMsg)) {
		return true
	}
	return false
}
func isPartitionRecoverFailure(err error) bool {
	if strings.Contains(strings.ToLower(err.Error()), strings.ToLower(errorPartitionRecoverMsg)) {
		return true
	}
	return false
}

func (dp *DataPartition) buildDataPartitionValidateCRCTask(ctx context.Context, validateCRCTasks []*DataPartitionValidateCRCTask, replicas []string) (err error) {
	if isRecover, err1 := dp.isRecover(); err1 != nil || isRecover {
		err = fmt.Errorf(errorPartitionRecoverMsg)
		return
	}
	// get the local extent info
	extents, err := dp.getLocalExtentInfoForValidateCRC()
	if err != nil {
		return err
	}
	leaderAddr := replicas[0]
	// new validate crc task for the leader
	validateCRCTasks[0] = NewDataPartitionValidateCRCTask(extents, leaderAddr, leaderAddr)
	validateCRCTasks[0].addr = leaderAddr

	// new validate crc task for the followers
	for index := 1; index < len(replicas); index++ {
		var followerExtents []storage.ExtentInfoBlock
		followerAddr := replicas[index]
		if followerExtents, err = dp.getRemoteExtentInfoForValidateCRCWithRetry(ctx, followerAddr); err != nil {
			return
		}
		validateCRCTasks[index] = NewDataPartitionValidateCRCTask(followerExtents, followerAddr, leaderAddr)
		validateCRCTasks[index].addr = followerAddr
	}
	return
}

func (dp *DataPartition) isRecover() (isRecover bool, err error) {
	var replyNum uint8
	isRecover, replyNum = dp.getRemoteReplicaRecoverStatus(context.Background())
	if int(replyNum) < len(dp.config.Hosts)/2 {
		err = fmt.Errorf("reply from remote replica is no enough")
		return
	}
	return isRecover, nil
}

// Get all replica recover status
func (dp *DataPartition) getRemoteReplicaRecoverStatus(ctx context.Context) (recovering bool, replyNum uint8) {
	hosts := dp.getReplicaClone()
	if len(hosts) == 0 {
		log.LogErrorf("action[getRemoteReplicaRecoverStatus] partition(%v) replicas is nil.", dp.partitionID)
		return
	}
	errSlice := make(map[string]error)
	var (
		wg   sync.WaitGroup
		lock sync.Mutex
	)
	for _, host := range hosts {
		if dp.IsLocalAddress(host) {
			continue
		}
		wg.Add(1)
		go func(curAddr string) {
			var isRecover bool
			var err error
			isRecover, err = dp.isRemotePartitionRecover(curAddr)

			ok := false
			lock.Lock()
			defer lock.Unlock()
			if err != nil {
				errSlice[curAddr] = err
			} else {
				ok = true
				if isRecover {
					recovering = true
				}
			}
			log.LogDebugf("action[getRemoteReplicaRecoverStatus]: get commit id[%v] ok[%v] from host[%v], pid[%v]", isRecover, ok, curAddr, dp.partitionID)
			wg.Done()
		}(host)
	}
	wg.Wait()
	replyNum = uint8(len(hosts) - 1 - len(errSlice))
	log.LogDebugf("action[getRemoteReplicaRecoverStatus]: get commit id from hosts[%v], pid[%v]", hosts, dp.partitionID)
	return
}

// Get target members recover status
func (dp *DataPartition) isRemotePartitionRecover(target string) (isRecover bool, err error) {
	if dp.disk == nil || dp.disk.space == nil || dp.disk.space.dataNode == nil {
		err = fmt.Errorf("action[getRemotePartitionSimpleInfo] data node[%v] not ready", target)
		return
	}
	profPort := dp.disk.space.dataNode.httpPort
	httpAddr := fmt.Sprintf("%v:%v", strings.Split(target, ":")[0], profPort)
	dataClient := http_client.NewDataClient(httpAddr, false)
	var dpInfo *proto.DNDataPartitionInfo
	for i := 0; i < 3; i++ {
		dpInfo, err = dataClient.GetPartitionSimple(dp.partitionID)
		if err == nil {
			break
		}
	}
	if err != nil {
		err = fmt.Errorf("action[getRemotePartitionSimpleInfo] datanode[%v] get partition failed, err:%v", target, err)
		return
	}
	isRecover = dpInfo.IsRecover
	log.LogDebugf("[getRemotePartitionSimpleInfo] partition(%v) recover(%v)", dp.partitionID, dpInfo.IsRecover)
	return
}
func (dp *DataPartition) getLocalExtentInfoForValidateCRC() (extents []storage.ExtentInfoBlock, err error) {
	if !dp.ExtentStore().IsFinishLoad() {
		err = storage.PartitionIsLoaddingErr
		return
	}
	extents, err = dp.extentStore.GetAllWatermarks(proto.AllExtentType, storage.ExtentFilterForValidateCRC())
	if err != nil {
		err = fmt.Errorf("getLocalExtentInfoForValidateCRC DataPartition(%v) err:%v", dp.partitionID, err)
		return
	}
	return
}

func (dp *DataPartition) getRemoteExtentInfoForValidateCRCWithRetry(ctx context.Context, target string) (extentFiles []storage.ExtentInfoBlock, err error) {
	for i := 0; i < GetRemoteExtentInfoForValidateCRCRetryTimes; i++ {
		extentFiles, err = dp.getRemoteExtentInfoForValidateCRC(ctx, target)
		if err == nil {
			return
		}
		log.LogWarnf("getRemoteExtentInfoForValidateCRCWithRetry PartitionID(%v) on(%v) err(%v)", dp.partitionID, target, err)
	}
	return
}

func (dp *DataPartition) getRemoteExtentInfoForValidateCRC(ctx context.Context, target string) (extentFiles []storage.ExtentInfoBlock, err error) {
	var packet = proto.NewPacketToGetAllExtentInfo(ctx, dp.partitionID)
	var conn *net.TCPConn
	if conn, err = gConnPool.GetConnect(target); err != nil {
		err = errors.Trace(err, errorGetConnectMsg)
		return
	}
	defer func() {
		gConnPool.PutConnectWithErr(conn, err)
	}()
	if err = packet.WriteToConn(conn, proto.WriteDeadlineTime); err != nil {
		err = errors.Trace(err, "write packet to connection failed")
		return
	}
	var reply = new(repl.Packet)
	reply.SetCtx(ctx)
	if err = reply.ReadFromConn(conn, proto.GetAllWatermarksDeadLineTime); err != nil {
		err = errors.Trace(err, "read reply from connection failed")
		return
	}
	if reply.ResultCode != proto.OpOk {
		err = errors.NewErrorf("reply result code: %v", reply.GetOpMsg())
		return
	}
	if reply.Size%20 != 0 {
		// 合法的data长度与20对齐，每20个字节存储一个Extent信息，[0:8)为FileID，[8:16)为Size，[16:20)为Crc
		err = errors.NewErrorf("illegal result data length: %v", len(reply.Data))
		return
	}
	extentFiles = make([]storage.ExtentInfoBlock, 0, len(reply.Data)/20)
	for index := 0; index < int(reply.Size)/20; index++ {
		var offset = index * 20
		var extentID = binary.BigEndian.Uint64(reply.Data[offset:])
		var size = binary.BigEndian.Uint64(reply.Data[offset+8:])
		var crc = binary.BigEndian.Uint32(reply.Data[offset+16:])
		eiBlock := storage.ExtentInfoBlock{
			storage.FileID: extentID,
			storage.Size:   size,
			storage.Crc:    uint64(crc),
		}
		extentFiles = append(extentFiles, eiBlock)
	}
	return
}

func (dp *DataPartition) validateCRC(validateCRCTasks []*DataPartitionValidateCRCTask) {
	if len(validateCRCTasks) <= 1 {
		return
	}
	var (
		extentInfo          storage.ExtentInfoBlock
		ok                  bool
		extentReplicaInfos  []storage.ExtentInfoBlock
		extentReplicaSource map[int]string
		extentCrcInfo       *proto.ExtentCrcInfo
		crcNotEqual         bool
		extentCrcResults    []*proto.ExtentCrcInfo
	)
	for extentID, localExtentInfo := range validateCRCTasks[0].extents {
		extentReplicaInfos = make([]storage.ExtentInfoBlock, 0, len(validateCRCTasks))
		extentReplicaSource = make(map[int]string, len(validateCRCTasks))
		extentReplicaInfos = append(extentReplicaInfos, localExtentInfo)
		extentReplicaSource[0] = validateCRCTasks[0].Source
		for i := 1; i < len(validateCRCTasks); i++ {
			extentInfo, ok = validateCRCTasks[i].extents[extentID]
			if !ok {
				continue
			}
			extentReplicaInfos = append(extentReplicaInfos, extentInfo)
			extentReplicaSource[len(extentReplicaInfos)-1] = validateCRCTasks[i].Source
		}
		if proto.IsTinyExtent(extentID) {
			extentCrcInfo, crcNotEqual = dp.checkTinyExtentFile(extentReplicaInfos, extentReplicaSource)
		} else {
			extentCrcInfo, crcNotEqual = dp.checkNormalExtentFile(extentReplicaInfos, extentReplicaSource)
		}
		if crcNotEqual {
			extentCrcResults = append(extentCrcResults, extentCrcInfo)
		}
	}

	if len(extentCrcResults) != 0 {
		dpCrcInfo := proto.DataPartitionExtentCrcInfo{
			PartitionID:    dp.partitionID,
			ExtentCrcInfos: extentCrcResults,
		}
		if err := MasterClient.NodeAPI().DataNodeValidateCRCReport(&dpCrcInfo); err != nil {
			log.LogErrorf("report DataPartition Validate CRC result failed,PartitionID(%v) err:%v", dp.partitionID, err)
			return
		}
	}
	return
}

func (dp *DataPartition) checkTinyExtentFile(extentInfos []storage.ExtentInfoBlock, extentReplicaSource map[int]string) (extentCrcInfo *proto.ExtentCrcInfo, crcNotEqual bool) {
	if len(extentInfos) <= 1 {
		return
	}
	if !needCrcRepair(extentInfos) {
		return
	}
	if !hasSameSize(extentInfos) {
		sb := new(strings.Builder)
		sb.WriteString(fmt.Sprintf("checkTinyExtentFileErr size not match, dpID[%v] FileID[%v] ", dp.partitionID, extentInfos[0][storage.FileID]))
		for i, extentInfo := range extentInfos {
			sb.WriteString(fmt.Sprintf("fm[%v]:size[%v] ", extentReplicaSource[i], extentInfo[storage.Size]))
		}
		log.LogWarn(sb.String())
		return
	}
	extentCrcInfo, crcNotEqual = getExtentCrcInfo(extentInfos, extentReplicaSource)
	return
}

func (dp *DataPartition) checkNormalExtentFile(extentInfos []storage.ExtentInfoBlock, extentReplicaSource map[int]string) (extentCrcInfo *proto.ExtentCrcInfo, crcNotEqual bool) {
	if len(extentInfos) <= 1 {
		return
	}
	if !needCrcRepair(extentInfos) {
		return
	}
	extentCrcInfo, crcNotEqual = getExtentCrcInfo(extentInfos, extentReplicaSource)
	return
}

func needCrcRepair(extentInfos []storage.ExtentInfoBlock) (needCheckCrc bool) {
	if len(extentInfos) <= 1 {
		return
	}
	baseCrc := extentInfos[0][storage.Crc]
	for _, extentInfo := range extentInfos {
		if extentInfo[storage.Crc] == 0 || extentInfo[storage.Crc] == EmptyCrcValue {
			return
		}
		if extentInfo[storage.Crc] != baseCrc {
			needCheckCrc = true
			return
		}
	}
	return
}

func hasSameSize(extentInfos []storage.ExtentInfoBlock) (same bool) {
	same = true
	if len(extentInfos) <= 1 {
		return
	}
	baseSize := extentInfos[0][storage.Size]
	for _, extentInfo := range extentInfos {
		if extentInfo[storage.Size] != baseSize {
			same = false
			return
		}
	}
	return
}

func getExtentCrcInfo(extentInfos []storage.ExtentInfoBlock, extentReplicaSource map[int]string) (extentCrcInfo *proto.ExtentCrcInfo, crcNotEqual bool) {
	if len(extentInfos) <= 1 {
		return
	}
	crcLocAddrMap := make(map[uint64][]string)
	for i, extentInfo := range extentInfos {
		crcLocAddrMap[extentInfo[storage.Crc]] = append(crcLocAddrMap[extentInfo[storage.Crc]], extentReplicaSource[i])
	}
	if len(crcLocAddrMap) <= 1 {
		return
	}
	crcNotEqual = true
	extentCrcInfo = &proto.ExtentCrcInfo{
		FileID:        extentInfos[0][storage.FileID],
		ExtentNum:     len(extentInfos),
		CrcLocAddrMap: crcLocAddrMap,
	}
	return
}

// repair
// Main function to perform the repair.
// The repair process can be described as follows:
// There are two types of repairs.
// The first one is called the normal extent repair, and the second one is called the tiny extent repair.
//  1. normal extent repair:
//     - the leader collects all the extent information from the followers.
//     - for each extent, we compare all the replicas to find the one with the largest size.
//     - periodically check the size of the local extent, and if it is smaller than the largest size,
//     add it to the tobeRepaired list, and generate the corresponding tasks.
//  2. tiny extent repair:
//     - when creating the new partition, add all tiny extents to the toBeRepaired list,
//     and the repair task will create all the tiny extents first.
//     - The leader of the replicas periodically collects the extent information of each follower
//     - for each extent, we compare all the replicas to find the one with the largest size.
//     - periodically check the size of the local extent, and if it is smaller than the largest size,
//     add it to the tobeRepaired list, and generate the corresponding tasks.
func (dp *DataPartition) repair(ctx context.Context, extentType uint8) {
	start := time.Now().UnixNano()
	if log.IsInfoEnabled() {
		log.LogInfof("DP %v: replication data repair start",
			dp.partitionID)
	}

	var tinyExtents []uint64 // unavailable extents 小文件写
	if extentType == proto.TinyExtentType {
		tinyExtents = dp.brokenTinyExtents()
		if len(tinyExtents) == 0 {
			return
		}
	}
	replicas := dp.getReplicaClone()
	if len(replicas) == 0 {
		log.LogErrorf("DP %v: no valid replicas, skip repair.", dp.partitionID)
		return
	}
	repairTasks, err := dp.buildDataPartitionRepairTask(ctx, replicas, extentType, tinyExtents)

	if err != nil {
		log.LogErrorf(errors.Stack(err))
		log.LogErrorf("action[repair] partition(%v) err(%v).",
			dp.partitionID, err)
		dp.moveToBrokenTinyExtentC(extentType, tinyExtents)
		return
	}

	// compare all the extents in the replicas to compute the good and bad ones
	availableTinyExtents, brokenTinyExtents := dp.prepareRepairTasks(repairTasks)

	// notify the replicas to repair the extent
	if err = dp.notifyFollowersToRepair(ctx, repairTasks); err != nil {
		dp.sendAllTinyExtentsToC(extentType, availableTinyExtents, brokenTinyExtents)
		log.LogErrorf("DP %v: notify followers to repair failed: %v",
			dp.partitionID, err)
		log.LogError(errors.Stack(err))
		return
	}

	// ask the leader to do the repair
	dp.DoRepairOnLeaderDisk(ctx, repairTasks[0])
	end := time.Now().UnixNano()

	// every time we need to figureAnnotatef out which extents need to be repaired and which ones do not.
	dp.sendAllTinyExtentsToC(extentType, availableTinyExtents, brokenTinyExtents)

	// error check
	if dp.extentStore.AvailableTinyExtentCnt()+dp.extentStore.BrokenTinyExtentCnt() != proto.TinyExtentCount {
		log.LogWarnf("action[repair] partition(%v) GoodTinyExtents(%v) "+
			"BadTinyExtents(%v) finish cost[%vms].", dp.partitionID, dp.extentStore.AvailableTinyExtentCnt(),
			dp.extentStore.BrokenTinyExtentCnt(), (end-start)/int64(time.Millisecond))
	}

	log.LogInfof("DP %v: replication data repair finish: AvailableTiny %v, BrokenTiny %v, elapsed %vms",
		dp.partitionID, dp.extentStore.AvailableTinyExtentCnt(), dp.extentStore.BrokenTinyExtentCnt(), (end-start)/int64(time.Millisecond))
}

func (dp *DataPartition) buildDataPartitionRepairTask(ctx context.Context, replicas []string, extentType uint8, tinyExtents []uint64) ([]*DataPartitionRepairTask, error) {
	var tasks = make([]*DataPartitionRepairTask, len(replicas))
	var localTinyDeleteRecordFileSize int64
	// get the local extent info
	localExtents, localBaseExtentID, err := dp.getLocalExtentInfo(extentType, tinyExtents)
	if err != nil {
		return nil, err
	}

	if !(dp.partitionStatus == proto.Unavailable) {
		localTinyDeleteRecordFileSize, err = dp.extentStore.LoadTinyDeleteFileOffset()
	}

	leaderAddr := replicas[0]
	// new repair task for the leader
	tasks[0] = NewDataPartitionRepairTask(leaderAddr, localExtents, localBaseExtentID, localTinyDeleteRecordFileSize, leaderAddr, leaderAddr)

	// new repair tasks for the followers
	for index := 1; index < len(replicas); index++ {
		followerAddr := replicas[index]
		extents, baseExtentID, err := dp.getRemoteExtentInfo(ctx, extentType, tinyExtents, followerAddr)
		if err != nil {
			log.LogErrorf("buildDataPartitionRepairTask PartitionID(%v) on(%v) err(%v)", dp.partitionID, followerAddr, err)
			continue
		}
		tasks[index] = NewDataPartitionRepairTask(followerAddr, extents, baseExtentID, localTinyDeleteRecordFileSize, followerAddr, leaderAddr)
	}
	return tasks, nil
}

func (dp *DataPartition) getLocalExtentInfo(extentType uint8, tinyExtents []uint64) (extents []storage.ExtentInfoBlock, baseExtentID uint64, err error) {
	if extentType == proto.NormalExtentType {
		if !dp.ExtentStore().IsFinishLoad() {
			err = storage.PartitionIsLoaddingErr
		} else {
			extents, err = dp.extentStore.GetAllWatermarks(extentType, storage.NormalExtentFilter())
		}
	} else {
		extents, err = dp.extentStore.GetAllWatermarks(extentType, storage.TinyExtentFilter(tinyExtents))
	}
	if err != nil {
		err = errors.Trace(err, "getLocalExtentInfo extent DataPartition(%v) GetAllWaterMark", dp.partitionID)
		return
	}
	baseExtentID = dp.extentStore.GetBaseExtentID()
	return
}

func (dp *DataPartition) getRemoteExtentInfo(ctx context.Context, extentType uint8, tinyExtents []uint64,
	target string) (extentFiles []storage.ExtentInfoBlock, baseExtentID uint64, err error) {

	// v1使用json序列化
	var version1Func = func() (extents []storage.ExtentInfoBlock, err error) {
		var packet = repl.NewPacketToGetAllWatermarks(ctx, dp.partitionID, extentType)
		if extentType == proto.TinyExtentType {
			if packet.Data, err = json.Marshal(tinyExtents); err != nil {
				err = errors.Trace(err, "marshal json failed")
				return
			}
			packet.Size = uint32(len(packet.Data))
		}
		var conn *net.TCPConn
		if conn, err = gConnPool.GetConnect(target); err != nil {
			err = errors.Trace(err, fmt.Sprintf("get connection failed: %v", err))
			return
		}
		defer func() {
			gConnPool.PutConnectWithErr(conn, err)
		}()
		if err = packet.WriteToConn(conn, proto.WriteDeadlineTime); err != nil {
			return
		}
		var reply = new(repl.Packet)
		reply.SetCtx(ctx)
		if err = reply.ReadFromConn(conn, proto.GetAllWatermarksDeadLineTime); err != nil {
			return
		}
		if reply.ResultCode != proto.OpOk {
			err = errors.New(string(reply.Data[:reply.Size]))
			return
		}
		extents = make([]storage.ExtentInfoBlock, 0)
		if err = json.Unmarshal(reply.Data[:reply.Size], &extents); err != nil {
			return
		}
		return
	}

	// v2使用二进制序列化
	var version2Func = func() (extents []storage.ExtentInfoBlock, err error) {
		var packet = repl.NewPacketToGetAllWatermarksV2(ctx, dp.partitionID, extentType)
		if extentType == proto.TinyExtentType {
			var buffer = bytes.NewBuffer(make([]byte, 0, len(tinyExtents)*8))
			for _, extentID := range tinyExtents {
				if err = binary.Write(buffer, binary.BigEndian, extentID); err != nil {
					err = errors.Trace(err, "binary encode failed")
					return
				}
			}
			packet.Data = buffer.Bytes()
			packet.Size = uint32(len(packet.Data))
		}
		var conn *net.TCPConn
		if conn, err = gConnPool.GetConnect(target); err != nil {
			err = errors.Trace(err, "get connection failed")
			return
		}
		defer func() {
			gConnPool.PutConnectWithErr(conn, err)
		}()
		if err = packet.WriteToConn(conn, proto.WriteDeadlineTime); err != nil {
			return
		}
		var reply = new(repl.Packet)
		reply.SetCtx(ctx)
		if err = reply.ReadFromConn(conn, proto.GetAllWatermarksDeadLineTime); err != nil {
			return
		}
		if reply.ResultCode != proto.OpOk {
			err = errors.New(string(reply.Data[:reply.Size]))
			return
		}
		if reply.Size%16 != 0 {
			// 合法的data长度与16对其，每16个字节存储一个Extent信息，[0:8)为FileID，[8:16)为Size。
			err = errors.NewErrorf("illegal result data length: %v", reply.Size)
			return
		}
		extents = make([]storage.ExtentInfoBlock, 0, reply.Size/16)
		for index := 0; index < int(reply.Size)/16; index++ {
			var offset = index * 16
			var extentID = binary.BigEndian.Uint64(reply.Data[offset : offset+8])
			var size = binary.BigEndian.Uint64(reply.Data[offset+8 : offset+16])
			eiBlock := storage.ExtentInfoBlock{
				storage.FileID: extentID,
				storage.Size:   size,
			}
			extents = append(extents, eiBlock)
		}
		return
	}

	// v3使用二进制序列化
	var version3Func = func() (extents []storage.ExtentInfoBlock, baseExtentID uint64, err error) {
		var packet = repl.NewPacketToGetAllWatermarksV3(ctx, dp.partitionID, extentType)
		if extentType == proto.TinyExtentType {
			var buffer = bytes.NewBuffer(make([]byte, 0, len(tinyExtents)*8))
			for _, extentID := range tinyExtents {
				if err = binary.Write(buffer, binary.BigEndian, extentID); err != nil {
					err = errors.Trace(err, "binary encode failed")
					return
				}
			}
			packet.Data = buffer.Bytes()
			packet.Size = uint32(len(packet.Data))
		}
		var conn *net.TCPConn
		if conn, err = gConnPool.GetConnect(target); err != nil {
			err = errors.Trace(err, "get connection failed")
			return
		}
		defer func() {
			gConnPool.PutConnectWithErr(conn, err)
		}()
		if err = packet.WriteToConn(conn, proto.WriteDeadlineTime); err != nil {
			err = errors.Trace(err, fmt.Sprintf("write packet to connection failed %v", err))
			return
		}
		var reply = new(repl.Packet)
		reply.SetCtx(ctx)
		if err = reply.ReadFromConn(conn, proto.GetAllWatermarksDeadLineTime); err != nil {
			err = errors.Trace(err, fmt.Sprintf("read reply from connection failed %v", err))
			return
		}
		if reply.ResultCode != proto.OpOk {
			err = errors.New(string(reply.Data[:reply.Size]))
			return
		}
		if reply.Size < 8 || (reply.Size-8)%16 != 0 {
			// 合法的data前8个字节存储baseExtentID且后续长度与16对其，每16个字节存储一个Extent信息，[0:8)为FileID，[8:16)为Size。
			err = errors.NewErrorf("illegal result data length: %v", reply.Size)
			return
		}
		baseExtentID = binary.BigEndian.Uint64(reply.Data[:8])
		extents = make([]storage.ExtentInfoBlock, 0, reply.Size/16)
		for index := 0; index < int(reply.Size-8)/16; index++ {
			var offset = 8 + index*16
			var extentID = binary.BigEndian.Uint64(reply.Data[offset : offset+8])
			var size = binary.BigEndian.Uint64(reply.Data[offset+8 : offset+16])
			eiBlock := storage.ExtentInfoBlock{
				storage.FileID: extentID,
				storage.Size:   size,
			}
			extents = append(extents, eiBlock)
		}
		return
	}

	var isUnknownOpError = func(err error) bool {
		return err != nil && strings.Contains(err.Error(), repl.ErrorUnknownOp.Error())
	}

	// 首先尝试使用V3版本获取远端Extent信息, 失败后则尝试使用V2和V1版本获取信息，以保证集群灰度过程中修复功能依然可以兼容。
	if extentFiles, baseExtentID, err = version3Func(); isUnknownOpError(err) {
		log.LogWarnf("partition(%v) get remote(%v) extent info by version 3 method failed"+
			" and will retry by using version 2 method: %v", dp.partitionID, target, err)
		if extentFiles, err = version2Func(); isUnknownOpError(err) {
			log.LogWarnf("partition(%v) get remote(%v) extent info by version 2 method failed"+
				" and will retry by using version 1 method: %v", dp.partitionID, target, err)
			extentFiles, err = version1Func()
		}
	}
	return
}

// DoRepairOnLeaderDisk asks the leader to perform the repair tasks.
func (dp *DataPartition) DoRepairOnLeaderDisk(ctx context.Context, repairTask *DataPartitionRepairTask) {
	store := dp.extentStore
	_ = store.AdvanceBaseExtentID(repairTask.BaseExtentID)
	for _, extentInfo := range repairTask.ExtentsToBeCreated {
		if !AutoRepairStatus {
			log.LogWarnf("AutoRepairStatus is False,so cannot Create extent(%v)", extentInfo.String())
			continue
		}
		if !store.IsFinishLoad() || store.IsDeleted(extentInfo[storage.FileID]) {
			continue
		}
		_ = store.Create(extentInfo[storage.FileID], extentInfo[storage.Inode], true)
	}

	if num := len(repairTask.ExtentsToBeDeleted); num > 0 {
		var batch = storage.BatchMarker(num)
		for _, extentInfo := range repairTask.ExtentsToBeDeleted {
			var extentID = extentInfo[storage.FileID]
			if !store.IsFinishLoad() || proto.IsTinyExtent(extentID) || store.IsDeleted(extentID) {
				continue
			}
			batch.Add(0, extentID, 0, 0)
		}
		_ = store.MarkDelete(batch)
	}

	var allReplicas = dp.getReplicaClone()
	for _, extentInfo := range repairTask.ExtentsToBeRepaired {
		if store.IsDeleted(extentInfo[storage.FileID]) {
			continue
		}
		majorSource := repairTask.ExtentsToBeRepairedSource[extentInfo[storage.FileID]]
		sources := []string{
			majorSource,
		}
		if len(allReplicas) > 0 {
			for _, replica := range allReplicas[1:] {
				if replica != majorSource {
					sources = append(sources, replica)
				}
			}
		}
		for _, source := range sources {
			err := dp.streamRepairExtent(ctx, extentInfo, source, NoForceRepair)
			if err != nil {
				err = errors.Trace(err, "DoRepairOnLeaderDisk %v", dp.applyRepairKey(int(extentInfo[storage.FileID])))
				localExtentInfo, opErr := dp.ExtentStore().Watermark(uint64(extentInfo[storage.FileID]))
				if opErr != nil {
					err = errors.Trace(err, opErr.Error())
				}
				err = errors.Trace(err, "partition(%v) remote(%v) local(%v)",
					dp.partitionID, extentInfo, localExtentInfo)
				log.LogWarnf("action[DoRepairOnLeaderDisk] err(%v).", err)
				continue
			}
			break
		}
	}
}

// DoExtentStoreRepairOnFollowerDisk performs the repairs of the extent store.
// 1. when the extent size is smaller than the max size on the record, start to repair the missing part.
// 2. if the extent does not even exist, create the extent first, and then repair.
func (dp *DataPartition) DoExtentStoreRepairOnFollowerDisk(repairTask *DataPartitionRepairTask) {
	store := dp.extentStore
	_ = store.AdvanceBaseExtentID(repairTask.BaseExtentID)
	for _, extentInfo := range repairTask.ExtentsToBeCreated {
		if proto.IsTinyExtent(extentInfo[storage.FileID]) || !dp.ExtentStore().IsFinishLoad() {
			continue
		}

		if store.IsDeleted(extentInfo[storage.FileID]) {
			continue
		}
		if store.IsExists(extentInfo[storage.FileID]) {
			var info = storage.ExtentInfoBlock{
				storage.FileID: extentInfo[storage.FileID],
				storage.Size:   extentInfo[storage.Size],
			}
			repairTask.ExtentsToBeRepaired = append(repairTask.ExtentsToBeRepaired, info)
			continue
		}
		if !AutoRepairStatus {
			log.LogWarnf("AutoRepairStatus is False,so cannot Create extent(%v)", extentInfo.String())
			continue
		}
		if err := store.Create(extentInfo[storage.FileID], extentInfo[storage.Inode], true); err != nil {
			continue
		}
		var info = storage.ExtentInfoBlock{
			storage.FileID: extentInfo[storage.FileID],
			storage.Size:   extentInfo[storage.Size],
		}
		repairTask.ExtentsToBeRepaired = append(repairTask.ExtentsToBeRepaired, info)
	}
	if num := len(repairTask.ExtentsToBeDeleted); num > 0 {
		var batch = storage.BatchMarker(num)
		for _, extentInfo := range repairTask.ExtentsToBeDeleted {
			var extentID = extentInfo[storage.FileID]
			if !store.IsFinishLoad() || proto.IsTinyExtent(extentID) || store.IsDeleted(extentID) {
				continue
			}
			batch.Add(0, extentID, 0, 0)
		}
		_ = store.MarkDelete(batch)
	}

	localAddr := fmt.Sprintf("%v:%v", LocalIP, LocalServerPort)
	allReplicas := dp.getReplicaClone()

	// 使用生产消费模型并行修复Extent。
	var startTime = time.Now()

	// 内部数据结构，用于包裹修复Extent相关必要信息
	type __ExtentRepairTask struct {
		ExtentInfo storage.ExtentInfoBlock
		Sources    []string
	}

	var extentRepairTaskCh = make(chan *__ExtentRepairTask, len(repairTask.ExtentsToBeRepaired))
	var extentRepairWorkerWG = new(sync.WaitGroup)
	for i := 0; i < NumOfFilesToRecoverInParallel; i++ {
		extentRepairWorkerWG.Add(1)
		var worker = func() {
			defer extentRepairWorkerWG.Done()
			for {
				var task = <-extentRepairTaskCh
				if task == nil {
					return
				}
				dp.doStreamExtentFixRepairOnFollowerDisk(context.Background(), task.ExtentInfo, task.Sources)
			}
		}
		var handlePanic = func(r interface{}) {
			// Worker 发生panic，进行报警
			var callstack = string(debug.Stack())
			log.LogCriticalf("Occurred panic while repair extent: %v\n"+
				"Callstack: %v\n", r, callstack)
			exporter.Warning(fmt.Sprintf("PANIC ORRCURRED!\n"+
				"Fix worker occurred panic and stopped:\n"+
				"Partition: %v\n"+
				"Message  : %v\n",
				dp.partitionID, r))
			return
		}
		async.RunWorker(worker, handlePanic)
	}
	var validExtentsToBeRepaired int
	for _, extentInfo := range repairTask.ExtentsToBeRepaired {
		if store.IsDeleted(extentInfo[storage.FileID]) || !store.IsExists(extentInfo[storage.FileID]) {
			continue
		}
		majorSource := repairTask.ExtentsToBeRepairedSource[extentInfo[storage.FileID]]
		sources := make([]string, 0, len(allReplicas))
		sources = append(sources, majorSource)
		for _, replica := range allReplicas {
			if replica == majorSource || replica == localAddr {
				continue
			}
			sources = append(sources, replica)
		}
		extentRepairTaskCh <- &__ExtentRepairTask{
			ExtentInfo: extentInfo,
			Sources:    sources,
		}
		validExtentsToBeRepaired++
	}
	close(extentRepairTaskCh)
	extentRepairWorkerWG.Wait()
	dp.doStreamFixTinyDeleteRecord(context.Background(), repairTask, time.Now().Unix()-dp.FullSyncTinyDeleteTime > MaxFullSyncTinyDeleteTime)
	log.LogInfof("partition[%v] repaired %v extents, cost %v", dp.partitionID, validExtentsToBeRepaired, time.Now().Sub(startTime))
}

func (dp *DataPartition) moveToBrokenTinyExtentC(extentType uint8, extents []uint64) {
	if extentType == proto.TinyExtentType {
		dp.extentStore.SendAllToBrokenTinyExtentC(extents)
	}
	return
}

func (dp *DataPartition) sendAllTinyExtentsToC(extentType uint8, availableTinyExtents, brokenTinyExtents []uint64) {
	if extentType != proto.TinyExtentType {
		return
	}
	for _, extentID := range availableTinyExtents {
		if proto.IsTinyExtent(extentID) {
			dp.extentStore.SendToAvailableTinyExtentC(extentID)
		}
	}
	for _, extentID := range brokenTinyExtents {
		if proto.IsTinyExtent(extentID) {
			dp.extentStore.SendToBrokenTinyExtentC(extentID)
		}
	}
}

func (dp *DataPartition) brokenTinyExtents() (brokenTinyExtents []uint64) {
	brokenTinyExtents = make([]uint64, 0)
	extentsToBeRepaired := MinTinyExtentsToRepair
	if dp.extentStore.AvailableTinyExtentCnt() <= MinAvaliTinyExtentCnt {
		extentsToBeRepaired = proto.TinyExtentCount
	}
	for i := 0; i < extentsToBeRepaired; i++ {
		extentID, err := dp.extentStore.GetBrokenTinyExtent()
		if err != nil {
			return
		}
		brokenTinyExtents = append(brokenTinyExtents, extentID)
	}
	return
}

func (dp *DataPartition) prepareRepairTasks(repairTasks []*DataPartitionRepairTask) (availableTinyExtents []uint64, brokenTinyExtents []uint64) {
	var (
		referenceExtents        = make(map[uint64]storage.ExtentInfoBlock)
		deletedExtents          = make(map[uint64]storage.ExtentInfoBlock)
		sources                 = make(map[uint64]string)
		maxBaseExtentID  uint64 = 0
	)

	for index := 0; index < len(repairTasks); index++ {
		repairTask := repairTasks[index]
		if repairTask == nil {
			continue
		}
		maxBaseExtentID = uint64(math.Max(float64(maxBaseExtentID), float64(repairTask.BaseExtentID)))
		for extentID, extentInfo := range repairTask.extents {
			if dp.extentStore.IsDeleted(extentID) {
				if _, exists := deletedExtents[extentID]; !exists {
					deletedExtents[extentID] = extentInfo
				}
				continue
			}
			info, exists := referenceExtents[extentID]
			if !exists || extentInfo[storage.Size] > info[storage.Size] {
				referenceExtents[extentID] = extentInfo
				sources[extentID] = repairTask.addr
			}
		}
	}

	dp.generateExtentCreationTasks(repairTasks, referenceExtents, sources)
	dp.generateBaseExtentIDTasks(repairTasks, maxBaseExtentID)
	availableTinyExtents, brokenTinyExtents = dp.generateExtentRepairTasks(repairTasks, referenceExtents, sources)
	dp.generateExtentDeletionTasks(repairTasks, deletedExtents)
	return
}

// Create a new extent if one of the replica is missing.
func (dp *DataPartition) generateExtentCreationTasks(repairTasks []*DataPartitionRepairTask, extentInfoMap map[uint64]storage.ExtentInfoBlock, sources map[uint64]string) {
	for extentID, extentInfo := range extentInfoMap {
		if proto.IsTinyExtent(extentID) {
			continue
		}
		for index := 0; index < len(repairTasks); index++ {
			repairTask := repairTasks[index]
			if repairTask == nil {
				continue
			}
			if _, ok := repairTask.extents[extentID]; !ok {
				if proto.IsTinyExtent(extentID) {
					continue
				}
				var (
					size = extentInfo[storage.Size]
					ino  = extentInfo[storage.Inode]
					ei   = storage.ExtentInfoBlock{
						storage.FileID: extentID,
						storage.Size:   size,
						storage.Inode:  ino,
					}
				)
				repairTask.ExtentsToBeRepairedSource[extentID] = sources[extentID]
				repairTask.ExtentsToBeCreated = append(repairTask.ExtentsToBeCreated, ei)
				repairTask.ExtentsToBeRepaired = append(repairTask.ExtentsToBeRepaired, ei)

				if log.IsInfoEnabled() {
					log.LogInfof("DP %v: REPAIR: gen extent creation: host %v, extent %v, size %v, ino %v", dp.partitionID, repairTask.addr, extentID, size, ino)
				}
			}
		}
	}
}

// proposeRepair an extent if the replicas do not have the same length.
func (dp *DataPartition) generateExtentRepairTasks(repairTasks []*DataPartitionRepairTask, maxSizeExtentMap map[uint64]storage.ExtentInfoBlock, sources map[uint64]string) (availableTinyExtents []uint64, brokenTinyExtents []uint64) {
	availableTinyExtents = make([]uint64, 0)
	brokenTinyExtents = make([]uint64, 0)
	for extentID, maxFileInfo := range maxSizeExtentMap {
		certain := true // 状态明确
		hasBeenRepaired := true
		for index := 0; index < len(repairTasks); index++ {
			if repairTasks[index] == nil {
				certain = false // 状态设置为不明确
				continue
			}
			extentInfo, ok := repairTasks[index].extents[extentID]
			if !ok {
				continue
			}
			if extentInfo[storage.Size] < maxFileInfo[storage.Size] {
				fixExtent := storage.ExtentInfoBlock{
					storage.FileID: extentID,
					storage.Size:   maxFileInfo[storage.Size],
				}
				repairTasks[index].ExtentsToBeRepaired = append(repairTasks[index].ExtentsToBeRepaired, fixExtent)
				repairTasks[index].ExtentsToBeRepairedSource[extentID] = sources[extentID]
				if log.IsInfoEnabled() {
					log.LogInfof("DP %v: REPAIR: gen extent repair: host(%v), extent(%v), size=(%v to %v)",
						dp.partitionID, repairTasks[index].addr, extentID, extentInfo[storage.Size], fixExtent[storage.Size])
				}
				hasBeenRepaired = false
			}

		}
		if proto.IsTinyExtent(extentID) {
			if certain && hasBeenRepaired {
				// 仅状态明确且被修复完成的tiny extent可以设为available
				availableTinyExtents = append(availableTinyExtents, extentID)
			} else {
				brokenTinyExtents = append(brokenTinyExtents, extentID)
			}
		}
	}
	return
}

func (dp *DataPartition) generateExtentDeletionTasks(repairTasks []*DataPartitionRepairTask, deletedExtents map[uint64]storage.ExtentInfoBlock) {
	for extentID, _ := range deletedExtents {
		if proto.IsTinyExtent(extentID) {
			continue
		}
		for index := 0; index < len(repairTasks); index++ {
			repairTask := repairTasks[index]
			if repairTask == nil {
				continue
			}
			if _, ok := repairTask.extents[extentID]; ok {
				ei := storage.ExtentInfoBlock{
					storage.FileID: extentID,
				}
				repairTask.ExtentsToBeDeleted = append(repairTask.ExtentsToBeDeleted, ei)
				if log.IsInfoEnabled() {
					log.LogInfof("DP %v: REPAIR: gen extent deletion: host(%v), extent(%v)", dp.partitionID, repairTask.addr, extentID)
				}
			}
		}
	}
}

func (dp *DataPartition) generateBaseExtentIDTasks(repairTasks []*DataPartitionRepairTask, maxBaseExtentID uint64) {
	for index := 0; index < len(repairTasks); index++ {
		if repairTasks[index] == nil {
			continue
		}
		repairTasks[index].BaseExtentID = maxBaseExtentID
	}
}

func (dp *DataPartition) notifyFollower(ctx context.Context, task *DataPartitionRepairTask) (err error) {
	p := repl.NewPacketToNotifyExtentRepair(ctx, dp.partitionID) // notify all the followers to repair
	var conn *net.TCPConn
	target := task.addr
	p.Data, _ = json.Marshal(task)
	p.Size = uint32(len(p.Data))
	conn, err = gConnPool.GetConnect(target)
	defer func() {
		log.LogInfof(fmt.Sprintf(ActionNotifyFollowerToRepair+" to host(%v) Partition(%v) failed(%v)", target, dp.partitionID, err))
	}()
	if err != nil {
		return err
	}
	defer gConnPool.PutConnect(conn, true)
	if err = p.WriteToConn(conn, proto.WriteDeadlineTime); err != nil {
		return err
	}
	if err = p.ReadFromConn(conn, proto.MaxWaitFollowerRepairTime); err != nil {
		return err
	}
	return err
}

// notifyFollowersToRepair notifies the followers to repair.
func (dp *DataPartition) notifyFollowersToRepair(ctx context.Context, members []*DataPartitionRepairTask) (err error) {
	var futures []*async.Future
	for i := 1; i < len(members); i++ {
		if members[i] == nil {
			continue
		}
		var future = async.NewFuture()
		go func(future *async.Future, task *DataPartitionRepairTask) {
			var err = dp.notifyFollower(ctx, task)
			future.Respond(nil, err)
		}(future, members[i])
		futures = append(futures, future)
	}
	for _, future := range futures {
		if _, e := future.Response(); e != nil {
			if err != nil {
				err = fmt.Errorf("%v: %v", e, err)
				continue
			}
			err = e
		}
	}
	return
}

// DoStreamExtentFixRepair executes the repair on the followers.
func (dp *DataPartition) doStreamExtentFixRepairOnFollowerDisk(ctx context.Context, remoteExtentInfo storage.ExtentInfoBlock, sources []string) {

	var err error
	for _, source := range sources {
		err = dp.streamRepairExtent(ctx, remoteExtentInfo, source, NoForceRepair)
		if err != nil {
			err = errors.Trace(err, "doStreamExtentFixRepairOnFollowerDisk %v", dp.applyRepairKey(int(remoteExtentInfo[storage.FileID])))
			localExtentInfo, opErr := dp.ExtentStore().Watermark(uint64(remoteExtentInfo[storage.FileID]))
			if opErr != nil {
				err = errors.Trace(err, opErr.Error())
			}
			err = errors.Trace(err, "partition(%v) remote(%v) local(%v)",
				dp.partitionID, remoteExtentInfo, localExtentInfo)
			log.LogWarnf("action[doStreamExtentFixRepairOnFollowerDisk] err(%v).", err)
			continue
		}
		break
	}
}

func (dp *DataPartition) applyRepairKey(extentID int) (m string) {
	return fmt.Sprintf("ApplyRepairKey(%v_%v)", dp.partitionID, extentID)
}

func (dp *DataPartition) tryLockExtentRepair(extentID uint64) (release func(), success bool) {
	dp.inRepairExtentMu.Lock()
	defer dp.inRepairExtentMu.Unlock()
	if _, has := dp.inRepairExtents[extentID]; has {
		success = false
		release = nil
		return
	}
	dp.inRepairExtents[extentID] = struct{}{}
	success = true
	release = func() {
		dp.inRepairExtentMu.Lock()
		defer dp.inRepairExtentMu.Unlock()
		delete(dp.inRepairExtents, extentID)
	}
	return
}

// The actual repair of an extent happens here.

func (dp *DataPartition) streamRepairExtent(ctx context.Context, remoteExtentInfo storage.ExtentInfoBlock, source string, forceRepair bool) (err error) {

	var (
		extentID   = remoteExtentInfo[storage.FileID]
		remoteSize = remoteExtentInfo[storage.Size]
	)

	// Checking store and extent state at first.
	var store = dp.ExtentStore()
	if !store.IsExists(extentID) || store.IsDeleted(extentID) {
		return
	}
	if !forceRepair {
		err = multirate.WaitConcurrency(context.Background(), proto.OpExtentRepairWrite_, dp.disk.Path)
		if err != nil {
			err = errors.Trace(err, proto.ConcurrentLimit)
			return
		}
		defer multirate.DoneConcurrency(proto.OpExtentRepairWrite_, dp.disk.Path)
	}
	var release2, success = dp.tryLockExtentRepair(extentID)
	if !success {
		return
	}
	defer release2()

	var localExtentInfo *storage.ExtentInfoBlock
	if forceRepair {
		if localExtentInfo, err = store.ForceWatermark(extentID); err != nil {
			return errors.Trace(err, "streamRepairExtent Watermark error")
		}
	} else {
		if !AutoRepairStatus && !proto.IsTinyExtent(extentID) {
			log.LogWarnf("AutoRepairStatus is False,so cannot AutoRepair extent(%v)", remoteExtentInfo.String())
			return
		}
		if localExtentInfo, err = store.Watermark(extentID); err != nil {
			return errors.Trace(err, "streamRepairExtent Watermark error")
		}
	}

	log.LogInfof("streamRepairExtent: partition[%v] start to fix extent [id: %v, modifytime: %v, localsize: %v, remotesize: %v] from [%v]",
		dp.partitionID, extentID, localExtentInfo[storage.ModifyTime], localExtentInfo[storage.Size], remoteSize, source)

	//if the data size of extentinfo struct is not equal with the data size of extent struct,use the data size of extent struct
	e, err := store.ExtentWithHeader(localExtentInfo)
	if err != nil {
		return errors.Trace(err, "streamRepairExtent extentWithHeader error")
	}

	if localExtentInfo[storage.Size] != uint64(e.Size()) {
		localExtentInfo[storage.Size] = uint64(e.Size())
	}
	if localExtentInfo[storage.Size] >= remoteSize {
		return nil
	}

	// size difference between the local extent and the remote extent
	sizeDiff := remoteExtentInfo[storage.Size] - localExtentInfo[storage.Size]
	request := repl.NewExtentRepairReadPacket(ctx, dp.partitionID, remoteExtentInfo[storage.FileID], int(localExtentInfo[storage.Size]), int(sizeDiff), false)
	if proto.IsTinyExtent(remoteExtentInfo[storage.FileID]) {
		if sizeDiff >= math.MaxUint32 {
			sizeDiff = math.MaxUint32 - unit.MB
		}
		request = repl.NewTinyExtentRepairReadPacket(ctx, dp.partitionID, remoteExtentInfo[storage.FileID], int(localExtentInfo[storage.Size]), int(sizeDiff), false)
	}
	var conn *net.TCPConn
	conn, err = gConnPool.GetConnect(source)
	if err != nil {
		return errors.Trace(err, "streamRepairExtent get conn from host(%v) error", source)
	}
	defer gConnPool.PutConnect(conn, true)

	if err = request.WriteToConn(conn, proto.WriteDeadlineTime); err != nil {
		err = errors.Trace(err, "streamRepairExtent send streamRead to host(%v) error", source)
		log.LogWarnf("action[streamRepairExtent] err(%v).", err)
		return
	}
	currFixOffset := localExtentInfo[storage.Size]

	var (
		hasRecoverySize uint64
		replyDataBuffer []byte
	)
	replyDataBuffer, _ = proto.Buffers.Get(unit.ReadBlockSize)
	defer func() {
		proto.Buffers.Put(replyDataBuffer[:unit.ReadBlockSize])
	}()
	var getReplyDataBuffer = func(size uint32) []byte {
		if int(size) > cap(replyDataBuffer) {
			return make([]byte, size)
		}
		return replyDataBuffer[:size]
	}

	for currFixOffset < remoteSize {
		reply := repl.NewPacket(ctx)
		if err = reply.ReadFromConnWithSpecifiedDataBuffer(conn, 60, getReplyDataBuffer); err != nil {
			err = errors.Trace(err, "streamRepairExtent receive data error,localExtentSize(%v) remoteExtentSize(%v)", currFixOffset, remoteSize)
			return
		}

		if reply.ResultCode != proto.OpOk {
			err = errors.Trace(fmt.Errorf("result code not ok"),
				"streamRepairExtent receive opcode error(%v) ,localExtentSize(%v) remoteExtentSize(%v)", string(reply.Data[:reply.Size]), currFixOffset, remoteSize)
			return
		}

		if reply.ReqID != request.ReqID || reply.PartitionID != request.PartitionID ||
			reply.ExtentID != request.ExtentID {
			err = errors.Trace(fmt.Errorf("unavali reply"), "streamRepairExtent receive unavalid "+
				"request(%v) reply(%v) ,localExtentSize(%v) remoteExtentSize(%v)", request.GetUniqueLogId(), reply.GetUniqueLogId(), currFixOffset, remoteSize)
			return
		}

		if !proto.IsTinyExtent(reply.ExtentID) && (reply.Size == 0 || reply.ExtentOffset != int64(currFixOffset)) {
			err = errors.Trace(fmt.Errorf("unavali reply"), "streamRepairExtent receive unavalid "+
				"request(%v) reply(%v) localExtentSize(%v) remoteExtentSize(%v)", request.GetUniqueLogId(), reply.GetUniqueLogId(), currFixOffset, remoteSize)
			return
		}
		if proto.IsTinyExtent(reply.ExtentID) && reply.ExtentOffset != int64(currFixOffset) {
			err = errors.Trace(fmt.Errorf("unavali reply"), "streamRepairExtent receive unavalid "+
				"request(%v) reply(%v) localExtentSize(%v) remoteExtentSize(%v)", request.GetUniqueLogId(), reply.GetUniqueLogId(), currFixOffset, remoteSize)
			return
		}

		log.LogInfof(fmt.Sprintf("action[streamRepairExtent] fix(%v_%v) start fix from(%v)"+
			" remoteSize(%v)localSize(%v) reply(%v).", dp.partitionID, extentID, remoteExtentInfo.String(),
			remoteSize, currFixOffset, reply.GetUniqueLogId()))
		if actualCrc := crc32.ChecksumIEEE(reply.Data[:reply.Size]); actualCrc != reply.CRC {
			err = fmt.Errorf("streamRepairExtent crc mismatch expectCrc(%v) actualCrc(%v) extent(%v_%v) start fix from(%v)"+
				" remoteSize(%v) localSize(%v) request(%v) reply(%v) ", reply.CRC, actualCrc, dp.partitionID, remoteExtentInfo.String(),
				source, remoteSize, currFixOffset, request.GetUniqueLogId(), reply.GetUniqueLogId())
			return errors.Trace(err, "streamRepairExtent receive data error")
		}

		// Write it to local extent file
		currRecoverySize := uint64(reply.Size)
		if proto.IsTinyExtent(extentID) {
			originalDataSize := uint64(reply.Size)
			var remoteAvaliSize uint64
			if reply.ArgLen == TinyExtentRepairReadResponseArgLen {
				remoteAvaliSize = binary.BigEndian.Uint64(reply.Arg[9:TinyExtentRepairReadResponseArgLen])
			}
			var tpObject *statistics.TpObject = nil
			var emptyResponse bool
			if emptyResponse = reply.Arg != nil && reply.Arg[0] == EmptyResponse; emptyResponse {
				if reply.KernelOffset > 0 && reply.KernelOffset != uint64(crc32.ChecksumIEEE(reply.Arg)) {
					err = fmt.Errorf("streamRepairExtent arg crc mismatch expectCrc(%v) actualCrc(%v) extent(%v_%v) fix from(%v) remoteAvaliSize(%v) "+
						"hasRecoverySize(%v) currRecoverySize(%v) request(%v) reply(%v)", reply.KernelOffset, crc32.ChecksumIEEE(reply.Arg), dp.partitionID, remoteExtentInfo.String(),
						source, remoteAvaliSize, hasRecoverySize+currRecoverySize, currRecoverySize, request.GetUniqueLogId(), reply.GetUniqueLogId())
					return errors.Trace(err, "streamRepairExtent receive data error")
				}
				currRecoverySize = binary.BigEndian.Uint64(reply.Arg[1:9])
			} else {
				tpObject = dp.monitorData[proto.ActionRepairWrite].BeforeTp()
			}

			if currFixOffset+currRecoverySize > remoteSize {
				msg := fmt.Sprintf("action[streamRepairExtent] fix(%v_%v), streamRepairTinyExtent,remoteAvaliSize(%v) currFixOffset(%v) currRecoverySize(%v), remoteExtentSize(%v), isEmptyResponse(%v), needRecoverySize is too big",
					dp.partitionID, localExtentInfo[storage.FileID], remoteAvaliSize, currFixOffset, currRecoverySize, remoteExtentInfo[storage.Size], emptyResponse)
				exporter.WarningCritical(msg)
				return errors.Trace(err, "streamRepairExtent repair data error ")
			}
			if !emptyResponse {
				err = dp.limit(context.Background(), proto.OpExtentRepairWrite_, uint32(currRecoverySize), multirate.FlowDisk)
				if err != nil {
					return
				}
			}
			err = store.TinyExtentRecover(extentID, int64(currFixOffset), int64(currRecoverySize), reply.Data[:originalDataSize], reply.CRC, emptyResponse)
			if tpObject != nil {
				tpObject.AfterTp(currRecoverySize)
			}

			if hasRecoverySize+currRecoverySize >= remoteAvaliSize {
				log.LogInfof("streamRepairTinyExtent(%v) recover fininsh,remoteAvaliSize(%v) "+
					"hasRecoverySize(%v) currRecoverySize(%v)", dp.applyRepairKey(int(localExtentInfo[storage.FileID])),
					remoteAvaliSize, hasRecoverySize+currRecoverySize, currRecoverySize)
				break
			}
		} else {
			err = dp.limit(context.Background(), proto.OpExtentRepairWrite_, uint32(currRecoverySize), multirate.FlowDisk)
			if err != nil {
				return
			}
			var tpObject = dp.monitorData[proto.ActionRepairWrite].BeforeTp()
			err = store.Write(ctx, extentID, int64(currFixOffset), int64(reply.Size), reply.Data[0:reply.Size], reply.CRC, storage.AppendWriteType, BufferWrite)
			tpObject.AfterTp(uint64(reply.Size))
		}

		// write to the local extent file
		if err != nil {
			err = errors.Trace(err, "streamRepairExtent repair data error ")
			return
		}
		hasRecoverySize += currRecoverySize
		currFixOffset += currRecoverySize
		if currFixOffset >= remoteSize {
			break
		}
	}
	return
}

/* The functions below implement the interfaces defined in the raft library. */
// raftfsm Apply puts the data onto the disk.
func (dp *DataPartition) handleRaftApply(command []byte, index uint64) (resp interface{}, err error) {
	defer func() {
		if err != nil {
			msg := fmt.Sprintf("partition [id: %v, disk: %v] apply command [index: %v] occurred error and will be stop: %v",
				dp.partitionID, dp.Disk().Path, index, err)
			log.LogErrorf(msg)
			exporter.WarningCritical(msg)
			dp.Disk().space.DetachDataPartition(dp.partitionID)
			dp.Disk().DetachDataPartition(dp)
			dp.Stop()
			log.LogCriticalf("partition [id: %v, disk: %v] apply command [index: %v] failed and stopped: %v",
				dp.partitionID, dp.Disk().Path, index, err)
			return
		}
		dp.advanceApplyID(index)
		dp.actionHolder.Unregister(index)
	}()
	var opItem *rndWrtOpItem
	if opItem, err = UnmarshalRandWriteRaftLog(command, true); err != nil {
		resp = proto.OpErr
		return
	}
	resp, err = dp.ApplyRandomWrite(opItem, index)
	PutRandomWriteOpItem(opItem)
	return
}

// ApplyMemberChange supports adding new raft member or deleting an existing raft member.
// It does not support updating an existing member at this point.
func (dp *DataPartition) handleRaftApplyMemberChange(confChange *raftProto.ConfChange, index uint64) (resp interface{}, err error) {
	defer func(index uint64) {
		if err != nil {
			msg := fmt.Sprintf("partition [id: %v, disk: %v] apply member change [index: %v] occurred error and will be stop: %v",
				dp.partitionID, dp.Disk().Path, index, err)
			log.LogErrorf(msg)
			exporter.WarningCritical(msg)
			dp.Disk().space.DetachDataPartition(dp.partitionID)
			dp.Disk().DetachDataPartition(dp)
			dp.Stop()
			log.LogCriticalf("partition [id: %v, disk: %v] apply member change [index: %v] failed and stopped: %v",
				dp.partitionID, dp.Disk().Path, index, err)
			return
		}
		dp.advanceApplyID(index)
		log.LogWarnf("partition [%v] apply member change [index: %v] %v %v", dp.partitionID, index, confChange.Type, confChange.Peer)
	}(index)

	// Change memory the status
	var (
		isUpdated bool
	)

	switch confChange.Type {
	case raftProto.ConfAddNode:
		req := &proto.AddDataPartitionRaftMemberRequest{}
		if err = json.Unmarshal(confChange.Context, req); err != nil {
			return
		}
		isUpdated, err = dp.addRaftNode(req, index)
	case raftProto.ConfRemoveNode:
		req := &proto.RemoveDataPartitionRaftMemberRequest{}
		if err = json.Unmarshal(confChange.Context, req); err != nil {
			return
		}
		isUpdated, err = dp.removeRaftNode(req, index)
	case raftProto.ConfAddLearner:
		req := &proto.AddDataPartitionRaftLearnerRequest{}
		if err = json.Unmarshal(confChange.Context, req); err != nil {
			return
		}
		isUpdated, err = dp.addRaftLearner(req, index)
	case raftProto.ConfPromoteLearner:
		req := &proto.PromoteDataPartitionRaftLearnerRequest{}
		if err = json.Unmarshal(confChange.Context, req); err != nil {
			return
		}
		isUpdated, err = dp.promoteRaftLearner(req, index)
	case raftProto.ConfUpdateNode:
		log.LogDebugf("[updateRaftNode]: not support.")
	}
	if err != nil {
		log.LogErrorf("action[ApplyMemberChange] dp(%v) type(%v) err(%v).", dp.partitionID, confChange.Type, err)
		return
	}
	if isUpdated {
		dp.DataPartitionCreateType = proto.NormalCreateDataPartition
		if err = dp.persist(nil, false); err != nil {
			log.LogErrorf("action[ApplyMemberChange] dp(%v) PersistMetadata err(%v).", dp.partitionID, err)
			return
		}
	}
	dp.proposeUpdateVolumeInfo()
	return
}

// Snapshot persists the in-memory data (as a snapshot) to the disk.
// Note that the data in each data partition has already been saved on the disk. Therefore there is no need to take the
// snapshot in this case.
func (dp *DataPartition) handleRaftSnapshot(recoverNode uint64) (raftProto.Snapshot, error) {
	var as = dp.applyStatus.Snap()
	var snapshotIndex = as.Truncated() + 1
	snapIterator := NewItemIterator(snapshotIndex)
	if log.IsInfoEnabled() {
		log.LogInfof("partition[%v] [applied: %v, truncated: %v] generate raft snapshot [index: %v] for peer[%v]",
			dp.partitionID, as.Applied(), as.Truncated(), snapshotIndex, recoverNode)
	}
	return snapIterator, nil
}

// ApplySnapshot asks the raft leader for the snapshot data to recover the contents on the local disk.
func (dp *DataPartition) handleRaftApplySnapshot(peers []raftProto.Peer, iterator raftProto.SnapIterator, snapV uint32) (err error) {
	// Never delete the raft log which hadn't applied, so snapshot no need.
	log.LogInfof("PartitionID(%v) ApplySnapshot from(%v)", dp.partitionID, dp.raftPartition.CommittedIndex())
	if dp.isCatchUp {
		msg := fmt.Sprintf("partition [id: %v, disk: %v] triggers an illegal raft snapshot recover and will be stop for data safe",
			dp.partitionID, dp.Disk().Path)
		log.LogErrorf(msg)
		dp.Disk().space.DetachDataPartition(dp.partitionID)
		dp.Disk().DetachDataPartition(dp)
		dp.Stop()
		exporter.WarningCritical(msg)
		log.LogCritical(msg)
	}
	defer func() {
		dp.isCatchUp = true
	}()
	for {
		if _, err = iterator.Next(); err != nil {
			if err != io.EOF {
				log.LogError(fmt.Sprintf("action[ApplySnapshot] PartitionID(%v) ApplySnapshot from(%v) failed,err:%v", dp.partitionID, dp.raftPartition.CommittedIndex(), err.Error()))
				return
			}
			return nil
		}
	}
}

// HandleFatalEvent notifies the application when panic happens.
func (dp *DataPartition) handleRaftFatalEvent(err *raft.FatalError) {
	dp.checkIsPartitionError(err.Err)
	log.LogErrorf("action[HandleFatalEvent] err(%v).", err)
}

// HandleLeaderChange notifies the application when the raft leader has changed.
func (dp *DataPartition) handleRaftLeaderChange(leader uint64) {
	defer func() {
		if r := recover(); r != nil {
			mesg := fmt.Sprintf("HandleLeaderChange(%v)  Raft Panic(%v)", dp.partitionID, r)
			panic(mesg)
		}
	}()
	if dp.config.NodeID == leader {
		if !gHasLoadDataPartition {
			go dp.raftPartition.TryToLeader(dp.partitionID)
		}
		dp.isRaftLeader = true
	}
	//If leader changed, that indicates the raft has elected a new leader,
	//the fault occurred checking to prevent raft brain split is no more needed.
	if dp.isNeedFaultCheck() {
		dp.setNeedFaultCheck(false)
		_ = dp.persistMetaDataOnly()
	}
}

func (dp *DataPartition) handleRaftAskRollback(original []byte, index uint64) (rollback []byte, err error) {
	if index <= dp.applyStatus.Applied() || len(original) == 0 {
		return
	}
	var opItem *rndWrtOpItem
	if opItem, err = UnmarshalRandWriteRaftLog(original, false); err != nil {
		return
	}
	defer func() {
		PutRandomWriteOpItem(opItem)
	}()
	err = dp.limit(context.Background(), proto.OpExtentRepairReadToRollback_, uint32(opItem.size), multirate.FlowDisk)
	if err != nil {
		return
	}
	var buf = make([]byte, opItem.size)
	var crc uint32
	if crc, err = dp.extentStore.Read(opItem.extentID, opItem.offset, opItem.size, buf[:opItem.size], false); err != nil {
		return
	}
	rollback, err = MarshalRandWriteRaftLog(opItem.opcode, opItem.extentID, opItem.offset, opItem.size, buf[:opItem.size], crc)
	log.LogWarnf("partition[%v] [disk: %v] handle ask rollback [index: %v, extent: %v, offset: %v, size: %v], CRC[%v -> %v]",
		dp.partitionID, dp.disk.Path, index, opItem.extentID, opItem.offset, opItem.size, opItem.crc, crc)
	return
}

// Put submits the raft log to the raft store.
func (dp *DataPartition) submitToRaft(cmd []byte) (err error) {
	if dp.raftPartition == nil {
		err = fmt.Errorf("%s", RaftNotStarted)
		return
	}
	const op = "SubmitToRaft"
	var tp = exporter.NewModuleTPUs(op)
	_, err = dp.raftPartition.Submit(cmd, raftProto.AckTypeCommitted)
	tp.Set(err)
	return
}

func (dp *DataPartition) advanceApplyID(applyID uint64) {
	if snap, success := dp.applyStatus.AdvanceApplied(applyID); !success {
		log.LogWarnf("Partition(%v) advance apply ID failed, curApplied[%v]", dp.partitionID, snap.Applied())
	}
	if !dp.isCatchUp {
		dp.isCatchUp = true
	}
}
