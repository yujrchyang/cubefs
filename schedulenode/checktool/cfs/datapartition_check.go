package cfs

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/schedulenode/checktool/cfs/multi_email"
	"github.com/cubefs/cubefs/sdk/http_client"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util/checktool"
	"github.com/cubefs/cubefs/util/log"
	"github.com/robfig/cron"
	"github.com/tiglabs/raft"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type DNDataPartitionInfo struct {
	VolName              string       `json:"volName"`
	ID                   uint64       `json:"id"`
	Size                 int          `json:"size"`
	Used                 int          `json:"used"`
	Status               int          `json:"status"`
	Path                 string       `json:"path"`
	FileCount            int          `json:"fileCount"`
	Replicas             []string     `json:"replicas"`
	TinyDeleteRecordSize int64        `json:"tinyDeleteRecordSize"`
	Peers                []Peer       `json:"peers"`
	Learners             []Learner    `json:"learners"`
	RaftStatus           *raft.Status `json:"raftStatus"`
}

// DataReplica represents the replica of a data partition
type DataReplica struct {
	Addr            string
	ReportTime      int64
	FileCount       uint32
	Status          int8
	HasLoadResponse bool   // if there is any response when loading
	Total           uint64 `json:"TotalSize"`
	Used            uint64 `json:"UsedSize"`
	IsLeader        bool
	NeedsToCompare  bool
	DiskPath        string
}

type DataPartitionInfo struct {
	PartitionID             uint64
	LastLoadedTime          int64
	ReplicaNum              uint8
	Status                  int8
	IsRecover               bool
	Replicas                []*DataReplica
	Hosts                   []string // host addresses
	Peers                   []Peer
	Learners                []Learner
	Zones                   []string
	MissingNodes            map[string]int64 // key: address of the missing node, value: when the node is missing
	VolName                 string
	VolID                   uint64
	OfflinePeerID           uint64
	FilesWithMissingReplica map[string]int64 // key: file name, value: last time when a missing replica is found
}

// DataPartitionResponse defines the response from a data node to the master that is related to a data partition.
type DataPartitionResponse struct {
	PartitionID uint64
	Status      int8
	ReplicaNum  uint8
	Hosts       []string
	LeaderAddr  string
	Epoch       uint64
	IsRecover   bool // dbbak not support
}

// VolView defines the view of a volume
type VolView struct {
	Name           string
	Owner          string
	Status         uint8
	FollowerRead   bool
	MetaPartitions []*MetaPartitionView
	DataPartitions []*DataPartitionResponse
	CreateTime     int64
}

func (s *ChubaoFSMonitor) scheduleToCheckDpPeerCorrupt() {
	crontab := cron.New()
	crontab.AddFunc("30 10,18 * * *", s.checkDpPeerCorrupt)
	crontab.Start()
	go func() {
		select {
		case <-s.ctx.Done():
			crontab.Stop()
		}
	}()
}

var badPeerPartitionMap sync.Map

func (s *ChubaoFSMonitor) checkDpPeerCorrupt() {
	dpCheckStartTime := time.Now()
	log.LogInfof("check all dataPartitions corrupt start")
	for _, host := range s.hosts {
		if host.isReleaseCluster {
			//release_db raft is not implemented
			continue
		}
		checkDpCorruptWg.Add(1)
		go checkHostDataPartition(host, s.checkPeerConcurrency)
	}
	checkDpCorruptWg.Wait()
	log.LogInfof("check dataPartition corrupt end, cost [%v]", time.Since(dpCheckStartTime))
	// print mp/dp raft pending applied check result
	for _, host := range s.hosts {
		printRaftPendingAndAppliedResult(host, partitionTypeDP)
	}
}

func (s *ChubaoFSMonitor) checkAvailableTinyExtents() {
	dpCheckStartTime := time.Now()
	log.LogInfof("checkAvailableTinyExtents start")
	for _, host := range s.hosts {
		if host.isReleaseCluster {
			continue
		}
		checkTinyExtentsByVol(host, s.checkAvailTinyVols)
	}
	log.LogInfof("checkAvailableTinyExtents end, cost [%v]", time.Since(dpCheckStartTime))
}

func checkTinyExtentsByVol(host *ClusterHost, vols []string) {
	mc := master.NewMasterClient([]string{host.host}, false)
	var err error
	for _, vol := range vols {
		var volInfo *SimpleVolView
		volInfo, err = getVolSimpleView(vol, host)
		if err != nil || volInfo == nil {
			log.LogErrorf("get volume simple view failed: %v err:%v", vol, err)
			continue
		}
		authKey := checktool.Md5(volInfo.Owner)
		var volView *VolView
		if volView, err = getDataPartitionsFromVolume(host, vol, authKey); err != nil {
			log.LogErrorf("Found an invalid vol: %v err:%v", vol, err)
			continue
		}
		elements := make([][]string, 0)
		elements = append(elements, []string{
			"partition",
			"warnType",
			"ReplicaNum",
			"hosts",
			"leader",
			"status",
			"recover",
			"availableCh",
			"brokenCh",
			"totalExtents",
			"availableMap",
			"brokenMap",
		})
		var lock sync.Mutex
		wg := sync.WaitGroup{}
		wg.Add(32)
		dpCh := make(chan *DataPartitionResponse, 1)
		var brokenRw = new(atomic.Int32)
		var recoverNum = new(atomic.Int32)
		var checkFailedNum = new(atomic.Int32)
		for i := 0; i < 32; i++ {
			go func() {
				defer wg.Done()
				for {
					select {
					case dp := <-dpCh:
						if dp == nil {
							return
						}
						result := checkDataPartitionAvailTiny(vol, dp, brokenRw, recoverNum, checkFailedNum, mc)
						if len(result) == 0 {
							continue
						}
						lock.Lock()
						elements = append(elements, result)
						lock.Unlock()
					}
				}
			}()
		}

		for _, dp := range volView.DataPartitions {
			dpCh <- dp
		}
		close(dpCh)
		wg.Wait()

		if len(elements) == 1 {
			continue
		}

		partitionStr := fmt.Sprintf("total[%v]rw[%v]ro[%v]broken[%v]broken_rw[%v]check_fail[%v]recover[%v]", len(volView.DataPartitions), volInfo.RwDpCnt,
			len(volView.DataPartitions)-volInfo.RwDpCnt, len(elements)-1, brokenRw.Load(), checkFailedNum.Load(), recoverNum.Load())
		summary := `Domain    : ` + host.host + `<br>` +
			`Volume    : ` + vol + `<br>` +
			`Partition : ` + partitionStr + `<br>`
		checktool.WarnBySpecialUmpKey(UMPCFSNodeTinyExtentCheckKey, fmt.Sprintf("Domain: %v Volume: %v BadPartitions: %v", host.host, vol, partitionStr))
		err = multi_email.Email(fmt.Sprintf("AvailableTinyExtentCheck [Domain: %v, Volume: %v]", host.host, vol), summary, elements)
		if err != nil {
			log.LogErrorf("send email failed, err:%v", err)
		}
	}
}

func checkDataPartitionAvailTiny(vol string, dp *DataPartitionResponse, brokenRw, recoverNum, checkFailedNum *atomic.Int32, mc *master.MasterClient) []string {
	var (
		err            error
		recoverReplica bool
		primaryLeader  string
		tinyExtents    []*http_client.TinyExtentsInfo
	)
	if len(dp.Hosts) < 1 {
		return []string{}
	}
	primaryLeader = dp.Hosts[0]
	result := []string{
		fmt.Sprintf("%v", dp.PartitionID),
		"",
		fmt.Sprintf("%v", dp.ReplicaNum),
		strings.Join(dp.Hosts, ","),
		dp.LeaderAddr,
		fmt.Sprintf("%v", dp.Status),
		"N/A",
		"N/A",
		"N/A",
		"N/A",
		"N/A",
		"N/A",
	}
	if dp.IsRecover {
		recoverNum.Add(1)
		result[1] = "InRecover"
		return result
	}
	dHost := fmt.Sprintf("%v:%v", strings.Split(primaryLeader, ":")[0], profPortMap[strings.Split(primaryLeader, ":")[1]])
	dataClient := http_client.NewDataClient(dHost, false)
	tinyExtents, err = dataClient.GetTinyExtents(dp.PartitionID)
	if err != nil {
		checkFailedNum.Add(1)
		log.LogErrorf("dp[%v] get tiny extents err:%v", dp.PartitionID, err)
		result[1] = "GetTinyFailed"
		return result
	}
	if len(tinyExtents) != 1 {
		checkFailedNum.Add(1)
		log.LogErrorf("dp[%v] get invalid tiny extents, len(%v)", dp.PartitionID, len(tinyExtents))
		result[1] = "GetTinyFailed"
		return result
	}
	if !(tinyExtents[0].TotalTinyExtent < 64 || tinyExtents[0].AvailableCh < 10) {
		return []string{}
	}
	var partition *proto.DataPartitionInfo
	partition, err = mc.AdminAPI().GetDataPartition(vol, dp.PartitionID)
	if err != nil {
		checkFailedNum.Add(1)
		log.LogErrorf("dp[%v] get data partition from master failed, err:%v", dp.PartitionID, err)
		result[1] = "GetPartitionFailed"
		return result
	}
	for _, replica := range partition.Replicas {
		if replica.IsRecover {
			recoverReplica = true
			break
		}
	}
	if dp.Status == ReadWrite {
		brokenRw.Add(1)
	}
	return []string{
		fmt.Sprintf("%v", dp.PartitionID),
		"BrokenTinyExtent",
		fmt.Sprintf("%v", dp.ReplicaNum),
		strings.Join(dp.Hosts, ","),
		dp.LeaderAddr,
		fmt.Sprintf("%v", dp.Status),
		fmt.Sprintf("%v", recoverReplica),
		fmt.Sprintf("%v", tinyExtents[0].AvailableCh),
		fmt.Sprintf("%v", tinyExtents[0].BrokenCh),
		fmt.Sprintf("%v", tinyExtents[0].TotalTinyExtent),
		fmt.Sprintf("%v", tinyExtents[0].AvailableTinyExtents),
		fmt.Sprintf("%v", tinyExtents[0].BrokenTinyExtents),
	}
}

func checkDataPartitionPeers(ch *ClusterHost, volName string, PartitionID uint64) {
	var (
		reqURL      string
		err         error
		badReplicas []PeerReplica
	)
	defer func() {
		if err != nil {
			log.LogErrorf("action[checkDataPartitionPeers] host[%v], vol[%v], id[%v], err[%v]", ch.host, volName,
				PartitionID, err)
		}
	}()
	badReplicas = make([]PeerReplica, 0)
	reqURL = fmt.Sprintf("http://%v/dataPartition/get?id=%v&name=%v", ch.host, PartitionID, volName)
	data, err := doRequest(reqURL, ch.isReleaseCluster)
	if err != nil {
		return
	}
	dp := &DataPartitionInfo{}
	if err = json.Unmarshal(data, dp); err != nil {
		log.LogErrorf("unmarshal to dp data[%v],err[%v]", string(data), err)
		return
	}
	replicaRaftStatusMap := make(map[string]*proto.Status, len(dp.Replicas)) // 不同副本的raft状态
	for _, r := range dp.Replicas {
		var dnPartition *proto.DNDataPartitionInfo
		addr := fmt.Sprintf("%v:%v", strings.Split(r.Addr, ":")[0], profPortMap[strings.Split(r.Addr, ":")[1]])
		dataClient := http_client.NewDataClient(addr, false)
		//check dataPartition by dataNode api
		for i := 0; i < 3; i++ {
			if dnPartition, err = dataClient.GetPartitionSimple(dp.PartitionID); err == nil {
				break
			}
			time.Sleep(1 * time.Second)
		}
		if !dp.IsRecover && err != nil {
			time.Sleep(10 * time.Second)
			if dnPartition, err = dataClient.GetPartitionSimple(dp.PartitionID); err == nil {
				break
			}
			if err != nil {
				msg := fmt.Sprintf("Domain [%v], data partition [%v] volName [%v] load failed in replica: "+
					"addr[%v], may be raft start failed or partition is stopped by fatal error", ch.host, dp.PartitionID, volName, addr)
				if strings.Contains(err.Error(), "partition not exist") {
					checktool.WarnBySpecialUmpKey(UMPKeyDataPartitionLoadFailed, msg)
				}
				continue
			}
		}
		if dnPartition == nil {
			continue
		}
		if !ch.isReleaseCluster {
			replicaRaftStatusMap[r.Addr] = dnPartition.RaftStatus
		}
		peerStrings := convertPeersToArray(dnPartition.Peers)
		sort.Strings(peerStrings)
		sort.Strings(dp.Hosts)

		diffPeerToHost := diffSliceString(peerStrings, dp.Hosts)
		diffHostToPeer := diffSliceString(dp.Hosts, peerStrings)
		if ((dp.ReplicaNum == 3 && len(peerStrings) == 4) || (dp.ReplicaNum == 5 && len(peerStrings) == 6)) && len(diffPeerToHost) == 1 && len(diffHostToPeer) == 0 {
			peerReplica := PeerReplica{
				VolName:       volName,
				ReplicationID: dp.PartitionID,
				PeerErrorInfo: "EXCESS_PEER",
				nodeAddr:      r.Addr,
				PeerAddr:      diffPeerToHost[0],
				ReplicaNum:    dp.ReplicaNum,
			}
			badReplicas = append(badReplicas, peerReplica)
		} else if ((dp.ReplicaNum == 3 && len(peerStrings) == 2) || (dp.ReplicaNum == 5 && len(peerStrings) == 4)) && len(diffHostToPeer) == 1 && len(diffPeerToHost) == 0 {
			peerReplica := PeerReplica{
				VolName:       volName,
				ReplicationID: dp.PartitionID,
				PeerErrorInfo: "LACK_PEER",
				nodeAddr:      r.Addr,
				PeerAddr:      diffHostToPeer[0],
				ReplicaNum:    dp.ReplicaNum,
			}
			badReplicas = append(badReplicas, peerReplica)
		} else if ((dp.ReplicaNum == 3 && len(peerStrings) == 3) || (dp.ReplicaNum == 5 && len(peerStrings) == 5)) && len(diffPeerToHost) == 1 && len(diffHostToPeer) == 0 {
			peerReplica := PeerReplica{
				VolName:       volName,
				ReplicationID: dp.PartitionID,
				PeerErrorInfo: "LACK_HOST",
				nodeAddr:      r.Addr,
				PeerAddr:      diffPeerToHost[0],
				ReplicaNum:    dp.ReplicaNum,
			}
			badReplicas = append(badReplicas, peerReplica)
		} else if len(peerStrings) != len(dp.Hosts) || len(diffPeerToHost)+len(diffHostToPeer) > 0 {
			if val, ok := badPeerPartitionMap.Load(dp.PartitionID); ok {
				msg := fmt.Sprintf("[Domain: %v, vol: %v, partition: %v, host: %v, unknown] peerStrings: %v , hostStrings: %v ", ch.host, volName, dp.PartitionID, r.Addr, peerStrings, dp.Hosts)
				checktool.WarnBySpecialUmpKey(UMPKeyDataPartitionPeerInconsistency, msg)
				badPeerPartitionMap.Store(dp.PartitionID, val.(int)+1)
			} else {
				badPeerPartitionMap.Store(dp.PartitionID, 1)
			}
		}
	}
	checkRaftReplicaStatus(ch, replicaRaftStatusMap, dp.PartitionID, dp.VolName, partitionTypeDP, dp.Hosts, dp.IsRecover, dp.Replicas, nil)
	if len(badReplicas) == 0 {
		badPeerPartitionMap.Delete(dp.PartitionID)
		return
	}
	//check if this partition has a simple replica error, if simple, auto repair it
	isUniquePeerReplica := true
	if len(badReplicas) > 1 {
		for _, nextPeerReplica := range badReplicas[1:] {
			if badReplicas[0].PeerAddr != nextPeerReplica.PeerAddr || badReplicas[0].PeerErrorInfo != nextPeerReplica.PeerErrorInfo {
				isUniquePeerReplica = false
				break
			}
		}
	}

	if isUniquePeerReplica {
		repairDp(ch.isReleaseCluster, ch.host, badReplicas[0])
	} else {
		for _, item := range badReplicas {
			msg := fmt.Sprintf("[Domain: %v, VolName: %v, ReplicationID: %v, MultiError], nodeAddr: %v, PeerErrorInfo: %v, PeerAddr:%v ", ch.host, item.VolName, item.ReplicationID, item.nodeAddr, item.PeerErrorInfo, item.PeerAddr)
			checktool.WarnBySpecialUmpKey(UMPKeyDataPartitionPeerInconsistency, msg)
		}
	}
}

func getDataPartitionsFromVolume(ch *ClusterHost, volName string, authKey string) (vv *VolView, err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("action[getDataPartitionsFromVolume] host[%v],vol[%v],err[%v]", ch.host, volName, err)
		}
	}()
	vv = &VolView{
		MetaPartitions: make([]*MetaPartitionView, 0),
		DataPartitions: make([]*DataPartitionResponse, 0),
	}
	reqURL := fmt.Sprintf("http://%v/client/vol?name=%v&authKey=%v", ch.host, volName, authKey)
	data, err := doRequest(reqURL, ch.isReleaseCluster)
	if err != nil {
		log.LogErrorf("request volView from [%v] failed ", ch.host)
		return
	}
	if err = json.Unmarshal(data, &vv); err != nil {
		log.LogErrorf("unmarshal to dp data[%v],err[%v]", string(data), err)
		return
	}
	return
}

func dataNodeGetPartition(ch *ClusterHost, addr string, id uint64) (node *DNDataPartitionInfo, err error) {
	var (
		port   string
		reqURL string
	)
	if ch.host == "cn.chubaofs.jd.local" || ch.host == "sparkchubaofs.jd.local" || ch.host == "idbbak.chubaofs.jd.local" || ch.host == "cn.chubaofs-seqwrite.jd.local" ||
		ch.host == "nl.chubaofs.jd.local" || ch.host == "cn.elasticdb.jd.local" || ch.host == "nl.chubaofs.ochama.com" {
		port = "6001"
	} else {
		log.LogInfof("action[checkDetaNodeDiskStat] host[%v] can not match its DN port", ch.host)
		return
	}
	reqURL = fmt.Sprintf("http://%v:%v/partition?id=%v", addr, port, id)
	data, err := doRequest(reqURL, ch.isReleaseCluster)
	if err != nil {
		log.LogErrorf("request dp from host: [%v] addr: [%v] failed , err: %v", ch.host, reqURL, err)
		return
	}
	dp := &node
	if err = json.Unmarshal(data, dp); err != nil {
		log.LogErrorf("[%v] unmarshal to dp data[%v],err[%v]", addr, string(data), err)
		return
	}
	return
}

func repairDp(release bool, host string, replica PeerReplica) {
	switch replica.PeerErrorInfo {
	case "LACK_PEER":
		masterClient := master.NewMasterClient([]string{host}, false)
		vol, err := masterClient.AdminAPI().GetVolumeSimpleInfo(replica.VolName)
		if err != nil {
			log.LogErrorf("[Domain: %v, PartitionID: %-2v , ErrorType: %v] repair failed, err:%v", host, replica.ReplicationID, replica.PeerErrorInfo, err)
			return
		}
		if vol.DpReplicaNum != replica.ReplicaNum {
			log.LogErrorf("[Domain: %v, PartitionID: %-2v , ErrorType: %v] repair failed, volume[%v] replica num is different[%v-%v], maybe in replica num transfer", host, replica.ReplicationID, replica.PeerErrorInfo, replica.VolName, vol.DpReplicaNum, replica.ReplicaNum)
			return
		}
		if err = decommissionDp(release, host, replica.VolName, replica.ReplicationID, replica.PeerAddr); err != nil {
			log.LogErrorf("[Domain: %v, PartitionID: %-2v , ErrorType: %v] repair failed, err:%v", host, replica.ReplicationID, replica.PeerErrorInfo, err)
			return
		}
		log.LogWarnf("[Domain: %v, PartitionID: %-2v , ErrorType: %v] has been automatically repaired, cmd[cfs-cli datapartition decommission %v %v]", host, replica.ReplicationID,
			replica.PeerErrorInfo, replica.PeerAddr, replica.ReplicationID)
	case "EXCESS_PEER":
		if err := addDpReplica(host, replica.ReplicationID, replica.PeerAddr); err != nil {
			log.LogErrorf("[Domain: %v, PartitionID: %-2v , ErrorType: %v] repair-addDpReplica failed, err:%v", host, replica.ReplicationID, replica.PeerErrorInfo, err)
			break
		}
		time.Sleep(time.Second * 3)
		if err := delDpReplica(host, replica.ReplicationID, replica.PeerAddr); err != nil {
			log.LogErrorf("[Domain: %v, PartitionID: %-2v , ErrorType: %v] repair-delDpReplica failed, err:%v", host, replica.ReplicationID, replica.PeerErrorInfo, err)
			break
		}
		log.LogWarnf("[Domain: %v, PartitionID: %-2v , ErrorType: %v] has been automatically repaired, cmd[cfs-cli datapartition add-replica %v %v && sleep 3 && cfs-cli datapartition del-replica %v %v]",
			host, replica.ReplicationID, replica.PeerErrorInfo, replica.PeerAddr, replica.ReplicationID, replica.PeerAddr, replica.ReplicationID)
	case "LACK_HOST":
		outputStr := fmt.Sprintf("[Domain: %v, PartitionID: %-2v , ErrorType: HOST2_PEER3] recommond cmd: cfs-cli datapartition add-replica %v %v",
			host, replica.ReplicationID, replica.PeerAddr, replica.ReplicationID)
		checktool.WarnBySpecialUmpKey(UMPKeyDataPartitionPeerInconsistency, outputStr)
	default:
		log.LogErrorf("wrong error info, %v", replica.PeerErrorInfo)
		return
	}
}

func decommissionDp(releaseDB bool, master string, vol string, partition uint64, addr string) (err error) {
	var reqURL string
	if releaseDB {
		reqURL = fmt.Sprintf("http://%v/dataPartition/offline?name=%v&id=%v&addr=%v", master, vol, partition, addr)
	} else {
		reqURL = fmt.Sprintf("http://%v/dataPartition/decommission?id=%v&addr=%v", master, partition, addr)
	}
	_, err = doRequest(reqURL, releaseDB)
	return
}

func addDpReplica(master string, partition uint64, addr string) (err error) {
	reqURL := fmt.Sprintf("http://%v/dataReplica/add?id=%v&addr=%v", master, partition, addr)
	_, err = doRequest(reqURL, false)
	return
}

func delDpReplica(master string, partition uint64, addr string) (err error) {
	reqURL := fmt.Sprintf("http://%v/dataReplica/delete?id=%v&addr=%v", master, partition, addr)
	_, err = doRequest(reqURL, false)
	return
}

func checkHostDataPartition(host *ClusterHost, concurrency int) {
	defer func() {
		checkDpCorruptWg.Done()
		if r := recover(); r != nil {
			msg := fmt.Sprintf("checkHostDataPartition panic:%v", r)
			log.LogError(msg)
			fmt.Println(msg)
		}
	}()
	log.LogInfof("check [%v] dataPartition corrupt begin", host)
	startTime := time.Now()
	volStats, _, err := getAllVolStat(host)
	if err != nil {
		log.LogErrorf("[%v] get allVolStat occurred error, err:%v", host, err)
		return
	}
	for _, vss := range volStats {
		var wg sync.WaitGroup
		var vol *SimpleVolView
		vol, err = getVolSimpleView(vss.Name, host)
		if err != nil || vol == nil {
			log.LogErrorf("get volume simple view failed: %v err:%v", vss.Name, err)
			continue
		}
		authKey := checktool.Md5(vol.Owner)
		var volView *VolView
		if volView, err = getDataPartitionsFromVolume(host, vss.Name, authKey); err != nil {
			log.LogErrorf("Found an invalid vol: %v err:%v", vol.Name, err)
			continue
		}
		if volView.DataPartitions == nil || len(volView.DataPartitions) == 0 {
			log.LogWarnf("action[checkHostDataPartition] get dataPartitions from volView failed, vol:%v", vol.Name)
			continue
		}
		dpChan := make(chan *DataPartitionResponse, 20)
		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func(w *sync.WaitGroup, ch chan *DataPartitionResponse) {
				defer w.Done()
				for dp := range ch {
					checkDataPartitionPeers(host, vol.Name, dp.PartitionID)
					time.Sleep(time.Millisecond * 13)
				}
			}(&wg, dpChan)
		}
		for _, dp := range volView.DataPartitions {
			dpChan <- dp
		}
		close(dpChan)
		wg.Wait()
	}

	log.LogInfof("check [%v] dataPartition corrupt end, cost [%v]", host, time.Since(startTime))
}
