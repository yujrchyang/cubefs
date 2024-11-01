package compare_meta

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/raftstore"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"reflect"
	"strings"
)

type volValue struct {
	ID                    uint64
	Name                  string
	ReplicaNum            uint8
	DpReplicaNum          uint8
	MpLearnerNum          uint8
	DpLearnerNum          uint8
	Status                uint8
	DataPartitionSize     uint64
	Capacity              uint64
	DpWriteableThreshold  float64
	Owner                 string
	FollowerRead          bool
	FollowerReadDelayCfg  proto.DpFollowerReadDelayConfig
	FollReadHostWeight    int
	NearRead              bool
	ForceROW              bool
	ForceRowModifyTime    int64
	EnableWriteCache      bool
	CrossRegionHAType     proto.CrossRegionHAType
	Authenticate          bool
	EnableToken           bool
	CrossZone             bool
	AutoRepair            bool
	VolWriteMutexEnable   bool
	VolWriteMutexHolder   string
	VolWriteMutexSlaves   map[string]string
	ZoneName              string
	OSSAccessKey          string
	OSSSecretKey          string
	CreateTime            int64
	Description           string
	DpSelectorName        string
	DpSelectorParm        string
	OSSBucketPolicy       proto.BucketAccessPolicy
	DPConvertMode         proto.ConvertMode
	MPConvertMode         proto.ConvertMode
	ExtentCacheExpireSec  int64
	MinWritableMPNum      int
	MinWritableDPNum      int
	TrashRemainingDays    uint32
	DefStoreMode          proto.StoreMode
	ConverState           proto.VolConvertState
	MpLayout              proto.MetaPartitionLayout
	IsSmart               bool
	SmartEnableTime       int64
	SmartRules            []string
	CompactTag            proto.CompactTag
	CompactTagModifyTime  int64
	EcDataNum             uint8
	EcParityNum           uint8
	EcSaveTime            int64
	EcWaitTime            int64
	EcTimeOut             int64
	EcRetryWait           int64
	EcMaxUnitSize         uint64
	EcEnable              bool
	ChildFileMaxCnt       uint32
	TrashCleanInterval    uint64
	BatchDelInodeCnt      uint32
	DelInodeInterval      uint32
	UmpCollectWay         exporter.UMPCollectMethod
	ReuseMP               bool
	EnableBitMapAllocator bool
	TrashCleanDuration    int32
	TrashCleanMaxCount    int32
	NewVolName            string
	NewVolID              uint64
	OldVolName            string
	FinalVolStatus        uint8
	RenameConvertStatus   proto.VolRenameConvertStatus
	MarkDeleteTime        int64

	RemoteCacheBoostPath   string
	RemoteCacheBoostEnable bool
	RemoteCacheAutoPrepare bool
	RemoteCacheTTL         int64
}
type metaPartitionValue struct {
	PartitionID    uint64
	Start          uint64
	End            uint64
	VolID          uint64
	ReplicaNum     uint8
	Status         int8
	VolName        string
	Hosts          string
	LearnerNum     uint8
	OfflinePeerID  uint64
	Peers          []proto.Peer
	Learners       []proto.Learner
	PanicHosts     []string
	IsRecover      bool
	PrePartitionID uint64
}
type dataPartitionValue struct {
	PartitionID     uint64
	CreateTime      int64
	ReplicaNum      uint8
	Hosts           string
	Peers           []proto.Peer
	Learners        []proto.Learner
	Status          int8
	VolID           uint64
	VolName         string
	OfflinePeerID   uint64
	Replicas        []*replicaValue
	IsRecover       bool
	IsFrozen        bool
	PanicHosts      []string
	IsManual        bool
	EcMigrateStatus uint8
}

type replicaValue struct {
	Addr     string
	DiskPath string
}

const (
	keySeparator               = "#"
	metaNodeAcronym            = "mn"
	dataNodeAcronym            = "dn"
	dataPartitionAcronym       = "dp"
	frozenDataPartitionAcronym = "frozen_dp"
	metaPartitionAcronym       = "mp"
	volAcronym                 = "vol"
	regionAcronym              = "region"
	idcAcronym                 = "idc"
	clusterAcronym             = "c"
	nodeSetAcronym             = "s"
	tokenAcronym               = "t"

	metaNodePrefix      = keySeparator + metaNodeAcronym + keySeparator
	dataNodePrefix      = keySeparator + dataNodeAcronym + keySeparator
	dataPartitionPrefix = keySeparator + dataPartitionAcronym + keySeparator
	volPrefix           = keySeparator + volAcronym + keySeparator
	regionPrefix        = keySeparator + regionAcronym + keySeparator
	idcPrefix           = keySeparator + idcAcronym + keySeparator
	metaPartitionPrefix = keySeparator + metaPartitionAcronym + keySeparator
	clusterPrefix       = keySeparator + clusterAcronym + keySeparator
	nodeSetPrefix       = keySeparator + nodeSetAcronym + keySeparator
	frozenDPPrefix      = keySeparator + frozenDataPartitionAcronym + keySeparator
	TokenPrefix         = keySeparator + tokenAcronym + keySeparator
	applied             = "applied"
)

var metaPrefixs = []string{
	metaNodePrefix,
	dataNodePrefix,
	applied,
	volPrefix,
	regionPrefix,
	idcPrefix,
	metaPartitionPrefix,
	clusterPrefix,
	nodeSetPrefix,
	frozenDPPrefix,
	TokenPrefix,
	dataPartitionPrefix,
}

type CompareMeta struct {
	cluster string
	domain  string
}

func NewCompare(cluster, domain string) *CompareMeta {
	return &CompareMeta{
		cluster: cluster,
		domain:  domain,
	}
}

func (c *CompareMeta) rangePrefix(iter func(string)) {
	for _, pre := range metaPrefixs {
		iter(pre)
	}
}

func rangeRocksDB(rs *raftstore.RocksDBStore, prefix []byte, iter func(string, []byte) bool) (err error) {
	snapshot := rs.RocksDBSnapshot()
	it := rs.Iterator(snapshot)
	defer func() {
		it.Close()
		rs.ReleaseSnapshot(snapshot)
	}()
	it.Seek(prefix)
	for ; it.ValidForPrefix(prefix); it.Next() {
		key := it.Key().Data()
		value := it.Value().Data()
		valueByte := make([]byte, len(value))
		copy(valueByte, value)
		iter(string(key), valueByte)
		it.Key().Free()
		it.Value().Free()
	}
	if err = it.Err(); err != nil {
		return err
	}
	return
}

func (c *CompareMeta) RangeCompareKeys(rocksPaths []string, dbMap map[string]*raftstore.RocksDBStore, cluster string, warn func(msg string), iter func(string)) {
	c.rangePrefix(func(prefix string) {
		badCount := 0
		badKeys := make([]string, 0)
		_ = rangeRocksDB(dbMap[rocksPaths[0]], []byte(prefix), func(key string, val []byte) bool {
			diff := false
			for _, dbPath := range rocksPaths[1:] {
				result, err := dbMap[dbPath].Get(key)
				if err != nil {
					log.LogErrorf("get rocksdb key:%v err:%v", key, err)
					return true
				}
				if !reflect.DeepEqual(val, result.([]byte)) {
					diff = true
				}
			}
			if diff {
				badCount++
				badKeys = append(badKeys, key)
				for _, dbPath := range rocksPaths {
					result, err := dbMap[dbPath].Get(key)
					if err != nil {
						log.LogErrorf("get rocksdb key:%v err:%v", key, err)
						return true
					}
					if badCount < 100 {
						out := handleBadItem(dbPath, prefix, key, result.([]byte))
						iter(out)
					}
				}
			}
			return true
		})
		if badCount > 0 {
			var msg string
			if len(badKeys) > 100 {
				msg = fmt.Sprintf("cluster:%v prefix:%v too many bad keys, badKeys num:%v", cluster, prefix, len(badKeys))
			} else {
				msg = fmt.Sprintf("cluster:%v prefix:%v badKeys:%v num:%v", cluster, prefix, badKeys, len(badKeys))
			}
			warn(msg)
		} else {
			log.LogInfof("cluster:%v prefix:%v passed", cluster, prefix)
		}
	})
	return
}

func formatPeers(peers []proto.Peer) string {
	sb := strings.Builder{}
	for _, peer := range peers {
		sb.WriteString(fmt.Sprintf("{%v,%v},", peer.ID, peer.Addr))
	}
	return sb.String()
}

func formatLearners(learners []proto.Learner) string {
	sb := strings.Builder{}
	for _, learner := range learners {
		sb.WriteString(fmt.Sprintf("{%v,%v},", learner.ID, learner.Addr))
	}
	return sb.String()
}

func handleBadItem(dbPath, prefix string, k string, v []byte) string {
	var out bytes.Buffer
	switch prefix {
	case dataPartitionPrefix:
		dpv := &dataPartitionValue{}
		if err := json.Unmarshal(v, dpv); err != nil {
			err = fmt.Errorf("action[loadDataPartitions],value:%v,unmarshal err:%v", string(v), err)
			log.LogErrorf("%v\n", err)
			return "NULL"
		}
		return fmt.Sprintf("dbPath:%v prefix:%v key:%v val:vol(%v)pid(%v)stat(%v)num(%v)recover(%v)hosts(%v)offlineid(%v)peers(%v)learner(%v)create(%v)", dbPath, prefix, k, dpv.VolID, dpv.PartitionID, dpv.Status, dpv.ReplicaNum, dpv.IsRecover, dpv.Hosts, dpv.OfflinePeerID, formatPeers(dpv.Peers), formatLearners(dpv.Learners), dpv.CreateTime)
	case metaPartitionPrefix:
		mpv := &metaPartitionValue{}
		if err := json.Unmarshal(v, mpv); err != nil {
			err = fmt.Errorf("action[loadMetaPartitions],value:%v,unmarshal err:%v", string(v), err)
			log.LogErrorf("%v\n", err)
			return "NULL"
		}
		return fmt.Sprintf("dbPath:%v prefix:%v key:%v val:vol(%v)pid(%v)stat(%v)num(%v)recover(%v)hosts(%v)offlineid(%v)peers(%v)learner(%v)start(%v)end(%v)pre(%v)", dbPath, prefix, k, mpv.VolID, mpv.PartitionID, mpv.Status, mpv.ReplicaNum, mpv.IsRecover, mpv.Hosts, mpv.OfflinePeerID, formatPeers(mpv.Peers), formatLearners(mpv.Learners), mpv.Start, mpv.End, mpv.PrePartitionID)
	case volPrefix:
		vv := &volValue{}
		if err := json.Unmarshal(v, vv); err != nil {
			err = fmt.Errorf("action[loadVols],value:%v,unmarshal err:%v", string(v), err)
			log.LogErrorf("%v\n", err)
			return "NULL"
		}
		return fmt.Sprintf("dbPath:%v prefix:%v key:%v val:name(%v)oname(%v)nname(%v)zone(%v)stat(%v)owner(%v)cap(%v)deltime(%v)", dbPath, prefix, k, vv.Name, vv.OldVolName, vv.NewVolName, vv.ZoneName, vv.Status, vv.Owner, vv.Capacity, vv.MarkDeleteTime)
	default:
		json.Indent(&out, v, "", "\t")
		result := fmt.Sprintf("dbPath:%v prefix:%v, key:%v, val:%v\n", dbPath, prefix, k, out.String())
		out.Reset()
		return result
	}
}
