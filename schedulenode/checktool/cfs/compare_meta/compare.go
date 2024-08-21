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
	"sync"
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
	PartitionID   uint64
	Start         uint64
	End           uint64
	VolID         uint64
	ReplicaNum    uint8
	LearnerNum    uint8
	Status        int8
	VolName       string
	Hosts         string
	OfflinePeerID uint64
	Peers         []proto.Peer
	Learners      []proto.Learner
	PanicHosts    []string
	IsRecover     bool
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

func (c *CompareMeta) RangePrefix(iter func(string)) {
	for _, pre := range metaPrefixs {
		iter(pre)
	}
}

func getRocksDataByPrefix(rs *raftstore.RocksDBStore, prefix []byte) (result map[string][]byte, err error) {
	result = make(map[string][]byte, 0)
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
		result[string(key)] = valueByte
		it.Key().Free()
		it.Value().Free()
	}
	if err = it.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *CompareMeta) Compare(rocksPaths []string, prefix string, dbMap map[string]*raftstore.RocksDBStore, cluster string, umpKey string) (resultStr string) {
	log.LogInfof("action[Compare] start load rocksdb data for cluster:%s prefix:%s\n", cluster, prefix)
	dataMap := make(map[string]map[string][]byte, 0)
	lock := sync.Mutex{}
	wg := sync.WaitGroup{}
	for _, rPath := range rocksPaths {
		wg.Add(1)
		go func(dataPath string, pfx string) {
			defer wg.Done()
			result, err1 := getRocksDataByPrefix(dbMap[dataPath], []byte(pfx))
			if err1 != nil {
				log.LogErrorf("load rocksdb from path:%s, prefix:%s, err:%v\n", dataPath, pfx, err1)
				return
			}
			lock.Lock()
			defer lock.Unlock()
			dataMap[dataPath] = result
		}(rPath, prefix)
	}
	wg.Wait()
	log.LogInfof("action[Compare] finished load rocksdb data for cluster:%s prefix:%s\n", cluster, prefix)

	dpMap := make(map[uint64]string, 0)
	mpMap := make(map[uint64]string, 0)
	volMap := make(map[string]string, 0)

	buf := bytes.Buffer{}
	// find common
	common := make(map[string][]byte, 0)
	for k, v := range dataMap[rocksPaths[0]] {
		common[k] = v
	}
	for k, v := range common {
		for path, m := range dataMap {
			if path == rocksPaths[0] {
				continue
			}
			if _, ok := m[k]; !ok {
				delete(common, k)
				continue
			}
			if !reflect.DeepEqual(v, m[k]) {
				delete(common, k)
			}
		}
	}
	same := true
	for _, v := range dataMap {
		if len(v) != len(common) {
			same = false
		}
	}
	if same {
		log.LogInfof("action[Compare] cluster:%s prefix:%s check success\n", cluster, prefix)
		return
	}
	buf.WriteString(fmt.Sprintf("prefix:%s found invalid data\n", prefix))
	buf.WriteString(fmt.Sprintf("common len:%d\n", len(common)))
	log.LogWarnf(fmt.Sprintf("cluster:%v, prefix:%s found invalid data, common:%v\n", cluster, prefix, len(common)))
	for path, m := range dataMap {
		log.LogInfof("cluster:%v, path:%v, prefix:%v, len:%v, common:%v\n", cluster, path, prefix, len(m), len(common))
	}
	for p := range dataMap {
		buf.WriteString(fmt.Sprintf(" path(%s) len:%d\n", p, len(dataMap[p])))
	}
	var out bytes.Buffer

	//print diff
	for path, m := range dataMap {
		for k, v := range m {
			if _, ok := common[k]; ok {
				continue
			}
			switch prefix {
			case dataPartitionPrefix:
				dpv := &dataPartitionValue{}
				if err := json.Unmarshal(v, dpv); err != nil {
					err = fmt.Errorf("action[loadDataPartitions],value:%v,unmarshal err:%v", string(v), err)
					log.LogErrorf("%v\n", err)
					continue
				}
				if _, ok := dpMap[dpv.PartitionID]; !ok {
					dpMap[dpv.PartitionID] = strings.Split(path, "/")[len(strings.Split(path, "/"))-1]
				} else {
					dpMap[dpv.PartitionID] = dpMap[dpv.PartitionID] + "," + strings.Split(path, "/")[len(strings.Split(path, "/"))-1]
				}
			case metaPartitionPrefix:
				mpv := &metaPartitionValue{}
				if err := json.Unmarshal(v, mpv); err != nil {
					err = fmt.Errorf("action[loadMetaPartitions],value:%v,unmarshal err:%v", string(v), err)
					log.LogErrorf("%v\n", err)
					continue
				}
				if _, ok := mpMap[mpv.PartitionID]; !ok {
					mpMap[mpv.PartitionID] = strings.Split(path, "/")[len(strings.Split(path, "/"))-1]
				} else {
					mpMap[mpv.PartitionID] = mpMap[mpv.PartitionID] + "," + strings.Split(path, "/")[len(strings.Split(path, "/"))-1]
				}
			case volPrefix:
				var vv *volValue
				if err := json.Unmarshal(v, vv); err != nil {
					err = fmt.Errorf("action[loadVols],value:%v,unmarshal err:%v", string(v), err)
					log.LogErrorf("%v\n", err)
					continue
				}
				if _, ok := volMap[vv.Name]; !ok {
					volMap[vv.Name] = strings.Split(path, "/")[len(strings.Split(path, "/"))-1]
				} else {
					volMap[vv.Name] = volMap[vv.Name] + "," + strings.Split(path, "/")[len(strings.Split(path, "/"))-1]
				}
			default:
			}
			json.Indent(&out, v, "", "\t")
			buf.WriteString(fmt.Sprintf("prefix:%v, path:%v, key:%v, json value:\n%v\n", prefix, path, k, out.String()))
			out.Reset()
		}
	}
	switch prefix {
	case dataPartitionPrefix:
		if len(dpMap) > 0 {
			msg := "cluster: " + cluster + ", found bad dp:{"
			for dp, hosts := range dpMap {
				msg += fmt.Sprintf("{dp:%v, hosts:%v}", dp, hosts)
			}
			msg += "}"
			exporter.WarningBySpecialUMPKey(umpKey, msg)
		}
	case metaPartitionPrefix:
		if len(mpMap) > 0 {
			msg := "cluster: " + cluster + ", found bad mp:{"
			for mp, hosts := range mpMap {
				msg += fmt.Sprintf("{mp:%v, hosts:%v}", mp, hosts)
			}
			msg += "}"
			exporter.WarningBySpecialUMPKey(umpKey, msg)
		}
	case volPrefix:
		if len(volMap) > 0 {
			msg := "cluster: " + cluster + ", found bad vol:{"
			for v, hosts := range volMap {
				msg += fmt.Sprintf("{vol:%v, hosts:%v}", v, hosts)
			}
			msg += "}"
			exporter.WarningBySpecialUMPKey(umpKey, msg)
		}
	default:
		exporter.WarningBySpecialUMPKey(umpKey, fmt.Sprintf("cluster: %v, found bad rocksdb data for prefix:%v", cluster, prefix))
	}
	return buf.String()
}
