package compare_meta_dbbak

import (
	"bytes"
	"encoding/json"
	"fmt"
	bsProto "github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/raftstore"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"reflect"
)

const (
	keySeparator         = "#"
	metaNodeAcronym      = "mn"
	dataNodeAcronym      = "dn"
	dataPartitionAcronym = "dp"
	metaPartitionAcronym = "mp"
	volAcronym           = "vol"
	clusterAcronym       = "c"
	tokenAcronym         = "t"
	metaNodePrefix       = keySeparator + metaNodeAcronym + keySeparator
	dataNodePrefix       = keySeparator + dataNodeAcronym + keySeparator
	dataPartitionPrefix  = keySeparator + dataPartitionAcronym + keySeparator
	volPrefix            = keySeparator + volAcronym + keySeparator
	metaPartitionPrefix  = keySeparator + metaPartitionAcronym + keySeparator
	clusterPrefix        = keySeparator + clusterAcronym + keySeparator
	TokenPrefix          = keySeparator + tokenAcronym + keySeparator
)

var metaPrefixs = []string{
	metaNodePrefix,
	dataNodePrefix,
	volPrefix,
	metaPartitionPrefix,
	clusterPrefix,
	TokenPrefix,
	dataPartitionPrefix,
}

type dataPartitionValue struct {
	PartitionID   uint64
	ReplicaNum    uint8
	Hosts         string
	PartitionType string
	IsManual      bool
	Replicas      []*replicaValue
}

type replicaValue struct {
	Addr     string
	DiskPath string
}

type metaPartitionValue struct {
	PartitionID   uint64
	ReplicaNum    uint8
	Start         uint64
	End           uint64
	Hosts         string
	Peers         []bsProto.Peer
	IsManual      bool
	OfflinePeerID uint64
}

type volValue struct {
	VolType            string
	ReplicaNum         uint8
	Status             uint8
	Capacity           uint64
	MinWritableDPNum   uint64
	MinWritableMPNum   uint64
	Owner              string
	CrossPod           bool
	AutoRepairCrossPod bool
	EnableToken        bool
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

func (c *CompareMeta) RangeCompareKeys(rocksPaths []string, dbMap map[string]*raftstore.RocksDBStore, cluster string, umpKey string, iter func(string)) {
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
			exporter.WarningBySpecialUMPKey(umpKey, msg)
		} else {
			log.LogInfof("cluster:%v prefix:%v passed", cluster, prefix)
		}
	})
	return
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
		return fmt.Sprintf("dbPath:%v prefix:%v key:%v val:pid(%v)ptype(%v)num(%v)manual(%v)hosts(%v)replicas(%v)", dbPath, prefix, k, dpv.PartitionID, dpv.PartitionType, dpv.ReplicaNum, dpv.IsManual, dpv.Hosts, dpv.Replicas)
	case metaPartitionPrefix:
		mpv := &metaPartitionValue{}
		if err := json.Unmarshal(v, mpv); err != nil {
			err = fmt.Errorf("action[loadMetaPartitions],value:%v,unmarshal err:%v", string(v), err)
			log.LogErrorf("%v\n", err)
			return "NULL"
		}
		return fmt.Sprintf("dbPath:%v prefix:%v key:%v val:pid(%v)num(%v)manual(%v)hosts(%v)start(%v)end(%v)offid(%v)peers(%v)", dbPath, prefix, k, mpv.PartitionID, mpv.ReplicaNum, mpv.IsManual, mpv.Hosts, mpv.Start, mpv.End, mpv.OfflinePeerID, mpv.Peers)
	case volPrefix:
		vv := &volValue{}
		if err := json.Unmarshal(v, vv); err != nil {
			err = fmt.Errorf("action[loadVols],value:%v,unmarshal err:%v", string(v), err)
			log.LogErrorf("%v\n", err)
			return "NULL"
		}
		return fmt.Sprintf("dbPath:%v prefix:%v key:%v val:num(%v)type(%v)cap(%v)stat(%v)owner(%v)", dbPath, prefix, k, vv.ReplicaNum, vv.VolType, vv.Capacity, vv.Status, vv.Owner)
	default:
		json.Indent(&out, v, "", "\t")
		result := fmt.Sprintf("dbPath:%v prefix:%v, key:%v, val:%v\n", dbPath, prefix, k, out.String())
		out.Reset()
		return result
	}
}
