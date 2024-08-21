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
	"strings"
	"sync"
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
	// regard {k, v} in host0 as the init common map
	common := make(map[string][]byte, 0)
	for k, v := range dataMap[rocksPaths[0]] {
		common[k] = v
	}
	for k, v := range common {
		for masterHost, m := range dataMap {
			if masterHost == rocksPaths[0] {
				continue
			}
			//delete the common key that not exist in other hosts
			if _, ok := m[k]; !ok {
				delete(common, k)
				continue
			}
			//delete the common key whose value not deep equal with value from other hosts
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
	buf.WriteString(fmt.Sprintf("prefix:%s found invalid data, common:%v\n", prefix, len(common)))
	log.LogWarnf(fmt.Sprintf("cluster:%v, prefix:%s found invalid data, common:%v\n", cluster, prefix, len(common)))
	for path, m := range dataMap {
		log.LogInfof("cluster:%v, path:%v, prefix:%v, len:%v, common:%v\n", cluster, path, prefix, len(m), len(common))
	}
	buf.WriteString(fmt.Sprintf("common len:%d\n", len(common)))
	for p := range dataMap {
		buf.WriteString(fmt.Sprintf(" path(%s) len:%d\n", p, len(dataMap[p])))
	}
	var out bytes.Buffer

	//print diff
	for path, m := range dataMap {
		host := strings.Split(path, "/")[len(strings.Split(path, "/"))-1]
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
					dpMap[dpv.PartitionID] = host
				} else {
					dpMap[dpv.PartitionID] = dpMap[dpv.PartitionID] + "," + host
				}
			case metaPartitionPrefix:
				mpv := &metaPartitionValue{}
				if err := json.Unmarshal(v, mpv); err != nil {
					err = fmt.Errorf("action[loadMetaPartitions],value:%v,unmarshal err:%v", string(v), err)
					log.LogErrorf("%v\n", err)
					continue
				}
				if _, ok := mpMap[mpv.PartitionID]; !ok {
					mpMap[mpv.PartitionID] = host
				} else {
					mpMap[mpv.PartitionID] = mpMap[mpv.PartitionID] + "," + host
				}
			case volPrefix:
				var vv *volValue
				if err := json.Unmarshal(v, vv); err != nil {
					err = fmt.Errorf("action[loadVols],value:%v,unmarshal err:%v", string(v), err)
					log.LogErrorf("%v\n", err)
					continue
				}
				if _, ok := volMap[k]; !ok {
					volMap[k] = host
				} else {
					volMap[k] = volMap[k] + "," + host
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
			msg := "cluster: " + cluster + ", found bad dp:["
			for dp := range dpMap {
				msg += fmt.Sprintf("%v,", dp)
			}
			msg += "]"
			exporter.WarningBySpecialUMPKey(umpKey, msg)
		}
	case metaPartitionPrefix:
		if len(mpMap) > 0 {
			msg := "cluster: " + cluster + ", found bad mp:["
			for mpid := range mpMap {
				msg += fmt.Sprintf("%v,", mpid)
			}
			msg += "]"
			exporter.WarningBySpecialUMPKey(umpKey, msg)
		}
	case volPrefix:
		if len(volMap) > 0 {
			msg := "cluster: " + cluster + ", found bad vol:["
			for v := range volMap {
				msg += fmt.Sprintf("%v,", v)
			}
			msg += "]"
			exporter.WarningBySpecialUMPKey(umpKey, msg)
		}
	default:
		exporter.WarningBySpecialUMPKey(umpKey, fmt.Sprintf("cluster: %v, found bad rocksdb data for prefix:%v", cluster, prefix))
	}
	return buf.String()
}
