package rebalance

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/cmd/common"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/schedulenode/worker"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
	"gorm.io/gorm"
	"net/http"
	"sync"
)

type ClusterConfig struct {
	ClusterName  string   `json:"clusterName"`
	MasterAddrs  []string `json:"mastersAddr"`
	MetaProfPort uint16   `json:"mnProfPort"`
	DataProfPort uint16   `json:"dnProfPort"`
	IsDBBack     bool     `json:"isDBBack"`
}

type ReBalanceWorker struct {
	worker.BaseWorker
	mcwRWMutex       sync.RWMutex
	reBalanceCtrlMap sync.Map
	clusterConfigMap sync.Map
	dbHandle         *gorm.DB
}

func NewReBalanceWorker() *ReBalanceWorker {
	return &ReBalanceWorker{}
}

func (rw *ReBalanceWorker) Start(cfg *config.Config) (err error) {
	return rw.Control.Start(rw, cfg, doStart)
}

func doStart(s common.Server, cfg *config.Config) (err error) {
	rw, ok := s.(*ReBalanceWorker)
	if !ok {
		err = errors.New("Invalid Node Type")
		return
	}
	rw.StopC = make(chan struct{}, 0)
	rw.reBalanceCtrlMap = sync.Map{}
	if err = rw.parseConfig(cfg); err != nil {
		log.LogErrorf("[doStart] parse config info failed, error(%v)", err)
		return
	}

	err = rw.OpenSql()
	if err != nil {
		return err
	}
	err = rw.loadInRunningRebalanced()
	if err != nil {
		return err
	}

	rw.registerHandler()
	return nil
}

func (rw *ReBalanceWorker) Shutdown() {
	rw.Control.Shutdown(rw, doShutdown)
}

func doShutdown(s common.Server) {
	m, ok := s.(*ReBalanceWorker)
	if !ok {
		return
	}
	close(m.StopC)
}

func (rw *ReBalanceWorker) Sync() {
	rw.Control.Sync()
}

func (rw *ReBalanceWorker) parseConfig(cfg *config.Config) (err error) {
	err = rw.ParseBaseConfig(cfg)

	clustersInfoData := cfg.GetJsonObjectSlice(config.ConfigKeyClusterInfo)
	for _, clusterInfoData := range clustersInfoData {
		clusterConf := new(ClusterConfig)
		if err = json.Unmarshal(clusterInfoData, clusterConf); err != nil {
			err = fmt.Errorf("parse cluster info failed:%v", err)
			return
		}
		rw.clusterConfigMap.Store(clusterConf.ClusterName, clusterConf)
	}
	return
}

func (rw *ReBalanceWorker) getClusterHost(cluster string) (host string, err error) {
	clusterInfo, ok := rw.clusterConfigMap.Load(cluster)
	if ok {
		clusterConf := clusterInfo.(*ClusterConfig)
		if len(clusterConf.MasterAddrs) > 0 {
			host = clusterInfo.(*ClusterConfig).MasterAddrs[0]
			return
		}
	}

	switch cluster {
	case SPARK:
		host = "cn.chubaofs.jd.local"
	case DBBAK:
		host = "cn.chubaofs-seqwrite.jd.local"
		err = fmt.Errorf("cluster:%v Not supported", cluster)
	case ELASTICDB:
		host = "cn.elasticdb.jd.local"
		//err = fmt.Errorf("cluster:%v Not supported", cluster)
	case TEST:
		host = "11.60.241.50:17010"
	}
	if len(host) == 0 {
		err = fmt.Errorf("cluster:%v Not supported", cluster)
	}
	return
}

func (rw *ReBalanceWorker) getDataNodePProfPort(host string) (port string) {
	rw.clusterConfigMap.Range(func(key, value interface{}) bool {
		clusterConfig := value.(*ClusterConfig)
		for _, addr := range clusterConfig.MasterAddrs {
			if addr == host {
				port = fmt.Sprintf("%v", clusterConfig.DataProfPort)
				return false
			}
		}
		return true
	})
	if port != "" {
		return
	}

	switch host {
	case "cn.chubaofs.jd.local", "cn.elasticdb.jd.local", "cn.chubaofs-seqwrite.jd.local", "nl.chubaofs.jd.local", "nl.chubaofs.ochama.com":
		port = "6001"
	case "192.168.0.11:17010", "192.168.0.12:17010", "192.168.0.13:17010":
		port = "17320"
	default:
		port = "6001"
	}
	return
}

func (rw *ReBalanceWorker) loadInRunningRebalanced() (err error) {
	var rInfos []*RebalancedInfoTable
	if rInfos, err = rw.GetRebalancedInfoByStatus(StatusRunning); err != nil {
		return
	}
	for _, info := range rInfos {
		if info.dstMetaNodePartitionMaxCount == 0 || info.dstMetaNodePartitionMaxCount > defaultDstMetaNodePartitionMaxCount {
			info.dstMetaNodePartitionMaxCount = defaultDstMetaNodePartitionMaxCount
		}
		err = rw.ReBalanceStart(info.Host, info.ZoneName, info.RType, info.HighRatio, info.LowRatio, info.GoalRatio,
			info.MaxBatchCount, info.MigrateLimitPerDisk, info.dstMetaNodePartitionMaxCount)
		if err != nil {
			log.LogErrorf("start rebalance info:%v err:%v", info, err)
		}
		log.LogInfof("rebalance start host:%v zoneName:%v", info.Host, info.ZoneName)
	}
	return
}

func (rw *ReBalanceWorker) registerHandler() {
	http.HandleFunc(proto.VersionPath, func(w http.ResponseWriter, _ *http.Request) {
		version := proto.MakeVersion("rebalance")
		marshal, _ := json.Marshal(version)
		if _, err := w.Write(marshal); err != nil {
			log.LogErrorf("write version has err:[%s]", err.Error())
		}
	})
	http.HandleFunc(RBStart, responseHandler(rw.handleStart))
	http.HandleFunc(RBStop, responseHandler(rw.handleStop))
	http.HandleFunc(RBStatus, responseHandler(rw.handleStatus))
	http.HandleFunc(RBReset, responseHandler(rw.handleReset))
	http.HandleFunc(RBInfo, responseHandler(rw.handleRebalancedInfo))
	http.HandleFunc(RBResetControl, responseHandler(rw.handleReSetControlParam))
	http.HandleFunc(RBList, rw.handleRebalancedList)
	http.HandleFunc(ZoneUsageRatio, rw.handleZoneUsageRatio)
}
