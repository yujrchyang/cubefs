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
	"strings"
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

func verifyCluster(cluster string) error {
	switch cluster {
	case SPARK, ELASTICDB, TEST, TestES:
		return nil
	default:
		return fmt.Errorf("cluster:%v Not supported", cluster)
	}
}

func (rw *ReBalanceWorker) getClusterHost(cluster string) (host string) {
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
	case ELASTICDB:
		host = "cn.elasticdb.jd.local"
	case CFS_AMS_MCA:
		host = "nl.chubaofs.jd.local"
	case OCHAMA:
		host = "nl.chubaofs.ochama.com"
	case TEST:
		host = "10.179.20.34:80"
	case TestES:
		host = "172.21.138.73:80"
	}
	return
}

func (rw *ReBalanceWorker) getDataNodePProfPort(cluster string) (port string) {
	rw.clusterConfigMap.Range(func(key, value interface{}) bool {
		clusterConfig := value.(*ClusterConfig)
		if clusterConfig.ClusterName == cluster {
			port = fmt.Sprintf("%v", clusterConfig.DataProfPort)
			return false
		}
		return true
	})
	if port != "" {
		return
	}

	switch cluster {
	case SPARK, DBBAK, ELASTICDB, CFS_AMS_MCA, OCHAMA:
		port = "6001"
	case TEST:
		port = "17031"
	case TestES:
		port = "17031"
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
		if info.DstMetaNodePartitionMaxCount == 0 || info.DstMetaNodePartitionMaxCount > defaultDstMetaNodePartitionMaxCount {
			info.DstMetaNodePartitionMaxCount = defaultDstMetaNodePartitionMaxCount
		}
		var reTaskID uint64
		reTaskID, e1 := rw.restartRunningTask(info)
		if e1 != nil || reTaskID != info.ID {
			log.LogErrorf("restart taskID(%v) failed: err(%v) re_taskID(%v)", info.ID, e1, reTaskID)
			continue
		}
		log.LogInfof("restart taskID(%v) success", info.ID)
	}
	return
}

func (rw *ReBalanceWorker) restartRunningTask(info *RebalancedInfoTable) (taskID uint64, err error) {
	isRestart := true

	var ctrl *ZoneReBalanceController
	if info.TaskType == ZoneAutoReBalance {
		ctrl, err = rw.newZoneCtrl(info.Cluster, info.ZoneName, info.RType, info.MaxBatchCount, info.HighRatio, info.LowRatio, info.GoalRatio,
			info.MigrateLimitPerDisk, info.DstMetaNodePartitionMaxCount, isRestart)
		if err != nil {
			rw.stopRebalanced(ctrl.Id, false)
			return
		}
	}
	if info.TaskType == NodesMigrate {
		ctrl, err = rw.newNodeMigrationCtrl(info.Cluster, info.RType, info.MaxBatchCount, info.DstMetaNodePartitionMaxCount,
			strings.Split(info.SrcNodes, ","), strings.Split(info.DstNodes, ","), isRestart, info.ID)
		if err != nil {
			rw.stopRebalanced(ctrl.Id, false)
			return
		}
	}
	taskID = ctrl.Id
	err = ctrl.ReBalanceStart()
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
	http.HandleFunc(RBList, pagingResponseHandler(rw.handleRebalancedList))
	http.HandleFunc(ZoneUsageRatio, rw.handleZoneUsageRatio)
	http.HandleFunc(RBRecordsQuery, pagingResponseHandler(rw.handleMigrateRecordsQuery))
}
