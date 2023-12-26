package rebalance

import (
	"encoding/json"
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

type ReBalanceWorker struct {
	worker.BaseWorker
	mcwRWMutex       sync.RWMutex
	reBalanceCtrlMap sync.Map
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
	return
}

func (rw *ReBalanceWorker) loadInRunningRebalanced() (err error) {
	var rInfos []*RebalancedInfoTable
	if rInfos, err = rw.GetRebalancedInfoByStatus(StatusRunning); err != nil {
		return
	}
	for _, info := range rInfos {
		err = rw.ReBalanceStart(info.Host, info.ZoneName, info.HighRatio, info.LowRatio, info.GoalRatio, info.MaxBatchCount, info.MigrateLimitPerDisk)
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
