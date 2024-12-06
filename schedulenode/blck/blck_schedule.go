package blck

import (
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/schedulenode/common"
	"github.com/cubefs/cubefs/schedulenode/worker"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/sdk/mysql"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/log"
	"strings"
	"sync"
	"time"
)

const (
	CheckRuleTableName = "blck_check_rules"
)

type BlockCheckTaskSchedule struct {
	sync.RWMutex
	worker.BaseWorker
	port          string
	masterAddr    map[string][]string
	mcw           map[string]*master.MasterClient
	storeTaskFunc func(workerType proto.WorkerType, clusterName string, task *proto.Task)
	mcwRWMutex    sync.RWMutex
}

func NewBlockCheckTaskSchedule(cfg *config.Config, storeFunc func(workerType proto.WorkerType, clusterName string,
	task *proto.Task)) (blckTaskSchedule *BlockCheckTaskSchedule, err error) {
	blckTaskSchedule = &BlockCheckTaskSchedule{}
	if err = blckTaskSchedule.parseConfig(cfg); err != nil {
		log.LogErrorf("[NewBlockCheckTaskSchedule] parse config info failed, error(%v)", err)
		return
	}
	if err = blckTaskSchedule.initBlockTaskScheduler(); err != nil {
		log.LogErrorf("[NewBlockCheckTaskSchedule] init compact worker failed, error(%v)", err)
		return
	}
	blckTaskSchedule.storeTaskFunc = storeFunc
	return
}

func (blckTaskSchedule *BlockCheckTaskSchedule) parseConfig(cfg *config.Config) (err error) {
	err = blckTaskSchedule.ParseBaseConfig(cfg)
	if err != nil {
		return
	}

	// parse cluster master address
	masters := make(map[string][]string)
	baseInfo := cfg.GetMap(config.ConfigKeyClusterAddr)
	for clusterName, value := range baseInfo {
		addresses := make([]string, 0)
		if valueSlice, ok := value.([]interface{}); ok {
			for _, item := range valueSlice {
				if addr, ok := item.(string); ok {
					addresses = append(addresses, addr)
				}
			}
		}
		masters[clusterName] = addresses
	}
	blckTaskSchedule.masterAddr = masters
	blckTaskSchedule.port = blckTaskSchedule.Port
	return
}

func (blckTaskSchedule *BlockCheckTaskSchedule) initBlockTaskScheduler() (err error) {
	blckTaskSchedule.WorkerType = proto.WorkerTypeBlockCheck
	blckTaskSchedule.TaskChan = make(chan *proto.Task, worker.DefaultTaskChanLength)

	blckTaskSchedule.mcw = make(map[string]*master.MasterClient)
	for cluster, addresses := range blckTaskSchedule.masterAddr {
		isDBBack := false
		for _, addr := range addresses {
			if strings.Contains(addr, "cn.chubaofs-seqwrite") || strings.Contains(addr, "dbbak") {
				isDBBack = true
				break
			}
		}
		if isDBBack {
			blckTaskSchedule.mcw[cluster] = master.NewMasterClientForDbBackCluster(addresses, false)
		} else {
			blckTaskSchedule.mcw[cluster] = master.NewMasterClient(addresses, false)
		}
		//blckTaskSchedule.mcw[cluster] = master.NewMasterClient(addresses, false)
	}

	if err = mysql.InitMysqlClient(blckTaskSchedule.MysqlConfig); err != nil {
		log.LogErrorf("[initBlockTaskScheduler] init mysql client failed, error(%v)", err)
		return
	}
	return
}

func (blckTaskSchedule *BlockCheckTaskSchedule) GetCreatorDuration() int {
	return blckTaskSchedule.WorkerConfig.TaskCreatePeriod
}

func isMetaOutVolume(volName string) bool {
	if volName == "jss-online" || volName == "jss-online-ssd" || volName == "ofw-cof-bak" ||
		volName == "ofw-cof" || volName == "ofw-cof-yc" {
		return true
	}
	return false
}

func (blckTaskSchedule *BlockCheckTaskSchedule) CreateTask(clusterID string, taskNum int64, runningTasks []*proto.Task, wns []*proto.WorkerNode) (newTasks []*proto.Task, err error) {
	blckTaskSchedule.RLock()
	defer blckTaskSchedule.RUnlock()

	_, ok := blckTaskSchedule.mcw[clusterID]
	if !ok {
		log.LogInfof("BlockCheckTaskSchedule CreateTask:cluster %s not exist", clusterID)
		return
	}
	masterClient := blckTaskSchedule.mcw[clusterID]

	var vols []*proto.VolInfo
	vols, err = masterClient.AdminAPI().ListVols("")
	if err != nil {
		return
	}

	var checkRules []*proto.CheckRule
	checkRules, err = mysql.SelectCheckRule(CheckRuleTableName, clusterID)
	if err != nil {
		return
	}

	var needCheckVols []string
	checkAll, checkVolumes, enableCheckOwners, disableCheckOwners, skipVolumes := common.ParseCheckAllRules(checkRules)
	if checkAll {
		for _, vol := range vols {
			if _, ok = skipVolumes[vol.Name]; ok {
				continue
			}
			if _, ok = disableCheckOwners[vol.Owner]; ok {
				continue
			}
			needCheckVols = append(needCheckVols, vol.Name)
		}
	} else {
		needCheckVols = append(needCheckVols, checkVolumes...)
		if len(enableCheckOwners) != 0 {
			for _, vol := range vols {
				if _, ok = enableCheckOwners[vol.Owner]; !ok {
					continue
				}
				needCheckVols = append(needCheckVols, vol.Name)
			}
		}
	}

	for _, volName := range needCheckVols {
		if volName == "" {
			continue
		}
		newTask := proto.NewDataTask(proto.WorkerTypeBlockCheck, clusterID, volName, 0, 0, "")
		if alreadyExist, _, _ := blckTaskSchedule.ContainTask(newTask, runningTasks); alreadyExist {
			continue
		}

		latestFinishedTime := blckTaskSchedule.GetLatestFinishedTime(newTask)
		if time.Since(latestFinishedTime) < DefaultCheckInterval {
			continue
		}

		var taskId uint64
		if taskId, err = blckTaskSchedule.AddTask(newTask); err != nil {
			log.LogErrorf("BlockCheckTaskSchedule CreateTask AddTask to database failed, cluster(%v), volume(%v), task(%v), err(%v)",
				clusterID, volName, newTask, err)
			continue
		}

		newTask.TaskId = taskId
		blckTaskSchedule.storeTaskFunc(blckTaskSchedule.WorkerType, clusterID, newTask)
	}
	return
}
