package mdck

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
	CheckRuleTableName = "mdck_check_rules"
)


type MetaDataCheckTaskSchedule struct {
	sync.RWMutex
	worker.BaseWorker
	port          string
	masterAddr    map[string][]string
	mcw           map[string]*master.MasterClient
	storeTaskFunc func(workerType proto.WorkerType, clusterName string, task *proto.Task)
	mcwRWMutex    sync.RWMutex
}

func NewMetaDataCheckTaskSchedule(cfg *config.Config, storeFunc func(workerType proto.WorkerType, clusterName string,
	task *proto.Task)) (mdckTaskSchedule *MetaDataCheckTaskSchedule, err error) {
	mdckTaskSchedule = &MetaDataCheckTaskSchedule{}
	if err = mdckTaskSchedule.parseConfig(cfg); err != nil {
		log.LogErrorf("[NewMetaDataCheckTaskSchedule] parse config info failed, error(%v)", err)
		return
	}
	if err = mdckTaskSchedule.initMetaDataCheckTaskScheduler(); err != nil {
		log.LogErrorf("[NewMetaDataCheckTaskSchedule] init meta data check task schedule failed, error(%v)", err)
		return
	}
	mdckTaskSchedule.storeTaskFunc = storeFunc
	return
}

func (mdckTaskSchedule *MetaDataCheckTaskSchedule) parseConfig(cfg *config.Config) (err error) {
	err = mdckTaskSchedule.ParseBaseConfig(cfg)
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
	mdckTaskSchedule.masterAddr = masters
	mdckTaskSchedule.port = mdckTaskSchedule.Port
	return
}

func (mdckTaskSchedule *MetaDataCheckTaskSchedule) initMetaDataCheckTaskScheduler() (err error) {
	mdckTaskSchedule.WorkerType = proto.WorkerTypeMetaDataCrcCheck
	mdckTaskSchedule.TaskChan = make(chan *proto.Task, worker.DefaultTaskChanLength)

	mdckTaskSchedule.mcw = make(map[string]*master.MasterClient)
	for cluster, addresses := range mdckTaskSchedule.masterAddr {
		isDBBack := false
		for _, addr := range addresses {
			if strings.Contains(addr, "cn.chubaofs-seqwrite") || strings.Contains(addr, "dbbak") {
				isDBBack = true
				break
			}
		}
		if isDBBack {
			mdckTaskSchedule.mcw[cluster] = master.NewMasterClientForDbBackCluster(addresses, false)
		} else {
			mdckTaskSchedule.mcw[cluster] = master.NewMasterClient(addresses, false)
		}
	}

	if err = mysql.InitMysqlClient(mdckTaskSchedule.MysqlConfig); err != nil {
		log.LogErrorf("[initMetaDataCheckTaskScheduler] init mysql client failed, error(%v)", err)
		return
	}
	return
}

func (mdckTaskSchedule *MetaDataCheckTaskSchedule) GetCreatorDuration() int {
	return mdckTaskSchedule.WorkerConfig.TaskCreatePeriod
}

func (mdckTaskSchedule *MetaDataCheckTaskSchedule) CreateTask(clusterID string, taskNum int64, runningTasks []*proto.Task, wns []*proto.WorkerNode) (newTasks []*proto.Task, err error) {
	mdckTaskSchedule.RLock()
	defer mdckTaskSchedule.RUnlock()

	_, ok := mdckTaskSchedule.mcw[clusterID]
	if !ok {
		log.LogInfof("MetaDataCheckTaskSchedule CreateTask:cluster %s not exist", clusterID)
		return
	}
	masterClient := mdckTaskSchedule.mcw[clusterID]
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
		newTask := proto.NewDataTask(proto.WorkerTypeMetaDataCrcCheck, clusterID, volName, 0, 0, "")
		if alreadyExist, _, _ := mdckTaskSchedule.ContainTask(newTask, runningTasks); alreadyExist {
			continue
		}

		latestFinishedTime := mdckTaskSchedule.GetLatestFinishedTime(newTask)
		if time.Since(latestFinishedTime) < DefaultCheckInterval {
			continue
		}

		var taskId uint64
		if taskId, err = mdckTaskSchedule.AddTask(newTask); err != nil {
			log.LogErrorf("MetaDataCheckTaskSchedule CreateTask AddTask to database failed, cluster(%v), volume(%v), task(%v), err(%v)",
				clusterID, volName, newTask, err)
			continue
		}

		newTask.TaskId = taskId
		mdckTaskSchedule.storeTaskFunc(mdckTaskSchedule.WorkerType, clusterID, newTask)
	}
	return
}
