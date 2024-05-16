package file

import (
	"fmt"
	"github.com/cubefs/cubefs/console/model"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util/exporter/backend/ump"
	"github.com/cubefs/cubefs/util/log"
	"runtime/debug"
	"strings"
	"sync"
)

const (
	MaxTaskCapacity = 10240
)

type TrashManager struct {
	metaProf     map[string]string // meta的pprof, 通过master metaNode获取
	masterClient map[string]*master.MasterClient
	volManager   map[string]*VolumeLoader
	taskChan     chan *TrashTask // 阻塞式清空回收站
	stopC        chan bool
	sync.RWMutex // volManager 的锁
}

func NewTrashManager(clusters []*model.ConsoleCluster) *TrashManager {
	trashManager := new(TrashManager)

	trashManager.stopC = make(chan bool, 0)
	trashManager.taskChan = make(chan *TrashTask, MaxTaskCapacity)
	trashManager.masterClient = make(map[string]*master.MasterClient, 0)
	trashManager.volManager = make(map[string]*VolumeLoader, 0)
	trashManager.metaProf = make(map[string]string)

	for _, cluster := range clusters {
		masterClient := master.NewMasterClient(strings.Split(cluster.MasterAddrs, ","), false)
		trashManager.masterClient[cluster.ClusterName] = masterClient
		trashManager.volManager[cluster.ClusterName] = NewVolumeLoader(cluster.ClusterName, strings.Split(cluster.MasterAddrs, ","))
		trashManager.metaProf[cluster.ClusterName] = cluster.MetaProf
	}

	go trashManager.taskReceiver()
	return trashManager
}

func (m *TrashManager) taskReceiver() {
	log.LogInfof("TrashManager-taskReceiver begin.")

	for {
		select {
		case t := <-m.taskChan:
			go func(t *TrashTask) {
				defer func() {
					msg := fmt.Sprintf("TrashManager: task: %v", t)
					if r := recover(); r != nil {
						var stack string
						stack = fmt.Sprintf(" %v :\n%s", r, string(debug.Stack()))
						ump.Alarm("cfs_console_trashManager", stack)
						log.LogCritical("%s%s\n", msg, stack)
					}
				}()
				switch t.TaskType {
				case CleanTrash:
					m.CleanTrashTaskHandler(t)
				}
			}(t)
		case <-m.stopC:
			log.LogInfof("TrashManager-taskReceiver stopped.")
			return
		}
	}
}

func (m *TrashManager) PutToTaskChan(task *TrashTask) {
	select {
	case m.taskChan <- task:
	default:
		log.LogWarnf("TrashManager: TrashTask chan is full")
	}
}
