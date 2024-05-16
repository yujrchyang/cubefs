package migration

import (
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/config"
)

type WorkerManager struct {
	workers []*Worker
}

func NewWorkerManager() (wm *WorkerManager) {
	wm = &WorkerManager{}
	return
}

func (wm *WorkerManager) InitWorkers(cfg *config.Config) (err error) {
	compactWorker := NewWorker(proto.WorkerTypeCompact)
	wm.workers = append(wm.workers, compactWorker)

	fileMigWorker := NewWorker(proto.WorkerTypeInodeMigration)
	wm.workers = append(wm.workers, fileMigWorker)
	err = wm.startWorkers(cfg)
	return
}

func (wm *WorkerManager) startWorkers(cfg *config.Config) (err error) {
	for _, worker := range wm.workers {
		err = worker.Start(cfg)
		if err != nil {
			return
		}
	}
	return
}

func (wm *WorkerManager) Shutdown() {
	for _, worker := range wm.workers {
		worker.Shutdown()
	}
}
