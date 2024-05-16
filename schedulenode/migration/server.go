package migration

import (
	"encoding/json"
	"github.com/cubefs/cubefs/cmd/common"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"net/http"
	"strconv"
)

type MigrationServer struct {
	wm      *WorkerManager
	Control common.Control
	stopC   chan struct{}
}

func NewDataMigrationServer() *MigrationServer {
	return &MigrationServer{}
}

func (dw *MigrationServer) Start(cfg *config.Config) (err error) {
	return dw.Control.Start(dw, cfg, doStart)
}

func doStart(s common.Server, cfg *config.Config) (err error) {
	dw, ok := s.(*MigrationServer)
	if !ok {
		err = errors.New("Invalid Node Type")
		return
	}
	dw.wm = NewWorkerManager()
	dw.stopC = make(chan struct{}, 0)
	if err = dw.startWorker(cfg); err != nil {
		return
	}
	dw.registerHandler()
	// init ump monitor and alarm module
	exporter.Init(exporter.NewOptionFromConfig(cfg).WithCluster(proto.RoleDataMigWorker).WithModule(proto.RoleDataMigWorker))
	return
}

func (dw *MigrationServer) startWorker(cfg *config.Config) (err error) {
	err = dw.wm.InitWorkers(cfg)
	return
}

func (dw *MigrationServer) Shutdown() {
	dw.Control.Shutdown(dw, doShutdown)
}

func doShutdown(s common.Server) {
	dw, ok := s.(*MigrationServer)
	if !ok {
		return
	}
	dw.wm.Shutdown()
	close(dw.stopC)
}

func (dw *MigrationServer) Sync() {
	dw.Control.Sync()
}

func (dw *MigrationServer) registerHandler() {
	http.HandleFunc(proto.VersionPath, func(w http.ResponseWriter, _ *http.Request) {
		version := proto.MakeVersion("dataMigration")
		marshal, _ := json.Marshal(version)
		if _, err := w.Write(marshal); err != nil {
			log.LogErrorf("write version has err:[%s]", err.Error())
		}
	})
	http.HandleFunc("/dataMig/status", dw.status)
}

func (dw *MigrationServer) status(w http.ResponseWriter, r *http.Request) {
	reply := []byte("ok")
	w.Header().Set("Content-Length", strconv.Itoa(len(reply)))
	if _, err := w.Write(reply); err != nil {
		log.LogErrorf("fail to write http reply[%s] len[%d].URL[%v],remoteAddr[%v] err:[%v]", string(reply), len(reply), r.URL, r.RemoteAddr, err)
	}
}
