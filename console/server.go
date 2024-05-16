package console

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"regexp"
	"strings"

	"github.com/cubefs/cubefs/console/cutil"
	"github.com/cubefs/cubefs/console/model"
	"github.com/cubefs/cubefs/console/proto"
	"github.com/cubefs/cubefs/console/service"
	"github.com/cubefs/cubefs/util/config"
	"github.com/cubefs/cubefs/util/log"
	"github.com/gorilla/mux"
	"github.com/samsarahq/thunder/graphql"
	"github.com/samsarahq/thunder/graphql/introspection"
)

type SchemaService interface {
	Schema() *graphql.Schema
}

type ConsoleNode struct {
	server *mux.Router
	stopC  chan bool

	clusters       []*model.ConsoleCluster
	registeredPath map[string]bool
}

func NewServer() *ConsoleNode {
	return &ConsoleNode{}
}

func (c *ConsoleNode) Sync() {
	if err := http.ListenAndServe(fmt.Sprintf(":%s", cutil.Global_CFG.Listen), c.server); err != nil {
		log.LogErrorf("sync console: ListenAndServe err:[%s]", err.Error())
	}
}

func (c *ConsoleNode) Shutdown() {
	close(c.stopC)
}

func (c *ConsoleNode) Start(conf *config.Config) error {
	c.stopC = make(chan bool)

	err := cutil.InitConsoleConfig(conf)
	if err != nil {
		return fmt.Errorf("load console config err: %v", err)
	}
	if err = c.parseConfig(&cutil.Global_CFG); err != nil {
		return fmt.Errorf("parse console config err: %v", err)
	}

	table := model.ConsoleCluster{}
	c.clusters, err = table.LoadConsoleClusterList("")
	if err != nil {
		log.LogErrorf("console start: query console cluster failed: %v", err)
		return err
	}
	log.LogDebugf("console clusters: %v", c.clusters)

	c.server = mux.NewRouter()
	c.registerSchema()
	c.registerHttpHandler()
	c.registerStaticResource()

	go c.InitCronTask()
	return nil
}

func (c *ConsoleNode) registerSchema() {
	loginService := service.NewLoginService(c.clusters)
	c.addHandle(proto.ConsoleLoginAPI, loginService.Schema(), loginService)

	monitorService := service.NewMonitorService(c.clusters)
	c.addHandle(proto.ConsoleMonitorAPI, monitorService.Schema(), monitorService)

	fileService := service.NewFileService(c.clusters)
	c.addHandle(proto.ConsoleFile, fileService.Schema(), fileService)
	c.addHandleFunc(c.registerURI(proto.ConsoleFileDown), fileService.DownFile)
	c.addHandleFunc(c.registerURI(proto.ConsoleFileUpload), fileService.UpLoadFile)

	volumeService := service.NewVolumeService(c.clusters)
	c.addHandle(proto.ConsoleVolume, volumeService.Schema(), volumeService)

	clusterService := service.NewClusterService(c.clusters)
	c.addHandle(proto.ConsoleCluster, clusterService.Schema(), clusterService)

	trafficService := service.NewTrafficService(c.clusters, c.stopC)
	c.addHandle(proto.ConsoleTraffic, trafficService.Schema(), trafficService)

	cliService := service.NewCliService(c.clusters)
	c.addHandle(proto.ConsoleCli, cliService.Schema(), cliService)
	// todo: solveXbp 需要做成多个模块通用的，不仅是cli, 需要抽象
	c.addHandleFunc(c.registerURI(proto.XBPCallBackPath), cliService.SolveXbpCallBack)

	scheduleService := service.NewScheduleTaskService(c.clusters)
	c.addHandle(proto.ConsoleSchedule, scheduleService.Schema(), scheduleService)
}

func (c *ConsoleNode) registerHttpHandler() {
	c.server.HandleFunc(c.registerURI(proto.ConsoleIQL), cutil.IQLFun)
	c.server.HandleFunc(c.registerURI(proto.VersionPath), versionHandler)
}

func (c *ConsoleNode) registerStaticResource() {
	indexPaths := []string{"overview", "login", "userDetails", "servers",
		"serverList", "dashboard", "volumeList", "volumeDetail", "fileList", "operations",
		"alarm", "authorization"}

	for _, path := range indexPaths {
		c.server.HandleFunc("/"+path, indexer).Methods("GET")
	}
	if cutil.Global_CFG.StaticResource {
		c.server.PathPrefix("/").HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			path := r.URL.Path
			if path == "/" || strings.HasPrefix(path, "/css/") || strings.HasPrefix(path, "/fonts/") ||
				strings.HasPrefix(path, "/img/") || strings.HasPrefix(path, "/js/") {
				http.FileServer(Assets).ServeHTTP(w, r)
			} else {
				indexer(w, r)
			}
		})
	}
}

func (c *ConsoleNode) InitCronTask() {
	if cutil.Global_CFG.CronTaskOn {
		service.InitTrafficCronTask(c.stopC)
	}
}

func (c *ConsoleNode) parseConfig(cfg *cutil.ConsoleConfig) (err error) {
	if len(cfg.Listen) == 0 {
		cfg.Listen = "80"
	}
	if match := regexp.MustCompile("^(\\d)+$").MatchString(cfg.Listen); !match {
		return cutil.NewInvalidConfiguration("listen port", cfg.Listen)
	}
	if len(cfg.LocalIP) == 0 || !validLocalIP(cfg.LocalIP) {
		return cutil.NewInvalidConfiguration("localIP", cfg.LocalIP)
	}

	cutil.CONSOLE_DB, err = cutil.OpenGorm(&cfg.ConsoleDBConfig)
	if err != nil {
		return fmt.Errorf("open dbHandler@%v failed, err: %v", cfg.ConsoleDBConfig.Database, err)
	}
	sreDBConfig := cfg.ConsoleDBConfig
	sreDBConfig.Database = "storage_sre"
	cutil.SRE_DB, err = cutil.OpenGorm(&sreDBConfig)
	if err != nil {
		return fmt.Errorf("open dbHandler@%v failed, err: %v", sreDBConfig.Database, err)
	}
	cutil.MYSQL_DB, err = cutil.OpenGorm(&cfg.MysqlConfig)
	if err != nil {
		return fmt.Errorf("open dbHandler@%v failed, err: %v", cfg.MysqlConfig.Database, err)
	}

	if cfg.ClickHouseConfig.Host == "" || cfg.ClickHouseConfig.User == "" || cfg.ClickHouseConfig.Password == "" {
		return cutil.NewInvalidConfiguration("ClickHouse config", nil)
	}
	cutil.ClickHouseDBHostAddr = cfg.ClickHouseConfig.Host
	cutil.ClickHouseDBROnlyUser = cfg.ClickHouseConfig.User
	cutil.ClickHouseDBPassword = cfg.ClickHouseConfig.Password

	//单点登陆
	if cfg.IsIntranet {
		if cfg.SSOConfig.Domain == "" || cfg.SSOConfig.AppCode == "" || cfg.SSOConfig.Token == "" {
			return fmt.Errorf("intranet env need to configure \"ssoAppCode\" and \"ssoDomain\" and \"ssoToken\"")
		}
	}
	cutil.CFS_S3, err = cutil.InitConsoleS3(&cfg.CFSS3Config)
	if err != nil {
		return fmt.Errorf("InitConsoleS3 failed: err(%v)", err)
	}
	return
}

func (c *ConsoleNode) addHandleFunc(uri string, f func(w http.ResponseWriter, r *http.Request) error) {
	c.server.HandleFunc(uri, func(writer http.ResponseWriter, request *http.Request) {
		if err := f(writer, request); err != nil {
			proto.BuildFailureResp(writer, http.StatusInternalServerError, err.Error())
		}
	})
}

func (c *ConsoleNode) addHandle(model string, schema *graphql.Schema, service interface{}) {
	c.registerURI(model)
	introspection.AddIntrospectionToSchema(schema)
	c.server.Handle(model, cutil.HTTPHandler(schema)).Methods("POST")
}

func (c *ConsoleNode) registerURI(path string) string {
	if c.registeredPath == nil {
		c.registeredPath = make(map[string]bool)
	}
	c.registeredPath[path] = true
	return path
}

func indexer(w http.ResponseWriter, r *http.Request) {
	file, err := Assets.Open("/index.html")
	if err != nil {
		proto.BuildFailureResp(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer file.Close()

	if n, e := io.Copy(w, file); n == 0 && e != nil {
		proto.BuildFailureResp(w, http.StatusInternalServerError, e.Error())
	}
}

func versionHandler(w http.ResponseWriter, r *http.Request) {
	log.LogIfNotNil(r.ParseForm())
	addr := r.Form["addr"]
	version := proto.VersionValue{
		Model: "error",
	}
	if len(addr) > 0 {
		if get, err := http.Get("http://" + addr[0] + "/version"); err != nil {
			version.CommitID = err.Error()
		} else {
			if all, err := ioutil.ReadAll(get.Body); err != nil {
				version.CommitID = err.Error()
			} else {
				if err := json.Unmarshal(all, &version); err != nil {
					version.CommitID = err.Error()
				}
			}
		}
	} else {
		version = proto.MakeVersion("console")
	}

	marshal, _ := json.Marshal(version)
	if _, err := w.Write(marshal); err != nil {
		log.LogErrorf("write version has err:[%s]", err.Error())
	}
}

func validLocalIP(host string) bool {
	if net.ParseIP(host) == nil {
		if _, err := net.LookupHost(host); err != nil {
			return false
		}
	}
	return true
}
