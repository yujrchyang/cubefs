package proto

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cubefs/cubefs/util/log"
	"net/http"
	"runtime"
	"strings"

	"github.com/cubefs/cubefs/proto"
)

const (
	VersionPath     = "/version"
	XBPCallBackPath = "/xbp" // 审批流回调地址

	//graphql master api
	AdminClusterAPI = "/api/cluster"

	//graphql console api
	ConsoleIQL              = "/iql"
	ConsoleLoginAPI         = "/login"
	ConsoleLogoutAPI        = "/logout"
	ConsoleFile             = "/file"
	ConsoleFileDown         = "/file/down"
	ConsoleFileUpload       = "/file/upload"
	ConsoleMonitorAPI       = "/monitor"
	ConsoleVolume           = "/console/volume"
	ConsoleVolumeClientList = "/console/volClientList"
	ConsoleCluster          = "/console/cluster"
	ConsoleMonitor          = "/console/monitor"
	ConsoleTraffic          = "/console/traffic"
	ConsoleSchedule         = "/console/schedule"
	ConsoleCli              = "/cli"
)

// todo: service type  -- 根据service type 获取对应的服务
type ServiceType int

const (
	LoginService ServiceType = iota
	MonitorService
	FileService
	VolumeService
	ClusterService
	TrafficService
	CliService
	ScheduleService
)

var (
	ErrUnSupportOperation = errors.New("unsupported operation for release_db cluster")
)

var (
	Version    = proto.BaseVersion
	CommitID   string
	BranchName string
	BuildTime  string
)

type VersionValue struct {
	Model      string
	Version    string
	CommitID   string
	BranchName string
	BuildTime  string
}

func MakeVersion(model string) VersionValue {
	return VersionValue{
		Model:      model,
		Version:    Version,
		CommitID:   CommitID,
		BranchName: BranchName,
		BuildTime:  fmt.Sprintf("%s %s %s %s", runtime.Version(), runtime.GOOS, runtime.GOARCH, BuildTime),
	}
}

func IsRelease(cluster string) bool {
	return strings.Compare(cluster, "cfs_dbBak") == 0 ||
		strings.Compare(cluster, "cfs_dbBack") == 0 || strings.Contains(cluster, "dbBack")
}

// todo: remove
type GeneralResp struct {
	Message string
	Code    int32
	Data    []byte
}

func BuildResponse(err error) *GeneralResp {
	resp := &GeneralResp{
		Code:    0,
		Message: "success!",
	}

	if err != nil {
		resp.Code = 1
		resp.Message = err.Error()
	}
	return resp
}

func Failure(msg string, code int32) *GeneralResp {
	return &GeneralResp{
		Message: msg,
		Code:    code,
	}
}

func BuildFailureResp(writer http.ResponseWriter, code int32, msg string) {
	writeResp(writer, Failure(msg, code))
}

func writeResp(w http.ResponseWriter, resp *GeneralResp) {
	data, err := json.Marshal(resp)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		data = []byte(err.Error())
	} else {
		w.WriteHeader(http.StatusOK)
	}
	w.Header().Set("Content-Type", "application/json")
	if _, err = w.Write(data); err != nil {
		log.LogErrorf("writeResp err: %v", err)
	}
}
