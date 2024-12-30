package service

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/cubefs/cubefs/console/cutil"
	"github.com/cubefs/cubefs/console/model"
	"github.com/cubefs/cubefs/console/proto"
	"github.com/cubefs/cubefs/sdk/graphql/client"
	"github.com/samsarahq/thunder/graphql"
	"github.com/samsarahq/thunder/graphql/schemabuilder"
)

type MonitorService struct {
	monitorAddr      string
	monitorCluster   string
	sreDomainName    string // 暂留
	dataExporterPort string
	metaExporterPort string

	masterClients map[string]*client.MasterGClient
}

func NewMonitorService(clusters []*model.ConsoleCluster) *MonitorService {
	return &MonitorService{
		monitorAddr:      cutil.Global_CFG.MonitorAddr,
		monitorCluster:   cutil.Global_CFG.MonitorCluster,
		sreDomainName:    cutil.Global_CFG.SreDomainName,
		dataExporterPort: cutil.Global_CFG.DataExporterPort,
		metaExporterPort: cutil.Global_CFG.MetaExporterPort,
		masterClients: func(clusters []*model.ConsoleCluster) map[string]*client.MasterGClient {
			res := make(map[string]*client.MasterGClient, 0)
			for _, cluster := range clusters {
				res[cluster.ClusterName] = client.NewMasterGClient(strings.Split(cluster.MasterAddrs, ","), cluster.IsRelease)
			}
			return res
		}(clusters),
	}
}
func (ms *MonitorService) SType() proto.ServiceType {
	return proto.MonitorService
}

func (ms *MonitorService) Schema() *graphql.Schema {
	schema := schemabuilder.NewSchema()
	query := schema.Query()
	query.FieldFunc("Query", ms.Query)
	query.FieldFunc("RangeQuery", ms.RangeQuery)
	query.FieldFunc("RangeQueryURL", ms.RangeQueryURL)
	query.FieldFunc("_empty", ms.empty)
	query.FieldFunc("FuseClientList", ms.FuseClientList)
	query.FieldFunc("VersionCheck", ms.VersionCheck)
	// todo:	1.  磁盘监控： 磁盘使用率list  磁盘监控：慢盘 繁忙80% 高io等待
	//			2.  网络监控： 节点list  流入>80% 流出>80% 重传高
	//			3.	cpu监控： top30
	//			4.  内存监控： top30
	query.FieldFunc("systemMetrics", ms.getSystemMetrics)
	query.FieldFunc("netMetrics", ms.getNetMetrics)
	query.FieldFunc("diskMetrics", ms.getDiskMetrics)
	// 慢盘 磁盘繁忙程度 >90%  io等待时间 > N 单位：ms
	query.FieldFunc("abnormalDisk", ms.getHighBusyDisk)
	query.FieldFunc("awaitDiskMs", ms.getHighAwaitDisk)
	return schema.MustBuild()
}

func (ms *MonitorService) DoXbpApply(apply *model.XbpApplyInfo) error {
	return nil
}

func (ms *MonitorService) empty(ctx context.Context, args struct {
	Empty bool
}) bool {
	return args.Empty
}

type MachineVersion struct {
	IP           string
	VersionValue *proto.VersionValue
	Message      string
}

func ErrMachineVersion(ip string, model string, err error) *MachineVersion {
	return &MachineVersion{
		IP: ip,
		VersionValue: &proto.VersionValue{
			Model: model,
		},
		Message: err.Error(),
	}
}

func (ms *MonitorService) VersionCheck(ctx context.Context, args struct{}) ([]*MachineVersion, error) {
	var result []*MachineVersion

	vi := proto.MakeVersion("console")
	result = append(result, &MachineVersion{
		IP:           "self",
		VersionValue: &vi,
		Message:      "success",
	})

	mc := ms.masterClients[cutil.GlobalCluster]
	query, err := mc.Query(ctx, proto.AdminClusterAPI, client.NewRequest(ctx, `{
		clusterView{
			dataNodes{
				addr
			},
			metaNodes{
				addr
			}
		}
	}`))

	if err != nil {
		return nil, err
	}
	for _, m := range mc.GetMasterAddrs() {
		ip := strings.Split(m, ":")[0]
		get, err := http.Get(fmt.Sprintf("http://%s/version", m))
		if err != nil {
			result = append(result, ErrMachineVersion(ip, "master", err))
			continue
		}

		all, err := ioutil.ReadAll(get.Body)
		if err != nil {
			result = append(result, ErrMachineVersion(ip, "master", err))
			continue
		}

		vi := &proto.VersionValue{}

		if err := json.Unmarshal(all, vi); err != nil {
			result = append(result, ErrMachineVersion(ip, "master", err))
			continue
		}

		result = append(result, &MachineVersion{
			IP:           ip,
			VersionValue: vi,
			Message:      "success",
		})
	}

	dList := query.GetValue("clusterView", "dataNodes").([]interface{})
	for _, d := range dList {
		ip := strings.Split(d.(map[string]interface{})["addr"].(string), ":")[0]
		get, err := http.Get(fmt.Sprintf("http://%s:%s/version", ip, ms.dataExporterPort))
		if err != nil {
			result = append(result, ErrMachineVersion(ip, "data", err))
			continue
		}

		all, err := ioutil.ReadAll(get.Body)
		if err != nil {
			result = append(result, ErrMachineVersion(ip, "data", err))
			continue
		}

		vi := &proto.VersionValue{}

		if err := json.Unmarshal(all, vi); err != nil {
			result = append(result, ErrMachineVersion(ip, "data", err))
			continue
		}

		result = append(result, &MachineVersion{
			IP:           ip,
			VersionValue: vi,
			Message:      "success",
		})
	}

	mList := query.GetValue("clusterView", "metaNodes").([]interface{})
	for _, m := range mList {
		ip := strings.Split(m.(map[string]interface{})["addr"].(string), ":")[0]
		get, err := http.Get(fmt.Sprintf("http://%s:%s/version", ip, ms.metaExporterPort))
		if err != nil {
			result = append(result, ErrMachineVersion(ip, "meta", err))
			continue
		}

		all, err := ioutil.ReadAll(get.Body)
		if err != nil {
			result = append(result, ErrMachineVersion(ip, "meta", err))
			continue
		}

		vi := &proto.VersionValue{}

		if err := json.Unmarshal(all, vi); err != nil {
			result = append(result, ErrMachineVersion(ip, "meta", err))
			continue
		}

		result = append(result, &MachineVersion{
			IP:           ip,
			VersionValue: vi,
			Message:      "success",
		})
	}

	return result, nil
}

func (ms *MonitorService) RangeQuery(ctx context.Context, args struct {
	Query string
	Start uint32
	End   uint32
	Step  uint32
}) (string, error) {

	args.Query = strings.ReplaceAll(args.Query, "$cluster", ms.monitorCluster)

	param := url.Values{}
	param.Set("query", args.Query)
	param.Set("start", strconv.Itoa(int(args.Start)))
	param.Set("end", strconv.Itoa(int(args.End)))
	param.Set("step", strconv.Itoa(int(args.Step)))

	resp, err := http.DefaultClient.Get(ms.monitorAddr + "/api/v1/query_range?" + param.Encode())

	if err != nil {
		return "", err
	}

	b, err := ioutil.ReadAll(resp.Body)

	return string(b), nil
}

func (ms *MonitorService) RangeQueryURL(ctx context.Context, args struct {
	Query string
	Start uint32
	End   uint32
	Step  uint32
}) (string, error) {

	_, _, err := permissions(ctx, ADMIN)
	if err != nil {
		return "", err
	}

	args.Query = strings.ReplaceAll(args.Query, "$cluster", ms.monitorCluster)

	param := url.Values{}
	param.Set("query", args.Query)
	param.Set("start", strconv.Itoa(int(args.Start)))
	param.Set("end", strconv.Itoa(int(args.End)))
	param.Set("step", strconv.Itoa(int(args.Step)))

	return ms.monitorAddr + "/api/v1/query_range?" + param.Encode(), nil
}

func (ms *MonitorService) Query(ctx context.Context, args struct {
	Query string
}) (string, error) {

	_, _, err := permissions(ctx, ADMIN)
	if err != nil {
		return "", err
	}

	args.Query = strings.ReplaceAll(args.Query, "$cluster", ms.monitorCluster)

	param := url.Values{}
	param.Set("query", args.Query)

	resp, err := http.DefaultClient.Get(ms.monitorAddr + "/api/v1/query?" + param.Encode())

	if err != nil {
		return "", err
	}

	b, err := ioutil.ReadAll(resp.Body)

	return string(b), nil
}

type FuseClient struct {
	Name     string `json:"__name__"`
	App      string `json:"app"`
	Cluster  string `json:"cluster"`
	Hostip   string `json:"hostip"`
	Instance string `json:"instance"`
	Job      string `json:"job"`
	Monitor  string `json:"monitor"`
	Role     string `json:"role"`
	Service  string `json:"service"`
}

type FuseRecord struct {
	Metric FuseClient `json:"metric"`
	Value  float64    `json:"value"`
}

func (ms *MonitorService) FuseClientList(ctx context.Context, args struct{}) ([]*FuseRecord, error) {

	_, _, err := permissions(ctx, ADMIN)
	if err != nil {
		return nil, err
	}

	query := `up{app="$app", role="fuseclient", cluster="$cluster"}`
	query = strings.ReplaceAll(query, "$cluster", ms.monitorCluster)

	param := url.Values{}
	param.Set("query", query)
	param.Set("time", strconv.Itoa(int(time.Now().Unix())))

	resp, err := http.DefaultClient.Get(ms.monitorAddr + "/api/v1/query?" + param.Encode())
	if err != nil {
		return nil, err
	}

	b, err := ioutil.ReadAll(resp.Body)

	result := struct {
		Data struct {
			Result []*struct {
				Metric FuseClient    `json:"metric"`
				Value  []interface{} `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}{}
	if err := json.Unmarshal(b, &result); err != nil {
		return nil, err
	}

	frs := make([]*FuseRecord, 0, len(result.Data.Result))

	for _, r := range result.Data.Result {
		frs = append(frs, &FuseRecord{
			Metric: r.Metric,
			Value:  r.Value[0].(float64),
		})
	}

	return frs, nil
}

func (ms *MonitorService) getDiskMetrics(ctx context.Context, args struct {
	Cluster    string
	Role       string
	DataCenter string
	Zone       string
	Ip         string
	Interval   int32
	//Start      string
	//End        string
}) (*proto.DiskMetricsResp, error) {
	var result = new(proto.DiskMetricsResp)
	var url = fmt.Sprintf("http://%s%s", ms.sreDomainName, proto.MonitorOsPath)
	var req = cutil.NewAPIRequest(http.MethodPost, url)
	r := &proto.MetricsRequest{
		ClusterName:    args.Cluster,
		RoleName:       args.Role,
		DataCenterName: args.DataCenter,
		ZoneName:       args.Zone,
		IpAddr:         args.Ip,
		IntervalType:   int(args.Interval),
		MetricType:     int(proto.DiskMetrics),
	}
	data, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	req.AddBody(data)
	resp, err := cutil.SendSimpleRequest(req, false)
	if err != nil {
		return nil, err
	}
	var metrics map[string][]*proto.MetricInfo
	if err = json.Unmarshal(resp, &metrics); err != nil {
		return nil, err
	}
	for key, metric := range metrics {
		switch key {
		case "min_disk_busy_percent":
			result.DiskBusyPercentMinute = metric
		case "min_fs_usage_percent":
			result.DiskUsagePercentMinute = metric
		default:
		}
	}
	return result, nil
}

func (ms *MonitorService) getNetMetrics(ctx context.Context, args struct {
	Cluster    string
	Role       string
	DataCenter string
	Zone       string
	Ip         string
	Interval   int32
	Start      string
	End        string
}) (*proto.NetMetricsResp, error) {
	var result = new(proto.NetMetricsResp)
	var url = fmt.Sprintf("http://%s%s", ms.sreDomainName, proto.MonitorOsPath)
	var req = cutil.NewAPIRequest(http.MethodPost, url)
	r := &proto.MetricsRequest{
		ClusterName:    args.Cluster,
		RoleName:       args.Role,
		DataCenterName: args.DataCenter,
		ZoneName:       args.Zone,
		IpAddr:         args.Ip,
		IntervalType:   int(args.Interval),
		MetricType:     int(proto.NetMetrics),
		Start:          args.Start,
		End:            args.End,
	}
	data, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	req.AddBody(data)
	resp, err := cutil.SendSimpleRequest(req, false)
	if err != nil {
		return nil, err
	}
	var metrics map[string][]*proto.MetricInfo
	if err = json.Unmarshal(resp, &metrics); err != nil {
		return nil, err
	}
	for key, metric := range metrics {
		switch key {
		case "min_net_dev_rx_bytes":
			result.NetRXBytesMinute = metric
		case "min_net_dev_tx_bytes":
			result.NetTXBytesMinute = metric
		case "min_tcp_retrans_count":
			result.TcpRetransCountMinute = metric
		default:
		}
	}
	return result, nil
}

func (ms *MonitorService) getSystemMetrics(ctx context.Context, args struct {
	Cluster    string
	Role       string
	DataCenter string // all
	Zone       string // all
	Ip         string
	Interval   int32  // 2-10分钟；3-1小时；4-1天
	Start      string // 20060102150405 不填就是空串“”
	End        string
}) (*proto.SystemMetricsResp, error) {
	var result = new(proto.SystemMetricsResp)
	var url = fmt.Sprintf("http://%s%s", ms.sreDomainName, proto.MonitorOsPath)
	var req = cutil.NewAPIRequest(http.MethodPost, url)
	r := &proto.MetricsRequest{
		ClusterName:    args.Cluster,
		RoleName:       args.Role,
		DataCenterName: args.DataCenter,
		ZoneName:       args.Zone,
		IpAddr:         args.Ip,
		IntervalType:   int(args.Interval),
		MetricType:     int(proto.SystemMetrics),
		Start:          args.Start,
		End:            args.End,
	}
	data, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	req.AddBody(data)
	resp, err := cutil.SendSimpleRequest(req, false)
	if err != nil {
		return nil, err
	}
	var metrics map[string][]*proto.MetricInfo
	if err = json.Unmarshal(resp, &metrics); err != nil {
		return nil, err
	}
	for key, metric := range metrics {
		switch key {
		case "min_cpu_usage_percent":
			result.CpuUsagePercentMinute = metric
		case "min_mem_usage_percent":
			result.MemUsagePercentMinute = metric
		default:
		}
	}
	return result, nil
}

// 磁盘80%, 且结果没有具体的百分比
func (ms *MonitorService) getAbnormalDiskMetrics(ctx context.Context, args struct {
	Cluster string
	Mode    int32
	Types   int32
	Start   string // 20060102150405
	End     string
}) ([]*model.DiskAnalysisResult, error) {
	var result []*model.DiskAnalysisResult
	var url = fmt.Sprintf("http://%s%s", ms.sreDomainName, proto.MonitorDiskAnalysisPath)
	var req = cutil.NewAPIRequest(http.MethodGet, url)
	r := &proto.DiskAnalysisRequest{
		ClusterName: args.Cluster,
		Mode:        proto.ToAnalysisMode(int(args.Mode)).String(),
		QueryTypes:  proto.ToQueryType(int(args.Types)).String(),
		StartTime:   args.Start,
		Format:      true,
		EndTime:     args.End,
	}
	data, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	req.AddBody(data)
	resp, err := cutil.SendSimpleRequest(req, false)
	if err != nil {
		return nil, err
	}
	if err = json.Unmarshal(resp, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// 磁盘90%以上，没有zone信息
func (ms *MonitorService) getHighBusyDisk(ctx context.Context, args struct {
	Cluster  string
	Role     string
	Zone     string
	Interval int32
}) (*proto.DiskMetricsResp, error) {
	var result = new(proto.DiskMetricsResp)
	var url = fmt.Sprintf("http://%s%s", ms.sreDomainName, proto.MonitorOsPath)
	var req = cutil.NewAPIRequest(http.MethodPost, url)
	r := &proto.MetricsRequest{
		ClusterName:    args.Cluster,
		RoleName:       args.Role,
		DataCenterName: "all",
		ZoneName:       args.Zone,
		IntervalType:   int(args.Interval),
		MetricType:     int(proto.DiskMetrics),
		Range:          "0,90",
	}
	data, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	req.AddBody(data)
	resp, err := cutil.SendSimpleRequest(req, false)
	if err != nil {
		return nil, err
	}
	var metrics map[string][]*proto.MetricInfo
	if err = json.Unmarshal(resp, &metrics); err != nil {
		return nil, err
	}
	for key, metric := range metrics {
		switch key {
		case "min_disk_busy_percent":
			result.DiskBusyPercentMinute = metric
		case "min_fs_usage_percent":
			result.DiskUsagePercentMinute = metric
		default:
		}
	}
	return result, nil
}

// 高io等待
func (ms *MonitorService) getHighAwaitDisk(ctx context.Context, args struct {
	Cluster string
	Start   string // 不指定时间为“”
	End     string // 默认显示最近3小时(compact)
	Time    int32  // 单位：ms
}) ([]*model.DiskAnalysisResult, error) {
	var (
		result []*model.DiskAnalysisResult
		err    error
	)
	if args.End == "" || args.Start == "" {
		args.End = time.Now().Format(proto.TimeFormatCompact)
		args.Start = time.Now().Add(time.Duration(-3) * time.Hour).Format(proto.TimeFormatCompact)
	}
	model := model.DiskAnalysisResult{}
	if result, err = model.LoadHighAwaitDisk(args.Cluster, args.Start, args.End, int(args.Time)); err != nil {
		return nil, err
	}
	return result, nil
}
