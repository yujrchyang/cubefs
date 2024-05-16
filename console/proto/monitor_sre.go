package proto

const (
	MonitorOsPath           = BaseCroup + "os"
	MonitorDiskAnalysisPath = BaseCroup + "getAbnormalDisks"
)

type MetricsType int

const (
	SystemMetrics MetricsType = iota
	NetMetrics
	DiskMetrics
)

type IntervalType int

const (
	_ IntervalType = iota
	v2
)

type AnalysisMode int

const (
	Normal AnalysisMode = iota
	Max
	Avg
)

func (mode AnalysisMode) String() string {
	switch mode {
	case Normal:
		return "normal"
	case Max:
		return "max"
	case Avg:
		return "avg"
	default:
		return ""
	}
}
func ToAnalysisMode(mode int) AnalysisMode {
	if mode == 0 {
		return Normal
	}
	if mode == 1 {
		return Max
	}
	if mode == 2 {
		return Avg
	}
	return Normal
}

type DiskQueryType int

const (
	Busy50 DiskQueryType = iota
	Busy80
	Await
)

func (query DiskQueryType) String() string {
	switch query {
	case Busy50:
		return "busy50"
	case Busy80:
		return "busy80"
	case Await:
		return "await"
	default:
		return ""
	}
}
func ToQueryType(query int) DiskQueryType {
	if query == 0 {
		return Busy50
	}
	if query == 1 {
		return Busy80
	}
	if query == 2 {
		// sre写死1s 不支持自定义
		return Await
	}
	return Busy80
}

type MetricInfo struct {
	MetricName string
	IPAddr     string
	Value      float64
	OtherInfo  string
}

// 统计指标： 操作系统，磁盘，网络
type MetricsRequest struct {
	ClusterName    string `json:"clusterName"`
	RoleName       string `json:"roleName"`   //角色名称 如果未选角色,填all即可
	DataCenterName string `json:"roomName"`   //机房名称 如果是全部,填all
	ZoneName       string `json:"zoneName"`   //zone名称
	IpAddr         string `json:"ipAddr"`     //如果未选择ip,此处填空串即可
	IntervalType   int    `json:"interval"`   //近10min 近1小时 近24小时 近3天
	MetricType     int    `json:"metricType"` //0/*系统指标*/  1/*网络指标*/  2/*磁盘指标*/
	Start          string `json:"start"`      // format: 20220113000000
	End            string `json:"end"`        // format: 20220113000000
	Range          string `json:"range"`      // 百分比范围
}

type DiskMetricsResp struct {
	// map[string][]*MetricInfo  minute
	DiskUsagePercentMinute []*MetricInfo
	DiskBusyPercentMinute  []*MetricInfo
}

type NetMetricsResp struct {
	NetRXBytesMinute      []*MetricInfo // 流入
	NetTXBytesMinute      []*MetricInfo // 流出
	TcpRetransCountMinute []*MetricInfo
}

type SystemMetricsResp struct {
	CpuUsagePercentMinute []*MetricInfo
	MemUsagePercentMinute []*MetricInfo
}

// 获取磁盘繁忙节点信息
type DiskAnalysisRequest struct {
	ClusterName string
	Mode        string // analysisMode: normal max avg
	QueryTypes  string // 磁盘繁忙程度： busy50 busy80 await
	StartTime   string // 20230718170000
	EndTime     string
	Format      bool // 不要二进制流 而是结构体形式返回
	Download    bool // csv
}
