package service

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/cubefs/cubefs/console/cutil"
	"github.com/cubefs/cubefs/console/model"
	cproto "github.com/cubefs/cubefs/console/proto"
	api "github.com/cubefs/cubefs/console/service/apiManager"
	flow "github.com/cubefs/cubefs/console/service/flowSchedule"
	"github.com/cubefs/cubefs/console/service/traffic"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/unit"
	"github.com/samsarahq/thunder/graphql"
	"github.com/samsarahq/thunder/graphql/schemabuilder"
)

type TrafficService struct {
	flowSchedule *flow.FlowSchedule
	api          *api.APIManager
	initOnce     sync.Once
	stopC        chan bool
}

func NewTrafficService(clusters []*model.ConsoleCluster, stopC chan bool) *TrafficService {
	ts := &TrafficService{
		flowSchedule: flow.NewFlowSchedule(cutil.Global_CFG.ClickHouseConfig.User, cutil.Global_CFG.ClickHouseConfig.Password, cutil.Global_CFG.ClickHouseConfig.Host),
		api:          api.NewAPIManager(clusters),
		stopC:        stopC,
	}
	return ts
}

func (ts *TrafficService) SType() cproto.ServiceType {
	return cproto.TrafficService
}

func (ts *TrafficService) Schema() *graphql.Schema {
	schema := schemabuilder.NewSchema()

	ts.registerObject(schema)
	ts.registerQuery(schema)
	ts.registerMutation(schema)
	return schema.MustBuild()
}

func (ts *TrafficService) registerObject(schema *schemabuilder.Schema) {
}

func (ts *TrafficService) registerQuery(schema *schemabuilder.Schema) {
	query := schema.Query()
	query.FieldFunc("topVol", ts.listTopVol)
	query.FieldFunc("abnormalVol", ts.abnormalVol)
	query.FieldFunc("noDeleteVol", ts.listNoDeleteVol)
	query.FieldFunc("zombieVolume", ts.getZombieVolume)
	query.FieldFunc("noClientVol", ts.getNoClientVol)

	query.FieldFunc("historyCurve", ts.volumeHistoryData)
	query.FieldFunc("topIncrease", ts.topIncrease)

	query.FieldFunc("listModuleType", ts.listModuleTypeHandle)
	query.FieldFunc("listOp", ts.listOpHandle)
	query.FieldFunc("listTopIp", ts.listTopIpHandle)
	query.FieldFunc("listTopVol", ts.listTopVolHandle)
	query.FieldFunc("listTopPartition", ts.listTopPartitionHandle)
	query.FieldFunc("volDetails", ts.volDetailsHandle)
	query.FieldFunc("volLatency", ts.volLatencyHandle)
	query.FieldFunc("ipDetails", ts.ipDetailsHandle)
	query.FieldFunc("ipLatency", ts.ipLatencyHandle)
	query.FieldFunc("clusterDetails", ts.clusterDetailsHandle)
	query.FieldFunc("clusterLatency", ts.clusterLatencyHandle)
	query.FieldFunc("clusterZoneLatency", ts.clusterZoneLatency) // ck查询 - zone延时曲线 区分模块 多条延时曲线(多action) 切换延时指标(max tp99 avg)
	query.FieldFunc("latencyDetail", ts.latencyDetailHandle)     // 所有延时曲线通用
}

func (ts *TrafficService) registerMutation(schema *schemabuilder.Schema) {
}

func (ts *TrafficService) DoXbpApply(apply *model.XbpApplyInfo) error {
	return nil
}

// 异常vol统计
func (ts *TrafficService) abnormalVol(ctx context.Context, args struct {
	Cluster *string
}) (*cproto.AbnormalVolResponse, error) {
	cluster := cutil.GetClusterParam(args.Cluster)

	zombieVols, err := model.LoadZombieVols(cluster)
	if err != nil {
		return nil, err
	}
	noDeletedVols, err := model.LoadNoDeleteVol(cluster)
	if err != nil {
		return nil, err
	}

	// 获取vol个数
	volumeList := traffic.GetVolList(cluster, ts.api)
	return &cproto.AbnormalVolResponse{
		Total:            int64(len(volumeList)),
		ZombieVolCount:   int64(len(zombieVols)),
		NoDeleteVolCount: int64(len(noDeletedVols)),
	}, nil

}

// 僵尸vol
func (ts *TrafficService) getZombieVolume(ctx context.Context, args struct {
	Cluster  *string
	Page     int32
	PageSize int32
}) (*cproto.ZombieVolResponse, error) {
	cluster := cutil.GetClusterParam(args.Cluster)
	result, err := model.LoadZombieVolDetails(cluster, int(args.Page), int(args.PageSize))
	if err != nil {
		return nil, err
	}
	total := len(result)
	if total > 0 {
		result = result[(args.Page-1)*args.PageSize : unit.Min(int(args.Page*args.PageSize), total)]
	}
	return &cproto.ZombieVolResponse{
		Total: int64(total),
		Data:  result,
	}, nil
}

// 只增不删vol
func (ts *TrafficService) listNoDeleteVol(ctx context.Context, args struct {
	Cluster  *string
	Page     int32
	PageSize int32
}) (*cproto.ZombieVolResponse, error) {
	cluster := cutil.GetClusterParam(args.Cluster)
	result, err := model.LoadNoDeletedVolDetails(cluster, int(args.Page), int(args.PageSize))
	if err != nil {
		return nil, err
	}
	total := len(result)
	if total > 0 {
		result = result[(args.Page-1)*args.PageSize : unit.Min(int(args.Page*args.PageSize), total)]
	}
	return &cproto.ZombieVolResponse{
		Total: int64(total),
		Data:  result,
	}, nil
}

// 没有客户端挂载的vol
func (ts *TrafficService) getNoClientVol(ctx context.Context, args struct {
	Cluster  *string
	Page     int32
	PageSize int32
	Keywords *string
}) (*cproto.ZombieVolResponse, error) {
	cluster := cutil.GetClusterParam(args.Cluster)
	// total
	// page
	result, err := model.LoadNoClientVolDetails(cluster, args.Keywords)
	if err != nil {
		return nil, err
	}
	total := len(result)
	if total > 0 {
		result = result[(args.Page-1)*args.PageSize : unit.Min(int(args.Page*args.PageSize), total)]
	}
	return &cproto.ZombieVolResponse{
		Total: int64(total),
		Data:  result,
	}, nil
}

// 文件数/使用量 top10
func (ts *TrafficService) listTopVol(ctx context.Context, args struct {
	Cluster *string
	TopN    *int32
	Zone    *string
	Source  *string
}) (*cproto.TopVolResponse, error) {
	cluster := cutil.GetClusterParam(args.Cluster)
	topN := flow.DefaultTopN
	if args.TopN != nil {
		topN = int(*args.TopN)
	}
	var zone string
	if args.Zone != nil {
		zone = *args.Zone
	}
	var source string
	if args.Source != nil {
		source = *args.Source
	}
	topInode, err := model.LoadInodeTopNVol(cluster, topN, zone, source, 0)
	if err != nil {
		return nil, err
	}
	topUsed, err := model.LoadUsedGBTopNVol(cluster, topN, zone, source, 0)
	if err != nil {
		return nil, err
	}
	return &cproto.TopVolResponse{
		TopInode: topInode,
		TopUsed:  topUsed,
	}, nil
}

// 使用量和文件个数历史曲线
func (ts *TrafficService) volumeHistoryData(ctx context.Context, args struct {
	Cluster   *string
	Volume    string
	Interval  int32 // 近1天 近一周 近一月
	StartDate int64 // 秒级时间戳
	EndDate   int64
}) ([]*model.VolumeHistoryCurve, error) {
	cluster := cutil.GetClusterParam(args.Cluster)
	request := &cproto.HistoryCurveRequest{
		Cluster:      cluster,
		Volume:       args.Volume,
		IntervalType: int(args.Interval),
		Start:        args.StartDate,
		End:          args.EndDate,
	}

	result, err := traffic.GetVolHistoryCurve(request)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (ts *TrafficService) topIncrease(ctx context.Context, args struct {
	Cluster    string
	Zone       string
	Mode       int32 // 0-查vol  1-查source
	StartTime  int64 // 秒级时间戳(10位)
	EndTime    int64
	Interval   int32   // 近一周 近一月 近三月 0
	StrictZone *int32  // 0-跨zone 1-单zone
	OrderBy    *int32  // 0-增量降序 0-增量升序
	Source     *string // zone-topVol的过滤
	OrderField *int32  // 0-增量 1-使用量
}) []*traffic.TopIncrease {
	request := &cproto.HistoryCurveRequest{
		Cluster:      args.Cluster,
		ZoneName:     args.Zone,
		Start:        args.StartTime,
		End:          args.EndTime,
		IntervalType: int(args.Interval),
	}

	strict := cutil.IntToBool(0)
	if args.StrictZone != nil {
		strict = cutil.IntToBool(int(*args.StrictZone))
	}
	orderBy := 0
	if args.OrderBy != nil {
		orderBy = int(*args.OrderBy)
	}
	source := ""
	if args.Source != nil {
		source = *args.Source
	}
	orderField := 0
	if args.OrderField != nil {
		orderField = int(*args.OrderField)
	}

	switch args.Mode {
	case traffic.TopVol:
		return traffic.GetTopIncreaseVol(request, strict, source, orderBy, orderField)

	case traffic.TopSource:
		return traffic.GetTopIncreaseSource(request, strict, orderBy, orderField)
	}
	return nil
}

func (ts *TrafficService) listModuleTypeHandle(ctx context.Context, args struct {
}) []string {
	return []string{
		cproto.ModuleDataNode,
		cproto.ModuleMetaNode,
		cproto.ModuleObjectNode,
		cproto.ModuleFlashNode,
	}
}

func (ts *TrafficService) listOpHandle(ctx context.Context, args struct {
	Module string
}) []string {
	return cproto.OPMap[args.Module]
}

func (ts *TrafficService) listTopIpHandle(ctx context.Context, args struct {
	Cluster   string
	Module    string
	Interval  int32
	StartTime int64 // 秒级时间戳
	EndTime   int64
	Page      *int32
	PageSize  *int32
	TopN      *int32
	OrderBy   *string
	Op        *string
	Volume    *string
	Zone      *string
}) (*cproto.TrafficResponse, error) {
	request := &cproto.TrafficRequest{
		ClusterName:  args.Cluster,
		Module:       strings.ToLower(args.Module),
		IntervalType: int(args.Interval),
		StartTime:    args.StartTime,
		EndTime:      args.EndTime,
	}
	if args.TopN != nil {
		request.TopN = int(*args.TopN)
	}
	if args.OrderBy != nil {
		request.OrderBy = *args.OrderBy
	}
	if args.Op != nil {
		request.OperationType = *args.Op
	}
	if args.Volume != nil {
		request.VolumeName = *args.Volume
	}
	if args.Zone != nil {
		request.Zone = *args.Zone
	}
	result, err := ts.flowSchedule.ListTopIPClickHouse(request)
	if err != nil {
		return nil, err
	}
	total := len(result)
	if args.Page != nil && args.PageSize != nil {
		page := *args.Page
		size := *args.PageSize
		if len(result) > 0 {
			result = result[(page-1)*size : unit.Min(int(page*size), len(result))]
		}
	}
	return &cproto.TrafficResponse{
		Total: total,
		Data:  result,
	}, nil
}

func (ts *TrafficService) listTopVolHandle(ctx context.Context, args struct {
	Cluster   string
	Module    string
	Interval  int32
	StartTime int64
	EndTime   int64
	Page      *int32
	PageSize  *int32
	TopN      *int32
	OrderBy   *string
	Op        *string
	Ip        *string
	Zone      *string
	Disk      *string
}) (*cproto.TrafficResponse, error) {
	request := &cproto.TrafficRequest{
		ClusterName:  args.Cluster,
		Module:       strings.ToLower(args.Module),
		IntervalType: int(args.Interval),
		StartTime:    args.StartTime,
		EndTime:      args.EndTime,
	}
	if args.TopN != nil {
		request.TopN = int(*args.TopN)
	}
	if args.OrderBy != nil {
		request.OrderBy = *args.OrderBy
	}
	if args.Op != nil {
		request.OperationType = *args.Op
	}
	if args.Ip != nil {
		request.IpAddr = *args.Ip
	}
	if args.Zone != nil {
		request.Zone = *args.Zone
	}
	if args.Disk != nil {
		request.Disk = *args.Disk
	}
	result, err := ts.flowSchedule.ListTopVolClickHouse(request)
	if err != nil {
		return nil, err
	}
	total := len(result)
	if args.Page != nil && args.PageSize != nil {
		page := *args.Page
		size := *args.PageSize
		if len(result) > 0 {
			result = result[(page-1)*size : unit.Min(int(page*size), len(result))]
		}
	}
	return &cproto.TrafficResponse{
		Total: total,
		Data:  result,
	}, nil
}

func (ts *TrafficService) listTopPartitionHandle(ctx context.Context, args struct {
	Cluster   string
	Module    string
	Interval  int32
	StartTime int64
	EndTime   int64
	Page      *int32
	PageSize  *int32
	TopN      *int32
	OrderBy   *string
	Op        *string
	Zone      *string
	Volume    *string
	Ip        *string
	Disk      *string
}) (*cproto.TrafficResponse, error) {
	request := &cproto.TrafficRequest{
		ClusterName:  args.Cluster,
		Module:       strings.ToLower(args.Module),
		IntervalType: int(args.Interval),
		StartTime:    args.StartTime,
		EndTime:      args.EndTime,
	}
	if args.TopN != nil {
		request.TopN = int(*args.TopN)
	}
	if args.OrderBy != nil {
		request.OrderBy = *args.OrderBy
	}
	if args.Op != nil {
		request.OperationType = *args.Op
	}
	if args.Zone != nil {
		request.Zone = *args.Zone
	}
	if args.Volume != nil {
		request.VolumeName = *args.Volume
	}
	if args.Ip != nil {
		request.IpAddr = *args.Ip
	}
	if args.Disk != nil {
		request.Disk = *args.Disk
	}
	result, err := ts.flowSchedule.ListTopPartitionClickHouse(request)
	if err != nil {
		return nil, err
	}
	total := len(result)
	if args.Page != nil && args.PageSize != nil {
		page := *args.Page
		size := *args.PageSize
		if len(result) > 0 {
			result = result[(page-1)*size : unit.Min(int(page*size), len(result))]
		}
	}
	return &cproto.TrafficResponse{
		Total: total,
		Data:  result,
	}, nil
}

func (ts *TrafficService) volDetailsHandle(ctx context.Context, args struct {
	Cluster   *string
	Module    string
	Interval  int32
	StartTime int64
	EndTime   int64
	Volume    string
	TopN      *int32  // 不指定action，默认的曲线条数
	OrderBy   *string // 基本不用，默认count
	Op        *string
}) (*cproto.TrafficDetailsResponse, error) {
	cluster := cutil.GetClusterParam(args.Cluster)
	request := &cproto.TrafficRequest{
		ClusterName:  cluster,
		VolumeName:   args.Volume,
		Module:       strings.ToLower(args.Module),
		IntervalType: int(args.Interval),
		StartTime:    args.StartTime,
		EndTime:      args.EndTime,
	}
	if args.TopN != nil {
		request.TopN = int(*args.TopN)
	}
	if args.Op != nil {
		request.OperationType = *args.Op
	}
	result, err := ts.flowSchedule.VolDetailsFromClickHouse(request)
	if err != nil {
		return nil, err
	}
	return &cproto.TrafficDetailsResponse{
		Data: result,
	}, err
}

func (ts *TrafficService) volLatencyHandle(ctx context.Context, args struct {
	Cluster   string
	Module    string
	Interval  int32
	StartTime int64
	EndTime   int64
	Volume    string
	Op        string
}) (*cproto.TrafficLatencyResponse, error) {
	request := &cproto.TrafficRequest{
		ClusterName:   args.Cluster,
		Module:        strings.ToLower(args.Module),
		IntervalType:  int(args.Interval),
		StartTime:     args.StartTime,
		EndTime:       args.EndTime,
		VolumeName:    args.Volume,
		OperationType: args.Op,
	}
	if request.EndTime-request.StartTime > 60*60*24 {
		return nil, fmt.Errorf("延时数据查询时间范围不能超过一天！")
	}
	result, err := ts.flowSchedule.VolLatency(request)
	if err != nil {
		return nil, err
	}
	return &cproto.TrafficLatencyResponse{
		Data: result,
	}, nil
}

func (ts *TrafficService) ipDetailsHandle(ctx context.Context, args struct {
	Cluster       *string
	Module        string
	Interval      int32
	StartTime     int64
	EndTime       int64
	Ip            string
	DrawDimension *int32
	TopN          *int32  // 不指定action，默认的曲线条数
	Op            *string // disk级别画线 op必填
	Disk          *string
}) (*cproto.TrafficDetailsResponse, error) {
	drawLevel := cproto.ActionDrawLineDimension
	if args.DrawDimension != nil {
		drawLevel = *args.DrawDimension
	}
	if drawLevel == cproto.DiskDrawLineDimension {
		if args.Op == nil {
			return nil, fmt.Errorf("disk级别曲线 请指定操作类型！")
		}
	}
	cluster := cutil.GetClusterParam(args.Cluster)
	request := &cproto.TrafficRequest{
		ClusterName:  cluster,
		IpAddr:       args.Ip,
		Module:       strings.ToLower(args.Module),
		IntervalType: int(args.Interval),
		StartTime:    args.StartTime,
		EndTime:      args.EndTime,
	}
	if args.TopN != nil {
		request.TopN = int(*args.TopN)
	}
	if args.Op != nil {
		request.OperationType = *args.Op
	}
	if args.Disk != nil {
		request.Disk = *args.Disk
	}
	var (
		result [][]*cproto.FlowScheduleResult
		err    error
	)
	switch drawLevel {
	case cproto.ActionDrawLineDimension:
		result, err = ts.flowSchedule.IPDetailsFromClickHouse(request)

	case cproto.DiskDrawLineDimension:
		result, err = ts.flowSchedule.IPDetailDiskLevel(request)
	}
	if err != nil {
		log.LogErrorf("ipDetail failed: request(%v) err(%v) diskLevel(%v)", request, err, args.DrawDimension)
		return nil, err
	}
	return &cproto.TrafficDetailsResponse{
		Data: result,
	}, err
}

// ip的延时信息稍微复杂 成线维度、disk输入
func (ts *TrafficService) ipLatencyHandle(ctx context.Context, args struct {
	Cluster       *string
	Module        string
	Interval      int32
	StartTime     int64
	EndTime       int64
	Ip            string
	Op            string
	DrawDimension *int32
	Disk          *string //disk级别画线 disk必填
}) (*cproto.TrafficLatencyResponse, error) {
	drawLevel := cproto.ActionDrawLineDimension
	if args.DrawDimension != nil {
		drawLevel = *args.DrawDimension
	}
	if drawLevel == cproto.DiskDrawLineDimension {
		if args.Disk == nil {
			return nil, fmt.Errorf("disk级别延时曲线 请指定disk！")
		}
	}
	cluster := cutil.GetClusterParam(args.Cluster)
	request := &cproto.TrafficRequest{
		ClusterName:   cluster,
		Module:        strings.ToLower(args.Module),
		IntervalType:  int(args.Interval),
		StartTime:     args.StartTime,
		EndTime:       args.EndTime,
		IpAddr:        args.Ip,
		OperationType: args.Op,
	}
	if args.Disk != nil {
		request.Disk = *args.Disk
	}
	if request.EndTime-request.StartTime > 60*60*24 {
		return nil, fmt.Errorf("延时数据查询时间范围不能超过一天！")
	}
	result, err := ts.flowSchedule.IpLatency(request)
	if err != nil {
		log.LogErrorf("ipLatency: request(%v) err(%v)", request, err)
		return nil, err
	}
	return &cproto.TrafficLatencyResponse{
		Data: result,
	}, nil
}

func (ts *TrafficService) clusterDetailsHandle(ctx context.Context, args struct {
	Cluster   string
	Module    string
	Interval  int32
	StartTime int64
	EndTime   int64
	Op        *string
	Zone      *string
	TopN      *int32
	OrderBy   *string
}) (*cproto.TrafficDetailsResponse, error) {
	request := &cproto.TrafficRequest{
		ClusterName:  args.Cluster,
		Module:       strings.ToLower(args.Module),
		IntervalType: int(args.Interval),
		StartTime:    args.StartTime,
		EndTime:      args.EndTime,
	}
	if args.Op != nil {
		request.OperationType = *args.Op
	}
	if args.Zone != nil {
		request.Zone = *args.Zone
	}
	if args.TopN != nil {
		request.TopN = int(*args.TopN)
	}
	if args.OrderBy != nil {
		request.OrderBy = *args.OrderBy
	}
	result, err := ts.flowSchedule.ClusterDetailsFromClickHouse(request)
	if err != nil {
		log.LogErrorf("clusterDetail failed: req(%v) err(%v)", request, err)
		return nil, err
	}
	return &cproto.TrafficDetailsResponse{
		Data: result,
	}, err
}

func (ts *TrafficService) clusterLatencyHandle(ctx context.Context, args struct {
	Cluster   *string
	Module    string
	Interval  int32
	StartTime int64
	EndTime   int64
	Op        string
	Zone      *string
}) (*cproto.TrafficLatencyResponse, error) {
	cluster := cutil.GetClusterParam(args.Cluster)
	request := &cproto.TrafficRequest{
		ClusterName:   cluster,
		Module:        strings.ToLower(args.Module),
		IntervalType:  int(args.Interval),
		StartTime:     args.StartTime,
		EndTime:       args.EndTime,
		OperationType: args.Op,
	}
	if args.Zone != nil {
		request.Zone = *args.Zone
	}
	if request.EndTime-request.StartTime > 60*60*24 {
		return nil, fmt.Errorf("延时数据查询时间范围不能超过一天！")
	}
	result, err := ts.flowSchedule.ClusterLatency(request)
	if err != nil {
		log.LogErrorf("clusterLatency failed: req(%v) err(%v)", request, err)
		return nil, err
	}
	return &cproto.TrafficLatencyResponse{
		Data: result,
	}, nil
}

// 集群 zone 模块 操作的延时信息 （10分钟 1小时 3小时，最长的时间间隔12小时）
// 所有的action max、tp99、avg分三个图
func (ts *TrafficService) clusterZoneLatency(ctx context.Context, args struct {
	Cluster   string
	Zone      string
	Module    string
	Interval  int32
	StartTime int64
	EndTime   int64
}) (*cproto.TrafficDetailsResponse, error) {
	request := &cproto.TrafficRequest{
		ClusterName:  args.Cluster,
		Module:       strings.ToLower(args.Module),
		IntervalType: int(args.Interval),
		StartTime:    args.StartTime,
		EndTime:      args.EndTime,
		Zone:         args.Zone,
	}
	if request.EndTime-request.StartTime > flow.OneDayInSecond {
		return nil, fmt.Errorf("时间范围不超过1天")
	}
	result, err := ts.flowSchedule.ZoneActionLatency(request)
	if err != nil {
		return nil, err
	}
	return &cproto.TrafficDetailsResponse{
		Data: result,
	}, err
}

func (ts *TrafficService) latencyDetailHandle(ctx context.Context, args struct {
	Cluster   string
	Module    string
	StartTime int64
	EndTime   int64
	Interval  int32
	Op        string
	OrderBy   *string // tp99 max avg
	Zone      *string
	Ip        *string
	Volume    *string
	Disk      *string
}) (*cproto.TrafficLatencyResponse, error) {
	request := &cproto.TrafficRequest{
		ClusterName:   args.Cluster,
		Module:        strings.ToLower(args.Module),
		StartTime:     args.StartTime,
		EndTime:       args.EndTime,
		OperationType: args.Op,
	}
	if args.OrderBy != nil {
		request.OrderBy = *args.OrderBy
	}
	if args.Zone != nil {
		request.Zone = *args.Zone
	}
	if args.Ip != nil {
		request.IpAddr = *args.Ip
	}
	if args.Volume != nil {
		request.VolumeName = *args.Volume
	}
	if args.Disk != nil {
		request.Disk = *args.Disk
	}
	result, err := ts.flowSchedule.ClusterLatencyDetail(request)
	if err != nil {
		return nil, err
	}
	return &cproto.TrafficLatencyResponse{
		Data: result,
	}, nil
}
