package service

import (
	"context"
	"fmt"
	"github.com/cubefs/cubefs/console/cutil"
	"github.com/cubefs/cubefs/console/model"
	cproto "github.com/cubefs/cubefs/console/proto"
	api "github.com/cubefs/cubefs/console/service/apiManager"
	"github.com/cubefs/cubefs/console/service/scheduleTask"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/unit"
	"github.com/samsarahq/thunder/graphql"
	"github.com/samsarahq/thunder/graphql/schemabuilder"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

type ScheduleTaskService struct {
	// 每种任务 对应一个worker
	api             *api.APIManager
	rebalanceWorker *scheduleTask.RebalanceWorker
	compactWorker   *scheduleTask.CompactWorker
}

func NewScheduleTaskService(clusters []*model.ConsoleCluster) *ScheduleTaskService {
	s := new(ScheduleTaskService)
	s.api = api.NewAPIManager(clusters)
	s.rebalanceWorker = scheduleTask.NewRebalanceWorker(clusters)
	s.compactWorker = scheduleTask.NewCompactWorker()
	s.compactWorker.SetApiManager(s.api)
	return s
}

func (s *ScheduleTaskService) Schema() *graphql.Schema {
	schema := schemabuilder.NewSchema()

	s.registerObject(schema)
	s.registerQuery(schema)
	s.registerMutation(schema)
	s.registerRebalanceAction(schema)

	return schema.MustBuild()
}

func (s *ScheduleTaskService) registerObject(schema *schemabuilder.Schema) {

}

func (s *ScheduleTaskService) registerQuery(schema *schemabuilder.Schema) {
	query := schema.Query()
	query.FieldFunc("listCompactVol", s.listCompactVol) //加过滤，符合条件的
	query.FieldFunc("checkCompactVol", s.checkCompactVol)
	query.FieldFunc("createCheckFrag", s.createCheckFrag)       // 创建check任务,返回id
	query.FieldFunc("getCheckFragRecord", s.getCheckFragRecord) // 任务列表， 按照时间排序（过滤（cluster vol）
}

func (s *ScheduleTaskService) registerMutation(schema *schemabuilder.Schema) {
	mutation := schema.Mutation()
	mutation.FieldFunc("batchModifyCompact", s.batchModifyVolCompact) // 批量关闭、批量打开(符合条件的)
	// 创建scheduleNode支持的任务
	mutation.FieldFunc("createScheduleTask", s.createScheduleTask)

}

// todo: 记录谁执行的rebalance任务
func (s *ScheduleTaskService) registerRebalanceAction(schema *schemabuilder.Schema) {
	query := schema.Query()
	mutation := schema.Mutation()
	query.FieldFunc("dropdownList", s.getRebalanceDropdownList)
	query.FieldFunc("rebalanceList", s.getRebalanceList)
	query.FieldFunc("rebalanceInfo", s.getRebalanceInfo)     // 只有进行中的任务 才能够查询info，停止的为null
	query.FieldFunc("queryTaskStatus", s.getRebalanceStatus) //任务进度
	query.FieldFunc("queryMigrateRecords", s.queryMigrateRecords)
	query.FieldFunc("getNodeUsage", s.getNodeUsage)

	mutation.FieldFunc("createRebalanceTask", s.createRebalanceTask)
	mutation.FieldFunc("createNodeMigrateTask", s.createNodeMigrateTask)
	mutation.FieldFunc("stopRebalanceTask", s.stopRebalanceTask)
	mutation.FieldFunc("resetControlRebalanceTask", s.resetControlRebalanceTask)
	mutation.FieldFunc("resetRebalance", s.resetRebalance)
}

func (s *ScheduleTaskService) createRebalanceTask(ctx context.Context, args struct {
	Cluster          string
	Module           string
	Zone             string
	HighRatio        float64
	LowRatio         float64
	GoalRatio        float64
	Concurrency      int32
	LimitDPonDisk    *int32
	LimitMPonDstNode *int32
}) (err error) {
	pin := ctx.Value(cutil.PinKey).(string)
	if !model.IsAdmin(pin) {
		return fmt.Errorf("没有操作权限")
	}
	switch args.Module {
	case "data":
		if args.LimitDPonDisk == nil {
			return fmt.Errorf("请指定最大迁出dp数/盘！")
		}

	case "meta":
		if args.LimitMPonDstNode == nil {
			return fmt.Errorf("请指定目标节点mp个数上限！")
		}
	}
	// 创建任务
	err = s.rebalanceWorker.CreateZoneAutoRebalanceTask(args.Cluster, args.Module, args.Zone, args.HighRatio, args.LowRatio, args.GoalRatio,
		int(args.Concurrency), args.LimitDPonDisk, args.LimitMPonDstNode)
	if err != nil {
		log.LogErrorf("createAutoRebalanceTask failed: erp[%v] err(%v)", ctx.Value(cutil.PinKey), err)
	}
	return
}

func (s *ScheduleTaskService) createNodeMigrateTask(ctx context.Context, args struct {
	Cluster          string
	Module           string
	SrcNodeList      []string
	DstNodeList      []string
	Concurrency      int32
	LimitDPonDisk    *int32 //data
	LimitMPonDstNode *int32 //meta
}) (err error) {
	pin := ctx.Value(cutil.PinKey).(string)
	if !model.IsAdmin(pin) {
		return fmt.Errorf("没有操作权限")
	}
	err = s.rebalanceWorker.CreateNodesMigrateTask(args.Cluster, args.Module, args.SrcNodeList, args.DstNodeList, int(args.Concurrency),
		args.LimitDPonDisk, args.LimitMPonDstNode)
	if err != nil {
		log.LogErrorf("createNodeMigrateTask failed: erp[%v] err(%v)", ctx.Value(cutil.PinKey), err)
	}
	return
}

func (s *ScheduleTaskService) stopRebalanceTask(ctx context.Context, args struct {
	Cluster string
	TaskID  uint64 // 任务id
}) (err error) {
	pin := ctx.Value(cutil.PinKey).(string)
	if !model.IsAdmin(pin) {
		return fmt.Errorf("没有操作权限")
	}
	err = s.rebalanceWorker.StopTask(args.Cluster, args.TaskID)
	if err != nil {
		log.LogErrorf("stopRebalanceTask failed: erp[%v] err(%v)", ctx.Value(cutil.PinKey), err)
	}
	return
}

func (s *ScheduleTaskService) resetControlRebalanceTask(ctx context.Context, args struct {
	Cluster          string
	Module           string
	TaskID           uint64
	TaskType         int32
	Zone             *string
	GoalRatio        *float64
	Concurrency      *int32
	LimitDPonDisk    *int32
	LimitMPonDstNode *int32
}) (err error) {
	pin := ctx.Value(cutil.PinKey).(string)
	if !model.IsAdmin(pin) {
		return fmt.Errorf("没有操作权限")
	}
	if args.GoalRatio == nil && args.Concurrency == nil && args.LimitDPonDisk == nil && args.LimitMPonDstNode == nil {
		return fmt.Errorf("请至少选择一项修改！")
	}
	err = s.rebalanceWorker.ResetControl(args.Cluster, args.Module, args.TaskID, int(args.TaskType), args.Zone,
		args.GoalRatio, args.Concurrency, args.LimitDPonDisk, args.LimitMPonDstNode)
	return
}

func (s *ScheduleTaskService) resetRebalance(ctx context.Context, args struct {
	Cluster string
}) (err error) {
	err = s.rebalanceWorker.ResetTask(args.Cluster)
	if err != nil {
		log.LogErrorf("resetRebalance failed: err(%v)", err)
	}
	return err
}

func (s *ScheduleTaskService) createScheduleTask(ctx context.Context, args struct {
	Cluster string
	Volume  string
}) (err error) {
	return nil
}

func (s *ScheduleTaskService) getRebalanceDropdownList(ctx context.Context, args struct {
	Name string
	// zone列表需要集群参数
	Cluster *string
}) []*cproto.CliOpMetric {
	dropdownList := make([]*cproto.CliOpMetric, 0)
	switch args.Name {
	case "cluster":
		for _, cluster := range cproto.RebalanceCluster {
			entry := cproto.NewCliOpMetric(cluster, cluster)
			dropdownList = append(dropdownList, entry)
		}
	case "module":
		for _, module := range cproto.RebalanceModule {
			entry := cproto.NewCliOpMetric(module, module)
			dropdownList = append(dropdownList, entry)
		}
	case "zone":
		if args.Cluster == nil {
			return nil
		}
		zoneList := s.api.GetDatanodeZoneList(*args.Cluster)
		if len(zoneList) <= 0 {
			return nil
		}
		for _, zone := range zoneList {
			entry := cproto.NewCliOpMetric(zone, zone)
			dropdownList = append(dropdownList, entry)
		}
	case "status":
		for indexCode, status := range cproto.RebalanceStatus {
			// +1 跳过全部(0)
			entry := cproto.NewCliOpMetric(strconv.Itoa(indexCode+1), status)
			dropdownList = append(dropdownList, entry)
		}
	case "taskType":
		for indexCode, taskType := range cproto.RebalanceTaskType {
			entry := cproto.NewCliOpMetric(strconv.Itoa(indexCode+1), taskType)
			dropdownList = append(dropdownList, entry)
		}
	}
	return dropdownList
}

// 任务列表: 展示顺序 创建时间的先后？
func (s *ScheduleTaskService) getRebalanceList(ctx context.Context, args struct {
	Page     int32
	PageSize int32
	Cluster  string
	Module   *string
	Zone     *string
	Status   *int32
	TaskType *int32
}) (*cproto.RebalanceListResp, error) {
	var (
		module   string
		zone     string
		status   int
		taskType int
	)
	if args.Zone != nil {
		zone = *args.Zone
	}
	if args.Status != nil {
		status = int(*args.Status)
	}
	if args.TaskType != nil {
		taskType = int(*args.TaskType)
	}
	if args.Module != nil {
		module = *args.Module
	}
	total, list, err := s.rebalanceWorker.GetTaskList(args.Cluster, module, zone, status, taskType, int(args.Page), int(args.PageSize))
	if err != nil {
		return nil, err
	}
	return &cproto.RebalanceListResp{
		Total: total,
		Data:  list,
	}, nil
}

// 详情(reset后，状态为停止的都会被移除)
func (s *ScheduleTaskService) getRebalanceInfo(ctx context.Context, args struct {
	TaskID uint64 //任务ID， 列表中返回的ID
}) (*cproto.RebalanceInfoView, error) {
	return s.rebalanceWorker.GetTaskInfo(args.TaskID)
}

// 获取某个任务的状态（刷新，进度）
func (s *ScheduleTaskService) getRebalanceStatus(ctx context.Context, args struct {
	TaskID uint64
}) (*cproto.RebalanceStatusInfo, error) {
	return s.rebalanceWorker.GetTaskStatus(args.TaskID)
}

func (s *ScheduleTaskService) queryMigrateRecords(ctx context.Context, args struct {
	Cluster  string
	Module   string
	Zone     *string
	Volume   *string
	Src      *string
	Dst      *string
	Pid      *uint64
	CreateAt *int64 // 秒级时间戳
	Page     int32
	PageSize int32
}) (*cproto.MigrateRecordListResp, error) {

	var dateStr string
	if args.CreateAt != nil {
		ts := time.Unix(*args.CreateAt, 0)
		dateStr = ts.Format(cproto.TimeFormatCompact)
	}
	total, records, err := s.rebalanceWorker.GetMigrateRecords(args.Cluster, args.Module, args.Zone, args.Volume, args.Src,
		args.Dst, dateStr, args.Pid, int(args.Page), int(args.PageSize))
	if err != nil {
		log.LogErrorf("queryMigrateRecords failed: err(%v)", err)
		return nil, err
	}
	return &cproto.MigrateRecordListResp{
		Total: total,
		Data:  records,
	}, nil

}

// todo：meta、data
func (s *ScheduleTaskService) getNodeUsage(ctx context.Context, args struct {
	Cluster   string
	Zone      string
	Module    string
	Prefix    *string
	HighRatio *float64
	LowRatio  *float64
}) (*cproto.NodeUsageRatio, error) {
	result := make([]*cproto.NodeUsage, 0)
	var (
		err   error
		nodes []*cproto.NodeUsage
	)
	switch args.Module {
	case "data":
		nodes, err = s.rebalanceWorker.GetZoneNodeUsageRatio(args.Cluster, args.Zone, args.Module)

	case "meta":
		nodes, err = s.rebalanceWorker.GetZoneNodeUsageRatio(args.Cluster, args.Zone, args.Module)
	}
	if err != nil {
		return nil, err
	}

	for _, node := range nodes {
		node.UsedRatio = math.Trunc(node.UsedRatio*1e6) / 1e6
	}
	if args.Prefix != nil {
		for _, node := range nodes {
			if strings.HasPrefix(node.Addr, *args.Prefix) {
				result = append(result, node)
			}
		}
		nodes = result
	}
	if args.HighRatio != nil {
		for _, node := range nodes {
			if node.UsedRatio >= *args.HighRatio {
				result = append(result, node)
			}
		}
		nodes = result
	}
	if args.LowRatio != nil {
		for _, node := range nodes {
			if node.UsedRatio <= *args.LowRatio {
				result = append(result, node)
			}
		}
		nodes = result
	}
	sort.SliceStable(result, func(i, j int) bool {
		return result[i].UsedRatio > result[j].UsedRatio
	})
	return &cproto.NodeUsageRatio{
		Nodes: nodes,
	}, nil
}

func (s *ScheduleTaskService) listCompactVol(ctx context.Context, args struct {
	Cluster  string
	Page     int32
	PageSize int32
	Status   *bool // compact状态
}) (*cproto.CompactVolListResp, error) {
	if cproto.IsRelease(args.Cluster) {
		return nil, cproto.ErrUnSupportOperation
	}
	allVol := s.compactWorker.ListCompactVol(args.Cluster, args.Status)
	data := allVol[(args.Page-1)*args.PageSize : unit.Min(int(args.Page*args.PageSize), len(allVol))]
	return &cproto.CompactVolListResp{
		Total: len(allVol),
		Data:  data,
	}, nil
}

func (s *ScheduleTaskService) checkCompactVol(ctx context.Context, args struct {
	Cluster string
}) (*cproto.CompactVolListResp, error) {
	if cproto.IsRelease(args.Cluster) {
		return nil, cproto.ErrUnSupportOperation
	}
	volList := s.compactWorker.CheckCompactVolList(args.Cluster)
	return &cproto.CompactVolListResp{
		Total: len(volList),
		Data:  volList,
	}, nil
}

func (s *ScheduleTaskService) batchModifyVolCompact(ctx context.Context, args struct {
	Cluster string
	Vols    []*proto.DataMigVolume // 有owner
	Status  bool                   //必填 开启或关闭
}) (err error) {
	if cproto.IsRelease(args.Cluster) {
		return cproto.ErrUnSupportOperation
	}
	if args.Status {
		return s.compactWorker.BatchOpenCompactVol(args.Cluster, args.Vols)
	} else {
		return s.compactWorker.BatchCloseCompactVol(args.Cluster, args.Vols)
	}
}

func (s *ScheduleTaskService) createCheckFrag(ctx context.Context, args struct {
	// todo: 参数
	Cluster string
}) (id uint64, err error) {
	request := &model.CheckFragRequest{}
	table := model.CheckCompactFragRecord{}
	id, err = table.InsertRecord(request)
	go s.compactWorker.CheckFrag(id, request)
	return
}

func (s *ScheduleTaskService) getCheckFragRecord(ctx context.Context, args struct {
	Page     int32 // 检索需要的
	PageSize int32
	Cluster  *string
	Volume   *string
	Id       *uint64
}) ([]*model.CheckCompactFragRecord, error) {
	table := model.CheckCompactFragRecord{}
	return table.LoadCompactRecord(args.Cluster, args.Volume, args.Id, int(args.Page), int(args.PageSize))
}
