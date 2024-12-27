package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/cubefs/cubefs/console/cutil"
	"github.com/cubefs/cubefs/console/model"
	cproto "github.com/cubefs/cubefs/console/proto"
	"github.com/cubefs/cubefs/console/service/cli"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"github.com/samsarahq/thunder/graphql"
	"github.com/samsarahq/thunder/graphql/schemabuilder"
)

type CliService struct {
	cli *cli.CliService
}

func NewCliService(clusters []*model.ConsoleCluster) *CliService {
	cliService := cli.NewCliService(clusters)
	return &CliService{
		cli: cliService,
	}
}

func (cs *CliService) SType() cproto.ServiceType {
	return cproto.CliService
}

func (cs *CliService) Schema() *graphql.Schema {
	schema := schemabuilder.NewSchema()

	cs.registerObject(schema)
	cs.registerQuery(schema)
	cs.registerMutation(schema)

	return schema.MustBuild()
}

func (cs *CliService) registerObject(schema *schemabuilder.Schema) {}

func (cs *CliService) registerQuery(schema *schemabuilder.Schema) {
	query := schema.Query()
	query.FieldFunc("listModule", cs.listModule)
	query.FieldFunc("listOperation", cs.listOperation)
	query.FieldFunc("listOpcode", cs.listOpCodeList)
	query.FieldFunc("getConfig", cs.getConfig)
	query.FieldFunc("getConfigList", cs.getConfigList)
	query.FieldFunc("operationHistory", cs.getOperationHistory)
}

func (cs *CliService) registerMutation(schema *schemabuilder.Schema) {
	mutation := schema.Mutation()
	mutation.FieldFunc("setConfig", cs.setConfig)
	mutation.FieldFunc("setConfigList", cs.setConfigList)
}

func (cs *CliService) listModule(ctx context.Context, args struct {
	Cluster *string
}) ([]*cproto.CliModule, error) {

	modules := cproto.CliModuleList
	return modules, nil
}

func (cs *CliService) listOperation(ctx context.Context, args struct {
	Cluster    string
	ModuleType int32
}) ([]*cproto.CliOperation, error) {
	cluster := args.Cluster
	result := make([]*cproto.CliOperation, 0)

	switch args.ModuleType {
	case cproto.KeyValueModuleType:
		table := &model.KeyValueOperation{}
		records, err := table.GetOperationByCluster(cluster, cproto.IsRelease(cluster))
		if err != nil {
			return nil, err
		}
		for _, record := range records {
			operation := cproto.NewCliOperation(int(record.ID), record.URI, true, record.ReleaseSupport > 0, record.SparkSupport > 0)
			result = append(result, operation)
		}

	default:
		operations := cproto.CliOperationMap[int(args.ModuleType)]
		if cproto.IsRelease(cluster) {
			for _, operation := range operations {
				if operation.ReleaseSupport {
					result = append(result, operation)
				}
			}
		} else {
			for _, operation := range operations {
				if operation.SparkSupport {
					result = append(result, operation)
				}
			}
		}
	}

	if result == nil {
		return nil, fmt.Errorf("undefined module type: %v:%v", args.ModuleType, cproto.GetModule(int(args.ModuleType)))
	}
	return result, nil
}

func (cs *CliService) listOpCodeList(ctx context.Context, args struct {
	Cluster       string
	ModuleType    int32
	OperationType *int32
	ValueName     string
	Keywords      *string
}) ([]*cproto.CliOpMetric, error) {
	if args.ModuleType != cproto.VolumeModuleType && args.OperationType == nil {
		return nil, fmt.Errorf("操作码不能为空")
	}
	cluster := args.Cluster
	result := make([]*cproto.CliOpMetric, 0)

	switch args.ModuleType {
	case cproto.KeyValueModuleType:
		table := &model.KeyValuePathParams{}
		params, err := table.GetPathParams(uint64(*args.OperationType), cproto.IsRelease(cluster))
		if err != nil {
			return nil, err
		}
		for _, param := range params {
			// 传值用code 展示用 msg(中包含类型）
			opMetric := cproto.NewCliOpMetric(param.ValueName, param.ValueName+":"+param.ValueType)
			result = append(result, opMetric)
		}

	default:
		switch args.ValueName {
		case "module":
			result = cs.getModuleList(cluster)

		case "zoneName":
			result = cs.getZoneOpMetrics(cluster, int(*args.OperationType))

		case "volume", "name":
			// volume关键字，不指定前缀，仅返回1000条数据
			// dropDown: 支持前缀，不支持多选
			// MultiSelect： 支持前缀，不支持多选
			result = cs.getVolList(cluster, args.Keywords)

		case "opcode":
			result = getRatelimitOpcode(cluster, int(*args.OperationType))

		case proto.RateLimitIndexKey:
			result = getRatelimitIndexOpMetrics()

		case "action":
			result = getObjectActionList()
		}
	}
	return result, nil
}

func (cs *CliService) getConfig(ctx context.Context, args struct {
	Cluster       string
	ModuleType    int32
	OperationType int32
	VolName       *string
}) ([]*cproto.CliValueMetric, error) {
	cluster := args.Cluster
	switch args.ModuleType {
	case cproto.ClusterModuleType:
		return cs.cli.GetClusterConfig(cluster, int(args.OperationType))

	case cproto.MetaNodeModuleType:
		return cs.cli.GetMetaNodeConfig(cluster, int(args.OperationType))

	case cproto.DataNodeModuleType:
		return cs.cli.GetDataNodeConfig(cluster, int(args.OperationType))

	case cproto.EcModuleType:
		return cs.cli.GetEcConfig(cluster, int(args.OperationType))

	case cproto.RateLimitModuleType:
		// todo： 增加release集群的operation
		return cs.cli.GetRateLimitConfig(cluster, int(args.OperationType))

	case cproto.NetworkModuleType:
		return cs.cli.GetNetworkConfig(cluster, int(args.OperationType))

	case cproto.VolumeModuleType:
		if args.VolName == nil {
			return nil, fmt.Errorf("请选择需要编辑的vol: ")
		}
		return cs.cli.GetVolumeConfig(cluster, int(args.OperationType), *args.VolName)

	default:
		return nil, fmt.Errorf("undefined module type: %v:%v", args.ModuleType, "")
	}
}

func (cs *CliService) setConfig(ctx context.Context, args struct {
	Cluster       string
	ModuleType    int32
	OperationType int32
	Metrics       []*cproto.CliValueMetric
	VolName       *string
}) (resp *cproto.GeneralResp, err error) {
	cluster := args.Cluster
	defer func() {
		if err != nil {
			return
		}
		if cutil.Global_CFG.EnableXBP {
			return
		}
		record := model.NewCliOperation(
			cluster,
			cproto.GetModule(int(args.ModuleType)),
			cproto.GetOpShortMsg(int(args.OperationType)),
			ctx.Value(cutil.PinKey).(string),
			fmt.Sprintf("%v", args.Metrics),
			0,
			model.CliOperationStatusSuccess,
		)
		if args.VolName != nil {
			record.SetCliRecordVolume(*args.VolName)
		}
		record.InsertRecord(record)
	}()
	switch args.ModuleType {
	case cproto.ClusterModuleType:
		err = cs.cli.SetClusterConfig(ctx, cluster, int(args.OperationType), args.Metrics, !cutil.Global_CFG.EnableXBP)

	case cproto.MetaNodeModuleType:
		err = cs.cli.SetMetaNodeConfig(ctx, cluster, int(args.OperationType), args.Metrics, !cutil.Global_CFG.EnableXBP)

	case cproto.DataNodeModuleType:
		err = cs.cli.SetDataNodeConfig(ctx, cluster, int(args.OperationType), args.Metrics, !cutil.Global_CFG.EnableXBP)

	case cproto.EcModuleType:
		err = cs.cli.SetEcConfig(ctx, cluster, int(args.OperationType), args.Metrics, !cutil.Global_CFG.EnableXBP)

	case cproto.RateLimitModuleType:
		err = cs.cli.SetRatelimitConfig(ctx, cluster, int(args.OperationType), args.Metrics, !cutil.Global_CFG.EnableXBP)

	case cproto.NetworkModuleType:
		err = cs.cli.SetNetworkConfig(ctx, cluster, int(args.OperationType), args.Metrics, !cutil.Global_CFG.EnableXBP)

	case cproto.VolumeModuleType:
		if args.VolName == nil {
			err = fmt.Errorf("卷名不能为空！")
		}
		err = cs.cli.SetVolumeConfig(ctx, cluster, int(args.OperationType), args.Metrics, *args.VolName, !cutil.Global_CFG.EnableXBP)

	default:
		err = fmt.Errorf("undefined module type: %v:%v", args.ModuleType, "")
	}
	return cproto.BuildResponse(err), nil
}

func (cs *CliService) getConfigList(ctx context.Context, args struct {
	Cluster       string
	ModuleType    int32
	OperationType int32
	Filters       *[]*cproto.CliValueFilter //
	VolName       *string                   // moduleType为vol时必填
	Page          int32
	PageSize      int32
}) (*cproto.CliValueConfigList, error) {
	var (
		result = make([][]*cproto.CliValueMetric, 0)
		resp   = new(cproto.CliValueConfigList)
		err    error
	)
	defer func() {
		if args.Filters != nil && len(*args.Filters) > 0 {
			result = filterMetricsList(result, *args.Filters)
		}
		resp.Total = len(result)
		resp.Data = pagingMetricsList(int(args.Page), int(args.PageSize), result)
	}()
	switch args.ModuleType {
	case cproto.RateLimitModuleType:
		result, err = cs.cli.GetRatelimitConfigList(args.Cluster, int(args.OperationType))

	case cproto.DataNodeModuleType:
		result, err = cs.cli.GetDataNodeConfigList(args.Cluster, int(args.OperationType))

	case cproto.MetaNodeModuleType:
		result, err = cs.cli.GetMetaNodeConfigList(args.Cluster, int(args.OperationType))

	case cproto.NetworkModuleType:
		result, err = cs.cli.GetNetworkConfigList(args.Cluster, int(args.OperationType))

	case cproto.VolumeModuleType:
		if args.VolName == nil {
			return nil, fmt.Errorf("卷名不能为空！")
		}
		result, err = cs.cli.GetVolumeConfigList(args.Cluster, int(args.OperationType), *args.VolName)

	case cproto.BatchModuleType:
		result, err = cs.cli.GetBatchConfigList(args.Cluster, int(args.OperationType))

	case cproto.KeyValueModuleType:
		result, err = cs.cli.GetKeyValueConfigList(args.Cluster, int(args.OperationType))

	case cproto.FileMigrateModuleType:
		result, err = cs.cli.GetFileMigrateConfigList(args.Cluster, int(args.OperationType))

	default:
		return nil, fmt.Errorf("undefined module type: %v:%v", args.ModuleType, "")
	}
	return resp, err
}

func (cs *CliService) setConfigList(ctx context.Context, args struct {
	Cluster       string
	ModuleType    int32
	OperationType int32
	Metrics       [][]*cproto.CliValueMetric // 里层[] length一致 都是一个限速结构
	VolName       *string
}) (resp *cproto.GeneralResp, err error) {
	operationMsg := cproto.GetOpShortMsg(int(args.OperationType))
	defer func() {
		if err != nil {
			return
		}
		if cutil.Global_CFG.EnableXBP {
			return
		}
		record := model.NewCliOperation(
			args.Cluster,
			cproto.GetModule(int(args.ModuleType)),
			operationMsg,
			ctx.Value(cutil.PinKey).(string),
			fmt.Sprintf("%v", args.Metrics),
			0,
			model.CliOperationStatusSuccess,
		)
		if args.VolName != nil {
			record.SetCliRecordVolume(*args.VolName)
		}
		record.InsertRecord(record)
	}()

	switch args.ModuleType {
	case cproto.RateLimitModuleType:
		var modifyMetrics [][]*cproto.CliValueMetric
		modifyMetrics, err = cs.cli.SetRatelimitConfigList(ctx, args.Cluster, int(args.OperationType), args.Metrics, !cutil.Global_CFG.EnableXBP)
		if err == nil {
			args.Metrics = modifyMetrics
		}

	case cproto.DataNodeModuleType:
		err = cs.cli.SetDataNodeConfigList(ctx, args.Cluster, int(args.OperationType), args.Metrics, !cutil.Global_CFG.EnableXBP)

	case cproto.MetaNodeModuleType:
		err = cs.cli.SetMetaNodeConfigList(ctx, args.Cluster, int(args.OperationType), args.Metrics, !cutil.Global_CFG.EnableXBP)

	case cproto.NetworkModuleType:
		err = cs.cli.SetNetworkConfigList(ctx, args.Cluster, int(args.OperationType), args.Metrics, !cutil.Global_CFG.EnableXBP)

	case cproto.VolumeModuleType:
		if args.VolName == nil {
			err = fmt.Errorf("卷名不能为空！")
		} else {
			err = cs.cli.SetVolumeConfigList(ctx, args.Cluster, int(args.OperationType), args.Metrics, *args.VolName, !cutil.Global_CFG.EnableXBP)
		}

	case cproto.BatchModuleType:
		err = cs.cli.SetBatchConfigList(ctx, args.Cluster, int(args.OperationType), args.Metrics, !cutil.Global_CFG.EnableXBP)

	case cproto.KeyValueModuleType:
		operationMsg, err = cs.cli.SetKeyValueConfigList(ctx, args.Cluster, int(args.OperationType), args.Metrics, !cutil.Global_CFG.EnableXBP)

	case cproto.FileMigrateModuleType:
		err = cs.cli.SetFileMigrateConfigList(ctx, args.Cluster, int(args.OperationType), args.Metrics, !cutil.Global_CFG.EnableXBP)

	default:
		err = fmt.Errorf("undefined module type: %v:%v", args.ModuleType, "")
	}
	return cproto.BuildResponse(err), nil
}

func getRatelimitOpcode(cluster string, operation int) []*cproto.CliOpMetric {
	var (
		opcodeList   []uint8
		opcodeExtend []int
	)
	isRelease := cproto.IsRelease(cluster)

	switch operation {
	case cproto.OpMetanodeRateLimit:
		if isRelease {
			opcodeList = cproto.ReleaseMetaRatelimitOpList
		} else {
			opcodeList = cproto.MetaRatelimitOpList
		}

	case cproto.OpDatanodeRateLimit:
		if isRelease {
			opcodeList = cproto.ReleaseDataRatelimitOpList
			opcodeExtend = cproto.ReleaseDataRatelimitOpList_ext
		} else {
			opcodeList = cproto.DataRatelimitOpList
			opcodeExtend = cproto.DataRatelimitOpList_ext
		}

	case cproto.OpFlashNodeZoneRate, cproto.OpFlashNodeVolRate, cproto.OpFlashnodeRateLimit:
		opcodeList = cproto.FlashRatelimitOpList

	case cproto.OpClientVolOpRateLimit:
		// todo: 这些opcode release是否支持
		opcodeList = cproto.ClientRatelimitOpList

	case cproto.OpApiReqBwRateLimit:
		if isRelease {
			opcodeList = cproto.ReleaseApiReqBwRateLimitOpList
		} else {
			opcodeList = cproto.ApiReqBwRateLimitOpList
		}

	default:
		return nil
	}

	result := make([]*cproto.CliOpMetric, 0)
	for _, opcode := range opcodeList {
		metric := new(cproto.CliOpMetric)
		metric.OpCode = strconv.Itoa(int(opcode))
		if operation == cproto.OpApiReqBwRateLimit {
			metric.OpMsg = cproto.GetApiOpMsg(opcode)
		} else {
			metric.OpMsg = cproto.GetOpMsg(opcode)
		}
		if metric.OpMsg == "" {
			metric.OpMsg = metric.OpCode
		}
		result = append(result, metric)
	}
	for _, opcode := range opcodeExtend {
		metric := new(cproto.CliOpMetric)
		metric.OpCode = strconv.Itoa(int(opcode))
		metric.OpMsg = proto.GetOpMsgExtend(opcode)
		if metric.OpMsg == "" {
			metric.OpMsg = metric.OpCode
		}
		result = append(result, metric)
	}
	return result
}

func getRatelimitIndexOpMetrics() []*cproto.CliOpMetric {
	opMetrics := make([]*cproto.CliOpMetric, 0, cproto.IndexTypeMax)
	for i := cproto.RatelimitIndexType(0); i < cproto.IndexTypeMax; i++ {
		metric := cproto.NewCliOpMetric(strconv.Itoa(int(i)), cproto.GetRatelimitIndexMsg(i))
		opMetrics = append(opMetrics, metric)
	}
	return opMetrics
}

func filterMetricsList(metricsList [][]*cproto.CliValueMetric, filterList []*cproto.CliValueFilter) [][]*cproto.CliValueMetric {
	result := make([][]*cproto.CliValueMetric, 0)
	filters := cproto.FormatFiltersToMap(filterList)
	if len(metricsList) == 0 {
		return metricsList
	}
	for _, metrics := range metricsList {
		match := true
		for _, metric := range metrics {
			if metric.IsFilter == 0 {
				continue
			}
			// 需要前端做判断，没有填值的不传过来
			if _, ok := filters[metric.ValueName]; !ok {
				continue
			}
			if filters[metric.ValueName] != metric.Value {
				match = false
				break
			}
		}
		if match {
			result = append(result, metrics)
		}
	}
	return result
}

func pagingMetricsList(page, pageSize int, metricsList [][]*cproto.CliValueMetric) [][]*cproto.CliValueMetric {
	if page <= 0 {
		page = 1
	}
	if pageSize <= 0 {
		pageSize = 10
	}
	if pageSize >= len(metricsList) {
		return metricsList
	}
	start := (page - 1) * pageSize
	end := page * pageSize
	if end > len(metricsList) {
		end = len(metricsList)
	}
	return metricsList[start:end]
}

func (cs *CliService) getModuleList(cluster string) []*cproto.CliOpMetric {
	module := make([]string, 0)
	module = append(module, strings.ToLower(cproto.RoleNameMaster))
	module = append(module, strings.ToLower(cproto.ModuleDataNode))
	module = append(module, strings.ToLower(cproto.ModuleMetaNode))
	if !cproto.IsRelease(cluster) {
		module = append(module, strings.ToLower(cproto.RoleNameFlashNode))
	}
	opMetrics := make([]*cproto.CliOpMetric, 0, len(module))
	for _, m := range module {
		metric := cproto.NewCliOpMetric(m, m)
		opMetrics = append(opMetrics, metric)
	}
	return opMetrics
}

func (cs *CliService) getZoneOpMetrics(cluster string, operation int) []*cproto.CliOpMetric {
	var (
		zones []string
	)
	switch operation {
	case cproto.OpFlashNodeVolRate, cproto.OpFlashNodeZoneRate, cproto.OpFlashnodeRateLimit:
		zones = cs.cli.GetFlashnodeZoneList(cluster)
	default:
		zones = cs.cli.GetDatanodeZoneList(cluster)
	}

	opMetrics := make([]*cproto.CliOpMetric, 0, len(zones))
	if zones == nil {
		// 	此情况为release集群
		opMetrics = append(opMetrics, &cproto.CliOpMetric{
			OpCode: cproto.EmptyZoneVolFlag,
			OpMsg:  cproto.EmptyZoneVolFlag,
		})
		return opMetrics
	}
	for _, zone := range zones {
		metric := cproto.NewCliOpMetric(zone, zone)
		opMetrics = append(opMetrics, metric)
	}
	return opMetrics
}

func (cs *CliService) getVolList(cluster string, keywords *string) []*cproto.CliOpMetric {
	vols := cs.cli.GetVolList(cluster)
	if vols == nil {
		return nil
	}

	opMetrics := make([]*cproto.CliOpMetric, 0, len(vols))
	for _, vol := range vols {
		if keywords != nil {
			key := strings.TrimSpace(*keywords)
			if !strings.HasPrefix(vol, key) {
				// 不包含前缀 不添加
				continue
			}
		}
		metric := cproto.NewCliOpMetric(vol, vol)
		opMetrics = append(opMetrics, metric)
	}
	// 返回部分数据
	if len(opMetrics) > cproto.MaxVolumeListBatch {
		opMetrics = opMetrics[:cproto.MaxVolumeListBatch]
	}
	return opMetrics
}

func getObjectActionList() []*cproto.CliOpMetric {
	actions := proto.AllActions
	opMetrics := make([]*cproto.CliOpMetric, 0, len(actions))

	for _, action := range actions {
		metric := cproto.NewCliOpMetric(action.String(), action.String())
		opMetrics = append(opMetrics, metric)
	}
	return opMetrics
}

func (cs *CliService) DoXbpApply(apply *model.XbpApplyInfo) (err error) {
	operationMsg := cproto.GetOpShortMsg(apply.OperationCode)
	module := cproto.GetModule(apply.ModuleType)
	defer func() {
		var opStatus int
		msg := fmt.Sprintf("doXbpApply: ticketID(%v) module(%v) operation(%v) pin(%v) params(%v)", apply.TicketID, module, operationMsg, apply.Pin, apply.Params)
		if err != nil {
			opStatus = model.CliOperationStatusFailed
			model.XbpApplyInfo{}.UpdateApplyStatus(apply.TicketID, cproto.XBP_DelpoyFailed)
			log.LogErrorf("%v: failed, err(%v)", msg, err)
		} else {
			opStatus = model.CliOperationStatusSuccess
			model.XbpApplyInfo{}.UpdateApplyStatus(apply.TicketID, cproto.XBP_Approved)
			log.LogInfof("%v: success", msg)
		}
		cliRecord := model.NewCliOperation(
			apply.Cluster,
			module,
			operationMsg,
			apply.Pin,
			fmt.Sprintf(apply.Params),
			apply.TicketID,
			opStatus,
		)
		cliRecord.SetCliRecordVolume(apply.Volume)
		cliRecord.SetErrMsg(err)
		cliRecord.InsertRecord(cliRecord)
	}()
	var argsMetric [][]*cproto.CliValueMetric
	if err = json.Unmarshal([]byte(apply.Params), &argsMetric); err != nil {
		log.LogErrorf("unmarshal argsMetrics failed: apply: %v, err(%v)", apply, err)
		return
	}

	if len(argsMetric) == 0 {
		err = fmt.Errorf("no available value metrics")
		return
	}
	if !apply.OperationIsList && len(argsMetric) > 1 {
		err = fmt.Errorf("non-list operation can only have one value metric")
		return
	}

	switch apply.ModuleType {
	case cproto.ClusterModuleType:
		if apply.OperationIsList {
			err = fmt.Errorf("non-list operation: %v", operationMsg)
		} else {
			err = cs.cli.SetClusterConfig(nil, apply.Cluster, apply.OperationCode, argsMetric[0], true)
		}

	case cproto.DataNodeModuleType:
		if apply.OperationIsList {
			err = cs.cli.SetDataNodeConfigList(nil, apply.Cluster, apply.OperationCode, argsMetric, true)
		} else {
			err = cs.cli.SetDataNodeConfig(nil, apply.Cluster, apply.OperationCode, argsMetric[0], true)
		}

	case cproto.MetaNodeModuleType:
		if apply.OperationIsList {
			err = cs.cli.SetMetaNodeConfigList(nil, apply.Cluster, apply.OperationCode, argsMetric, true)
		} else {
			err = cs.cli.SetMetaNodeConfig(nil, apply.Cluster, apply.OperationCode, argsMetric[0], true)
		}

	case cproto.NetworkModuleType:
		if apply.OperationIsList {
			err = cs.cli.SetNetworkConfigList(nil, apply.Cluster, apply.OperationCode, argsMetric, true)
		} else {
			err = cs.cli.SetNetworkConfig(nil, apply.Cluster, apply.OperationCode, argsMetric[0], true)
		}

	case cproto.RateLimitModuleType:
		if apply.OperationIsList {
			_, err = cs.cli.SetRatelimitConfigList(nil, apply.Cluster, apply.OperationCode, argsMetric, true)
		} else {
			err = cs.cli.SetRatelimitConfig(nil, apply.Cluster, apply.OperationCode, argsMetric[0], true)
		}

	case cproto.VolumeModuleType:
		if apply.OperationIsList {
			err = cs.cli.SetVolumeConfigList(nil, apply.Cluster, apply.OperationCode, argsMetric, apply.Volume, true)
		} else {
			err = cs.cli.SetVolumeConfig(nil, apply.Cluster, apply.OperationCode, argsMetric[0], apply.Volume, true)
		}

	case cproto.BatchModuleType:
		if !apply.OperationIsList {
			err = fmt.Errorf("only-list operation: %v", operationMsg)
		} else {
			err = cs.cli.SetBatchConfigList(nil, apply.Cluster, apply.OperationCode, argsMetric, true)
		}

	case cproto.KeyValueModuleType:
		if !apply.OperationIsList {
			err = fmt.Errorf("only-list operation: %v", operationMsg)
		} else {
			operationMsg, err = cs.cli.SetKeyValueConfigList(nil, apply.Cluster, apply.OperationCode, argsMetric, true)
		}
	case cproto.FileMigrateModuleType:
		if !apply.OperationIsList {
			err = fmt.Errorf("only-list operation: %v", operationMsg)
		} else {
			err = cs.cli.SetFileMigrateConfigList(nil, apply.Cluster, apply.OperationCode, argsMetric, true)
		}
	}
	return err
}

func (cs *CliService) getOperationHistory(ctx context.Context, args struct {
	Cluster       string
	ModuleType    int32
	OperationType int32
	Page          int32
	PageSize      int32
}) (*cproto.CliOperationHistoryResponse, error) {
	// 查表，操作记录， 结果入表
	module := cproto.GetModule(int(args.ModuleType))
	operation := cproto.GetOpShortMsg(int(args.OperationType))
	total, records, err := cs.cli.GetOperationHistory(args.Cluster, module, operation, int(args.Page), int(args.PageSize))
	if err != nil {
		log.LogErrorf("GetOperationHistory failed: cluster(%s) module(%s) operation(%s) err(%v)", args.Cluster, module, operation, err)
		return nil, err
	}
	return &cproto.CliOperationHistoryResponse{
		Total:   int(total),
		Records: records,
	}, nil
}
