package service

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/console/cutil"
	"github.com/cubefs/cubefs/console/model"
	cproto "github.com/cubefs/cubefs/console/proto"
	api "github.com/cubefs/cubefs/console/service/apiManager"
	cluster_service "github.com/cubefs/cubefs/console/service/cluster"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/http_client"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/util/log"
	"github.com/samsarahq/thunder/graphql"
	"github.com/samsarahq/thunder/graphql/schemabuilder"
)

type ClusterService struct {
	globalClusterMutex sync.Mutex // 仅在该结构中修改
	sreDomainName      string     // todo: remove
	masterAddrs        map[string][]string
	clusters           []*model.ConsoleCluster
	api                *api.APIManager
	rdma               *cluster_service.RDMAService

	clusterInfoCache []*model.ClusterInfo // 集群缓存
	cacheExpire      time.Time
}

func NewClusterService(clusters []*model.ConsoleCluster) *ClusterService {
	masters := make(map[string][]string)
	for _, clusterInfo := range clusters {
		addrList := strings.Split(clusterInfo.MasterAddrs, ",")
		masters[clusterInfo.ClusterName] = addrList
	}

	log.LogInfof("NewClusterService: masters(%v)", masters)
	cs := &ClusterService{
		masterAddrs: masters,
		clusters:    clusters,
		api:         api.NewAPIManager(clusters),
	}
	cs.rdma = cluster_service.NewRDMAService(cs.api)
	return cs
}

func (cs *ClusterService) SType() cproto.ServiceType {
	return cproto.ClusterService
}

func (cs *ClusterService) Schema() *graphql.Schema {
	schema := schemabuilder.NewSchema()

	cs.registerObject(schema)
	cs.registerQuery(schema)
	cs.registerMutation(schema)
	cs.registerRDMAAction(schema)
	return schema.MustBuild()
}

func (cs *ClusterService) registerObject(schema *schemabuilder.Schema) {}

func (cs *ClusterService) registerQuery(schema *schemabuilder.Schema) {
	query := schema.Query()
	query.FieldFunc("clusterInfoList", cs.clusterInfoList)            // 数据库查询操作
	query.FieldFunc("healthInfo", cs.clusterHealthInfo)               // 数据库查询 & format, mutation(可先不做)
	query.FieldFunc("healthHistoryInfo", cs.clusterHealthHistoryInfo) // 数据库查询 + topo接口
	query.FieldFunc("zoneList", cs.getZoneList)                       // todo: 这个zoneList有问题，有些不用的了zone还在列表
	query.FieldFunc("clusterCapacity", cs.clusterZoneCurve)           // 数据库查询-zone容量曲线
	query.FieldFunc("zoneUsageOverView", cs.zoneUsageOverView)        // 各zone使用率柱状图
	query.FieldFunc("sourceUsageOverview", cs.sourceUsageOverview)    // source使用率曲线
	query.FieldFunc("sourceList", cs.getSourceListFromJED)

	query.FieldFunc("clusterView", cs.clusterView)
	query.FieldFunc("masterList", cs.masterList)
	query.FieldFunc("dataNodeList", cs.dataNodeList)
	query.FieldFunc("metaNodeList", cs.metaNodeList)
	query.FieldFunc("metaPartitionList", cs.metaPartitionList)
	query.FieldFunc("dataPartitionList", cs.dataPartitionList)
	//query.FieldFunc("alarmList", cs.alarmList)
	query.FieldFunc("flashGroupList", cs.flashGroupList) // 带分页(个位数 检索由前端做)
	query.FieldFunc("flashNodeList", cs.flashNodeList)   // 增加入参fgID
	query.FieldFunc("getFlashGroup", cs.getFlashGroup)
	query.FieldFunc("dataNodeView", cs.getDataNodeView)
}

// todo: 要想xbp回调，操作需要单独封装函数
func (cs *ClusterService) registerMutation(schema *schemabuilder.Schema) {
	mutation := schema.Mutation()
	// data
	mutation.FieldFunc("resetDataNode", cs.resetDataNode)
	mutation.FieldFunc("startRiskFixDataNode", cs.startRiskFixDataNode)
	mutation.FieldFunc("stopRiskFixDataNode", cs.stopRiskFixDataNode)
	mutation.FieldFunc("decommissionDataNode", cs.decommissionDataNode)
	mutation.FieldFunc("decommissionDisk", cs.decommissionDisk)
	mutation.FieldFunc("decommissionDataPartition", cs.decommissionDataPartition)
	mutation.FieldFunc("reloadDataPartition", cs.reloadDataPartition) //加载
	mutation.FieldFunc("resetDataPartition", cs.resetDataPartition)   //重置
	mutation.FieldFunc("stopDataPartition", cs.stopDataPartition)
	mutation.FieldFunc("setRecoverDataPartition", cs.setRecoverDataPartition)
	mutation.FieldFunc("addDataReplica", cs.addDataReplica)
	mutation.FieldFunc("deleteDataReplica", cs.deleteDataReplica)
	mutation.FieldFunc("addDataLearner", cs.addDataLearner)
	mutation.FieldFunc("promoteDataLearner", cs.promoteDataLearner)
	// meta
	mutation.FieldFunc("resetMetaNode", cs.resetMetaNode)
	mutation.FieldFunc("decommissionMetaNode", cs.decommissionMetaNode)
	mutation.FieldFunc("decommissionFlashNode", cs.decommissionFlashNode)
	mutation.FieldFunc("decommissionMetaPartition", cs.decommissionMetaPartition)
	mutation.FieldFunc("reloadMetaPartition", cs.reloadMetaPartition)
	mutation.FieldFunc("resetMetaPartition", cs.resetMetaPartition)
	mutation.FieldFunc("resetCursor", cs.resetCursor)
	mutation.FieldFunc("addMetaReplica", cs.addMetaReplica)
	mutation.FieldFunc("deleteMetaReplica", cs.deleteMetaReplica)
	mutation.FieldFunc("addMetaLearner", cs.addMetaLearner)
	mutation.FieldFunc("promoteMetaLearner", cs.promoteMetaLearner)
	// flashnode
	mutation.FieldFunc("createFlashGroup", cs.createFlashGroup)
	mutation.FieldFunc("removeFlashGroup", cs.removeFlashGroup)
	mutation.FieldFunc("updateFlashGroup", cs.updateFlashGroup) // 状态 删除节点 增加节点，入参形式
	mutation.FieldFunc("updateFlashNode", cs.updateFlashNode)
	mutation.FieldFunc("setPingSortFlashNode", cs.setPingSortFlashNode)
	mutation.FieldFunc("setStackReadFlashNode", cs.setStackReadFlashNode)
	mutation.FieldFunc("setTimeoutFlashNode", cs.setTimeoutFlashNode)
	mutation.FieldFunc("evictCacheFlashNode", cs.evictCacheFlashNode)
}

func (cs *ClusterService) registerRDMAAction(schema *schemabuilder.Schema) {
	// 查询操作
	query := schema.Query()
	query.FieldFunc("rdmaClusterConf", cs.rdmaClusterConf)
	query.FieldFunc("rdmaDataNodeList", cs.rdmaDataNodeList)
	query.FieldFunc("rdmaMetaNodeList", cs.rdmaMetaNodeList)
	query.FieldFunc("rdmaVolumeList", cs.rdmaVolumeList)
	query.FieldFunc("rdmaNodeView", cs.rdmaNodeView)
	//query.FieldFunc("nodeRdmaConn", cs.getNodeRdmaConn) // todo?
	// 修改操作
	mutation := schema.Mutation()
	mutation.FieldFunc("setClusterRdmaConf", cs.setClusterRdmaConf)
	mutation.FieldFunc("setNodeRdmaConf", cs.setNodeRdmaConf)
	mutation.FieldFunc("setVolumeRdmaConf", cs.setVolumeRdmaConf)
	mutation.FieldFunc("setSendEnable", cs.setNodeSendEnable) // 本端发送开关
	mutation.FieldFunc("setConnEnable", cs.setNodeConnEnable) // 对端发送开关
}

func (cs *ClusterService) DoXbpApply(apply *model.XbpApplyInfo) error {
	// todo: 集群操作增加xbp审批
	return nil
}

func (cs *ClusterService) getCluster(name string) *model.ConsoleCluster {
	clusters := cs.clusters
	for _, cluster := range clusters {
		if cluster.ClusterName == name {
			return cluster
		}
	}
	return nil
}

func (cs *ClusterService) clusterInfoList(ctx context.Context, args struct{}) ([]*model.ClusterInfo, error) {
	// 用内存cache，避免每次查询数据库
	if cs.clusterInfoCache == nil || cs.cacheExpire.Before(time.Now()) {
		clusters := make([]string, 0)
		for cluster, _ := range cs.masterAddrs {
			clusters = append(clusters, cluster)
		}
		result, _ := model.ClusterInfo{}.GetClusterListFromMysql(clusters)
		var isExistFunc = func(name string) bool {
			for _, cluster := range result {
				if cluster.ClusterName == name {
					cluster.ClusterNameZH = cs.getCluster(name).ClusterNameZH
					return true
				}
			}
			return false
		}
		for _, name := range clusters {
			if isExistFunc(name) {
				continue
			}
			result = append(result, cs.clusterSummaryView(cs.getCluster(name)))
		}
		sort.Slice(result, func(i, j int) bool {
			// 字典序降序....spark排前
			return result[i].ClusterName > result[j].ClusterName
		})
		cs.clusterInfoCache = result
		cs.cacheExpire = time.Now().Add(time.Minute * 10)
		return result, nil
	} else {
		return cs.clusterInfoCache, nil
	}
}

func (cs *ClusterService) clusterSummaryView(cluster *model.ConsoleCluster) *model.ClusterInfo {
	return cluster_service.FormatClusterSummaryView(cs.api, cluster)
}

func (cs *ClusterService) clusterHealthInfo(ctx context.Context, args struct {
	Cluster  *string
	RoleName string
	Page     int32
	PageSize int32
}) (*cproto.CFSRoleHealthInfoResponse, error) {
	cluster := cutil.GetClusterParam(args.Cluster)
	var searchCondition = map[string]interface{}{
		"cluster_name": cluster,
		"role":         args.RoleName,
		"status":       model.AbnormalStatus, //告警未消除
	}

	queryRequest := &model.RangeQueryRequest{
		Page:     int(args.Page),
		PageSize: int(args.PageSize),
	}
	alarmInfos, totalNum, err := model.AbnormalRecord{}.LoadAlarmRecordsWithOffset(searchCondition, queryRequest)
	if err != nil {
		return nil, err
	}
	info := new(cproto.CFSRoleHealthInfoResponse)
	info.ClusterName = cluster
	info.RoleName = args.RoleName
	info.TotalNum = totalNum
	info.AlarmList = make([]*cproto.CFSRoleHealthInfo, 0)
	info.AlarmLevel = model.AbnormalAlarmLevelNormal
	for _, alarm := range alarmInfos {
		if alarm.AlarmLevel > info.AlarmLevel {
			info.AlarmLevel = alarm.AlarmLevel
		}
		healthInfo := new(cproto.CFSRoleHealthInfo)
		healthInfo.IpAddr = alarm.IpAddr
		healthInfo.AlarmData = alarm.AlarmData
		if alarm.Role == cproto.ModuleDataNode && alarm.AlarmType == cproto.AbnormalTypeBadDisk && len(alarm.DiskSnInfo) != 0 {
			healthInfo.AlarmData = fmt.Sprintf("%v    %v", alarm.AlarmData, alarm.DiskSnInfo)
		}
		healthInfo.AlarmLevel = alarm.AlarmLevel
		healthInfo.StartTime = alarm.StartTime.Format(time.DateTime)
		healthInfo.AlarmType = alarm.AlarmType
		healthInfo.ID = alarm.ID
		healthInfo.AlarmTypeDes = alarm.AlarmTypeDes
		timeMinute := time.Now().Sub(alarm.StartTime).Minutes()
		healthInfo.DurationTime = durationFormat(int64(timeMinute))
		healthInfo.AlarmOrigin = alarm.AlarmOrigin
		healthInfo.EmergencyResponsible = ""
		healthInfo.IDCStatus = alarm.IDC
		//if healthInfo.IDCStatus {
		//	healthInfo.IDCHandleProcess = cutil.GetOrderStatus(alarm.IDCOrderId)
		//	healthInfo.OrderId = alarm.IDCOrderId
		//}
		if handles, has := cproto.GlobalAlarmTypeToHandle[healthInfo.AlarmType]; has {
			healthInfo.Handles = handles
		} else {
			healthInfo.Handles = make([]*cproto.Operation, 0)
		}
		info.AlarmList = append(info.AlarmList, healthInfo)
	}
	return info, nil
}

func (cs *ClusterService) clusterHealthHistoryInfo(ctx context.Context, args struct {
	Cluster  *string
	RoleName string
}) (*cproto.CFSRoleHealthInfoResponse, error) {
	cluster := cutil.GetClusterParam(args.Cluster)
	alarmInfos, err := model.AbnormalRecord{}.LoadHistoryAlarmRecords(cluster, args.RoleName)
	if err != nil {
		return nil, err
	}
	topology, err := cs.GetTopology(cluster)
	if err != nil {
		log.LogWarnf("clusterHealthHistoryInfo: GetTopology failed: err(%v)", err)
	}
	info := new(cproto.CFSRoleHealthInfoResponse)
	info.ClusterName = cluster
	info.RoleName = args.RoleName
	info.TotalNum = int64(len(alarmInfos))
	info.AlarmList = make([]*cproto.CFSRoleHealthInfo, 0)
	info.AlarmLevel = model.AbnormalAlarmLevelNormal
	for _, alarm := range alarmInfos {
		if alarm.AlarmLevel > info.AlarmLevel {
			info.AlarmLevel = alarm.AlarmLevel
		}
		healthInfo := new(cproto.CFSRoleHealthInfo)
		healthInfo.IpAddr = alarm.IpAddr
		healthInfo.AlarmData = alarm.AlarmData
		healthInfo.AlarmLevel = alarm.AlarmLevel
		healthInfo.StartTime = alarm.StartTime.Format(time.DateTime)
		healthInfo.AlarmType = alarm.AlarmType
		healthInfo.ID = alarm.ID
		healthInfo.AlarmTypeDes = alarm.AlarmTypeDes
		timeMinute := time.Now().Sub(alarm.StartTime).Minutes()
		healthInfo.DurationTime = durationFormat(int64(timeMinute))
		healthInfo.AlarmOrigin = alarm.AlarmOrigin
		healthInfo.EmergencyResponsible = ""
		healthInfo.IDCStatus = alarm.IDC
		if healthInfo.IDCStatus {
			healthInfo.IDCHandleProcess = cutil.GetOrderStatus(alarm.IDCOrderId)
			healthInfo.OrderId = alarm.IDCOrderId
		}
		if checkIsInCluster(healthInfo.IpAddr, info.RoleName, topology) {
			healthInfo.IsInCluster = "是"
		} else {
			healthInfo.IsInCluster = "否"
		}
		info.AlarmList = append(info.AlarmList, healthInfo)
	}
	return info, nil
}

func (cs *ClusterService) getZoneList(ctx context.Context, args struct {
	Cluster *string
}) []string {
	if args.Cluster == nil || *args.Cluster == "" {
		return nil
	}
	return cs.api.GetClusterZoneNameList(*args.Cluster)
}

// todo: 前端修改
func (cs *ClusterService) getZoneListV2(ctx context.Context, args struct {
	Cluster string
}) []*cproto.ZoneInfo {
	zoneNameList := cs.api.GetClusterZoneNameList(args.Cluster)
	result := make([]*cproto.ZoneInfo, 0, len(zoneNameList))
	zoneSources := model.ZoneSourceMapper{}.GetSourcesInZone(zoneNameList)
	var findSource = func(zone string) string {
		for _, zs := range zoneSources {
			if zs.Zone == zone {
				return zs.Sources
			}
		}
		return ""
	}
	for _, zone := range zoneNameList {
		zInfo := &cproto.ZoneInfo{
			ZoneName: zone,
			Sources:  findSource(zone),
		}
		result = append(result, zInfo)
	}
	return result
}

func (cs *ClusterService) clusterZoneCurve(ctx context.Context, args struct {
	Cluster      *string
	ZoneName     string
	IntervalType int32 // 0-自定义时间 1-近1天 2-近1周 3-近1月
	Start        int64 // 秒级
	End          int64
}) ([]*cproto.ClusterResourceData, error) {
	var (
		cluster = cutil.GetClusterParam(args.Cluster)
		result  []*cproto.ClusterResourceData
		err     error
	)

	r := &cproto.CFSClusterResourceRequest{
		ClusterName:  cluster,
		ZoneName:     args.ZoneName,
		IntervalType: int(args.IntervalType),
		StartTime:    args.Start,
		EndTime:      args.End,
	}
	result, err = getZoneCapacityCurve(r)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (cs *ClusterService) zoneUsageOverView(ctx context.Context, args struct {
	Cluster string
}) ([]*cproto.ZoneUsageOverview, error) {
	return cs.GetZoneUsageOverview(args.Cluster)
}

func (cs *ClusterService) sourceUsageOverview(ctx context.Context, args struct {
	Cluster      string
	IntervalType int32 // 0-自定义时间 1-近1天 2-近1周 3-近1月
	Start        int64 // 秒级
	End          int64
	Source       *string //todo: 下拉列表接口
	Zone         *string //source zone 都是过滤项
}) (*cproto.SourceUsageResponse, error) {
	var (
		zone   string
		source string
	)
	if args.Zone != nil {
		zone = *args.Zone
	}
	if args.Source != nil {
		source = *args.Source
	}

	data, err := cs.getSourceUsageInfo(args.Cluster, source, zone, args.IntervalType, args.Start, args.End)
	if err != nil {
		return nil, err
	}
	return &cproto.SourceUsageResponse{
		Data: data,
	}, nil
}

func (cs *ClusterService) getSourceListFromJED(ctx context.Context, args struct {
	Cluster string
}) ([]string, error) {
	return cluster_service.GetSourceList()
}

func (cs *ClusterService) clusterView(ctx context.Context, args struct {
	Cluster *string
}) (*cproto.ConsoleClusterView, error) {
	cluster := cutil.GetClusterParam(args.Cluster)
	return cluster_service.FormatConsoleClusterView(cs.api, cs.getCluster(cluster)), nil
}

func (cs *ClusterService) masterList(ctx context.Context, args struct {
	Cluster  *string
	Page     int32
	PageSize int32
}) *cproto.MasterListResponse {
	var leader string
	cluster := cutil.GetClusterParam(args.Cluster)
	// 获取所有的master 列表
	addrs := cs.masterAddrs[cluster]
	result := make([]*cproto.MasterView, 0, len(addrs))
	if cproto.IsRelease(cluster) {
		cv, err := cs.api.GetClusterViewCacheRelease(cluster, true)
		if err != nil {
			log.LogErrorf("masterList: getClusterViewCache failed: cluster(%v) err(%v)", cluster, err)
			return nil
		}
		leader = cv.LeaderAddr
	} else {
		cv, err := cs.api.GetClusterViewCache(cluster, true)
		if err != nil {
			log.LogErrorf("masterList: getClusterViewCache failed: cluster(%v) err(%v)", cluster, err)
			return nil
		}
		leader = cv.LeaderAddr
	}
	for _, addr := range addrs {
		view := &cproto.MasterView{
			Addr:     addr,
			IsLeader: leader == addr,
		}
		result = append(result, view)
	}
	return &cproto.MasterListResponse{
		Data:  cutil.Paginate(int(args.Page), int(args.PageSize), result),
		Total: len(result),
	}
}

func (cs *ClusterService) metaNodeList(ctx context.Context, args struct {
	Cluster  *string
	Addr     *string
	Zone     *string
	Page     int32
	PageSize int32
}) (*cproto.NodeListResponse, error) {
	nodes := make([]*cproto.NodeViewDetail, 0)
	var (
		cluster = cutil.GetClusterParam(args.Cluster)
		zone    string
		addr    string
	)
	if args.Zone != nil {
		zone = *args.Zone
		if cproto.IsRelease(cluster) {
			zone = ""
		}
	}
	if args.Addr != nil {
		addr = *args.Addr
	}
	nodeList, err := filterMetaNodeList(cluster, zone, addr, cs.api)
	if err != nil {
		return nil, err
	}
	nodePart := cutil.Paginate(int(args.Page), int(args.PageSize), nodeList)
	for _, node := range nodePart {
		if view := getMetaNodeView(cluster, node, cs.api); view != nil {
			nodes = append(nodes, view)
		}
	}
	return cproto.NewNodeListResponse(len(nodeList), nodes), nil
}

func filterMetaNodeList(cluster, zone, host string, api *api.APIManager) ([]string, error) {
	addrPortList := strings.Split(host, ":")
	host = addrPortList[0]

	result := make([]string, 0)
	if zone != "" {
		mc := api.GetMasterClient(cluster)
		topo, err := mc.AdminAPI().GetTopology()
		if err != nil {
			return nil, err
		}
		for _, zoneView := range topo.Zones {
			if zoneView.Name != zone {
				continue
			}
			for _, nodeSet := range zoneView.NodeSet {
				for _, nodeView := range nodeSet.MetaNodes {
					if host != "" && !strings.HasPrefix(nodeView.Addr, host) {
						continue
					}
					result = append(result, nodeView.Addr)
				}
			}
		}
		sort.SliceStable(result, func(i, j int) bool {
			return result[i] < result[j]
		})
		return result, nil
	}
	if cproto.IsRelease(cluster) {
		cv, err := api.GetClusterViewCacheRelease(cluster, false)
		if err != nil {
			return nil, err
		}
		for _, node := range cv.MetaNodes {
			if host != "" && !strings.HasPrefix(node.Addr, host) {
				continue
			}
			result = append(result, node.Addr)
		}
	} else {
		cv, err := api.GetClusterViewCache(cluster, false)
		if err != nil {
			return nil, err
		}
		for _, node := range cv.MetaNodes {
			if host != "" && !strings.HasPrefix(node.Addr, host) {
				continue
			}
			result = append(result, node.Addr)
		}
	}
	return result, nil
}

func getMetaNodeView(cluster, addr string, api *api.APIManager) *cproto.NodeViewDetail {
	//addrPortList := strings.Split(addr, ":")
	//if len(addrPortList) < 2 {
	//	addr = fmt.Sprintf("%s:%s", addr, api.GetNodeProfPort(cluster, cproto.RoleNameMetaNode))
	//}
	if cproto.IsRelease(cluster) {
		rc := api.GetReleaseClient(cluster)
		nodeInfo, err := rc.GetMetaNode(addr)
		if err != nil {
			log.LogErrorf("getMetaNodeView faild: cluster(%v) addr(%v) err(%v)", cluster, addr, err)
			return nil
		}
		return cluster_service.FormatMetaNodeView(nodeInfo, cluster, nil)
	} else {
		mc := api.GetMasterClient(cluster)
		nodeInfo, err := mc.NodeAPI().GetMetaNode(addr)
		if err != nil {
			log.LogErrorf("getMetaNodeView faild: cluster(%v) addr(%v) err(%v)", cluster, addr, err)
			return nil
		}
		clusterInfo := api.GetClusterInfo(cluster)
		clusterRDMAConf, err := cluster_service.GetClusterRDMAConf(cluster, clusterInfo.MasterDomain, "metaNode")
		return cluster_service.FormatMetaNodeView(nodeInfo, cluster, clusterRDMAConf)
	}
}

func (cs *ClusterService) dataNodeList(ctx context.Context, args struct {
	Cluster  *string
	Addr     *string
	Zone     *string
	Page     int32
	PageSize int32
}) (*cproto.NodeListResponse, error) {
	nodes := make([]*cproto.NodeViewDetail, 0)
	var (
		cluster = cutil.GetClusterParam(args.Cluster)
		zone    string
		addr    string
	)
	if args.Zone != nil {
		zone = *args.Zone
		if cproto.IsRelease(cluster) {
			zone = ""
		}
	}
	if args.Addr != nil {
		addr = *args.Addr
	}
	nodeList, err := filterDataNodeList(cluster, zone, addr, cs.api)
	if err != nil {
		return nil, err
	}
	nodePart := cutil.Paginate(int(args.Page), int(args.PageSize), nodeList)
	for _, node := range nodePart {
		if view := getDataNodeView(cluster, node, cs.api); view != nil {
			nodes = append(nodes, view)
		}
	}
	return cproto.NewNodeListResponse(len(nodeList), nodes), nil
}

func filterDataNodeList(cluster, zone, host string, api *api.APIManager) ([]string, error) {
	addrPortList := strings.Split(host, ":")
	host = addrPortList[0]

	result := make([]string, 0)
	if zone != "" {
		mc := api.GetMasterClient(cluster)
		topo, err := mc.AdminAPI().GetTopology()
		if err != nil {
			return nil, err
		}
		for _, zoneView := range topo.Zones {
			if zoneView.Name != zone {
				continue
			}
			for _, nodeSet := range zoneView.NodeSet {
				for _, nodeView := range nodeSet.DataNodes {
					if host != "" && !strings.HasPrefix(nodeView.Addr, host) {
						continue
					}
					result = append(result, nodeView.Addr)
				}
			}
		}
		sort.SliceStable(result, func(i, j int) bool {
			return result[i] < result[j]
		})
		return result, nil
	}
	if cproto.IsRelease(cluster) {
		cv, err := api.GetClusterViewCacheRelease(cluster, false)
		if err != nil {
			return nil, err
		}
		for _, node := range cv.DataNodes {
			if host != "" && !strings.HasPrefix(node.Addr, host) {
				continue
			}
			result = append(result, node.Addr)
		}
	} else {
		cv, err := api.GetClusterViewCache(cluster, false)
		if err != nil {
			return nil, err
		}
		for _, node := range cv.DataNodes {
			if host != "" && !strings.HasPrefix(node.Addr, host) {
				continue
			}
			result = append(result, node.Addr)
		}
	}
	return result, nil
}
func getDataNodeView(cluster, addr string, api *api.APIManager) *cproto.NodeViewDetail {
	if cproto.IsRelease(cluster) {
		rc := api.GetReleaseClient(cluster)
		nodeInfo, err := rc.GetDataNode(addr)
		if err != nil {
			log.LogErrorf("getDataNodeView faild: cluster(%v) addr(%v) err(%v)", cluster, addr, err)
			return nil
		}
		cv, err := api.GetClusterViewCacheRelease(cluster, false)
		if err != nil {
			log.LogWarnf("getDataNodeView: getClusterViewCache failed: err(%v)", err)
		} else {
			for _, node := range cv.DataNodes {
				if addr == node.Addr {
					nodeInfo.IsActive = node.Status
				}
			}
		}
		return cluster_service.FormatDataNodeView(nodeInfo, cluster, nil)
	} else {
		mc := api.GetMasterClient(cluster)
		nodeInfo, err := mc.NodeAPI().GetDataNode(addr)
		if err != nil {
			log.LogErrorf("getDataNodeView faild: cluster(%v) addr(%v) err(%v)", cluster, addr, err)
			return nil
		}
		clusterInfo := api.GetClusterInfo(cluster)
		clusterRDMAConf, err := cluster_service.GetClusterRDMAConf(cluster, clusterInfo.MasterDomain, "dataNode")
		return cluster_service.FormatDataNodeView(nodeInfo, cluster, clusterRDMAConf)
	}
}

func (cs *ClusterService) getDataNodeView(ctx context.Context, args struct {
	Cluster string
	Addr    string
}) (*cproto.NodeViewDetail, error) {
	if cproto.IsRelease(args.Cluster) {
		return nil, cproto.ErrUnSupportOperation
	}
	mc := cs.api.GetMasterClient(args.Cluster)
	dHost := fmt.Sprintf("%s:%s", strings.Split(args.Addr, ":"), mc.DataNodeProfPort)
	dataClient := http_client.NewDataClient(dHost, false)
	partitions, err := dataClient.GetPartitionsFromNode()
	if err != nil {
		log.LogErrorf("getDataNodeView failed: cluster(%v) addr(%v) err(%v)", args.Cluster, args.Addr, err)
		return nil, err
	}
	var nodeView *cproto.NodeViewDetail
	if partitions != nil {
		nodeView.RiskCount = partitions.RiskCount
		nodeView.RiskFixerRunning = partitions.RiskFixerRunning
	}
	return nodeView, nil
}

func (cs *ClusterService) flashGroupList(ctx context.Context, args struct {
	Cluster  string
	ID       *uint64 // 点击ID 展示详细的信息？
	Page     int32
	PageSize int32
}) (*cproto.FlashGroupListResponse, error) {
	cluster := args.Cluster
	if cproto.IsRelease(cluster) {
		return nil, nil
	}
	mc := cs.api.GetMasterClient(cluster)
	fgView, err := mc.AdminAPI().ListFlashGroups(true, true)
	if err != nil {
		log.LogErrorf("flashGroupList failed: err(%v)", err)
		return nil, err
	}
	result := make([]*cproto.ClusterFlashGroupView, 0)
	if args.ID != nil {
		for _, fg := range fgView.FlashGroups {
			if fg.ID == *args.ID {
				result = append(result, cluster_service.FormatFlashGroup(&fg))
			}
		}
		return cproto.NewFlashGroupListResponse(len(result), cutil.Paginate(int(args.Page), int(args.PageSize), result)), nil
	}
	sort.Slice(fgView.FlashGroups, func(i, j int) bool {
		return fgView.FlashGroups[i].ID < fgView.FlashGroups[j].ID
	})
	views := cutil.Paginate(int(args.Page), int(args.PageSize), fgView.FlashGroups)
	for _, view := range views {
		result = append(result, cluster_service.FormatFlashGroup(&view))
	}
	return cproto.NewFlashGroupListResponse(len(fgView.FlashGroups), result), nil
}

func (cs *ClusterService) getFlashGroup(ctx context.Context, args struct {
	Cluster string
	ID      uint64
}) (*cproto.FlashGroupDetail, error) {
	if cproto.IsRelease(args.Cluster) {
		return nil, cproto.ErrUnSupportOperation
	}
	detail := &cproto.FlashGroupDetail{
		Host: make([]string, 0),
		Zone: make([]*cproto.ZoneInfo, 0),
	}
	mc := cs.api.GetMasterClient(args.Cluster)
	fgView, err := mc.AdminAPI().GetFlashGroup(args.ID)
	if err != nil {
		return nil, err
	}
	for zone, nodes := range fgView.ZoneFlashNodes {
		ipList := make([]string, 0)
		for _, node := range nodes {
			ipList = append(ipList, node.Addr)
		}
		zoneInfo := &cproto.ZoneInfo{
			ZoneName:   zone,
			NodeNumber: len(nodes),
			IpList:     ipList,
		}
		detail.Host = append(detail.Host, ipList...)
		detail.Zone = append(detail.Zone, zoneInfo)
	}
	return detail, nil
}

func (cs *ClusterService) createFlashGroup(ctx context.Context, args struct {
	Cluster string
	Slots   *string
}) (*cproto.ClusterFlashGroupView, error) {
	cluster := args.Cluster
	if cproto.IsRelease(cluster) {
		return nil, cproto.ErrUnSupportOperation
	}
	var slots string
	if args.Slots != nil {
		slots = *args.Slots
	}
	mc := cs.api.GetMasterClient(cluster)
	fgView, err := mc.AdminAPI().CreateFlashGroup(slots)
	if err != nil {
		return nil, err
	}
	return cluster_service.FormatFlashGroup(&fgView), err
}

func (cs *ClusterService) updateFlashGroup(ctx context.Context, args struct {
	Cluster      string
	ID           uint64
	IsActive     *bool
	IsAddNode    *bool
	IsDeleteNode *bool
	Level        *int32  // 0-指定节点 1-输zone&count
	Addr         *string // addr & zone&&count 任选
	Zone         *string
	Count        *int32
}) (err error) {
	cluster := args.Cluster
	if cproto.IsRelease(cluster) {
		return fmt.Errorf("集群: %s 不支持缓存", cluster)
	}
	mc := cs.api.GetMasterClient(cluster)
	if args.IsActive != nil {
		_, err = mc.AdminAPI().SetFlashGroup(args.ID, *args.IsActive)
		if err != nil {
			return
		}
		return
	}
	var (
		addr  string
		zone  string
		count int32
	)
	if args.Addr != nil {
		addr = *args.Addr
	}
	if args.Zone != nil {
		zone = *args.Zone
	}
	if args.Count != nil {
		count = *args.Count
	}
	if addr == "" && zone == "" {
		return fmt.Errorf("addr和zone不能同时为空")
	}
	// todo: 根据level 入参传值
	if args.IsAddNode != nil {
		_, err = mc.AdminAPI().FlashGroupAddFlashNode(args.ID, int(count), zone, addr)
	}
	if args.IsDeleteNode != nil {
		_, err = mc.AdminAPI().FlashGroupRemoveFlashNode(args.ID, int(count), zone, addr)
	}
	if err != nil {
		log.LogErrorf("updateFlashGroup failed: fg(%v) err(%v)", args.ID, err)
		return
	}
	return
}

func (cs *ClusterService) removeFlashGroup(ctx context.Context, args struct {
	Cluster string
	ID      uint64
}) (err error) {
	cluster := args.Cluster
	if cproto.IsRelease(cluster) {
		return cproto.ErrUnSupportOperation
	}
	mc := cs.api.GetMasterClient(cluster)
	_, err = mc.AdminAPI().RemoveFlashGroup(args.ID)
	if err != nil {
		return
	}
	return
}

func (cs *ClusterService) flashNodeList(ctx context.Context, args struct {
	Cluster  string
	FgID     *uint64
	Addr     *string
	Page     int32
	PageSize int32
}) (*cproto.FlashNodeListResponse, error) {
	cluster := args.Cluster
	if cproto.IsRelease(cluster) {
		return nil, nil
	}
	result := make([]*cproto.ClusterFlashNodeView, 0)
	view, err := cs.api.CacheManager.GetFlashNodeViewCache(cluster, false)
	if err != nil {
		log.LogErrorf("flashNodeList failed: err(%v)", err)
		return nil, err
	}
	if args.FgID != nil {
		for _, node := range view {
			if node.FlashGroupID == *args.FgID {
				result = append(result, node)
			}
		}
		return cproto.NewFlashNodeListResponse(len(result), cutil.Paginate(int(args.Page), int(args.PageSize), result)), nil
	}
	if args.Addr != nil && *args.Addr != "" {
		for _, node := range view {
			if strings.HasPrefix(node.Addr, *args.Addr) {
				result = append(result, node)
			}
		}
		return cproto.NewFlashNodeListResponse(len(result), cutil.Paginate(int(args.Page), int(args.PageSize), result)), nil
	}
	sort.SliceStable(view, func(i, j int) bool {
		return view[i].ID < view[j].ID
	})
	return cproto.NewFlashNodeListResponse(len(view), cutil.Paginate(int(args.Page), int(args.PageSize), view)), nil
}

func (cs *ClusterService) updateFlashNode(ctx context.Context, args struct {
	Cluster string
	Addr    string
	State   bool // isEnable: true/false
}) (err error) {
	cluster := args.Cluster
	defer func() {
		msg := fmt.Sprintf("updateFlashNode:%v-%v", args.Addr, args.State)
		if err == nil {
			log.LogInfof("%s success", msg)
			record := model.NewNodeOperation(
				cluster,
				cproto.ModuleFlashNode,
				args.Addr,
				proto.AdminSetFlashNode,
				ctx.Value(cutil.PinKey).(string),
				strconv.FormatBool(args.State),
			)
			record.InsertRecord(record)
		} else {
			log.LogErrorf("%s, err:%v", msg, err)
		}
	}()
	if cproto.IsRelease(cluster) {
		return fmt.Errorf("集群: %s 不支持缓存", cluster)
	}
	mc := cs.api.GetMasterClient(cluster)
	err = mc.NodeAPI().SetFlashNodeState(args.Addr, strconv.FormatBool(args.State))
	return
}

func (cs *ClusterService) setPingSortFlashNode(ctx context.Context, args struct {
	Cluster string
	Addr    string
	State   bool
}) (err error) {
	cluster := args.Cluster
	defer func() {
		msg := fmt.Sprintf("setPingSort:%v-%v", args.Addr, args.State)
		if err == nil {
			log.LogInfof("%s success", msg)
			record := model.NewNodeOperation(
				cluster,
				cproto.ModuleFlashNode,
				args.Addr,
				cproto.FlashNodeSetPingSort,
				ctx.Value(cutil.PinKey).(string),
				strconv.FormatBool(args.State),
			)
			record.InsertRecord(record)
		} else {
			log.LogErrorf("%s, err:%v", msg, err)
		}
	}()
	if cproto.IsRelease(cluster) {
		return fmt.Errorf("集群: %s 不支持缓存", cluster)
	}
	mc := cs.api.GetMasterClient(cluster)
	if args.Addr == "all" {
		view, err := cs.api.CacheManager.GetFlashNodeViewCache(cluster, true)
		if err != nil {
			return err
		}
		errHosts := make([]string, 0)
		for _, node := range view {
			err = setFlashNodePingSort(node.Addr, mc.FlashNodeProfPort, args.State)
			if err != nil {
				errHosts = append(errHosts, node.Addr)
				continue
			}
		}
		if len(errHosts) != 0 {
			return fmt.Errorf("totalCount:%v failedCount: %v host: %v", len(view), len(errHosts), errHosts)
		}
	} else {
		err = setFlashNodePingSort(args.Addr, mc.FlashNodeProfPort, args.State)
	}

	return err
}

func setFlashNodePingSort(host string, port uint16, enable bool) error {
	fnClient := http_client.NewFlashClient(fmt.Sprintf("%v:%v", strings.Split(host, ":")[0], port), false)
	err := fnClient.SetFlashNodePing(enable)
	if err != nil {
		log.LogErrorf("setFlashNodePingSort failed: host(%v) err(%v)", host, err)
	}
	return err
}

func (cs *ClusterService) setStackReadFlashNode(ctx context.Context, args struct {
	Cluster string
	Addr    string
	State   bool
}) (err error) {

	cluster := args.Cluster
	defer func() {
		msg := fmt.Sprintf("setStackRead:%v-%v", args.Addr, args.State)
		if err == nil {
			log.LogInfof("%s success", msg)
			record := model.NewNodeOperation(
				cluster,
				cproto.ModuleFlashNode,
				args.Addr,
				cproto.FlashNodeSetStackRead,
				ctx.Value(cutil.PinKey).(string),
				strconv.FormatBool(args.State),
			)
			record.InsertRecord(record)
		} else {
			log.LogErrorf("%s, err:%v", msg, err)
		}
	}()
	if cproto.IsRelease(cluster) {
		return fmt.Errorf("集群: %s 不支持缓存", cluster)
	}
	mc := cs.api.GetMasterClient(cluster)
	if args.Addr == "all" {
		view, err := cs.api.CacheManager.GetFlashNodeViewCache(cluster, true)
		if err != nil {
			return err
		}
		errHosts := make([]string, 0)
		for _, node := range view {
			err = setFlashNodeStackRead(node.Addr, mc.FlashNodeProfPort, args.State)
			if err != nil {
				errHosts = append(errHosts, node.Addr)
				continue
			}
		}
		if len(errHosts) != 0 {
			return fmt.Errorf("totalCount:%v failedCount: %v host: %v", len(view), len(errHosts), errHosts)
		}
	} else {
		err = setFlashNodeStackRead(args.Addr, mc.FlashNodeProfPort, args.State)
	}

	return err
}

func setFlashNodeStackRead(host string, port uint16, enable bool) error {
	fnClient := http_client.NewFlashClient(fmt.Sprintf("%v:%v", strings.Split(host, ":")[0], port), false)
	err := fnClient.SetFlashNodeStack(enable)
	if err != nil {
		log.LogErrorf("setFlashNodeStackRead failed: host(%v) err(%v)", host, err)
	}
	return err
}

func (cs *ClusterService) setTimeoutFlashNode(ctx context.Context, args struct {
	Cluster   string
	Addr      string
	TimeoutMs uint64
}) (err error) {

	cluster := args.Cluster
	defer func() {
		msg := fmt.Sprintf("setTimeoutMs:%v-%v", args.Addr, args.TimeoutMs)
		if err == nil {
			log.LogInfof("%s success", msg)
			record := model.NewNodeOperation(
				cluster,
				cproto.ModuleFlashNode,
				args.Addr,
				cproto.FlashNodeSetTimeoutMs,
				ctx.Value(cutil.PinKey).(string),
				strconv.FormatUint(args.TimeoutMs, 10),
			)
			record.InsertRecord(record)
		} else {
			log.LogErrorf("%s, err:%v", msg, err)
		}
	}()
	if cproto.IsRelease(cluster) {
		return fmt.Errorf("集群: %s 不支持缓存", cluster)
	}
	mc := cs.api.GetMasterClient(cluster)
	if args.Addr == "all" {
		view, err := cs.api.CacheManager.GetFlashNodeViewCache(cluster, true)
		if err != nil {
			return err
		}
		errHosts := make([]string, 0)
		for _, node := range view {
			err = setFlashNodeTimeoutMs(node.Addr, mc.FlashNodeProfPort, args.TimeoutMs)
			if err != nil {
				errHosts = append(errHosts, node.Addr)
				continue
			}
		}
		if len(errHosts) != 0 {
			return fmt.Errorf("totalCount:%v failedCount: %v host: %v", len(view), len(errHosts), errHosts)
		}
	} else {
		err = setFlashNodeTimeoutMs(args.Addr, mc.FlashNodeProfPort, args.TimeoutMs)
	}
	return err
}

func setFlashNodeTimeoutMs(host string, port uint16, timeoutMs uint64) error {
	fnClient := http_client.NewFlashClient(fmt.Sprintf("%v:%v", strings.Split(host, ":")[0], port), false)
	err := fnClient.SetFlashNodeReadTimeout(int(timeoutMs))
	if err != nil {
		log.LogErrorf("setFlashNodeTimeoutMs failed: host(%v) err(%v)", host, err)
	}
	return err
}

func (cs *ClusterService) evictCacheFlashNode(ctx context.Context, args struct {
	Cluster string
	Level   uint32 // 0-按vol 1-按节点
	Addr    *string
	Volume  *string
	Inode   *uint64 // 0 - 表示所有节点
}) (err error) {
	cluster := args.Cluster
	if args.Volume == nil && args.Addr == nil {
		return fmt.Errorf("请指定卷或节点！")
	}
	mc := cs.api.GetMasterClient(cluster)
	view, err := cs.api.CacheManager.GetFlashNodeViewCache(cluster, true)
	if err != nil {
		return err
	}
	if args.Level == 0 {
		// 按vol的清理的逻辑
		if args.Volume == nil {
			return fmt.Errorf("请输入待清理的vol！")
		}
		if args.Addr == nil && args.Inode == nil {
			return fmt.Errorf("请至少输入一项：节点地址或inode！")
		}
		_, err = mc.AdminAPI().GetVolumeSimpleInfo(*args.Volume)
		if err != nil {
			return err
		}
		err = evictVolCache(*args.Volume, args.Addr, args.Inode, mc.FlashNodeProfPort, view)
	}
	if args.Level == 1 {
		// 按节点清理的逻辑
		// 从列表回显的 不会有addr不存在的问题
		err = evictAllCache(*args.Addr, mc.FlashNodeProfPort)
	}
	return err
}

func evictVolCache(volume string, addr *string, inode *uint64, port uint16, nodeViews []*cproto.ClusterFlashNodeView) error {
	var (
		host string
		ino  uint64
		err  error
	)
	if addr != nil {
		host = *addr
	}
	if inode != nil {
		ino = *inode
	}
	// todo: 校验inode是否存在, 不存在会否清掉所有的cache？
	for _, node := range nodeViews {
		if host != "" && node.Addr != host {
			continue
		}
		fnClient := http_client.NewFlashClient(fmt.Sprintf("%v:%v", strings.Split(host, ":")[0], port), false)
		if ino > 0 {
			err = fnClient.EvictInode(volume, ino)
		} else {
			err = fnClient.EvictVol(volume)
		}
		if err != nil {
			log.LogWarnf("evictVolCache: vol(%v) targetHost(%v) targetIno(%v) currHost(%v) err(%v)", volume, host, ino, node.Addr, err)
		}
	}
	return err
}

func evictAllCache(host string, port uint16) error {
	fnClient := http_client.NewFlashClient(fmt.Sprintf("%v:%v", strings.Split(host, ":")[0], port), false)
	err := fnClient.EvictAll()
	if err != nil {
		log.LogErrorf("evictAllCache failed: host(%v) err(%v)", host, err)
	}
	return err
}

func (cs *ClusterService) metaPartitionList(ctx context.Context, args struct {
	Cluster  string
	Addr     *string
	ID       *uint64
	Page     *int32
	PageSize *int32
}) (*cproto.PartitionListResponse, error) {
	cluster := args.Cluster
	result := make([]*cproto.PartitionViewDetail, 0)
	if args.ID != nil {
		result = getMetaPartition(cluster, []uint64{*args.ID}, cs.api)
		return cproto.NewPartitionListResponse(len(result), result), nil
	}
	var partitions []uint64
	if args.Addr != nil && *args.Addr != "" {
		if cproto.IsRelease(cluster) {
			rc := cs.api.GetReleaseClient(cluster)
			nodeInfo, err := rc.GetMetaNode(*args.Addr)
			if err != nil {
				log.LogErrorf("metaPartitionList: getMetaNode failed, cluster(%v) addr(%v) err(%v)", cluster, *args.Addr, err)
				return nil, err
			}
			partitions = nodeInfo.PersistenceMetaPartitions
		} else {
			mc := cs.api.GetMasterClient(cluster)
			nodeInfo, err := mc.NodeAPI().GetMetaNode(*args.Addr)
			if err != nil {
				log.LogErrorf("metaPartitionList: getMetaNode failed, cluster(%v) addr(%v) err(%v)", cluster, *args.Addr, err)
				return nil, err
			}
			partitions = nodeInfo.PersistenceMetaPartitions
		}
	}
	sort.SliceStable(partitions, func(i, j int) bool {
		return partitions[i] < partitions[j]
	})
	if args.Page != nil && args.PageSize != nil {
		page := *args.Page
		pageSize := *args.PageSize
		mps := cutil.Paginate(int(page), int(pageSize), partitions)
		result = getMetaPartition(cluster, mps, cs.api)
	}
	return cproto.NewPartitionListResponse(len(partitions), result), nil
}

func getMetaPartition(cluster string, mpIDs []uint64, api *api.APIManager) []*cproto.PartitionViewDetail {
	sort.SliceStable(mpIDs, func(i, j int) bool {
		return mpIDs[i] < mpIDs[j]
	})
	result := make([]*cproto.PartitionViewDetail, 0, len(mpIDs))
	if cproto.IsRelease(cluster) {
		rc := api.GetReleaseClient(cluster)
		for _, mpID := range mpIDs {
			mp, err := rc.GetMetaPartition(mpID)
			if err != nil {
				log.LogWarnf("getMetaPartition: getMpInfo failed, cluster(%v) mpID(%v) err(%v)", cluster, mpID, err)
				continue
			}
			view := cluster_service.FormatMetaPartitionView(mp, cluster)
			result = append(result, view)
		}
	} else {
		mc := api.GetMasterClient(cluster)
		for _, mpID := range mpIDs {
			mp, err := mc.ClientAPI().GetMetaPartition(mpID, proto.PlaceholderVol)
			if err != nil {
				log.LogWarnf("getMetaPartition: getMpInfo failed, cluster(%v) mpID(%v) err(%v)", cluster, mpID, err)
				continue
			}
			view := cluster_service.FormatMetaPartitionView(mp, cluster)
			result = append(result, view)
		}
	}
	return result
}

func (cs *ClusterService) dataPartitionList(ctx context.Context, args struct {
	Cluster  string
	Addr     *string
	ID       *uint64
	Page     *int32
	PageSize *int32
}) (*cproto.PartitionListResponse, error) {
	cluster := args.Cluster
	result := make([]*cproto.PartitionViewDetail, 0)

	if args.ID != nil {
		result = getDataPartition(cluster, []uint64{*args.ID}, cs.api)
		return cproto.NewPartitionListResponse(len(result), result), nil
	}

	var partitions []uint64
	if args.Addr != nil && *args.Addr != "" {
		if cproto.IsRelease(cluster) {
			rc := cs.api.GetReleaseClient(cluster)
			nodeInfo, err := rc.GetDataNode(*args.Addr)
			if err != nil {
				log.LogErrorf("dataPartitionList: getDataNode failed, cluster(%v) addr(%v) err(%v)", cluster, *args.Addr, err)
				return nil, err
			}
			partitions = nodeInfo.PersistenceDataPartitions
		} else {
			mc := cs.api.GetMasterClient(cluster)
			// todo: datanode中 有别的info
			nodeInfo, err := mc.NodeAPI().GetDataNode(*args.Addr)
			if err != nil {
				log.LogErrorf("dataPartitionList: getDataNode failed, cluster(%v) addr(%v) err(%v)", cluster, *args.Addr, err)
				return nil, err
			}
			partitions = nodeInfo.PersistenceDataPartitions
		}
	}
	sort.SliceStable(partitions, func(i, j int) bool {
		return partitions[i] < partitions[j]
	})
	if args.Page != nil && args.PageSize != nil {
		page := *args.Page
		pageSize := *args.PageSize
		dps := cutil.Paginate(int(page), int(pageSize), partitions)
		result = getDataPartition(cluster, dps, cs.api)
	}
	return cproto.NewPartitionListResponse(len(partitions), result), nil
}

func getDataPartition(cluster string, dpIDs []uint64, api *api.APIManager) []*cproto.PartitionViewDetail {
	sort.SliceStable(dpIDs, func(i, j int) bool {
		return dpIDs[i] < dpIDs[j]
	})
	result := make([]*cproto.PartitionViewDetail, 0, len(dpIDs))
	if cproto.IsRelease(cluster) {
		rc := api.GetReleaseClient(cluster)
		for _, dpID := range dpIDs {
			dp, err := rc.GetDataPartition(dpID)
			if err != nil {
				log.LogWarnf("getDataPartition: getDpInfo failed, cluster(%v) dpID(%v) err(%v)", cluster, dpID, err)
				continue
			}
			view := cluster_service.FormatDataPartitionView(dp, cluster)
			result = append(result, view)
		}
	} else {
		mc := api.GetMasterClient(cluster)
		for _, dpID := range dpIDs {
			dp, err := mc.AdminAPI().GetDataPartition("", dpID)
			if err != nil {
				log.LogWarnf("getDataPartition: getDpInfo failed, cluster(%v) dpID(%v) err(%v)", cluster, dpID, err)
				continue
			}
			view := cluster_service.FormatDataPartitionView(dp, cluster)
			result = append(result, view)
		}
	}
	return result
}

func (cs *ClusterService) addMetaReplica(ctx context.Context, args struct {
	Cluster   *string
	ID        uint64
	Addr      string
	StoreMode int32
}) (resp *cproto.GeneralResp, err error) {
	cluster := cutil.GetClusterParam(args.Cluster)
	defer func() {
		msg := fmt.Sprintf("addMetaReplica:%v-%v", args.ID, args.Addr)
		if err == nil {
			log.LogInfof("%s successfully", msg)
			record := model.NewNodeOperation(
				cluster,
				cproto.ModuleMetaNode,
				args.Addr,
				proto.AdminAddMetaReplica,
				ctx.Value(cutil.PinKey).(string),
				strconv.FormatUint(args.ID, 10),
			)
			record.InsertRecord(record)
		} else {
			log.LogErrorf("%s, err: %v", msg, err)
		}
	}()

	if cproto.IsRelease(cluster) {
		rc := cs.api.GetReleaseClient(cluster)
		err = rc.AddMetaReplica(args.ID, args.Addr)
	} else {
		mc := cs.api.GetMasterClient(cluster)
		// todo: 根据vol类型选择flag
		err = mc.AdminAPI().AddMetaReplica(args.ID, args.Addr, proto.DefaultAddReplicaType, int(args.StoreMode))
	}
	return cproto.BuildResponse(err), err
}

func (cs *ClusterService) deleteMetaReplica(ctx context.Context, args struct {
	Cluster *string
	ID      uint64
	Addr    string
}) (resp *cproto.GeneralResp, err error) {
	cluster := cutil.GetClusterParam(args.Cluster)
	defer func() {
		msg := fmt.Sprintf("deleteMetaReplica:%v-%v", args.ID, args.Addr)
		if err == nil {
			log.LogInfof("%s successfully", msg)
			record := model.NewNodeOperation(
				cluster,
				cproto.ModuleMetaNode,
				args.Addr,
				proto.AdminDeleteMetaReplica,
				ctx.Value(cutil.PinKey).(string),
				strconv.FormatUint(args.ID, 10),
			)
			record.InsertRecord(record)
		} else {
			log.LogErrorf("%s, err: %v", msg, err)
		}
	}()

	if cproto.IsRelease(cluster) {
		rc := cs.api.GetReleaseClient(cluster)
		err = rc.DeleteMetaReplica(args.ID, args.Addr)
	} else {
		mc := cs.api.GetMasterClient(cluster)
		err = mc.AdminAPI().DeleteMetaReplica(args.ID, args.Addr)
	}
	return cproto.BuildResponse(err), err
}

func (cs *ClusterService) addMetaLearner(ctx context.Context, args struct {
	Cluster     *string
	ID          uint64
	Addr        string
	StoreMode   int32
	AutoPromote bool
	Threshold   int32
}) (resp *cproto.GeneralResp, err error) {
	cluster := cutil.GetClusterParam(args.Cluster)
	defer func() {
		msg := fmt.Sprintf("addMetaLearner:%v-%v", args.ID, args.Addr)
		if err == nil {
			log.LogInfof("%s successfully", msg)
			record := model.NewNodeOperation(
				cluster,
				cproto.ModuleMetaNode,
				args.Addr,
				proto.AdminAddMetaReplicaLearner,
				ctx.Value(cutil.PinKey).(string),
				strconv.FormatUint(args.ID, 10),
			)
			record.InsertRecord(record)
		} else {
			log.LogErrorf("%s, err: %v", msg, err)
		}
	}()

	if cproto.IsRelease(cluster) {
		return cproto.BuildResponse(cproto.ErrUnSupportOperation), cproto.ErrUnSupportOperation
	}
	mc := cs.api.GetMasterClient(cluster)
	err = mc.AdminAPI().AddMetaReplicaLearner(args.ID, args.Addr, args.AutoPromote, uint8(args.Threshold), proto.AutoChooseAddrForQuorumVol, int(args.StoreMode))
	return cproto.BuildResponse(err), err
}

func (cs *ClusterService) promoteMetaLearner(ctx context.Context, args struct {
	Cluster *string
	ID      uint64
	Addr    string
}) (resp *cproto.GeneralResp, err error) {
	cluster := cutil.GetClusterParam(args.Cluster)
	defer func() {
		msg := fmt.Sprintf("promote metaLearner:%v-%v", args.ID, args.Addr)
		if err == nil {
			log.LogInfof("%s successfully", msg)
			record := model.NewNodeOperation(
				cluster,
				cproto.ModuleMetaNode,
				args.Addr,
				proto.AdminPromoteMetaReplicaLearner,
				ctx.Value(cutil.PinKey).(string),
				strconv.FormatUint(args.ID, 10),
			)
			record.InsertRecord(record)
		} else {
			log.LogErrorf("%s, err: %v", msg, err)
		}
	}()

	if cproto.IsRelease(cluster) {
		return cproto.BuildResponse(cproto.ErrUnSupportOperation), cproto.ErrUnSupportOperation
	}
	mc := cs.api.GetMasterClient(cluster)
	err = mc.AdminAPI().PromoteMetaReplicaLearner(args.ID, args.Addr)
	return cproto.BuildResponse(err), err
}

func (cs *ClusterService) addDataReplica(ctx context.Context, args struct {
	Cluster     *string
	ID          uint64
	Addr        string
	ReplicaType int32
}) (resp *cproto.GeneralResp, err error) {
	cluster := cutil.GetClusterParam(args.Cluster)
	defer func() {
		msg := fmt.Sprintf("addDataReplica:%v-%v", args.ID, args.Addr)
		if err == nil {
			log.LogInfof("%s successfully", msg)
			record := model.NewNodeOperation(
				cluster,
				cproto.ModuleDataNode,
				args.Addr,
				proto.AdminAddDataReplica,
				ctx.Value(cutil.PinKey).(string),
				strconv.FormatUint(args.ID, 10),
			)
			record.InsertRecord(record)
		} else {
			log.LogErrorf("%s, err: %v", msg, err)
		}
	}()

	if cproto.IsRelease(cluster) {
		return cproto.BuildResponse(cproto.ErrUnSupportOperation), cproto.ErrUnSupportOperation
	}
	mc := cs.api.GetMasterClient(cluster)
	err = mc.AdminAPI().AddDataReplica(args.ID, args.Addr, proto.AddReplicaType(args.ReplicaType))
	return cproto.BuildResponse(err), err
}

func (cs *ClusterService) deleteDataReplica(ctx context.Context, args struct {
	Cluster *string
	ID      uint64 // pid
	Addr    string
}) (resp *cproto.GeneralResp, err error) {
	cluster := cutil.GetClusterParam(args.Cluster)
	defer func() {
		msg := fmt.Sprintf("deleteDataReplica:%v-%v", args.ID, args.Addr)
		if err == nil {
			log.LogInfof("%s successfully", msg)
			record := model.NewNodeOperation(
				cluster,
				cproto.ModuleDataNode,
				args.Addr,
				proto.AdminDeleteDataReplica,
				ctx.Value(cutil.PinKey).(string),
				strconv.FormatUint(args.ID, 10),
			)
			record.InsertRecord(record)
		} else {
			log.LogErrorf("%s, err: %v", msg, err)
		}
	}()

	if cproto.IsRelease(cluster) {
		return cproto.BuildResponse(cproto.ErrUnSupportOperation), cproto.ErrUnSupportOperation
	}
	mc := cs.api.GetMasterClient(cluster)
	err = mc.AdminAPI().DeleteDataReplica(args.ID, args.Addr)
	return cproto.BuildResponse(err), err
}

func (cs *ClusterService) addDataLearner(ctx context.Context, args struct {
	Cluster     *string
	ID          uint64
	Addr        string
	AutoPromote bool
	Threshold   int32
}) (resp *cproto.GeneralResp, err error) {
	cluster := cutil.GetClusterParam(args.Cluster)
	defer func() {
		msg := fmt.Sprintf("addDataLearner:%v-%v", args.ID, args.Addr)
		if err == nil {
			log.LogInfof("%s successfully", msg)
			record := model.NewNodeOperation(
				cluster,
				cproto.ModuleDataNode,
				args.Addr,
				proto.AdminAddDataReplicaLearner,
				ctx.Value(cutil.PinKey).(string),
				strconv.FormatUint(args.ID, 10),
			)
			record.InsertRecord(record)
		} else {
			log.LogErrorf("%s, err: %v", msg, err)
		}
	}()

	if cproto.IsRelease(cluster) {
		return cproto.BuildResponse(cproto.ErrUnSupportOperation), cproto.ErrUnSupportOperation
	}
	mc := cs.api.GetMasterClient(cluster)
	err = mc.AdminAPI().AddDataLearner(args.ID, args.Addr, args.AutoPromote, uint8(args.Threshold))
	return cproto.BuildResponse(err), err
}

func (cs *ClusterService) promoteDataLearner(ctx context.Context, args struct {
	Cluster *string
	ID      uint64
	Addr    string
}) (resp *cproto.GeneralResp, err error) {
	cluster := cutil.GetClusterParam(args.Cluster)
	defer func() {
		msg := fmt.Sprintf("promoteDataLearner:%v-%v", args.ID, args.Addr)
		if err == nil {
			log.LogInfof("%s successfully", msg)
			record := model.NewNodeOperation(
				cluster,
				cproto.ModuleDataNode,
				args.Addr,
				proto.AdminPromoteDataReplicaLearner,
				ctx.Value(cutil.PinKey).(string),
				strconv.FormatUint(args.ID, 10),
			)
			record.InsertRecord(record)
		} else {
			log.LogErrorf("%s, err: %v", msg, err)
		}
	}()

	if cproto.IsRelease(cluster) {
		return cproto.BuildResponse(cproto.ErrUnSupportOperation), cproto.ErrUnSupportOperation
	}
	mc := cs.api.GetMasterClient(cluster)
	err = mc.AdminAPI().PromoteDataLearner(args.ID, args.Addr)
	return cproto.BuildResponse(err), err
}

// 若需要object、LB等信息 依赖此接口
func (cs *ClusterService) GetTopology(cluster string) (*cproto.CFSSRETopology, error) {
	var result *cproto.CFSSRETopology
	var url = fmt.Sprintf("http://%s%s", cs.sreDomainName, cproto.ClusterTopologyPath)
	var req = cutil.NewAPIRequest(http.MethodPost, url)
	r := struct {
		ClusterName string `json:"clusterName"`
	}{
		ClusterName: cluster,
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

func durationFormat(minutes int64) string {
	if minutes >= 0 && minutes < 60 {
		return strconv.FormatInt(minutes, 10) + " min"
	} else if minutes >= 60 && minutes < 1440 {
		return strconv.FormatInt(minutes/60, 10) + " hour " + strconv.FormatInt(minutes%60, 10) + " min"
	} else if minutes >= 1440 {
		return strconv.FormatInt(minutes/1440, 10) + " day " + strconv.FormatInt((minutes%1440)/60, 10) +
			" hour " + strconv.FormatInt((minutes%1440)%60, 10) + " min"
	}
	return ""
}

func checkIsInCluster(addr string, role string, cfsTopo *cproto.CFSSRETopology) (ok bool) {
	if cfsTopo == nil {
		return true
	}

	switch role {
	case cproto.RoleNameMaster:
		for _, dataCenter := range cfsTopo.Master.DataCenterInfo {
			for _, ip := range dataCenter.IpList {
				if strings.Contains(addr, ip) {
					return true
				}
			}
		}

	case cproto.RoleNameDataNode:
		for _, dataNodeDataCenter := range cfsTopo.DataNode.DataCenterInfo {
			for _, zoneInfo := range dataNodeDataCenter.ZoneList {
				for _, node := range zoneInfo.IpList {
					if strings.Contains(addr, node) {
						return true
					}
				}
			}
		}

	case cproto.RoleNameMetaNode:
		for _, metaNodeDataCenter := range cfsTopo.MetaNode.DataCenterInfo {
			for _, zoneInfo := range metaNodeDataCenter.ZoneList {
				for _, node := range zoneInfo.IpList {
					if strings.Contains(addr, node) {
						return true
					}
				}
			}
		}

	case cproto.RoleNameObjectNode:
		for _, dataCenter := range cfsTopo.ObjectNode.DataCenterInfo {
			for _, ip := range dataCenter.IpList {
				if strings.Contains(addr, ip) {
					return true
				}
			}
		}

	case cproto.RoleNameMasterLBNode:
		for _, dataCenter := range cfsTopo.MasterLBNode.DataCenterInfo {
			for _, ip := range dataCenter.IpList {
				if strings.Contains(addr, ip) {
					return true
				}
			}
		}

	case cproto.RoleNameObjectLBNode:
		for _, dataCenter := range cfsTopo.ObjectLBNode.DataCenterInfo {
			for _, ip := range dataCenter.IpList {
				if strings.Contains(addr, ip) {
					return true
				}
			}
		}

	case cproto.RoleNameLBNode:
		for _, dataCenter := range cfsTopo.LBNode.DataCenterInfo {
			for _, ip := range dataCenter.IpList {
				if strings.Contains(addr, ip) {
					return true
				}
			}
		}
	default:
		return false
	}
	return
}

func getZoneCapacityCurve(request *cproto.CFSClusterResourceRequest) (resource []*cproto.ClusterResourceData, err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("getZoneCapacityCurve failed: cluster(%v) zone(%v) err(%v)", request.ClusterName, request.ZoneName, err)
		}
	}()
	var (
		isHour   bool
		pointNum int
	)
	table := model.ChubaofsClusterInfoInMysql{}
	clusterInfo, err := table.GetClusterInfoFromMysql(request.ClusterName)
	if err != nil {
		return nil, nil
	}

	switch request.IntervalType {
	case cproto.ResourceNoType:
		if request.StartTime == 0 || request.EndTime == 0 {
			err = fmt.Errorf("err request info, lack start time or end time")
			return nil, err
		}
		if resource, err = GetCapacityDataWithTime(clusterInfo, request); err != nil {
			return nil, err
		}
		return
	case cproto.ResourceLatestOneDay:
		pointNum = 6 * 24
	case cproto.ResourceLatestOneWeek:
		isHour = true
		pointNum = 7 * 24
	case cproto.ResourceLatestOneMonth:
		isHour = true
		pointNum = 30 * 24
	default:
		err = fmt.Errorf("unknown interval type:%v", request.IntervalType)
		return
	}

	if resource, err = GetCapacityData(clusterInfo, request, pointNum, isHour); err != nil {
		return nil, err
	}
	return
}

func GetCapacityData(clusterInfo *model.ChubaofsClusterInfoInMysql, requestInfo *cproto.CFSClusterResourceRequest, pointNum int, isHour bool) (data []*cproto.ClusterResourceData, err error) {
	zoneNameList := getZoneNameList(clusterInfo, requestInfo)
	condition := map[string]interface{}{
		"cluster_name": requestInfo.ClusterName,
	}
	if isHour {
		condition["is_hour"] = isHour
	}
	data = make([]*cproto.ClusterResourceData, 0)
	// 时间倒数 取点（有的数据不是近7天）
	err = cutil.SRE_DB.Table(model.ChubaofsClusterCapacityInMysql{}.TableName()).Where(condition).
		Where("zone_name in (?)", zoneNameList).Order("date desc").Limit(pointNum * len(zoneNameList)).
		Select("round(unix_timestamp(record_time)+0) as date, sum(total_gb) as total_gb, sum(used_gb) as used_gb, round((sum(used_gb)/sum(total_gb))*100, 2) as used_ratio").
		Group("date").Scan(&data).Error
	if err != nil {
		log.LogWarnf("queryZoneCapacity failed: %v", err)
	}
	// reverse切片
	reverseSlice := func(s []*cproto.ClusterResourceData) {
		for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
			s[i], s[j] = s[j], s[i]
		}
	}
	reverseSlice(data)
	return
}

func GetCapacityDataWithTime(clusterInfo *model.ChubaofsClusterInfoInMysql, requestInfo *cproto.CFSClusterResourceRequest) (data []*cproto.ClusterResourceData, err error) {
	zoneNameList := getZoneNameList(clusterInfo, requestInfo)
	var (
		pointNum  int
		isHour    bool
		startTime time.Time
	)
	getPointNum := func(start, end int64) {
		startTime = time.Unix(start, 0).Local()
		endTime := time.Unix(end, 0).Local()
		timeSpan := endTime.Sub(startTime)
		if timeSpan > time.Hour*24 {
			isHour = true
		}
		if isHour {
			pointNum = int(math.Ceil(endTime.Sub(startTime).Hours()))
		} else {
			pointNum = int(math.Ceil(endTime.Sub(startTime).Minutes() / 10))
		}
	}
	getPointNum(requestInfo.StartTime, requestInfo.EndTime)
	getPointNum(requestInfo.StartTime, requestInfo.EndTime)
	condition := map[string]interface{}{
		"cluster_name": requestInfo.ClusterName,
	}
	if isHour {
		condition["is_hour"] = isHour
	}
	data = make([]*cproto.ClusterResourceData, 0)
	err = cutil.SRE_DB.Table(model.ChubaofsClusterCapacityInMysql{}.TableName()).
		Where(condition).Where("record_time > ?", startTime).
		Where("zone_name in (?)", zoneNameList).Limit(pointNum * len(zoneNameList)).
		Select("round(unix_timestamp(record_time)+0) as date, sum(total_gb) as total_gb, sum(used_gb) as used_gb, round((sum(used_gb)/sum(total_gb))*100, 2) as used_ratio").
		Group("date").Scan(&data).Error
	if err != nil {
		log.LogWarnf("queryZoneCapacity failed: %v", err)
	}
	return
}

func (cs *ClusterService) GetZoneUsageOverview(clusterName string) ([]*cproto.ZoneUsageOverview, error) {
	zoneNameList := cs.api.GetClusterZoneNameList(clusterName)
	if len(zoneNameList) == 0 {
		return nil, fmt.Errorf("get zone list failed")
	}
	data := make([]*cproto.ZoneUsageOverview, 0)
	err := cutil.SRE_DB.Table(model.ChubaofsClusterCapacityInMysql{}.TableName()).
		Where("cluster_name = ?", clusterName).Where("zone_name in (?)", zoneNameList).
		Group("date, zone_name").Order("date DESC").Limit(len(zoneNameList)).
		Select("zone_name, round(unix_timestamp(record_time)+0) as date, sum(total_gb) as total_gb, sum(used_gb) as used_gb, round((sum(used_gb)/sum(total_gb))*100, 2) as used_ratio").
		Scan(&data).Error
	if err != nil {
		log.LogWarnf("GetZoneUsageOverview failed: cluster(%v) err(%v)", clusterName, err)
	}
	return data, err
}

func (cs *ClusterService) getSourceUsageInfo(cluster, source, zone string, interval int32, startTs, endTs int64) ([][]*cproto.SourceUsedInfo, error) {
	start, end, err := cproto.ParseHistoryCurveRequestTime(int(interval), startTs, endTs)
	if err != nil {
		return nil, err
	}
	return cluster_service.GetSourceUsageInfo(cluster, source, zone, start, end)
}

func getZoneNameList(clusterInfo *model.ChubaofsClusterInfoInMysql, requestInfo *cproto.CFSClusterResourceRequest) []string {
	//zone name: all 表示获取整个集群的容量变化曲线（请求中机房名称为空、zone name名称为空代表获取整个集群的）
	var zoneNameList = make([]string, 1)
	zoneNameList[0] = "all"
	if clusterInfo.IsRelease {
		//获取某个机房的容量变化曲线，因为release版本没有zone name的概念，所以获取某个机房的容量等同于获取机房下default zone的容量
		if requestInfo.DataCenterName != "" && strings.Compare(requestInfo.DataCenterName, cproto.All) != 0 {
			zoneNameList[0] = fmt.Sprintf("%s_default", requestInfo.DataCenterName)
		}
	} else {
		if requestInfo.ZoneName != "" {
			// 获取指定zone的容量变化曲线
			zoneNameList[0] = requestInfo.ZoneName
		} else if requestInfo.DataCenterName != "" {
			// 指定机房，未指定zone，获取整个机房的容量变化曲线
			zoneNameList = model.ChubaoFSZoneToRoomMapInMysql{}.GetZoneNameList(clusterInfo.ClusterName, requestInfo.DataCenterName)
		}
		//if requestInfo.DataCenterName != ""{
		//	if requestInfo.ZoneName != "" {
		//		//获取指定机房指定zone name下的容量变化曲线
		//		zoneNameList[0] = requestInfo.ZoneName
		//	} else {
		//		//指定机房名称，未指定zone name,则获取整个机房的容量变化曲线
		//		zoneNameList = model.GetZoneNameList(clusterInfo.ClusterName, requestInfo.DataCenterName)
		//	}
		//}
	}
	return zoneNameList
}

func (cs *ClusterService) resetMetaNode(ctx context.Context, args struct {
	Cluster *string
	Addr    string
}) (err error) {
	cluster := cutil.GetClusterParam(args.Cluster)
	defer func() {
		msg := fmt.Sprintf("resetMetaNode: %v", args.Addr)
		if err == nil {
			log.LogInfof("%s success", msg)
			record := model.NewNodeOperation(
				cluster,
				cproto.ModuleMetaNode,
				args.Addr,
				proto.AdminResetCorruptMetaNode,
				ctx.Value(cutil.PinKey).(string),
			)
			record.InsertRecord(record)
		} else {
			log.LogErrorf("%s, err: %v", msg, err)
		}
	}()

	if cproto.IsRelease(cluster) {
		return cproto.ErrUnSupportOperation
	}
	mc := cs.api.GetMasterClient(cluster)
	err = mc.AdminAPI().ResetCorruptMetaNode(args.Addr)
	return
}

func (cs *ClusterService) resetDataNode(ctx context.Context, args struct {
	Cluster *string
	Addr    string
}) (err error) {
	cluster := cutil.GetClusterParam(args.Cluster)
	defer func() {
		msg := fmt.Sprintf("resetDataNode:%v", args.Addr)
		if err == nil {
			log.LogInfof("%s success", msg)
			record := model.NewNodeOperation(
				cluster,
				cproto.ModuleDataNode,
				args.Addr,
				proto.AdminResetCorruptDataNode,
				ctx.Value(cutil.PinKey).(string),
			)
			record.InsertRecord(record)
		} else {
			log.LogErrorf("%s, err: %v", msg, err)
		}
	}()

	if cproto.IsRelease(cluster) {
		return cproto.ErrUnSupportOperation
	}
	mc := cs.api.GetMasterClient(cluster)
	err = mc.AdminAPI().ResetCorruptDataNode(args.Addr)
	return
}

func (cs *ClusterService) startRiskFixDataNode(ctx context.Context, args struct {
	Cluster string
	Addr    string
}) (err error) {
	defer func() {
		msg := fmt.Sprintf("startRiskFixDataNode: cluster(%s) node(%s)", args.Cluster, args.Addr)
		if err == nil {
			log.LogInfof("%s success", msg)
			record := model.NewNodeOperation(
				args.Cluster,
				cproto.ModuleDataNode,
				args.Addr,
				"/risk/startFix",
				ctx.Value(cutil.PinKey).(string),
			)
			record.InsertRecord(record)
		} else {
			log.LogErrorf("%s, err: %v", msg, err)
		}
	}()
	var dHost string
	if cproto.IsRelease(args.Cluster) {
		return cproto.ErrUnSupportOperation
		//rc := cs.api.GetReleaseClient(args.Cluster)
		//dHost = fmt.Sprintf("%s:%s", strings.Split(args.Addr, ":")[0], rc.DatanodeProf)
	} else {
		mc := cs.api.GetMasterClient(args.Cluster)
		dHost = fmt.Sprintf("%v:%v", strings.Split(args.Addr, ":")[0], mc.DataNodeProfPort)
	}
	dataClient := http_client.NewDataClient(dHost, false)
	return dataClient.StartRiskFix()
}

func (cs *ClusterService) stopRiskFixDataNode(ctx context.Context, args struct {
	Cluster string
	Addr    string
}) (err error) {
	defer func() {
		msg := fmt.Sprintf("stopRiskFixDataNode: cluster(%s) node(%s)", args.Cluster, args.Addr)
		if err == nil {
			log.LogInfof("%s success", msg)
			record := model.NewNodeOperation(
				args.Cluster,
				cproto.ModuleDataNode,
				args.Addr,
				"/risk/stopFix",
				ctx.Value(cutil.PinKey).(string),
			)
			record.InsertRecord(record)
		} else {
			log.LogErrorf("%s, err: %v", msg, err)
		}
	}()
	var dHost string
	if cproto.IsRelease(args.Cluster) {
		return cproto.ErrUnSupportOperation
		//rc := cs.api.GetReleaseClient(args.Cluster)
		//dHost = fmt.Sprintf("%s:%s", strings.Split(args.Addr, ":")[0], rc.DatanodeProf)
	} else {
		mc := cs.api.GetMasterClient(args.Cluster)
		dHost = fmt.Sprintf("%v:%v", strings.Split(args.Addr, ":")[0], mc.DataNodeProfPort)
	}
	dataClient := http_client.NewDataClient(dHost, false)
	return dataClient.StopRiskFix()
}

func (cs *ClusterService) stopDataPartition(ctx context.Context, args struct {
	Cluster string
	ID      uint64
	Addr    string
}) (err error) {
	defer func() {
		msg := fmt.Sprintf("stopDataPartition: cluster(%s) dp(%v) host(%s)", args.Cluster, args.ID, args.Addr)
		if err == nil {
			log.LogInfof("%s success", msg)
			record := model.NewNodeOperation(
				args.Cluster,
				cproto.ModuleDataNode,
				args.Addr,
				"/stopPartition",
				ctx.Value(cutil.PinKey).(string),
				strconv.FormatUint(args.ID, 10),
			)
			record.InsertRecord(record)
		} else {
			log.LogErrorf("%s, err: %v", msg, err)
		}
	}()
	//mc := cs.api.GetMasterClient(args.Cluster)
	//dp, err := mc.AdminAPI().GetDataPartition("", args.ID)
	//if err != nil {
	//	return
	//}
	//var exist bool
	//for _, h := range dp.Hosts {
	//	if h == args.Addr {
	//		exist = true
	//		break
	//	}
	//}
	//if !exist {
	//	err = fmt.Errorf("host[%v] not exist in dp hosts[%v]", args.Addr, dp.Hosts)
	//	return
	//}
	var dHost string
	if cproto.IsRelease(args.Cluster) {
		rc := cs.api.GetReleaseClient(args.Cluster)
		dHost = fmt.Sprintf("%s:%s", strings.Split(args.Addr, ":")[0], rc.DatanodeProf)
	} else {
		mc := cs.api.GetMasterClient(args.Cluster)
		dHost = fmt.Sprintf("%v:%v", strings.Split(args.Addr, ":")[0], mc.DataNodeProfPort)
	}
	dataClient := http_client.NewDataClient(dHost, false)
	return dataClient.StopPartition(args.ID)
}

func (cs *ClusterService) setRecoverDataPartition(ctx context.Context, args struct {
	Cluster string
	ID      uint64
}) (err error) {
	defer func() {
		msg := fmt.Sprintf("setRecoverDataPartition: cluster(%s) dp(%v)", args.Cluster, args.ID)
		if err == nil {
			log.LogInfof("%s success", msg)
			record := model.NewNodeOperation(
				args.Cluster,
				cproto.ModuleDataNode,
				strconv.FormatUint(args.ID, 10),
				proto.AdminDataPartitionSetIsRecover,
				ctx.Value(cutil.PinKey).(string),
				strconv.FormatUint(args.ID, 10),
			)
			record.InsertRecord(record)
		} else {
			log.LogErrorf("%s, err: %v", msg, err)
		}
	}()
	if cproto.IsRelease(args.Cluster) {
		mc := cs.api.GetMasterClient(args.Cluster)
		_, err = mc.AdminAPI().ResetRecoverDataPartition(args.ID)
	} else {
		rc := cs.api.GetReleaseClient(args.Cluster)
		err = rc.SetIsRecoverDataPartition(args.ID)
	}
	return
}

func (cs *ClusterService) decommissionFlashNode(ctx context.Context, args struct {
	Cluster *string
	Addr    string
}) (resp *cproto.GeneralResp, err error) {
	cluster := cutil.GetClusterParam(args.Cluster)
	defer func() {
		msg := fmt.Sprintf("decommissionFlashNode:%v", args.Addr)
		if err == nil {
			log.LogInfof("%s success", msg)
			record := model.NewNodeOperation(
				cluster,
				cproto.ModuleFlashNode,
				args.Addr,
				proto.DecommissionFlashNode,
				ctx.Value(cutil.PinKey).(string),
			)
			record.InsertRecord(record)
		} else {
			log.LogErrorf("%s, err: %v", msg, err)
		}
	}()

	if cproto.IsRelease(cluster) {
		return cproto.BuildResponse(cproto.ErrUnSupportOperation), cproto.ErrUnSupportOperation
	}
	mc := cs.api.GetMasterClient(cluster)
	_, err = mc.NodeAPI().FlashNodeDecommission(args.Addr)
	cs.api.UpdateFlashNodeViewCache(cluster)
	return cproto.BuildResponse(err), err
}

func (cs *ClusterService) decommissionMetaNode(ctx context.Context, args struct {
	Cluster *string
	Addr    string
}) (resp *cproto.GeneralResp, err error) {
	cluster := cutil.GetClusterParam(args.Cluster)
	defer func() {
		msg := fmt.Sprintf("decommissionMetaNode:%v", args.Addr)
		if err == nil {
			log.LogInfof("%s success", msg)
			record := model.NewNodeOperation(
				cluster,
				cproto.ModuleMetaNode,
				args.Addr,
				proto.DecommissionMetaNode,
				ctx.Value(cutil.PinKey).(string),
			)
			record.InsertRecord(record)
		} else {
			log.LogErrorf("%s, err: %v", msg, err)
		}
	}()

	if cproto.IsRelease(cluster) {
		rc := cs.api.GetReleaseClient(cluster)
		err := rc.MetaNodeOffline(args.Addr, "")
		return cproto.BuildResponse(err), err
	}
	mc := cs.api.GetMasterClient(cluster)
	err = mc.NodeAPI().MetaNodeDecommission(args.Addr)
	// 更新cache
	cs.api.UpdateClusterViewCache(cluster)
	return cproto.BuildResponse(err), err
}

func (cs *ClusterService) decommissionDataNode(ctx context.Context, args struct {
	Cluster *string
	Addr    string
}) (resp *cproto.GeneralResp, err error) {
	cluster := cutil.GetClusterParam(args.Cluster)
	defer func() {
		msg := fmt.Sprintf("decommissionDataNode:%v", args.Addr)
		if err == nil {
			log.LogInfof("%s success", msg)
			record := model.NewNodeOperation(
				cluster,
				cproto.ModuleDataNode,
				args.Addr,
				proto.DecommissionDataNode,
				ctx.Value(cutil.PinKey).(string),
			)
			record.InsertRecord(record)
		} else {
			log.LogErrorf("%s, err: %v", msg, err)
		}
	}()

	if cproto.IsRelease(cluster) {
		rc := cs.api.GetReleaseClient(cluster)
		err := rc.DataNodeOffline(args.Addr, "")
		return cproto.BuildResponse(err), err
	}
	mc := cs.api.GetMasterClient(cluster)
	err = mc.NodeAPI().DataNodeDecommission(args.Addr)
	// 更新cache
	cs.api.UpdateClusterViewCache(cluster)
	return cproto.BuildResponse(err), err
}

func (cs *ClusterService) decommissionDisk(ctx context.Context, args struct {
	Cluster  *string
	Addr     string
	DiskPath string
}) (resp *cproto.GeneralResp, err error) {
	cluster := cutil.GetClusterParam(args.Cluster)
	defer func() {
		msg := fmt.Sprintf("decommission Disk:%v-%v", args.Addr, args.DiskPath)
		if err == nil {
			log.LogInfof("%s successfully", msg)
			record := model.NewNodeOperation(
				cluster,
				cproto.ModuleDataNode,
				args.Addr,
				proto.DecommissionDisk,
				ctx.Value(cutil.PinKey).(string),
				args.DiskPath,
			)
			record.InsertRecord(record)
		} else {
			log.LogErrorf("%s, err: %v", msg, err)
		}
	}()
	if cproto.IsRelease(cluster) {
		rc := cs.api.GetReleaseClient(cluster)
		err = rc.DiskOffline(args.Addr, args.DiskPath, "")
		return cproto.BuildResponse(err), err
	}
	mc := cs.api.GetMasterClient(cluster)
	err = mc.NodeAPI().DataNodeDiskDecommission(args.Addr, args.DiskPath, true)
	return cproto.BuildResponse(err), err
}

func (cs *ClusterService) decommissionMetaPartition(ctx context.Context, args struct {
	Cluster   *string
	Addr      string
	ID        uint64
	VolName   string
	DestAddr  *string // 可以不指定
	StoreMode *int32  // [0:默认 1:Mem, 2:Rocks]
}) (resp *cproto.GeneralResp, err error) {
	cluster := cutil.GetClusterParam(args.Cluster)
	defer func() {
		msg := fmt.Sprintf("decommission mpID:%v addr:%v volName:%v ", args.ID, args.DestAddr, args.VolName)
		if err != nil {
			log.LogErrorf("%s, err: %v", msg, err)
		} else {
			record := model.NewNodeOperation(
				cluster,
				cproto.ModuleMetaNode,
				args.Addr,
				proto.AdminDecommissionMetaPartition,
				ctx.Value(cutil.PinKey).(string),
			)
			record.InsertRecord(record)
			log.LogInfof("%s successfully", msg)
		}
	}()
	var (
		destAddr  string
		storeMode int32
	)
	if args.DestAddr == nil {
		destAddr = ""
	} else {
		destAddr = *args.DestAddr
	}
	if args.StoreMode != nil {
		storeMode = *args.StoreMode
	} else {
		storeMode = int32(0)
	}

	if cproto.IsRelease(cluster) {
		rc := cs.api.GetReleaseClient(cluster)
		err = rc.MetaPartitionOffline(args.Addr, args.VolName, args.ID, destAddr)
		return cproto.BuildResponse(err), err
	}
	mc := cs.api.GetMasterClient(cluster)
	err = mc.AdminAPI().DecommissionMetaPartition(args.ID, args.Addr, destAddr, int(storeMode))
	return cproto.BuildResponse(err), err
}

func (cs *ClusterService) decommissionDataPartition(ctx context.Context, args struct {
	Cluster  *string
	Addr     string
	ID       uint64
	VolName  string
	DestAddr *string
}) (resp *cproto.GeneralResp, err error) {
	cluster := cutil.GetClusterParam(args.Cluster)
	defer func() {
		msg := fmt.Sprintf("decommission DP:%v", args.ID)
		if err != nil {
			log.LogErrorf("%s, err: %v", msg, err)
		} else {
			record := model.NewNodeOperation(
				cluster,
				cproto.ModuleDataNode,
				args.Addr,
				proto.AdminDecommissionDataPartition,
				ctx.Value(cutil.PinKey).(string),
				strconv.FormatUint(args.ID, 10),
			)
			record.InsertRecord(record)
			log.LogInfof("%s success", msg)
		}
	}()
	var destAddr string
	if args.DestAddr == nil {
		destAddr = ""
	} else {
		destAddr = *args.DestAddr
	}
	if cproto.IsRelease(cluster) {
		rc := cs.api.GetReleaseClient(cluster)
		err = rc.DataPartitionOffline(args.Addr, args.VolName, args.ID)
		return cproto.BuildResponse(err), err
	}
	mc := cs.api.GetMasterClient(cluster)
	err = mc.AdminAPI().DecommissionDataPartition(args.ID, args.Addr, destAddr)
	return cproto.BuildResponse(err), err
}

// 放在副本(replicas)列表 host可以下拉
func (cs *ClusterService) reloadDataPartition(ctx context.Context, args struct {
	Cluster *string
	Addr    string
	ID      uint64
	VolName string
}) (resp *cproto.GeneralResp, err error) {
	cluster := cutil.GetClusterParam(args.Cluster)
	defer func() {
		msg := fmt.Sprintf("load dp[%v] vol[%v]", args.ID, args.VolName)
		if err != nil {
			log.LogErrorf("%s, err: %s", msg, err)
		} else {
			record := model.NewNodeOperation(
				cluster,
				cproto.ModuleDataNode,
				args.Addr,
				"/reloadPartition",
				ctx.Value(cutil.PinKey).(string),
				strconv.FormatUint(args.ID, 10),
			)
			record.InsertRecord(record)
			log.LogInfof("%s successfully", msg)
		}
	}()

	if cproto.IsRelease(cluster) {
		rc := cs.api.GetReleaseClient(cluster)
		err = rc.LoadDataPartition(args.ID, args.VolName)
		return
	}
	mc := cs.api.GetMasterClient(cluster)
	dp, err := mc.AdminAPI().GetDataPartition("", args.ID)
	if err != nil {
		return
	}
	var diskPath string
	var exist bool
	for _, r := range dp.Replicas {
		if r.Addr == args.Addr {
			exist = true
			diskPath = r.DiskPath
			break
		}
	}
	if !exist {
		err = fmt.Errorf("host[%v] not exist in hosts[%v]", args.Addr, dp.Hosts)
		return cproto.BuildResponse(err), err
	}
	partitionPath := fmt.Sprintf("datapartition_%v_%v", args.ID, dp.Replicas[0].Total)
	dHost := fmt.Sprintf("%v:%v", strings.Split(args.Addr, ":")[0], mc.DataNodeProfPort)
	dataClient := http_client.NewDataClient(dHost, false)
	err = dataClient.ReLoadPartition(partitionPath, diskPath)
	return cproto.BuildResponse(err), err
}

func (cs *ClusterService) reloadMetaPartition(ctx context.Context, args struct {
	Cluster *string
	Addr    string
	ID      uint64
	VolName string
}) (resp *cproto.GeneralResp, err error) {
	cluster := cutil.GetClusterParam(args.Cluster)
	defer func() {
		msg := fmt.Sprintf("reload mp[%v %v]", args.ID, args.Addr)
		if err != nil {
			log.LogErrorf("%s, err: %v", msg, err)
		} else {
			record := model.NewNodeOperation(
				cluster,
				cproto.ModuleMetaNode,
				args.Addr,
				"/reloadPartition",
				ctx.Value(cutil.PinKey).(string),
			)
			record.InsertRecord(record)
			log.LogInfof("%s", msg)
		}
	}()

	if cproto.IsRelease(cluster) {
		rc := cs.api.GetReleaseClient(cluster)
		err = rc.LoadMetaPartition(args.ID, args.VolName)
		return cproto.BuildResponse(err), err
	}
	mc := cs.api.GetMasterClient(cluster)
	host := fmt.Sprintf("%v:%v", strings.Split(args.Addr, ":")[0], mc.MetaNodeProfPort)
	_, err = cs.api.ReloadMetaPartition(host, args.ID)
	return cproto.BuildResponse(err), err
}

func (cs *ClusterService) resetDataPartition(ctx context.Context, args struct {
	Cluster *string
	ID      uint64
	Addrs   *string // split by ","
}) (resp *cproto.GeneralResp, err error) {
	cluster := cutil.GetClusterParam(args.Cluster)
	defer func() {
		msg := fmt.Sprintf("reset dp[%v]", args.ID)
		if err != nil {
			log.LogErrorf("%s, err: %v", msg, err)
		} else {
			record := model.NewNodeOperation(
				cluster,
				cproto.ModuleDataNode,
				*args.Addrs,
				proto.AdminResetDataPartition,
				ctx.Value(cutil.PinKey).(string),
				strconv.FormatUint(args.ID, 10),
			)
			record.InsertRecord(record)
			log.LogInfof("%s", msg)
		}
	}()
	client := cs.api.GetMasterClient(cluster)
	if args.Addrs != nil {
		err = client.AdminAPI().ManualResetDataPartition(args.ID, *args.Addrs)
	} else {
		err = client.AdminAPI().ResetDataPartition(args.ID)
	}
	return cproto.BuildResponse(err), err
}

func (cs *ClusterService) resetMetaPartition(ctx context.Context, args struct {
	Cluster *string
	ID      uint64
	Addrs   *string
}) (resp *cproto.GeneralResp, err error) {
	cluster := cutil.GetClusterParam(args.Cluster)
	defer func() {
		msg := fmt.Sprintf("reset mp[%v]", args.ID)
		if err != nil {
			log.LogErrorf("%s, err: %v", msg, err)
		} else {
			record := model.NewNodeOperation(
				cluster,
				cproto.ModuleMetaNode,
				*args.Addrs,
				proto.AdminResetMetaPartition,
				ctx.Value(cutil.PinKey).(string),
				strconv.FormatUint(args.ID, 10),
			)
			record.InsertRecord(record)
			log.LogInfof("%s success", msg)
		}
	}()
	client := cs.api.GetMasterClient(cluster)
	if args.Addrs != nil {
		err = client.AdminAPI().ManualResetMetaPartition(args.ID, *args.Addrs)
	} else {
		err = client.AdminAPI().ResetMetaPartition(args.ID)
	}
	return cproto.BuildResponse(err), err
}

func (cs *ClusterService) resetCursor(ctx context.Context, args struct {
	Cluster   *string
	ID        uint64
	Type      *string // add/sub
	NewCursor *uint64 // for sub
	Force     *bool   // for sub
}) (*proto.CursorResetResponse, error) {
	var (
		cluster          = cutil.GetClusterParam(args.Cluster)
		resetType        = ""
		cursor    uint64 = 0
		force            = false
		ip               = ""
	)
	if args.Type != nil {
		resetType = *args.Type
	}
	if args.NewCursor != nil {
		cursor = *args.NewCursor
	}
	if args.Force != nil {
		force = *args.Force
	}
	client := cs.api.GetMasterClient(cluster)
	mp, err := client.ClientAPI().GetMetaPartition(args.ID, proto.PlaceholderVol)
	if err != nil {
		return nil, err
	}
	for _, replica := range mp.Replicas {
		if replica.IsLeader {
			ip = strings.Split(replica.Addr, ":")[0]
		}
	}
	ip += ":" + strconv.Itoa(int(client.MetaNodeProfPort))
	mnClient := meta.NewMetaHttpClient(ip, false)
	resp, err := mnClient.ResetCursor(args.ID, resetType, cursor, force)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (cs *ClusterService) rdmaClusterConf(ctx context.Context, args struct {
	Cluster string
}) (*cproto.ClusterRDMAConfView, error) {
	return cs.rdma.GetClusterRDMAConf(args.Cluster)
}

func (cs *ClusterService) rdmaDataNodeList(ctx context.Context, args struct {
	Cluster string
}) (*cproto.RDMANodeViewResponse, error) {
	nodeList, err := cs.rdma.GetRDMANodeList(args.Cluster, "dataNode")
	if err != nil {
		return nil, err
	}
	return &cproto.RDMANodeViewResponse{
		Total: len(nodeList),
		Data:  nodeList,
	}, nil
}

func (cs *ClusterService) rdmaMetaNodeList(ctx context.Context, args struct {
	Cluster string
}) (*cproto.RDMANodeViewResponse, error) {
	nodeList, err := cs.rdma.GetRDMANodeList(args.Cluster, "metaNode")
	if err != nil {
		return nil, err
	}
	return &cproto.RDMANodeViewResponse{
		Total: len(nodeList),
		Data:  nodeList,
	}, nil
}

func (cs *ClusterService) rdmaVolumeList(ctx context.Context, args struct {
	Cluster string
}) (*cproto.RDMAVolumeListResponse, error) {
	return cs.rdma.GetRDMAVolumeList(args.Cluster)
}

func (cs *ClusterService) rdmaNodeView(ctx context.Context, args struct {
	Cluster string
	Addr    string
}) (*cproto.RDMANodeStatusView, error) {
	return cs.rdma.GetRDMANodeView(args.Cluster, args.Addr)
}

func (cs *ClusterService) setClusterRdmaConf(ctx context.Context, args struct {
	Cluster           string
	ClusterRDMAEnable bool
	ClusterRDMASend   bool
	ReConnDelayTime   int64
}) error {
	// for test: use fake return
	return nil
	return cs.rdma.SetClusterRDMAConf(args.Cluster, args.ClusterRDMAEnable, args.ClusterRDMASend, args.ReConnDelayTime)
}

func (cs *ClusterService) setNodeRdmaConf(ctx context.Context, args struct {
	Cluster         string
	Addr            string
	Pod             string
	NodeRDMAService bool
	NodeRDMASend    bool
	NodeRDMARecv    bool
	NodeType        string // data或meta
}) error {
	// for test: use fake return
	return nil
	return cs.rdma.SetNodeRDMAConf(args.Cluster, args.Addr, args.Pod, args.NodeRDMAService, args.NodeRDMASend, args.NodeRDMARecv, args.NodeType)
}

func (cs *ClusterService) setVolumeRdmaConf(ctx context.Context, args struct {
	Cluster    string
	VolList    []string
	EnableRDMA bool
}) error {
	// for test: use fake return
	return nil
	return cs.rdma.BatchSetVolumeRDMAConf(args.Cluster, args.VolList, args.EnableRDMA)
}

func (cs *ClusterService) setNodeSendEnable(ctx context.Context, args struct {
	Cluster    string
	Addr       string
	EnableSend bool
	Reason     string
}) error {
	// for test: use fake return
	return nil
	return cs.rdma.SetNodeSendEnable(args.Cluster, args.Addr, args.EnableSend, args.Reason)
}

func (cs *ClusterService) setNodeConnEnable(ctx context.Context, args struct {
	Cluster    string
	Addr       string
	AddrList   []string
	EnableConn bool
	Reason     string
}) error {
	// for test: use fake return
	return nil
	return cs.rdma.BatchSetNodeConnEnable(args.Cluster, args.Addr, args.AddrList, args.EnableConn, args.Reason)
}
