package scheduleTask

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/console/cutil"
	"github.com/cubefs/cubefs/console/model"
	cproto "github.com/cubefs/cubefs/console/proto"
	"github.com/cubefs/cubefs/util/log"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	listTask  = "/rebalance/list"
	getInfo   = "/rebalance/info"
	getStatus = "/rebalance/status"

	startTask           = "/rebalance/start"
	stopTask            = "/rebalance/stop"
	resetTask           = "/rebalance/reset"
	resetControl        = "/rebalance/resetControl"
	queryRecords        = "/rebalance/queryRecords"
	queryZoneUsageRatio = "/zone/UsageRatio"
)

const (
	clusterParam      = "cluster"
	zoneParam         = "zoneName"
	statusParam       = "status"
	taskTypeParam     = "taskType"
	pageParam         = "page"
	pageSizeParam     = "pageSize"
	taskIdParam       = "taskId"
	highRatioParam    = "highRatio"
	lowRatioParam     = "lowRatio"
	goalRatioParam    = "goalRatio"
	diskLimitParam    = "migrateLimit"
	dstMpLimitParam   = "dstMetaNodeMaxPartitionCount"
	concurrencyParam  = "maxBatchCount"
	srcNodesListParam = "srcNodes"
	dstNodesListParam = "dstNodes"
	moduleTypeParam   = "type"

	volNameParam   = "volume"
	pidParam       = "pid"
	srcHostParam   = "src"
	dstHostParam   = "dst"
	queryDateParam = "date"
)

const (
	_ = iota
	ZoneAutoReBalance
	NodesMigrate
	VolumeMigrate
)

type RebalanceWorker struct {
	serverMap map[string]string // 集群-server地址
}

func NewRebalanceWorker(clusters []*model.ConsoleCluster) *RebalanceWorker {
	serverMap := make(map[string]string)
	for _, cluster := range clusters {
		serverMap[cluster.ClusterName] = cluster.RebalanceHost
	}
	return &RebalanceWorker{
		serverMap: serverMap,
	}
}

func (r *RebalanceWorker) getServerAddr(cluster string) string {
	return r.serverMap[cluster]
}

func (r *RebalanceWorker) GetTaskList(cluster, module, zone string, status, taskType int, page, pageSize int) (total int, list []*cproto.RebalanceInfoView, err error) {
	req := cutil.NewAPIRequest(http.MethodGet, fmt.Sprintf("http://%s%s", r.getServerAddr(cluster), listTask))
	req.AddParam(clusterParam, cluster)
	req.AddParam(moduleTypeParam, module)
	req.AddParam(zoneParam, zone)
	req.AddParam(statusParam, strconv.Itoa(status))
	req.AddParam(taskTypeParam, strconv.Itoa(taskType))
	req.AddParam(pageParam, strconv.Itoa(page))
	req.AddParam(pageSizeParam, strconv.Itoa(pageSize))

	data, err := cutil.SendSimpleRequest(req, true)
	if err != nil {
		log.LogErrorf("GetTaskList failed: cluster(%v) zone(%v) err(%v)", cluster, zone, err)
		return 0, nil, err
	}
	resp := new(cproto.PageResponse)
	if err = json.Unmarshal(data, &resp); err != nil {
		log.LogErrorf("json unmarshal failed, err(%v)", err)
		return 0, nil, err
	}
	if resp.Code != http.StatusOK {
		log.LogWarnf("GetTaskList: resp failed, msg(%v)", resp.Msg)
		return 0, nil, fmt.Errorf("%s", resp.Msg)
	}
	taskList := make([]*cproto.ReBalanceInfoTable, 0)
	if err = json.Unmarshal(resp.Data, &taskList); err != nil {
		log.LogErrorf("unmarshal resp.data failed: err(%v)", err)
		return 0, nil, err
	}
	var formatList = func(infos []*cproto.ReBalanceInfoTable) []*cproto.RebalanceInfoView {
		result := make([]*cproto.RebalanceInfoView, 0, len(infos))
		for _, info := range infos {
			entry := formatRebalanceView(info)
			result = append(result, entry)
		}
		return result
	}
	return resp.TotalCount, formatList(taskList), nil
}

func (r *RebalanceWorker) GetTaskInfo(id uint64) (*cproto.RebalanceInfoView, error) {
	req := cutil.NewAPIRequest(http.MethodGet, fmt.Sprintf("http://%s%s", r.getServerAddr(cutil.GlobalCluster), getInfo))
	req.AddParam(taskIdParam, strconv.FormatUint(id, 10))

	data, err := cutil.SendSimpleRequest(req, true)
	if err != nil {
		return nil, err
	}
	resp := new(cproto.TaskResponse)
	if err = json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	if resp.Code != http.StatusOK {
		return nil, fmt.Errorf("response err: %s", resp.Msg)
	}

	var info *cproto.ReBalanceInfo
	if err = json.Unmarshal(resp.Data, &info); err != nil {
		return nil, err
	}
	if info == nil {
		// 200 但data为null
		return nil, nil
	}

	result := &cproto.RebalanceInfoView{
		TaskID:                   info.ID,
		Cluster:                  info.Cluster,
		Zone:                     info.ZoneName,
		VolName:                  info.VolName,
		Module:                   formatRType(info.RType),
		TaskType:                 info.TaskType,
		Status:                   info.Status,
		Concurrency:              info.MaxBatchCount,
		HighRatio:                info.HighRatio,
		LowRatio:                 info.LowRatio,
		GoalRatio:                info.GoalRatio,
		MigrateLimitPerDisk:      info.MigrateLimitPerDisk,
		DstMetaPartitionMaxCount: info.DstMetaNodePartitionMaxCount,
		CreatedAt:                info.CreatedAt.Format(time.DateTime),
		UpdatedAt:                info.UpdatedAt.Format(time.DateTime),
		SrcNodesUsageRatio:       info.SrcNodesUsageRatio,
		DstNodesUsageRatio:       info.DstNodesUsageRatio,
	}
	return result, nil
}

func (r *RebalanceWorker) GetTaskStatus(id uint64) (*cproto.RebalanceStatusInfo, error) {
	req := cutil.NewAPIRequest(http.MethodGet, fmt.Sprintf("http://%s%s", r.getServerAddr(cutil.GlobalCluster), getStatus))
	req.AddParam(taskIdParam, strconv.FormatUint(id, 10))

	data, err := cutil.SendSimpleRequest(req, true)
	if err != nil {
		return nil, err
	}
	resp := new(cproto.TaskResponse)
	if err = json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	if resp.Code != http.StatusOK {
		return nil, fmt.Errorf("response err: %s", resp.Msg)
	}
	var info *cproto.RebalanceStatusInfo
	if err = json.Unmarshal(resp.Data, &info); err != nil {
		return nil, err
	}
	if info == nil {
		return nil, nil
	}
	return info, nil
}

func (r *RebalanceWorker) CreateZoneAutoRebalanceTask(cluster, module, zone string, high, low, goal float64, concurrency int, limitDPonDisk, limitMPonDst *int32) error {
	req := cutil.NewAPIRequest(http.MethodPost, fmt.Sprintf("http://%s%s", r.getServerAddr(cluster), startTask))
	req.AddParam(taskTypeParam, strconv.Itoa(ZoneAutoReBalance))
	req.AddParam(clusterParam, cluster)
	req.AddParam(moduleTypeParam, module)
	req.AddParam(zoneParam, zone)
	req.AddParam(highRatioParam, strconv.FormatFloat(high, 'f', 5, 64))
	req.AddParam(lowRatioParam, strconv.FormatFloat(low, 'f', 5, 64))
	req.AddParam(goalRatioParam, strconv.FormatFloat(goal, 'f', 5, 64))
	req.AddParam(concurrencyParam, strconv.Itoa(concurrency))
	if limitDPonDisk != nil {
		req.AddParam(diskLimitParam, strconv.Itoa(int(*limitDPonDisk)))
	}
	if limitMPonDst != nil {
		req.AddParam(dstMpLimitParam, strconv.Itoa(int(*limitMPonDst)))
	}

	data, err := cutil.SendSimpleRequest(req, true)
	if err != nil {
		return err
	}
	resp := new(cproto.TaskResponse)
	if err = json.Unmarshal(data, &resp); err != nil {
		return err
	}
	if resp.Code != http.StatusOK {
		return fmt.Errorf("response err: %s", resp.Msg)
	}
	return nil
}

func (r *RebalanceWorker) CreateNodesMigrateTask(cluster, module string, srcNodeList []string, dstNodesList []string, concurrency int, maxDpCountPerDisk, maxMpCountOnDst *int32) error {
	req := cutil.NewAPIRequest(http.MethodGet, fmt.Sprintf("http://%s%s", r.getServerAddr(cluster), startTask))
	req.AddParam(taskTypeParam, strconv.Itoa(NodesMigrate))
	req.AddParam(clusterParam, cluster)
	req.AddParam(moduleTypeParam, module)
	req.AddParam(srcNodesListParam, strings.Join(srcNodeList, ","))
	req.AddParam(dstNodesListParam, strings.Join(dstNodesList, ","))
	req.AddParam(concurrencyParam, strconv.Itoa(concurrency))
	if maxDpCountPerDisk != nil {
		req.AddParam(diskLimitParam, strconv.Itoa(int(*maxDpCountPerDisk)))
	}
	if maxMpCountOnDst != nil {
		req.AddParam(dstMpLimitParam, strconv.Itoa(int(*maxMpCountOnDst)))
	}
	data, err := cutil.SendSimpleRequest(req, true)
	if err != nil {
		return err
	}
	resp := new(cproto.TaskResponse)
	if err = json.Unmarshal(data, &resp); err != nil {
		return err
	}
	if resp.Code != http.StatusOK {
		return fmt.Errorf("response err: %s", resp.Msg)
	}
	return nil
}

func (r *RebalanceWorker) StopTask(cluster string, taskID uint64) error {
	req := cutil.NewAPIRequest(http.MethodPost, fmt.Sprintf("http://%s%s", r.getServerAddr(cluster), stopTask))
	req.AddParam(clusterParam, cluster)
	req.AddParam(taskIdParam, strconv.FormatUint(taskID, 10))

	data, err := cutil.SendSimpleRequest(req, true)
	if err != nil {
		return err
	}
	resp := new(cproto.TaskResponse)
	if err = json.Unmarshal(data, &resp); err != nil {
		return err
	}
	if resp.Code != http.StatusOK {
		return fmt.Errorf("response err: %s", resp.Msg)
	}
	return nil
}

func (r *RebalanceWorker) ResetTask(cluster string) error {
	req := cutil.NewAPIRequest(http.MethodPost, fmt.Sprintf("http://%s%s", r.getServerAddr(cluster), resetTask))

	data, err := cutil.SendSimpleRequest(req, true)
	if err != nil {
		return err
	}
	resp := new(cproto.TaskResponse)
	if err = json.Unmarshal(data, &resp); err != nil {
		return err
	}
	if resp.Code != http.StatusOK {
		return fmt.Errorf("response err: %s", resp.Msg)
	}
	return nil
}

func (r *RebalanceWorker) ResetControl(cluster, module string, taskId uint64, taskType int, zone *string, goal *float64, concurrency, limitDPonDisk, limitMPonDst *int32) error {
	req := cutil.NewAPIRequest(http.MethodPost, fmt.Sprintf("http://%s%s", r.getServerAddr(cluster), resetControl))
	req.AddParam(clusterParam, cluster)
	req.AddParam(moduleTypeParam, module)
	req.AddParam(taskTypeParam, strconv.Itoa(taskType))
	req.AddParam(taskIdParam, strconv.FormatUint(taskId, 10))
	if zone != nil {
		req.AddParam(zoneParam, *zone)
	}
	if goal != nil {
		req.AddParam(goalRatioParam, strconv.FormatFloat(*goal, 'f', 5, 64))
	}
	if concurrency != nil {
		req.AddParam(concurrencyParam, strconv.Itoa(int(*concurrency)))
	}
	if limitDPonDisk != nil {
		req.AddParam(diskLimitParam, strconv.Itoa(int(*limitDPonDisk)))
	}
	if limitMPonDst != nil {
		req.AddParam(dstMpLimitParam, strconv.Itoa(int(*limitMPonDst)))
	}

	data, err := cutil.SendSimpleRequest(req, true)
	if err != nil {
		return err
	}
	resp := new(cproto.TaskResponse)
	if err = json.Unmarshal(data, &resp); err != nil {
		return err
	}
	if resp.Code != http.StatusOK {
		return fmt.Errorf("response err: %s", resp.Msg)
	}
	return nil
}

func (r *RebalanceWorker) GetMigrateRecords(cluster, module string, zone, volume, srcHost, dstHost *string, dateStr string, pid *uint64, page, pageSize int) (int, []*cproto.MigrateRecord, error) {
	req := cutil.NewAPIRequest(http.MethodPost, fmt.Sprintf("http://%s%s", r.getServerAddr(cluster), queryRecords))
	req.AddParam(clusterParam, cluster)
	req.AddParam(moduleTypeParam, module)
	if zone != nil {
		req.AddParam(zoneParam, *zone)
	}
	if volume != nil {
		req.AddParam(volNameParam, *volume)
	}
	if srcHost != nil {
		req.AddParam(srcHostParam, *srcHost)
	}
	if dstHost != nil {
		req.AddParam(dstHostParam, *dstHost)
	}
	req.AddParam(queryDateParam, dateStr)
	if pid != nil {
		req.AddParam(pidParam, strconv.FormatUint(*pid, 10))
	}
	req.AddParam(pageParam, strconv.Itoa(page))
	req.AddParam(pageSizeParam, strconv.Itoa(pageSize))

	data, err := cutil.SendSimpleRequest(req, true)
	if err != nil {
		return 0, nil, err
	}
	resp := new(cproto.PageResponse)
	if err = json.Unmarshal(data, &resp); err != nil {
		return 0, nil, err
	}
	if resp.Code != http.StatusOK {
		return 0, nil, fmt.Errorf("response err: %s", resp.Msg)
	}
	records := make([]*cproto.MigrateRecord, 0)
	if err = json.Unmarshal(resp.Data, &records); err != nil {
		return 0, nil, err
	}
	return resp.TotalCount, records, nil
}

func (r *RebalanceWorker) GetZoneNodeUsageRatio(cluster, zone, module string) ([]*cproto.NodeUsage, error) {
	req := cutil.NewAPIRequest(http.MethodGet, fmt.Sprintf("http://%s%s", r.getServerAddr(cluster), queryZoneUsageRatio))
	req.AddParam(clusterParam, cluster)
	req.AddParam(zoneParam, zone)
	req.AddParam(moduleTypeParam, module)
	data, err := cutil.SendSimpleRequest(req, false)
	if err != nil {
		return nil, err
	}
	resp := make([]*cproto.NodeUsage, 0)
	if err = json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func formatRebalanceView(info *cproto.ReBalanceInfoTable) *cproto.RebalanceInfoView {
	view := &cproto.RebalanceInfoView{
		TaskID:                   info.ID,
		Cluster:                  info.Cluster,
		Zone:                     info.ZoneName,
		VolName:                  info.VolName,
		Module:                   formatRType(info.RType),
		TaskType:                 info.TaskType,
		Status:                   info.Status,
		Concurrency:              info.MaxBatchCount,
		HighRatio:                info.HighRatio,
		LowRatio:                 info.LowRatio,
		GoalRatio:                info.GoalRatio,
		MigrateLimitPerDisk:      info.MigrateLimitPerDisk,
		DstMetaPartitionMaxCount: info.DstMetaNodePartitionMaxCount,
		CreatedAt:                info.CreatedAt.Format(time.DateTime),
		UpdatedAt:                info.UpdatedAt.Format(time.DateTime),
	}
	return view
}

//func formatSrcNode(nodes []*cproto.SrcDatanode) []*cproto.SrcNodeWithDisk {
//	result := make([]*cproto.SrcNodeWithDisk, 0, len(nodes))
//	for _, node := range nodes {
//		view := &cproto.SrcNodeWithDisk{
//			Addr:       node.Addr,
//			UsageRatio: node.UsageRatio,
//			Disk:       formatDiskView(node.Disk),
//		}
//		result = append(result, view)
//	}
//	return result
//}
//
//func formatDiskView(disks []*cproto.Disk) []*cproto.DiskView {
//	views := make([]*cproto.DiskView, 0, len(disks))
//	for _, disk := range disks {
//		view := &cproto.DiskView{
//			Path:          disk.Path,
//			Total:         cutil.FormatSize(disk.Total),
//			Used:          cutil.FormatSize(disk.Used),
//			MigratedSize:  cutil.FormatSize(disk.MigratedSize),
//			MigratedCount: disk.MigratedCount,
//			MigrateLimit:  disk.MigrateLimit,
//		}
//		views = append(views, view)
//	}
//	return views
//}

func formatTaskStatus(status int) string {
	switch status {
	case 0:
		return "全部"
	case 1:
		return "已完成"
	case 2:
		return "迁移中"
	case 3:
		return "停止"
	}
	return ""
}

func formatRType(rType int) string {
	switch rType {
	case 0:
		return "data"
	case 1:
		return "meta"
	}
	return ""
}

func formatTaskType(taskType int) string {
	switch taskType {
	case 1:
		return "zone自动均衡"
	case 2:
		return "节点迁移"
	case 3:
		return "vol迁移"
	}
	return ""
}
