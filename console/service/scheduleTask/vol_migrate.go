package scheduleTask

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/console/cutil"
	cproto "github.com/cubefs/cubefs/console/proto"
	"github.com/cubefs/cubefs/util/log"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	VolMigrateCreate       = "/volMig/create"
	VolMigrateResetControl = "/volMig/resetCtrl"
	VolMigrateStop         = "/volMig/stop"
	VolMigrateStatus       = "/volMig/status"

	ParamSrcZone              = "srcZone"
	ParamDstZone              = "dstZone"
	ParamClusterConcurrency   = "clusterConcurrency"
	ParamVolConcurrency       = "volConcurrency"
	ParamPartitionConcurrency = "partitionConcurrency"
	ParamWaitSecond           = "waitSecond"
)

func (r *RebalanceWorker) GetVolMigrateList(cluster, module, zone, volume string, status, page, pageSize int) (total int, list []*cproto.VolMigrateView, err error) {
	req := cutil.NewAPIRequest(http.MethodGet, fmt.Sprintf("http://%s%s", r.getServerAddr(cluster), listTask))
	req.AddParam(clusterParam, cluster)
	req.AddParam(moduleTypeParam, module)
	req.AddParam(zoneParam, zone)
	req.AddParam(statusParam, strconv.Itoa(status))
	req.AddParam(taskTypeParam, strconv.Itoa(VolumeMigrate))
	req.AddParam(volNameParam, volume)
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
	var formatList = func(infos []*cproto.ReBalanceInfoTable) []*cproto.VolMigrateView {
		result := make([]*cproto.VolMigrateView, 0, len(infos))
		for _, info := range infos {
			entry := formatVolMigrateView(info)
			result = append(result, entry)
		}
		return result
	}
	return resp.TotalCount, formatList(taskList), nil
}

func (r *RebalanceWorker) GetVolMigrateStatus(id uint64) (*cproto.VolMigrateTaskStatus, error) {
	req := cutil.NewAPIRequest(http.MethodGet, fmt.Sprintf("http://%s%s", r.getServerAddr(cutil.GlobalCluster), VolMigrateStatus))
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
	var statusInfo *cproto.VolMigrateTaskStatus
	if err = json.Unmarshal(resp.Data, &statusInfo); err != nil {
		return nil, err
	}
	if statusInfo == nil {
		return nil, nil
	}
	return statusInfo, nil
}

func (r *RebalanceWorker) CreateVolMigrateTask(cluster, module string, volList []string, srcZone, dstZone string,
	clusterConcurrency, volConcurrency, partitionConcurrency, waitSeconds int, dstDataNodeUsage float64, dstMetaPartitionLimit int) error {
	req := cutil.NewAPIRequest(http.MethodGet, fmt.Sprintf("http://%s%s", r.getServerAddr(cutil.GlobalCluster), VolMigrateCreate))
	req.AddParam(clusterParam, cluster)
	req.AddParam(moduleTypeParam, module)
	req.AddParam(ParamSrcZone, srcZone)
	req.AddParam(ParamDstZone, dstZone)
	req.AddParam(ParamClusterConcurrency, strconv.Itoa(clusterConcurrency))
	req.AddParam(ParamVolConcurrency, strconv.Itoa(volConcurrency))
	req.AddParam(ParamPartitionConcurrency, strconv.Itoa(partitionConcurrency))
	req.AddParam(ParamWaitSecond, strconv.Itoa(waitSeconds))
	req.AddParam(highRatioParam, strconv.FormatFloat(dstDataNodeUsage, 'f', 5, 64))
	req.AddParam(dstMpLimitParam, strconv.Itoa(dstMetaPartitionLimit))
	volListData, err := json.Marshal(volList)
	if err != nil {
		return err
	}
	req.AddBody(volListData)
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

func (r *RebalanceWorker) StopVolMigrateTask(cluster string, id uint64) error {
	req := cutil.NewAPIRequest(http.MethodGet, fmt.Sprintf("http://%s%s", r.getServerAddr(cutil.GlobalCluster), VolMigrateStop))
	req.AddParam(clusterParam, cluster)
	req.AddParam(taskIdParam, strconv.FormatUint(id, 10))

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

func (r *RebalanceWorker) ResetVolMigrateTask(cluster string, id uint64, clusterConcurrency, volConcurrency, partitionConcurrency, waitSeconds int) error {
	req := cutil.NewAPIRequest(http.MethodGet, fmt.Sprintf("http://%s%s", r.getServerAddr(cutil.GlobalCluster), VolMigrateResetControl))
	req.AddParam(clusterParam, cluster)
	req.AddParam(taskIdParam, strconv.FormatUint(id, 10))
	req.AddParam(ParamClusterConcurrency, strconv.Itoa(clusterConcurrency))
	req.AddParam(ParamVolConcurrency, strconv.Itoa(volConcurrency))
	req.AddParam(ParamPartitionConcurrency, strconv.Itoa(partitionConcurrency))
	req.AddParam(ParamWaitSecond, strconv.Itoa(waitSeconds))

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

func formatVolMigrateView(info *cproto.ReBalanceInfoTable) *cproto.VolMigrateView {
	view := &cproto.VolMigrateView{
		TaskID:                info.ID,
		Cluster:               info.Cluster,
		Volumes:               strings.Split(info.VolName, ","),
		Module:                formatRType(info.RType),
		Status:                info.Status,
		SrcZone:               info.ZoneName,
		DstZone:               info.DstZone,
		ClusterConcurrency:    info.MaxBatchCount,
		VolConcurrency:        info.VolBatchCount,
		PartitionConcurrency:  info.PartitionBatchCount,
		WaitSeconds:           info.RoundInterval,
		DstDataNodeUsageRatio: info.HighRatio,
		DstMetaPartitionLimit: info.DstMetaNodePartitionMaxCount,
		CreatedAt:             info.CreatedAt.Format(time.DateTime),
		UpdatedAt:             info.UpdatedAt.Format(time.DateTime),
	}
	return view
}
