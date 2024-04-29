package rebalance

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util/log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	SPARK       = "spark"
	DBBAK       = "cfs_dbBack"
	ELASTICDB   = "mysql"
	CFS_AMS_MCA = "cfs_AMS_MCA"
	OCHAMA      = "nl_ochama"
	TEST        = "delete_ek_test"
	TestES      = "test-es-db"
)

func (rw *ReBalanceWorker) handleStart(w http.ResponseWriter, req *http.Request) (int, interface{}, error) {
	cluster := req.URL.Query().Get(ParamCluster)
	moduleTypeStr := req.URL.Query().Get(ParamRebalanceType)
	taskTypeStr := req.URL.Query().Get(ParamTaskType)
	if cluster == "" || moduleTypeStr == "" || taskTypeStr == "" {
		return http.StatusBadRequest, nil, ErrParamsNotFount
	}
	taskType, err := strconv.Atoi(taskTypeStr)
	if err != nil || TaskType(taskType) >= MaxTaskType {
		return http.StatusBadRequest, 0, fmt.Errorf("invalid task type: %v", taskTypeStr)
	}
	if err = verifyCluster(cluster); err != nil {
		return http.StatusBadRequest, nil, err
	}
	rType, err := ConvertRebalanceTypeStr(moduleTypeStr)
	if err != nil {
		return http.StatusBadRequest, nil, err
	}
	// zoneAutoRebalance
	zoneName := req.URL.Query().Get(ParamZoneName)
	highRatioStr := req.URL.Query().Get(ParamHighRatio)
	lowRatioStr := req.URL.Query().Get(ParamLowRatio)
	goalRatioStr := req.URL.Query().Get(ParamGoalRatio)
	// nodesMigrate
	srcNodesStr := req.URL.Query().Get(ParamSrcNodesList)
	dstNodesStr := req.URL.Query().Get(ParamDstNodesList)
	// common
	maxBatchCountStr := req.URL.Query().Get(ParamClusterMaxBatchCount)
	migrateLimitPerDiskStr := req.URL.Query().Get(ParamMigrateLimitPerDisk)
	dstMetaNodePartitionMaxCountStr := req.URL.Query().Get(ParamDstMetaNodePartitionMaxCount)

	maxBatchCount := defaultClusterMaxBatchCount
	if maxBatchCountStr != "" {
		if tmp, err := strconv.Atoi(maxBatchCountStr); err == nil {
			maxBatchCount = tmp
		}
	}
	migrateLimitPerDisk := defaultMigrateLimitPerDisk
	if migrateLimitPerDiskStr != "" {
		if tmp, err := strconv.Atoi(migrateLimitPerDiskStr); err == nil {
			migrateLimitPerDisk = tmp
		}
	}
	dstMetaNodePartitionMaxCount := defaultDstMetaNodePartitionMaxCount
	if dstMetaNodePartitionMaxCountStr != "" {
		if tmp, err := strconv.Atoi(dstMetaNodePartitionMaxCountStr); err == nil && tmp < defaultDstMetaNodePartitionMaxCount {
			dstMetaNodePartitionMaxCount = tmp
		}
	}

	switch TaskType(taskType) {
	case ZoneAutoReBalance:
		if zoneName == "" || highRatioStr == "" || lowRatioStr == "" || goalRatioStr == "" {
			return http.StatusBadRequest, nil, ErrParamsNotFount
		}
		highRatio, er := strconv.ParseFloat(highRatioStr, 64)
		if er != nil {
			return http.StatusBadRequest, nil, er
		}
		lowRatio, er := strconv.ParseFloat(lowRatioStr, 64)
		if er != nil {
			return http.StatusBadRequest, nil, er
		}
		goalRatio, er := strconv.ParseFloat(goalRatioStr, 64)
		if er != nil {
			return http.StatusBadRequest, nil, er
		}
		if er = checkRatio(highRatio, lowRatio, goalRatio); er != nil {
			return http.StatusBadRequest, nil, er
		}
		_, err = rw.ReBalanceStart(cluster, zoneName, rType, highRatio, lowRatio, goalRatio, maxBatchCount, migrateLimitPerDisk,
			dstMetaNodePartitionMaxCount)
		if err != nil {
			return http.StatusInternalServerError, nil, err
		}

	case NodesMigrate:
		if srcNodesStr == "" || dstNodesStr == "" {
			return http.StatusBadRequest, nil, ErrParamsNotFount
		}
		isValid, srcNodeList, dstNodeList, commonNode := checkInputAddrValid(srcNodesStr, dstNodesStr)
		if !isValid {
			return http.StatusBadRequest, nil, fmt.Errorf("%v: %v", ErrInputNodesInvalid, commonNode)
		}
		_, err = rw.NodesReBalanceStart(cluster, rType, maxBatchCount, dstMetaNodePartitionMaxCount, srcNodeList, dstNodeList)
		if err != nil {
			return http.StatusInternalServerError, nil, err
		}
	}
	return http.StatusOK, nil, nil
}

func checkInputAddrValid(srcNodes, dstNodes string) (bool, []string, []string, []string) {
	// srcNodes 和 dstNodes 不能有重叠的, src/dst 内部去重
	srcList := strings.Split(strings.TrimSpace(srcNodes), ",")
	dstList := strings.Split(strings.TrimSpace(dstNodes), ",")
	commonNodes := make([]string, 0)
	srcNodesMap := make(map[string]struct{})
	dstNodesMap := make(map[string]struct{})
	for _, srcNode := range srcList {
		srcNodesMap[srcNode] = struct{}{}
	}
	for _, dstNode := range dstList {
		if _, ok := srcNodesMap[dstNode]; ok {
			commonNodes = append(commonNodes, dstNode)
		} else {
			dstNodesMap[dstNode] = struct{}{}
		}
	}
	if len(commonNodes) > 0 {
		return false, nil, nil, commonNodes
	}
	srcList = make([]string, 0, len(srcNodesMap))
	dstList = make([]string, 0, len(dstNodesMap))
	for src := range srcNodesMap {
		srcList = append(srcList, src)
	}
	for dst := range dstNodesMap {
		dstList = append(dstList, dst)
	}
	return true, srcList, dstList, nil
}

func (rw *ReBalanceWorker) handleReSetControlParam(w http.ResponseWriter, req *http.Request) (int, interface{}, error) {
	taskIdStr := req.URL.Query().Get(ParamQueryTaskId)
	if taskIdStr == "" {
		return http.StatusBadRequest, nil, ErrParamsNotFount
	}
	taskID, err := strconv.ParseUint(taskIdStr, 10, 64)
	if err != nil {
		return http.StatusBadRequest, nil, err
	}

	goalRatioStr := req.URL.Query().Get(ParamGoalRatio)
	maxBatchCountStr := req.URL.Query().Get(ParamClusterMaxBatchCount)
	migrateLimitPerDiskStr := req.URL.Query().Get(ParamMigrateLimitPerDisk)
	dstMetaNodePartitionMaxCountStr := req.URL.Query().Get(ParamDstMetaNodePartitionMaxCount)
	var (
		goalRatio              float64
		maxBatchCount          int
		migrateLimitPerDisk    int
		dstMNPartitionMaxCount int
	)
	if goalRatioStr != "" {
		goalRatio, err = strconv.ParseFloat(goalRatioStr, 64)
		if err != nil {
			return http.StatusBadRequest, nil, err
		}
	}
	if maxBatchCountStr != "" {
		maxBatchCount, err = strconv.Atoi(maxBatchCountStr)
		if err != nil {
			return http.StatusBadRequest, nil, err
		}
	}
	if migrateLimitPerDiskStr != "" {
		migrateLimitPerDisk, err = strconv.Atoi(migrateLimitPerDiskStr)
		if err != nil {
			return http.StatusBadRequest, nil, err
		}
	}
	if dstMetaNodePartitionMaxCountStr != "" {
		dstMNPartitionMaxCount, err = strconv.Atoi(dstMetaNodePartitionMaxCountStr)
		if err != nil {
			return http.StatusBadRequest, nil, err
		}
	}
	err = rw.ReSetControlParam(taskID, goalRatio, maxBatchCount, migrateLimitPerDisk, dstMNPartitionMaxCount)
	if err != nil {
		return http.StatusInternalServerError, nil, err
	}
	return http.StatusOK, nil, nil
}

func (rw *ReBalanceWorker) handleStop(w http.ResponseWriter, req *http.Request) (int, interface{}, error) {
	cluster := req.URL.Query().Get(ParamCluster)
	err := verifyCluster(cluster)
	if err != nil {
		return http.StatusBadRequest, nil, err
	}
	taskIdStr := req.URL.Query().Get(ParamQueryTaskId)
	if taskIdStr == "" {
		return http.StatusBadRequest, nil, ErrParamsNotFount
	}
	taskID, err := strconv.ParseUint(taskIdStr, 10, 64)
	if err != nil {
		return http.StatusBadRequest, nil, err
	}

	err = rw.ReBalanceStop(taskID)
	if err != nil {
		return http.StatusInternalServerError, nil, err
	}
	return http.StatusOK, nil, nil
}

// 迁移进度(进行中)、迁移详情(已完成)接口、手动停止
func (rw *ReBalanceWorker) handleStatus(w http.ResponseWriter, req *http.Request) (int, interface{}, error) {
	taskIdStr := req.URL.Query().Get(ParamQueryTaskId)
	if taskIdStr == "" {
		return http.StatusBadRequest, nil, ErrParamsNotFount
	}
	taskId, err := strconv.ParseUint(taskIdStr, 10, 64)
	if err != nil {
		return http.StatusBadRequest, nil, err
	}

	var rInfo *RebalancedInfoTable
	rInfo, err = rw.GetRebalancedInfoByID(taskId)
	if err != nil {
		return http.StatusInternalServerError, nil, err
	}

	var statusInfo *RebalanceStatusInfo
	switch rInfo.Status {
	case int(StatusStop):
		// 历史的： 查询迁移总数 查表
		statusInfo, err = rw.getStoppedTaskStatus(rInfo, Status(rInfo.Status))

	case int(StatusRunning):
		// 进行中的：查询内存中记录的
		statusInfo, err = rw.getRunningTaskStatus(rInfo)

	case int(StatusTerminating):
		statusInfo, err = rw.getStoppedTaskStatus(rInfo, Status(rInfo.Status))

	default:
		return http.StatusBadRequest, nil, ErrWrongStatus
	}

	if err != nil {
		return http.StatusInternalServerError, nil, err
	}
	return http.StatusOK, statusInfo, nil
}

func (rw *ReBalanceWorker) handleReset(w http.ResponseWriter, req *http.Request) (int, interface{}, error) {
	rw.ResetZoneMap()
	return http.StatusOK, nil, nil
}

func (rw *ReBalanceWorker) handleRebalancedInfo(w http.ResponseWriter, req *http.Request) (int, interface{}, error) {
	id := req.URL.Query().Get(ParamQueryTaskId)
	idNum, err := strconv.ParseUint(id, 10, 64)
	if err != nil {
		return http.StatusBadRequest, nil, err
	}
	var rInfo *ReBalanceInfo
	rw.reBalanceCtrlMap.Range(func(key, value interface{}) bool {
		ctrl := value.(*ZoneReBalanceController)
		if idNum != ctrl.Id {
			return true
		}
		rInfo = ctrl.formatToReBalanceInfo()
		return false
	})
	if rInfo == nil {
		// 通过taskID查表
		recordInfo, err := rw.GetRebalancedInfoByID(idNum)
		if err != nil {
			return http.StatusInternalServerError, nil, err
		}
		rInfo = &ReBalanceInfo{
			*recordInfo,
			nil,
			nil,
		}
	}
	return http.StatusOK, rInfo, nil
}

func (rw *ReBalanceWorker) handleRebalancedList(w http.ResponseWriter, req *http.Request) (code int, total int64, data interface{}, err error) {
	cluster := req.URL.Query().Get(ParamCluster)
	if cluster == "" {
		return http.StatusBadRequest, 0, nil, ErrParamsNotFount
	}
	rTypeStr := req.URL.Query().Get(ParamRebalanceType)
	zoneName := req.URL.Query().Get(ParamZoneName)
	taskTypeStr := req.URL.Query().Get(ParamTaskType)
	statusStr := req.URL.Query().Get(ParamStatus)
	var (
		status   int                // 0, 查全部, 不指定某种任务类型
		rType    = MaxRebalanceType // 查询时，max表示全部，不特指某种类型
		taskType = MaxTaskType
	)
	if err = verifyCluster(cluster); err != nil {
		return http.StatusBadRequest, 0, nil, err
	}
	if status, err = strconv.Atoi(statusStr); err != nil {
		return http.StatusBadRequest, 0, nil, err
	}
	if rTypeStr != "" {
		rType, err = ConvertRebalanceTypeStr(rTypeStr)
		if err != nil {
			return http.StatusBadRequest, 0, nil, err
		}
	}
	if tType, err := strconv.Atoi(taskTypeStr); err != nil || TaskType(tType) > MaxTaskType {
		return http.StatusBadRequest, 0, nil, err
	} else {
		taskType = TaskType(tType)
	}
	page := req.URL.Query().Get(ParamPage)
	pageSize := req.URL.Query().Get(ParamPageSize)
	pageNum, err := strconv.Atoi(page)
	if err != nil {
		return http.StatusBadRequest, 0, nil, err
	}
	pageSizeNum, err := strconv.Atoi(pageSize)
	if err != nil {
		return http.StatusBadRequest, 0, nil, err
	}

	totalCount, err := rw.GetRebalancedInfoTotalCount(cluster, zoneName, rType, taskType, status)
	if err != nil {
		return http.StatusInternalServerError, 0, nil, err
	}
	infoList, err := rw.GetRebalancedInfoList(cluster, zoneName, rType, taskType, status, pageNum, pageSizeNum)
	if err != nil {
		return http.StatusInternalServerError, 0, nil, err
	}
	return http.StatusOK, totalCount, infoList, nil
}

func (rw *ReBalanceWorker) handleMigrateRecordsQuery(w http.ResponseWriter, req *http.Request) (int, int64, interface{}, error) {
	cluster := req.URL.Query().Get(ParamCluster)
	zone := req.URL.Query().Get(ParamZoneName)
	module := req.URL.Query().Get(ParamRebalanceType)
	volume := req.URL.Query().Get(ParamVolName)
	pidStr := req.URL.Query().Get(ParamPid)
	src := req.URL.Query().Get(ParamSrcHost)
	dst := req.URL.Query().Get(ParamDstHost)
	dateStr := req.URL.Query().Get(ParamQueryDate)
	page := req.URL.Query().Get(ParamPage)
	pageSizeStr := req.URL.Query().Get(ParamPageSize)

	cond := make(map[string]interface{})
	if err := verifyCluster(cluster); err != nil {
		return http.StatusBadRequest, 0, nil, err
	}
	cond["cluster_name"] = cluster
	if zone != "" {
		cond["zone_name"] = zone
	}
	var rType = 0
	if module == "meta" {
		rType = 1
	}
	cond["rebalance_type"] = rType
	if volume != "" {
		cond["vol_name"] = volume
	}
	if pidStr != "" {
		pid, err := strconv.ParseUint(pidStr, 10, 64)
		if err != nil {
			return http.StatusBadRequest, 0, nil, err
		}
		cond["partition_id"] = pid
	}
	var createAt string
	if dateStr != "" {
		createTime, err := time.Parse("20060102150405", dateStr)
		if err != nil {
			return http.StatusBadRequest, 0, nil, err
		}
		createAt = createTime.Format(time.DateTime)
	}
	pageNumber, _ := strconv.Atoi(page)
	pageSize, _ := strconv.Atoi(pageSizeStr)
	total, records, err := rw.GetMigrateRecordsByCond(cond, src, dst, createAt, pageNumber, pageSize)
	if err != nil {
		return http.StatusInternalServerError, 0, nil, err
	}
	return http.StatusOK, total, records, err
}

func (rw *ReBalanceWorker) handleZoneUsageRatio(w http.ResponseWriter, req *http.Request) {
	cluster := req.URL.Query().Get(ParamCluster)
	zoneName := req.URL.Query().Get(ParamZoneName)
	err := verifyCluster(cluster)
	if err != nil {
		buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	clusterHost := rw.getClusterHost(cluster)
	typeStr := req.URL.Query().Get(ParamRebalanceType)
	var nodeInfo []*NodeUsageInfo
	switch typeStr {
	case "meta":
		nodeInfo, err = loadMetaNodeUsageRatio(clusterHost, zoneName)
	default:
		nodeInfo, err = loadDataNodeUsageRatio(clusterHost, zoneName)
	}
	if err != nil {
		buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}

	sort.Slice(nodeInfo, func(i, j int) bool {
		return nodeInfo[i].UsageRatio > nodeInfo[j].UsageRatio
	})
	buildSuccessResp(w, nodeInfo)
}

func loadMetaNodeUsageRatio(clusterHost, zoneName string) (metaNodeInfo []*NodeUsageInfo, err error) {
	mc := master.NewMasterClient([]string{clusterHost}, false)
	topologyView, err := mc.AdminAPI().GetTopology()
	if err != nil {
		return
	}
	var zoneMetaNodes []string
	for _, zone := range topologyView.Zones {
		if zone.Name != zoneName {
			continue
		}
		for _, nodeSetView := range zone.NodeSet {
			for _, metaNode := range nodeSetView.MetaNodes {
				zoneMetaNodes = append(zoneMetaNodes, metaNode.Addr)
			}
		}
	}
	var (
		wg sync.WaitGroup
		ch = make(chan struct{}, 10)
		mu sync.Mutex
	)
	for _, metaNodeAddr := range zoneMetaNodes {
		wg.Add(1)
		ch <- struct{}{}
		go func(metaNodeAddr string) {
			defer func() {
				<-ch
				wg.Done()
			}()
			node, errForGet := mc.NodeAPI().GetMetaNode(metaNodeAddr)
			if errForGet != nil {
				log.LogErrorf("handleZoneUsageRatio get dataNode:%v err:%v", metaNodeAddr, errForGet)
				return
			}
			mu.Lock()
			metaNodeInfo = append(metaNodeInfo, &NodeUsageInfo{
				Addr:       metaNodeAddr,
				UsageRatio: node.Ratio,
			})
			mu.Unlock()
		}(metaNodeAddr)
	}
	wg.Wait()
	return
}

func loadDataNodeUsageRatio(host, zoneName string) (dataNodeInfo []*NodeUsageInfo, err error) {
	mc := master.NewMasterClient([]string{host}, false)
	topologyView, err := mc.AdminAPI().GetTopology()
	if err != nil {
		return
	}
	var zoneDataNodes []string
	for _, zone := range topologyView.Zones {
		if zone.Name != zoneName {
			continue
		}
		for _, nodeSetView := range zone.NodeSet {
			for _, dataNode := range nodeSetView.DataNodes {
				zoneDataNodes = append(zoneDataNodes, dataNode.Addr)
			}
		}
	}
	var (
		wg sync.WaitGroup
		ch = make(chan struct{}, 10)
		mu sync.Mutex
	)
	for _, dataNodeAddr := range zoneDataNodes {
		wg.Add(1)
		ch <- struct{}{}
		go func(dataNodeAddr string) {
			defer func() {
				<-ch
				wg.Done()
			}()
			node, errForGet := mc.NodeAPI().GetDataNode(dataNodeAddr)
			if errForGet != nil {
				log.LogErrorf("handleZoneUsageRatio get dataNode:%v err:%v", dataNodeAddr, errForGet)
				return
			}
			var used uint64
			for _, partitionReport := range node.DataPartitionReports {
				used += partitionReport.Used
			}
			mu.Lock()
			dataNodeInfo = append(dataNodeInfo, &NodeUsageInfo{
				Addr:       dataNodeAddr,
				UsageRatio: float64(used) / float64(node.Total),
			})
			mu.Unlock()
		}(dataNodeAddr)
	}
	wg.Wait()
	return
}

func convertDiskView(diskMap map[string]*Disk) (diskView []DiskView) {
	for _, disk := range diskMap {
		diskView = append(diskView, DiskView{
			Path:          disk.path,
			Total:         disk.total,
			Used:          disk.used,
			MigratedSize:  disk.migratedSize,
			MigratedCount: disk.migratedCount,
			MigrateLimit:  disk.migrateLimit,
		})
	}
	sort.Slice(diskView, func(i, j int) bool {
		return diskView[i].Path < diskView[j].Path
	})
	return
}

type handler func(w http.ResponseWriter, req *http.Request) (status int, data interface{}, err error)

func responseHandler(h handler) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		status, data, err := h(w, req)
		if err != nil {
			log.LogWarn(err.Error())
			buildFailureResp(w, status, err.Error())
			return
		}
		buildSuccessResp(w, data)
	}
}

func pagingResponseHandler(h func(w http.ResponseWriter, req *http.Request) (int, int64, interface{}, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		status, total, data, err := h(w, req)
		if err != nil {
			log.LogWarn(err.Error())
			buildFailureResp(w, status, err.Error())
			return
		}
		buildPagingSuccessResp(w, data, total)
	}
}

func buildSuccessResp(w http.ResponseWriter, data interface{}) {
	buildJSONResp(w, http.StatusOK, data, "success")
}

func buildPagingSuccessResp(w http.ResponseWriter, data interface{}, totalCount int64) {
	buildPagingJSONResp(w, http.StatusOK, totalCount, data, "success")
}

func buildFailureResp(w http.ResponseWriter, code int, msg string) {
	buildJSONResp(w, code, nil, msg)
}

// Create response for the API request.
func buildJSONResp(w http.ResponseWriter, code int, data interface{}, msg string) {
	var (
		jsonBody []byte
		err      error
	)
	w.WriteHeader(code)
	w.Header().Set("Content-Type", "application/json")
	body := struct {
		Code int
		Msg  string
		Data interface{}
	}{
		Code: code,
		Msg:  msg,
		Data: data,
	}
	if jsonBody, err = json.Marshal(body); err != nil {
		return
	}
	w.Write(jsonBody)
}

func buildPagingJSONResp(w http.ResponseWriter, code int, totalCount int64, data interface{}, msg string) {
	var (
		jsonBody []byte
		err      error
	)
	w.WriteHeader(code)
	w.Header().Set("Content-Type", "application/json")
	body := struct {
		Code       int
		Msg        string
		TotalCount int64
		Data       interface{}
	}{
		Code:       code,
		Msg:        msg,
		TotalCount: totalCount,
		Data:       data,
	}
	if jsonBody, err = json.Marshal(body); err != nil {
		return
	}
	w.Write(jsonBody)
}
