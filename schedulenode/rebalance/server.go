package rebalance

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util/log"
	"net/http"
	"sort"
	"strconv"
	"sync"
)

const (
	SPARK     = "spark"
	DBBAK     = "dbbak"
	ELASTICDB = "elasticDB"
	TEST      = "test"
)

func (rw *ReBalanceWorker) handleStart(w http.ResponseWriter, req *http.Request) (int, interface{}, error) {
	cluster := req.URL.Query().Get(ParamCluster)
	zoneName := req.URL.Query().Get(ParamZoneName)
	highRatioStr := req.URL.Query().Get(ParamHighRatio)
	lowRatioStr := req.URL.Query().Get(ParamLowRatio)
	goalRatioStr := req.URL.Query().Get(ParamGoalRatio)
	maxBatchCountStr := req.URL.Query().Get(ParamClusterMaxBatchCount)
	migrateLimitPerDiskStr := req.URL.Query().Get(ParamMigrateLimitPerDisk)
	highRatio, err := strconv.ParseFloat(highRatioStr, 64)
	if err != nil {
		return http.StatusBadRequest, nil, err
	}
	lowRatio, err := strconv.ParseFloat(lowRatioStr, 64)
	if err != nil {
		return http.StatusBadRequest, nil, err
	}
	goalRatio, err := strconv.ParseFloat(goalRatioStr, 64)
	if err != nil {
		return http.StatusBadRequest, nil, err
	}
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
	host, err := getClusterHost(cluster)
	if err != nil {
		return http.StatusBadRequest, nil, err
	}
	err = rw.ReBalanceStart(host, zoneName, highRatio, lowRatio, goalRatio, maxBatchCount, migrateLimitPerDisk)
	if err != nil {
		return http.StatusBadRequest, nil, err
	}

	return http.StatusOK, nil, nil
}

func (rw *ReBalanceWorker) handleReSetControlParam(w http.ResponseWriter, req *http.Request) (int, interface{}, error) {
	cluster := req.URL.Query().Get(ParamCluster)
	zoneName := req.URL.Query().Get(ParamZoneName)
	goalRatioStr := req.URL.Query().Get(ParamGoalRatio)
	maxBatchCountStr := req.URL.Query().Get(ParamClusterMaxBatchCount)
	migrateLimitPerDiskStr := req.URL.Query().Get(ParamMigrateLimitPerDisk)
	var (
		goalRatio           float64
		maxBatchCount       int
		migrateLimitPerDisk int
		host                string
		err                 error
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
	host, err = getClusterHost(cluster)
	if err != nil {
		return http.StatusBadRequest, nil, err
	}
	err = rw.ReSetControlParam(host, zoneName, goalRatio, maxBatchCount, migrateLimitPerDisk)
	if err != nil {
		return http.StatusBadRequest, nil, err
	}
	return http.StatusOK, nil, nil
}

func (rw *ReBalanceWorker) handleStop(w http.ResponseWriter, req *http.Request) (int, interface{}, error) {
	cluster := req.URL.Query().Get(ParamCluster)
	zoneName := req.URL.Query().Get(ParamZoneName)
	host, err := getClusterHost(cluster)
	if err != nil {
		return http.StatusBadRequest, nil, err
	}
	err = rw.ReBalanceStop(host, zoneName)
	if err != nil {
		return http.StatusBadRequest, nil, err
	}
	err = rw.stopRebalanced(host, zoneName)
	if err != nil {
		return http.StatusBadRequest, nil, err
	}
	return http.StatusOK, nil, nil
}

func (rw *ReBalanceWorker) handleStatus(w http.ResponseWriter, req *http.Request) (int, interface{}, error) {
	cluster := req.URL.Query().Get(ParamCluster)
	zoneName := req.URL.Query().Get(ParamZoneName)
	host, err := getClusterHost(cluster)
	if err != nil {
		return http.StatusBadRequest, nil, err
	}
	status, err := rw.ReBalanceStatus(host, zoneName)
	if err != nil {
		return http.StatusBadRequest, -1, err
	}
	switch status {
	case StatusStop:
		return http.StatusOK, "Stop", nil
	case StatusRunning:
		return http.StatusOK, "Running", nil
	case StatusTerminating:
		return http.StatusOK, "Terminating", nil
	default:
		return http.StatusInternalServerError, status, fmt.Errorf("wrong Status with status id %v", status)
	}
}

func (rw *ReBalanceWorker) handleReset(w http.ResponseWriter, req *http.Request) (int, interface{}, error) {
	rw.ResetZoneMap()
	return http.StatusOK, nil, nil
}

func (rw *ReBalanceWorker) handleRebalancedInfo(w http.ResponseWriter, req *http.Request) (int, interface{}, error) {
	id := req.URL.Query().Get(ParamId)
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
		var (
			srcNodesUsageRatio []SrcDataNode
			dstNodesUsageRatio []DstDataNode
		)
		for _, node := range ctrl.srcNodes {
			diskView := convertDiskView(node.disks)
			srcNodesUsageRatio = append(srcNodesUsageRatio, SrcDataNode{
				Addr:       node.Addr,
				UsageRatio: node.Usage(),
				Disk:       diskView,
			})
		}
		for _, node := range ctrl.dstNodes {
			dstNodesUsageRatio = append(dstNodesUsageRatio, DstDataNode{
				Addr:       node.Addr,
				UsageRatio: node.UsageRatio,
			})
		}
		reBalanceInfo := &ReBalanceInfo{
			RebalancedInfoTable: RebalancedInfoTable{
				ID:                  ctrl.Id,
				Host:                ctrl.cluster,
				ZoneName:            ctrl.zoneName,
				Status:              int(ctrl.status),
				MaxBatchCount:       ctrl.clusterMaxBatchCount,
				HighRatio:           ctrl.highRatio,
				LowRatio:            ctrl.lowRatio,
				GoalRatio:           ctrl.goalRatio,
				MigrateLimitPerDisk: ctrl.migrateLimitPerDisk,
				CreatedAt:           ctrl.createdAt,
				UpdatedAt:           ctrl.updatedAt,
			},
			SrcNodesUsageRatio: srcNodesUsageRatio,
			DstNodesUsageRatio: dstNodesUsageRatio,
		}
		rInfo = reBalanceInfo
		return false
	})
	return http.StatusOK, rInfo, nil
}

func (rw *ReBalanceWorker) handleRebalancedList(w http.ResponseWriter, req *http.Request) {
	cluster := req.URL.Query().Get(ParamCluster)
	zoneName := req.URL.Query().Get(ParamZoneName)
	var (
		host string
		err  error
	)
	if len(cluster) > 0 {
		host, err = getClusterHost(cluster)
		if err != nil {
			buildFailureResp(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	status := req.URL.Query().Get(ParamStatus)
	statusNum, err := strconv.Atoi(status)
	if err != nil {
		buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	page := req.URL.Query().Get(ParamPage)
	pageNum, err := strconv.Atoi(page)
	if err != nil {
		buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	PageSize := req.URL.Query().Get(ParamPageSize)
	PageSizeNum, err := strconv.Atoi(PageSize)
	if err != nil {
		buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	totalCount, err := rw.GetRebalancedInfoTotalCount(host, zoneName, statusNum)
	if err != nil {
		buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	infoList, err := rw.GetRebalancedInfoList(host, zoneName, pageNum, PageSizeNum, statusNum)
	if err != nil {
		buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	buildPagingSuccessResp(w, infoList, totalCount)
}

func (rw *ReBalanceWorker) handleZoneUsageRatio(w http.ResponseWriter, req *http.Request) {
	cluster := req.URL.Query().Get(ParamCluster)
	zoneName := req.URL.Query().Get(ParamZoneName)
	host, err := getClusterHost(cluster)
	if err != nil {
		buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	mc := master.NewMasterClient([]string{host}, false)
	topologyView, err := mc.AdminAPI().GetTopology()
	if err != nil {
		buildFailureResp(w, http.StatusBadRequest, err.Error())
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
		dataNodeInfo []*DstDataNode
		wg           sync.WaitGroup
		ch           = make(chan struct{}, 10)
		mu           sync.Mutex
	)
	for _, dataNodeAddr := range zoneDataNodes {
		wg.Add(1)
		ch <- struct{}{}
		go func(dataNodeAddr string) {
			defer func() {
				<-ch
				wg.Done()
			}()
			node, err := mc.NodeAPI().GetDataNode(dataNodeAddr)
			if err != nil {
				log.LogErrorf("handleZoneUsageRatio get dataNode:%v err:%v", dataNodeAddr, err)
				return
			}
			var used uint64
			for _, partitionReport := range node.DataPartitionReports {
				used += partitionReport.Used
			}
			mu.Lock()
			dataNodeInfo = append(dataNodeInfo, &DstDataNode{
				Addr:       dataNodeAddr,
				UsageRatio: float64(used) / float64(node.Total),
			})
			mu.Unlock()
		}(dataNodeAddr)
	}
	wg.Wait()
	sort.Slice(dataNodeInfo, func(i, j int) bool {
		return dataNodeInfo[i].UsageRatio > dataNodeInfo[j].UsageRatio
	})
	buildSuccessResp(w, dataNodeInfo)
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

func getClusterHost(cluster string) (host string, err error) {
	switch cluster {
	case SPARK:
		host = "cn.chubaofs.jd.local"
	case DBBAK:
		host = "cn.chubaofs-seqwrite.jd.local"
		err = fmt.Errorf("cluster:%v Not supported", cluster)
	case ELASTICDB:
		host = "cn.elasticdb.jd.local"
		//err = fmt.Errorf("cluster:%v Not supported", cluster)
	case TEST:
		host = "test.chubaofs.jd.local"
	}
	if len(host) == 0 {
		err = fmt.Errorf("cluster:%v Not supported", cluster)
	}
	return
}
