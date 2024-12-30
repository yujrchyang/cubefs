package flowSchedule

import (
	"fmt"
	"strings"

	"github.com/cubefs/cubefs/console/cutil"
	cproto "github.com/cubefs/cubefs/console/proto"
)

const (
	clusterCommonPrefix = "chubaofs_cluster_"
	clusterCommonSuffix = "_latency_table_view_v2_dist "
)

func (f *FlowSchedule) ClusterDetailsFromClickHouse(req *cproto.TrafficRequest) (results [][]*cproto.FlowScheduleResult, err error) {
	if req.ClusterName == "" || req.Module == "" {
		return nil, fmt.Errorf("require params: cluster:%s and module:%s", req.ClusterName, req.Module)
	}
	var (
		sqlLine string
		level   cproto.DataGranularity
	)
	parseOrderAndTopN(req, queryDetail)
	if level, err = getStartAndEndFromRequest(req, false); err != nil {
		return
	}
	timeFilter := getSelectDateFormat(level)
	table := clusterCommonPrefix + getTableLevelFromDataGranularity(level) + clusterCommonSuffix
	var part string
	if cutil.GlobalCluster == "spark" {
		part = fmt.Sprintf("sum(total_count) as count, " +
			"sum(total_size) as size, " +
			"CEIL(sum(total_size) / sum(total_count)) as avg_size ")
	} else {
		part = fmt.Sprintf("sum(count) as total_count, " +
			"sum(size) as total_size, " +
			"CEIL(sum(size) / sum(count)) as avg_size ")
		table = getOriginalTable(false)
	}
	sqlLine = fmt.Sprintf(""+
		"SELECT "+
		"%s, "+
		"action, "+
		"%s "+
		"FROM "+
		"chubaofs_monitor_data_storage.%s "+
		"WHERE "+
		"event_date >= '%s' AND event_date <= '%s' "+
		"AND cluster_name = '%s' "+
		"AND module = '%s' ",
		timeFilter, part, table, parseTimestampToDataTime(req.StartTime), parseTimestampToDataTime(req.EndTime), req.ClusterName, req.Module)
	if req.Zone != "" {
		sqlLine += fmt.Sprintf(""+
			"AND zone = '%s' ", req.Zone)
	}
	if req.OperationType == "" {
		topActionSqlLine := fmt.Sprintf(""+
			"SELECT "+
			"action "+
			"FROM "+
			"chubaofs_monitor_data_storage.%s "+
			"WHERE "+
			"event_date >= '%s' AND event_date <= '%s' "+
			"AND cluster_name = '%s' "+
			"AND module = '%s' ",
			table, parseTimestampToDataTime(req.StartTime), parseTimestampToDataTime(req.EndTime), req.ClusterName, req.Module)
		if req.Module == strings.ToLower(cproto.ModuleMetaNode) {
			topActionSqlLine += "AND action LIKE 'op%' "
		}
		topActionSqlLine += fmt.Sprintf(""+
			"GROUP BY "+
			"action "+
			"ORDER BY "+
			"SUM(%s) DESC "+
			"LIMIT "+
			"%v",
			req.OrderBy, req.TopN)
		sqlLine += fmt.Sprintf(""+
			"AND action global in (%s) ", topActionSqlLine)
	} else {
		sqlLine += fmt.Sprintf("AND action = '%s' ", req.OperationType)
	}
	sqlLine += fmt.Sprintf("" +
		"GROUP BY " +
		"event_date, cluster_name, module, action " +
		"ORDER BY " +
		"action, event_date")
	return f.queryDetail(sqlLine, cproto.ActionDrawLineDimension)
}

func (f *FlowSchedule) ClusterLatency(req *cproto.TrafficRequest) (result []*cproto.FlowScheduleResult, err error) {
	var (
		level          cproto.DataGranularity
		timeFilter     string
		quantileFilter string
		table          string
	)
	if level, err = getStartAndEndFromRequest(req, false); err != nil {
		return
	}
	timeFilter = getSelectDateFormat(level)
	quantileFilter = "quantileExact(0.99)(tp99)"
	if level >= cproto.MinuteGranularity && req.EndTime-req.StartTime > 12*60*60 {
		quantileFilter = "CEIL(quantileTDigest(0.99)(tp99))"
	}

	table = getLatencyQueryTable(req.ClusterName, req.Module)
	sqlLine := fmt.Sprintf(""+
		"SELECT "+
		"%s, "+
		"SUM(count) as total_count, "+
		"MAX(max) as max_latency, "+
		"CEIL(SUM(count * avg) / total_count) as avg_latency, "+
		"%s as tp99_latency "+
		"FROM "+
		"chubaofs_monitor_data_storage.%s "+
		"WHERE "+
		"event_date >= '%s' AND event_date <= '%s' "+
		"AND module = '%s' "+
		"AND action = '%s' ",
		timeFilter, quantileFilter, table, parseTimestampToDataTime(req.StartTime), parseTimestampToDataTime(req.EndTime),
		req.Module, req.OperationType)
	if req.Zone != "" {
		sqlLine += fmt.Sprintf(""+
			"AND zone = '%s' ", req.Zone)
	}
	// 延时 groupBy 为啥不用加zone条件字段
	sqlLine += fmt.Sprintf("" +
		"GROUP BY " +
		"event_date, module, action " +
		"ORDER BY " +
		"event_date ASC ")

	return f.queryLatency(sqlLine)
}

func (f *FlowSchedule) ZoneActionLatency(req *cproto.TrafficRequest) (result [][]*cproto.FlowScheduleResult, err error) {
	// 1. 解析时间
	var (
		tableLevel          cproto.DataGranularity
		timeFilter          string
		quantileFilter      string
		zoneFilter          string
		clusterFilter       string // 查询原表的需要cluster条件
		actionFilterSqlLine string // 操作类型不用指定， 展示多个
		sqlLine             string
	)
	if tableLevel, err = getStartAndEndFromRequestZoneLatency(req); err != nil {
		return nil, err
	}
	if cproto.IsRelease(req.ClusterName) || req.ClusterName == "cfs_AMS_MCA" {
		zoneFilter = fmt.Sprintf("1 = 1")
	} else {
		zoneFilter = fmt.Sprintf("zone = '%s'", req.Zone)
	}

	table := clusterCommonPrefix + "min" + clusterCommonSuffix
	actionFilterSqlLine = fmt.Sprintf(""+
		"SELECT "+
		"action "+
		"FROM "+
		"chubaofs_monitor_data_storage.%s "+
		"WHERE "+
		"event_date >= '%s' AND event_date <= '%s' "+
		"AND cluster_name = '%s' "+
		"AND module = '%s' "+
		"AND %s "+
		"GROUP BY "+
		"action ",
		table, parseTimestampToDataTime(req.StartTime), parseTimestampToDataTime(req.EndTime), req.ClusterName, req.Module, zoneFilter,
	)

	timeFilter = getSelectDateFormat(tableLevel)
	quantileFilter = "quantileExact(0.99)(tp99)"
	if tableLevel >= cproto.MinuteGranularity && req.EndTime-req.StartTime > 12*60*60 {
		quantileFilter = "CEIL(quantileTDigest(0.99)(tp99))"
	}
	table = getLatencyQueryTable(req.ClusterName, req.Module)
	if req.ClusterName == "cfs_AMS_MCA" {
		clusterFilter = fmt.Sprintf("cluster_name = '%s'", req.ClusterName)
	} else {
		clusterFilter = "1 = 1"
	}
	sqlLine = fmt.Sprintf(""+
		"SELECT "+
		"%s, "+
		"action, "+
		"SUM(count) as total_count, "+
		"SUM(size) as total_size, "+
		"MAX(max) as max_latency, "+
		"%s as tp99_latency, "+
		"CEIL(SUM(count * avg) / total_count) as avg_latency "+
		"FROM "+
		"chubaofs_monitor_data_storage.%s "+
		"WHERE "+
		"event_date >= '%s' AND event_date <= '%s' "+
		"AND %s "+
		"AND module = '%s' "+
		"AND %s "+
		"AND action global in (%s) "+
		"GROUP BY "+
		"event_date, module, zone, action "+
		"ORDER BY "+
		"event_date, action ASC",
		timeFilter, quantileFilter, table, parseTimestampToDataTime(req.StartTime), parseTimestampToDataTime(req.EndTime),
		clusterFilter, req.Module, zoneFilter, actionFilterSqlLine,
	)
	return f.queryActionsLatency(sqlLine)
}

func (f *FlowSchedule) ClusterLatencyDetail(req *cproto.TrafficRequest) (results []*cproto.FlowScheduleResult, err error) {
	var (
		sqlLine string
	)
	parseOrderAndTopN(req, queryLatency)
	if req.StartTime%60 == 0 {
		// bug: 整点的秒级 认为是分钟级
		req.EndTime = req.StartTime + 59
	}
	table := getOriginalTable(cutil.GlobalCluster == "spark")
	sqlLine = fmt.Sprintf(""+
		"SELECT "+
		"volume_name, ip, zone, pid, disk_path, count, size, max, avg, tp99 "+
		"FROM "+
		"chubaofs_monitor_data_storage.%s "+
		"WHERE "+
		"event_date >= '%s' "+
		"AND event_date <= '%s' "+
		"AND cluster_name = '%s' "+
		"AND module = '%s' "+
		"AND action = '%s' ",
		table, parseTimestampToDataTime(req.StartTime), parseTimestampToDataTime(req.EndTime),
		req.ClusterName, req.Module, req.OperationType)
	if req.Zone != "" {
		if cproto.IsRelease(req.ClusterName) || req.ClusterName == "cfs_AMS_MCA" {
			sqlLine += fmt.Sprintf("AND 1 = 1 ")
		} else {
			sqlLine += fmt.Sprintf("AND zone = '%s' ", req.Zone)
		}
	}
	if req.IpAddr != "" {
		sqlLine += fmt.Sprintf("AND ip = '%s' ", req.IpAddr)
	}
	if req.VolumeName != "" {
		sqlLine += fmt.Sprintf("AND volume_name = '%s' ", req.VolumeName)
	}
	if req.Disk != "" {
		sqlLine += fmt.Sprintf("AND disk_path = '%s' ", req.Disk)
	}
	sqlLine += fmt.Sprintf(""+
		"ORDER BY "+
		"%s DESC "+
		"LIMIT %v",
		req.OrderBy, req.TopN)
	return f.queryLatencyDetail(sqlLine)
}
