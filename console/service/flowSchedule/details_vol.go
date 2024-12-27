package flowSchedule

import (
	"fmt"
	"github.com/cubefs/cubefs/console/cutil"
	cproto "github.com/cubefs/cubefs/console/proto"
	"github.com/cubefs/cubefs/util/log"
	"strings"
)

const (
	volCommonPrefix = "chubaofs_volume_"
	volCommonSuffix = "_latency_table_view_v2_dist"
)

func (f *FlowSchedule) VolDetailsFromClickHouse(req *cproto.TrafficRequest) (results [][]*cproto.FlowScheduleResult, err error) {
	if req.ClusterName == "" || req.Module == "" || req.VolumeName == "" {
		return nil, fmt.Errorf("require params: cluster:%s and module:%s and volume:%s", req.ClusterName, req.Module, req.VolumeName)
	}
	var (
		sqlLine string
	)
	parseOrderAndTopN(req, queryDetail)
	var level cproto.DataGranularity
	if level, err = getStartAndEndFromRequest(req, false); err != nil {
		return
	}
	timeFilter := getSelectDateFormat(level)
	table := volCommonPrefix + getTableLevelFromDataGranularity(level) + volCommonSuffix
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
	sqlLine = fmt.Sprintf("SELECT "+
		"%s, "+
		"action, "+
		"%s "+
		"FROM "+
		"chubaofs_monitor_data_storage.%s "+
		"WHERE "+
		"event_date >= '%s' AND event_date <= '%s' "+
		"AND cluster_name = '%s' "+
		"AND module = '%s' "+
		"AND volume_name = '%s' ",
		timeFilter, part, table, parseTimestampToDataTime(req.StartTime), parseTimestampToDataTime(req.EndTime),
		req.ClusterName, req.Module, req.VolumeName)

	if req.OperationType == "" {
		topActionSqlLine := fmt.Sprintf(""+
			"SELECT "+
			"action "+
			"FROM "+
			"chubaofs_monitor_data_storage.%s  "+
			"WHERE "+
			"event_date >= '%s' AND event_date <= '%s' "+
			"AND cluster_name = '%s' "+
			"AND module = '%s' "+
			"AND volume_name = '%s' ",
			table, parseTimestampToDataTime(req.StartTime), parseTimestampToDataTime(req.EndTime), req.ClusterName, req.Module, req.VolumeName)
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
		sqlLine += fmt.Sprintf("AND action global in (%s) ", topActionSqlLine)
	} else {
		// 统计某个op(module) 某个vol
		sqlLine += fmt.Sprintf("AND action = '%s' ", req.OperationType)
	}
	sqlLine += fmt.Sprintf("" +
		"GROUP BY " +
		"event_date, cluster_name, volume_name, module, action " +
		"ORDER BY " +
		"action, event_date")
	return f.queryDetail(sqlLine, cproto.ActionDrawLineDimension)
}

func (f *FlowSchedule) VolLatency(req *cproto.TrafficRequest) (result []*cproto.FlowScheduleResult, err error) {
	var level cproto.DataGranularity
	if level, err = getStartAndEndFromRequest(req, false); err != nil {
		log.LogErrorf("parse start end time from request failed: err(%v)", err)
		return
	}
	timeFilter := getSelectDateFormat(level)
	quantileFilter := "quantileExact(0.99)(tp99)"
	if level >= cproto.MinuteGranularity && req.EndTime-req.StartTime > 6*oneHourInSecond {
		quantileFilter = "CEIL(quantileTDigest(0.99)(tp99))"
	}
	table := getLatencyQueryTable(req.ClusterName, req.Module)
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
		"AND volume_name = '%s' "+
		"AND module = '%s' "+
		"AND action = '%s' "+
		"GROUP BY "+
		"event_date, volume_name, module, action "+
		"ORDER BY "+
		"event_date ASC ",
		timeFilter, quantileFilter, table, parseTimestampToDataTime(req.StartTime), parseTimestampToDataTime(req.EndTime),
		req.VolumeName, req.Module, req.OperationType)
	return f.queryLatency(sqlLine)
}

// 一般只查询一分钟或一秒钟的数据
func (f *FlowSchedule) VolLatencyDetail(req *cproto.TrafficRequest) (results []*cproto.FlowScheduleResult, err error) {
	var (
		sqlLine string
	)
	parseOrderAndTopN(req, queryLatency)
	if req.StartTime%60 == 0 {
		// 整点 认为是分钟级
		req.EndTime = req.StartTime + 59
	}
	table := getOriginalTable(cutil.GlobalCluster == "spark")
	sqlLine = fmt.Sprintf(""+
		"SELECT "+
		"volume_name, ip, zone, pid, disk_path, count, size, max, avg, tp99 "+
		"FROM "+
		"chubaofs_monitor_data_storage.%s "+
		"WHERE "+
		"cluster_name = %s "+
		"AND module = %s "+
		"AND action = %s "+
		"AND volume_name = %s "+
		"AND event_date >= %s "+
		"AND event_date <= %s "+
		"ORDER BY "+
		"%s DESC "+
		"LIMIT "+
		"%v",
		table, req.ClusterName, req.Module, req.OperationType, req.VolumeName,
		parseTimestampToDataTime(req.StartTime), parseTimestampToDataTime(req.EndTime),
		req.OrderBy, req.TopN)
	return f.queryLatencyDetail(sqlLine)
}
