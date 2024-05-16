package flowSchedule

import (
	"fmt"
	"github.com/cubefs/cubefs/console/cutil"
	cproto "github.com/cubefs/cubefs/console/proto"
)

const (
	ipCommonPrefix = "chubaofs_ip_"
	ipCommonSuffix = "_latency_table_view_v2_dist"
)

func (f *FlowSchedule) IPDetailsFromClickHouse(req *cproto.TrafficRequest) (results [][]*cproto.FlowScheduleResult, err error) {
	if req.ClusterName == "" || req.Module == "" || req.IpAddr == "" {
		return nil, fmt.Errorf("require params: cluster:%s and module:%s and ip:%s", req.ClusterName, req.Module, req.IpAddr)
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
	table := ipCommonPrefix + getTableLevelFromDataGranularity(level) + ipCommonSuffix
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
		"AND ip = '%s' ",
		timeFilter, part, table, parseTimestampToDataTime(req.StartTime), parseTimestampToDataTime(req.EndTime),
		req.ClusterName, req.Module, req.IpAddr)
	if req.Disk != "" {
		sqlLine += fmt.Sprintf(""+
			"AND disk_path = '%s' ", req.Disk)
	}
	if req.OperationType == "" {
		// todo：和上面同一张聚合表
		topActionSqlLine := fmt.Sprintf(""+
			"SELECT "+
			"action "+
			"FROM "+
			"chubaofs_monitor_data_storage.%s "+
			"WHERE "+
			"event_date >= '%s' AND event_date <= '%s' "+
			"AND cluster_name = '%s' "+
			"AND module = '%s' "+
			"AND ip = '%s' "+
			"GROUP BY "+
			"action "+
			"ORDER BY "+
			"SUM(%s) DESC "+
			"LIMIT "+
			"%v",
			table, parseTimestampToDataTime(req.StartTime), parseTimestampToDataTime(req.EndTime),
			req.ClusterName, req.Module, req.IpAddr, req.OrderBy, req.TopN)
		sqlLine += fmt.Sprintf(""+
			"AND action global in (%s) ", topActionSqlLine)
	} else {
		sqlLine += fmt.Sprintf(""+
			"AND action = '%s' ", req.OperationType)
	}
	sqlLine += fmt.Sprintf("" +
		"GROUP BY " +
		"event_date,cluster_name, module, action, ip " +
		"ORDER BY " +
		"action, event_date")
	return f.queryDetail(sqlLine, cproto.ActionDrawLineDimension)
}

// disk维度详情曲线
func (f *FlowSchedule) IPDetailDiskLevel(req *cproto.TrafficRequest) (result [][]*cproto.FlowScheduleResult, err error) {
	var level cproto.DataGranularity
	if level, err = getStartAndEndFromRequest(req, false); err != nil {
		return
	}
	timeFilter := getSelectDateFormat(level)
	table := ipCommonPrefix + getTableLevelFromDataGranularity(level) + ipCommonSuffix
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
	sqlLine := fmt.Sprintf(""+
		"SELECT "+
		"%s, "+
		"disk_path, "+
		"%s "+
		"FROM "+
		"chubaofs_monitor_data_storage.%s "+
		"WHERE "+
		"event_date >= '%s' AND event_date <= '%s' "+
		"AND cluster_name = '%s' "+
		"AND module = '%s' "+
		"AND action = '%s' "+
		"AND ip = '%s' ",
		timeFilter, part, table, parseTimestampToDataTime(req.StartTime), parseTimestampToDataTime(req.EndTime),
		req.ClusterName, req.Module, req.OperationType, req.IpAddr)
	if req.Disk != "" {
		sqlLine += fmt.Sprintf(""+
			"AND disk_path = '%s' ", req.Disk)
	}
	sqlLine += fmt.Sprintf("" +
		"GROUP BY " +
		"event_date, cluster_name, module, action, ip, disk_path " +
		"ORDER BY " +
		"event_date, disk_path")
	return f.queryDetail(sqlLine, cproto.DiskDrawLineDimension)
}

// 延时曲线（不用区分disk action）
func (f *FlowSchedule) IpLatency(req *cproto.TrafficRequest) (result []*cproto.FlowScheduleResult, err error) {
	var level cproto.DataGranularity
	if level, err = getStartAndEndFromRequest(req, false); err != nil {
		return
	}
	timeFilter := getSelectDateFormat(level)
	quantileFilter := "quantileExact(0.99)(tp99)"
	if level >= cproto.MinuteGranularity && req.EndTime-req.StartTime > 12*60*60 {
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
		"AND module = '%s' "+
		"AND action = '%s' "+
		"AND ip = '%s' ",
		timeFilter, quantileFilter, table, parseTimestampToDataTime(req.StartTime), parseTimestampToDataTime(req.EndTime),
		req.Module, req.OperationType, req.IpAddr)
	if req.Disk != "" {
		sqlLine += fmt.Sprintf(""+
			"AND disk_path = '%s'", req.Disk)
	}
	sqlLine += fmt.Sprintf("" +
		"GROUP BY " +
		"event_date, module, action, ip " +
		"ORDER BY " +
		"event_date ASC")
	return f.queryLatency(sqlLine)
}

func (f *FlowSchedule) IpLatencyDetail(req *cproto.TrafficRequest) (results []*cproto.FlowScheduleResult, err error) {
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
		"event_date >= %s "+
		"AND event_date <= %s "+
		"AND cluster_name = %s "+
		"AND module = %s "+
		"AND action = %s "+
		"AND ip = %s ",
		table, req.ClusterName, req.Module, req.OperationType, req.IpAddr,
		parseTimestampToDataTime(req.StartTime), parseTimestampToDataTime(req.EndTime))
	if req.Disk != "" {
		sqlLine += fmt.Sprintf("AND disk = %s ", req.Disk)
	}
	sqlLine += fmt.Sprintf(""+
		"ORDER BY "+
		"%s DESC "+
		"LIMIT "+
		"%v",
		req.OrderBy, req.TopN)
	return f.queryLatencyDetail(sqlLine)
}
