package flowSchedule

import (
	"fmt"
	"github.com/cubefs/cubefs/console/cutil"
	cproto "github.com/cubefs/cubefs/console/proto"
)

var (
	defaultLimitSize = 3000
)

func (f *FlowSchedule) ListTopPartitionClickHouse(req *cproto.TrafficRequest) (result []*cproto.FlowScheduleResult, err error) {
	if req.ClusterName == "" || req.Module == "" {
		return nil, fmt.Errorf("require params: cluster:%s and module:%s", req.ClusterName, req.Module)
	}
	var (
		sqlLine string
	)
	parseOrderAndTopN(req, queryTopN)
	if _, err = getStartAndEndFromRequest(req, true); err != nil {
		return
	}
	// 只能查原表，聚合表没有过滤条件
	table := getOriginalTable(cutil.GlobalCluster == "spark")
	sqlLine = fmt.Sprintf(""+
		"SELECT "+
		"pid, "+
		"volume_name, "+
		"sum(count) as total_count, "+
		"sum(size) as total_size, "+
		"CEIL(sum(size) / sum(count)) as avg_size "+
		"FROM "+
		"chubaofs_monitor_data_storage.%s "+
		"WHERE  "+
		"cluster_name = '%s' "+
		"AND module = '%s' "+
		"AND event_date >= '%s' "+
		"AND event_date <= '%s' ",
		table, req.ClusterName, req.Module, parseTimestampToDataTime(req.StartTime), parseTimestampToDataTime(req.EndTime))

	if req.OperationType != "" {
		sqlLine += fmt.Sprintf("AND action = '%s' ", req.OperationType)
	}
	if req.IpAddr != "" {
		sqlLine += fmt.Sprintf("AND ip = '%s' ", req.IpAddr)
	}
	if req.VolumeName != "" {
		sqlLine += fmt.Sprintf("AND volume_name = '%s' ", req.VolumeName)
	}
	if req.Zone != "" {
		sqlLine += fmt.Sprintf("AND zone = '%s' ", req.Zone)
	}
	if req.Disk != "" {
		sqlLine += fmt.Sprintf("AND disk_path = '%s' ", req.Disk)
	}

	sqlLine += fmt.Sprintf(""+
		"GROUP BY "+
		"pid, volume_name "+
		"ORDER BY "+
		"%s DESC "+
		"LIMIT "+
		"%v",
		req.OrderBy, req.TopN)
	return f.queryTopN(sqlLine, "pid")
}
