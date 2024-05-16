package flowSchedule

import (
	"fmt"
	"github.com/cubefs/cubefs/console/cutil"
	cproto "github.com/cubefs/cubefs/console/proto"
)

// 次数 流量 可以调用这个接口，延时详情需要调用这个接口
func (f *FlowSchedule) ListTopIPClickHouse(req *cproto.TrafficRequest) (results []*cproto.FlowScheduleResult, err error) {
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
	table := getOriginalTable(cutil.GlobalCluster == "spark")
	sqlLine = fmt.Sprintf(""+
		"SELECT "+
		"ip, "+
		"sum(count) as total_count, "+
		"sum(size) as total_size, "+
		"CEIL(sum(size) / sum(count)) as avg_size "+
		"FROM "+
		"chubaofs_monitor_data_storage.%s "+
		"WHERE "+
		"event_date >= '%s' AND event_date <= '%s' "+
		"AND cluster_name = '%s' "+
		"AND module = '%s' ",
		table, parseTimestampToDataTime(req.StartTime), parseTimestampToDataTime(req.EndTime),
		req.ClusterName, req.Module)
	if req.Zone != "" {
		sqlLine += fmt.Sprintf("AND zone = '%s' ", req.Zone)
	}
	if req.VolumeName != "" {
		sqlLine += fmt.Sprintf("AND volume_name = '%s' ", req.VolumeName)
	}
	if req.OperationType != "" {
		sqlLine += fmt.Sprintf("AND action = '%s' ", req.OperationType)
	}
	sqlLine += fmt.Sprintf(""+
		"GROUP BY "+
		"ip "+
		"ORDER BY  "+
		"%s DESC "+
		"LIMIT "+
		"%v",
		req.OrderBy, req.TopN)
	return f.queryTopN(sqlLine, "ip")
}
