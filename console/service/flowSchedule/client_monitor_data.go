package flowSchedule

import (
	"fmt"
	"github.com/cubefs/cubefs/console/proto"
)

const (
	clientTableName = "ck_chubaofs_client_monitor_data_min_test_view_dist"
)

// action zone count size time， ratio(该zone占总的比例, 拉出来自己算)

// todo: 展示每分钟的count size, 如果指定了 源ip的zone， 可以加个跨机房比例
func (f *FlowSchedule) ListClientMonitorData(req *proto.TrafficRequest) (results [][]*proto.FlowScheduleResult, err error) {
	if req.ClusterName == "" || req.VolumeName == "" || req.IpAddr == "" || req.OperationType == "" {
		return nil, fmt.Errorf("require params: cluster[%s] volume[%s] ip[%s] action[%s]", req.ClusterName, req.VolumeName, req.IpAddr, req.OperationType)
	}
	var (
		sqlLine string
	)
	if _, err = getStartAndEndFromRequest(req, false); err != nil {
		return
	}
	sqlLine = fmt.Sprintf(""+
		"SELECT "+
		"event_date, "+
		"action, "+
		"zone, "+
		"sum(total_size) as size, "+
		"sum(total_count) as count "+
		"FROM "+
		"chubaofs_monitor_data_storage.%s "+
		"WHERE "+
		"event_date >= '%s' AND event_date <= '%s' "+
		"AND cluster_name = '%s' "+
		"AND module = 'client' "+
		"AND volume_name = '%s' "+
		"AND ip = '%s' "+
		"AND action = '%s' ",
		clientTableName, parseTimestampToDataTime(req.StartTime), parseTimestampToDataTime(req.EndTime), req.ClusterName,
		req.VolumeName, req.IpAddr, req.OperationType,
	)
	if req.Zone != "" {
		sqlLine += fmt.Sprintf("AND zone = '%s' ", req.Zone)
	} else {
		topActionSqlLine := fmt.Sprintf(""+
			"SELECT "+
			"zone "+
			"FROM "+
			"chubaofs_monitor_data_storage.%s "+
			"WHERE "+
			"event_date >= '%s' AND event_date <= '%s' "+
			"AND cluster_name = '%s' "+
			"AND module = 'client' "+
			"AND volume_name = '%s' "+
			"AND action = '%s' "+
			"GROUP BY "+
			"zone ",
			clientTableName, parseTimestampToDataTime(req.StartTime), parseTimestampToDataTime(req.EndTime),
			req.ClusterName, req.VolumeName, req.OperationType)
		sqlLine += fmt.Sprintf("AND zone global in (%s) ", topActionSqlLine)
	}
	sqlLine += fmt.Sprintf("" +
		"GROUP BY " +
		"event_date, cluster_name, volume_name, module, action, zone " +
		"ORDER BY " +
		"action, zone, event_date")
	return f.queryDetail(sqlLine, proto.ClientZoneLineDimension)
}
