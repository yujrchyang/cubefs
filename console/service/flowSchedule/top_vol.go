package flowSchedule

import (
	"fmt"
	"github.com/cubefs/cubefs/console/cutil"
	cproto "github.com/cubefs/cubefs/console/proto"
	"github.com/cubefs/cubefs/util/log"
	"strings"
)

type GetTopologyFunc func(cluster, module string) *cproto.CFSTopology

func (f *FlowSchedule) ListTopVolClickHouse(req *cproto.TrafficRequest) (result []*cproto.FlowScheduleResult, err error) {
	var (
		sqlLine string
	)
	parseOrderAndTopN(req, queryTopN)
	if _, err = getStartAndEndFromRequest(req, true); err != nil {
		return
	}
	// 只能查原表 聚合表没有筛选条件
	table := getOriginalTable(cutil.GlobalCluster == "spark")
	sqlLine = fmt.Sprintf(""+
		"SELECT "+
		"volume_name, "+
		"sum(count) as total_count, "+
		"sum(size) as total_size, "+
		"CEIL(sum(size) / sum(count)) as avg_size "+
		"FROM "+
		"chubaofs_monitor_data_storage.%s "+
		"WHERE "+
		"event_date >= '%s' AND event_date <= '%s' "+
		"AND cluster_name = '%s' "+
		"AND module = '%s' ",
		table, parseTimestampToDataTime(req.StartTime), parseTimestampToDataTime(req.EndTime), req.ClusterName, req.Module)

	if req.OperationType != "" {
		sqlLine += fmt.Sprintf("AND action = '%s' ", req.OperationType)
	}
	if req.IpAddr != "" {
		sqlLine += fmt.Sprintf("AND ip = '%s' ", req.IpAddr)
	}
	if req.Zone != "" {
		sqlLine += fmt.Sprintf("AND zone = '%s' ", req.Zone)
	}
	if req.Disk != "" {
		sqlLine += fmt.Sprintf("AND disk_path = '%s' ", req.Disk)
	}
	sqlLine += fmt.Sprintf(""+
		"GROUP BY "+
		"volume_name "+
		"ORDER BY "+
		"%s DESC "+
		"LIMIT %v",
		req.OrderBy, req.TopN)
	return f.queryTopN(sqlLine, "vol")
}

func getIpListByZone(cluster, module, zone string, topo GetTopologyFunc) []string {
	view := topo(cluster, module)
	if view == nil {
		log.LogErrorf("getIpListByZone: topo view is nil")
		return nil
	}
	var zoneView *cproto.ZoneView
	if module == "datanode" {
		zoneView = view.DataMap[zone]
	} else if module == "metanode" {
		zoneView = view.MetaMap[zone]
	} else if module == "flashnode" {
		zoneView = view.FlashMap[zone]
	}
	if zoneView != nil {
		return zoneView.IPs
	}
	return nil
}

func convertListToStrForSql(ips []string) string {
	if len(ips) == 0 {
		return ""
	}
	var builder strings.Builder
	delimiter := "'"
	comma := ","
	// '***.***.***.***',
	builder.Grow(20 * len(ips))
	for _, fullIP := range ips {
		ip := strings.Split(fullIP, ":")[0]
		builder.WriteString(delimiter)
		builder.WriteString(ip)
		builder.WriteString(delimiter)
		builder.WriteString(comma)
	}
	return builder.String()
}
