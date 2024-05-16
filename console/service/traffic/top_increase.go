package traffic

import (
	"fmt"
	"github.com/cubefs/cubefs/console/cutil"
	cproto "github.com/cubefs/cubefs/console/proto"
	"github.com/cubefs/cubefs/util/log"
	"time"
)

const (
	TopVol int32 = iota
	TopSource
)

type TopIncrease struct {
	ClusterName string  `gorm:"column:cluster"`
	VolumeName  string  `gorm:"column:volume"`
	ZoneName    string  `gorm:"column:zone"`
	CreateTime  string  `gorm:"column:create_time"`
	CapacityTb  float64 `gorm:"column:capacity_tb"`
	IncreaseTb  float64 `gorm:"column:increase_tb"`
	UsedTb      float64 `gorm:"column:used_tb"`
	Erp         string  `gorm:"column:pin"`
	Source      string  `gorm:"column:source"`
	Department  string  `gorm:"column:department"`
	UpdateTime  string  `gorm:"column:update_time"`
}

func (v *TopIncrease) String() string {
	return fmt.Sprintf("{%s %s %s used: %v inc: %v}\n", v.VolumeName, v.ZoneName, v.Source, v.UsedTb, v.IncreaseTb)
}

/*
start 0, end 0: 近1天，今天相较昨天
start 值, end 0: 从start 到现在
start 值, end 值：区间 变化
start 0, end 值：过去-现在 -(变化)
*/
func parseTopIncRequestTime(req *cproto.HistoryCurveRequest) (start, end time.Time) {
	defer func() {
		start = time.Date(start.Year(), start.Month(), start.Day(), start.Hour(), 0, 0, 0, time.Local)
		end = time.Date(end.Year(), end.Month(), end.Day(), end.Hour(), 0, 0, 0, time.Local)
	}()
	var (
		now = time.Now()
	)
	switch req.IntervalType {
	case cproto.ResourceLatestOneDay:
		start = now.AddDate(0, 0, -1)
		end = now

	case cproto.ResourceLatestOneWeek:
		start = now.AddDate(0, 0, -7)
		end = now

	case cproto.ResourceLatestOneMonth:
		start = now.AddDate(0, -1, 0)
		end = now

	default:
		if req.Start == 0 {
			start = now.AddDate(0, 0, -1)
		} else {
			start = time.Unix(req.Start, 0)
		}
		if req.End == 0 {
			end = now
		} else {
			end = time.Unix(req.End, 0)
		}
	}
	return
}

func GetTopIncreaseVol(req *cproto.HistoryCurveRequest, strictZone bool, orderBy int) []*TopIncrease {
	var (
		zoneFilter    string
		orderByFilter string
		topN          = 10
	)
	if strictZone {
		zoneFilter = fmt.Sprintf("zone = '%s'", req.ZoneName)
	} else {
		zoneFilter = fmt.Sprintf("zone like '%s'", "%"+req.ZoneName+"%")
	}

	if orderBy == 0 {
		orderByFilter = fmt.Sprintf("increase_tb DESC ")
	} else if orderBy == 1 {
		orderByFilter = fmt.Sprintf("increase_tb ASC ")
	}

	if cproto.IsRelease(req.Cluster) {
		// for db_back集群
		zoneFilter = fmt.Sprintf("1 = 1")
	}

	start1, end1 := parseTopIncRequestTime(req)
	start0 := start1.Add(-1 * time.Hour)
	end0 := end1.Add(-1 * time.Hour)

	startFilter := fmt.Sprintf("update_time >= '%s' and update_time < '%s'", start0.Format(time.DateTime), start1.Format(time.DateTime))
	endFilter := fmt.Sprintf("update_time >= '%s' and update_time < '%s'", end0.Format(time.DateTime), end1.Format(time.DateTime))
	if log.IsDebugEnabled() {
		log.LogDebugf("startFilter: %v, endFilter: %v", startFilter, endFilter)
	}
	timeFormatter := "DATE_FORMAT(b.update_time, '%Y-%m-%d %H:%i:%s')"
	sqlStr := fmt.Sprintf(""+
		"select "+
		"b.cluster, b.volume, b.zone, "+
		"b.create_time, (b.used_gb - coalesce(a.used_gb, 0))/1024 as increase_tb, b.used_gb/1024 as used_tb, "+
		"b.pin, b.source, b.department, %s as update_time "+
		"from "+
		"("+
		"select "+
		"volume, (used_gb*dp_replica_num) as used_gb "+
		"from console_volume_history_info "+
		"where "+
		"cluster = ? "+
		"and %s "+
		"and %s "+
		"group by "+
		"volume"+
		") a "+
		"right join "+
		"("+
		"select "+
		"cluster, volume, zone, create_time, (used_gb*dp_replica_num) as used_gb, pin, source, department, update_time "+
		"from console_volume_history_info "+
		"where "+
		"cluster = ? "+
		"and %s "+
		"and %s "+
		"group by "+
		"volume"+
		") b "+
		"on a.volume = b.volume "+
		"order by "+
		"%s "+
		"limit ?",
		timeFormatter, zoneFilter, startFilter, zoneFilter, endFilter, orderByFilter)
	if log.IsDebugEnabled() {
		log.LogDebugf("%s", sqlStr)
	}
	result := make([]*TopIncrease, 0)
	if err := cutil.CONSOLE_DB.Raw(sqlStr, req.Cluster, req.Cluster, topN).Scan(&result).Error; err != nil {
		log.LogErrorf("GetTopIncreaseVol failed: cluster(%v) zone(%v) err(%v)", req.Cluster, req.ZoneName, err)
		return nil
	}
	if log.IsDebugEnabled() {
		log.LogDebugf("GetTopIncreaseVol: \n%v", result)
	}
	return result
}

func GetTopIncreaseSource(req *cproto.HistoryCurveRequest, strictZone bool, orderBy int) []*TopIncrease {
	var (
		zoneFilter    string
		orderByFilter string
		topN          = 10
	)
	if strictZone {
		zoneFilter = fmt.Sprintf("zone = '%s'", req.ZoneName)
	} else {
		zoneFilter = fmt.Sprintf("zone like '%s'", "%"+req.ZoneName+"%")
	}

	if orderBy == 0 {
		orderByFilter = fmt.Sprintf("increase_tb DESC ")
	} else if orderBy == 1 {
		orderByFilter = fmt.Sprintf("increase_tb ASC ")
	}

	if cproto.IsRelease(req.Cluster) {
		// for db_back集群
		zoneFilter = fmt.Sprintf("1 = 1")
	}

	start1, end1 := parseTopIncRequestTime(req)
	start0 := start1.Add(-1 * time.Hour)
	end0 := end1.Add(-1 * time.Hour)

	startFilter := fmt.Sprintf("update_time >= '%s' and update_time < '%s'", start0.Format(time.DateTime), start1.Format(time.DateTime))
	endFilter := fmt.Sprintf("update_time >= '%s' and update_time < '%s'", end0.Format(time.DateTime), end1.Format(time.DateTime))
	timeFormatter := "DATE_FORMAT(b.update_time, '%Y-%m-%d %H:%i:%s')"
	sqlStr := fmt.Sprintf(""+
		"select "+
		"b.cluster, b.total/1024 as capacity_tb, "+
		"(b.used_gb - coalesce(a.used_gb, 0))/1024 as increase_tb, b.used_gb/1024 as used_tb, "+
		"b.source, %s as update_time "+
		"from "+
		"("+
		"select "+
		"source, sum(used_gb * dp_replica_num) as used_gb "+
		"from console_volume_history_info "+
		"where "+
		"cluster = ? "+
		"and %s "+
		"and %s "+
		"group by "+
		"source"+
		") a "+
		"right join "+
		"("+
		"select "+
		"cluster, sum(used_gb * dp_replica_num) as used_gb, sum(total_gb) as total, source, update_time "+
		"from console_volume_history_info "+
		"where "+
		"cluster = ? "+
		"and %s "+
		"and %s "+
		"group by "+
		"source"+
		") b "+
		"on a.source = b.source "+
		"order by "+
		"%s "+
		"limit ?",
		timeFormatter, zoneFilter, startFilter, zoneFilter, endFilter, orderByFilter)
	if log.IsDebugEnabled() {
		log.LogDebugf("%s", sqlStr)
	}
	result := make([]*TopIncrease, 0)
	if err := cutil.CONSOLE_DB.Raw(sqlStr, req.Cluster, req.Cluster, topN).Scan(&result).Error; err != nil {
		log.LogErrorf("GetTopIncreaseSource failed: cluster(%v) zone(%v) err(%v)", req.Cluster, req.ZoneName, err)
		return nil
	}
	if log.IsDebugEnabled() {
		log.LogDebugf("GetTopIncreaseSource: \n%v", result)
	}
	return result
}

func GetTopVolByZone(cluster, zone string, strict bool) []*TopIncrease {
	// 今天0点 到 明天0点
	topN := 10
	now := time.Now()
	oneDayAfter := time.Now().AddDate(0, 0, 1)
	start := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.Local)
	end := time.Date(oneDayAfter.Year(), oneDayAfter.Month(), oneDayAfter.Day(), 0, 0, 0, 0, time.Local)

	var zoneFilter string
	if strict {
		zoneFilter = fmt.Sprintf("zone_name = '%s'", zone)
	} else {
		zoneFilter = fmt.Sprintf("zone_name like '%s'", "%"+zone+"%")
	}
	if cproto.IsRelease(cluster) {
		zoneFilter = fmt.Sprintf("1 = 1 ")
	}

	sqlStr := fmt.Sprintf(""+
		"select "+
		"cluster, zone, volume, create_time, used_gb "+
		"from console_volume_history_info "+
		"where "+
		"cluster = ? "+
		"and %s"+
		"and update_time >= ? "+
		"and update_time < ? "+
		"order by "+
		"used_gb DESC "+
		"limit ?",
		zoneFilter)
	if log.IsDebugEnabled() {
		log.LogDebugf("%s", sqlStr)
	}
	result := make([]*TopIncrease, 0)
	if err := cutil.SRE_DB.Raw(sqlStr, cluster, start.Format(time.DateTime), end.Format(time.DateTime), topN).Scan(&result).Error; err != nil {
		log.LogErrorf("GetTopVolByZone failed: cluster(%v) zone(%v) err(%v)", cluster, zone, err)
		return nil
	}
	if log.IsDebugEnabled() {
		log.LogDebugf("GetTopVolByZone: %v", result)
	}
	return result
}
