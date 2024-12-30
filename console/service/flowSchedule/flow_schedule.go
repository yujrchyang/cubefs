package flowSchedule

import (
	"bufio"
	"fmt"
	"github.com/cubefs/cubefs/console/cutil"
	cproto "github.com/cubefs/cubefs/console/proto"
	"github.com/cubefs/cubefs/util/log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	DefaultTopN           = 10
	defaultDetailOrderBy  = "total_count"
	defaultLatencyOrderBy = "max"

	originalTable = "ck_chubaofs_monitor_data_table_v2_dist"
	// 非聚合表，集群过滤表
	spark_datanode_latency_table = "chubaofs_spark_datanode_latency_table_view_v2_dist"
	spark_non_data_latency_table = "chubaofs_spark_latency_table_view_v2_dist"
	mysql_latency_table          = "chubaofs_mysql_latency_table_view_v2_dist"
	dbBak_latency_table          = "chubaofs_cfs_dbBack_latency_table_view_v2_dist"

	tenMinutesInSecond = 10 * 60
	oneHourInSecond    = 60 * 60
	OneDayInSecond     = 24 * 60 * 60
)

type queryType int

const (
	queryDetail queryType = iota
	queryLatency
	queryTopN
)

type FlowSchedule struct {
	clickHouseDBUser     string
	clickHouseDBPassword string
	clickHouseDBHost     string
}

func NewFlowSchedule(user, password, host string) *FlowSchedule {
	schedule := new(FlowSchedule)

	schedule.clickHouseDBUser = user
	schedule.clickHouseDBHost = host
	schedule.clickHouseDBPassword = password
	return schedule
}

// 根据环境获取表名
func getOriginalTable(online bool) string {
	if online {
		return originalTable
	} else {
		return "ck_chubaofs_monitor_data_table_test_dist"
	}
}

func (f *FlowSchedule) queryTopN(sqlLine, topType string) (results []*cproto.FlowScheduleResult, err error) {
	if log.IsDebugEnabled() {
		log.LogDebugf("queryTopN: %s", sqlLine)
	}
	url := fmt.Sprintf("http://%s:%s@%s/?query=%s", f.clickHouseDBUser, f.clickHouseDBPassword,
		f.clickHouseDBHost, url.QueryEscape(sqlLine))
	if log.IsDebugEnabled() {
		log.LogDebugf("queryTopN: %s", url)
	}
	resp, err := http.Get(url)
	if err != nil {
		log.LogErrorf("send query request to ck failed, err: %v", err)
		return
	}
	defer resp.Body.Close()
	// 解析ck返回值
	results = make([]*cproto.FlowScheduleResult, 0)
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		// 以制表符cut line
		fields := strings.Split(line, "\t")
		// ip count size avg, 4个
		if len(fields) != 4 && len(fields) != 5 {
			log.LogErrorf("queryTopN: wrong result format")
			return
		}
		entry := new(cproto.FlowScheduleResult)
		index := 0
		switch topType {
		case "ip":
			entry.IpAddr = fields[index]
		case "vol":
			entry.VolumeName = fields[index]
		case "pid":
			entry.PartitionID, _ = strconv.ParseUint(fields[index], 10, 64)
			index++
			entry.VolumeName = fields[index]
		}
		entry.Count, _ = strconv.ParseUint(fields[index+1], 10, 64)
		entry.Size, _ = strconv.ParseUint(fields[index+2], 10, 64)
		entry.AvgSize, _ = strconv.ParseUint(fields[index+3], 10, 64)
		results = append(results, entry)
	}
	return
}

func (f *FlowSchedule) queryDetail(sqlLine string, drawDimension int32) (results [][]*cproto.FlowScheduleResult, err error) {
	if log.IsDebugEnabled() {
		log.LogDebugf("queryDetail: %s", sqlLine)
	}
	url := fmt.Sprintf("http://%s:%s@%s/?query=%s", f.clickHouseDBUser, f.clickHouseDBPassword,
		f.clickHouseDBHost, url.QueryEscape(sqlLine))
	if log.IsDebugEnabled() {
		log.LogDebugf("queryDetail: %s", url)
	}
	resp, err := http.Get(url)
	if err != nil {
		log.LogErrorf("send query request to ck failed, err: %v", err)
		return
	}
	defer resp.Body.Close()

	queryResult := make(map[string][]*cproto.FlowScheduleResult, 0)
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, "\t")
		// action time count size avg_size
		if len(fields) != 5 {
			log.LogErrorf("queryDetail: wrong result format")
			return
		}
		entry := new(cproto.FlowScheduleResult)
		var time_ time.Time
		time_, err = time.Parse("2006-01-02 15:04:05", fields[0])
		if err != nil {
			return
		}
		if drawDimension == cproto.DiskDrawLineDimension {
			entry.Disk = fields[1]
		} else {
			entry.OperationType = fields[1]
		}
		entry.Time = time_.Format("2006/01/02 15:04:05")
		if drawDimension == cproto.ClientZoneLineDimension {
			entry.Zone = fields[2]
			entry.Size, _ = strconv.ParseUint(fields[3], 10, 64)
			entry.Count, _ = strconv.ParseUint(fields[4], 10, 64)
		} else {
			entry.Count, _ = strconv.ParseUint(fields[2], 10, 64)
			entry.Size, _ = strconv.ParseUint(fields[3], 10, 64)
			entry.AvgSize, _ = strconv.ParseUint(fields[4], 10, 64)
		}

		if drawDimension == cproto.ActionDrawLineDimension {
			if _, ok := queryResult[entry.OperationType]; !ok {
				queryResult[entry.OperationType] = make([]*cproto.FlowScheduleResult, 0)
			}
			queryResult[entry.OperationType] = append(queryResult[entry.OperationType], entry)
		} else if drawDimension == cproto.DiskDrawLineDimension {
			if _, ok := queryResult[entry.Disk]; !ok {
				queryResult[entry.Disk] = make([]*cproto.FlowScheduleResult, 0)
			}
			queryResult[entry.Disk] = append(queryResult[entry.Disk], entry)
		} else if drawDimension == cproto.ClientZoneLineDimension {
			if _, ok := queryResult[entry.Zone]; !ok {
				queryResult[entry.Zone] = make([]*cproto.FlowScheduleResult, 0)
			}
			queryResult[entry.Zone] = append(queryResult[entry.Zone], entry)
		}
	}
	results = make([][]*cproto.FlowScheduleResult, 0, len(queryResult))
	for _, resultList := range queryResult {
		results = append(results, resultList)
	}
	return
}

func (f *FlowSchedule) queryLatency(sqlLine string) (results []*cproto.FlowScheduleResult, err error) {
	if log.IsDebugEnabled() {
		log.LogDebugf("queryLatency: %s", sqlLine)
	}
	url := fmt.Sprintf("http://%s:%s@%s/?query=%s", f.clickHouseDBUser, f.clickHouseDBPassword,
		f.clickHouseDBHost, url.QueryEscape(sqlLine))
	if log.IsDebugEnabled() {
		log.LogDebugf("queryLatency: %s", url)
	}
	resp, err := http.Get(url)
	if err != nil {
		log.LogErrorf("send query request to ck failed, err: %v", err)
		return
	}
	defer resp.Body.Close()

	results = make([]*cproto.FlowScheduleResult, 0)
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, "\t")
		if len(fields) != 5 {
			log.LogErrorf("queryLatency: wrong result format")
			return
		}
		entry := new(cproto.FlowScheduleResult)
		var time_ time.Time
		time_, err = time.Parse("2006-01-02 15:04:05", fields[0])
		if err != nil {
			return
		}
		entry.Time = time_.Format("2006/01/02 15:04:05")
		entry.Max, _ = strconv.ParseUint(fields[2], 10, 64)
		entry.Avg, _ = strconv.ParseUint(fields[3], 10, 64)
		entry.Tp99, _ = strconv.ParseUint(fields[4], 10, 64)
		results = append(results, entry)
	}
	return
}

func (f *FlowSchedule) queryActionsLatency(sqlLine string) (results [][]*cproto.FlowScheduleResult, err error) {
	url := fmt.Sprintf("http://%s:%s@%s/?query=%s", f.clickHouseDBUser, f.clickHouseDBPassword,
		f.clickHouseDBHost, url.QueryEscape(sqlLine))
	if log.IsDebugEnabled() {
		log.LogDebugf("queryActionsLatency: %s", url)
	}
	resp, err := http.Get(url)
	if err != nil {
		log.LogErrorf("send query request to ck failed, err: %v", err)
		return
	}
	defer resp.Body.Close()

	queryResult := make(map[string][]*cproto.FlowScheduleResult, 0)
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, "\t")
		if len(fields) != 7 {
			log.LogErrorf("queryActionsLatency: wrong result format")
			return
		}
		entry := new(cproto.FlowScheduleResult)
		var time_ time.Time
		time_, err = time.Parse("2006-01-02 15:04:05", fields[0])
		if err != nil {
			return
		}
		entry.Time = time_.Format("2006/01/02 15:04:05")
		entry.OperationType = fields[1]
		entry.Count, _ = strconv.ParseUint(fields[2], 10, 64)
		entry.Size, _ = strconv.ParseUint(fields[3], 10, 64)
		entry.Max, _ = strconv.ParseUint(fields[4], 10, 64)
		entry.Tp99, _ = strconv.ParseUint(fields[5], 10, 64)
		entry.Avg, _ = strconv.ParseUint(fields[6], 10, 64)
		if _, ok := queryResult[entry.OperationType]; !ok {
			queryResult[entry.OperationType] = make([]*cproto.FlowScheduleResult, 0)
		}
		queryResult[entry.OperationType] = append(queryResult[entry.OperationType], entry)
	}
	results = make([][]*cproto.FlowScheduleResult, 0, len(queryResult))
	for _, resultList := range queryResult {
		results = append(results, resultList)
	}
	return
}

func (f *FlowSchedule) queryLatencyDetail(sqlLine string) (results []*cproto.FlowScheduleResult, err error) {
	url := fmt.Sprintf("http://%s:%s@%s/?query=%s", f.clickHouseDBUser, f.clickHouseDBPassword,
		f.clickHouseDBHost, url.QueryEscape(sqlLine))
	if log.IsDebugEnabled() {
		log.LogDebugf("d: %s", url)
	}
	resp, err := http.Get(url)
	if err != nil {
		log.LogErrorf("send query request to ck failed, err: %v", err)
		return
	}
	defer resp.Body.Close()

	results = make([]*cproto.FlowScheduleResult, 0)
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, "\t")
		if len(fields) != 10 {
			log.LogErrorf("queryLatency: wrong result format")
			return
		}
		entry := new(cproto.FlowScheduleResult)
		entry.VolumeName = fields[0]
		entry.IpAddr = fields[1]
		entry.Zone = fields[2]
		entry.PartitionID, _ = strconv.ParseUint(fields[3], 10, 64)
		entry.Disk = fields[4]
		entry.Count, _ = strconv.ParseUint(fields[5], 10, 64)
		entry.Size, _ = strconv.ParseUint(fields[6], 10, 64)
		entry.Max, _ = strconv.ParseUint(fields[7], 10, 64)
		entry.Avg, _ = strconv.ParseUint(fields[8], 10, 64)
		entry.Tp99, _ = strconv.ParseUint(fields[9], 10, 64)
		results = append(results, entry)
	}
	return
}

// 10min 1Hour 1Day
// 不需要返回time类型 返回秒级时间戳 后面转化
// exactTime - 是否查精确的时间点(详情列表)
func getStartAndEndFromRequest(req *cproto.TrafficRequest, exactTime bool) (level cproto.DataGranularity, err error) {
	var (
		end   = time.Now().Unix()
		start int64
	)
	switch req.IntervalType {
	case cproto.IntervalTypeNull:
		if req.StartTime == 0 || req.EndTime == 0 {
			return 0, fmt.Errorf("请选择起始时间！")
		}
		if req.StartTime == req.EndTime {
			if !exactTime {
				req.EndTime = req.StartTime + 59
			} else {
				// 精确时间点 查top，需要判断曲线点粒度
				// bug：整点秒级的详情，会被视为分钟，从而返回聚合结果
				if req.StartTime%60 == 0 {
					req.EndTime = req.StartTime + 59
				}
			}
			return cproto.SecondGranularity, nil
		}
		span := req.EndTime - req.StartTime
		if span < oneHourInSecond {
			return cproto.SecondGranularity, nil
		} else if span <= OneDayInSecond {
			return cproto.MinuteGranularity, nil
		} else if span <= 60*60*24*2 {
			return cproto.TenMinuteGranularity, nil
		} else {
			return 0, fmt.Errorf("时间跨度最大为2天")
		}

	case cproto.IntervalTypeLatestTenMin:
		start = end - tenMinutesInSecond
		level = cproto.SecondGranularity

	case cproto.IntervalTypeLatestOneHour:
		start = end - oneHourInSecond
		level = cproto.MinuteGranularity

	case cproto.IntervalTypeLatestOneDay:
		start = end - OneDayInSecond
		level = cproto.MinuteGranularity

	default:
		err = fmt.Errorf("undefined interval type")
		return
	}
	req.StartTime = start
	req.EndTime = end
	return
}

func getStartAndEndFromRequestZoneLatency(req *cproto.TrafficRequest) (level cproto.DataGranularity, err error) {
	var (
		end   = time.Now().Unix()
		start int64
	)
	switch req.IntervalType {
	case cproto.IntervalTypeNull:
		if req.StartTime == 0 || req.EndTime == 0 {
			return 0, fmt.Errorf("请选择起始时间！")
		}
		if req.StartTime == req.EndTime {
			req.EndTime = req.StartTime + 60
			return cproto.SecondGranularity, nil
		}
		span := req.EndTime - req.StartTime
		if span < oneHourInSecond {
			return cproto.SecondGranularity, nil
		} else if span <= OneDayInSecond {
			return cproto.MinuteGranularity, nil
		} else if span <= 60*60*24 {
			return cproto.TenMinuteGranularity, nil
		} else {
			return 0, fmt.Errorf("时间跨度最大为1天")
		}

	case cproto.IntervalTypeLatestTenMin:
		start = end - tenMinutesInSecond
		level = cproto.SecondGranularity

	case cproto.IntervalTypeLatestOneHour:
		start = end - oneHourInSecond
		level = cproto.MinuteGranularity

	case cproto.IntervalTypeLatestOneDay:
		// 近3小时
		start = end - oneHourInSecond*3
		level = cproto.MinuteGranularity

	default:
		err = fmt.Errorf("undefined interval type")
		return
	}
	req.StartTime = start
	req.EndTime = end
	return
}

func getSelectDateFormat(level cproto.DataGranularity) (timeFilter string) {
	timeFilter = "event_date"
	switch level {
	case cproto.SecondGranularity:

	case cproto.MinuteGranularity:
		timeFilter = "toStartOfMinute(event_date) AS event_date"

	case cproto.TenMinuteGranularity:
		timeFilter = "toStartOfInterval(event_date, INTERVAL 10 MINUTE) AS event_date"

	default:
	}
	return timeFilter
}

func getTableLevelFromDataGranularity(level cproto.DataGranularity) string {
	switch level {
	case cproto.SecondGranularity:
		return "second"
	case cproto.MinuteGranularity:
		return "min"
	case cproto.TenMinuteGranularity:
		return "min"
	}
	return "min"
}

func getLatencyQueryTable(cluster, module string) (tableName string) {
	switch cluster {
	case "spark":
		if module == strings.ToLower(cproto.ModuleDataNode) {
			return spark_datanode_latency_table
		}
		return spark_non_data_latency_table

	case "mysql":
		return mysql_latency_table

	case "cfs_dbBack":
		return dbBak_latency_table
	}
	return getOriginalTable(cutil.GlobalCluster == "spark")
}

func parseOrderAndTopN(req *cproto.TrafficRequest, qt queryType) {
	if req.TopN == 0 {
		req.TopN = DefaultTopN
	}
	switch qt {
	case queryDetail:
		if req.OrderBy == "" {
			if cutil.GlobalCluster == "spark" {
				req.OrderBy = defaultDetailOrderBy
			} else {
				req.OrderBy = "count"
			}

		}
	case queryTopN:
		req.OrderBy = defaultDetailOrderBy

	case queryLatency:
		if req.OrderBy == "" {
			req.OrderBy = defaultLatencyOrderBy
		}
	}
	return
}

func parseTimestampToDataTime(ts int64) string {
	t := time.Unix(ts, 0)
	return t.Format(time.DateTime)
}

func parseDateTimeToTimestamp(dataTime string) int64 {
	t, err := time.Parse(time.DateTime, dataTime)
	if err != nil {
		return 0
	}
	return t.Unix()
}
