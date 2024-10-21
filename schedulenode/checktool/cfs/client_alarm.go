package cfs

import (
	"fmt"
	"strings"
	"time"

	"github.com/cubefs/cubefs/util/checktool/mdc"
	"github.com/cubefs/cubefs/util/checktool/ump"
	"github.com/cubefs/cubefs/util/exporter"
)

const (
	clientAlarmInterval     = 60
	umpKeyMysqlClientPrefix = "mysql_kbpclient_"
	umpKeySparkClientPrefix = "spark_client_"
	umpKeyWarningSufix      = "warning"
	umpKeyFatalSufix        = "fatal"
)

func (s *ChubaoFSMonitor) clientAlarm() {
	now := time.Now().Unix()
	begin := now / clientAlarmInterval * clientAlarmInterval
	end := begin + int64(clientAlarmInterval)
	s.clientAlarmImpl(umpKeyMysqlClientPrefix, begin, end)
	s.clientAlarmImpl(umpKeySparkClientPrefix, begin, end)
}

func (s *ChubaoFSMonitor) clientAlarmImpl(prefix string, begin int64, end int64) (alarmCount int) {
	var (
		alarmRecords *ump.AlarmRecordResponse
		err          error
	)
	for i := 0; i < 10; i++ {
		alarmRecords, err = s.umpClient.GetAlarmRecords(alarmRecordsMethod, UmpAppName, UmpPlatForm, prefix+umpKeyWarningSufix, begin*1000, end*1000)
		if err == nil {
			break
		}
		time.Sleep(time.Second)
	}
	if err != nil || alarmRecords == nil {
		return
	}
	ignoreContents := []string{"no such file or directory", "/readProcess/register", "timeout", "NotExistErr", "network is unreachable"}
	for _, record := range alarmRecords.Records {
		var ignore bool
		for _, ignoreContent := range ignoreContents {
			if strings.Contains(record.Content, ignoreContent) {
				ignore = true
				break
			}
		}
		if ignore {
			continue
		}
		ipRes := strings.Split(record.Content, "报警主机")
		if len(ipRes) > 1 {
			ip := strings.Trim(ipRes[1], ":： ")
			abnormal := isIpPingAbnormal(ip, record.AlarmTime-300*1000, record.AlarmTime+60*1000)
			if abnormal {
				continue
			}
			// put host first to avoid being truncated
			record.Content = fmt.Sprintf("报警主机：%s，%s", ip, strings.Trim(ipRes[0], ",，"))
		}
		alarmCount++
		exporter.WarningBySpecialUMPKey(prefix+umpKeyFatalSufix, record.Content)
	}
	return
}

func isIpPingAbnormal(ip string, begin int64, end int64) bool {
	mdcApi := mdc.NewMDCOpenApiWithTimeout(mdc.MDCToken, []string{"min_ping"}, mdc.CnSiteType, 5*time.Second)
	conditions := make([]map[string]interface{}, 0)
	conditions = append(conditions, map[string]interface{}{"ip": ip})
	var (
		re  []*mdc.ResponseInfo
		err error
	)
	for i := 0; i < 10; i++ {
		re, err = mdcApi.Query(begin, end, conditions, 0)
		if err == nil {
			break
		}
	}
	if err != nil {
		return false
	}
	var isSeriesAbnormal = func(series []*mdc.PointNew) bool {
		for _, singleData := range series {
			if singleData != nil && singleData.Value == 1 {
				return true
			}
		}
		return false
	}
	for _, resp := range re {
		if resp == nil {
			continue
		}
		for _, metric := range resp.MetricResponseList {
			if metric != nil && isSeriesAbnormal(metric.Series) {
				return true
			}
		}
	}
	return false
}
