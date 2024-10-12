package cfs

import (
	"strings"
	"time"

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
	if err != nil {
		return
	}
	ignoreContents := []string{"no such file or directory", "/readProcess/register", "timeout", "NotExistErr"}
	for _, record := range alarmRecords.Records {
		var ignore bool
		for _, ignoreContent := range ignoreContents {
			if strings.Contains(record.Content, ignoreContent) {
				ignore = true
				break
			}
		}
		if !ignore {
			alarmCount++
			exporter.WarningBySpecialUMPKey(prefix+umpKeyFatalSufix, record.Content)
		}
	}
	return
}
