package cfs

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/cubefs/cubefs/util/checktool/mdc"
	"github.com/cubefs/cubefs/util/checktool/ump"
	"github.com/cubefs/cubefs/util/log"
)

const (
	clientAlarmInterval     = 60 * time.Second
	clusterMysql            = "mysql"
	clusterSpark            = "spark"

	cfgPushJMQInterval = "pushJMQInterval"
)

type JMQConfig struct {
	Addr  string `json:"addr"`
	Topic string `json:"topic"`
	Group string `json:"group"`
}

type clientJMQMsg struct {
	Cluster string `json:"cluster"`
	Volume  string `json:"volume"`
	Ip      string `json:"ip"`
	Error   string `json:"error"`
}

func (s *ChubaoFSMonitor) clientAlarm() {
	// only execute on one node
	if s.envConfig.JMQ == nil || s.envConfig.JMQ.Addr == "" {
		log.LogInfof("client alarm does not config jmq config...")
		return
	}
	now := time.Now()
	begin := now.Add(-clientAlarmInterval - 5*time.Second)
	end := now
	s.clientAlarmImpl(clusterMysql, begin, end)
	s.clientAlarmImpl(clusterSpark, begin, end)
}

func (s *ChubaoFSMonitor) clientAlarmImpl(cluster string, begin time.Time, end time.Time) (alarmCount int) {
	var (
		clientUmpPrefix string
		alarmRecords    *ump.AlarmRecordResponse
		err             error
	)
	if cluster == clusterMysql {
		clientUmpPrefix = umpKeyMysqlClientPrefix
	} else if cluster == clusterSpark {
		clientUmpPrefix = umpKeySparkClientPrefix
	}
	for i := 0; i < 10; i++ {
		alarmRecords, err = s.umpClient.GetAlarmRecords(alarmRecordsMethod, UmpAppName, UmpPlatForm, clientUmpPrefix+umpKeyWarningSufix, begin.UnixMilli(), end.UnixMilli())
		if err == nil {
			break
		}
		time.Sleep(time.Second)
	}
	if err != nil || alarmRecords == nil {
		log.LogErrorf("clientAlarm: GetAlarmRecords failed")
		return
	}
	ignoreContents := []string{"no such file or directory", "/readProcess/register", "timeout", "NotExistErr", "network is unreachable", "not exists"}
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
		vol, ip := parseClientWarning(record.Content)
		abnormal := isIpPingAbnormal(ip, record.AlarmTime-300*1000, record.AlarmTime+60*1000)
		if abnormal {
			log.LogInfof("ping abnormal vol(%v) ip(%v)", vol, ip)
			continue
		}
		ipRes := strings.Split(record.Content, "报警主机")
		if len(ipRes) > 1 {
			// put host first to avoid being truncated
			record.Content = fmt.Sprintf("报警主机：%s，%s", ip, strings.Trim(ipRes[0], ",，"))
		}
		alarmCount++
		warnBySpecialUmpKeyWithPrefix(clientUmpPrefix+umpKeyFatalSufix, record.Content)
		log.LogInfof("clientAlarm: ump(%v) content(%v)", clientUmpPrefix+umpKeyFatalSufix, record.Content)
		insertRes := s.insertClientMysqlMsg(cluster, vol, ip, record.Content, end.Add(-time.Second*time.Duration(s.pushJMQInterval)), end)
		if insertRes {
			msg, err := json.Marshal(clientJMQMsg{Cluster: cluster, Volume: vol, Ip: ip, Error: record.Content})
			if err == nil {
				sendClientMQMsg(s.envConfig.JMQ.Addr, s.envConfig.JMQ.Topic, s.envConfig.JMQ.Group, string(msg))
			} else {
				log.LogErrorf("clientAlarm: marshal failed, err(%v)", err)
			}
		}
	}
	return
}

func parseClientWarning(content string) (vol string, ip string) {
	volRegex := regexp.MustCompile(`volume\([^()]*\)`)
	re := volRegex.FindStringSubmatch(content)
	if len(re) > 0 {
		volSlice := strings.FieldsFunc(re[0], func(r rune) bool { return r == '(' || r == ')' })
		if len(volSlice) > 1 {
			vol = volSlice[1]
		}
	}

	ipRegex := regexp.MustCompile(`报警主机[:：][0-9.]*`)
	re = ipRegex.FindStringSubmatch(content)
	if len(re) > 0 {
		ipSlice := strings.FieldsFunc(re[0], func(r rune) bool { return r == ':' || r == '：' })
		if len(ipSlice) > 1 {
			ip = ipSlice[1]
		}
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

func (s *ChubaoFSMonitor) insertClientMysqlMsg(cluster string, volume string, ip string, content string, begin time.Time, end time.Time) bool {
	var count int64
	msg := fmt.Sprintf("cluster(%v) volume(%v) ip(%v) content(%v) begin(%v) end(%v)", cluster, volume, ip, content, begin, end)
	s.sreDB.Table("client_fatal").Where("cluster = ? and volume = ? and ip = ? and create_time between ? and ?", cluster, volume, ip, begin, end).Count(&count)
	if count > 0 {
		log.LogInfof("clientAlarm: no need to insert mysql, %v", msg)
		return false
	}
	result := s.sreDB.Exec("insert into client_fatal (cluster,volume,ip,content) values (?,?,?,?)", cluster, volume, ip, content)
	if result.Error != nil {
		log.LogErrorf("clientAlarm: insert mysql failed, %v err(%v)", msg, result.Error)
		return false
	} else {
		log.LogInfof("clientAlarm: insert mysql, %v", msg)
	}
	return true
}

func sendClientMQMsg(address string, topic string, clientId string, msgVal string) {
	msg := fmt.Sprintf("address(%v) topic(%v) clientId(%v) msgVal(%v)", address, topic, clientId, msgVal)
	if address == "" || topic == "" || clientId == "" {
		log.LogErrorf("clientAlarm: no jmq config, %v", msg)
		return
	}
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.ClientID = clientId
	producer, err := sarama.NewSyncProducer([]string{address}, config)
	defer func() {
		if producer != nil {
			producer.Close()
		}
	}()
	if err != nil {
		log.LogErrorf("clientAlarm: newMQProducer failed, err(%v)", err)
		return
	}

	partition, offset, err := producer.SendMessage(&sarama.ProducerMessage{Topic: topic, Value: sarama.StringEncoder(msgVal)})
	if err != nil {
		log.LogErrorf("clientAlarm: SendMessage failed, %v err(%v)", msg, err)
		return
	} else {
		log.LogInfof("clientAlarm: SendMessage, %v partition(%v) offset(%v)", msg, partition, offset)
	}
	return
}
