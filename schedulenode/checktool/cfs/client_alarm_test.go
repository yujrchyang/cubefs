package cfs

import (
	"testing"
	"time"

	"github.com/cubefs/cubefs/util/checktool/ump"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func TestClientAlarm(t *testing.T) {
	m := NewChubaoFSMonitor(context.Background())
	m.umpClient = ump.NewUmpClient("", umpOpenAPiDomain)
	m.envConfig = new(EnvConfig)
	m.envConfig.JMQ = &JMQConfig{Addr: "", Topic: "", Group: ""}
	m.sreDB, _ = gorm.Open(mysql.New(mysql.Config{DSN: ""}), &gorm.Config{})
	begin := time.Date(2024, 10, 14, 21, 12, 0, 0, time.Local)
	alarmCount := m.clientAlarmImpl(clusterSpark, begin, begin.Add(10*time.Minute))
	assert.NotZero(t, alarmCount)
	begin = time.Date(2024, 10, 15, 13, 17, 0, 0, time.Local)
	alarmCount = m.clientAlarmImpl(clusterSpark, begin, begin.Add(10*time.Minute))
	assert.Zero(t, alarmCount)

	// network of ip is not available
	begin = time.Date(2024, 10, 21, 07, 51, 0, 0, time.Local)
	alarmCount = m.clientAlarmImpl(clusterSpark, begin, begin.Add(10*time.Minute))
	assert.Zero(t, alarmCount)
}

func TestParseClientWarning(t *testing.T) {
	content := "volume(rd-finance-m-prd-ht87a-xx-20) act(cfs_pwrite_inode) - id(1) ino(33554770) size(131072) offset(2097152) re(-5) err(no such file or directory)，报警主机：11.145.226.59"
	vol, ip := parseClientWarning(content)
	assert.True(t, vol == "rd-finance-m-prd-ht87a-xx-20" && ip == "11.145.226.59")
}
