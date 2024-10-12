package cfs

import (
	"testing"
	"time"

	"github.com/cubefs/cubefs/util/checktool/ump"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
)

func TestClientAlarm(t *testing.T) {
	m := NewChubaoFSMonitor(context.Background())
	m.umpClient = ump.NewUmpClient("", umpOpenAPiDomain)
	begin := time.Date(2024, 10, 14, 21, 12, 0, 0, time.Local).Unix()
	alarmCount := m.clientAlarmImpl(umpKeySparkClientPrefix, begin, begin+600)
	assert.NotZero(t, alarmCount)
	begin = time.Date(2024, 10, 15, 13, 17, 0, 0, time.Local).Unix()
	alarmCount = m.clientAlarmImpl(umpKeySparkClientPrefix, begin, begin+600)
	assert.Zero(t, alarmCount)
}
