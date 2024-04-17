package cfs

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRegexDp(t *testing.T) {
	content := "警告，应用：chubaofs-node，自定义监控：spark_master_dp_has_not_recover，action[checkDiskRecoveryProgress] clusterID[spark],has[11] has offlined more than 24 hours,still not recovered,ids[map[1.1.11.1:6000#/data0#101184:101184 1.1.11.1:6000#/data3#102234:102234 1.1.11.1:6000#/data5#105702:105702 1.1.11.1:6000#/data5#277110:277110 1.1.11.1:6000#/data1#702659:702659 1.1.11.1:6000#/data0#336086:336086 1.1.11.1:6000#/data1#1291575:1291575 1.1.11.1:6000#/data5#332012:332012 1.1.11.1:6000#/data4#124619:124619 1.1.11.1:6000#/data8#273407:273407 1.1.11.1，报警主机：1.1.11.1"
	dps := parseNotRecoverPartition(content)
	t.Logf("data partition ids: %v", dps)
	assert.Greater(t, len(dps), 1)
}
