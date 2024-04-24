package cfs

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestParseFailedDpInfo(t *testing.T) {
	content := "cluster[spark],vol[jdrtc_lf_rh_ssd],dp[1182752],replica[11.163.49.139:6000] load failed by datanode"
	record, err := ParseFailedDpInfo(content)
	if err != nil {
		t.Error(err.Error())
	}
	assert.Equal(t, "spark", record.Cluster, "parsed cluster name is invalid")
	assert.Equal(t, "jdrtc_lf_rh_ssd", record.VolName, "parsed cluster name is invalid")
	assert.Equal(t, 1182752, record.DpId, "parsed cluster name is invalid")
	assert.Equal(t, "11.163.49.139:6000", record.Replica, "parsed cluster name is invalid")
}

func TestParseFailedDpInfoExceptNil(t *testing.T) {
	content := "action[extractStatus],partitionID:1182475  replicaNum:3  liveReplicas:2   Status:1  RocksDBHost:[11.163.49.22:6000 11.163.49.36:6000 11.163.49.141:6000],liveHosts:[11.163.49.22:6000 11.163.49.36:6000] ，报警主机：11.3.133.9"
	record, err := ParseFailedDpInfo(content)
	if err != nil {
		t.Error(err.Error())
	}
	assert.Nil(t, record, "expected record is nil")
}