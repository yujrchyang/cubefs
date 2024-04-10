package rebalance

import (
	"fmt"
	"github.com/cubefs/cubefs/util/config"
	"testing"
)

var (
	rw *ReBalanceWorker
)
func init() {
	mysqlConfig := &config.MysqlConfig{
		Url : "11.13.125.198",
		Username: "root",
		Password: "1qaz@WSX",
		Database: "rebalance_dp_record",
		Port: 80,
	}
	rw = &ReBalanceWorker{}
	rw.MysqlConfig = mysqlConfig
	err := rw.OpenSql()
	if err != nil {
		panic(err)
	}
}

func TestInsertOrUpdateRebalancedInfo(t *testing.T) {
	clusterHost := "cn.chubaofs.jd.local"
	zoneName := "default"
	maxBatchCount := 50
	highRatio := 0.85
	lowRatio := 0.3
	goalRatio := 0.6
	migrateLimitPerDisk := 11
	status := StatusRunning
	rInfo, err := rw.insertOrUpdateRebalancedInfo(clusterHost, zoneName, RebalanceData, maxBatchCount, highRatio, lowRatio, goalRatio, migrateLimitPerDisk, defaultDstMetaNodePartitionMaxCount, status)
	if err != nil {
		t.Fatalf("err:%v", err)
	}
	fmt.Println(rInfo.ID)
}

func TestStopRebalanced(t *testing.T) {
	clusterHost := "cn.chubaofs.jd.local"
	zoneName := "default"
	err := rw.stopRebalanced(clusterHost, zoneName, RebalanceData)
	if err != nil {
		t.Fatalf("err:%v", err)
	}
}

func TestLoadInRunningRebalanced(t *testing.T) {
	rw.loadInRunningRebalanced()
}