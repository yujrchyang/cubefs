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
		Url:      "11.13.125.198",
		Username: "root",
		Password: "1qaz@WSX",
		//Database: "rebalance_dp_record",
		Database: "rebalance_dp_test",
		Port:     80,
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
	rInfo, err := rw.insertRebalanceInfo(clusterHost, zoneName, RebalanceData, maxBatchCount, highRatio, lowRatio, goalRatio, migrateLimitPerDisk, defaultDstMetaNodePartitionMaxCount, status)
	if err != nil {
		t.Fatalf("err:%v", err)
	}
	fmt.Println(rInfo.ID)
}

func TestLoadInRunningRebalanced(t *testing.T) {
	rw.loadInRunningRebalanced()
}

func Test_GetMigrateRecordsByCond(t *testing.T) {
	cond := make(map[string]interface{})
	cond["cluster_name"] = "sparkchubaofs.jd.local"
	cond["zone_name"] = "default"
	cond["vol_name"] = "datahubcfs"
	cond["rebalance_type"] = 0
	//cond["partition_id"] = 63535
	total, records, err := rw.GetMigrateRecordsByCond(cond, "11.5.115.8", "", "2023-08-11 00:00:00", 1, 3)
	if err != nil {
		t.Fatalf("query err: %v", err)
	}
	fmt.Println(fmt.Sprintf("total: %v, len(records): %v", total, len(records)))
}

func Test_fillClusterForAllRecords(t *testing.T) {
	host := "11.60.241.50:17010"
	err := rw.dbHandle.Table(RebalancedInfoTable{}.TableName()).Where("host = ?", host).
		Update("cluster", "chubaofs01").Error
	if err != nil {
		t.Fatal(err)
	}
	err = rw.dbHandle.Table(MigrateRecordTable{}.TableName()).Where("cluster_name = ?", host).
		Update("cluster_name", "chubaofs01").Error
	if err != nil {
		t.Fatal(err)
	}
}
