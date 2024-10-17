package rebalance

import (
	"fmt"
	"github.com/cubefs/cubefs/util/config"
	"github.com/stretchr/testify/assert"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"net/http"
	"testing"
	"time"
)

var (
	cluster   = "sparkchubaofs.jd.local"
	zoneName  = "rh_hbase_ssd"
	highRatio = 0.7
	lowRatio  = 0.5
	goalRatio = 0.6
)

func initTestDB() (*gorm.DB, error) {
	cfg := &config.MysqlConfig{
		Database: "rebalance_dp_record",
		Url:      "11.13.125.198",
		Username: "root",
		Password: "1qaz@WSX",
		Port:     80,
	}
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8&parseTime=True&loc=Local", cfg.Username, cfg.Password, cfg.Url, cfg.Port, cfg.Database)
	mysqlCfg := mysql.Config{
		DSN: dsn,
	}
	dbHandle, err := gorm.Open(mysql.New(mysqlCfg))
	return dbHandle, err
}

func TestNodesReBalance(t *testing.T) {
	var err error
	rw = new(ReBalanceWorker)
	rw.dbHandle, err = initTestDB()
	if err != nil {
		t.Fatal(err)
	}
	srcNodeList := []string{"11.60.241.50:17310"}
	dstNodeList := []string{"11.60.241.112:17310"}

	ctrl := newNodeMigrateController(1, "11.60.241.50:17010", RebalanceData, srcNodeList, dstNodeList, rw)
	t.Run("test node migrate", func(t *testing.T) {
		err = ctrl.ReBalanceStart()
		if err != nil {
			t.Fatal(err)
		}
	})
}

func printMigrate(info *MigrateRecordTable) error {
	fmt.Println(fmt.Sprintf("zone : %v migrate dp: %v from node: %v disk: %v to node: %v", info.ZoneName, info.PartitionID, info.SrcAddr, info.SrcDisk, info.DstAddr))
	fmt.Println(fmt.Sprintf("node usage: %v -> %v", info.OldUsage, info.NewUsage))
	fmt.Println(fmt.Sprintf("disk usage %v -> %v", info.OldDiskUsage, info.NewDiskUsage))
	return nil
}

func TestReBalanceZone(t *testing.T) {
	rw := &ReBalanceWorker{}
	ctrl := newZoneReBalanceController(1, cluster, zoneName, RebalanceData, highRatio, lowRatio, goalRatio, rw)

	t.Run("test reBalance", func(t *testing.T) {
		err := ctrl.ReBalanceStart()
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestReBalanceStart(t *testing.T) {
	rw := ReBalanceWorker{}
	taskID, err := rw.ZoneReBalanceStart(cluster, zoneName, RebalanceData, highRatio, lowRatio, goalRatio, 50, 10, defaultDstMetaNodePartitionMaxCount)
	if err != nil {
		t.Fatal(err)
	}
	for {
		time.Sleep(time.Second * 15)
		status, err := rw.ReBalanceStatus(cluster, RebalanceData, taskID)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Println("++++++++++++++++++++++++status++++++++++++++++++++++++")
		fmt.Println(status)
		if status == 1 {
			break
		}

	}
}

func TestReBalanceStop(t *testing.T) {
	rw := ReBalanceWorker{}
	taskID, err := rw.ZoneReBalanceStart(cluster, zoneName, RebalanceData, highRatio, lowRatio, goalRatio, 50, 10, defaultDstMetaNodePartitionMaxCount)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second * 30)
	status, err := rw.ReBalanceStatus(cluster, RebalanceData, taskID)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("++++++++++++++++++++++++status++++++++++++++++++++++++")
	fmt.Println(status)
	err = rw.ReBalanceStop(taskID)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second * 5)
	status, err = rw.ReBalanceStatus(cluster, RebalanceData, taskID)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("++++++++++++++++++++++++status++++++++++++++++++++++++")
	fmt.Println(status)
}

func TestReBalanceReStart(t *testing.T) {
	rw := ReBalanceWorker{}
	taskID, err := rw.ZoneReBalanceStart(cluster, zoneName, RebalanceData, highRatio, lowRatio, goalRatio, 50, 10, defaultDstMetaNodePartitionMaxCount)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second * 30)
	status, err := rw.ReBalanceStatus(cluster, RebalanceData, taskID)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("++++++++++++++++++++++++status++++++++++++++++++++++++")
	fmt.Println(status)
	err = rw.ReBalanceStop(taskID)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second * 5)
	status, err = rw.ReBalanceStatus(cluster, RebalanceData, taskID)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("++++++++++++++++++++++++status++++++++++++++++++++++++")
	fmt.Println(status)
	taskID, err = rw.ZoneReBalanceStart(cluster, zoneName, RebalanceData, highRatio, lowRatio, goalRatio, 50, 10, defaultDstMetaNodePartitionMaxCount)
	if err != nil {
		t.Fatal(err)
	}
	for {
		time.Sleep(time.Second * 15)
		status, err := rw.ReBalanceStatus(cluster, RebalanceData, taskID)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Println("++++++++++++++++++++++++status++++++++++++++++++++++++")
		fmt.Println(status)
		if status == 1 {
			break
		}

	}
}

func TestReBalanceDupStart(t *testing.T) {
	rw := ReBalanceWorker{}
	taskID, err := rw.ZoneReBalanceStart(cluster, zoneName, RebalanceData, highRatio, lowRatio, goalRatio, 50, 10, defaultDstMetaNodePartitionMaxCount)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Second)
	taskID, err = rw.ZoneReBalanceStart(cluster, zoneName, RebalanceData, highRatio, lowRatio, goalRatio, 50, 10, defaultDstMetaNodePartitionMaxCount)
	if err != nil {
		t.Fatal(err)
	}
	for {
		time.Sleep(time.Second * 15)
		status, err := rw.ReBalanceStatus(cluster, RebalanceData, taskID)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Println("++++++++++++++++++++++++status++++++++++++++++++++++++")
		fmt.Println(status)
		if status == 1 {
			break
		}

	}
}

func TestServer(t *testing.T) {
	rw := ReBalanceWorker{}
	rw.registerHandler()
	fmt.Println("listen at 18080")
	err := http.ListenAndServe(":18080", nil)
	if err != nil {
		t.Fatal(err)
	}
}

func Test_GetDataNode(t *testing.T) {
	dc := getDataHttpClient("11.127.110.5", "17031")
	stats, err := dc.GetDatanodeStats()
	assert.Nil(t, err)
	fmt.Printf("nodeStatsInfo: %v", stats)
}
