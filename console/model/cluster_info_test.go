package model

import (
	"fmt"
	"github.com/cubefs/cubefs/console/cutil"
	"github.com/cubefs/cubefs/util/config"
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"
	"testing"
	"time"
)

var (
	console, _ = InitTestDB()
	sre, _     = InitTestSreDB()
	mysql, _   = InitTestMysqlDB()
)

func InitTestDB() (*gorm.DB, error) {
	var err error
	cfg := &config.MysqlConfig{
		Database: "console-test",
		Url:      "11.13.125.198",
		Username: "root",
		Password: "1qaz@WSX",
		Port:     80,
	}
	cutil.CONSOLE_DB, err = cutil.OpenGorm(cfg)
	return cutil.CONSOLE_DB, err
}

func InitTestSreDB() (*gorm.DB, error) {
	var err error
	cfg := &config.MysqlConfig{
		Database: "storage_sre",
		Url:      "11.13.125.198",
		Username: "root",
		Password: "1qaz@WSX",
		Port:     80,
	}
	cutil.SRE_DB, err = cutil.OpenGorm(cfg)
	return cutil.SRE_DB, err
}

func InitTestMysqlDB() (*gorm.DB, error) {
	var err error
	cfg := &config.MysqlConfig{
		Database: "chubaofs_res",
		Url:      "gateht3b.jed.jddb.com",
		Username: "chubaofs_res_rr",
		Password: "R5E4JvQ9okW8NbEL",
		Port:     3358,
	}
	cutil.MYSQL_DB, err = cutil.OpenGorm(cfg)
	return cutil.MYSQL_DB, err
}

func TestInsertConsoleCluster(t *testing.T) {

	c1 := &ConsoleCluster{
		ClusterName:   "spark",
		ClusterNameZH: "国内全功能集群",
		MasterDomain:  "cn.chubaofs.jd.local",
		MasterAddrs:   "172.20.131.37:8868,172.20.73.93:8868,10.199.145.148:8868,11.3.81.100:8868,11.3.133.9:8868",
		ObjectDomain:  "objectcfs.jd.local",
		MetaProf:      "9092",
		DataProf:      "6001",
		IsRelease:     false,
		UpdateTime:    time.Now(),
	}
	c1 = &ConsoleCluster{
		ClusterName:   "chubaofs01",
		ClusterNameZH: "测试",
		MasterAddrs:   "11.26.64.185:17010,11.27.91.28:17010,11.27.109.178:17010",
		MetaProf:      "17220",
		IsRelease:     false,
		UpdateTime:    time.Now(),
	}
	c1 = &ConsoleCluster{
		ClusterName:   "mysql",
		ClusterNameZH: "Elasticdb",
		MasterAddrs:   "11.26.64.185:17010,11.27.91.28:17010,11.27.109.178:17010",
		MetaProf:      "17220",
		IsRelease:     false,
		UpdateTime:    time.Now(),
	}
	ConsoleCluster{}.InsertConsoleCluster(c1)

}

func TestLoadConsoleClusterList(t *testing.T) {
	InitTestDB()
	table := ConsoleCluster{}
	clusters, err := table.LoadConsoleClusterList("")
	assert.NoError(t, err)
	fmt.Printf("%v\n", clusters)

	clusters, err = table.LoadConsoleClusterList("spark")
	assert.NoError(t, err)
	assert.Equal(t, 1, len(clusters))
	//fmt.Printf("%v\n", clusters)

	clusters, err = table.LoadConsoleClusterList("cmdkfhhf")
	assert.NoError(t, err)
	assert.Empty(t, clusters)
	//fmt.Printf("%v\n", clusters)
}
