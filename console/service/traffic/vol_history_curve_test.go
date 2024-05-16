package traffic

import (
	"fmt"
	"github.com/cubefs/cubefs/console/cutil"
	"github.com/cubefs/cubefs/console/proto"
	"github.com/cubefs/cubefs/util/config"
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"
	"testing"
)

var (
	sre, _     = InitTestSreDB()
	console, _ = InitTestDB()
	mysql, _   = InitTestMysqlDB()
)

func InitTestDB() (*gorm.DB, error) {
	var err error
	cfg := &config.MysqlConfig{
		Database: "console",
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

func Test_history_curve(t *testing.T) {
	req1 := &proto.HistoryCurveRequest{
		Cluster:      "cfs_dbBack",
		Volume:       "mysql-backup",
		IntervalType: 1,
		Start:        0,
		End:          0,
	}
	//req1 = &proto.HistoryCurveRequest{
	//	Cluster:      "spark",
	//	Volume:       "dcc_3vol",
	//	IntervalType: 0,
	//	Start:        1695222037,
	//	End:          1695698340,
	//}
	res, err := GetVolHistoryCurve(req1)
	assert.NoError(t, err)
	fmt.Printf("res: %v\n", res)
}
