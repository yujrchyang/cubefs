package cutil

import (
	"fmt"
	"github.com/cubefs/cubefs/util/config"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

var (
	SRE_DB                *gorm.DB
	CONSOLE_DB            *gorm.DB
	MYSQL_DB              *gorm.DB
	ClickHouseDBHostAddr  string
	ClickHouseDBROnlyUser string
	ClickHouseDBPassword  string
)

const (
	defaultMaxIdleConns = 3
	defaultMaxOpenConns = 8
)

func OpenGorm(cfg *config.MysqlConfig) (dbHandle *gorm.DB, err error) {
	mysqlConfig := mysql.Config{
		DSN:                       DataSourceName(cfg),
		DefaultStringSize:         191,
		DisableDatetimePrecision:  true,
		DontSupportRenameIndex:    true,
		DontSupportRenameColumn:   true,
		SkipInitializeWithVersion: false,
	}
	dbHandle, err = gorm.Open(mysql.New(mysqlConfig))
	if err != nil {
		return
	}
	db, _ := dbHandle.DB()

	db.SetMaxIdleConns(defaultMaxIdleConns)
	db.SetMaxOpenConns(defaultMaxOpenConns)
	return
}

func DataSourceName(config *config.MysqlConfig) string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8&loc=Local&parseTime=true", config.Username, config.Password, config.Url, config.Port, config.Database)
}
