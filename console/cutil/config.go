package cutil

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/util/config"
)

var (
	// todo: remove this, 就不该有这个全局的集群。。。。邪门 意味着后端同时仅能支持一个集群的
	GlobalCluster = "spark"
	Global_CFG    ConsoleConfig // 全局变量 配置
)

type ConsoleConfig struct {
	Role     string `json:"role"`
	Listen   string `json:"listen"`
	LogDir   string `json:"logDir"`
	LogLevel string `json:"logLevel"`
	LocalIP  string `json:"localIP"` // 或者域名

	MonitorAddr      string `json:"monitorAddr"`
	MonitorCluster   string `json:"monitorCluster"`
	MetaExporterPort string `json:"metaExporterPort"`
	DataExporterPort string `json:"dataExporterPort"`

	MysqlConfig      config.MysqlConfig `json:"mysqlConfig"`
	ConsoleDBConfig  config.MysqlConfig `json:"consoleDBConfig"`
	ClickHouseConfig ClickHouseConfig   `json:"clickHouseConfig"`
	SSOConfig        SSOConfig          `json:"ssoConfig"`
	CFSS3Config      S3Config           `json:"s3Config"`

	HbaseQueryAddr string `json:"hbaseQueryAddr"` // 查询vol的客户端
	SreDomainName  string `json:"sreDomainName"`  // 查询磁盘、网络、系统资源

	IsIntranet     bool `json:"isIntranet"`     // 登录方式，内网需配置下列参数
	StaticResource bool `json:"staticResource"` // 静态资源方式
	CronTaskOn     bool `json:"cronTaskOn"`     // 定时任务开关

	EnableXBP        bool      `json:"enableXbp"` // 是否开启审批流
	XbpProcessConfig XBPConfig `json:"xbpConfig"` // xbp配置参数
}

type XBPConfig struct {
	Domain    string `json:"domain"`     // 域名
	ProcessID uint64 `json:"process_id"` // 流程id
	APIUser   string `json:"api_user"`   // api user
	APISign   string `json:"api_sign"`   // api sign
}

type ClickHouseConfig struct {
	Host     string
	User     string
	Password string
}

type SSOConfig struct {
	Domain  string
	AppCode string
	Token   string
}

type S3Config struct {
	AccessKey string `json:"access_key"`
	SecretKey string `json:"secret_key"`
	EndPoint  string `json:"end_point"`
	Region    string `json:"region"`
	Bucket    string `json:"bucket"`
}

func InitConsoleConfig(cfg *config.Config) error {
	Global_CFG = ConsoleConfig{
		MysqlConfig:      config.MysqlConfig{},
		ConsoleDBConfig:  config.MysqlConfig{},
		ClickHouseConfig: ClickHouseConfig{},
		SSOConfig:        SSOConfig{},
		CFSS3Config:      S3Config{},
		XbpProcessConfig: XBPConfig{},
	}
	if err := json.Unmarshal(cfg.Raw, &Global_CFG); err != nil {
		return err
	}
	return nil
}

func NewInvalidConfiguration(key string, value interface{}) error {
	return fmt.Errorf("invalid configuration: %v:[%v]", key, value)
}

func GetClusterParam(cluster *string) string {
	if cluster == nil || (cluster != nil && *cluster == "") {
		return GlobalCluster
	} else {
		return *cluster
	}
}
