package model

import (
	"github.com/cubefs/cubefs/console/cutil"
)

type DiskAnalysisResult struct {
	ID          uint64 `gorm:"column:id;"`
	ClusterName string `gorm:"column:cluster_name;"`
	Time        string `gorm:"column:time;"`
	Mode        int    `gorm:"column:analysis_mode;"`
	Type        int    `gorm:"column:type;"`
	IP          string `gorm:"column:ip;"`
	DiskName    string `gorm:"column:disk_name;"`
	MountPath   string `gorm:"column:mount_path;"`
	AwaitTime   int64  `gorm:"column:await_time;"`
}

func (DiskAnalysisResult) TableName() string {
	return "disk_analysis_result"
}

// mount_path 没赋值
func (table DiskAnalysisResult) LoadHighAwaitDisk(cluster, startStr, entStr string, awaitTime int) (r []*DiskAnalysisResult, err error) {
	r = make([]*DiskAnalysisResult, 0)
	err = cutil.SRE_DB.Table(table.TableName()).Where("cluster_name = ? and time >= ? and time <= ? and analysis_mode = 0 and type = 3 and await_time >= ?",
		cluster, startStr, entStr, awaitTime).Group("ip,disk_name").Order("time,ip").Scan(&r).Error
	return
}
