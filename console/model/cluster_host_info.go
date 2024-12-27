package model

import (
	"github.com/cubefs/cubefs/console/cutil"
	"github.com/cubefs/cubefs/util/log"
	"time"
)

const (
	ModuleTypeMaster int = iota
	ModuleTypeDataNode
	ModuleTypeMetaNode
	ModuleTypeObjectNode
	ModuleTypeFlashNode
)

type ClusterHostInfo struct {
	ClusterName string    `gorm:"column:cluster_name"`
	ZoneName    string    `gorm:"column:zone_name"`
	HostAddr    string    `gorm:"column:host_addr"`
	ModuleType  int       `gorm:"column:module_type"`
	Capacity    uint64    `gorm:"column:capacity"`
	Used        uint64    `gorm:"column:used"`
	UsedRatio   float64   `gorm:"column:used_ratio"`
	DpNum       uint32    `gorm:"column:dp_num"`
	MpNum       uint32    `gorm:"column:mp_num"`
	UpdateTime  time.Time `gorm:"column:update_time"`
}

func (ClusterHostInfo) TableName() string {
	return "cluster_host_info"
}

func CleanExpiredHosts(timeStr string) error {
	return cutil.CONSOLE_DB.Where("update_time < ?", timeStr).Delete(&ClusterHostInfo{}).Error
}

// insert batch
func StoreHostInfoRecords(records []*ClusterHostInfo) (err error) {
	length := len(records)
	for i := 0; i < length; i += maxInsertBatchNum {
		end := i + maxInsertBatchNum
		if end >= length {
			end = length
		}
		if err = cutil.CONSOLE_DB.CreateInBatches(records[i:end], len(records[i:end])).Error; err != nil {
			log.LogWarnf("StoreHostInfoRecords: batch insert failed: %v", err)
		}
	}
	return
}
