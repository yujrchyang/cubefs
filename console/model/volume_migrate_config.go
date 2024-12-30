package model

import (
	"github.com/cubefs/cubefs/console/cutil"
	"github.com/cubefs/cubefs/util/log"
	"time"
)

type VolumeMigrateConfig struct {
	Id            uint64    `gorm:"column:id"`
	ClusterName   string    `gorm:"column:cluster"`
	VolName       string    `gorm:"column:volume"`
	Smart         int       `gorm:"column:smart"`
	SmartRules    string    `gorm:"column:smart_rules"`
	SsdDirs       string    `gorm:"column:ssd_dirs"`
	HddDirs       string    `gorm:"column:hdd_dirs"`
	Compact       int       `gorm:"column:compact"`
	MigrationBack int       `gorm:"column:migrate_back"`
	UpdateTime    time.Time `gorm:"column:update_time"`
	// todo: 使用量是单副本前还是✖️副本数后
	HddCapacity float64 `gorm:"column:hdd_capacity"`
	SsdCapacity float64 `gorm:"column:ssd_capacity"`
	ReplicaNum  int     `gorm:"column:replica_num"`
}

func (VolumeMigrateConfig) TableName() string {
	return "volume_migrate_config"
}

func BatchInsertVolConfig(records []*VolumeMigrateConfig) (err error) {
	length := len(records)
	for i := 0; i < length; i += maxInsertBatchNum {
		end := i + maxInsertBatchNum
		if end > length {
			end = length
		}
		if err = cutil.CONSOLE_DB.Table(VolumeMigrateConfig{}.TableName()).CreateInBatches(records[i:end], len(records[i:end])).Error; err != nil {
			log.LogWarnf("BatchInsertVolConfig: batch insert failed: %v", err)
		}
	}
	return err
}

func LoadVolHddSsdCapacityData(cluster, volume string, start, end time.Time) (records []*VolumeMigrateConfig, err error) {
	records = make([]*VolumeMigrateConfig, 0)
	if err = cutil.CONSOLE_DB.Table(VolumeMigrateConfig{}.TableName()).
		Where("cluster = ? AND volume = ? AND update_time > ? AND update_time < ?", cluster, volume, start, end).
		Find(&records).Error; err != nil {
		log.LogErrorf("LoadVolHddSsdCapacityData failed: vol(%v) err(%v)", volume, err)
	}
	return
}
