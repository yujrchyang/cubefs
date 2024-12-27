package model

import (
	"fmt"
	"time"

	"github.com/cubefs/cubefs/console/cutil"
	"github.com/cubefs/cubefs/util/log"
)

var MaxHistoryDataNum = 100000

type VolumeSummaryView struct {
	ID              int64     `gorm:"column:id;"`
	ClusterName     string    `gorm:"column:cluster_name;"`
	VolName         string    `gorm:"column:vol_name;"`
	Capacity        string    `gorm:"column:capacity;"`
	UsedGB          float64   `gorm:"column:used_gb;"`
	CrossZoneHAType string    `gorm:"column:cross_zone_type;"`
	Zones           string    `gorm:"column:zone_name;"`
	Ops             int64     `gorm:"column:ops;"`
	InodeCount      int64     `gorm:"column:inode_count;"`
	Organization    string    `gorm:"column:app_organization;"`
	UpdateTime      time.Time `gorm:"-"` // dataTime类型会转化为uint8
}

func (VolumeSummaryView) TableName() string {
	return "volume_summary"
}

// 字段：volName inodeCount  usedGB
// 最新0点的更新的数据
func LoadInodeTopNVol(cluster string, topN int, zone, source string, orderBy int) (r []*VolumeSummaryView, err error) {
	r = make([]*VolumeSummaryView, 0)

	end := time.Now()
	start := end.Add(-10 * time.Minute)
	dbHandle := cutil.CONSOLE_DB.Table(ConsoleVolume{}.TableName()).
		Select("volume as vol_name, inode_count").
		Where("update_time >= ? AND update_time < ?", start, end).
		Where("cluster = ?", cluster)
	if len(zone) > 0 {
		dbHandle.Where("zone = ?", zone)
	}
	if len(source) > 0 {
		dbHandle.Where("source = ?", source)
	}
	if orderBy == 0 {
		dbHandle.Order("inode_count DESC")
	} else {
		dbHandle.Order("inode_count ASC")
	}
	if err = dbHandle.Limit(topN).
		Scan(&r).Error; err != nil {
		log.LogErrorf("LoadInodeTopNVol failed: cluster(%v) err(%v)", cluster, err)
	}
	return
}

func LoadUsedGBTopNVol(cluster string, topN int, zone, source string, orderBy int) (r []*VolumeSummaryView, err error) {
	r = make([]*VolumeSummaryView, 0)

	end := time.Now()
	start := end.Add(-10 * time.Minute)
	dbHandle := cutil.CONSOLE_DB.Table(ConsoleVolume{}.TableName()).
		Select("volume as vol_name, used_gb").
		Where("cluster = ?", cluster).
		Where("update_time >= ? and update_time < ?", start, end)
	if len(zone) > 0 {
		dbHandle.Where("zone = ?", zone)
	}
	if len(source) > 0 {
		dbHandle.Where("source = ?", source)
	}
	if orderBy == 0 {
		dbHandle.Order("used_gb DESC")
	} else {
		dbHandle.Order("used_gb ASC")
	}
	if err = dbHandle.Limit(topN).
		Scan(&r).Error; err != nil {
		log.LogErrorf("LoadUsedGBTopNVol failed: cluster(%v) err(%v)", cluster, err)
	}
	return
}

type VolumeHistoryCurve struct {
	Date          string  `gorm:"column:update_time"`
	Capacity      int64   `gorm:"column:total_gb"`
	UsedGB        int64   `gorm:"column:used_gb"`
	UsedRatio     float64 `gorm:"column:used_ratio"`
	InodeCount    int64   `gorm:"column:inode_count"`
	WritableDpNum int64   `gorm:"column:writable_dp_num"`
}

func (c *VolumeHistoryCurve) String() string {
	return fmt.Sprintf("{date: %v, capacity: %v, usedGB: %v, usedRatio: %v, inodecount: %v, writebleDpNum: %v}\n",
		c.Date, c.Capacity, c.UsedGB, c.UsedRatio, c.InodeCount, c.WritableDpNum)
}
