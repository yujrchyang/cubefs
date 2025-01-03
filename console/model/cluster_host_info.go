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

type TopHostResponse struct {
	Total int
	Data  []*ClusterHostInfo
}

type ClusterHostInfo struct {
	ID           uint64    `gorm:"column:id"`
	ClusterName  string    `gorm:"column:cluster"`
	ZoneName     string    `gorm:"column:zone"`
	Host         string    `gorm:"column:addr"`
	ModuleType   int       `gorm:"column:module_type"`
	Capacity     uint64    `gorm:"column:capacity"`
	Used         uint64    `gorm:"column:used"`
	UsedRatio    float64   `gorm:"column:used_ratio"`
	PartitionCnt int       `gorm:"column:partition_count"`
	InodeCount   uint64    `gorm:"column:inode_count"`
	DentryCount  uint64    `gorm:"column:dentry_count"`
	UpdateTime   time.Time `gorm:"column:update_time;default:CURRENT_TIMESTAMP"`
	UpdateAt     string    `gorm:"-"`
}

func (ClusterHostInfo) TableName() string {
	return "cluster_host_info"
}

func CleanExpiredHosts(cluster, timeStr string) error {
	err := cutil.CONSOLE_DB.Table(ClusterHostInfo{}.TableName()).
		Where("cluster = ? AND update_time < ?", cluster, timeStr).
		Delete(&ClusterHostInfo{}).Error
	if err != nil {
		log.LogErrorf("CleanExpiredHosts failed: cluster(%v) err(%v)", cluster, err)
	}
	return err
}

func StoreHostInfoRecords(records []*ClusterHostInfo) (err error) {
	length := len(records)
	for i := 0; i < length; i += maxInsertBatchNum {
		end := i + maxInsertBatchNum
		if end >= length {
			end = length
		}
		if err = cutil.CONSOLE_DB.Table(ClusterHostInfo{}.TableName()).CreateInBatches(records[i:end], len(records[i:end])).Error; err != nil {
			log.LogWarnf("StoreHostInfoRecords: batch insert failed: %v", err)
		}
	}
	return
}

// 查询top host
// cluster、 zone、 起止时间都是必填参数
// 只能选出一个点的时间 不支持范围内多点
func (c ClusterHostInfo) LoadTopHostInfo(cluster, zone string, moduleType int, start, end time.Time, orderBy string, page, pageSize int32) (records []*ClusterHostInfo, err error) {
	err = cutil.CONSOLE_DB.Table(c.TableName()).
		Where("cluster = ? AND zone = ? AND module_type = ?", cluster, zone, moduleType).
		Where("update_time >= ? AND update_time < ?", start, end).
		Order(orderBy + " DESC").
		Offset(int((page - 1) * pageSize)).Limit(int(pageSize)).
		Find(&records).Error
	if err != nil {
		log.LogErrorf("LoadTopHostInfo: %v %v %v, err: %v", cluster, zone, moduleType, err)
	}
	return
}

func (c ClusterHostInfo) LoadTopHostCount(cluster, zone string, moduleType int, start, end time.Time) (total int64, err error) {
	err = cutil.CONSOLE_DB.Table(c.TableName()).
		Where("cluster = ? AND zone = ? AND module_type = ?", cluster, zone, moduleType).
		Where("update_time >= ? AND update_time < ?", start, end).
		Count(&total).Error
	if err != nil {
		log.LogErrorf("LoadTopHostCount: %v %v %v, err: %v", cluster, zone, moduleType, err)
	}
	return
}

type ClusterZoneInfo struct {
	ID         uint64    `gorm:"column:id"`
	Cluster    string    `gorm:"column:cluster"`
	Zone       string    `gorm:"column:zone"`
	Module     string    `gorm:"column:module"`
	TotalGB    uint64    `gorm:"column:total_gb"`
	UsedGB     uint64    `gorm:"column:used_gb"`
	IsHour     bool      `gorm:"column:is_hour"`
	UpdateTime time.Time `gorm:"column:update_time;default:CURRENT_TIMESTAMP"`
}

func (c ClusterZoneInfo) TableName() string {
	return "cluster_zone_info"
}

func (c ClusterZoneInfo) CleanExpiredRecords(timeStr string) error {
	return cutil.CONSOLE_DB.Table(c.TableName()).Where("update_time < ?", timeStr).
		Delete(&ClusterZoneInfo{}).Error
}

func (c ClusterZoneInfo) StoreZoneInfoRecords(records []*ClusterZoneInfo) error {
	length := len(records)
	for i := 0; i < length; i += maxInsertBatchNum {
		end := i + maxInsertBatchNum
		if end >= length {
			end = length
		}
		if err := cutil.CONSOLE_DB.Table(c.TableName()).
			CreateInBatches(records[i:end], len(records[i:end])).Error; err != nil {
			log.LogWarnf("StoreHostInfoRecords: batch insert failed: %v", err)
		}
	}
	return nil
}

func (c ClusterZoneInfo) LoadZoneUsedInfoByModule(cluster, module, zone string, start, end time.Time) ([]*ClusterZoneInfo, error) {
	records := make([]*ClusterZoneInfo, 0)
	err := cutil.CONSOLE_DB.Table(c.TableName()).
		Where("update_time >= ? AND update_time < ?", start, end).
		Where("cluster = ? AND module = ? AND zone = ?", cluster, module, zone).
		Find(&records).Error
	if err != nil {
		log.LogErrorf("LoadZoneUsedInfoByModule failed: %v %v %v", cluster, module, zone)
		return nil, err
	}
	return records, nil
}
