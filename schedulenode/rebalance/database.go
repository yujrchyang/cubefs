package rebalance

import (
	"fmt"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"strings"
	"time"
)

func (rw *ReBalanceWorker) DataSourceName() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8&parseTime=True&loc=Local", rw.MysqlConfig.Username, rw.MysqlConfig.Password, rw.MysqlConfig.Url, rw.MysqlConfig.Port, rw.MysqlConfig.Database)
}

func (rw *ReBalanceWorker) OpenSql() (err error) {
	mysqlConfig := mysql.Config{
		DSN:                       rw.DataSourceName(), // data source name
		DefaultStringSize:         191,                 // string 类型字段的默认长度
		DisableDatetimePrecision:  true,                // 禁用 datetime 精度，MySQL 5.6 之前的数据库不支持
		DontSupportRenameIndex:    true,                // 重命名索引时采用删除并新建的方式，MySQL 5.7 之前的数据库和 MariaDB 不支持重命名索引
		DontSupportRenameColumn:   true,                // 用 `change` 重命名列，MySQL 8 之前的数据库和 MariaDB 不支持重命名列
		SkipInitializeWithVersion: false,               // 根据版本自动配置
	}

	if rw.dbHandle, err = gorm.Open(mysql.New(mysqlConfig)); err != nil {
		return
	}
	sqlDB, _ := rw.dbHandle.DB()
	sqlDB.SetMaxIdleConns(rw.MysqlConfig.MaxIdleConns)
	sqlDB.SetMaxOpenConns(rw.MysqlConfig.MaxOpenConns)
	err = rw.dbHandle.AutoMigrate(MigrateRecordTable{})
	if err != nil {
		return
	}
	return
}

const (
	RECORD_NOT_FOUND = "record not found"
)

type MigrateRecordTable struct {
	ID int `gorm:"column:id;"`

	ClusterName  string        `gorm:"column:cluster_name;"`
	ZoneName     string        `gorm:"column:zone_name;"`
	RType        RebalanceType `gorm:"column:rebalance_type;"`
	VolName      string        `gorm:"column:vol_name;"`
	PartitionID  uint64        `gorm:"column:partition_id;"`
	SrcAddr      string        `gorm:"column:src_addr;"`
	SrcDisk      string        `gorm:"column:src_disk"`
	DstAddr      string        `gorm:"column:dst_addr;"`
	OldUsage     float64       `gorm:"column:old_usage"`
	NewUsage     float64       `gorm:"column:new_usage"`
	OldDiskUsage float64       `gorm:"column:old_disk_usage"`
	NewDiskUsage float64       `gorm:"column:new_disk_usage"`
	TaskId       uint64        `gorm:"column:task_id;"`

	CreatedAt time.Time `gorm:"column:created_at"`
}

func (MigrateRecordTable) TableName() string {
	return "migrate_record"
}

func (rw *ReBalanceWorker) PutMigrateInfoToDB(info *MigrateRecordTable) error {
	return rw.dbHandle.Create(info).Error
}

func (rw *ReBalanceWorker) GetMigrateRecordsByZoneName(clusterName, zoneName string) (migrateRecords []*MigrateRecordTable, err error) {
	migrateRecords = make([]*MigrateRecordTable, 0)
	err = rw.dbHandle.Table(MigrateRecordTable{}.TableName()).
		Where("cluster_name = ? AND zone_name = ?", clusterName, zoneName).
		Find(&migrateRecords).Error
	return
}

func (rw *ReBalanceWorker) GetMigrateRecordsBySrcNode(host string) (migrateRecords []*MigrateRecordTable, err error) {
	migrateRecords = make([]*MigrateRecordTable, 0)
	err = rw.dbHandle.Table(MigrateRecordTable{}.TableName()).
		Where("src_addr like ?", fmt.Sprintf("%%%s%%", host)).
		Find(&migrateRecords).Error
	return
}

type RebalancedInfoTable struct {
	ID                           uint64        `gorm:"column:id;"`
	Host                         string        `gorm:"column:host;"`
	ZoneName                     string        `gorm:"column:zone_name;"`
	VolName                      string        `gorm:"column:vol_name;"`
	RType                        RebalanceType `gorm:"column:rebalance_type;"`
	Status                       int           `gorm:"column:status;"`
	MaxBatchCount                int           `gorm:"column:max_batch_count"`
	HighRatio                    float64       `gorm:"column:high_ratio;"`
	LowRatio                     float64       `gorm:"column:low_ratio;"`
	GoalRatio                    float64       `gorm:"column:goal_ratio;"`
	MigrateLimitPerDisk          int           `gorm:"column:migrate_limit_per_disk;"`
	dstMetaNodePartitionMaxCount int           `gorm:"column:dst_metanode_partition_max_count"`
	CreatedAt                    time.Time     `gorm:"column:created_at"`
	UpdatedAt                    time.Time     `gorm:"column:updated_at"`
}

func (RebalancedInfoTable) TableName() string {
	return "rebalanced_info"
}

func (rw *ReBalanceWorker) PutRebalancedInfoToDB(info *RebalancedInfoTable) error {
	return rw.dbHandle.Create(info).Error
}

func (rw *ReBalanceWorker) GetRebalancedInfoByStatus(status Status) (rebalancedInfo []*RebalancedInfoTable, err error) {
	rebalancedInfo = make([]*RebalancedInfoTable, 0)
	err = rw.dbHandle.Table(RebalancedInfoTable{}.TableName()).
		Where("status=?", status).
		Find(&rebalancedInfo).Error
	return
}

func (rw *ReBalanceWorker) GetAllRebalancedInfo() (rebalancedInfo []*RebalancedInfoTable, err error) {
	rebalancedInfo = make([]*RebalancedInfoTable, 0)
	err = rw.dbHandle.Table(RebalancedInfoTable{}.TableName()).
		Find(&rebalancedInfo).Error
	return
}

func (rw *ReBalanceWorker) GetRebalancedInfoByHostAndZoneName(host, zoneName string, rType RebalanceType) (rebalancedInfo *RebalancedInfoTable, err error) {
	rebalancedInfo = &RebalancedInfoTable{}
	err = rw.dbHandle.Table(RebalancedInfoTable{}.TableName()).
		Where("host=? and zone_name=? and rebalance_type=?", host, zoneName, rType).
		First(rebalancedInfo).Error
	return
}

func (rw *ReBalanceWorker) UpdateRebalancedInfo(info *RebalancedInfoTable) (affected int64, err error) {
	result := rw.dbHandle.Table(RebalancedInfoTable{}.TableName()).
		Updates(info)
	affected = result.RowsAffected
	err = result.Error
	return
}

func (rw *ReBalanceWorker) insertOrUpdateRebalancedInfo(clusterHost, zoneName string, rType RebalanceType, maxBatchCount int,
	highRatio, lowRatio, goalRatio float64, migrateLimitPerDisk, dstMNPartitionMaxCount int, status Status) (
	rInfo *RebalancedInfoTable, err error) {
	rInfo, err = rw.GetRebalancedInfoByHostAndZoneName(clusterHost, zoneName, rType)
	if err != nil {
		if err.Error() != RECORD_NOT_FOUND {
			return nil, err
		} else {
			rInfo = new(RebalancedInfoTable)
		}
	}
	rInfo.Status = int(status)
	rInfo.MaxBatchCount = maxBatchCount
	rInfo.HighRatio = highRatio
	rInfo.LowRatio = lowRatio
	rInfo.GoalRatio = goalRatio
	rInfo.MigrateLimitPerDisk = migrateLimitPerDisk
	rInfo.RType = rType
	rInfo.dstMetaNodePartitionMaxCount = dstMNPartitionMaxCount
	if rInfo.ID > 0 {
		rInfo.UpdatedAt = time.Now()
		_, err = rw.UpdateRebalancedInfo(rInfo)
	} else {
		rInfo.Host = clusterHost
		rInfo.ZoneName = zoneName
		rInfo.CreatedAt = time.Now()
		rInfo.UpdatedAt = rInfo.CreatedAt
		err = rw.PutRebalancedInfoToDB(rInfo)
	}
	return
}

func (rw *ReBalanceWorker) stopRebalanced(clusterHost, zoneName string, rType RebalanceType) (err error) {
	var (
		rInfo *RebalancedInfoTable
	)
	if rInfo, err = rw.GetRebalancedInfoByHostAndZoneName(clusterHost, zoneName, rType); err != nil {
		return
	}
	rInfo.Status = int(StatusStop)
	if rInfo.ID > 0 {
		rInfo.ZoneName = fmt.Sprintf("%v#%v", rInfo.ZoneName, rInfo.ID)
		rInfo.UpdatedAt = time.Now()
		_, err = rw.UpdateRebalancedInfo(rInfo)
	}
	return
}

func (rw *ReBalanceWorker) GetRebalancedInfoTotalCount(host, zoneName string, rType RebalanceType, statusNum int) (count int64, err error) {
	tx := rw.dbHandle.Table(RebalancedInfoTable{}.TableName())
	if len(host) > 0 && len(zoneName) > 0 {
		tx.Where("host=? AND zone_name like ?", host, fmt.Sprintf("%v%%", zoneName))
	} else if len(host) > 0 {
		tx.Where("host=?", host)
	} else if len(zoneName) > 0 {
		tx.Where("zone_name like ?", fmt.Sprintf("%v%%", zoneName))
	}
	tx.Where("rebalance_type=?", rType)
	if statusNum > 0 {
		tx.Where("status=?", statusNum)
	}
	err = tx.Count(&count).Error
	return
}

func (rw *ReBalanceWorker) GetRebalancedInfoList(host, zoneName string, rType RebalanceType, page, pageSize, statusNum int) (rebalancedInfo []*RebalancedInfoTable, err error) {
	limit := pageSize
	offset := (page - 1) * pageSize
	tx := rw.dbHandle.Table(RebalancedInfoTable{}.TableName()).
		Order("id DESC").
		Limit(limit).Offset(offset)
	if len(host) > 0 && len(zoneName) > 0 {
		tx.Where("host=? and zone_name like ?", host, fmt.Sprintf("%v%%", zoneName))
	} else if len(host) > 0 {
		tx.Where("host=?", host)
	} else if len(zoneName) > 0 {
		tx.Where("zone_name like ?", fmt.Sprintf("%v%%", zoneName))
	}
	tx.Where("rebalance_type=?", rType)
	if statusNum > 0 {
		tx.Where("status=?", statusNum)
	}
	if err = tx.Find(&rebalancedInfo).Error; err != nil {
		return
	}
	for _, info := range rebalancedInfo {
		info.ZoneName = strings.Split(info.ZoneName, "#")[0]
	}
	return
}
