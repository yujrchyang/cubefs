package rebalance

import (
	"fmt"
	"github.com/cubefs/cubefs/util/log"
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
	ID uint64 `gorm:"column:id;"`

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
	TaskType     TaskType      `gorm:"column:task_type"`
	NeedDelete   int           `gorm:"column:need_delete"`   // 1- 需要删除  0-不需要删除
	FinishDelete int           `gorm:"column:finish_delete"` // 0-默认值 1-成功 -1-失败
	CreatedAt    time.Time     `gorm:"column:created_at"`
	UpdateAt     time.Time     `gorm:"column:update_at"`
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

func (rw *ReBalanceWorker) GetMigrateRecordsByCond(cond map[string]interface{}, src, dst, date string, page, pageSize int) (total int64, migrateRecords []*MigrateRecordTable, err error) {
	migrateRecords = make([]*MigrateRecordTable, 0)
	query := rw.dbHandle.Table(MigrateRecordTable{}.TableName()).
		Where(cond)
	if src != "" {
		query.Where("src_addr like ?", fmt.Sprintf("%%%s%%", src))
	}
	if dst != "" {
		query.Where("dst_addr like ?", fmt.Sprintf("%%%s%%", dst))
	}
	if date != "" {
		query.Where("created_at >= ?", date)
	}
	err = query.Count(&total).Error
	if err != nil {
		return 0, nil, err
	}
	if page > 0 && pageSize > 0 {
		query.Limit(pageSize).Offset((page - 1) * pageSize)
	}
	err = query.Find(&migrateRecords).Error
	if err != nil {
		return total, nil, err
	}
	return
}

func (rw *ReBalanceWorker) GetPartitionMigCount(taskID, pid uint64) (count int, err error) {
	records := make([]*MigrateRecordTable, 0)
	err = rw.dbHandle.Table(MigrateRecordTable{}.TableName()).
		Where("task_id = ? and partition_id = ?", taskID, pid).
		Find(&records).Error
	if err != nil {
		return
	}
	return len(records), err
}

func (rw *ReBalanceWorker) GetNeedDeleteReplicaDp(createTime time.Time) ([]*MigrateRecordTable, error) {
	records := make([]*MigrateRecordTable, 0)
	err := rw.dbHandle.Table(MigrateRecordTable{}.TableName()).
		Where("need_delete = ? and finish_delete != ? and created_at <= ?", DeleteFlagForTwoReplica, FinishDeleteSuccess, createTime).
		Find(&records).Error
	if err != nil {
		log.LogErrorf("GetNeedDeleteReplicaDp failed: time(%v) err(%v)", createTime, err)
	}
	return records, err
}

func (rw *ReBalanceWorker) UpdateNeedDeleteReplicaDpRecords(records []*MigrateRecordTable) {
	var (
		failedList  []uint64
		successList []uint64
		defaultList []uint64
		err         error
	)
	for _, record := range records {
		switch record.FinishDelete {
		case FinishDeleteDefault:
			defaultList = append(defaultList, record.ID)
		case FinishDeleteFailed:
			failedList = append(failedList, record.ID)
		case FinishDeleteSuccess:
			successList = append(successList, record.ID)
		}
	}
	if len(defaultList) > 0 {
		for i := 0; i < 10; i++ {
			err = rw.dbHandle.Table(MigrateRecordTable{}.TableName()).Where("id in ?", defaultList).
				Updates(map[string]interface{}{
					"finish_delete": FinishDeleteDefault,
					"update_at":     time.Now(),
				}).Error
			if err == nil {
				break
			}
		}
		if err != nil {
			log.LogErrorf("UpdateNeedDeleteReplicaDpRecords: defaultList failed: %v", defaultList)
		}
	}
	if len(failedList) > 0 {
		for i := 0; i < 10; i++ {
			err = rw.dbHandle.Table(MigrateRecordTable{}.TableName()).Where("id in ?", failedList).
				Updates(map[string]interface{}{
					"finish_delete": FinishDeleteFailed,
					"update_at":     time.Now(),
				}).Error
			if err == nil {
				break
			}
		}
		if err != nil {
			log.LogErrorf("UpdateNeedDeleteReplicaDpRecords: failedList failed: %v", successList)
		}
	}
	if len(successList) > 0 {
		for i := 0; i < 10; i++ {
			err = rw.dbHandle.Table(MigrateRecordTable{}.TableName()).Where("id in ?", successList).
				Updates(map[string]interface{}{
					"finish_delete": FinishDeleteSuccess,
					"update_at":     time.Now(),
				}).Error
			if err == nil {
				break
			}
		}
		if err != nil {
			log.LogErrorf("UpdateNeedDeleteReplicaDpRecords: successList failed: %v", successList)
		}
	}
}

// todo: 迁移任务 dst src存表
type RebalancedInfoTable struct {
	ID                           uint64        `gorm:"column:id;"`
	Cluster                      string        `gorm:"column:cluster"`
	Host                         string        `gorm:"column:host;"`
	ZoneName                     string        `gorm:"column:zone_name;"`
	VolName                      string        `gorm:"column:vol_name;"`
	RType                        RebalanceType `gorm:"column:rebalance_type;"`
	TaskType                     TaskType      `gorm:"column:task_type"` // rebalance or 节点迁移
	Status                       int           `gorm:"column:status;"`
	MaxBatchCount                int           `gorm:"column:max_batch_count"`
	HighRatio                    float64       `gorm:"column:high_ratio;"`
	LowRatio                     float64       `gorm:"column:low_ratio;"`
	GoalRatio                    float64       `gorm:"column:goal_ratio;"`
	MigrateLimitPerDisk          int           `gorm:"column:migrate_limit_per_disk;"`
	DstMetaNodePartitionMaxCount int           `gorm:"column:dst_metanode_partition_max_count"`
	SrcNodes                     string        `gorm:"column:src_nodes_list"`
	DstNodes                     string        `gorm:"column:dst_nodes_list"`
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

func (rw *ReBalanceWorker) GetRebalancedInfoByID(id uint64) (info *RebalancedInfoTable, err error) {
	info = &RebalancedInfoTable{}
	err = rw.dbHandle.Table(RebalancedInfoTable{}.TableName()).
		Where("id = ?", id).First(&info).Error
	return
}

func (rw *ReBalanceWorker) GetRebalancedInfoByZone(cluster, zoneName string, rType RebalanceType) (rebalancedInfo *RebalancedInfoTable, err error) {
	rebalancedInfo = &RebalancedInfoTable{}
	err = rw.dbHandle.Table(RebalancedInfoTable{}.TableName()).
		Where("cluster = ? and zone_name=? and rebalance_type=?", cluster, zoneName, rType).
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

func (rw *ReBalanceWorker) insertRebalanceInfo(cluster, zoneName string, rType RebalanceType, maxBatchCount int,
	highRatio, lowRatio, goalRatio float64, migrateLimitPerDisk, dstMNPartitionMaxCount int, status Status) (rInfo *RebalancedInfoTable, err error) {
	rInfo, err = rw.GetRebalancedInfoByZone(cluster, zoneName, rType)
	if err != nil && err.Error() != RECORD_NOT_FOUND {
		return nil, err
	}
	if rInfo.ID > 0 {
		return nil, fmt.Errorf("已存在相同任务[%v]，请勿重复创建！", rInfo.ID)
	}
	rInfo = new(RebalancedInfoTable)
	rInfo.Status = int(status)
	rInfo.MaxBatchCount = maxBatchCount
	rInfo.HighRatio = highRatio
	rInfo.LowRatio = lowRatio
	rInfo.GoalRatio = goalRatio
	rInfo.MigrateLimitPerDisk = migrateLimitPerDisk
	rInfo.RType = rType
	rInfo.TaskType = ZoneAutoReBalance
	rInfo.DstMetaNodePartitionMaxCount = dstMNPartitionMaxCount
	rInfo.Cluster = cluster
	rInfo.Host = rw.getClusterHost(cluster)
	rInfo.ZoneName = zoneName
	rInfo.CreatedAt = time.Now()
	rInfo.UpdatedAt = rInfo.CreatedAt
	err = rw.PutRebalancedInfoToDB(rInfo)
	return
}

func (rw *ReBalanceWorker) updateRebalancedInfo(cluster, zoneName string, rType RebalanceType, maxBatchCount int,
	highRatio, lowRatio, goalRatio float64, migrateLimitPerDisk, dstMNPartitionMaxCount int, status Status) (rInfo *RebalancedInfoTable, err error) {
	rInfo, err = rw.GetRebalancedInfoByZone(cluster, zoneName, rType)
	if err != nil {
		return nil, err
	}
	rInfo.Status = int(status)
	rInfo.MaxBatchCount = maxBatchCount
	rInfo.HighRatio = highRatio
	rInfo.LowRatio = lowRatio
	rInfo.GoalRatio = goalRatio
	rInfo.MigrateLimitPerDisk = migrateLimitPerDisk
	rInfo.RType = rType
	rInfo.TaskType = ZoneAutoReBalance
	rInfo.DstMetaNodePartitionMaxCount = dstMNPartitionMaxCount
	rInfo.UpdatedAt = time.Now()
	_, err = rw.UpdateRebalancedInfo(rInfo)
	return
}

// 节点迁移只能更新一部分参数
func (rw *ReBalanceWorker) updateNodesRebalanceInfo(taskId uint64, maxBatchCount int, migrateLimitPerDisk int, dstMNPartitionMaxCount int) error {
	err := rw.dbHandle.Table(RebalancedInfoTable{}.TableName()).Where("id = ?", taskId).
		Updates(map[string]interface{}{
			"max_batch_count":                  maxBatchCount,
			"migrate_limit_per_disk":           migrateLimitPerDisk,
			"dst_metanode_partition_max_count": dstMNPartitionMaxCount,
		}).Error
	return err
}

func (rw *ReBalanceWorker) createNodesRebalanceInfo(cluster string, rType RebalanceType, maxBatchCount int, dstMNPartitionMaxCount int,
	srcNodes, dstNodes string, status Status) (rInfo *RebalancedInfoTable, err error) {

	rInfo = new(RebalancedInfoTable)
	rInfo.Cluster = cluster
	rInfo.Host = rw.getClusterHost(cluster)
	rInfo.RType = rType
	rInfo.TaskType = NodesMigrate
	rInfo.Status = int(status)
	rInfo.MaxBatchCount = maxBatchCount
	rInfo.MigrateLimitPerDisk = -1
	rInfo.DstMetaNodePartitionMaxCount = dstMNPartitionMaxCount
	rInfo.SrcNodes = srcNodes
	rInfo.DstNodes = dstNodes
	rInfo.CreatedAt = time.Now()
	rInfo.UpdatedAt = rInfo.CreatedAt
	err = rw.PutRebalancedInfoToDB(rInfo)
	return
}

func (rw *ReBalanceWorker) updateRestartNodesRebalanceInfo(rInfo *RebalancedInfoTable) (*RebalancedInfoTable, error) {
	rInfo.UpdatedAt = time.Now()
	_, err := rw.UpdateRebalancedInfo(rInfo)
	return rInfo, err
}

func (rw *ReBalanceWorker) stopRebalanced(taskId uint64, isManualStop bool) (err error) {
	if taskId <= 0 {
		return fmt.Errorf("请指定任务ID")
	}
	var rInfo *RebalancedInfoTable
	rInfo, err = rw.GetRebalancedInfoByID(taskId)
	if err != nil {
		return
	}

	if isManualStop {
		rInfo.Status = int(StatusTerminating)
	} else {
		rInfo.Status = int(StatusStop)
	}
	if rInfo.ID > 0 {
		rInfo.ZoneName = fmt.Sprintf("%v#%v", rInfo.ZoneName, rInfo.ID)
		rInfo.UpdatedAt = time.Now()
		_, err = rw.UpdateRebalancedInfo(rInfo)
	}
	return
}

func (rw *ReBalanceWorker) GetRebalancedInfoTotalCount(cluster, zoneName string, rType RebalanceType, taskType TaskType, status int) (count int64, err error) {
	tx := rw.dbHandle.Table(RebalancedInfoTable{}.TableName())
	if len(cluster) > 0 {
		tx.Where("cluster = ?", cluster)
	}
	if len(zoneName) > 0 {
		tx.Where("zone_name like ?", fmt.Sprintf("%v%%", zoneName))
	}
	if rType < MaxRebalanceType {
		tx.Where("rebalance_type = ?", rType)
	}
	if taskType > 0 && taskType < MaxTaskType {
		tx.Where("task_type = ?", taskType)
	}
	if status > 0 {
		tx.Where("status = ?", status)
	}
	err = tx.Count(&count).Error
	return
}

func (rw *ReBalanceWorker) GetRebalancedInfoList(cluster, zoneName string, rType RebalanceType, taskType TaskType, status, page, pageSize int) (rebalancedInfo []*RebalancedInfoTable, err error) {
	limit := pageSize
	offset := (page - 1) * pageSize
	tx := rw.dbHandle.Table(RebalancedInfoTable{}.TableName()).
		Order("id DESC").
		Limit(limit).Offset(offset)
	if len(cluster) > 0 {
		tx.Where("cluster = ?", cluster)
	}
	if len(zoneName) > 0 {
		tx.Where("zone_name like ?", fmt.Sprintf("%v%%", zoneName))
	}
	if rType < MaxRebalanceType {
		tx.Where("rebalance_type = ?", rType)
	}
	if taskType > 0 && taskType < MaxTaskType {
		tx.Where("task_type = ?", taskType)
	}
	if status > 0 {
		tx.Where("status = ?", status)
	}
	if err = tx.Find(&rebalancedInfo).Error; err != nil {
		return
	}
	for _, info := range rebalancedInfo {
		info.ZoneName = strings.Split(info.ZoneName, "#")[0]
	}
	return
}

func (rw *ReBalanceWorker) GetSrcNodeInfoList(taskId uint64) ([]*RebalanceNodeInfo, error) {
	records := make([]*RebalanceNodeInfo, 0)
	err := rw.dbHandle.Table(MigrateRecordTable{}.TableName()).
		Select("src_addr as addr, count(partition_id) as total_count").
		Where("task_id = ?", taskId).
		Group("task_id, src_addr").
		Find(&records).Error
	if err != nil {
		return nil, err
	}
	return records, nil
}

func (rw *ReBalanceWorker) GetDstNodeInfoList(taskId uint64) ([]*RebalanceNodeInfo, error) {
	records := make([]*RebalanceNodeInfo, 0)
	err := rw.dbHandle.Table(MigrateRecordTable{}.TableName()).
		Select("dst_addr as addr, count(partition_id) as total_count").
		Where("task_id = ?", taskId).
		Group("task_id, dst_addr").
		Find(&records).Error
	if err != nil {
		return nil, err
	}
	return records, nil
}
