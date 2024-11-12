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
	DstAddr      string        `gorm:"column:dst_addr"`
	OldUsage     float64       `gorm:"column:old_usage"`
	NewUsage     float64       `gorm:"column:new_usage"`
	OldDiskUsage float64       `gorm:"column:old_disk_usage"`
	NewDiskUsage float64       `gorm:"column:new_disk_usage"`
	TaskId       uint64        `gorm:"column:task_id"`
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
	// vols迁移, srcZone用zone_name字段
	DstZone             string `gorm:"column:dst_zone"`
	VolBatchCount       int    `gorm:"column:vol_batch_count"`
	PartitionBatchCount int    `gorm:"column:partition_batch_count"`
	RoundInterval       int    `gorm:"column:round_interval"`
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

func (rw *ReBalanceWorker) GetRebalancedInfoTotalCount(cluster, zoneName, volume string, rType RebalanceType, taskType TaskType, status int) (count int64, err error) {
	tx := rw.dbHandle.Table(RebalancedInfoTable{}.TableName())
	if len(cluster) > 0 {
		tx.Where("cluster = ?", cluster)
	}
	if rType < MaxRebalanceType {
		tx.Where("rebalance_type = ?", rType)
	}
	if taskType > 0 && taskType < MaxTaskType {
		tx.Where("task_type = ?", taskType)
	}
	if len(zoneName) > 0 {
		tx.Where("zone_name like ?", fmt.Sprintf("%%%s%%", zoneName))
	}
	if len(volume) > 0 {
		tx.Where("vol_name like ?", fmt.Sprintf("%%%s%%", volume))
	}
	if status > 0 {
		tx.Where("status = ?", status)
	}
	err = tx.Count(&count).Error
	return
}

func (rw *ReBalanceWorker) GetRebalancedInfoList(cluster, zoneName, volume string, rType RebalanceType, taskType TaskType, status, page, pageSize int) (rebalancedInfo []*RebalancedInfoTable, err error) {
	limit := pageSize
	offset := (page - 1) * pageSize
	tx := rw.dbHandle.Table(RebalancedInfoTable{}.TableName()).
		Order("id DESC").
		Limit(limit).Offset(offset)
	if len(cluster) > 0 {
		tx.Where("cluster = ?", cluster)
	}
	if rType < MaxRebalanceType {
		tx.Where("rebalance_type = ?", rType)
	}
	if taskType > 0 && taskType < MaxTaskType {
		tx.Where("task_type = ?", taskType)
	}
	if len(zoneName) > 0 {
		tx.Where("zone_name like ?", fmt.Sprintf("%%%v%%", zoneName))
	}
	if len(volume) > 0 {
		tx.Where("vol_name like ?", fmt.Sprintf("%%%s%%", volume))
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

func (rw *ReBalanceWorker) updateMigrateControl(taskID uint64, clusterCurrency, volCurrency, partitionCurrency, roundInterval int) error {
	err := rw.dbHandle.Table(RebalancedInfoTable{}.TableName()).Where("id = ?", taskID).
		Updates(map[string]interface{}{
			"max_batch_count":       clusterCurrency,
			"vol_batch_count":       volCurrency,
			"partition_batch_count": partitionCurrency,
			"round_interval":        roundInterval,
			"updated_at":            time.Now(),
		}).Error
	if err != nil {
		log.LogErrorf("updateMigrateControl: taskID(%v) err(%v)", taskID, err)
	}
	return err
}

func (rw *ReBalanceWorker) updateMigrateTaskStatus(taskID uint64, status int) error {
	err := rw.dbHandle.Table(RebalancedInfoTable{}.TableName()).Where("id = ?", taskID).
		Updates(map[string]interface{}{
			"status":     status,
			"updated_at": time.Now(),
		}).Error
	if err != nil {
		log.LogErrorf("updateMigrateTaskStatus: taskID(%v) status(%v) err(%v)", taskID, status, err)
	}
	return err
}

func (rw *ReBalanceWorker) insertMigrateTask(taskInfo *MigVolTask) (id uint64, err error) {
	info := &RebalancedInfoTable{
		Cluster:       taskInfo.cluster,
		ZoneName:      taskInfo.srcZone,
		DstZone:       taskInfo.dstZone,
		VolName:       strings.Join(taskInfo.vols, ","),
		TaskType:      VolsMigrate,
		Status:        int(taskInfo.status),
		MaxBatchCount: taskInfo.clusterCurrency,
		VolBatchCount: taskInfo.volCurrency,
		RoundInterval: taskInfo.roundInterval,
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}
	switch taskInfo.module {
	case "data":
		info.RType = RebalanceData
		info.PartitionBatchCount = taskInfo.volDpCurrency
		info.HighRatio = taskInfo.maxDstDatanodeUsage
	case "meta":
		info.RType = RebalanceMeta
		info.PartitionBatchCount = taskInfo.volMpCurrency
		info.HighRatio = taskInfo.maxDstMetanodeUsage
		info.DstMetaNodePartitionMaxCount = taskInfo.maxDstMetaPartitionCnt
	}
	err = rw.dbHandle.Table(RebalancedInfoTable{}.TableName()).Create(&info).Error
	if err != nil {
		log.LogErrorf("insertMigrateTask failed: taskInfo(%v) err(%v)", taskInfo, err)
	}
	id = info.ID
	return
}

type VolumeMigrateTable struct {
	ID          uint64    `gorm:"column:id"`
	ClusterName string    `gorm:"column:cluster_name"`
	VolName     string    `gorm:"column:volume"`
	SrcZone     string    `gorm:"column:src_zone"`
	DstZone     string    `gorm:"column:dst_zone"`
	Module      string    `gorm:"column:module"`
	Partitions  int       `gorm:"column:partition_count"`
	Status      Status    `gorm:"column:status"` // 1-完成 2-进行中 3-终止
	TaskId      uint64    `gorm:"column:task_id"`
	CreatedAt   time.Time `gorm:"column:created_at"`
	UpdateAt    time.Time `gorm:"column:updated_at"`
}

func (VolumeMigrateTable) TableName() string {
	return "volume_migrate"
}

func (rw *ReBalanceWorker) GetVolMigrateRecord(taskID uint64, volname string) (info *VolumeMigrateTable, err error) {
	info = new(VolumeMigrateTable)
	err = rw.dbHandle.Table(VolumeMigrateTable{}.TableName()).
		Where("task_id = ? and volume = ?", taskID, volname).
		First(&info).Error
	if err != nil {
		log.LogWarnf("GetVolMigrateRecord failed: taskID(%v) vol(%v) err(%v)", taskID, volname, err)
	}
	return
}

// 更新状态 进行中/完成/终止， for进度状态： 已完成的个数 进行中的个数  未开始
// taskID-volname 建立索引
// insert赋值create_time, update更新update_time
func (rw *ReBalanceWorker) insertOrUpdateVolMigrateTask(taskID uint64, cluster, volName, srcZone, dstZone, module string, partitionsNum int, status Status) (id uint64, err error) {
	var info *VolumeMigrateTable
	info, err = rw.GetVolMigrateRecord(taskID, volName)
	if err != nil {
		if err.Error() != RECORD_NOT_FOUND {
			return 0, err
		} else {
			info = new(VolumeMigrateTable)
		}
	}
	info.Status = status
	if info.ID > 0 {
		info.Partitions = partitionsNum
		info.UpdateAt = time.Now()
		err = rw.dbHandle.Table(VolumeMigrateTable{}.TableName()).Updates(info).Error
	} else {
		info.Status = status
		info.ClusterName = cluster
		info.VolName = volName
		info.SrcZone = srcZone
		info.DstZone = dstZone
		info.TaskId = taskID
		info.Module = module
		info.Partitions = partitionsNum
		info.CreatedAt = time.Now()
		info.UpdateAt = info.CreatedAt
		err = rw.dbHandle.Table(VolumeMigrateTable{}.TableName()).Create(info).Error
	}
	return info.ID, err
}

// 没有状态，只是插入记录
func (rw *ReBalanceWorker) insertPartitionMigRecords(record *MigrateRecordTable) error {
	err := rw.dbHandle.Table(MigrateRecordTable{}.TableName()).Create(record).Error
	if err != nil {
		log.LogErrorf("insertPartitionMigRecords failed: record(%v) err(%v)", record, err)
	}
	return err
}

// 更新vol任务的状态
func (rw *ReBalanceWorker) updateVolRecordStatus(id uint64, status Status) error {
	err := rw.dbHandle.Table(VolumeMigrateTable{}.TableName()).
		Where("id = ?", id).
		Update("status", status).Error
	if err != nil {
		log.LogErrorf("updateVolRecordStatus failed: id(%v) err(%v)", id, err)
	}
	return err
}

// 批量更新vol任务的状态
func (rw *ReBalanceWorker) updateVolRecordsStatus(ids []uint64, status Status) error {
	err := rw.dbHandle.Table(VolumeMigrateTable{}.TableName()).
		Where("id IN ?", ids).
		Update("status", status).Error
	if err != nil {
		log.LogErrorf("updateVolRecordsStatus failed: err(%v)", err)
	}
	return err
}

// 查询vol迁移的dp迁移记录
func (rw *ReBalanceWorker) queryVolMigPartitionRecords(taskID uint64, volName string) ([]*MigrateRecordTable, int, error) {
	records := make([]*MigrateRecordTable, 0)
	err := rw.dbHandle.Table(MigrateRecordTable{}.TableName()).
		Where("task_id = ? and vol_name = ?", taskID, volName).
		Find(&records).Error
	if err != nil {
		log.LogWarnf("queryVolMigPartitionRecords failed: taskID(%v) vol(%v)", taskID, volName)
	}
	return records, len(records), err
}

// 查询迁移了多少个dp
func (rw *ReBalanceWorker) getVolMigPartitionCount(taskID uint64, volName string) (total int, err error) {
	records := make([]*MigrateRecordTable, 0)
	err = rw.dbHandle.Table(MigrateRecordTable{}.TableName()).
		Where("task_id = ? and vol_name = ?", taskID, volName).
		Group("partition_id").
		Find(&records).Error
	if err != nil {
		log.LogErrorf("getVolMigPartitionCount failed: taskID(%v) vol(%v) err(%v)", taskID, volName, err)
		return
	}
	total = len(records)
	return
}

// 查询当前任务相关的vol记录
func (rw *ReBalanceWorker) getVolRecordByStatus(taskId uint64, status Status) ([]*VolumeMigrateTable, error) {
	records := make([]*VolumeMigrateTable, 0)
	err := rw.dbHandle.Table(VolumeMigrateTable{}.TableName()).
		Where("task_id = ? AND status = ?", taskId, status).
		Find(&records).Error
	if err != nil {
		log.LogErrorf("getVolRecordByStatus failed: task(%v) err(%v)", taskId, err)
	}
	return records, err
}
