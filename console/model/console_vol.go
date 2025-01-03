package model

import (
	"fmt"
	"time"

	"github.com/cubefs/cubefs/console/cutil"
	"github.com/cubefs/cubefs/util/log"
)

const (
	maxInsertBatchNum      = 1000
	zombieVolumeQueryMonth = 3
	noDeleteVolumeQueryDay = 20
)

type ZombieVolPeriod int

const (
	ZombiePeriodDay ZombieVolPeriod = iota
	ZombiePeriodMonth
)

type ConsoleVolumeOps struct {
	ID             uint64    `gorm:"column:id"`
	Cluster        string    `gorm:"column:cluster"`
	Zone           string    `gorm:"column:zone"`
	Source         string    `gorm:"column:source"`
	Volume         string    `gorm:"column:volume"`
	TotalGB        uint64    `gorm:"column:total_gb"`
	UsedGB         uint64    `gorm:"column:used_gb"`
	Ops            uint64    `gorm:"column:ops"`
	CreateInodeOps uint64    `gorm:"column:create_inode_ops"`
	EvictInodeOps  uint64    `gorm:"column:evict_inode_ops"`
	PeriodMode     int       `gorm:"column:period_mode"`
	OwnerId        string    `gorm:"column:pin"`
	Department     string    `gorm:"column:department"`
	CreateTime     time.Time `gorm:"column:create_time"`
}

func (ConsoleVolumeOps) TableName() string {
	return "console_volume_ops"
}

func BatchInsertVolumeOps(records []*ConsoleVolumeOps) (err error) {
	length := len(records)
	for i := 0; i < length; i += maxInsertBatchNum {
		end := i + maxInsertBatchNum
		if end > length {
			end = length
		}
		if err = cutil.CONSOLE_DB.Table(ConsoleVolumeOps{}.TableName()).CreateInBatches(records[i:end], len(records[i:end])).Error; err != nil {
			log.LogWarnf("BatchInsertVolumeOps: insert failed: %v", err)
		}
	}
	return
}

func LoadZombieVols(cluster string) (records []*ConsoleVolumeOps, err error) {
	records = make([]*ConsoleVolumeOps, 0)
	// 近3月没有请求
	startTime := time.Now().AddDate(0, -zombieVolumeQueryMonth, 0).Format(time.DateTime)
	err = cutil.CONSOLE_DB.Table(ConsoleVolumeOps{}.TableName()).
		Where("create_time > ? AND cluster = ?", startTime, cluster).
		Group("cluster, volume").
		Having("SUM(ops) < 10").
		Scan(&records).Error
	if err != nil {
		log.LogErrorf("LoadZombieVols failed: cluster(%v) start(%v) err(%v)", cluster, startTime, err)
	}
	return
}

func LoadZombieVolDetails(cluster string, pageNum, pageSize int) ([]*ConsoleVolumeOps, error) {
	result := make([]*ConsoleVolumeOps, 0)
	startTime := time.Now().AddDate(0, -zombieVolumeQueryMonth, 0).Format(time.DateTime)
	err := cutil.CONSOLE_DB.Table(ConsoleVolumeOps{}.TableName()).
		Where("create_time > ? AND cluster = ?", startTime, cluster).
		Group("cluster, volume").
		Having("SUM(ops) < 10").
		Order("SUM(ops) ASC").
		Limit(pageSize).Offset((pageNum - 1) * pageSize).
		Find(&result).Error
	if err != nil {
		log.LogErrorf("LoadZombieVolDetails failed: cluster[%s] err: %v", cluster, err)
	}
	return result, err
}

func LoadNoDeleteVol(cluster string) (records []*ConsoleVolumeOps, err error) {
	// 近20天 无删除 但有写
	records = make([]*ConsoleVolumeOps, 0)
	startTime := time.Now().AddDate(0, 0, -noDeleteVolumeQueryDay).Format(time.DateTime)
	err = cutil.CONSOLE_DB.Table(ConsoleVolumeOps{}.TableName()).
		Select("cluster, volume").
		Where("create_time > ? AND cluster = ?", startTime, cluster).
		Group("cluster, volume").
		Having("SUM(evict_inode_ops) < 10 AND SUM(create_inode_ops) > 0").
		Scan(&records).Error
	if err != nil {
		log.LogErrorf("LoadNoDeleteVol failed: cluster(%v) err(%v)", cluster, err)
	}
	return
}

func LoadNoDeletedVolDetails(cluster string, pageNum, pageSize int) ([]*ConsoleVolumeOps, error) {
	result := make([]*ConsoleVolumeOps, 0)
	startTime := time.Now().AddDate(0, 0, -noDeleteVolumeQueryDay).Format(time.DateTime)
	err := cutil.CONSOLE_DB.Table(ConsoleVolumeOps{}.TableName()).
		Where("create_time > ? AND cluster = ?", startTime, cluster).
		Group("cluster, volume").
		Having("SUM(evict_inode_ops) < 10 AND SUM(create_inode_ops) > 0").
		Order("SUM(create_inode_ops) DESC").
		Limit(pageSize).Offset((pageNum - 1) * pageSize).
		Find(&result).Error
	if err != nil {
		log.LogErrorf("LoadNoDeletedVolDetails failed: cluster[%s] err: %v", cluster, err)
	}
	return result, nil
}

// todo: how long has no client considered it a no-client-vol
// cluster volume 容量 使用量 客户端个数 pin 部门 创建时间
func LoadNoClientVolDetails(cluster string, keywords *string) ([]*ConsoleVolumeOps, error) {
	result := make([]*ConsoleVolumeOps, 0)
	sqlStr := fmt.Sprintf("" +
		"select " +
		"a.cluster as cluster, a.volume as volume, a.client_count as client_count, a.update_time as update_time " +
		"b.total_gb as total_gb, b.used_gb as used_gb, b.owner_id as owner_id, b.department as department, b.create_time as create_time " +
		"from console_volume_info " +
		"left join chubaofs_volume_info_table b on a.cluster = b.cluster_name and a.volume = b.volume_name " +
		"where a.cluster = ? AND a.client_count = 0 ")
	if keywords != nil {
		sqlStr += fmt.Sprintf("AND a.volume like '%s'", "%"+*keywords+"%")
	}
	sqlStr += "order by cluster, volume, update_time DESC " +
		"limit 1"
	if err := cutil.SRE_DB.Raw(sqlStr, cluster).Find(&result).Error; err != nil {
		log.LogErrorf("LoadNoClientVolDetails failed: cluster(%v) err(%v)", cluster, err)
		return nil, err
	}
	return result, nil
}

func CleanExpiredVolumeOps(timeStr string) error {
	return cutil.CONSOLE_DB.Where("create_time < ?", timeStr).Delete(&ConsoleVolumeOps{}).Error
}

type ConsoleVolume struct {
	Cluster        string    `gorm:"column:cluster"`
	Volume         string    `gorm:"column:volume"`
	Ops            uint64    `gorm:"column:ops"`
	CreateInodeOps uint64    `gorm:"column:create_inode_ops"`
	EvictInodeOps  uint64    `gorm:"column:evict_inode_ops"`
	PeriodMode     int       `gorm:"column:period_mode"`
	TotalGB        uint64    `gorm:"column:total_gb"`
	UsedGB         uint64    `gorm:"column:used_gb"`
	UsedRatio      float64   `gorm:"column:used_ratio"`
	InodeCount     uint64    `gorm:"column:inode_count"`
	WritableDpNum  int       `gorm:"column:writable_dp_num"`
	ClientCount    int       `gorm:"column:client_count"`
	OwnerId        string    `gorm:"column:owner_id"`
	Department     string    `gorm:"column:department"`
	CreateTime     string    `gorm:"column:create_time"`
	UpdateTime     time.Time `gorm:"column:update_time"`
	// 新增字段
	WritableMpNum int    `gorm:"column:writable_mp_num"`
	MpNum         int    `gorm:"column:mp_num"`
	Zone          string `gorm:"column:zone"`
	DpReplicas    int    `gorm:"column:dp_replica_num"`
	Pin           string `gorm:"column:pin"`
	Source        string `gorm:"column:source"`
}

func (ConsoleVolume) TableName() string {
	return "console_volume_info"
}

func (zv *ConsoleVolume) String() string {
	return fmt.Sprintf("{volName: %v total_gb: %v used_gb: %v owner_id: %v department: %v create_time: %v}",
		zv.Volume, zv.TotalGB, zv.UsedGB, zv.OwnerId, zv.Department, zv.CreateTime)
}

func StoreVolumeRecords(records []*ConsoleVolume) (err error) {
	length := len(records)
	for i := 0; i < length; i += maxInsertBatchNum {
		end := i + maxInsertBatchNum
		if end > length {
			end = length
		}
		if err = cutil.CONSOLE_DB.Table(ConsoleVolume{}.TableName()).CreateInBatches(records[i:end], len(records[i:end])).Error; err != nil {
			log.LogWarnf("StoreVolumeRecords: batch insert failed: %v", err)
		}
	}
	return
}

func UpdateVolumeOps(records []*ConsoleVolumeOps) (err error) {
	now := time.Now()
	updateTime := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.Local)
	for _, record := range records {
		err = cutil.CONSOLE_DB.Table(VolumeHistoryTableName).Where("cluster = ? AND volume = ? AND update_time = ?", record.Cluster, record.Volume, updateTime).
			Updates(map[string]interface{}{
				"ops":              record.Ops,
				"create_inode_ops": record.CreateInodeOps,
				"evict_inode_ops":  record.EvictInodeOps,
			}).Error
		if err != nil {
			log.LogWarnf("UpdateVolumeOps: cluster(%v) volume(%v) err(%v)", record.Cluster, record.Volume, err)
		}
	}
	return nil
}

// load Volume info curve
func (table ConsoleVolume) LoadVolumeInfo(cluster, volume string, start, end time.Time) (records []*VolumeHistoryCurve, err error) {
	records = make([]*VolumeHistoryCurve, 0)
	err = cutil.CONSOLE_DB.Table(table.TableName()).Select("cluster, volume, total_gb, used_gb, used_ratio, inode_count, writable_dp_num, DATE_FORMAT(update_time, '%Y-%m-%d %H:%i:%s') as update_time").
		Where("cluster = ? AND volume = ? AND update_time >= ? AND update_time <= ?", cluster, volume, start, end).
		Group("cluster, volume, update_time").
		Order("update_time ASC").
		Limit(MaxHistoryDataNum).
		Find(&records).Error
	return
}

func (table ConsoleVolume) GetVolumeClientCount(cluster, volume string) int64 {
	var record *ConsoleVolume
	end := time.Now()
	start := time.Now().Add(-10 * time.Minute)
	err := cutil.CONSOLE_DB.Table(table.TableName()).
		Where("cluster = ? AND volume = ? AND update_time > ? AND update_time <= ?", cluster, volume, start, end).
		Group("cluster, volume, update_time").
		Order("update_time DESC").
		Limit(1).
		Scan(&record).Error
	if err != nil {
		log.LogErrorf("GetVolumeClientCount failed: cluster(%v) vol(%v) err(%v)", cluster, volume, err)
		return 0
	}
	if record == nil {
		return 0
	}
	return int64(record.ClientCount)
}

func (table ConsoleVolume) LoadVolumeInfoByCluster(cluster string, date time.Time) (records []*ConsoleVolume, err error) {
	records = make([]*ConsoleVolume, 0)
	err = cutil.CONSOLE_DB.Table(table.TableName()).Where("cluster = ? AND update_time = ?", cluster, date).
		Find(&records).Error
	return
}

func CleanExpiredVolumeInfo(start, end, cluster string) error {
	err := cutil.CONSOLE_DB.Where("update_time >= ? AND update_time < ? AND cluster = ?", start, end, cluster).
		Delete(&ConsoleVolume{}).Error
	if err != nil {
		log.LogErrorf("CleanExpiredVolumeInfo failed: start(%v) end(%v) err(%v)", start, end, err)
	}
	return err
}

type VolumeInfoTable struct {
	ID               uint      `json:"id" gorm:"column:id;primary_key;AUTO_INCREMENT"`
	ClusterName      string    `json:"clusterName" gorm:"column:cluster_name;comment:集群英文名称"`
	VolumeName       string    `json:"volumeName" gorm:"column:volume_name;comment:卷名"`
	TotalGB          uint64    `json:"totalGB" gorm:"column:total_gb;comment:卷总容量"`
	UsedGB           uint64    `json:"usedGB" gorm:"column:used_gb;comment:卷使用容量"`
	DataPartitionNum int       `json:"dpNum" gorm:"column:data_partition_num;comment:数据分片个数"`
	MetaPartitionNum int       `json:"mpNum" gorm:"column:meta_partition_num;comment:元数据分片个数"`
	ClientNum        uint64    `json:"clientNum" gorm:"column:client_num;comment：客户端数"`
	IsCrossZone      bool      `json:"isCrossZone" gorm:"column:is_cross_zone;comment:是否跨机房"`
	FollowerRead     bool      `json:"isFollowerRead" gorm:"column:follower_read;comment:是否允许从副本读取数据"`
	OwnerId          string    `json:"ownerId" gorm:"column:owner_id;comment:所属人"`
	Email            string    `json:"email" gorm:"column:email;comment:所属人邮箱"`
	Department       string    `json:"department" gorm:"column:department;comment:所属部门"`
	CreateTime       string    `json:"createTime" gorm:"column:create_time;comment:创建时间"`
	UpdateTime       time.Time `json:"-" gorm:"column:update_time;comment:更新时间"`
	ZoneName         string    `json:"zoneName" gorm:"column:zone_name;comment:zone_name"`
	Source           string    `json:"source" gorm:"column:source;comment:来源"`
}

func (VolumeInfoTable) TableName() string {
	return "chubaofs_volume_info_table"
}

func (table VolumeInfoTable) GetVolumeInfo(cluster string, volumes []string) ([]*VolumeInfoTable, error) {
	res := make([]*VolumeInfoTable, 0)
	if err := cutil.SRE_DB.Table(table.TableName()).Raw(""+
		"select "+
		"volume_name, total_gb, used_gb, owner_id, department, create_time "+
		"from "+
		"chubaofs_volume_info_table ",
	).Where("cluster_name = ? AND volume_name in (?)", cluster, volumes).Scan(&res).Error; err != nil {
		return nil, err
	}
	return res, nil
}

var (
	VolumeHistoryTableName = "console_volume_history_info"
)

func StoreVolumeHistoryInfo(records []*ConsoleVolume) {
	length := len(records)
	for i := 0; i < length; i += maxInsertBatchNum {
		end := i + maxInsertBatchNum
		if end > length {
			end = length
		}
		if err := cutil.CONSOLE_DB.Table(VolumeHistoryTableName).CreateInBatches(records[i:end], len(records[i:end])).Error; err != nil {
			log.LogWarnf("StoreVolumeHistoryInfo: batch insert failed: %v", err)
		}
	}
	return
}

func LoadVolumeHistoryData(cluster, volume string, start, end time.Time) (result []*VolumeHistoryCurve, err error) {
	result = make([]*VolumeHistoryCurve, 0)
	err = cutil.CONSOLE_DB.Table(VolumeHistoryTableName).Select("cluster, volume, used_gb, used_ratio, inode_count, writable_dp_num, DATE_FORMAT(update_time, '%Y-%m-%d %H:%i:%s') as update_time").
		Where("cluster = ? AND volume = ? AND update_time >= ? AND update_time <= ?", cluster, volume, start, end).
		Group("cluster, volume, update_time").
		Order("update_time ASC").
		Limit(MaxHistoryDataNum).
		Find(&result).Error
	return
}

func CleanExpiredVolumeHistoryInfo(cluster, timeStr string) error {
	err := cutil.CONSOLE_DB.Table(VolumeHistoryTableName).
		Where("update_time <= ? AND cluster = ?", timeStr, cluster).
		Delete(&ConsoleVolume{}).Error
	if err != nil {
		log.LogErrorf("MigrateVolumeHistoryData: clean expired records failed: cluster(%v) expireTime(%v) err(%v)", cluster, timeStr, err)
	}
	return err
}
