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
		// todo: 测试环境就几个vol  batch 插入这么慢 不合理，135368.318ms
		if err = cutil.CONSOLE_DB.Table(ConsoleVolume{}.TableName()).CreateInBatches(records[i:end], len(records[i:end])).Error; err != nil {
			log.LogWarnf("StoreVolumeRecords: batch insert failed: %v", err)
		}
	}
	return
}

func UpdateVolumeOps(records []*ConsoleVolume) (err error) {
	now := time.Now()
	updateTime := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.Local)
	for _, record := range records {
		err = cutil.CONSOLE_DB.Table(volumeHistoryTableName).Where("cluster = ? AND volume = ? AND update_time = ?", record.Cluster, record.Volume, updateTime).
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

// load cluster all volumeInfo for history table
func (table ConsoleVolume) LoadVolumeInfoByCluster(cluster string, date time.Time) (records []*ConsoleVolume, err error) {
	records = make([]*ConsoleVolume, 0)
	err = cutil.CONSOLE_DB.Table(table.TableName()).Where("cluster = ? AND update_time = ?", cluster, date).
		Find(&records).Error
	return
}

// 近3月没有请求
func (table ConsoleVolume) LoadZombieVols(cluster string) (records []*ConsoleVolume, err error) {
	records = make([]*ConsoleVolume, 0)
	startTime := time.Now().AddDate(0, -zombieVolumeQueryMonth, 0).Format(time.DateTime)
	sqlStr := fmt.Sprintf("" +
		"select " +
		"cluster, volume, sum(ops) as ops " +
		"from " +
		"console_volume_history_info " +
		"where " +
		"cluster = ? " +
		"and update_time >= ? " +
		"and period_mode = 0 " +
		"group by " +
		"cluster, volume " +
		"having ops < 10 ",
	)
	// raw 后的cluster不起作用
	err = cutil.CONSOLE_DB.Raw(sqlStr, cluster, startTime).Scan(&records).Error
	return
}

func (table ConsoleVolume) LoadNoDeleteVol(cluster string) (records []*ConsoleVolume, err error) {
	// 近20天 无删除 但有写
	records = make([]*ConsoleVolume, 0)
	startTime := time.Now().AddDate(0, 0, -noDeleteVolumeQueryDay).Format(time.DateTime)
	sqlStr := fmt.Sprintf("" +
		"select " +
		"cluster, volume, sum(create_inode_ops) as create_inode_ops, sum(evict_inode_ops) as evict_inode_ops " +
		"from " +
		"console_volume_history_info " +
		"where " +
		"cluster = ? " +
		"and period_mode = 0 " +
		"and evict_inode_ops = 0 " +
		"and update_time >= ? " +
		"group by " +
		"cluster, volume " +
		"having create_inode_ops > 0",
	)
	err = cutil.CONSOLE_DB.Raw(sqlStr, cluster, startTime).Scan(&records).Error
	if err != nil {
		log.LogErrorf("LoadNoDeleteVol: cluster: %v, err: %v", cluster, err)
	}
	return
}

func CleanExpiredVolumeOps(timeStr string) error {
	return cutil.CONSOLE_DB.Where("update_time < ?", timeStr).Delete(&ConsoleVolume{}).Error
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

func LoadZombieVolDetails(cluster string) ([]*ConsoleVolume, error) {
	result := make([]*ConsoleVolume, 0)
	startTime := time.Now().AddDate(0, -zombieVolumeQueryMonth, 0).Format(time.DateTime)
	sqlStr := fmt.Sprintf("" +
		"select " +
		"a.cluster as cluster, a.volume as volume, sum(a.ops) as ops, a.total_gb as total_gb, a.used_gb as used_gb, " +
		"b.owner_id as owner_id, b.department as department, b.create_time as create_time " +
		"from console_volume_history_info a " +
		"left join storage_sre.chubaofs_volume_info_table b on a.cluster = b.cluster_name and a.volume = b.volume_name " +
		"where a.period_mode = 0 " +
		"and a.cluster = ? " +
		"and a.update_time >= ? " +
		"group by a.cluster, a.volume " +
		"having sum(a.ops) < 10 " +
		"order by ops",
	)
	if err := cutil.CONSOLE_DB.Raw(sqlStr, cluster, startTime).Scan(&result).Error; err != nil {
		log.LogErrorf("LoadZombieVolDetails failed: cluster[%s] err: %v", cluster, err)
		return nil, err
	}
	return result, nil
}

func LoadNoDeletedVolDetails(cluster string) ([]*ConsoleVolume, error) {
	result := make([]*ConsoleVolume, 0)
	startTime := time.Now().AddDate(0, 0, -noDeleteVolumeQueryDay).Format(time.DateTime)
	sqlStr := fmt.Sprintf("" +
		"select " +
		"a.cluster as cluster, a.volume as volume, sum(a.create_inode_ops) as create_inode_ops, sum(a.evict_inode_ops) as evict_inode_ops, " +
		"a.total_gb as total_gb, a.used_gb as used_gb, b.owner_id as owner_id, b.department as department, b.create_time as create_time " +
		"from console_volume_history_info a " +
		"left join storage_sre.chubaofs_volume_info_table b on a.cluster = b.cluster_name and a.volume = b.volume_name " +
		"where a.period_mode = 0 " +
		"and a.cluster = ? " +
		"and a.evict_inode_ops = 0 " +
		"and a.update_time >= ? " +
		"group by a.cluster, a.volume " +
		"having create_inode_ops > 0 " +
		"order by create_inode_ops DESC",
	)
	if err := cutil.CONSOLE_DB.Raw(sqlStr, cluster, startTime).Scan(&result).Error; err != nil {
		log.LogErrorf("LoadZombieVolDetails failed: cluster[%s] err: %v", cluster, err)
		return nil, err
	}
	return result, nil
}

// todo: how long has no client considered it a no-client-vol
// cluster volume 容量 使用量 客户端个数 pin 部门 创建时间
func LoadNoClientVolDetails(cluster string, keywords *string) ([]*ConsoleVolume, error) {
	result := make([]*ConsoleVolume, 0)
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

var (
	volumeHistoryTableName = "console_volume_history_info"
)

func StoreVolumeHistoryInfo(records []*ConsoleVolume) {
	length := len(records)
	for i := 0; i < length; i += maxInsertBatchNum {
		end := i + maxInsertBatchNum
		if end > length {
			end = length
		}
		if err := cutil.CONSOLE_DB.Table(volumeHistoryTableName).CreateInBatches(records[i:end], len(records[i:end])).Error; err != nil {
			log.LogWarnf("StoreVolumeHistoryInfo: batch insert failed: %v", err)
		}
	}
	return
}

func LoadVolumeHistoryData(cluster, volume string, start, end time.Time) (result []*VolumeHistoryCurve, err error) {
	result = make([]*VolumeHistoryCurve, 0)
	err = cutil.CONSOLE_DB.Table(volumeHistoryTableName).Select("cluster, volume, used_gb, used_ratio, inode_count, writable_dp_num, DATE_FORMAT(update_time, '%Y-%m-%d %H:%i:%s') as update_time").
		Where("cluster = ? AND volume = ? AND update_time >= ? AND update_time <= ?", cluster, volume, start, end).
		Group("cluster, volume, update_time").
		Order("update_time ASC").
		Limit(MaxHistoryDataNum).
		Find(&result).Error
	return
}

func CleanExpiredVolumeHistoryInfo(timeStr string) error {
	return cutil.CONSOLE_DB.Table(volumeHistoryTableName).Where("update_time < ?", timeStr).Delete(&ConsoleVolume{}).Error
}
