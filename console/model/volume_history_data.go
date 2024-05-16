package model

import "time"

type VolumeHistoryData struct {
	ID                    uint      `json:"id" gorm:"column:id;primary_key;AUTO_INCREMENT"`
	ClusterName           string    `json:"clusterName" gorm:"column:cluster_name;comment:集群英文名称"`
	VolumeName            string    `json:"volumeName" gorm:"column:volume_name;comment:集群编号"`
	TotalGB               uint64    `json:"totalGB" gorm:"column:total_gb;comment:卷总容量"`
	UsedGB                uint64    `json:"usedGB" gorm:"column:used_gb;comment:卷使用容量"`
	DataPartitionNum      int       `json:"dpNum" gorm:"column:data_partition_num;comment:数据分片个数"`
	MetaPartitionNum      int       `json:"mpNum" gorm:"column:meta_partition_num;comment:元数据分片个数"`
	ClientNum             uint64    `json:"clientNum" gorm:"column:client_num;comment:卷总容量"`
	IsCrossZone           bool      `json:"isCrossZone" gorm:"column:is_cross_zone;comment:是否跨机房"`
	FollowerRead          bool      `json:"isFollowerRead" gorm:"column:follower_read;comment:是否允许从副本读取数据"`
	OwnerId               string    `json:"ownerId" gorm:"column:owner_id;comment:所属人"`
	Email                 string    `json:"email" gorm:"column:email;comment:所属人邮箱"`
	Department            string    `json:"department" gorm:"column:department;comment:所属部门"`
	FirstLevelDepartment  string    `json:"-" gorm:"column:org_first_level;comment:一级部门"`
	SecondLevelDepartment string    `json:"-" gorm:"column:org_second_level;comment:二级部门"`
	ThirdLevelDepartment  string    `json:"-" gorm:"column:org_third_level;comment:三级部门"`
	ApproveBy             string    `json:"approveBy" gorm:"column:approve_by;comment:审批人"`
	ApplicationName       string    `json:"-" gorm:"column:application_name;comment:JDOS应用名称"`
	SystemName            string    `json:"-" gorm:"column:system_name;comment:JDOS系统名称"`
	CreateTime            string    `json:"createTime" gorm:"column:create_time;comment:创建时间"`
	UpdateTime            time.Time `json:"-" gorm:"column:update_time;comment:更新时间"`
	ZoneName              string    `json:"zoneName" gorm:"column:zone_name;comment:zone_name"`
	Source                string    `json:"source" gorm:"column:source;comment:来源"`
}

func (VolumeHistoryData) TableName() string {
	return "chubaofs_volume_history_data"
}
