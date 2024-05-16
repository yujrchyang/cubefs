package model

import (
	"github.com/cubefs/cubefs/console/cutil"
	"time"
)

type VolumeClientVersion struct {
	Id               uint64    `gorm:"column:id;comment:自增主键"`
	ClientIp         string    `gorm:"column:client_ip;comment:客户端的IP信息"`
	ClientId         string    `gorm:"column:client_id;comment:里面是ip@毫秒级时间戳"`
	ClientTime       time.Time `gorm:"column:client_time;comment:客户端上报时的时间信息"`
	Version          string    `gorm:"column:version;comment:客户端的版本信息"`
	FilesRead        int64     `gorm:"column:files_read;comment:"`
	ReadByte         int64     `gorm:"column:read_byte;comment:"`
	TotalReadTime    int64     `gorm:"column:total_read_time;comment:"`
	ErrorRead        int64     `gorm:"column:error_read;comment:"`
	FinalReadError   int64     `gorm:"column:final_read_error;comment:"`
	FilesWrite       int64     `gorm:"column:files_write;comment:"`
	WriteByte        int64     `gorm:"column:write_byte;comment:"`
	TotalWriteTime   int64     `gorm:"column:total_write_time;comment:"`
	ErrorWrite       int64     `gorm:"column:error_write;comment:"`
	FinalWriteError  int64     `gorm:"column:final_write_error;comment:"`
	TotalConnections int64     `gorm:"column:total_connections;comment:"`
	ZkAddr           string    `gorm:"column:zk_addr;comment:zk 地址"`
	VolName          string    `gorm:"column:volname;comment:"`
	CommitID         string    `gorm:"column:commit_id;comment:"`
	MountPoint       string    `gorm:"column:mount_point;comment:"`
}

// 客户端版本汇报表
func (VolumeClientVersion) TableName() string {
	return "checktool_jss_sdk_info"
}

func (table VolumeClientVersion) GetVolClientList(volume string) (clientInfos []*VolumeClientVersion, err error) {
	var zkAddr string

	clientInfos = make([]*VolumeClientVersion, 0)
	// and zk_addr in (?)
	if err = cutil.SRE_DB.Table(table.TableName()).Where("volname = ? ", volume, zkAddr).Scan(&clientInfos).Error; err != nil {
		return nil, err
	}
	return clientInfos, nil
}
