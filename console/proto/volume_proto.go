package proto

import (
	"fmt"
	"github.com/cubefs/cubefs/proto"
)

const (
	MaxVolumeListBatch = 1000
)

type VolumeIPInfo struct {
	VolumeName string `json:"VOLUME_NAME"`
	ClientIP   string `json:"CLIENT_IP"`
	Time       string `json:"TIME"`
}

// {"VOLUME_NAME":"3cs-device-pre-ht86a-xx-80","COUNT":2}
type VolumeClientCount struct {
	VolumeName string `json:"VOLUME_NAME"`
	Count      uint64 `json:"COUNT"`
}

type VolInfo struct {
	Name        string // 卷名
	Owner       string // owner
	TotalSize   string // 总容量
	UsedSize    string // 使用量
	UsedRatio   string // 使用率
	FileCount   uint64 // 文件数   -- 需要调接口
	ReplicaNum  uint8  // 副本数   -- 需要调接口
	ClientCount int32  // 客户端数量
	CreateTime  string // 创建时间
	DpCount     int32  // dp数量
	MpCount     int32  // mp数量
}

type VolInfoResponse struct {
	Data  []*VolInfo
	Total int
}

type UserPermission struct {
	UserID  string
	Access  string
	IsOwner bool
}

type Vol struct {
	Name                   string
	Owner                  string
	Capacity               uint64 // 容量
	DpReplicaNum           uint8  // 副本数
	DefaultStoreMode       uint8  //1-内存 2-RocksDB
	TrashRemainingDays     uint8  //回收站天数
	DpSelectorName         string //dpSelector default kfaster
	EnableToken            bool   //token挂载
	ForceROW               bool   //强制ROW
	FollowerRead           bool   //follower读
	RemoteCacheBoostEnable bool   //分布式缓存
	EnableWriteCache       bool   //sdk写缓存
	// todo: more
}

func (v *Vol) String() string {
	return fmt.Sprintf("name:%v owner:%v cap:%v replica:%v storeMode:%v trashDays:%v dpSelector:%v enableToken:%v "+
		"ROW:%v followerRead:%v remoteCache:%v writeCache:%v", v.Name, v.Owner, v.Capacity, v.DpReplicaNum, v.DefaultStoreMode,
		v.TrashRemainingDays, v.DpSelectorName, v.EnableToken, v.ForceROW, v.FollowerRead, v.RemoteCacheBoostEnable, v.EnableWriteCache)
}

type VolDataPartitionsResp struct {
	Total int
	Data  []*proto.DataPartitionsView
}
