package model

import (
	"github.com/cubefs/cubefs/console/cutil"
	"github.com/cubefs/cubefs/util/log"
	"time"
)

type CheckCompactFragRecord struct {
	ID                 uint64            `gorm:"column:id"`
	Request            *CheckFragRequest `gorm:"embedded"`
	Status             int               `gorm:"column:status"` // 默认为0 完成改为1
	ResultDownloadLink string            `gorm:"column:result_link"`
	CreateTime         time.Time         `gorm:"column:create_time"`
}

func (CheckCompactFragRecord) TableName() string {
	return "compact_check_frag_record"
}

func (table CheckCompactFragRecord) InsertRecord(request *CheckFragRequest) (id uint64, err error) {
	record := &CheckCompactFragRecord{
		Request:    request,
		CreateTime: time.Now(),
	}
	if err = cutil.CONSOLE_DB.Table(table.TableName()).Create(&record).Error; err != nil {
		log.LogErrorf("InsertRecord err(%v) request(%v)", err, request)
		return 0, err
	}
	return record.ID, nil
}

func (table CheckCompactFragRecord) UpdateRecord(id uint64, resultLink string) {
	updates := make(map[string]interface{})
	updates["result_link"] = resultLink
	updates["status"] = 1

	if err := cutil.CONSOLE_DB.Table(table.TableName()).Where("id = ?", id).Updates(updates).Error; err != nil {
		log.LogErrorf("UpdateRecord failed: id(%v) result_link(%v) err(%v)", id, resultLink, err)
	}
	return
}

func (table CheckCompactFragRecord) LoadCompactRecord(cluster, volume *string, id *uint64, page, pageSize int) (records []*CheckCompactFragRecord, err error) {
	// 有什么条件加什么 排序
	records = make([]*CheckCompactFragRecord, 0)
	query := cutil.CONSOLE_DB.Table(table.TableName()).Where("1 = 1")
	if cluster != nil {
		query.Where("cluster = ?", *cluster)
	}
	if volume != nil {
		query.Where("volume = ?", *volume)
	}
	if id != nil {
		query.Where("id = ?", *id)
	}
	query.Order("create_time desc").Limit(pageSize).Offset((page - 1) * pageSize)
	if err = query.Find(&records).Error; err != nil {
		log.LogErrorf("LoadCompactRecord failed: %v %v id(%v) err(%v)", cluster, volume, id, err)
	}
	return
}

type CheckFragRequest struct {
	Cluster          string `gorm:"column:cluster"`
	Volume           string `gorm:"column:volume"`
	EkMinLength      uint64 `gorm:"column:ek_min_length"`
	EkMaxAvgSize     uint64 `gorm:"column:ek_max_avgSize"`
	InodeMinSize     uint64 `gorm:"column:inode_min_size"`
	VolConcurrency   uint64 `gorm:"column:vol_concurrency"`
	MpConcurrency    uint64 `gorm:"column:mp_concurrency"`
	InodeConcurrency uint64 `gorm:"column:inode_concurrency"`
	SaveOverlap      bool
	SaveFragment     bool
	SizeRange        string `gorm:"column:size_range"` //,隔开
}
