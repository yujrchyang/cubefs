package model

import (
	"fmt"
	"github.com/cubefs/cubefs/console/cutil"
	"github.com/cubefs/cubefs/util/log"
	"time"
)

// cli 操作表中增加字段 ticket_ID 执行结果(isSuccess) errMsg

type XbpApplyInfo struct {
	ID              uint   `gorm:"column:id;primary_key;AUTO_INCREMENT"`
	TicketID        uint64 `gorm:"column:ticket_id"`
	Cluster         string `gorm:"column:cluster"`
	Volume          string `gorm:"column:volume"`
	Host            string `gorm:"column:host"`
	PartitionID     uint64 `gorm:"column:pid"`
	ModuleType      int    `gorm:"column:module_type"`
	Module          string `gorm:"column:module"`
	OperationCode   int    `gorm:"column:opcode"`
	Operation       string `gorm:"column:operation"`
	OperationIsList bool   `gorm:"column:isList"`
	Params          string `gorm:"column:params"`
	Pin             string `gorm:"column:pin"`
	Approver        string `gorm:"column:approver"`
	Status          int    `gorm:"column:status;default:0"`
	ServiceType     int    `gorm:"column:service_type"`

	CreateTime time.Time `gorm:"column:create_time;default:CURRENT_TIMESTAMP"`
	UpdateTime time.Time `gorm:"column:update_time;default:NULL"`
}

func (XbpApplyInfo) TableName() string {
	return "xbp_apply_record"
}

func (table XbpApplyInfo) String() string {
	return fmt.Sprintf("\n"+
		"apply {\n"+
		"ticket_id: %v, \n"+
		"pin: %v, \n"+
		"cluster: %s, \n"+
		"module: %s, \n"+
		"operation: %s, \n"+
		"params: %s \n"+
		"} \n",
		table.TicketID, table.Pin, table.Cluster, table.Module, table.Operation, table.Params)
}

func (table XbpApplyInfo) InsertXbpApply(apply *XbpApplyInfo) {
	if err := cutil.CONSOLE_DB.Table(table.TableName()).Create(&apply).Error; err != nil {
		log.LogErrorf("InsertXbpApply failed: %v, err: %v", apply, err)
	}
}

func (table XbpApplyInfo) LoadXbpApply(ticketID uint64) *XbpApplyInfo {
	record := new(XbpApplyInfo)
	res := cutil.CONSOLE_DB.Table(table.TableName()).Where("ticket_id = ?", ticketID).
		Limit(1).Scan(&record)
	if res.Error != nil {
		log.LogErrorf("LoadXbpApply failed: ticketID[%v], err(%v)", ticketID, res.Error)
		return nil
	}
	if res.RowsAffected == 0 {
		log.LogErrorf("LoadXbpApply: can't find apply record by ticketID[%v]", ticketID)
		return nil
	}
	return record
}

func (table XbpApplyInfo) UpdateApplyStatus(ticketID uint64, status int) {
	updateTime := time.Now()
	if err := cutil.CONSOLE_DB.Table(table.TableName()).
		Where("ticket_id = ?", ticketID).
		Updates(map[string]interface{}{
			"status":      status,
			"update_time": updateTime,
		}).Error; err != nil {
		log.LogErrorf("UpdateApplyStatus failed: ticketID[%v] status[%v]", ticketID, status)
	}
}

func (table XbpApplyInfo) GetXbpApplyRecordsCount(cluster, module, operation string) (count int64, err error) {
	err = cutil.CONSOLE_DB.Table(table.TableName()).
		Where("cluster = ? AND module = ? AND operation = ?", cluster, module, operation).
		Count(&count).
		Error
	if err != nil {
		log.LogErrorf("GetXbpApplyRecordsCount failed: cluster(%v) module(%v) operation(%v) err(%v)", cluster, module, operation, err)
	}
	return
}

func (table XbpApplyInfo) GetXbpApplyRecord(cluster, module, operation string, pageNum, pageSize int) (results []*XbpApplyInfo, err error) {
	err = cutil.CONSOLE_DB.Table(table.TableName()).
		Where("cluster = ? AND module = ? AND operation = ?", cluster, module, operation).
		Order("create_time DESC").
		Offset((pageNum - 1) * pageSize).
		Limit(pageSize).
		Find(&results).Error
	if err != nil {
		log.LogErrorf("GetXbpApplyRecord failed: cluster(%v) module(%v) operation(%v) err(%v)", cluster, module, operation, err)
	}
	return
}
