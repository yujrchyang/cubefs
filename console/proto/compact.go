package proto

import (
	"fmt"
	"github.com/cubefs/cubefs/util/log"
	"strconv"
	"strings"
	"time"
)

type MigrateConfig struct {
	Id            uint64    `gorm:"column:id"`
	ClusterName   string    `gorm:"column:cluster"`
	VolName       string    `gorm:"column:volume"`
	Smart         int       `gorm:"column:smart"`
	SmartRules    string    `gorm:"column:smart_rules"`
	SsdDirs       string    `gorm:"column:ssd_dirs"`
	HddDirs       string    `gorm:"column:hdd_dirs"`
	Compact       int       `gorm:"column:compact"`
	MigrationBack int       `gorm:"column:migrate_back"`
	CreateTime    time.Time `gorm:"column:create_time"`
	UpdateTime    time.Time `gorm:"column:update_time"`
}

func (mc MigrateConfig) String() string {
	return fmt.Sprintf("{vol: %v, smart: %v, rules: %v, hddDirs: %v, compact: %v, migBack: %v}", mc.VolName,
		mc.Smart, mc.SmartRules, mc.HddDirs, mc.Compact, mc.MigrationBack)
}

type MigrateConfigList struct {
	Data  []*MigrateConfigView
	Total int
}

type MigrateConfigView struct {
	Id          uint64
	Cluster     string
	Volume      string
	Smart       int // 前端沟通 显示开启关闭
	Compact     int
	MigrateBack int
	Rules       string
	HddDirs     string
	CreateAt    string
	UpdateAt    string
	// todo: 冷热介质数据占比
}

func FormatMigrateConfigView(config *MigrateConfig) *MigrateConfigView {
	view := &MigrateConfigView{
		Id:          config.Id,
		Cluster:     config.ClusterName,
		Volume:      config.VolName,
		Smart:       config.Smart,
		Compact:     config.Compact,
		MigrateBack: config.MigrationBack,
		HddDirs:     config.HddDirs,
		CreateAt:    config.CreateTime.Format(time.DateTime),
		UpdateAt:    config.UpdateTime.Format(time.DateTime),
	}
	view.Rules = formatSmartRules(config.SmartRules)
	return view
}

type RulesType int

const (
	RulesNoType RulesType = iota
	RulesOneDayType
	RulesFiveDayType
	RulesTenDayType
	RulesOneMonthType
)

type RulesUnit int

const (
	RulesUnitSecond RulesUnit = iota
	RulesUnitDay
	RulesUnitTimeStamp
)

const (
	rulesSuffix    = ":hdd"
	rulesPrefix    = "inodeAccessTime:"
	suffix         = "未访问过"
	tsSuffix       = "以来"
	intervalPrefix = "近"
)

func formatSmartRules(rules string) string {
	rulesField := strings.Split(rules, ":")
	if len(rulesField) != 4 {
		log.LogErrorf("错误的rules规则：%v", rules)
		return rules
	}
	var rulesStr string
	switch rulesField[1] {
	case "sec":
		rulesStr += intervalPrefix + rulesField[2] + "秒" + suffix
	case "days":
		rulesStr += intervalPrefix + rulesField[2] + "天" + suffix
	case "timestamp":
		// 时间戳解析成字符串
		ts, err := strconv.ParseInt(rulesField[2], 10, 64)
		if err != nil {
			return ""
		}
		rulesStr += time.Unix(ts, 0).Format(time.DateTime) + tsSuffix + suffix
	}
	return rulesStr
}

func ParseSmartRules(rulesType RulesType, interval int64, rulesUnit RulesUnit) (string, error) {
	var smartRules string
	smartRules += rulesPrefix
	switch rulesType {
	case RulesNoType:
		if interval > 0 && rulesUnit <= RulesUnitTimeStamp {
			switch rulesUnit {
			case RulesUnitSecond:
				smartRules += "sec:" + strconv.FormatInt(interval, 10) + rulesSuffix
			case RulesUnitDay:
				smartRules += "days:" + strconv.FormatInt(interval, 10) + rulesSuffix
			case RulesUnitTimeStamp:
				smartRules += "timestamp:" + strconv.FormatInt(interval, 10) + rulesSuffix
			}
		} else {
			return "", fmt.Errorf("错误输入！无法解析规则！")
		}

	case RulesOneDayType:
		smartRules += "days:" + "1" + rulesSuffix
	case RulesFiveDayType:
		smartRules += "days:" + "5" + rulesSuffix
	case RulesTenDayType:
		smartRules += "days:" + "10" + rulesSuffix
	case RulesOneMonthType:
		smartRules += "days:" + "30" + rulesSuffix
	default:
		return "", fmt.Errorf("不支持的迁移规则！")
	}
	return smartRules, nil
}
