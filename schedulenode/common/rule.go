package common

import (
	"github.com/cubefs/cubefs/proto"
	"strconv"
	"strings"
)

type SpecialOwnerCheckRule struct {
	CheckIntervalMin     uint64
	SafeCleanIntervalMin uint64
}

func ParseSpecialOwnerRules(rules []*proto.CheckRule) (specialCheckRules map[string]*SpecialOwnerCheckRule)  {
	specialCheckRules = make(map[string]*SpecialOwnerCheckRule)
	var err error
	for _, rule := range rules {
		if rule.RuleType != "check_special_owner" { //对一些owner的volume配置特殊的检查逻辑
			continue
		}
		if rule.RuleValue == "" {
			continue
		}
		ruleInfoArr := strings.Split(rule.RuleValue, "_")
		if len(ruleInfoArr) != 3 {
			continue
		}

		specialOwner := ruleInfoArr[0]
		checkRule := new(SpecialOwnerCheckRule)
		if checkRule.CheckIntervalMin, err = strconv.ParseUint(ruleInfoArr[1], 10, 64); err != nil {
			continue
		}

		if checkRule.SafeCleanIntervalMin, err = strconv.ParseUint(ruleInfoArr[2], 10, 64); err != nil {
			continue
		}
		specialCheckRules[specialOwner] = checkRule
	}
	return
}

func ParseCheckAllRules(rules []*proto.CheckRule) (checkAll bool, checkVolumes []string,
	enableCheckOwners, disableCheckOwners map[string]byte, skipVolumes map[string]byte) {
	skipVolumes = make(map[string]byte, 0)
	enableCheckOwners = make(map[string]byte, 0)
	disableCheckOwners = make(map[string]byte, 0)
	for _, rule := range rules {
		switch rule.RuleType {
		case "check_all": //是否检查所有的volume
			parseValue, err := strconv.ParseBool(rule.RuleValue)
			if err != nil {
				return
			}
			checkAll = parseValue
		case "enable_check":
			checkVolumes = append(checkVolumes, rule.RuleValue)
		case "enable_check_owner":
			enableCheckOwners[rule.RuleValue] = 0
		case "disable_check":
			skipVolumes[rule.RuleValue] = 0
		case "disable_check_owner":
			disableCheckOwners[rule.RuleValue] = 0
		}
	}
	return
}
