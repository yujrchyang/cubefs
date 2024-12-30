package proto

import (
	"testing"
)

func Test_ParseSmartRules(t *testing.T) {
	testCases := []struct {
		name      string
		ruleType  RulesType
		ruleUnit  RulesUnit
		timeValue int64
		wanted    string
	}{
		{
			ruleType:  RulesNoType,
			ruleUnit:  RulesUnitSecond,
			timeValue: 100,
			wanted:    "inodeAccessTime:sec:100:hdd",
		},
		{
			ruleType:  RulesNoType,
			ruleUnit:  RulesUnitTimeStamp,
			timeValue: 177723,
			wanted:    "inodeAccessTime:timestamp:177723:hdd",
		},
	}
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			if actual, err := ParseSmartRules(tt.ruleType, tt.timeValue, tt.ruleUnit); err != nil && actual != tt.wanted {
				t.Errorf("Test_ParseSmartRules: case[%v],expect[%v],but[%v]", tt.name, tt.wanted, actual)
				return
			}
		})
	}
}
