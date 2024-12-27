package metanode

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_getMaxApplyIDHosts(t *testing.T) {
	testCases := []struct{
		name		string
		applyID		uint64
		applyIDMap	map[string]uint64
		expectSelf	bool
		expectHosts	[]string
		expectMaxID	uint64
	} {
		{
			name: "test01",
			applyID: 0,
			applyIDMap: map[string]uint64{"192.168.0.21:17020":0, "192.168.0.22:17020":5, "192.168.0.23:17020":1},
			expectSelf: false,
			expectHosts: []string{"192.168.0.22:17020"},
			expectMaxID: 5,
		},
		{
			name: "test02",
			applyID: 0,
			applyIDMap: map[string]uint64{"192.168.0.21:17020":0, "192.168.0.22:17020":0, "192.168.0.23:17020":0},
			expectSelf: true,
			expectHosts: []string{},
			expectMaxID: 0,
		},
		{
			name: "test02",
			applyID: 10,
			applyIDMap: map[string]uint64{"192.168.0.21:17020":0, "192.168.0.22:17020":5, "192.168.0.23:17020":1},
			expectSelf: true,
			expectHosts: []string{},
			expectMaxID: 10,
		},
		{
			name: "test02",
			applyID: 5,
			applyIDMap: map[string]uint64{"192.168.0.21:17020":5, "192.168.0.22:17020":10, "192.168.0.23:17020":10},
			expectSelf: false,
			expectHosts: []string{"192.168.0.23:17020", "192.168.0.22:17020"},
			expectMaxID: 10,
		},
	}
	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			mp := &metaPartition{applyID: tt.applyID}
			isSelf, targetHosts, maxID := mp.getMaxApplyIDHosts(tt.applyIDMap)
			assert.Equalf(t, isSelf, tt.expectSelf, "is self applyID")
			assert.ElementsMatchf(t, targetHosts, tt.expectHosts, "target hosts")
			assert.Equalf(t, maxID, tt.expectMaxID, "max applyID")
		})
	}
}