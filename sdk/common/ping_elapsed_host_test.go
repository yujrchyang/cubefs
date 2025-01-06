package common

import (
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
	"time"
)

func Test_ParseRankedRuleStr(t *testing.T) {
	//解析规则
	testCases := []struct {
		name string
		rule string
		want [][]time.Duration
	}{
		{
			name: "empty rule",
			rule: "",
			want: nil,
		},
		{
			name: "test1",
			rule: "200",
			want: [][]time.Duration{
				{200 * time.Microsecond},
			},
		},
		{
			name: "test2",
			rule: "200,4000",
			want: [][]time.Duration{
				{200 * time.Microsecond},
				{4 * time.Millisecond},
			},
		},
		{
			name: "test3",
			rule: "200-400-800,4000",
			want: [][]time.Duration{
				{200 * time.Microsecond, 400 * time.Microsecond, 800 * time.Microsecond},
				{4 * time.Millisecond},
			},
		},
		{
			name: "test4",
			rule: "200,4000-5000-6000",
			want: [][]time.Duration{
				{200 * time.Microsecond},
				{4 * time.Millisecond, 5 * time.Millisecond, 6 * time.Millisecond},
			},
		},
	}
	for _, tc := range testCases {
		get := ParseRankedRuleStr(tc.rule)
		assert.Equal(t, tc.want, get, "failed: "+tc.name)
	}
}

func Test_classifyHostsByPingLevel(t *testing.T) {
	// host分区
	tp := &PingElapsedSortedHosts{
		sortedHosts: make([]*hostPingElapsed, 0),
	}
	for i := 0; i < 5; i++ {
		st := &hostPingElapsed{
			host:    strconv.Itoa(i),
			elapsed: time.Duration(i + 10),
		}
		tp.sortedHosts = append(tp.sortedHosts, st)
	}

}
