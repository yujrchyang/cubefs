package flowSchedule

import (
	"fmt"
	cproto "github.com/cubefs/cubefs/console/proto"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_getStartAndEndFromRequest(t *testing.T) {
	req := &cproto.TrafficRequest{
		IntervalType: 2,
	}
	level, err := getStartAndEndFromRequest(req, false)
	assert.Nil(t, err)

	fmt.Printf("level := %v\n", level)

	start := parseTimestampToDataTime(req.StartTime)
	end := parseTimestampToDataTime(req.EndTime)
	fmt.Printf("start: %v end: %v\n", start, end)
}
