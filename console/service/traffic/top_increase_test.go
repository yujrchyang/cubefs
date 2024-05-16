package traffic

import (
	"fmt"
	cproto "github.com/cubefs/cubefs/console/proto"
	"testing"
)

func Test_GetTopIncreaseVol(t *testing.T) {
	request := &cproto.HistoryCurveRequest{
		Cluster:  "cfs_dbBack",
		ZoneName: "guan_ssd",

		IntervalType: 1,
		Start:        0,
		End:          0,
	}
	result := GetTopIncreaseVol(request, false, 0)
	if len(result) == 0 {
		t.Fatal("result len = 0")
	}
	msg := fmt.Sprintf("%v", result)
	println(msg)
}

func Test_GetTopIncreaseSource(t *testing.T) {
	request := &cproto.HistoryCurveRequest{
		Cluster:      "spark",
		ZoneName:     "runhui_ssd",
		IntervalType: 1,
		Start:        0,
		End:          0,
	}
	result := GetTopIncreaseSource(request, true, 0)
	if len(result) == 0 {
		t.Fatal("result len = 0")
	}
	msg := fmt.Sprintf("%v", result)
	println(msg)
}

func Test_GetTopVolByZone(t *testing.T) {
	result := GetTopVolByZone("cfs_dbBack", "", false)
	if len(result) == 0 {
		t.Fatal("result len = 0")
	}
	msg := fmt.Sprintf("%v", result)
	println(msg)
}

func Test_GetStartEndTime(t *testing.T) {
	//req1 := &cproto.HistoryCurveRequest{
	//	IntervalType: 1,
	//}
	//req2 := &cproto.HistoryCurveRequest{
	//	IntervalType: 2,
	//}
	req3 := &cproto.HistoryCurveRequest{
		IntervalType: 3,
	}
	start, end := parseTopIncRequestTime(req3)
	fmt.Printf("start: %v end: %v", start, end)
}
