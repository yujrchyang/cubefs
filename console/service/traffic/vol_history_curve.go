package traffic

import (
	"fmt"
	"time"

	"github.com/cubefs/cubefs/console/model"
	cproto "github.com/cubefs/cubefs/console/proto"
)

const (
	secondsForOneDay     = 24 * 60 * 60
	secondsForThreeMonth = 3 * 30 * secondsForOneDay
)

func GetVolHistoryCurve(req *cproto.HistoryCurveRequest) (result []*model.VolumeHistoryCurve, err error) {
	start, end, err := parseHistoryCurveRequestTime(req.IntervalType, req.Start, req.End)
	if err != nil {
		return nil, err
	}
	table := model.ConsoleVolume{}
	startKeepTime := time.Now().AddDate(0, 0, -volumeInfoKeepDay)
	/*                    24h
	 *  _______________|____________|
	 *      历史(天)  begin   10min    end
	 */
	if start.Before(startKeepTime) {
		if end.Before(startKeepTime) {
			return model.LoadVolumeHistoryData(req.Cluster, req.Volume, start, end)
		}
		// 联表查询
		result = make([]*model.VolumeHistoryCurve, 0)
		r1, err := model.LoadVolumeHistoryData(req.Cluster, req.Volume, start, startKeepTime)
		if err == nil {
			result = append(result, r1...)
		}
		r2, err := table.LoadVolumeInfo(req.Cluster, req.Volume, startKeepTime, end)
		if err == nil {
			result = append(result, r2...)
		}
		return result, err

	} else {
		return table.LoadVolumeInfo(req.Cluster, req.Volume, start, end)
	}
}

func parseHistoryCurveRequestTime(interval int, startDate, endDate int64) (start, end time.Time, err error) {
	end = time.Now()
	switch interval {
	case cproto.ResourceNoType:
		if startDate == 0 && endDate == 0 {
			err = fmt.Errorf("start and end cannot both be 0")
			return
		}
		if endDate-startDate >= int64(secondsForThreeMonth) {
			err = fmt.Errorf("时间跨度不超过3个月！")
			return
		}
		if startDate == 0 {
			startDate = endDate - int64(secondsForOneDay)
		}
		if endDate == 0 {
			endDate = end.Unix()
		}
		if startDate == endDate {
			endDate = startDate + int64(secondsForOneDay)
		}
		end = time.Unix(endDate, 0)
		start = time.Unix(startDate, 0)

	case cproto.ResourceLatestOneDay:
		start = end.AddDate(0, 0, -1)

	case cproto.ResourceLatestOneWeek:
		start = end.AddDate(0, 0, -7)

	case cproto.ResourceLatestOneMonth:
		start = end.AddDate(0, -1, 0)

	default:
		err = fmt.Errorf("undefined interval type: %v", interval)
		return
	}
	// ？为啥秒和纳秒都是0
	end = time.Date(end.Year(), end.Month(), end.Day(), end.Hour(), end.Minute(), 0, 0, time.Local)
	start = time.Date(start.Year(), start.Month(), start.Day(), start.Hour(), start.Minute(), 0, 0, time.Local)
	return
}
