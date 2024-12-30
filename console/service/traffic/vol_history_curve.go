package traffic

import (
	"time"

	"github.com/cubefs/cubefs/console/model"
	cproto "github.com/cubefs/cubefs/console/proto"
)

func GetVolHistoryCurve(req *cproto.HistoryCurveRequest) (result []*model.VolumeHistoryCurve, err error) {
	start, end, err := cproto.ParseHistoryCurveRequestTime(req.IntervalType, req.Start, req.End)
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
