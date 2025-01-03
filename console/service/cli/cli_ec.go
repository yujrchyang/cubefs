package cli

import (
	"context"
	"fmt"
	cproto "github.com/cubefs/cubefs/console/proto"
	"github.com/cubefs/cubefs/util/log"
)

func (cli *CliService) GetEcConfig(cluster string, operation int) (result []*cproto.CliValueMetric, err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("GetEcConfig: cluster[%v] operation(%v:%v) err(%v)", cluster, operation, cproto.GetOperationShortMsg(operation), err)
		}
	}()

	if cproto.IsRelease(cluster) {
		return nil, ErrUnSupportOperation
	}
	cv, err := cli.api.GetClusterViewCache(cluster, false)
	if err != nil {
		return nil, err
	}

	switch operation {
	case cproto.OpSetEcConfig:
		result = cproto.FormatArgsToValueMetrics(operation, cv.EcScrubEnable, cv.EcScrubPeriod, cv.EcMaxScrubExtents, cv.MaxCodecConcurrent)

	default:
		return nil, fmt.Errorf("undefined operation code: %v:%v", operation, cproto.GetOperationShortMsg(operation))
	}
	return
}

func (cli *CliService) SetEcConfig(ctx context.Context, cluster string, operation int, metrics []*cproto.CliValueMetric, skipXbp bool) (err error) {
	defer func() {
		msg := fmt.Sprintf("SetEcConfig: cluster[%v] operation(%v:%v)", cluster, operation, cproto.GetOperationShortMsg(operation))
		if err != nil {
			log.LogErrorf("%s, err:%v", msg, err)
		} else {
			log.LogInfof("%v, metrics:%v", msg, metrics)
		}
		cli.api.GetClusterViewCache(cluster, true)
	}()

	if cproto.IsRelease(cluster) {
		err = ErrUnSupportOperation
		return
	}

	var args map[string]interface{}
	args, err = cproto.ParseValueMetricsToArgs(operation, metrics)
	if err != nil {
		return
	}
	mc := cli.api.GetMasterClient(cluster)

	switch operation {
	case cproto.OpSetEcConfig:
		if !skipXbp {
			return cli.createXbpApply(ctx, cluster, cproto.EcModuleType, operation, [][]*cproto.CliValueMetric{metrics}, nil, nil, false)
		}
		err = mc.AdminAPI().UpdateEcInfo(args[metrics[0].ValueName].(bool), int(args[metrics[1].ValueName].(uint64)),
			int(args[metrics[2].ValueName].(uint64)), int(args[metrics[3].ValueName].(int64)))

	default:
	}
	return
}
