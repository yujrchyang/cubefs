package cli

import (
	"context"
	"fmt"
	cproto "github.com/cubefs/cubefs/console/proto"
	"github.com/cubefs/cubefs/util/log"
)

func (cli *CliService) GetFlashNodeConfig(cluster string, operation int) (result []*cproto.CliValueMetric, err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("GetFlashNodeConfig: cluster[%v] operation[%v:%v] err(%v)", cluster, operation, cproto.GetOperationShortMsg(operation), err)
		}
	}()

	switch operation {
	case cproto.OpFlashNodeReadTimeoutUs:
		timeoutUs, err := cli.getFlashNodeReadTimeoutUs(cluster)
		if err != nil {
			return nil, err
		}
		result = cproto.FormatArgsToValueMetrics(operation, timeoutUs)

	case cproto.OpFlashNodeDisableStack:
		disableStack, err := cli.getFlashNodeDisableStack(cluster)
		if err != nil {
			return nil, err
		}
		result = cproto.FormatArgsToValueMetrics(operation, disableStack)

	default:
	}

	return
}

func (cli *CliService) SetFlashNodeConfig(ctx context.Context, cluster string, operation int, metrics []*cproto.CliValueMetric, skipXbp bool) (err error) {
	defer func() {
		msg := fmt.Sprintf("SetFlashNodeConfig: cluster[%v] operation(%v:%v)", cluster, operation, cproto.GetOperationShortMsg(operation))
		if err != nil {
			log.LogErrorf("%s, err(%v)", msg, err)
		} else {
			log.LogInfof("%s, metrics:%v", msg, metrics)
		}
		cli.api.UpdateLimitInfoCache(cluster)
	}()
	var (
		args   map[string]interface{} // 参数校验用
		params map[string]string      // 请求接口用
	)
	args, err = cproto.ParseValueMetricsToArgs(operation, metrics)
	if err != nil {
		return
	}
	params, _, err = cproto.ParseValueMetricsToParams(operation, metrics)
	if err != nil {
		return
	}

	switch operation {
	case cproto.OpFlashNodeReadTimeoutUs:
		timeoutUs := args[metrics[0].ValueName].(uint64)
		if timeoutUs < 500 {
			return fmt.Errorf("请输入 >= 500 的正整数")
		}
		if !skipXbp {
			goto createXbpApply
		}
		goto setRateLimit

	case cproto.OpFlashNodeDisableStack:
		if !skipXbp {
			goto createXbpApply
		}
		goto setRateLimit

	default:
		return fmt.Errorf("undefined operation code: %v:%v", operation, cproto.GetOperationShortMsg(operation))
	}
createXbpApply:
	return cli.createXbpApply(ctx, cluster, cproto.FlashNodeModuleType, operation, [][]*cproto.CliValueMetric{metrics}, nil, nil, false)

setRateLimit:
	return cli.api.SetRatelimitInfo(cluster, params)
}

func (cli *CliService) getFlashNodeReadTimeoutUs(cluster string) (uint64, error) {
	limitInfo, err := cli.api.GetLimitInfoCache(cluster, false)
	if err != nil {
		return 0, err
	}
	return limitInfo.FlashNodeReadTimeoutUs, nil
}

func (cli *CliService) getFlashNodeDisableStack(cluster string) (bool, error) {
	limitInfo, err := cli.api.GetLimitInfoCache(cluster, false)
	if err != nil {
		return false, err
	}
	return limitInfo.FlashNodeDisableStack, nil
}
