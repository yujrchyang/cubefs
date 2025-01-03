package cli

import (
	"context"
	"fmt"
	cproto "github.com/cubefs/cubefs/console/proto"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

func (cli *CliService) GetNetworkConfig(cluster string, operation int) (result []*cproto.CliValueMetric, err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("GetNetworkConfig failed: cluster[%v] operation(%v) err(%v)", cluster, cproto.GetOperationShortMsg(operation), err)
		}
	}()

	result = make([]*cproto.CliValueMetric, 0)
	switch operation {
	case cproto.OpRemoteReadConnTimeoutMs:
		if remoteReadConnTimeoutMs, err := cli.getRemoteReadConnTimeout(cluster); err != nil {
			return nil, err
		} else {
			result = cproto.FormatArgsToValueMetrics(operation, remoteReadConnTimeoutMs)
		}

	default:
		err = fmt.Errorf("undefined operation code: %v:%v", operation, cproto.GetOperationShortMsg(operation))
	}
	return
}

func (cli *CliService) SetNetworkConfig(ctx context.Context, cluster string, operation int, metric []*cproto.CliValueMetric, skipXbp bool) (err error) {
	defer func() {
		msg := fmt.Sprintf("SetNetworkConfig: cluster[%v] operation(%v) metric(%v)", cluster, cproto.GetOperationShortMsg(operation), metric)
		if err != nil {
			log.LogErrorf("%s err(%v)", msg, err)
		} else {
			log.LogInfof("%s", msg)
		}
		cli.api.UpdateLimitInfoCache(cluster)
	}()

	args, err := cproto.ParseValueMetricsToArgs(operation, metric)
	if err != nil {
		return
	}
	params, _, err := cproto.ParseValueMetricsToParams(operation, metric)
	if err != nil {
		return
	}

	switch operation {
	case cproto.OpRemoteReadConnTimeoutMs:
		readTimeoutMs := args[metric[0].ValueName].(int64)
		if readTimeoutMs < 0 {
			// 0 没有意义，客户端不会使用(默认or之前的值)，但master中参数会是0
			return fmt.Errorf("请输入 >= 0的正整数")
		}
		if !skipXbp {
			return cli.createXbpApply(ctx, cluster, cproto.NetworkModuleType, operation, [][]*cproto.CliValueMetric{metric}, nil, nil, false)
		}
		return cli.api.SetRatelimitInfo(cluster, params)

	default:
		err = fmt.Errorf("undefined operation code: %v:%v", operation, cproto.GetOperationShortMsg(operation))
		return
	}
}

func (cli *CliService) GetNetworkConfigList(cluster string, operation int) (result [][]*cproto.CliValueMetric, err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("GetNetworkConfigList failed: cluster[%v] operation(%v) err(%v)", cluster, cproto.GetOperationShortMsg(operation), err)
		}
	}()

	result = make([][]*cproto.CliValueMetric, 0)
	switch operation {
	case cproto.OpNetworkFlowRatio:
		moduleFlowRatio, err := cli.getNetworkFlowRatio(cluster)
		if err != nil {
			return nil, err
		}
		if len(moduleFlowRatio) == 0 {
			result = append(result, cproto.FormatOperationNilData(operation, "string", "uint64"))
		}
		for module, flowRatio := range moduleFlowRatio {
			result = append(result, cproto.FormatArgsToValueMetrics(operation, module, flowRatio))
		}

	case cproto.OpReadConnTimeoutMs:
		zoneReadConnTimeout, err := cli.getReadConnTimeout(cluster)
		if err != nil {
			return nil, err
		}
		if len(zoneReadConnTimeout) == 0 {
			result = append(result, cproto.FormatOperationNilData(operation, "string", "int64"))
		}
		for zone, readTimeout := range zoneReadConnTimeout {
			result = append(result, cproto.FormatArgsToValueMetrics(operation, zone, readTimeout))
		}

	case cproto.OpWriteConnTimeoutMs:
		zoneWriteConnTimeout, err := cli.getWriteConnTimeout(cluster)
		if err != nil {
			return nil, err
		}
		if len(zoneWriteConnTimeout) == 0 {
			result = append(result, cproto.FormatOperationNilData(operation, "string", "int64"))
		}
		for zone, writeTimeout := range zoneWriteConnTimeout {
			result = append(result, cproto.FormatArgsToValueMetrics(operation, zone, writeTimeout))
		}

	default:
		err = fmt.Errorf("undefined operation code: %v:%v", operation, cproto.GetOperationShortMsg(operation))
	}
	return
}

func (cli *CliService) SetNetworkConfigList(ctx context.Context, cluster string, operation int, metrics [][]*cproto.CliValueMetric, skipXbp bool) (err error) {
	defer func() {
		msg := fmt.Sprintf("SetNetworkConfigList: cluster[%v] operation(%v) metrics(%v) ", cluster, cproto.GetOperationShortMsg(operation), metrics)
		if err != nil {
			log.LogErrorf("%s err(%v)", msg, err)
		} else {
			log.LogInfof("%s", msg)
		}
		cli.api.UpdateLimitInfoCache(cluster)
	}()

	params := make([]map[string]string, 0)
	args := make([]map[string]interface{}, 0)
	for _, metric := range metrics {
		var (
			arg     map[string]interface{}
			param   map[string]string
			isEmpty bool
		)
		// 校验非空
		param, isEmpty, err = cproto.ParseValueMetricsToParams(operation, metric)
		if err != nil || isEmpty {
			continue
		}
		// 校验有效
		arg, err = cproto.ParseValueMetricsToArgs(operation, metric)
		if err != nil {
			return
		}
		// 非空，且有效。才加入到list中
		params = append(params, param)
		args = append(args, arg)
	}

	switch operation {
	case cproto.OpNetworkFlowRatio:
		for _, arg := range args {
			for _, baseMetric := range cproto.GetCliOperationBaseMetrics(operation) {
				switch baseMetric.ValueName {
				case "module":
					module := arg[baseMetric.ValueName].(string)
					if module == "" {
						return fmt.Errorf("请指定要设置的模块！")
					}

				case proto.NetworkFlowRatioKey:
					ratio := arg[baseMetric.ValueName].(uint64)
					if ratio < 0 || ratio > 100 {
						return fmt.Errorf("请输入0～100之间的数")
					}
				}
			}
		}
		if !skipXbp {
			return cli.createXbpApply(ctx, cluster, cproto.NetworkModuleType, operation, metrics, nil, nil, true)
		}
		goto setRateLimit

	case cproto.OpReadConnTimeoutMs:
		for _, arg := range args {
			for _, baseMetric := range cproto.GetCliOperationBaseMetrics(operation) {
				switch baseMetric.ValueName {
				case "zoneName":
					// 可以为空
				case proto.ReadConnTimeoutMsKey:
					readMs := arg[baseMetric.ValueName].(int64)
					if readMs < 0 {
						return fmt.Errorf("请输入 >=0 的正整数")
					}
				}
			}
		}
		if !skipXbp {
			return cli.createXbpApply(ctx, cluster, cproto.NetworkModuleType, operation, metrics, nil, nil, true)
		}
		goto setRateLimit

	case cproto.OpWriteConnTimeoutMs:
		for _, arg := range args {
			for _, baseMetric := range cproto.GetCliOperationBaseMetrics(operation) {
				switch baseMetric.ValueName {
				case "zoneName":
					// 可以为空
				case proto.WriteConnTimeoutMsKey:
					readMs := arg[baseMetric.ValueName].(int64)
					if readMs < 0 {
						return fmt.Errorf("请输入 >=0 的正整数")
					}
				}
			}
		}
		if !skipXbp {
			return cli.createXbpApply(ctx, cluster, cproto.NetworkModuleType, operation, metrics, nil, nil, true)
		}
		goto setRateLimit

	default:
		err = fmt.Errorf("undefined operation code: %v:%v", operation, cproto.GetOperationShortMsg(operation))
		return
	}

setRateLimit:
	for _, param := range params {
		err = cli.api.SetRatelimitInfo(cluster, param)
		if err != nil {
			log.LogWarnf("SetNetworkConfigList: operation(%v) args(%v) err(%v)", cproto.GetOperationShortMsg(operation), args, err)
			continue
		}
	}
	return
}

func (cli *CliService) getRemoteReadConnTimeout(cluster string) (int64, error) {
	if cproto.IsRelease(cluster) {
		return 0, ErrUnSupportOperation
	} else {
		limitInfo, err := cli.api.GetLimitInfoCache(cluster, false)
		if err != nil {
			return 0, err
		}
		return limitInfo.RemoteReadConnTimeout, nil
	}
}

func (cli *CliService) getNetworkFlowRatio(cluster string) (map[string]uint64, error) {
	if cproto.IsRelease(cluster) {
		limitInfo, err := cli.api.GetLimitInfoCacheRelease(cluster, false)
		if err != nil {
			return nil, err
		}
		return limitInfo.NetworkFlowRatio, nil
	} else {
		limitInfo, err := cli.api.GetLimitInfoCache(cluster, false)
		if err != nil {
			return nil, err
		}
		return limitInfo.NetworkFlowRatio, nil
	}
}

func (cli *CliService) getReadConnTimeout(cluster string) (map[string]int64, error) {
	if cproto.IsRelease(cluster) {
		return nil, ErrUnSupportOperation
	} else {
		limitInfo, err := cli.api.GetLimitInfoCache(cluster, false)
		if err != nil {
			return nil, err
		}
		result := make(map[string]int64)
		for zone, connCfg := range limitInfo.ZoneNetConnConfig {
			result[zone] = connCfg.ReadTimeoutNs / 1e6
		}
		return result, nil
	}
}

func (cli *CliService) getWriteConnTimeout(cluster string) (map[string]int64, error) {
	if cproto.IsRelease(cluster) {
		return nil, ErrUnSupportOperation
	} else {
		limitInfo, err := cli.api.GetLimitInfoCache(cluster, false)
		if err != nil {
			return nil, err
		}
		result := make(map[string]int64)
		for zone, connCfg := range limitInfo.ZoneNetConnConfig {
			result[zone] = connCfg.WriteTimeoutNs / 1e6
		}
		return result, nil
	}
}
