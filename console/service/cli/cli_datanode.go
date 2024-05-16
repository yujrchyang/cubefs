package cli

import (
	"context"
	"fmt"
	cproto "github.com/cubefs/cubefs/console/proto"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"strings"
)

func (cli *CliService) GetDataNodeConfig(cluster string, operation int) (result []*cproto.CliValueMetric, err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("GetDataNodeConfig: cluster[%v] operation(%v:%v) err(%v)", cluster, operation, cproto.GetOpShortMsg(operation), err)
		}
	}()

	switch operation {
	case cproto.OpDataNodeFixTinyDeleteRecordLimit:
		dataNodeFixTinyDeleteRecordLimitOnDisk, err := cli.getDataNodeFixTinyDeleteRecordLimitOnDisk(cluster)
		if err != nil {
			return nil, err
		}
		result = cproto.FormatArgsToValueMetrics(operation, dataNodeFixTinyDeleteRecordLimitOnDisk)

	case cproto.OpDataNodeNormalExtentDeleteExpire:
		dataNodeNormalExtentDeleteExpire, err := cli.getDataNodeNormalExtentDeleteExpire(cluster)
		if err != nil {
			return nil, err
		}
		result = cproto.FormatArgsToValueMetrics(operation, dataNodeNormalExtentDeleteExpire)

	case cproto.OpDataNodeFlushFDInterval:
		flushFDInterval, err := cli.getDataNodeFlushFDInterval(cluster)
		if err != nil {
			return nil, err
		}
		result = cproto.FormatArgsToValueMetrics(operation, flushFDInterval)

	case cproto.OpDataSyncWALOnUnstableEnableState:
		enable, err := cli.getDataSyncWALOnUnstableEnableState(cluster)
		if err != nil {
			return nil, err
		}
		result = cproto.FormatArgsToValueMetrics(operation, enable)

	case cproto.OpDataNodeFlushFDParallelismOnDisk:
		lism, err := cli.getDataNodeFlushFDParallelismOnDisk(cluster)
		if err != nil {
			return nil, err
		}
		result = cproto.FormatArgsToValueMetrics(operation, lism)

	case cproto.OpDataPartitionConsistencyMode:
		mode, err := cli.getDataPartitionConsistencyMode(cluster)
		if err != nil {
			return nil, err
		}
		result = cproto.FormatArgsToValueMetrics(operation, mode)

	case cproto.OpDataNodeDiskReservedRatio:
		ratio, err := cli.getDataNodeDiskReservedRatio(cluster)
		if err != nil {
			return nil, err
		}
		result = cproto.FormatArgsToValueMetrics(operation, ratio)

	case cproto.OpDataNodeRepairTaskCount:
		if !cproto.IsRelease(cluster) {
			return nil, fmt.Errorf("请通过限速配置设置该参数！")
		}
		repairTaskCount, err := cli.getDataNodeRepairTaskCount(cluster)
		if err != nil {
			return nil, err
		}
		result = cproto.FormatArgsToValueMetrics(operation, repairTaskCount)

	default:
	}
	return
}

func (cli *CliService) SetDataNodeConfig(ctx context.Context, cluster string, operation int, metrics []*cproto.CliValueMetric, skipXbp bool) (err error) {
	defer func() {
		msg := fmt.Sprintf("SetDataNodeConfig: cluster[%v] operation(%v:%v)", cluster, operation, cproto.GetOpShortMsg(operation))
		if err != nil {
			log.LogErrorf("%s err(%v)", msg, err)
		} else {
			log.LogInfof("%v, metrics:%v", msg, metrics)
			cli.api.UpdateLimitInfoCache(cluster)
		}
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
	case cproto.OpDataNodeFlushFDInterval:
		if !skipXbp {
			goto createXbpApply
		}
		goto setRateLimit

	case cproto.OpDataSyncWALOnUnstableEnableState:
		if !skipXbp {
			goto createXbpApply
		}
		goto setRateLimit

	case cproto.OpDataNodeNormalExtentDeleteExpire:
		normalExpire := args[metrics[0].ValueName].(uint64)
		if normalExpire < 600 {
			return fmt.Errorf("请输入 >= 600 的正整数")
		}
		if !skipXbp {
			goto createXbpApply
		}
		goto setRateLimit

	case cproto.OpDataNodeFixTinyDeleteRecordLimit:
		if !skipXbp {
			goto createXbpApply
		}
		goto setRateLimit

	case cproto.OpDataNodeFlushFDParallelismOnDisk:
		if !skipXbp {
			goto createXbpApply
		}
		goto setRateLimit

	case cproto.OpDataPartitionConsistencyMode:
		mode := args[metrics[0].ValueName].(int64)
		if mode != 0 && mode != 1 {
			return fmt.Errorf("请在0 或 1之间选择！")
		}
		params["module"] = strings.ToLower(cproto.RoleNameDataNode)
		if !skipXbp {
			goto createXbpApply
		}
		goto setRateLimit

	case cproto.OpDataNodeDiskReservedRatio:
		ratio := args[metrics[0].ValueName].(float64)
		if ratio < proto.DataNodeDiskReservedMinRatio || ratio > proto.DataNodeDiskReservedMaxRatio {
			return fmt.Errorf("请输入 0.01 ～ 0.1 之间的数")
		}
		params["module"] = strings.ToLower(cproto.RoleNameDataNode)
		if !skipXbp {
			goto createXbpApply
		}
		goto setRateLimit

	case cproto.OpDataNodeRepairTaskCount:
		if !skipXbp {
			goto createXbpApply
		}
		goto setRateLimit
	}
createXbpApply:
	return cli.createXbpApply(ctx, cluster, cproto.DataNodeModuleType, operation, [][]*cproto.CliValueMetric{metrics}, nil, nil, false)

setRateLimit:
	return cli.api.SetRatelimitInfo(cluster, params)
}

func (cli *CliService) GetDataNodeConfigList(cluster string, operation int) (result [][]*cproto.CliValueMetric, err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("GetDataNodeConfigList: cluster[%v] operation(%v:%v) err(%v)", cluster, operation, cproto.GetOpShortMsg(operation), err)
		}
	}()

	result = make([][]*cproto.CliValueMetric, 0)

	switch operation {
	default:
	}
	return
}

func (cli *CliService) SetDataNodeConfigList(ctx context.Context, cluster string, operation int, metrics [][]*cproto.CliValueMetric, skipXbp bool) (err error) {
	defer func() {
		msg := fmt.Sprintf("SetDataNodeConfigList: cluster[%v] operation(%v) metrics(%v)", cluster, cproto.GetOpShortMsg(operation), metrics)
		if err != nil {
			log.LogErrorf("%s err(%v)", msg, err)
		} else {
			log.LogInfof("%s", msg)
		}
	}()

	switch operation {
	default:
		goto update
	}

update:
	var args map[string]string
	for _, metric := range metrics {
		var isEmpty bool
		args, isEmpty, err = cproto.ParseValueMetricsToParams(operation, metric)
		if err != nil || isEmpty {
			continue
		}
		err = cli.api.SetRatelimitInfo(cluster, args)
		if err != nil {
			log.LogWarnf("SetDataNodeConfigList: operation(%v) args(%v) err(%v)", cproto.GetOpShortMsg(operation), args, err)
			continue
		}
	}
	cli.api.UpdateLimitInfoCache(cluster)
	return
}

func (cli *CliService) getDataNodeFlushFDInterval(cluster string) (uint32, error) {
	if cproto.IsRelease(cluster) {
		return 0, ErrUnSupportOperation
	}
	limitInfo, err := cli.api.GetLimitInfoCache(cluster, false)
	if err != nil {
		return 0, err
	}
	return limitInfo.DataNodeFlushFDInterval, nil
}

func (cli *CliService) getDataSyncWALOnUnstableEnableState(cluster string) (bool, error) {
	if cproto.IsRelease(cluster) {
		return false, ErrUnSupportOperation
	}
	limitInfo, err := cli.api.GetLimitInfoCache(cluster, false)
	if err != nil {
		return false, err
	}
	return limitInfo.DataSyncWALOnUnstableEnableState, nil
}

func (cli *CliService) getDataNodeFixTinyDeleteRecordLimitOnDisk(cluster string) (uint64, error) {
	if cproto.IsRelease(cluster) {
		return 0, ErrUnSupportOperation
	}
	limitInfo, err := cli.api.GetLimitInfoCache(cluster, false)
	if err != nil {
		return 0, err
	}
	return limitInfo.DataNodeFixTinyDeleteRecordLimitOnDisk, nil
}

func (cli *CliService) getDataNodeNormalExtentDeleteExpire(cluster string) (uint64, error) {
	if cproto.IsRelease(cluster) {
		return 0, ErrUnSupportOperation
	}
	limitInfo, err := cli.api.GetLimitInfoCache(cluster, false)
	if err != nil {
		return 0, err
	}
	return limitInfo.DataNodeNormalExtentDeleteExpire, nil
}

func (cli *CliService) getDataNodeFlushFDParallelismOnDisk(cluster string) (uint64, error) {
	if cproto.IsRelease(cluster) {
		return 0, ErrUnSupportOperation
	}
	limitInfo, err := cli.api.GetLimitInfoCache(cluster, false)
	if err != nil {
		return 0, err
	}
	return limitInfo.DataNodeFlushFDParallelismOnDisk, nil
}

func (cli *CliService) getDataPartitionConsistencyMode(cluster string) (int32, error) {
	if cproto.IsRelease(cluster) {
		return 0, ErrUnSupportOperation
	}
	limitInfo, err := cli.api.GetLimitInfoCache(cluster, false)
	if err != nil {
		return 0, err
	}
	return limitInfo.DataPartitionConsistencyMode.Int32(), nil
}

func (cli *CliService) getDataNodeDiskReservedRatio(cluster string) (float64, error) {
	if cproto.IsRelease(cluster) {
		return 0, ErrUnSupportOperation
	}
	limitInfo, err := cli.api.GetLimitInfoCache(cluster, false)
	if err != nil {
		return 0, err
	}
	return limitInfo.DataNodeDiskReservedRatio, nil
}

func (cli *CliService) getDataNodeRepairTaskCount(cluster string) (uint64, error) {
	limitInfo, err := cli.api.GetLimitInfoCacheRelease(cluster, false)
	if err != nil {
		return 0, err
	}
	return limitInfo.DataNodeRepairLimitOnDisk, nil
}
