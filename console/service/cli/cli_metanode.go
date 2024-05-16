package cli

import (
	"context"
	"fmt"
	cproto "github.com/cubefs/cubefs/console/proto"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

func (cli *CliService) GetMetaNodeConfig(cluster string, operation int) (result []*cproto.CliValueMetric, err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("GetMetaNodeConfig: cluster(%v) operation(%v:%v) err(%v)", cluster, operation, cproto.GetOpShortMsg(operation), err)
		}
	}()

	switch operation {
	case cproto.OpSetThreshold:
		metaNodeThreshold, err := cli.getMetaNodeThreshold(cluster)
		if err != nil {
			return nil, err
		}
		result = cproto.FormatArgsToValueMetrics(operation, metaNodeThreshold)

	case cproto.OpSetRocksDBDiskThreshold:
		metaNodeRocksdbDiskThreshold, err := cli.getRocksdbDiskThreshold(cluster)
		if err != nil {
			return nil, err
		}
		result = cproto.FormatArgsToValueMetrics(operation, metaNodeRocksdbDiskThreshold)

	case cproto.OpSetMemModeRocksDBDiskThreshold:
		metaNodeRocksdbDiskThreshold, err := cli.getMemModeRocksdbDiskThreshold(cluster)
		if err != nil {
			return nil, err
		}
		result = cproto.FormatArgsToValueMetrics(operation, metaNodeRocksdbDiskThreshold)

	case cproto.OpMetaNodeDumpWaterLevel:
		metaNodeDumpWaterLevel, err := cli.getMetaNodeDumpWaterLevel(cluster)
		if err != nil {
			return nil, err
		}
		result = cproto.FormatArgsToValueMetrics(operation, metaNodeDumpWaterLevel)

	case cproto.OpMetaRocksDBConf:
		if cproto.IsRelease(cluster) {
			return nil, ErrUnSupportOperation
		}
		limitInfo, err := cli.api.GetLimitInfoCache(cluster, false)
		if err != nil {
			return nil, err
		}
		result = cproto.FormatArgsToValueMetrics(operation,
			limitInfo.RocksDBDiskReservedSpace,
			limitInfo.MetaRockDBWalFileSize,
			limitInfo.MetaRocksWalMemSize,
			limitInfo.MetaRocksLogSize,
			limitInfo.MetaRocksLogReservedTime,
			limitInfo.MetaRocksLogReservedCnt,
			limitInfo.MetaRocksDisableFlushFlag,
			limitInfo.MetaRocksFlushWalInterval,
			limitInfo.MetaRocksWalTTL,
		)

	case cproto.OpSetMetaBitMapAllocator:
		maxUsedFactor, minFreeFactor, err := cli.getBitMapAllocator(cluster)
		if err != nil {
			return nil, err
		}
		result = cproto.FormatArgsToValueMetrics(operation,
			maxUsedFactor, minFreeFactor)

	case cproto.OpSetTrashCleanConfig:
		if cproto.IsRelease(cluster) {
			return nil, ErrUnSupportOperation
		}
		limitInfo, err := cli.api.GetLimitInfoCache(cluster, false)
		if err != nil {
			return nil, err
		}
		result = cproto.FormatArgsToValueMetrics(operation,
			limitInfo.MetaTrashCleanInterval,
			limitInfo.TrashCleanDurationEachTime,
			limitInfo.TrashItemCleanMaxCountEachTime,
		)

	case cproto.OpSetMetaRaftConfig:
		if cproto.IsRelease(cluster) {
			return nil, ErrUnSupportOperation
		}
		limitInfo, err := cli.api.GetLimitInfoCache(cluster, false)
		if err != nil {
			return nil, err
		}
		result = cproto.FormatArgsToValueMetrics(operation,
			limitInfo.MetaRaftLogSize,
			limitInfo.MetaRaftCap,
			limitInfo.MetaSyncWALOnUnstableEnableState,
		)

	case cproto.OpMetaClientRequestConf:
		if cproto.IsRelease(cluster) {
			return nil, ErrUnSupportOperation
		}
		limitInfo, err := cli.api.GetLimitInfoCache(cluster, false)
		if err != nil {
			return nil, err
		}
		result = cproto.FormatArgsToValueMetrics(operation,
			limitInfo.ClientReqRemoveDupFlag,
			limitInfo.ClientReqRecordsReservedMin,
			limitInfo.ClientReqRecordsReservedCount,
		)
	case cproto.OpMetaNodeDeleteBatchCount:
		if cproto.IsRelease(cluster) {
			return nil, ErrUnSupportOperation
		}
		limitInfo, err := cli.api.GetLimitInfoCache(cluster, false)
		if err != nil {
			return nil, err
		}
		result = cproto.FormatArgsToValueMetrics(operation, limitInfo.MetaNodeDeleteBatchCount)

	case cproto.OpMetaNodeDeleteWorkerSleepMs:
		if cproto.IsRelease(cluster) {
			return nil, ErrUnSupportOperation
		}
		limitInfo, err := cli.api.GetLimitInfoCache(cluster, false)
		if err != nil {
			return nil, err
		}
		result = cproto.FormatArgsToValueMetrics(operation, limitInfo.MetaNodeDeleteWorkerSleepMs)
	case cproto.OpMetaNodeReadDirLimitNum:
		limitNum, err := cli.getReadDirLimit(cluster)
		if err != nil {
			return nil, err
		}
		result = cproto.FormatArgsToValueMetrics(operation, limitNum)
	case cproto.OpDeleteEKRecordFileMaxMB:
		if cproto.IsRelease(cluster) {
			return nil, ErrUnSupportOperation
		}
		limitInfo, err := cli.api.GetLimitInfoCache(cluster, false)
		if err != nil {
			return nil, err
		}
		result = cproto.FormatArgsToValueMetrics(operation, limitInfo.DeleteEKRecordFileMaxMB)

	default:
	}
	return
}

func (cli *CliService) SetMetaNodeConfig(ctx context.Context, cluster string, operation int, metrics []*cproto.CliValueMetric, skipXbp bool) (err error) {
	var (
		args      map[string]interface{}
		isRelease = cproto.IsRelease(cluster)
	)
	defer func() {
		msg := fmt.Sprintf("SetMetaNodeConfig: cluster[%v] operation(%v:%v)", cluster, operation, cproto.GetOpShortMsg(operation))
		if err != nil {
			log.LogErrorf("%s, err(%v)", msg, err)
		} else {
			log.LogInfof("%v, metrics:%v", msg, metrics)
			cli.api.UpdateLimitInfoCache(cluster)
			cli.api.UpdateClusterViewCache(cluster)
		}
	}()

	if args, err = cproto.ParseValueMetricsToArgs(operation, metrics); err != nil {
		return
	}
	var params map[string]string
	params, _, err = cproto.ParseValueMetricsToParams(operation, metrics)
	if err != nil {
		return
	}

	switch operation {
	case cproto.OpSetThreshold:
		threshold := args[metrics[0].ValueName].(float64)
		if threshold < 0 || threshold > 1.0 {
			return fmt.Errorf("请设置0.00～1.00之间的数")
		}
		if !skipXbp {
			goto createXbpApply
		}
		return cli.setMetaNodeThreshold(cluster, threshold)

	case cproto.OpSetRocksDBDiskThreshold:
		threshold := args[metrics[0].ValueName].(float64)
		if threshold < 0 || threshold > 1.0 {
			return fmt.Errorf("请设置0.00～1.00之间的数")
		}
		if !skipXbp {
			goto createXbpApply
		}
		return cli.setRocksdbDiskThreshold(cluster, threshold)

	case cproto.OpSetMemModeRocksDBDiskThreshold:
		threshold := args[metrics[0].ValueName].(float64)
		if threshold < 0 || threshold > 1.0 {
			return fmt.Errorf("请设置0.00～1.00之间的数")
		}
		if !skipXbp {
			goto createXbpApply
		}
		return cli.setMetaNodeMemModeRocksDBDiskThreshold(cluster, threshold)

	case cproto.OpMetaNodeDumpWaterLevel:
		waterLevel := args[metrics[0].ValueName].(uint64)
		if waterLevel <= 0 {
			return fmt.Errorf("请输入 >0 的正整数")
		}
		if !skipXbp {
			goto createXbpApply
		}
		return cli.api.SetRatelimitInfo(cluster, params)

	case cproto.OpMetaRocksDBConf:
		for _, baseMetric := range cproto.GetCliOperationBaseMetrics(operation) {
			var val uint64
			// todo: 不允许设0的值，传0了就删掉（用户想设置0的情况 不是报错 而是直接把参数删除了）
			switch baseMetric.ValueName {
			case proto.RocksDBDiskReservedSpaceKey, proto.MetaRockDBWalFileMaxMB, proto.MetaRocksDBWalMemMaxMB, proto.MetaRocksDBLogMaxMB,
				proto.MetaRocksLogReservedDay, proto.MetaRocksLogReservedCnt, proto.MetaRocksWalFlushIntervalKey, proto.MetaRocksWalTTLKey:
				val = args[baseMetric.ValueName].(uint64)
				if val == 0 {
					delete(params, baseMetric.ValueName)
				}
			case proto.MetaRocksDisableFlushWalKey:
				val = args[baseMetric.ValueName].(uint64)
				if !(val == 0 || val == 1) {
					return fmt.Errorf("%v: 请在0和1之间选值传递！", proto.MetaRocksDisableFlushWalKey)
				}
			}
		}
		if !skipXbp {
			goto createXbpApply
		}
		return cli.api.SetRatelimitInfo(cluster, params)

	case cproto.OpSetMetaBitMapAllocator:
		for _, baseMetric := range cproto.GetCliOperationBaseMetrics(operation) {
			var val float64
			switch baseMetric.ValueName {
			case proto.AllocatorMaxUsedFactorKey:
				val = args[baseMetric.ValueName].(float64)
			case proto.AllocatorMinFreeFactorKey:
				val = args[baseMetric.ValueName].(float64)
			}
			if val < 0 || val > 1 {
				return fmt.Errorf("请输入0.00～1.00 之前的数")
			}
		}
		if !skipXbp {
			goto createXbpApply
		}
		return cli.api.SetRatelimitInfo(cluster, params)

	case cproto.OpSetTrashCleanConfig:
		for _, baseMetric := range cproto.GetCliOperationBaseMetrics(operation) {
			switch baseMetric.ValueName {
			case proto.MetaTrashCleanIntervalKey:
			case proto.TrashCleanDurationKey:
				// int32
				val := args[baseMetric.ValueName].(int64)
				if val < 0 {
					return fmt.Errorf("请输入 >0 的数")
				}
			case proto.TrashItemCleanMaxCountKey:
				val := args[baseMetric.ValueName].(int64)
				if val < 0 {
					return fmt.Errorf("请输入 >0 的数")
				}
			}
		}
		if !skipXbp {
			goto createXbpApply
		}
		return cli.api.SetRatelimitInfo(cluster, params)

	case cproto.OpSetMetaRaftConfig:
		for _, baseMetric := range cproto.GetCliOperationBaseMetrics(operation) {
			switch baseMetric.ValueName {
			case proto.MetaRaftLogSizeKey:
				size := args[baseMetric.ValueName].(int64)
				if size < 0 {
					return fmt.Errorf("请输入 >0 的数")
				}
			case proto.MetaRaftLogCapKey:
				size := args[baseMetric.ValueName].(int64)
				if size < 0 {
					return fmt.Errorf("请输入 >0 的数")
				}
			}
		}
		if !skipXbp {
			goto createXbpApply
		}
		return cli.api.SetRatelimitInfo(cluster, params)

	case cproto.OpMetaClientRequestConf:
		for _, baseMetric := range cproto.GetCliOperationBaseMetrics(operation) {
			switch baseMetric.ValueName {
			case proto.ClientReqRecordReservedMinKey:
				reservedMin := args[baseMetric.ValueName].(int64)
				if reservedMin < 0 {
					return fmt.Errorf("请输入 >0 的数")
				}
			case proto.ClientReqRecordReservedCntKey:
				reservedCnt := args[baseMetric.ValueName].(int64)
				if reservedCnt < 0 {
					return fmt.Errorf("请输入 >0 的数")
				}
			}
		}
		if !skipXbp {
			goto createXbpApply
		}
		return cli.api.SetRatelimitInfo(cluster, params)

	case cproto.OpMetaNodeDeleteBatchCount:
		if !skipXbp {
			goto createXbpApply
		}
		return cli.api.SetRatelimitInfo(cluster, params)

	case cproto.OpMetaNodeDeleteWorkerSleepMs:
		if !skipXbp {
			goto createXbpApply
		}
		return cli.api.SetRatelimitInfo(cluster, params)

	case cproto.OpDeleteEKRecordFileMaxMB:
		if !skipXbp {
			goto createXbpApply
		}
		return cli.api.SetRatelimitInfo(cluster, params)

	case cproto.OpMetaNodeReadDirLimitNum:
		readDirLimitNum := args[metrics[0].ValueName].(uint64)
		if readDirLimitNum < 500000 {
			return fmt.Errorf("请设置>500000的值")
		}
		if !skipXbp {
			goto createXbpApply
		}
		if isRelease {
			rc := cli.api.GetReleaseClient(cluster)
			return rc.SetReadDirLimit(readDirLimitNum)
		} else {
			return cli.api.SetRatelimitInfo(cluster, params)
		}

	default:
	}

createXbpApply:
	return cli.createXbpApply(ctx, cluster, cproto.MetaNodeModuleType, operation, [][]*cproto.CliValueMetric{metrics}, nil, nil, false)
}

func (cli *CliService) GetMetaNodeConfigList(cluster string, operation int) (result [][]*cproto.CliValueMetric, err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("GetMetaNodeConfigList: cluster[%v] operation(%v:%v) err(%v)", cluster, operation, cproto.GetOpShortMsg(operation), err)
		}
	}()

	result = make([][]*cproto.CliValueMetric, 0)

	switch operation {
	case cproto.OpMetaNodeDumpSnapCountByZone:
		countMap, err := cli.getMetaNodeDumpSnapCount(cluster)
		if err != nil {
			return nil, err
		}
		if len(countMap) == 0 {
			result = append(result, cproto.FormatOperationNilData(operation, "string", "uint64"))
		}
		for zone, count := range countMap {
			result = append(result, cproto.FormatArgsToValueMetrics(operation, zone, count))
		}
	default:
	}
	return
}

// 修改返回值
func (cli *CliService) SetMetaNodeConfigList(ctx context.Context, cluster string, operation int, metrics [][]*cproto.CliValueMetric, skipXbp bool) (err error) {
	defer func() {
		msg := fmt.Sprintf("SetMetaNodeConfigList: cluster[%v] operation(%v:%v) metrics: %v", cluster, operation, cproto.GetOpShortMsg(operation), metrics)
		if err != nil {
			log.LogErrorf("%s, err(%v)", msg, err)
		} else {
			log.LogInfof("%s", msg)
			cli.api.UpdateLimitInfoCache(cluster)
		}
	}()

	isRelease := cproto.IsRelease(cluster)

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
	case cproto.OpMetaNodeDumpSnapCountByZone:
		if isRelease {
			err = ErrUnSupportOperation
			return
		}
		if !skipXbp {
			return cli.createXbpApply(ctx, cluster, cproto.MetaNodeModuleType, operation, metrics, nil, nil, true)
		}
		goto setRateLimit
	default:
	}

setRateLimit:
	for _, param := range params {
		err = cli.api.SetRatelimitInfo(cluster, param)
		if err != nil {
			log.LogWarnf("SetMetaNodeConfigList: operation(%v:%v) args(%v) err(%v)", operation, cproto.GetOpShortMsg(operation), args, err)
			continue
		}
	}
	return
}

func (cli *CliService) getMetaNodeThreshold(cluster string) (float32, error) {
	if cproto.IsRelease(cluster) {
		// 没有接口获取，只能修改
		return 0, nil
	}
	cv, err := cli.api.GetClusterViewCache(cluster, true)
	if err != nil {
		return 0, err
	}
	return cv.MetaNodeThreshold, nil
}
func (cli *CliService) setMetaNodeThreshold(cluster string, threshold float64) error {
	if cproto.IsRelease(cluster) {
		rc := cli.api.GetReleaseClient(cluster)
		err := rc.SetMetaNodeThreshold(threshold)
		return err
	}
	mc := cli.api.GetMasterClient(cluster)
	err := mc.AdminAPI().SetMetaNodeThreshold(threshold)
	return err
}

func (cli *CliService) getRocksdbDiskThreshold(cluster string) (float32, error) {
	if cproto.IsRelease(cluster) {
		return 0, ErrUnSupportOperation
	}
	cv, err := cli.api.GetClusterViewCache(cluster, true)
	if err != nil {
		return 0, err
	}
	return cv.MetaNodeRocksdbDiskThreshold, nil
}
func (cli *CliService) setRocksdbDiskThreshold(cluster string, threshold float64) error {
	if cproto.IsRelease(cluster) {
		return ErrUnSupportOperation
	}
	mc := cli.api.GetMasterClient(cluster)
	err := mc.AdminAPI().SetMetaNodeRocksDBDiskThreshold(threshold)
	return err
}
func (cli *CliService) getMemModeRocksdbDiskThreshold(cluster string) (float32, error) {
	if cproto.IsRelease(cluster) {
		return 0, ErrUnSupportOperation
	}
	cv, err := cli.api.GetClusterViewCache(cluster, true)
	if err != nil {
		return 0, err
	}
	return cv.MetaNodeMemModeRocksdbDiskThreshold, nil
}
func (cli *CliService) setMetaNodeMemModeRocksDBDiskThreshold(cluster string, threshold float64) error {
	if cproto.IsRelease(cluster) {
		return ErrUnSupportOperation
	}
	mc := cli.api.GetMasterClient(cluster)
	err := mc.AdminAPI().SetMetaNodeMemModeRocksDBDiskThreshold(threshold)
	return err
}

func (cli *CliService) getMetaNodeDumpWaterLevel(cluster string) (uint64, error) {
	if cproto.IsRelease(cluster) {
		return 0, ErrUnSupportOperation
	}
	limitInfo, err := cli.api.GetLimitInfoCache(cluster, false)
	if err != nil {
		return 0, err
	}
	return limitInfo.MetaNodeDumpWaterLevel, nil
}

func (cli *CliService) getMetaNodeDumpSnapCount(cluster string) (map[string]uint64, error) {
	if cproto.IsRelease(cluster) {
		return nil, ErrUnSupportOperation
	}
	limitInfo, err := cli.api.GetLimitInfoCache(cluster, false)
	if err != nil {
		return nil, err
	}
	return limitInfo.MetaNodeDumpSnapCountByZone, nil
}
func (cli *CliService) getBitMapAllocator(cluster string) (float64, float64, error) {
	if cproto.IsRelease(cluster) {
		limitInfo, err := cli.api.GetLimitInfoCacheRelease(cluster, false)
		if err != nil {
			return 0, 0, err
		}
		return limitInfo.BitMapAllocatorMaxUsedFactor, limitInfo.BitMapAllocatorMinFreeFactor, nil
	} else {
		limitInfo, err := cli.api.GetLimitInfoCache(cluster, false)
		if err != nil {
			return 0, 0, err
		}
		return limitInfo.BitMapAllocatorMaxUsedFactor, limitInfo.BitMapAllocatorMinFreeFactor, nil
	}
}
func (cli *CliService) getReadDirLimit(cluster string) (uint64, error) {
	if cproto.IsRelease(cluster) {
		limitInfo, err := cli.api.GetLimitInfoCacheRelease(cluster, false)
		if err != nil {
			return 0, err
		}
		return limitInfo.ReadDirLimitNum, nil
	} else {
		limitInfo, err := cli.api.GetLimitInfoCache(cluster, false)
		if err != nil {
			return 0, err
		}
		return limitInfo.MetaNodeReadDirLimitNum, nil
	}
}
