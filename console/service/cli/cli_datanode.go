package cli

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	cproto "github.com/cubefs/cubefs/console/proto"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/http_client"
	"github.com/cubefs/cubefs/util/log"
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
	case cproto.OpDataNodeExtentRepairTask:
		result = append(result, cproto.FormatOperationNilData(operation, "uint64", "string", "string"))

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
	var params []map[string]string
	for _, metric := range metrics {
		var (
			param   map[string]string
			isEmpty bool
		)
		param, isEmpty, err = cproto.ParseValueMetricsToParams(operation, metric)
		if err != nil || isEmpty {
			continue
		}
		_, err = cproto.ParseValueMetricsToArgs(operation, metric)
		if err != nil {
			return err
		}
		params = append(params, param)
	}

	switch operation {
	case cproto.OpDataNodeExtentRepairTask:
		if !skipXbp {
			return cli.createXbpApply(ctx, cluster, cproto.DataNodeModuleType, operation, metrics, nil, nil, true)
		}
		return cli.batchStartExtentRepairTask(cluster, operation, params)

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

func (cli *CliService) batchStartExtentRepairTask(cluster string, operation int, params []map[string]string) error {
	var errResult error
	mc := cli.api.GetMasterClient(cluster)
	for _, param := range params {
		var (
			dHost  string
			pid    uint64
			extIDs []uint64
		)
		for _, baseMetric := range cproto.GetCliOperationBaseMetrics(operation) {
			switch baseMetric.ValueName {
			case "extentID":
				extentsStrList := strings.Split(param[baseMetric.ValueName], "-")
				for _, idStr := range extentsStrList {
					if eid, err := strconv.ParseUint(idStr, 10, 64); err != nil {
						return fmt.Errorf("解析extID失败：%v %v", idStr, err)
					} else {
						extIDs = append(extIDs, eid)
					}
				}

			case "host":
				dHost = fmt.Sprintf("%s:%v", strings.Split(param[baseMetric.ValueName], ":"), mc.DataNodeProfPort)

			case "pid":
				id, err := strconv.ParseUint(param[baseMetric.ValueName], 10, 64)
				if err != nil {
					return fmt.Errorf("解析分片ID失败：%v %v", param[baseMetric.ValueName], err)
				} else {
					pid = id
				}
			}
		}
		err := cli.RepairExtents(cluster, dHost, pid, extIDs)
		if err != nil {
			errResult = fmt.Errorf("%v, %v", errResult, err)
		}
	}
	return errResult
}

func (cli *CliService) RepairExtents(cluster string, host string, partitionID uint64, extentIDs []uint64) (err error) {
	var dp *proto.DataPartitionInfo
	if partitionID < 0 || len(extentIDs) == 0 {
		return
	}
	mc := cli.api.GetMasterClient(cluster)
	dp, err = mc.AdminAPI().GetDataPartition("", partitionID)
	if err != nil {
		return
	}
	var exist bool
	for _, h := range dp.Hosts {
		if h == host {
			exist = true
			break
		}
	}
	if !exist {
		err = fmt.Errorf("host[%v] not exist in hosts[%v]", host, dp.Hosts)
		return
	}
	dHost := fmt.Sprintf("%v:%v", strings.Split(host, ":")[0], mc.DataNodeProfPort)
	dataClient := http_client.NewDataClient(dHost, false)
	partition, err := dataClient.GetPartitionFromNode(partitionID)
	if err != nil {
		return
	}
	partitionPath := fmt.Sprintf("datapartition_%v_%v", partitionID, dp.Replicas[0].Total)
	var extMap map[uint64]string
	if len(extentIDs) == 1 {
		err = dataClient.RepairExtent(extentIDs[0], partition.Path, partitionID)
	} else {
		extentsStrs := make([]string, 0)
		for _, e := range extentIDs {
			extentsStrs = append(extentsStrs, strconv.FormatUint(e, 10))
		}
		extMap, err = dataClient.RepairExtentBatch(strings.Join(extentsStrs, "-"), partition.Path, partitionID)
	}
	if err != nil {
		if _, e := dataClient.GetPartitionFromNode(partitionID); e == nil {
			return
		}
		for i := 0; i < 3; i++ {
			if e := dataClient.ReLoadPartition(partitionPath, strings.Split(partition.Path, "/datapartition")[0]); e == nil {
				break
			}
		}
		return
	}
	if len(extMap) > 0 {
		fmt.Printf("repair result: %v\n", extMap)
	}
	return nil
}
