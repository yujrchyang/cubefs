package cli

import (
	"context"
	"fmt"
	cproto "github.com/cubefs/cubefs/console/proto"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"strconv"
	"strings"
)

func (cli *CliService) GetBatchConfigList(cluster string, operation int) (result [][]*cproto.CliValueMetric, err error) {
	defer func() {
		msg := fmt.Sprintf("GetBatchConfigList: cluster[%v] operation(%v)", cluster, cproto.GetOpShortMsg(operation))
		if err != nil {
			log.LogErrorf("%s err(%v)", msg, err)
		}
	}()

	result = make([][]*cproto.CliValueMetric, 0)
	switch operation {
	case cproto.OpBatchSetVolForceROW:
		result = append(result, cproto.FormatArgsToValueMetrics(operation, "", false))

	case cproto.OpBatchSetVolWriteCache:
		result = append(result, cproto.FormatArgsToValueMetrics(operation, "", false))

	case cproto.OpBatchSetVolInodeReuse:
		result = append(result, cproto.FormatArgsToValueMetrics(operation, "", false))

	case cproto.OpBatchSetVolConnConfig:
		result = append(result, cproto.FormatOperationNilData(operation, "string", "int64", "int64"))

	case cproto.OpBatchSetVolDpSelector:
		result = append(result, cproto.FormatOperationNilData(operation, "string", "string", "int64"))

	case cproto.OpBatchSetVolReplicaNum:
		result = append(result, cproto.FormatOperationNilData(operation, "string", "int64"))

	case cproto.OpBatchSetVolFollowerReadCfg:
		result = append(result, cproto.FormatOperationNilData(operation, "string", "int64", "int64"))

	default:
		err = fmt.Errorf("undefined operation code: %v:%v", operation, cproto.GetOpShortMsg(operation))
	}
	return
}

func (cli *CliService) SetBatchConfigList(ctx context.Context, cluster string, operation int, metrics [][]*cproto.CliValueMetric, skipXbp bool) (err error) {
	defer func() {
		msg := fmt.Sprintf("SetBatchConfigList: cluster[%v] operation(%v) metrics(%v) ", cluster, cproto.GetOpShortMsg(operation), metrics)
		if err != nil {
			log.LogErrorf("%s err(%v)", msg, err)
		} else {
			log.LogInfof("%s", msg)
		}
	}()
	isRelease := cproto.IsRelease(cluster)

	var getArgsFunc = func(op int, input [][]*cproto.CliValueMetric) ([]map[string]interface{}, error) {
		args := make([]map[string]interface{}, 0)
		noEmpty := getNoEmptyBatchMetrics(op, input)
		if len(noEmpty) < 1 {
			return nil, ErrEmptyInputMetrics
		}
		for _, metric := range noEmpty {
			arg, e := cproto.ParseValueMetricsToArgs(operation, metric)
			if e != nil {
				return nil, e
			}
			args = append(args, arg)
		}
		return args, nil
	}

	switch operation {
	case cproto.OpBatchSetVolForceROW:
		if isRelease {
			return ErrUnSupportOperation
		}
		argList, e := getArgsFunc(operation, metrics)
		if e != nil {
			return e
		}
		volArgs := make(map[string]bool)
		for _, args := range argList {
			volList, forceRow, e1 := getBatchUpdateVolForceROWArgs(args, operation)
			if e1 != nil {
				return e1
			}
			for _, volName := range volList {
				// 如果一个vol在两个批里，以后面设置的为准
				volArgs[volName] = forceRow
			}
		}
		if !skipXbp {
			return cli.createXbpApply(ctx, cluster, cproto.BatchModuleType, operation, metrics, nil, nil, true)
		}
		err = cli.batchUpdateVolForceROW(cluster, volArgs)

	case cproto.OpBatchSetVolWriteCache:
		if isRelease {
			return ErrUnSupportOperation
		}
		argList, e := getArgsFunc(operation, metrics)
		if e != nil {
			return e
		}
		volArgs := make(map[string]bool)
		for _, args := range argList {
			volList, writeCache, e1 := getBatchUpdateVolWriteCacheArgs(args, operation)
			if e1 != nil {
				return e1
			}
			for _, volName := range volList {
				volArgs[volName] = writeCache
			}
		}
		if !skipXbp {
			return cli.createXbpApply(ctx, cluster, cproto.BatchModuleType, operation, metrics, nil, nil, true)
		}
		err = cli.batchUpdateVolWriteCache(cluster, volArgs)

	case cproto.OpBatchSetVolInodeReuse:
		volArgs := make(map[string]bool)
		argList, e := getArgsFunc(operation, metrics)
		if e != nil {
			return e
		}
		for _, args := range argList {
			volList, reuseEnable, e1 := getBatchUpdateVolInodeReuseArgs(args, operation)
			if e1 != nil {
				return e1
			}
			for _, volName := range volList {
				volArgs[volName] = reuseEnable
			}
		}
		if !skipXbp {
			return cli.createXbpApply(ctx, cluster, cproto.BatchModuleType, operation, metrics, nil, nil, true)
		}
		err = cli.batchUpdateVolInodeReuse(cluster, volArgs)

	case cproto.OpBatchSetVolConnConfig:
		argList, e := getArgsFunc(operation, metrics)
		if e != nil {
			return e
		}
		for _, args := range argList {
			volList, readTimeout, writeTimeout, e1 := getBatchUpdateVolConnConfigArgs(args, operation)
			if e1 != nil {
				return e1
			}
			if skipXbp {
				if err = cli.batchUpdateVolConnConfig(cluster, volList, readTimeout*1e6, writeTimeout*1e6); err != nil {
					return
				}
			}
		}
		if !skipXbp {
			return cli.createXbpApply(ctx, cluster, cproto.BatchModuleType, operation, metrics, nil, nil, true)
		}

	case cproto.OpBatchSetVolDpSelector:
		argList, e := getArgsFunc(operation, metrics)
		if e != nil {
			return e
		}
		for _, args := range argList {
			volList, dpSelectorName, dpSelectorParm, e1 := getBatchUpdateVolDpSelectorArgs(args, operation)
			if e1 != nil {
				return e1
			}
			if skipXbp {
				if err = cli.batchUpdateVolDpSelector(cluster, volList, dpSelectorName, dpSelectorParm); err != nil {
					return err
				}
			}
		}
		if !skipXbp {
			return cli.createXbpApply(ctx, cluster, cproto.BatchModuleType, operation, metrics, nil, nil, true)
		}

	case cproto.OpBatchSetVolReplicaNum:
		argList, e := getArgsFunc(operation, metrics)
		if e != nil {
			return e
		}
		for _, args := range argList {
			volList, replicaNum, e1 := getBatchSetVolumeReplicaNumArgs(args, operation)
			if e1 != nil {
				return e1
			}
			if skipXbp {
				if err = cli.batchSetVolReplicaNum(cluster, volList, int(replicaNum)); err != nil {
					return err
				}
			}
		}
		if !skipXbp {
			return cli.createXbpApply(ctx, cluster, cproto.BatchModuleType, operation, metrics, nil, nil, true)
		}

	case cproto.OpBatchSetVolFollowerReadCfg:
		argList, e := getArgsFunc(operation, metrics)
		if e != nil {
			return e
		}
		for _, args := range argList {
			volList, weight, interval, e1 := getBatchSetVolFollowerReadCfgArgs(args, operation)
			if e1 != nil {
				return e1
			}
			if skipXbp {
				if err = cli.batchSetVolFollowerReadCfg(cluster, volList, weight, interval); err != nil {
					return err
				}
			}
		}
		if !skipXbp {
			return cli.createXbpApply(ctx, cluster, cproto.BatchModuleType, operation, metrics, nil, nil, true)
		}

	default:
	}
	return
}

func getNoEmptyBatchMetrics(operation int, metrics [][]*cproto.CliValueMetric) (noEmptyMetrics [][]*cproto.CliValueMetric) {
	noEmptyMetrics = make([][]*cproto.CliValueMetric, 0)
	for _, metric := range metrics {
		var isEmpty bool
		for _, valueMetric := range metric {
			if valueMetric.ValueName == "volume" && valueMetric.Value == "" {
				isEmpty = true
				break
			}
		}
		if !isEmpty {
			noEmptyMetrics = append(noEmptyMetrics, metric)
		}
	}
	return noEmptyMetrics
}

func getBatchUpdateVolForceROWArgs(args map[string]interface{}, operation int) (volList []string, forceROW bool, err error) {
	for _, baseMetric := range cproto.GetCliOperationBaseMetrics(operation) {
		switch baseMetric.ValueName {
		case "volume":
			volListStr := args[baseMetric.ValueName].(string)
			if volListStr == "" {
				err = fmt.Errorf("请指定要设置的vol！")
				return
			}
			volList = strings.Split(volListStr, ",")

		case "forceROW":
			forceROW = args[baseMetric.ValueName].(bool)
		}
	}
	return
}

func (cli *CliService) batchUpdateVolForceROW(cluster string, volArgs map[string]bool) error {
	log.LogInfof("test: volList: %v", volArgs)
	var errResult error
	var params = make(map[string]string)
	for vol, forceRow := range volArgs {
		forceROWKey := "forceROW"
		params[forceROWKey] = strconv.FormatBool(forceRow)
		err := cli.updateVolume(cluster, vol, params)
		if err != nil {
			errResult = fmt.Errorf("%v, %v", errResult, err)
		}
		delete(params, forceROWKey)
	}
	return errResult
}

func getBatchUpdateVolWriteCacheArgs(args map[string]interface{}, operation int) (volList []string, writeCache bool, err error) {
	for _, baseMetric := range cproto.GetCliOperationBaseMetrics(operation) {
		switch baseMetric.ValueName {
		case "volume":
			volListStr := args[baseMetric.ValueName].(string)
			if volListStr == "" {
				err = fmt.Errorf("请指定要设置的vol！")
				return
			}
			volList = strings.Split(volListStr, ",")

		case "writeCache":
			writeCache = args[baseMetric.ValueName].(bool)
		}
	}
	return
}

func (cli *CliService) batchUpdateVolWriteCache(cluster string, volArgs map[string]bool) error {
	log.LogInfof("test: volList: %v", volArgs)
	var errResult error
	var params = make(map[string]string)
	for vol, writeCache := range volArgs {
		writeCacheKey := "writeCache"
		params[writeCacheKey] = strconv.FormatBool(writeCache)
		err := cli.updateVolume(cluster, vol, params)
		if err != nil {
			errResult = fmt.Errorf("%v, %v", errResult, err)
		}
		delete(params, writeCacheKey)
	}
	return errResult
}

func getBatchUpdateVolInodeReuseArgs(args map[string]interface{}, operation int) (volList []string, enableReuse bool, err error) {
	for _, baseMetric := range cproto.GetCliOperationBaseMetrics(operation) {
		switch baseMetric.ValueName {
		case "volume":
			volListStr := args[baseMetric.ValueName].(string)
			if volListStr == "" {
				err = fmt.Errorf("请指定要设置的vol！")
				return
			}
			volList = strings.Split(volListStr, ",")

		case proto.EnableBitMapAllocatorKey:
			enableReuse = args[baseMetric.ValueName].(bool)
		}
	}
	return
}

func (cli *CliService) batchUpdateVolInodeReuse(cluster string, volArgs map[string]bool) error {
	log.LogInfof("test: volList: %v", volArgs)
	var errResult error
	var params = make(map[string]string)
	for vol, reuseEnable := range volArgs {
		var err error
		params[proto.EnableBitMapAllocatorKey] = strconv.FormatBool(reuseEnable)
		if cproto.IsRelease(cluster) {
			err = cli.updateVolumeRelease(cluster, vol, params)
		} else {
			err = cli.updateVolume(cluster, vol, params)
		}
		if err != nil {
			errResult = fmt.Errorf("%v, %v", errResult, err)
		}
		delete(params, proto.EnableBitMapAllocatorKey)
	}
	return errResult
}

func getBatchUpdateVolConnConfigArgs(args map[string]interface{}, operation int) (volList []string, readTimeout, writeTimeout int64, err error) {
	for _, baseMetric := range cproto.GetCliOperationBaseMetrics(operation) {
		switch baseMetric.ValueName {
		case "volume":
			volListStr := args[baseMetric.ValueName].(string)
			if volListStr == "" {
				err = fmt.Errorf("请指定要设置的vol！")
				return
			}
			volList = strings.Split(volListStr, ",")

		case "readConnTimeout":
			readTimeout = args[baseMetric.ValueName].(int64)
			if readTimeout < 0 {
				err = fmt.Errorf("请输入>=0的值！")
				return
			}

		case "writeConnTimeout":
			writeTimeout = args[baseMetric.ValueName].(int64)
			if writeTimeout < 0 {
				err = fmt.Errorf("请输入>=0的值！")
				return
			}
		}
	}
	return
}

func (cli *CliService) batchUpdateVolConnConfig(cluster string, volList []string, readTimeout int64, writeTimeout int64) error {
	var errResult error
	var params = make(map[string]string)
	if readTimeout > 0 {
		params["readConnTimeout"] = strconv.FormatInt(readTimeout, 10)
	}
	if writeTimeout > 0 {
		params["writeConnTimeout"] = strconv.FormatInt(writeTimeout, 10)
	}
	for _, vol := range volList {
		var err error
		if cproto.IsRelease(cluster) {
			err = cli.updateVolumeRelease(cluster, vol, params)
		} else {
			err = cli.updateVolume(cluster, vol, params)
		}
		if err != nil {
			errResult = fmt.Errorf("%v, %v", errResult, err)
		}
	}
	return errResult
}

func getBatchUpdateVolDpSelectorArgs(args map[string]interface{}, operation int) (volList []string, dpSelectorName string, dpSelectorParm int64, err error) {
	for _, baseMetric := range cproto.GetCliOperationBaseMetrics(operation) {
		switch baseMetric.ValueName {
		case "volume":
			volListStr := args[baseMetric.ValueName].(string)
			if volListStr == "" {
				err = fmt.Errorf("请指定要设置的vol！")
				return
			}
			volList = strings.Split(volListStr, ",")

		case "dpSelectorName":
			dpSelectorName = args[baseMetric.ValueName].(string)
			if !(dpSelectorName == "default" || dpSelectorName == "kfaster") {
				err = fmt.Errorf("请在default/kfaster中选择！")
				return
			}

		case "dpSelectorParm":
			dpSelectorParm = args[baseMetric.ValueName].(int64)
			if dpSelectorName == "kfaster" && (dpSelectorParm <= 0 || dpSelectorParm >= 100) {
				err = fmt.Errorf("请输入(0, 100)之间的值！")
				return
			}
		}
	}
	return
}

func (cli *CliService) batchUpdateVolDpSelector(cluster string, volList []string, dpSelectorName string, dpSelectorParm int64) error {
	var errResult error
	var params = make(map[string]string)
	params["dpSelectorName"] = dpSelectorName
	if dpSelectorParm > 0 {
		params["dpSelectorParm"] = strconv.FormatInt(dpSelectorParm, 10)
	} else {
		params["dpSelectorParm"] = "0"
	}
	for _, vol := range volList {
		var err error
		if cproto.IsRelease(cluster) {
			err = cli.updateVolumeRelease(cluster, vol, params)
		} else {
			err = cli.updateVolume(cluster, vol, params)
		}
		if err != nil {
			errResult = fmt.Errorf("%v, %v", errResult, err)
		}
	}
	return errResult
}

func getBatchSetVolumeReplicaNumArgs(args map[string]interface{}, operation int) (volList []string, replicaNum int64, err error) {
	for _, baseMetric := range cproto.GetCliOperationBaseMetrics(operation) {
		switch baseMetric.ValueName {
		case "volume":
			volListStr := args[baseMetric.ValueName].(string)
			if volListStr == "" {
				err = fmt.Errorf("请指定要设置的vol！")
				return
			}
			volList = strings.Split(volListStr, ",")

		case "replicaNum":
			replicaNum = args[baseMetric.ValueName].(int64)
			if !(replicaNum == 2 || replicaNum == 3 || replicaNum == 5) {
				err = fmt.Errorf("副本数为2或3或5！")
				return
			}
		}
	}
	return
}

func (cli *CliService) batchSetVolReplicaNum(cluster string, volList []string, replicaNum int) error {
	var errResult error
	var params = make(map[string]string)
	params["replicaNum"] = strconv.Itoa(replicaNum)

	for _, vol := range volList {
		var err error
		if cproto.IsRelease(cluster) {
			err = cli.updateVolumeRelease(cluster, vol, params)
		} else {
			err = cli.updateVolume(cluster, vol, params)
		}
		if err != nil {
			errResult = fmt.Errorf("%v, %v", errResult, err)
		}
	}
	return errResult
}

func getBatchSetVolFollowerReadCfgArgs(args map[string]interface{}, operation int) (volList []string, weight, interval int64, err error) {
	for _, baseMetric := range cproto.GetCliOperationBaseMetrics(operation) {
		switch baseMetric.ValueName {
		case "volume":
			volListStr := args[baseMetric.ValueName].(string)
			if volListStr == "" {
				err = fmt.Errorf("请指定要设置的vol！")
				return
			}
			volList = strings.Split(volListStr, ",")

		case "follReadHostWeight":
			weight = args[baseMetric.ValueName].(int64)
			if !(weight > 0 && weight < 100) {
				err = fmt.Errorf("请输入0～100之间的数")
				return
			}

		case "hostDelayInterval":
			interval = args[baseMetric.ValueName].(int64)
		}
	}
	return
}

func (cli *CliService) batchSetVolFollowerReadCfg(cluster string, volList []string, lowestDelayHostWeight, collectInterval int64) error {
	var errResult error
	var params = make(map[string]string)
	params["follReadHostWeight"] = strconv.FormatInt(lowestDelayHostWeight, 10)
	params["hostDelayInterval"] = strconv.FormatInt(collectInterval, 10)

	for _, vol := range volList {
		var err error
		if cproto.IsRelease(cluster) {
			err = cli.updateVolumeRelease(cluster, vol, params)
		} else {
			err = cli.updateVolume(cluster, vol, params)
		}
		if err != nil {
			errResult = fmt.Errorf("%v, %v", errResult, err)
		}
	}
	return errResult
}
