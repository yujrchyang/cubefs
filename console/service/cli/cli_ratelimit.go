package cli

import (
	"context"
	"fmt"
	"github.com/cubefs/cubefs/util/multirate"
	"strconv"
	"strings"

	cproto "github.com/cubefs/cubefs/console/proto"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

func (cli *CliService) GetRateLimitConfig(cluster string, operation int) (result []*cproto.CliValueMetric, err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("GetRateLimitConfig failed: cluster[%v] operation:%v err:%v", cluster, cproto.GetOpShortMsg(operation), err)
		}
	}()
	switch operation {
	case cproto.OpDataNodeDeleteRateLimit:
		dataNodeDeleteLimitRate, err := cli.getDataNodeDeleteRateLimit(cluster)
		if err != nil {
			return nil, err
		}
		result = cproto.FormatArgsToValueMetrics(operation, dataNodeDeleteLimitRate)
	case cproto.OpSetBandwidthLimiter:
		bandwidthLimit, err := cli.getBandwidthLimit(cluster)
		if err != nil {
			return nil, err
		}
		result = cproto.FormatArgsToValueMetrics(operation, bandwidthLimit)

	default:
		err = fmt.Errorf("undefined operation code: %v:%v", operation, cproto.GetOpShortMsg(operation))
	}
	return
}

func (cli *CliService) SetRatelimitConfig(ctx context.Context, cluster string, operation int, metrics []*cproto.CliValueMetric, skipXbp bool) (err error) {
	defer func() {
		msg := fmt.Sprintf("SetRatelimitConfig: cluster[%v] operation(%v:%v)", cluster, operation, cproto.GetOpShortMsg(operation))
		if err != nil {
			log.LogErrorf("%s, err(%v)", msg, err)
		} else {
			log.LogInfof("%s, metrics:%v", msg, metrics)
			cli.api.UpdateClusterViewCache(cluster)
			cli.api.UpdateLimitInfoCache(cluster)
		}
	}()

	var args map[string]interface{}
	args, err = cproto.ParseValueMetricsToArgs(operation, metrics)
	if err != nil {
		return
	}
	var params map[string]string
	params, _, err = cproto.ParseValueMetricsToParams(operation, metrics)
	if err != nil {
		return
	}

	switch operation {
	case cproto.OpDataNodeDeleteRateLimit:
		if !skipXbp {
			goto createXbpApply
		}
		return cli.api.SetRatelimitInfo(cluster, params)

	case cproto.OpSetBandwidthLimiter:
		bwBytes := args[metrics[0].ValueName].(uint64)
		if bwBytes > 0 && bwBytes < 100*1024*1024 {
			return fmt.Errorf("带宽限速不能<100MB")
		}
		if !skipXbp {
			goto createXbpApply
		}
		return cli.setBandwidthLimiter(cluster, bwBytes)

	default:
		err = fmt.Errorf("undefined operation code: %v:%v", operation, cproto.GetOpShortMsg(operation))
		return
	}

createXbpApply:
	return cli.createXbpApply(ctx, cluster, cproto.RateLimitModuleType, operation, [][]*cproto.CliValueMetric{metrics}, nil, nil, false)
}

func (cli *CliService) GetRatelimitConfigList(cluster string, operation int) (result [][]*cproto.CliValueMetric, err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("GetRateLimitConfigList failed: cluster[%v] operation:%v err:%v", cproto.GetOpShortMsg(operation), err)
		}
	}()

	result = make([][]*cproto.CliValueMetric, 0)

	switch operation {
	case cproto.OpClientReadVolRateLimit:
		clientReadVolRateLimitMap, err := cli.getClientReadVolRateLimitMap(cluster)
		if err != nil {
			return nil, err
		}
		if len(clientReadVolRateLimitMap) == 0 {
			result = append(result, cproto.FormatOperationNilData(operation, "string", "uint64"))
		}
		for vol, readLimit := range clientReadVolRateLimitMap {
			result = append(result, cproto.FormatArgsToValueMetrics(operation, vol, readLimit))
		}

	case cproto.OpClientWriteVolRateLimit:
		clientWriteVolRateLimitMap, err := cli.getClientWriteVolRateLimitMap(cluster)
		if err != nil {
			return nil, err
		}
		if len(clientWriteVolRateLimitMap) == 0 {
			result = append(result, cproto.FormatOperationNilData(operation, "string", "uint64"))
		}
		for vol, writeLimit := range clientWriteVolRateLimitMap {
			result = append(result, cproto.FormatArgsToValueMetrics(operation, vol, writeLimit))
		}

	case cproto.OpObjectNodeActionRateLimit:
		objectNodeActionRateLimit, err := cli.getObjectNodeActionRateLimit(cluster)
		if err != nil {
			return nil, err
		}
		if len(objectNodeActionRateLimit) == 0 {
			result = append(result, cproto.FormatOperationNilData(operation, "string", "string", "uint64"))
		}
		for action, limit := range objectNodeActionRateLimit {
			result = append(result, cproto.FormatArgsToValueMetrics(operation, "", action, limit))
		}

	case cproto.OpFlashNodeZoneRate:
		flashNodeLimitMap, err := cli.getFlashNodeLimitMap(cluster)
		if err != nil {
			return nil, err
		}
		if len(flashNodeLimitMap) == 0 {
			result = append(result, cproto.FormatOperationNilData(operation, "string", "uint64"))
		}
		for zone, limit := range flashNodeLimitMap {
			result = append(result, cproto.FormatArgsToValueMetrics(operation, zone, limit))
		}

	case cproto.OpFlashNodeVolRate:
		flashNodeVolLimitMap, err := cli.getFlashNodeVolLimitMap(cluster)
		if err != nil {
			return nil, err
		}
		if len(flashNodeVolLimitMap) == 0 {
			result = append(result, cproto.FormatOperationNilData(operation, "string", "string", "uint64"))
		}
		for zone, volLimit := range flashNodeVolLimitMap {
			for vol, limit := range volLimit {
				result = append(result, cproto.FormatArgsToValueMetrics(operation, zone, vol, limit))
			}
		}

	case cproto.OpApiReqBwRateLimit:
		apiLimitMap, err := cli.getApiReqBwLimitMap(cluster)
		if err != nil {
			return nil, err
		}
		if len(apiLimitMap) == 0 {
			result = append(result, cproto.FormatOperationNilData(operation, "uint8", "int64"))
		}
		for apiOp, limit := range apiLimitMap {
			result = append(result, cproto.FormatArgsToValueMetrics(operation, apiOp, limit))
		}

	case cproto.OpDatanodeRateLimit:
		result, err = cli.getLimitMetricsByModule(cluster, cproto.RoleNameDataNode, operation)

	case cproto.OpMetanodeRateLimit:
		result, err = cli.getLimitMetricsByModule(cluster, cproto.RoleNameMetaNode, operation)

	case cproto.OpFlashnodeRateLimit:
		result, err = cli.getLimitMetricsByModule(cluster, cproto.RoleNameFlashNode, operation)

	default:
		err = fmt.Errorf("undefined operation code: %v:%v", operation, cproto.GetOpShortMsg(operation))
	}
	return
}

func (cli *CliService) SetRatelimitConfigList(ctx context.Context, cluster string, operation int, metrics [][]*cproto.CliValueMetric, skipXbp bool) (result [][]*cproto.CliValueMetric, err error) {
	defer func() {
		msg := fmt.Sprintf("SetRatelimitConfigList: cluster[%v] operation(%v) metrics(%v)", cluster, cproto.GetOpShortMsg(operation), metrics)
		if err != nil {
			log.LogErrorf("%s err: %v", msg, err)
		} else {
			log.LogInfof("%s", msg)
			cli.api.UpdateLimitInfoCache(cluster)
		}
	}()

	isRelease := cproto.IsRelease(cluster)
	// todo: 前端区分编辑和新增后可remove,
	switch operation {
	case cproto.OpFlashnodeRateLimit, cproto.OpDatanodeRateLimit, cproto.OpMetanodeRateLimit, cproto.OpFlashNodeZoneRate, cproto.OpFlashNodeVolRate:
		metrics, err = cli.getChangedMetrics(metrics, cluster, operation)
		if err != nil {
			return
		}
	default:
	}
	result = metrics

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
	case cproto.OpClientReadVolRateLimit, cproto.OpClientWriteVolRateLimit:
		for _, arg := range args {
			for _, baseMetric := range cproto.GetCliOperationBaseMetrics(operation) {
				switch baseMetric.ValueName {
				case "volume":
					volume := arg[baseMetric.ValueName].(string)
					if volume == "" {
						return nil, fmt.Errorf("请指定要限速的vol！")
					}
				case "clientReadVolRate":
					ratelimit := arg[baseMetric.ValueName].(uint64)
					if ratelimit != 0 && ratelimit < 100 {
						return nil, fmt.Errorf("请输入 >100 的限速值")
					}
				}
			}
		}
		if !skipXbp {
			goto createXbpApply
		}
		err = cli.batchSetRateLimit(cluster, operation, params)
		return

	case cproto.OpObjectNodeActionRateLimit:
		if isRelease {
			err = ErrUnSupportOperation
			return
		}
		if !skipXbp {
			goto createXbpApply
		}
		err = cli.batchSetRateLimit(cluster, operation, params)
		return

	case cproto.OpFlashNodeZoneRate:
		if isRelease {
			err = ErrUnSupportOperation
			return
		}
		if !skipXbp {
			goto createXbpApply
		}
		err = cli.batchSetRateLimit(cluster, operation, params)
		return

	case cproto.OpFlashNodeVolRate:
		if isRelease {
			err = ErrUnSupportOperation
			return
		}
		if !skipXbp {
			goto createXbpApply
		}
		err = cli.batchSetRateLimit(cluster, operation, params)
		return

	case cproto.OpApiReqBwRateLimit:
		for _, arg := range args {
			for _, baseMetric := range cproto.GetCliOperationBaseMetrics(operation) {
				switch baseMetric.ValueName {
				case "apiReqBwRate":
					ratelimit := arg[baseMetric.ValueName].(int64)
					if ratelimit != 0 && ratelimit < 100 {
						return nil, fmt.Errorf("请输入 >100 的限速值")
					}
				}
			}
		}
		if !skipXbp {
			goto createXbpApply
		}
		err = cli.batchSetRateLimit(cluster, operation, params)
		return

	case cproto.OpFlashnodeRateLimit:
		if isRelease {
			err = ErrUnSupportOperation
			return
		}
		if !skipXbp {
			goto createXbpApply
		}
		err = cli.batchSetRateLimit(cluster, operation, params)
		return

	case cproto.OpDatanodeRateLimit:
		if !skipXbp {
			goto createXbpApply
		}
		err = cli.batchSetRateLimit(cluster, operation, params)
		return

	case cproto.OpMetanodeRateLimit:
		if !skipXbp {
			goto createXbpApply
		}
		err = cli.batchSetRateLimit(cluster, operation, params)
		return

	default:
		err = fmt.Errorf("undefined operation code: %v:%v", operation, cproto.GetOpShortMsg(operation))
	}

createXbpApply:
	err = cli.createXbpApply(ctx, cluster, cproto.RateLimitModuleType, operation, result, nil, nil, true)
	return
}

func (cli *CliService) getChangedMetrics(metrics [][]*cproto.CliValueMetric, cluster string, operation int) ([][]*cproto.CliValueMetric, error) {
	var (
		result = make([][]*cproto.CliValueMetric, 0)
	)
	switch operation {
	case cproto.OpFlashnodeRateLimit, cproto.OpDatanodeRateLimit, cproto.OpMetanodeRateLimit:
		originRateLimit := make(map[string]int64, 0)
		rateLimit, err := cli.getRateLimit(cluster)
		if err != nil {
			log.LogErrorf("getChangedMetrics: get rateLimitInfo from cache failed: err(%v)", err)
			return nil, err
		}
		zoneVolOpMap := getRatelimitByModule(rateLimit, moduleByOperation(operation))
		if len(zoneVolOpMap) == 0 {
			return metrics, nil
		}
		originRateLimit = formatRateLimitToMap(zoneVolOpMap)
		newMetrics := make(map[string]int64)
		// 增 改
		for _, oneMetric := range metrics {
			key, limit, empty, e1 := getMetricsKeyValue(oneMetric, operation)
			if e1 != nil {
				return nil, e1
			}
			if empty {
				continue
			}
			newMetrics[key] = limit
			if value, ok := originRateLimit[key]; ok && value == limit {
				continue
			}
			result = append(result, oneMetric)
			log.LogInfof("add metric: cluster(%v) op(%v) metric(%v)", cluster, cproto.GetOpShortMsg(operation), oneMetric)
		}

	case cproto.OpFlashNodeZoneRate, cproto.OpFlashNodeVolRate:
		if cproto.IsRelease(cluster) {
			return nil, ErrUnSupportOperation
		}
		originRateLimit := make(map[string]uint64)
		limitInfo, err := cli.api.GetLimitInfoCache(cluster, false)
		if err != nil {
			return nil, err
		}
		if operation == cproto.OpFlashNodeZoneRate {
			for zone, limit := range limitInfo.FlashNodeLimitMap {
				originRateLimit[zone] = limit
			}
		}
		if operation == cproto.OpFlashNodeVolRate {
			for zone, volLimitMap := range limitInfo.FlashNodeVolLimitMap {
				for vol, limit := range volLimitMap {
					originRateLimit[zone+":"+vol] = limit
				}
			}
		}
		newMetrics := make(map[string]int64)
		for _, oneMetric := range metrics {
			key, limit, empty, e1 := getMetricsKeyValue(oneMetric, operation)
			if e1 != nil {
				return nil, e1
			}
			if empty {
				continue
			}
			newMetrics[key] = limit
			if value, ok := originRateLimit[key]; ok && value == uint64(limit) {
				continue
			}
			result = append(result, oneMetric)
			log.LogInfof("add metric: cluster(%v) op(%v) metric(%v)", cluster, cproto.GetOpShortMsg(operation), oneMetric)
		}
	}
	// 删
	//for key, _ := range originRateLimit {
	//	if _, ok := newMetrics[key]; !ok {
	//		// 新的里面没有 证明该限速被删除了 值设为0
	//		// 顺序： zone vol op index val
	//		params := strings.Split(key, ":")
	//		if len(params) != 4 {
	//			err = fmt.Errorf("parse rateLimitKey failed: params field is not 4")
	//			log.LogErrorf("getChangedMetrics: cluster(%v) module(%v) op(%v) metrics(%v) err(%v)", cluster, module,
	//				cproto.GetOpShortMsg(operation), metrics, err)
	//			return nil, err
	//		}
	//		op, _ := strconv.Atoi(params[2])
	//		index, _ := strconv.Atoi(params[3])
	//		delMetric := cproto.FormatArgsToValueMetrics(operation, params[0], params[1], op, index, 0)
	//		result = append(result, delMetric)
	//		log.LogInfof("del metric: cluster(%v) module(%v) op(%v) metric(%v)", cluster, module, cproto.GetOpShortMsg(operation), delMetric)
	//	}
	//}
	if log.IsDebugEnabled() {
		log.LogDebugf("getChangedMetrics: input(len)=%v output(len)=%v res(%v)", len(metrics), len(result), result)
	}
	return result, nil
}

func getMetricsKeyValue(metrics []*cproto.CliValueMetric, operation int) (key string, value int64, empty bool, err error) {
	switch operation {
	case cproto.OpFlashnodeRateLimit, cproto.OpDatanodeRateLimit, cproto.OpMetanodeRateLimit:
		return formatLimitMetricsKeyValue(metrics)
	case cproto.OpFlashNodeZoneRate:
		empty = true
		for _, metric := range metrics {
			if metric.Value != "" {
				empty = false
			}
			switch metric.ValueName {
			case "zoneName":
				key = strings.TrimSpace(metric.Value)
			case proto.FlashNodeRateKey:
				var val uint64
				val, err = strconv.ParseUint(metric.Value, 10, 64)
				value = int64(val)
			}
		}
		if empty {
			return
		}

	case cproto.OpFlashNodeVolRate:
		empty = true
		var zone string
		var vol string
		for _, metric := range metrics {
			if metric.Value != "" {
				empty = false
			}
			switch metric.ValueName {
			case "zoneName":
				zone = strings.TrimSpace(metric.Value)
			case "volume":
				vol = strings.TrimSpace(metric.Value)
			case proto.FlashNodeRateKey:
				var val uint64
				val, err = strconv.ParseUint(metric.Value, 10, 64)
				value = int64(val)
			}
		}
		if empty {
			return
		}
		key = zone + ":" + vol
	}
	return
}

// zoneName volume opcode proto.RateLimitIndexKey proto.RateLimitKey
func formatLimitMetricsKeyValue(metrics []*cproto.CliValueMetric) (key string, value int64, empty bool, err error) {
	var (
		zone, vol string
		op, index string
		limit     int64
	)
	empty = true
	for _, metric := range metrics {
		if metric.Value != "" {
			empty = false
		}
		switch metric.ValueName {
		case "zoneName":
			zone = strings.TrimSpace(metric.Value)
		case "volume":
			vol = strings.TrimSpace(metric.Value)
		case "opcode":
			op = metric.Value
		case proto.RateLimitIndexKey:
			index = metric.Value
		case proto.RateLimitKey:
			limit, _ = strconv.ParseInt(metric.Value, 10, 64)
		}
	}
	if empty {
		return
	}
	if zone == "" && vol == "" {
		err = fmt.Errorf("zone和volume不能同时为空！")
	}
	if zone != "" && vol != "" {
		err = fmt.Errorf("zone和volume只能选一个配置！")
	}
	var keyBuilder strings.Builder
	keyBuilder.WriteString(zone + ":")
	keyBuilder.WriteString(vol + ":")
	keyBuilder.WriteString(op + ":")
	keyBuilder.WriteString(index)

	key = keyBuilder.String()
	value = limit
	return
}

// zone和vol 都设定成默认值  _ ?
func formatRateLimitToMap(zoneVolOpMap map[string]map[int]proto.AllLimitGroup) map[string]int64 {
	result := make(map[string]int64)
	var (
		zone string
		vol  string
	)
	for zoneOrVol, opMap := range zoneVolOpMap {
		var prefix string
		if strings.HasPrefix(zoneOrVol, multirate.ZonePrefix) {
			zone = strings.TrimPrefix(zoneOrVol, multirate.ZonePrefix)
		} else {
			vol = strings.TrimPrefix(zoneOrVol, multirate.VolPrefix)
		}
		if zone == "" && vol == "" {
			zone = cproto.EmptyZoneVolFlag
		}
		prefix += zone + ":" + vol + ":"

		for opcode, rateLimit := range opMap {
			if !multirate.HaveLimit(rateLimit) {
				continue
			}
			// 只把有值的拿出来
			keyPrefix := prefix + strconv.Itoa(opcode) + ":"
			for limitIndex, limit := range rateLimit {
				if limit > 0 {
					key := keyPrefix + strconv.Itoa(limitIndex)
					result[key] = limit
				}
			}
		}
	}
	if log.IsDebugEnabled() {
		log.LogDebugf("formatRateLimitToMap: %v", result)
	}
	return result
}

func moduleByOperation(operation int) (module string) {
	switch operation {
	case cproto.OpFlashnodeRateLimit:
		module = strings.ToLower(cproto.RoleNameFlashNode)

	case cproto.OpDatanodeRateLimit:
		module = strings.ToLower(cproto.ModuleDataNode)

	case cproto.OpMetanodeRateLimit:
		module = strings.ToLower(cproto.RoleNameMetaNode)

	default:
		return ""
	}
	return
}

func (cli *CliService) getMetaNodeReqRateLimit(cluster string) (uint64, error) {
	if cproto.IsRelease(cluster) {
		limitInfo, err := cli.api.GetLimitInfoCacheRelease(cluster, false)
		if err != nil {
			return 0, err
		}
		return limitInfo.MetaNodeReqRateLimit, nil
	} else {
		limitInfo, err := cli.api.GetLimitInfoCache(cluster, false)
		if err != nil {
			return 0, err
		}
		return limitInfo.MetaNodeReqRateLimit, nil
	}
}

func (cli *CliService) getMetaNodeReqOpRateLimitMap(cluster string) (map[uint8]uint64, error) {
	if cproto.IsRelease(cluster) {
		limitInfo, err := cli.api.GetLimitInfoCacheRelease(cluster, false)
		if err != nil {
			return nil, err
		}
		return limitInfo.MetaNodeReqOpRateLimitMap, nil
	} else {
		limitInfo, err := cli.api.GetLimitInfoCache(cluster, false)
		if err != nil {
			return nil, err
		}
		return limitInfo.MetaNodeReqOpRateLimitMap, nil
	}
}

func (cli *CliService) getMetaNodeReqVolOpRateLimitMap(cluster string) (map[string]map[uint8]uint64, error) {
	if cproto.IsRelease(cluster) {
		return nil, ErrUnSupportOperation
	}
	limitInfo, err := cli.api.GetLimitInfoCache(cluster, false)
	if err != nil {
		return nil, err
	}
	return limitInfo.MetaNodeReqVolOpRateLimitMap, nil
}

func (cli *CliService) getDataNodeDeleteRateLimit(cluster string) (uint64, error) {
	if cproto.IsRelease(cluster) {
		return 0, ErrUnSupportOperation
	}
	limitInfo, err := cli.api.GetLimitInfoCache(cluster, false)
	if err != nil {
		return 0, err
	}
	return limitInfo.DataNodeDeleteLimitRate, nil
}

func (cli *CliService) getDataNodeReqZoneRateLimit(cluster string) (map[string]uint64, error) {
	if cproto.IsRelease(cluster) {
		limitInfo, err := cli.api.GetLimitInfoCacheRelease(cluster, false)
		if err != nil {
			return nil, err
		}
		return limitInfo.DataNodeReqZoneRateLimitMap, nil
	} else {
		limitInfo, err := cli.api.GetLimitInfoCache(cluster, false)
		if err != nil {
			return nil, err
		}
		return limitInfo.DataNodeReqZoneRateLimitMap, nil
	}
}

func (cli *CliService) getDataNodeReqZoneOpRateLimit(cluster string) (map[string]map[uint8]uint64, error) {
	if cproto.IsRelease(cluster) {
		limitInfo, err := cli.api.GetLimitInfoCacheRelease(cluster, false)
		if err != nil {
			return nil, err
		}
		return limitInfo.DataNodeReqZoneOpRateLimitMap, nil
	} else {
		limitInfo, err := cli.api.GetLimitInfoCache(cluster, false)
		if err != nil {
			return nil, err
		}
		return limitInfo.DataNodeReqZoneOpRateLimitMap, nil
	}
}

func (cli *CliService) getDataNodeReqZoneVolOpRateLimitMap(cluster string) (map[string]map[string]map[uint8]uint64, error) {
	if cproto.IsRelease(cluster) {
		limitInfo, err := cli.api.GetLimitInfoCacheRelease(cluster, false)
		if err != nil {
			return nil, err
		}
		return limitInfo.DataNodeReqZoneVolOpRateLimitMap, nil
	} else {
		limitInfo, err := cli.api.GetLimitInfoCache(cluster, false)
		if err != nil {
			return nil, err
		}
		return limitInfo.DataNodeReqZoneVolOpRateLimitMap, nil
	}
}

func (cli *CliService) getClientReadVolRateLimitMap(cluster string) (map[string]uint64, error) {
	if cproto.IsRelease(cluster) {
		limitInfo, err := cli.api.GetLimitInfoCacheRelease(cluster, false)
		if err != nil {
			return nil, err
		}
		return limitInfo.ClientReadVolRateLimitMap, nil
	} else {
		limitInfo, err := cli.api.GetLimitInfoCache(cluster, false)
		if err != nil {
			return nil, err
		}
		return limitInfo.ClientReadVolRateLimitMap, nil
	}
}

func (cli *CliService) getClientWriteVolRateLimitMap(cluster string) (map[string]uint64, error) {
	if cproto.IsRelease(cluster) {
		limitInfo, err := cli.api.GetLimitInfoCacheRelease(cluster, false)
		if err != nil {
			return nil, err
		}
		return limitInfo.ClientWriteVolRateLimitMap, nil
	} else {
		limitInfo, err := cli.api.GetLimitInfoCache(cluster, false)
		if err != nil {
			return nil, err
		}
		return limitInfo.ClientWriteVolRateLimitMap, nil
	}
}

func (cli *CliService) getObjectNodeActionRateLimit(cluster string) (map[string]int64, error) {
	if cproto.IsRelease(cluster) {
		return nil, ErrUnSupportOperation
	}
	limitInfo, err := cli.api.GetLimitInfoCache(cluster, false)
	if err != nil {
		return nil, err
	}
	return limitInfo.ObjectNodeActionRateLimit, nil
}

func (cli *CliService) getFlashNodeLimitMap(cluster string) (map[string]uint64, error) {
	if cproto.IsRelease(cluster) {
		return nil, ErrUnSupportOperation
	}
	limitInfo, err := cli.api.GetLimitInfoCache(cluster, false)
	if err != nil {
		return nil, err
	}
	return limitInfo.FlashNodeLimitMap, nil
}

func (cli *CliService) getFlashNodeVolLimitMap(cluster string) (map[string]map[string]uint64, error) {
	if cproto.IsRelease(cluster) {
		return nil, ErrUnSupportOperation
	}
	limitInfo, err := cli.api.GetLimitInfoCache(cluster, false)
	if err != nil {
		return nil, err
	}
	return limitInfo.FlashNodeVolLimitMap, nil
}

func (cli *CliService) getApiReqBwLimitMap(cluster string) (map[uint8]int64, error) {
	// 不能从ratelimit接口拿，没赋值
	if cproto.IsRelease(cluster) {
		rc := cli.api.GetReleaseClient(cluster)
		limtMap, err := rc.GetAPIReqBwRateLimitInfo()
		return limtMap, err
	}
	limitMap, err := cli.api.AdminGetAPIReqBwRateLimitInfo(cluster)
	return limitMap, err
}

func (cli *CliService) getRateLimit(cluster string) (map[string]map[string]map[int]proto.AllLimitGroup, error) {
	if cproto.IsRelease(cluster) {
		limitInfo, err := cli.api.GetLimitInfoCacheRelease(cluster, false)
		if err != nil {
			return nil, err
		}
		return limitInfo.RateLimit, nil
	}
	limitInfo, err := cli.api.GetLimitInfoCache(cluster, false)
	if err != nil {
		return nil, err
	}
	return limitInfo.RateLimit, nil
}

func (cli *CliService) getBandwidthLimit(cluster string) (uint64, error) {
	if cproto.IsRelease(cluster) {
		clusterView, err := cli.api.GetClusterViewCacheRelease(cluster, true)
		if err != nil {
			return 0, err
		}
		return clusterView.BandwidthLimit, nil
	}
	clusterView, err := cli.api.GetClusterViewCache(cluster, true)
	if err != nil {
		return 0, err
	}
	return clusterView.BandwidthLimit, nil
}

func (cli *CliService) setBandwidthLimiter(cluster string, limit uint64) error {
	if cproto.IsRelease(cluster) {
		rc := cli.api.GetReleaseClient(cluster)
		return rc.SetBandwidthLimiter(limit)
	}
	return cli.api.AdminSetBandwidthLimiter(cluster, limit)
}

func getRatelimitByModule(ratelimit map[string]map[string]map[int]proto.AllLimitGroup, module string) map[string]map[int]proto.AllLimitGroup {
	for m, zoneVolOpMap := range ratelimit {
		if strings.ToLower(module) == m {
			return zoneVolOpMap
		}
	}
	return nil
}

func getRatelimitIndexAndValue(limitGroup proto.AllLimitGroup) (map[int]int64, error) {
	// 把有值的提出来
	limitMap := make(map[int]int64)
	if multirate.HaveLimit(limitGroup) {
		for index, value := range limitGroup {
			if value > 0 {
				limitMap[index] = value
			}
		}
		return limitMap, nil
	}
	return nil, fmt.Errorf("no limit value is set")
}

func (cli *CliService) getLimitMetricsByModule(cluster, module string, operation int) ([][]*cproto.CliValueMetric, error) {
	result := make([][]*cproto.CliValueMetric, 0)
	// map[module]map[zone:|vol:]map[op]AllLimitGroup
	var rateLimit map[string]map[int]proto.AllLimitGroup
	if cproto.IsRelease(cluster) {
		limitInfo, err := cli.api.GetLimitInfoCacheRelease(cluster, false)
		if err != nil {
			return nil, err
		}
		for m, zoneVolOpMap := range limitInfo.RateLimit {
			if strings.ToLower(module) == m {
				rateLimit = zoneVolOpMap
				break
			}
		}
	} else {
		limitInfo, err := cli.api.GetLimitInfoCache(cluster, false)
		if err != nil {
			return nil, err
		}
		for m, zoneVolOpMap := range limitInfo.RateLimit {
			if strings.ToLower(module) == m {
				rateLimit = zoneVolOpMap
				break
			}
		}
	}
	if len(rateLimit) == 0 {
		result = append(result, cproto.FormatOperationNilData(operation, "string", "string", "int64", "int", "int64"))
		return result, nil
	}
	for zoneOrVol, opMap := range rateLimit {
		var zone string
		var vol string
		if strings.HasPrefix(zoneOrVol, multirate.ZonePrefix) {
			zone = strings.TrimPrefix(zoneOrVol, multirate.ZonePrefix)
		} else {
			vol = strings.TrimPrefix(zoneOrVol, multirate.VolPrefix)
		}
		if zone == "" && vol == "" {
			// 不支持zone和vol都不设置的情况
			zone = cproto.EmptyZoneVolFlag
		}
		for op, rateLimitGroup := range opMap {
			limitMap, err := getRatelimitIndexAndValue(rateLimitGroup)
			if err != nil {
				// 正常不会出现 op一个限速值都没设置的情况。master逻辑加了清理
				continue
			}
			for index, value := range limitMap {
				// timeout ns->ms 转化
				if index == 0 {
					value = value / 1e6
				}
				result = append(result, cproto.FormatArgsToValueMetrics(operation, zone, vol, op, index, value))
			}
		}
	}
	return result, nil
}

// todo: 校验非空和有效 集成进来
func (cli *CliService) batchSetRateLimit(cluster string, operation int, params []map[string]string) error {
	var setRateLimitFunc = func(cluster string, args map[string]string) error {
		if module := moduleByOperation(operation); module != "" {
			args["module"] = module
		}
		err := cli.api.SetRatelimitInfo(cluster, args)
		if err != nil {
			log.LogWarnf("batchSetRateLimit: cluster(%v) operation(%v) args(%v) err(%v)", cluster, cproto.GetOpShortMsg(operation), args, err)
		}
		return err
	}

	var (
		err      error
		errStack error
	)
	for _, param := range params {
		// 定制化的参数校验
		if rateLimitIndex, ok := param[proto.RateLimitIndexKey]; ok && rateLimitIndex == "0" {
			// 用户输入的时间 ms转化为ns
			rateLimitStr := param["rateLimit"]
			if rateLimitStr != "" {
				timeout_ms, er := strconv.ParseInt(rateLimitStr, 10, 64)
				if er != nil {
					return er
				}
				param["rateLimit"] = strconv.FormatInt(timeout_ms*1e6, 10)
			}
		}

		if volumeName, ok := param["volume"]; ok && cproto.IsMultiSelectValue(operation) {
			// 多选下拉，解析
			if volumeName == "" {
				continue
			}
			volList := strings.Split(volumeName, ",")
			for _, volName := range volList {
				param["volume"] = volName
				err = setRateLimitFunc(cluster, param)
				if err != nil {
					// 把错误组合在一起
					errStack = fmt.Errorf("%v\n%v", errStack, err)
					continue
				}
			}
		} else {
			err = setRateLimitFunc(cluster, param)
			if err != nil {
				errStack = fmt.Errorf("%v\n%v", errStack, err)
				continue
			}
		}
	}
	return errStack
}
