package cli

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	cproto "github.com/cubefs/cubefs/console/proto"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"strconv"
	"strings"
)

func (cli *CliService) GetVolumeConfig(cluster string, operation int, volName string) ([]*cproto.CliValueMetric, error) {
	switch operation {
	case cproto.OpSetVolume, cproto.OpSetVolFollowerRead, cproto.OpSetVolTrash, cproto.OpSetVolSmart, cproto.OpSetVolRemoteCache,
		cproto.OpSetVolMeta, cproto.OpSetVolAuthentication, cproto.OpSetVolumeConnConfig, cproto.OpSetVolMinRWPartition, cproto.OpVolSetChildFileMaxCount:
		return cli.getVolume(operation, cluster, volName)

	case cproto.OpVolAddMp:
		var start uint64 = 0
		if cproto.IsRelease(cluster) {
			//rc := cli.api.GetReleaseClient(cluster)
			//volInfo, err := rc.AdminGetVol(volName)
			//if err != nil {
			//	return nil, err
			//}
			return cproto.FormatArgsToValueMetrics(operation,
				//volInfo.MpCnt,
				//volInfo.RwMpCnt,
				start,
			), nil
		} else {
			//mc := cli.api.GetMasterClient(cluster)
			//volInfo, err := mc.AdminAPI().GetVolumeSimpleInfo(volName)
			//if err != nil {
			//	return nil, err
			//}
			return cproto.FormatArgsToValueMetrics(operation,
				//volInfo.MpCnt,
				//volInfo.RwMpCnt,
				start), nil
		}

	case cproto.OpVolAddDp, cproto.OpVolAddDpRelease:
		if cproto.IsRelease(cluster) {
			//rc := cli.api.GetReleaseClient(cluster)
			//volInfo, err := rc.AdminGetVol(volName)
			//if err != nil {
			//	return nil, err
			//}
			return cproto.FormatArgsToValueMetrics(operation,
				//volInfo.DpCnt,
				//volInfo.RwDpCnt,
				0,
				//"",
			), nil
		} else {
			//mc := cli.api.GetMasterClient(cluster)
			//volInfo, err := mc.AdminAPI().GetVolumeSimpleInfo(volName)
			//if err != nil {
			//	return nil, err
			//}
			return cproto.FormatArgsToValueMetrics(operation,
				//volInfo.DpCnt,
				//volInfo.RwDpCnt,
				0), nil
		}
	case cproto.OpSetVolumeRelease:
		return cli.getVolumeRelease(operation, cluster, volName)

	case cproto.OpVolClearTrash:
		return cproto.FormatArgsToValueMetrics(operation, false), nil

	default:
	}
	return nil, nil
}

func (cli *CliService) SetVolumeConfig(ctx context.Context, cluster string, operation int, metrics []*cproto.CliValueMetric, volName string, skipXbp bool) (err error) {
	defer func() {
		msg := fmt.Sprintf("SetVolumeConfig: cluster[%v] vol(%v) op(%v) metrics(%v)", cluster, volName, cproto.GetOpShortMsg(operation), metrics)
		if err != nil {
			log.LogErrorf("%s, err: %v", msg, err)
		} else {
			log.LogInfof("%s", msg)
		}
	}()
	var (
		params map[string]string
		args   map[string]interface{}
	)
	params, _, err = cproto.ParseValueMetricsToParams(operation, metrics)
	if err != nil {
		return
	}
	args, err = cproto.ParseValueMetricsToArgs(operation, metrics)
	if err != nil {
		return
	}

	switch operation {
	case cproto.OpSetVolume, cproto.OpSetVolFollowerRead, cproto.OpSetVolTrash, cproto.OpSetVolSmart, cproto.OpSetVolRemoteCache,
		cproto.OpSetVolMeta, cproto.OpSetVolAuthentication:
		if cproto.IsRelease(cluster) {
			return fmt.Errorf("请选择: %s", cproto.GetOpShortMsg(cproto.OpSetVolumeRelease))
		}
		for _, baseMetric := range cproto.GetCliOperationBaseMetrics(operation) {
			switch baseMetric.ValueName {
			case "dpSelectorParm":
				dpParamStr := args[baseMetric.ValueName].(string)
				dpParam, err := strconv.ParseInt(dpParamStr, 10, 8)
				if err != nil {
					return err
				}
				if dpParam <= 0 || dpParam >= 100 {
					return fmt.Errorf("请输入0~100之间的数")
				}
			}
		}
		if !skipXbp {
			goto createXbpApply
		}
		return cli.updateVolume(cluster, volName, params)

	case cproto.OpSetVolumeConnConfig:
		if cproto.IsRelease(cluster) {
			return fmt.Errorf("请选择: %s", cproto.GetOpShortMsg(cproto.OpSetVolumeRelease))
		}
		for _, baseMetric := range cproto.GetCliOperationBaseMetrics(operation) {
			switch baseMetric.ValueName {
			case "readConnTimeout":
				readMs := args[baseMetric.ValueName].(int64)
				if readMs < 0 {
					return fmt.Errorf("请输入 >=0 的值")
				}
				params[baseMetric.ValueName] = strconv.FormatInt(readMs*1e6, 10)
			case "writeConnTimeout":
				writeMs := args[baseMetric.ValueName].(int64)
				if writeMs < 0 {
					return fmt.Errorf("请输入 >=0 的值")
				}
				params[baseMetric.ValueName] = strconv.FormatInt(writeMs*1e6, 10)
			}
		}
		if !skipXbp {
			goto createXbpApply
		}
		return cli.updateVolume(cluster, volName, params)

	case cproto.OpSetVolMinRWPartition:
		var minDp, minMp int64
		for _, baseMetric := range cproto.GetCliOperationBaseMetrics(operation) {
			switch baseMetric.ValueName {
			case "minWritableDp":
				minDp = args[baseMetric.ValueName].(int64)
				if minDp <= 0 {
					return fmt.Errorf("请输入 >0 的值")
				}
			case "minWritableMp":
				minMp = args[baseMetric.ValueName].(int64)
				if minMp <= 0 {
					return fmt.Errorf("请输入 >0 的值")
				}
			}
		}
		if !skipXbp {
			goto createXbpApply
		}
		return cli.SetVolMinRWPartition(cluster, volName, minDp, minMp)

	case cproto.OpVolAddDp, cproto.OpVolAddDpRelease:
		var count int64
		for _, baseMetric := range cproto.GetCliOperationBaseMetrics(operation) {
			switch baseMetric.ValueName {
			case "count":
				count = args[baseMetric.ValueName].(int64)
				if count <= 0 {
					return fmt.Errorf("请输入>0的数")
				}
			}
		}
		if !skipXbp {
			goto createXbpApply
		}
		return cli.volAddDp(cluster, volName, count)

	case cproto.OpVolAddMp:
		var inodestart uint64
		for _, baseMetric := range cproto.GetCliOperationBaseMetrics(operation) {
			switch baseMetric.ValueName {
			case "start":
				inodestart = args[baseMetric.ValueName].(uint64)
				if inodestart < 0 {
					return fmt.Errorf("请输入>=0的inode start")
				}
			}
		}
		if !skipXbp {
			goto createXbpApply
		}
		return cli.volAddMp(cluster, volName, inodestart)

	case cproto.OpVolSetChildFileMaxCount:
		var maxCount uint64
		for _, baseMetric := range cproto.GetCliOperationBaseMetrics(operation) {
			switch baseMetric.ValueName {
			case proto.ChildFileMaxCountKey:
				maxCount = args[baseMetric.ValueName].(uint64)
			}
		}
		if !skipXbp {
			goto createXbpApply
		}
		mc := cli.api.GetMasterClient(cluster)
		_, err = mc.AdminAPI().SetVolChildFileMaxCount(volName, uint32(maxCount))
		return

	case cproto.OpSetVolumeRelease:
		for _, baseMetric := range cproto.GetCliOperationBaseMetrics(operation) {
			switch baseMetric.ValueName {
			case "readConnTimeout":
				readMs := args[baseMetric.ValueName].(int64)
				if readMs == 0 {
					delete(params, baseMetric.ValueName)
				} else if readMs < 1000 {
					return fmt.Errorf("请输入>=1000的值，单位毫秒")
				} else {
					params[baseMetric.ValueName] = strconv.FormatInt(readMs*1e6, 10)
				}
			case "writeConnTimeout":
				writeMs := args[baseMetric.ValueName].(int64)
				if writeMs == 0 {
					delete(params, baseMetric.ValueName)
				} else if writeMs < 0 {
					return fmt.Errorf("请输入>0的值，单位毫秒")
				} else {
					params[baseMetric.ValueName] = strconv.FormatInt(writeMs*1e6, 10)
				}
			case proto.BitMapSnapFrozenHour:
				// todo: check是否可以=0
			}
		}
		if !skipXbp {
			goto createXbpApply
		}
		return cli.updateVolumeRelease(cluster, volName, params)

	case cproto.OpVolClearTrash:
		doClean := args[metrics[0].ValueName].(bool)
		if !skipXbp {
			goto createXbpApply
		}
		return cli.cleanVolTrash(cluster, volName, doClean)

	default:
		err = fmt.Errorf("undefined operation code: %v:%v", operation, cproto.GetOpShortMsg(operation))
		return
	}

createXbpApply:
	return cli.createXbpApply(ctx, cluster, cproto.VolumeModuleType, operation, [][]*cproto.CliValueMetric{metrics}, &volName, nil, false)
}

func (cli *CliService) GetVolumeConfigList(cluster string, operation int, volName string) (result [][]*cproto.CliValueMetric, err error) {

	defer func() {
		msg := fmt.Sprintf("GetVolumeConfigList: cluster(%v) vol(%v) operation(%v)", cluster, volName, cproto.GetOpShortMsg(operation))
		if err != nil {
			log.LogErrorf("%s, err(%v)", msg, err)
		} else {
			log.LogInfof("%s, metrics: %v", msg, result)
		}
	}()

	result = make([][]*cproto.CliValueMetric, 0)

	switch operation {
	case cproto.OpClientVolOpRateLimit:
		var clientVolOpRateLimit map[uint8]int64
		clientVolOpRateLimit, err = cli.getVolRateLimit(cluster, volName)
		if err != nil {
			return nil, err
		}
		if len(clientVolOpRateLimit) == 0 {
			result = append(result, cproto.FormatOperationNilData(operation, "uint8", "uint64"))
			return
		}
		for opcode, limit := range clientVolOpRateLimit {
			result = append(result, cproto.FormatArgsToValueMetrics(operation, opcode, limit))
		}

	case cproto.OpVolS3ActionRateLimit:
		var s3ActionRatelimit map[string]int64
		s3ActionRatelimit, err = cli.getVolS3ActionRatelimit(cluster, volName)
		if err != nil {
			return nil, err
		}
		if len(s3ActionRatelimit) == 0 {
			result = append(result, cproto.FormatOperationNilData(operation, "string", "int64"))
			return
		}
		for action, limit := range s3ActionRatelimit {
			result = append(result, cproto.FormatArgsToValueMetrics(operation, action, limit))
		}

	default:
	}
	return
}
func (cli *CliService) SetVolumeConfigList(ctx context.Context, cluster string, operation int, metrics [][]*cproto.CliValueMetric, volName string, skipXbp bool) (err error) {
	defer func() {
		msg := fmt.Sprintf("SetVolumeConfigList: cluster(%v) vol(%v) op(%v) metrics(%v)", cluster, volName, cproto.GetOpShortMsg(operation), metrics)
		if err != nil {
			log.LogErrorf("%s, err(%v)", msg, err)
		} else {
			log.LogInfof("%s", msg)
		}
	}()

	switch operation {
	case cproto.OpClientVolOpRateLimit:
		metrics, err = cli.getVolOpRateLimitChanged(metrics, cluster, volName)
		if err != nil {
			return err
		}
		if !skipXbp {
			return cli.createXbpApply(ctx, cluster, cproto.VolumeModuleType, operation, metrics, &volName, nil, true)
		}
		goto update

	case cproto.OpVolS3ActionRateLimit:
		metrics, err = cli.getVolS3RateLimitChanged(metrics, cluster, volName)
		if err != nil {
			return err
		}
		if !skipXbp {
			return cli.createXbpApply(ctx, cluster, cproto.VolumeModuleType, operation, metrics, &volName, nil, true)
		}
		goto update

	default:
	}

update:
	var args map[string]string
	for _, metric := range metrics {
		var isEmpty bool
		args, isEmpty, err = cproto.ParseValueMetricsToParams(operation, metric)
		if err != nil || isEmpty {
			continue
		}
		args["volume"] = volName
		err = cli.api.SetRatelimitInfo(cluster, args)
		if err != nil {
			log.LogWarnf("SetVolumeConfigList: op(%v:%v) args(%v) err(%v)")
			continue
		}
	}
	return
}

func (cli *CliService) getVolume(operation int, cluster, volName string) ([]*cproto.CliValueMetric, error) {
	if cproto.IsRelease(cluster) {
		return nil, fmt.Errorf("请选择: %s", cproto.GetOpShortMsg(cproto.OpSetVolumeRelease))
	}
	mc := cli.api.GetMasterClient(cluster)
	volInfo, err := mc.AdminAPI().GetVolumeSimpleInfo(volName)
	if err != nil {
		log.LogErrorf("getSetVolumeMetric: adminGetVol failed, cluster(%v) vol(%v) err(%v)", cluster, volName, err)
		return nil, err
	}
	switch operation {
	case cproto.OpSetVolume:
		return cproto.FormatArgsToValueMetrics(operation,
			volInfo.VolWriteMutexEnable,
			volInfo.ForceROW,
			volInfo.EnableWriteCache,
			volInfo.AutoRepair,
			volInfo.ZoneName,
			volInfo.Capacity,
			volInfo.DpReplicaNum,
			volInfo.MpReplicaNum,
			int(volInfo.CrossRegionHAType),
			volInfo.ExtentCacheExpireSec,
			volInfo.CompactTag,
			int(volInfo.UmpCollectWay),
			volInfo.DpSelectorName,
			volInfo.DpSelectorParm,
		), nil

	case cproto.OpSetVolumeConnConfig:
		return cproto.FormatArgsToValueMetrics(operation,
			volInfo.ConnConfig.ReadTimeoutNs/1e6,
			volInfo.ConnConfig.WriteTimeoutNs/1e6,
		), nil

	case cproto.OpSetVolMeta:
		return cproto.FormatArgsToValueMetrics(operation,
			volInfo.EnableBitMapAllocator,
			volInfo.BitMapSnapFrozenHour,
			volInfo.EnableRemoveDupReq,
			int(volInfo.DefaultStoreMode),
			fmt.Sprintf("%d,%d", volInfo.MpLayout.PercentOfMP, volInfo.MpLayout.PercentOfReplica),
			volInfo.BatchDelInodeCnt,
			volInfo.DelInodeInterval,
			volInfo.TruncateEKCountEveryTime,
		), nil

	case cproto.OpSetVolAuthentication:
		return cproto.FormatArgsToValueMetrics(operation,
			volInfo.Authenticate,
			volInfo.EnableToken,
			int(volInfo.OSSBucketPolicy),
		), nil

	case cproto.OpSetVolFollowerRead:
		return cproto.FormatArgsToValueMetrics(operation,
			volInfo.FollowerRead,
			volInfo.DpFolReadDelayConfig.DelaySummaryInterval,
			volInfo.FolReadHostWeight,
			volInfo.NearRead,
		), nil

	case cproto.OpSetVolTrash:
		return cproto.FormatArgsToValueMetrics(operation,
			volInfo.TrashRemainingDays,
			volInfo.TrashCleanDuration,
			volInfo.TrashCleanInterval,
			volInfo.TrashCleanMaxCount,
		), nil

	case cproto.OpSetVolSmart:
		return cproto.FormatArgsToValueMetrics(operation,
			volInfo.IsSmart,
			strings.Join(volInfo.SmartRules, ","),
		), nil

	case cproto.OpSetVolRemoteCache:
		return cproto.FormatArgsToValueMetrics(operation,
			volInfo.RemoteCacheBoostEnable,
			volInfo.RemoteCacheAutoPrepare,
			volInfo.RemoteCacheBoostPath,
			volInfo.RemoteCacheTTL,
		), nil

	case cproto.OpSetVolMinRWPartition:
		return cproto.FormatArgsToValueMetrics(operation,
			volInfo.MinWritableDPNum,
			volInfo.MinWritableMPNum,
		), nil

	case cproto.OpVolSetChildFileMaxCount:
		return cproto.FormatArgsToValueMetrics(operation,
			volInfo.ChildFileMaxCount), nil

	default:
	}
	return nil, nil
}

func (cli *CliService) updateVolume(cluster, vol string, args map[string]string) error {
	if cproto.IsRelease(cluster) {
		return fmt.Errorf("请选择: %s", cproto.GetOpShortMsg(cproto.OpSetVolumeRelease))
	}
	mc := cli.api.GetMasterClient(cluster)
	volInfo, err := mc.AdminAPI().GetVolumeSimpleInfo(vol)
	if err != nil {
		log.LogErrorf("setVolumeConfig: get vol owner failed: cluster(%v)vol(%v) err(%v)", cluster, vol, err)
		return err
	}
	args["authKey"] = computeAuthKey(volInfo.Owner)
	return cli.api.UpdateVolume(cluster, vol, args)
}

func (cli *CliService) getVolumeRelease(operation int, cluster, volName string) ([]*cproto.CliValueMetric, error) {
	if !cproto.IsRelease(cluster) {
		return nil, fmt.Errorf("%s专用！", cproto.GetOpShortMsg(cproto.OpSetVolumeRelease))
	}
	rc := cli.api.GetReleaseClient(cluster)
	volInfo, err := rc.AdminGetVol(volName)
	if err != nil {
		log.LogErrorf("getSetVolumeMetric: adminGetVol failed, cluster(%v) vol(%v) err(%v)", cluster, volName, err)
		return nil, err
	}
	var (
		readTimeoutMs  int64 = 0
		writeTimeoutMs int64 = 0
	)
	if volInfo.ConnConfig != nil {
		readTimeoutMs = int64(volInfo.ConnConfig.ReadConnTimeout) / 1e6
		writeTimeoutMs = int64(volInfo.ConnConfig.WriteConnTimeout) / 1e6
	}
	return cproto.FormatArgsToValueMetrics(operation,
		volInfo.Capacity,
		volInfo.MinWritableDPNum,
		volInfo.MinWritableMPNum,
		volInfo.BatchDelInodeCnt,
		readTimeoutMs,
		writeTimeoutMs,
		volInfo.EnableToken,
		volInfo.CrossPod,
		volInfo.AutoRepairCrossPod,
		volInfo.EnableBitmapAllocator,
		volInfo.BitMapSnapFrozenHour,
	), nil
}

func (cli *CliService) SetVolMinRWPartition(cluster, volume string, minDp, minMp int64) error {
	if cproto.IsRelease(cluster) {
		return cproto.ErrUnSupportOperation
	}
	mc := cli.api.GetMasterClient(cluster)
	if err := mc.AdminAPI().SetVolMinRWPartition(volume, int(minMp), int(minDp)); err != nil {
		log.LogErrorf("SetVolMinRWPartition failed: vol(%v)err(%v)", volume, err)
		return err
	}
	return nil
}

func (cli *CliService) volAddDp(cluster, volume string, count int64) error {
	if cproto.IsRelease(cluster) {
		rc := cli.api.GetReleaseClient(cluster)
		return rc.CreateDataPartition(volume, int(count))
	} else {
		mc := cli.api.GetMasterClient(cluster)
		return mc.AdminAPI().CreateDataPartition(volume, int(count))
	}
}

func (cli *CliService) volAddMp(cluster, volume string, inodeStart uint64) error {
	if cproto.IsRelease(cluster) {
		rc := cli.api.GetReleaseClient(cluster)
		return rc.CreateMetaPartition(volume, inodeStart)
	} else {
		mc := cli.api.GetMasterClient(cluster)
		return mc.AdminAPI().CreateMetaPartition(volume, inodeStart)
	}
}

func (cli *CliService) updateVolumeRelease(cluster, volName string, args map[string]string) error {
	if !cproto.IsRelease(cluster) {
		return fmt.Errorf("%s专用！", cproto.GetOpShortMsg(cproto.OpSetVolumeRelease))
	}
	rc := cli.api.GetReleaseClient(cluster)
	volInfo, err := rc.AdminGetVol(volName)
	if err != nil {
		log.LogErrorf("setVolumeConfigRelease: get vol owner failed, vol(%v)err(%v)", volName, err)
		return err
	}
	args["authKey"] = computeAuthKey(volInfo.Owner)
	return rc.UpdateVolume(volName, args)
}

func (cli *CliService) createVolume(cluster string, args map[string]string) error {
	if cproto.IsRelease(cluster) {
		rc := cli.api.GetReleaseClient(cluster)
		return rc.CreateVolByParamsMap(args)
	} else {
		return cli.api.CreateVolByParamsMap(cluster, args)
	}
}

func (cli *CliService) SetDiskUsage(cluster string, args map[string]string) error {
	if cproto.IsRelease(cluster) {
		rc := cli.api.GetReleaseClient(cluster)
		return rc.SetDiskUsageByKeyValue(args)
	} else {
		return cli.api.SetDiskUsageByKeyValue(cluster, args)
	}
}

func (cli *CliService) getVolS3ActionRatelimit(cluster, volume string) (map[string]int64, error) {
	if cproto.IsRelease(cluster) {
		return nil, ErrUnSupportOperation
	}
	mc := cli.api.GetMasterClient(cluster)
	limitInfo, err := mc.AdminAPI().GetLimitInfo(volume)
	if err != nil {
		log.LogErrorf("getVolS3Ratelimit: vol(%v)err(%v)", volume, err)
		return nil, err
	}
	return limitInfo.ObjectNodeActionRateLimit, nil
}

func (cli *CliService) getVolRateLimit(cluster, volume string) (map[uint8]int64, error) {
	var limitMap map[uint8]int64
	if cproto.IsRelease(cluster) {
		rc := cli.api.GetReleaseClient(cluster)
		limitInfo, err := rc.GetLimitInfo(volume)
		if err != nil {
			log.LogErrorf("getVolRateLimit: cluster(%v) vol(%v) err(%v)", cluster, volume, err)
			return nil, err
		}
		limitMap = limitInfo.ClientVolOpRateLimit
	} else {
		mc := cli.api.GetMasterClient(cluster)
		limitInfo, err := mc.AdminAPI().GetLimitInfo(volume)
		if err != nil {
			log.LogErrorf("getVolRateLimit: cluster(%v) vol(%v) err(%v)", cluster, volume, err)
			return nil, err
		}
		limitMap = limitInfo.ClientVolOpRateLimit
	}
	return limitMap, nil
}

// cli.cleanVolTrash(cluster, volName, doClean)
func (cli *CliService) cleanVolTrash(cluster, volname string, doClean bool) error {
	if doClean {
		mc := cli.api.GetMasterClient(cluster)
		if vv, err := mc.AdminAPI().GetVolumeSimpleInfo(volname); err != nil {
			return err
		} else if vv.TrashRemainingDays <= 0 {
			return fmt.Errorf("该vol[%s]未开启回收站", volname)
		}
		// 没有错 并且开启了回收站的
		return cli.api.CleanVolTrash(cluster, volname)
	}
	return nil
}

// 1. 增  --- 输出有metrics  (并发 后端已有，不请求)
// 2. 删  --- 输出空         （并发  后端已经没了 也不用设置0）
// 3. 增删均有 ---- 输出增的 删的没有
func (cli *CliService) getVolOpRateLimitChanged(metrics [][]*cproto.CliValueMetric, cluster, volume string) (result [][]*cproto.CliValueMetric, err error) {
	limitMap, err := cli.getVolRateLimit(cluster, volume)
	if err != nil {
		return nil, err
	}
	result = make([][]*cproto.CliValueMetric, 0)
	input := make(map[uint8]int64)
	for _, metric := range metrics {
		op, limit, empty := formatMetricsToVolLimitMap(metric)
		if empty {
			continue
		}
		if limit == 0 {
			return nil, fmt.Errorf("客户端vol op限速禁止设0，-1取消")
		}
		input[op] = limit
		// 不变的
		if val, ok := limitMap[op]; ok && val == limit {
			continue
		}
		// 变得 增的
		result = append(result, metric)
		if log.IsDebugEnabled() {
			log.LogDebugf("add metric: cluster(%v) vol(%v) metric(%v)", cluster, volume, metric)
		}
	}
	// 删， 均通过设0取消
	//for key, _ := range limitMap {
	//	if _, ok := input[key]; !ok {
	//		delMetric := cproto.FormatArgsToValueMetrics(cproto.OpClientVolOpRateLimit, key, 0)
	//		result = append(result, delMetric)
	//		if log.IsDebugEnabled() {
	//			log.LogDebugf("del metric: cluster(%v) vol(%v) metric(%v)", cluster, volume, delMetric)
	//		}
	//	}
	//}
	if log.IsDebugEnabled() {
		log.LogDebugf("getVolOpRateLimitChanged: input(len)=%v output(len)=%v res: %v", len(metrics), len(result), result)
	}
	return result, nil
}

func (cli *CliService) getVolS3RateLimitChanged(metrics [][]*cproto.CliValueMetric, cluster, volume string) (result [][]*cproto.CliValueMetric, err error) {
	actionLimitMap, err := cli.getVolS3ActionRatelimit(cluster, volume)
	if err != nil {
		return nil, err
	}
	result = make([][]*cproto.CliValueMetric, 0)
	input := make(map[string]int64)
	for _, metric := range metrics {
		action, limit, empty := formatMetricsVolS3LimitMap(metric)
		if empty {
			continue
		}
		if limit == 0 {
			return nil, fmt.Errorf("客户端vol s3 op限速禁止设0，-1取消")
		}
		input[action] = limit
		// 不变的
		if val, ok := actionLimitMap[action]; ok && val == limit {
			continue
		}
		// 变化 & 新增的
		result = append(result, metric)
		if log.IsDebugEnabled() {
			log.LogDebugf("add metric: cluster(%v) vol(%v) metric(%v)", cluster, volume, metric)
		}
	}
	if log.IsDebugEnabled() {
		log.LogDebugf("getVolS3RateLimitChanged: len(input)=%v len(output)=%v res: %v")
	}
	return result, nil
}

func formatMetricsToVolLimitMap(metrics []*cproto.CliValueMetric) (key uint8, limit int64, empty bool) {
	var (
		opcode uint64
	)
	empty = true
	for _, metric := range metrics {
		if metric.Value != "" {
			empty = false
		}
		switch metric.ValueName {
		case "opcode":
			opcode, _ = strconv.ParseUint(metric.Value, 10, 8)

		case "clientVolOpRate":
			limit, _ = strconv.ParseInt(metric.Value, 10, 64)
		}
	}
	if empty {
		return
	}
	return uint8(opcode), limit, empty
}

func formatMetricsVolS3LimitMap(metrics []*cproto.CliValueMetric) (action string, limit int64, empty bool) {
	empty = true
	for _, metric := range metrics {
		if metric.Value != "" {
			empty = false
		}
		switch metric.ValueName {
		case "action":
			action = metric.Value

		case "objectVolActionRate":
			limit, _ = strconv.ParseInt(metric.Value, 10, 64)

		}
	}
	if empty {
		return
	}
	return
}

func computeAuthKey(key string) (authKey string) {
	h := md5.New()
	_, _ = h.Write([]byte(key))
	cipherStr := h.Sum(nil)
	return strings.ToLower(hex.EncodeToString(cipherStr))
}
