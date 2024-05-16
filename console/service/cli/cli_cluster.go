package cli

import (
	"context"
	"fmt"
	cproto "github.com/cubefs/cubefs/console/proto"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"strings"
)

const (
	ClientHotUpgradePrefix = "http://storage.jd.local/"
)

func (cli *CliService) GetClusterConfig(cluster string, operation int) (result []*cproto.CliValueMetric, err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("GetClusterConfig failed: cluster[%v] operation(%v:%v) err:%v", cluster, operation, cproto.GetOpShortMsg(operation), err)
		}
	}()

	switch operation {
	case cproto.OpSetClientPkgAddr:
		addr, err := cli.getClientPkgAddr(cluster)
		if err != nil {
			return nil, err
		}
		result = cproto.FormatArgsToValueMetrics(operation, addr)

	case cproto.OpSetRemoteCacheBoostEnableStatus:
		boostEnable, err := cli.getClusterRemoteCacheBoostStatus(cluster)
		if err != nil {
			return nil, err
		}
		result = cproto.FormatArgsToValueMetrics(operation, boostEnable)

	case cproto.OpSetNodeState:
		result = cproto.FormatArgsToValueMetrics(operation, "", "", "", "")

	case cproto.OpSetNodeSetCapacity:
		capacity, err := cli.getNodeSetCapacity(cluster)
		if err != nil {
			return nil, err
		}
		result = cproto.FormatArgsToValueMetrics(operation, capacity)

	case cproto.OpMonitorSummarySecond:
		summarySec, reportSec, err := cli.getMonitorSummarySec(cluster)
		if err != nil {
			return nil, err
		}
		result = cproto.FormatArgsToValueMetrics(operation, summarySec, reportSec)

	case cproto.OpLogMaxMB:
		logMaxSize, err := cli.getLogMaxSize(cluster)
		if err != nil {
			return nil, err
		}
		result = cproto.FormatArgsToValueMetrics(operation, logMaxSize)

	case cproto.OpTopologyManager:
		fetchMin, forceSec, err := cli.getTopoManager(cluster)
		if err != nil {
			return nil, err
		}
		result = cproto.FormatArgsToValueMetrics(operation, fetchMin, forceSec)

	case cproto.OpAutoMergeNodeSet:
		enableAutoMerge, err := cli.getAutoMergeNodeSet(cluster)
		if err != nil {
			return nil, err
		}
		result = cproto.FormatArgsToValueMetrics(operation, enableAutoMerge)

	case cproto.OpSetClusterFreeze:
		enableFreeze, err := cli.getClusterFreeze(cluster)
		if err != nil {
			return nil, err
		}
		result = cproto.FormatArgsToValueMetrics(operation, enableFreeze)

	default:
		return nil, fmt.Errorf("undefined operation code: %v:%v", operation, cproto.GetOpShortMsg(operation))
	}
	return
}

func (cli *CliService) SetClusterConfig(ctx context.Context, cluster string, operation int, metrics []*cproto.CliValueMetric, skipXbp bool) (err error) {
	defer func() {
		msg := fmt.Sprintf("SetClusterConfig: cluster[%v] operation(%v:%v)", cluster, operation, cproto.GetOpShortMsg(operation))
		if err != nil {
			log.LogErrorf("%s, err:%v", msg, err)
		} else {
			log.LogInfof("%v, metrics:%v", msg, metrics)
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
	case cproto.OpSetClientPkgAddr:
		addr := args[metrics[0].ValueName].(string)
		if !strings.HasPrefix(addr, ClientHotUpgradePrefix) {
			return fmt.Errorf("请输入有效的热升级地址！")
		}
		if !skipXbp {
			goto createXbpApply
		}
		return cli.setClientPkgAddr(cluster, addr)

	case cproto.OpSetNodeState:
		var (
			addrs    []string
			nodeType string
			zone     string
			state    string
		)
		for _, baseMetric := range cproto.GetCliOperationBaseMetrics(operation) {
			switch baseMetric.ValueName {
			case "addrList":
				addrListStr := args[baseMetric.ValueName].(string)
				if addrListStr == "" {
					return fmt.Errorf("请输入有效的addrList参数！")
				}
				addrs = strings.Split(addrListStr, ";")
			case "nodeType":
				nodeType = args[baseMetric.ValueName].(string)
				if nodeType != "dataNode" && nodeType != "metaNode" && nodeType != "all" {
					return fmt.Errorf("请输入正确的nodeType!")
				}
			case "zoneName":
				zone = args[baseMetric.ValueName].(string)
			case "state":
				state = args[baseMetric.ValueName].(string)
			}
		}
		if !skipXbp {
			goto createXbpApply
		}
		return cli.setNodeState(cluster, addrs, nodeType, zone, state)

	case cproto.OpSetNodeSetCapacity:
		capacity := args[metrics[0].ValueName].(int64)
		if capacity != 0 && capacity < 64 {
			return fmt.Errorf("请输入 >= 64 的正整数")
		}
		if !skipXbp {
			goto createXbpApply
		}
		return cli.setNodeSetCapacity(cluster, capacity)

	case cproto.OpSetRemoteCacheBoostEnableStatus:
		if !skipXbp {
			goto createXbpApply
		}
		return cli.api.SetRatelimitInfo(cluster, params)

	case cproto.OpMonitorSummarySecond:
		for _, baseMetric := range cproto.GetCliOperationBaseMetrics(operation) {
			switch baseMetric.ValueName {
			case "monitorSummarySec":
				summarySec := args[baseMetric.ValueName].(uint64)
				if summarySec > 0 && summarySec < 5 {
					return fmt.Errorf("请设置 >=5 的正整数！")
				}

			case "monitorReportSec":
				reportSec := args[baseMetric.ValueName].(uint64)
				if reportSec > 0 && reportSec < 5 {
					return fmt.Errorf("请设置 >=5 的正整数！")
				}
			}
		}
		if !skipXbp {
			goto createXbpApply
		}
		return cli.api.SetRatelimitInfo(cluster, params)

	case cproto.OpLogMaxMB:
		if !skipXbp {
			goto createXbpApply
		}
		return cli.api.SetRatelimitInfo(cluster, params)

	case cproto.OpTopologyManager:
		for _, baseMetric := range cproto.GetCliOperationBaseMetrics(operation) {
			switch baseMetric.ValueName {
			case proto.TopologyFetchIntervalMinKey:
				fetchMin := args[baseMetric.ValueName].(int64)
				if fetchMin < 0 {
					return fmt.Errorf("请设置 >0 的正整数！")
				}
				if fetchMin == 0 {
					delete(params, baseMetric.ValueName)
				}

			case proto.TopologyForceFetchIntervalSecKey:
				forceSec := args[baseMetric.ValueName].(int64)
				if forceSec < 0 {
					return fmt.Errorf("请设置 >0 的正整数！")
				}
				if forceSec == 0 {
					delete(params, baseMetric.ValueName)
				}
			}
		}
		if !skipXbp {
			goto createXbpApply
		}
		return cli.api.SetRatelimitInfo(cluster, params)

	case cproto.OpAutoMergeNodeSet:
		enableAutoMerge := args[metrics[0].ValueName].(bool)
		if !skipXbp {
			goto createXbpApply
		}
		return cli.setAutoMergeNodeSet(cluster, enableAutoMerge)

	case cproto.OpSetClusterFreeze:
		enableFreeze := args[metrics[0].ValueName].(bool)
		if !skipXbp {
			goto createXbpApply
		}
		return cli.setClusterFreeze(cluster, enableFreeze)

	default:
		err = fmt.Errorf("undefined operation code: %v:%v", operation, cproto.GetOpShortMsg(operation))
		return
	}

createXbpApply:
	return cli.createXbpApply(ctx, cluster, cproto.ClusterModuleType, operation, [][]*cproto.CliValueMetric{metrics}, nil, nil, false)

}

func (cli *CliService) getClientPkgAddr(cluster string) (string, error) {
	if cproto.IsRelease(cluster) {
		return "", ErrUnSupportOperation
	}
	cv, err := cli.api.GetClusterViewCache(cluster, true)
	if err != nil {
		return "", err
	}
	return cv.ClientPkgAddr, nil
}

func (cli *CliService) setClientPkgAddr(cluster, addr string) error {
	if cproto.IsRelease(cluster) {
		return ErrUnSupportOperation
	}
	mc := cli.api.GetMasterClient(cluster)
	err := mc.ClientAPI().SetClientPkgAddr(addr)
	if err != nil {
		return err
	}
	return nil
}

func (cli *CliService) getClusterRemoteCacheBoostStatus(cluster string) (bool, error) {
	if cproto.IsRelease(cluster) {
		return false, ErrUnSupportOperation
	}
	mc := cli.api.GetMasterClient(cluster)
	limitInfo, err := mc.AdminAPI().GetLimitInfo("")
	if err != nil {
		log.LogErrorf("getClusterRemoteCacheBoostStatus: get limit info failed, err(%v)", err)
		return false, err
	}
	return limitInfo.RemoteCacheBoostEnable, nil
}

func (cli *CliService) setNodeState(cluster string, addrs []string, nodeType, zone, state string) error {
	if cproto.IsRelease(cluster) {
		return ErrUnSupportOperation
	}
	mc := cli.api.GetMasterClient(cluster)
	if err := mc.AdminAPI().SetNodeToOfflineState(addrs, nodeType, zone, state); err != nil {
		log.LogErrorf("setNodeState: SetNodeToOfflineState failed, err(%v)", err)
		return err
	}
	return nil
}

func (cli *CliService) getNodeSetCapacity(cluster string) (int, error) {
	if cproto.IsRelease(cluster) {
		return 0, ErrUnSupportOperation
	}
	cvCache, err := cli.api.GetClusterViewCache(cluster, false)
	if err != nil {
		return 0, err
	}
	return cvCache.NodeSetCapacity, nil
}

func (cli *CliService) setNodeSetCapacity(cluster string, cap int64) error {
	if cproto.IsRelease(cluster) {
		return ErrUnSupportOperation
	}
	mc := cli.api.GetMasterClient(cluster)
	if _, err := mc.AdminAPI().SetNodeSetCapacity(int(cap)); err != nil {
		log.LogErrorf("setNodeState: SetNodeSetCapacity failed, err(%v)", err)
		return err
	}
	return nil
}

func (cli *CliService) getMonitorSummarySec(cluster string) (uint64, uint64, error) {
	if cproto.IsRelease(cluster) {
		limitInfoCache, err := cli.api.GetLimitInfoCacheRelease(cluster, false)
		if err != nil {
			return 0, 0, err
		}
		return limitInfoCache.MonitorSummarySec, limitInfoCache.MonitorReportSec, nil
	}
	limitInfoCache, err := cli.api.GetLimitInfoCache(cluster, false)
	if err != nil {
		return 0, 0, err
	}
	return limitInfoCache.MonitorSummarySec, limitInfoCache.MonitorSummarySec, nil
}

func (cli *CliService) getLogMaxSize(cluster string) (uint64, error) {
	if cproto.IsRelease(cluster) {
		return 0, ErrUnSupportOperation
	}
	limitInfoCache, err := cli.api.GetLimitInfoCache(cluster, false)
	if err != nil {
		return 0, err
	}
	return limitInfoCache.LogMaxSize, nil
}

func (cli *CliService) getTopoManager(cluster string) (int64, int64, error) {
	if cproto.IsRelease(cluster) {
		limitInfoCache, err := cli.api.GetLimitInfoCacheRelease(cluster, false)
		if err != nil {
			return 0, 0, err
		}
		return limitInfoCache.TopologyFetchIntervalMin, limitInfoCache.TopologyForceFetchIntervalSec, nil
	}
	limitInfoCache, err := cli.api.GetLimitInfoCache(cluster, false)
	if err != nil {
		return 0, 0, err
	}
	return limitInfoCache.TopologyFetchIntervalMin, limitInfoCache.TopologyForceFetchIntervalSec, nil
}

func (cli *CliService) getAutoMergeNodeSet(cluster string) (bool, error) {
	if cproto.IsRelease(cluster) {
		return false, ErrUnSupportOperation
	} else {
		cvCache, err := cli.api.GetClusterViewCache(cluster, false)
		if err != nil {
			return false, err
		}
		return cvCache.AutoMergeNodeSet, nil
	}
}

func (cli *CliService) setAutoMergeNodeSet(cluster string, enableAutoMerge bool) error {
	if cproto.IsRelease(cluster) {
		return ErrUnSupportOperation
	}
	return cli.api.AdminSetAutoMergeNodeSet(cluster, enableAutoMerge)
}

func (cli *CliService) getClusterFreeze(cluster string) (bool, error) {
	if cproto.IsRelease(cluster) {
		viewCache, err := cli.api.GetClusterViewCacheRelease(cluster, false)
		if err != nil {
			return false, err
		}
		return viewCache.DisableAutoAlloc, nil
	} else {
		viewCache, err := cli.api.GetClusterViewCache(cluster, false)
		if err != nil {
			return false, err
		}
		return viewCache.DisableAutoAlloc, nil
	}
}

func (cli *CliService) setClusterFreeze(cluster string, enableFreeze bool) error {
	if cproto.IsRelease(cluster) {
		rc := cli.api.GetReleaseClient(cluster)
		return rc.SetClusterFreeze(enableFreeze)
	} else {
		mc := cli.api.GetMasterClient(cluster)
		return mc.AdminAPI().IsFreezeCluster(enableFreeze)
	}
}
