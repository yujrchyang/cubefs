package apiManager

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/console/cutil"
	"github.com/cubefs/cubefs/console/model"
	cproto "github.com/cubefs/cubefs/console/proto"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util/log"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
)

var (
	sdk *APIManager // 用于其他service下的package调用
)

func SetSdkApiManager(s *APIManager) {
	sdk = s
}
func GetSdkApiManager() *APIManager {
	// 非定时任务链路 禁止调用
	return sdk
}

type APIManager struct {
	sync.RWMutex
	clusters      []*model.ConsoleCluster // for domain name
	masterClient  map[string]*master.MasterClient
	releaseClient map[string]*ReleaseClient

	CacheManager *cacheManager
}

func NewAPIManager(clusters []*model.ConsoleCluster) *APIManager {
	var (
		masterClients  = make(map[string]*master.MasterClient)
		releaseClients = make(map[string]*ReleaseClient)
	)
	for _, clusterInfo := range clusters {
		if clusterInfo.IsRelease {
			releaseClients[clusterInfo.ClusterName] = newReleaseClient(clusterInfo)
		} else {
			masterClients[clusterInfo.ClusterName] = newMasterclient(clusterInfo)
		}
	}
	manager := &APIManager{
		clusters:      clusters,
		masterClient:  masterClients,
		releaseClient: releaseClients,
	}
	manager.CacheManager = initCacheManager(manager)
	return manager
}

func newMasterclient(clusterInfo *model.ConsoleCluster) *master.MasterClient {
	addrs := strings.Split(clusterInfo.MasterAddrs, separator)
	mc := master.NewMasterClientWithoutTimeout(addrs, false)
	dnProf, _ := strconv.ParseUint(clusterInfo.DataProf, 10, 16)
	mnProf, _ := strconv.ParseUint(clusterInfo.MetaProf, 10, 16)
	fnProf, _ := strconv.ParseUint(clusterInfo.FlashProf, 10, 16)
	mc.DataNodeProfPort = uint16(dnProf)
	mc.MetaNodeProfPort = uint16(mnProf)
	mc.FlashNodeProfPort = uint16(fnProf)

	return mc
}

func newReleaseClient(clusterInfo *model.ConsoleCluster) *ReleaseClient {
	addrs := strings.Split(clusterInfo.MasterAddrs, separator)
	releaseClient := NewReleaseClient(addrs, clusterInfo.MasterDomain, clusterInfo.ClusterName, releaseClientDefaultTimeout)
	releaseClient.DatanodeProf = clusterInfo.DataProf
	releaseClient.MetanodeProf = clusterInfo.MetaProf
	return releaseClient
}

func (api *APIManager) IsRelease(clusterName string) bool {
	clusters := api.GetConsoleCluster()

	for _, cluster := range clusters {
		if cluster.ClusterName == clusterName {
			return cluster.IsRelease
		}
	}
	return false
}

func (api *APIManager) GetConsoleCluster() []*model.ConsoleCluster {
	api.RLock()
	defer api.RUnlock()

	clusters := api.clusters
	return clusters
}

func (api *APIManager) GetClusterInfo(cluster string) *model.ConsoleCluster {
	api.RLock()
	defer api.RUnlock()

	for _, clusterInfo := range api.clusters {
		if clusterInfo.ClusterName == cluster {
			return clusterInfo
		}
	}
	return nil
}

func (api *APIManager) GetDataNodeProf(cluster string) string {
	api.RLock()
	defer api.RUnlock()

	for _, clusterInfo := range api.clusters {
		if clusterInfo.ClusterName == cluster {
			return clusterInfo.DataProf
		}
	}
	return ""
}

func (api *APIManager) GetMetaNodeProf(cluster string) string {
	api.RLock()
	defer api.RUnlock()

	for _, clusterInfo := range api.clusters {
		if clusterInfo.ClusterName == cluster {
			return clusterInfo.MetaProf
		}
	}
	return ""
}

func (api *APIManager) GetMasterClient(cluster string) *master.MasterClient {
	api.RLock()
	defer api.RUnlock()

	client := api.masterClient[cluster]
	return client
}

func (api *APIManager) GetReleaseClient(cluster string) *ReleaseClient {
	api.RLock()
	defer api.RUnlock()

	client := api.releaseClient[cluster]
	return client
}

func (api *APIManager) GetClusterViewCache(cluster string, forceRefresh bool) (*proto.ClusterView, error) {
	value, err := api.CacheManager.GetClusterViewCache(cluster, forceRefresh)
	if err == nil {
		if cv, ok := value.(*proto.ClusterView); ok {
			return cv, nil
		}
	}
	log.LogDebugf("GetClusterViewCache: cluster:%v force:%v err:%v", cluster, forceRefresh, err)
	return nil, err
}

func (api *APIManager) GetClusterViewCacheRelease(cluster string, forceRefresh bool) (*cproto.ClusterView, error) {
	value, err := api.CacheManager.GetClusterViewCache(cluster, forceRefresh)
	if err == nil {
		if cv, ok := value.(*cproto.ClusterView); ok {
			return cv, nil
		}
	}
	log.LogDebugf("GetClusterViewCache: cluster:%v force:%v err:%v", cluster, forceRefresh, err)
	return nil, err
}

func (api *APIManager) UpdateClusterViewCache(cluster string) error {
	return api.CacheManager.UpdateClusterViewCache(cluster)
}

func (api *APIManager) UpdateFlashNodeViewCache(cluster string) error {
	return api.CacheManager.UpdateFlashNodeViewCache(cluster)
}

func (api *APIManager) GetNodeProfPort(cluster, module string) string {
	if cproto.IsRelease(cluster) {
		cv, err := api.GetClusterViewCacheRelease(cluster, false)
		if err != nil {
			return ""
		}
		switch module {
		case cproto.RoleNameDataNode:
			if len(cv.DataNodes) > 0 {
				_, port, found := strings.Cut(cv.DataNodes[0].Addr, ":")
				if found {
					return port
				}
			}
		case cproto.RoleNameMetaNode:
			if len(cv.MetaNodes) > 0 {
				_, port, found := strings.Cut(cv.MetaNodes[0].Addr, ":")
				if found {
					return port
				}
			}
		}
	} else {
		cv, err := api.GetClusterViewCache(cluster, false)
		if err != nil {
			return ""
		}
		switch module {
		case cproto.RoleNameDataNode:
			if len(cv.DataNodes) > 0 {
				_, port, found := strings.Cut(cv.DataNodes[0].Addr, ":")
				if found {
					return port
				}
			}
		case cproto.RoleNameMetaNode:
			if len(cv.MetaNodes) > 0 {
				_, port, found := strings.Cut(cv.MetaNodes[0].Addr, ":")
				if found {
					return port
				}
			}
		}
	}
	return ""
}

// 更新时机: get的时候发现过期 & 修改后更新
func (api *APIManager) GetLimitInfoCache(cluster string, forceUpdate bool) (*proto.LimitInfo, error) {
	info, err := api.CacheManager.GetLimitInfoCache(cluster, forceUpdate)
	if err == nil {
		if limitInfo, ok := info.(*proto.LimitInfo); ok {
			return limitInfo, nil
		}
	}
	log.LogDebugf("GetLimitInfoCache: cluster:%v force:%v err:%v", cluster, forceUpdate, err)
	return nil, err
}

func (api *APIManager) GetLimitInfoCacheRelease(cluster string, forceUpdate bool) (*cproto.LimitInfo, error) {
	info, err := api.CacheManager.GetLimitInfoCache(cluster, forceUpdate)
	if err == nil {
		if limitInfo, ok := info.(*cproto.LimitInfo); ok {
			return limitInfo, nil
		}
	}
	log.LogDebugf("GetLimitInfoCache: cluster:%v force:%v err:%v", cluster, forceUpdate, err)
	return nil, err
}

func (api *APIManager) UpdateLimitInfoCache(cluster string) error {
	return api.CacheManager.UpdateLimitInfoCache(cluster)
}

func (api *APIManager) GetClusterZoneNameList(cluster string) (zones []string) {
	zones = make([]string, 0)
	if cproto.IsRelease(cluster) {
		zones = append(zones, cproto.All)
		return
	}
	zoneList := api.GetDatanodeZoneList(cluster)
	for _, zone := range zoneList {
		zones = append(zones, zone)
	}
	return zones
}

func (api *APIManager) GetDatanodeZoneList(cluster string) []string {
	if cproto.IsRelease(cluster) {
		return nil
	}
	mc := api.GetMasterClient(cluster)
	// zone/list接口会包含废弃的zone
	cs, err := mc.AdminAPI().GetClusterStat()
	if err != nil {
		log.LogErrorf("GetDatanodeZoneList failed: GetClusterStat err(%v)", err)
		return nil
	}
	zoneList := make([]string, 0)
	for zone := range cs.ZoneStatInfo {
		zoneList = append(zoneList, zone)
	}
	sort.Strings(zoneList)
	return zoneList
}

func (api *APIManager) AdminGetCluster(cluster string) (*proto.ClusterView, error) {
	clusterInfo := api.GetClusterInfo(cluster)
	req := cutil.NewAPIRequest(http.MethodGet, fmt.Sprintf("http://%s%s", clusterInfo.MasterDomain, proto.AdminGetCluster))
	data, err := cutil.SendSimpleRequest(req, false)
	if err != nil {
		log.LogErrorf("adminGetCluster failed: cluster(%v) err(%v)", cluster, err)
		return nil, err
	}
	cv := new(proto.ClusterView)
	if err = json.Unmarshal(data, cv); err != nil {
		return nil, err
	}
	return cv, nil
}

func (api *APIManager) SetRatelimitInfo(cluster string, args map[string]string) error {
	if cproto.IsRelease(cluster) {
		return api.GetReleaseClient(cluster).SetRatelimit(args)
	} else {
		return api.setRatelimitInfo(cluster, args)
	}
}

// 目前只支持增 不支持删。删除的话需要设0
func (api *APIManager) setRatelimitInfo(cluster string, args map[string]string) error {
	info := api.GetClusterInfo(cluster)
	req := cutil.NewAPIRequest(http.MethodPost, fmt.Sprintf("http://%s%s", info.MasterDomain, AdminSetNodeInfo))
	for key, value := range args {
		req.AddParam(key, value)
	}
	_, err := cutil.SendSimpleRequest(req, false)
	if err != nil {
		log.LogErrorf("setRatelimitInfo failed: cluster[%v] args[%v] err:%v", cluster, args, err)
	}
	return err
}

func (api *APIManager) UpdateVolume(cluster, vol string, args map[string]string) error {
	info := api.GetClusterInfo(cluster)
	req := cutil.NewAPIRequest(http.MethodPost, fmt.Sprintf("http://%s%s", info.MasterDomain, AdminUpdateVol))
	req.AddParam("name", vol)
	for key, value := range args {
		req.AddParam(key, value)
	}
	_, err := cutil.SendSimpleRequest(req, false)
	if err != nil {
		log.LogErrorf("UpdateVolume failed: cluster(%v)vol(%v)args(%v)err(%v)", cluster, vol, err)
	}
	return err
}

func (api *APIManager) CreateVolume(cluster, name, owner, zone, description string, capacity int, replicaNum int,
) error {
	var (
		defaultMpCount          = 3
		defaultDpSize           = 120 // GB
		defaultFollowerRead     = true
		defaultForceROW         = false
		defaultEnableWriteCache = false
		crossRegionHAType       = 0 // 0-default, 1-quorum
		defaultMpReplicas       = 3
		defaultVolWriteMutex    = false
		defaultTrashDays        = 0
		defaultStoreMode        = 1 // 1-Mem 2- Rocks
		defaultBitMapAllocator  = false
	)
	info := api.GetClusterInfo(cluster)
	req := cutil.NewAPIRequest(http.MethodPost, fmt.Sprintf("http://%s%s", info.MasterDomain, AdminCreateVol))
	req.AddHeader("isTimeOut", "false")
	req.AddParam("name", name)
	req.AddParam("owner", owner)
	req.AddParam("zoneName", zone)
	req.AddParam("description", description)
	req.AddParam("capacity", strconv.Itoa(capacity))
	req.AddParam("replicaNum", strconv.Itoa(replicaNum))
	req.AddParam("mpCount", strconv.Itoa(defaultMpCount))
	req.AddParam("size", strconv.Itoa(defaultDpSize))
	req.AddParam("ecDataNum", strconv.Itoa(4))
	req.AddParam("ecParityNum", strconv.Itoa(2))
	req.AddParam("ecEnable", strconv.FormatBool(false))
	req.AddParam("followerRead", strconv.FormatBool(defaultFollowerRead))
	req.AddParam("forceROW", strconv.FormatBool(defaultForceROW))
	req.AddParam("writeCache", strconv.FormatBool(defaultEnableWriteCache))
	req.AddParam("crossRegion", strconv.Itoa(crossRegionHAType))
	req.AddParam("autoRepair", strconv.FormatBool(false))
	req.AddParam("mpReplicaNum", strconv.Itoa(defaultMpReplicas))
	req.AddParam("volWriteMutex", strconv.FormatBool(defaultVolWriteMutex))
	req.AddParam("trashRemainingDays", strconv.Itoa(defaultTrashDays))
	req.AddParam("storeMode", strconv.Itoa(defaultStoreMode))
	req.AddParam("metaLayout", "")
	req.AddParam("smart", strconv.FormatBool(false))
	req.AddParam("smartRules", "")
	req.AddParam("compactTag", "")
	req.AddParam("hostDelayInterval", strconv.Itoa(0))
	req.AddParam("batchDelInodeCnt", strconv.Itoa(0))
	req.AddParam("delInodeInterval", strconv.Itoa(0))
	req.AddParam(proto.EnableBitMapAllocatorKey, strconv.FormatBool(defaultBitMapAllocator))

	data, err := cutil.SendSimpleRequest(req, false)
	if err != nil {
		log.LogErrorf("CreateVolume failed: vol(%v)owner(%v) err(%v)", name, owner, err)
	}
	log.LogInfof("CreateVolume success: msg(%s)", string(data))
	return err
}

func (api *APIManager) SetDiskUsageByKeyValue(cluster string, params map[string]string) error {
	info := api.GetClusterInfo(cluster)
	req := cutil.NewAPIRequest(http.MethodPost, fmt.Sprintf("http://%s%s", info.MasterDomain, AdminSetDiskUsage))
	for key, value := range params {
		req.AddParam(key, value)
	}

	data, err := cutil.SendSimpleRequest(req, false)
	if err != nil {
		log.LogErrorf("SetDiskUsageByKeyValue failed: cluster(%s) params(%v) err:%v", cluster, params, err)
		return err
	}
	log.LogInfof("SetDiskUsageByKeyValue success: cluster(%v) params(%v) msg(%v)", cluster, params, string(data))
	return nil
}

func (api *APIManager) CreateVolByParamsMap(cluster string, params map[string]string) error {
	info := api.GetClusterInfo(cluster)
	req := cutil.NewAPIRequest(http.MethodPost, fmt.Sprintf("http://%s%s", info.MasterDomain, AdminCreateVol))
	for key, value := range params {
		req.AddParam(key, value)
	}

	data, err := cutil.SendSimpleRequest(req, false)
	if err != nil {
		log.LogErrorf("CreateVolByParamsMap failed: cluster(%s) params(%v) err:%v", cluster, params, err)
		return err
	}
	log.LogInfof("CreateVolByParamsMap success: cluster(%v) params(%v) msg(%v)", cluster, params, string(data))
	return nil
}

func (api *APIManager) ReloadMetaPartition(host string, pid uint64) (string, error) {
	req := cutil.NewAPIRequest(http.MethodPost, fmt.Sprintf("http://%s%s", host, ReloadPartition))
	req.AddParam("pid", strconv.FormatUint(pid, 10))

	data, err := cutil.SendSimpleRequest(req, false)
	if err != nil {
		log.LogErrorf("ReloadMetaPartition failed: host(%s) mpID(%v) err:%v", host, pid, err)
		return "", err
	}
	return string(data), err
}

func (api *APIManager) ListUsersOfVol(cluster, vol string) ([]string, error) {
	info := api.GetClusterInfo(cluster)
	req := cutil.NewAPIRequest(http.MethodGet, fmt.Sprintf("http://%s%s", info.MasterDomain, UsersOfVol))
	req.AddParam("name", vol)
	data, err := cutil.SendSimpleRequest(req, false)
	if err != nil {
		log.LogErrorf("ListUsersOfVol failed: cluster(%v)vol(%v)err(%v)", cluster, vol, err)
		return nil, err
	}
	users := make([]string, 0)
	if err = json.Unmarshal(data, &users); err != nil {
		return nil, err
	}
	return users, nil
}

func (api *APIManager) GetModuleTopo(cluster, module string) *cproto.CFSTopology {

	mc := api.GetMasterClient(cluster)
	var (
		result = &cproto.CFSTopology{
			DataMap:  make(map[string]*cproto.ZoneView),
			MetaMap:  make(map[string]*cproto.ZoneView),
			FlashMap: make(map[string]*cproto.ZoneView),
		}
		topo           *proto.TopologyView
		zoneFlashNodes map[string][]*proto.FlashNodeViewInfo
		err            error
	)
	// all
	switch module {
	case "datanode":
		topo, err = mc.AdminAPI().GetTopology()

	case "metanode":
		topo, err = mc.AdminAPI().GetTopology()

	case "flashnode":
		zoneFlashNodes, err = mc.AdminAPI().GetAllFlashNodes(true)
	default:
	}
	if err != nil {
		log.LogErrorf("GetModuleTopo failed: cluster(%v) module(%v) err(%v)", cluster, module, err)
		return nil
	}
	log.LogDebugf("GetModuleTopo: topo(%v) zoneOfFlash(%v) cluster(%v) module(%v)", topo, zoneFlashNodes, cluster, module)
	if topo != nil {
		for _, zone := range topo.Zones {
			zoneName := zone.Name
			datanodes := make([]string, 0)
			metanodes := make([]string, 0)
			for _, setView := range zone.NodeSet {
				for _, nodeView := range setView.DataNodes {
					datanodes = append(datanodes, nodeView.Addr)
				}
				for _, nodeView := range setView.MetaNodes {
					metanodes = append(metanodes, nodeView.Addr)
				}
			}
			result.DataMap[zoneName] = &cproto.ZoneView{
				IPs: datanodes,
			}
			result.MetaMap[zoneName] = &cproto.ZoneView{
				IPs: metanodes,
			}
		}
	}
	if zoneFlashNodes != nil {
		for zone, nodeList := range zoneFlashNodes {
			flashnodes := make([]string, 0)
			for _, node := range nodeList {
				flashnodes = append(flashnodes, node.Addr)
			}
			result.FlashMap[zone] = &cproto.ZoneView{
				IPs: flashnodes,
			}
		}
	}
	log.LogDebugf("GetModuleTopo: result: %v", result)
	return result
}

func (api *APIManager) GetTopology(cluster string) *proto.TopologyView {
	if cproto.IsRelease(cluster) {
		//AdminGetPodMapInfo            = "/admin/getPodMapInfo"
		return nil
	}
	mc := api.GetMasterClient(cluster)
	topo, err := mc.AdminAPI().GetTopology()
	if err != nil {
		log.LogErrorf("GetTopology failed: cluster(%v) err(%v)", cluster, err)
		return nil
	}
	return topo
}

func (api *APIManager) GetVolNameList(cluster string) ([]string, error) {
	if cproto.IsRelease(cluster) {
		rc := api.GetReleaseClient(cluster)
		return rc.GetAllVolList()
	}

	vols, err := api.CacheManager.GetVolListCache(cluster, true)
	if err != nil {
		return nil, err
	}
	return vols, nil
}

func (api *APIManager) AdminGetAPIReqBwRateLimitInfo(cluster string) (map[uint8]int64, error) {
	clusterInfo := api.GetClusterInfo(cluster)
	req := cutil.NewAPIRequest(http.MethodGet, fmt.Sprintf("http://%s%s", clusterInfo.MasterDomain, proto.AdminGetAPIReqBwRateLimitInfo))
	data, err := cutil.SendSimpleRequest(req, false)
	if err != nil {
		log.LogErrorf("AdminGetAPIReqBwRateLimitInfo failed: cluster(%v)err(%v)", cluster, err)
		return nil, err
	}
	limitInfo := new(proto.LimitInfo)
	if err = json.Unmarshal(data, &limitInfo); err != nil {
		return nil, err
	}
	return limitInfo.ApiReqBwRateLimitMap, nil
}

func (api *APIManager) AdminSetBandwidthLimiter(cluster string, bwLimit uint64) error {
	clusterInfo := api.GetClusterInfo(cluster)
	req := cutil.NewAPIRequest(http.MethodGet, fmt.Sprintf("http://%s%s", clusterInfo.MasterDomain, proto.AdminBandwidthLimiterSet))
	req.AddParam("bw", strconv.FormatUint(bwLimit, 10))
	_, err := cutil.SendSimpleRequest(req, false)
	if err != nil {
		log.LogErrorf("AdminGetAPIReqBwRateLimitInfo failed: cluster(%v)err(%v)", cluster, err)
	}
	return err
}

func (api *APIManager) AdminSetAutoMergeNodeSet(cluster string, enable bool) error {
	clusterInfo := api.GetClusterInfo(cluster)
	req := cutil.NewAPIRequest(http.MethodGet, fmt.Sprintf("http://%s%s", clusterInfo.MasterDomain, proto.AdminClusterAutoMergeNodeSet))
	req.AddParam("enable", strconv.FormatBool(enable))
	_, err := cutil.SendSimpleRequest(req, false)
	if err != nil {
		log.LogErrorf("AdminSetAutoMergeNodeSet failed: cluster(%v) err(%v)", cluster, err)
	}
	return err
}

func (api *APIManager) CleanVolTrash(cluster, volname string) error {
	trashServer := "11.39.2.223:17333"
	req := cutil.NewAPIRequest(http.MethodGet, fmt.Sprintf("http://%s%s", trashServer, "/trash/clean"))
	req.AddParam("cluster", cluster)
	req.AddParam("vol", volname)
	req.AddParam("path", "/")
	_, err := cutil.SendSimpleRequest(req, false)
	if err != nil {
		log.LogErrorf("CleanVolTrash failed: vol(%v) err(%v)", volname, err)
		return err
	}
	log.LogInfof("CleanVolTrash success: cluster(%v) vol(%v)", cluster, volname)
	return err
}

func (api *APIManager) MigrateConfigList(cluster string) ([]*cproto.MigrateConfig, error) {
	req := cutil.NewAPIRequest(http.MethodGet, fmt.Sprintf("http://%s%s", api.GetClusterInfo(cluster).FileMigrateHost, "/migrationConfig/list"))
	req.AddParam("cluster", cluster)
	data, err := cutil.SendSimpleRequest(req, false)
	if err != nil {
		log.LogErrorf("MigrateConfigList failed: cluster(%v) err(%v)", cluster, err)
		return nil, err
	}
	configList := make([]*cproto.MigrateConfig, 0)
	if err = json.Unmarshal(data, &configList); err != nil {
		log.LogErrorf("MigrateConfigList failed: cluster(%v) err(%v)", cluster, err)
		return nil, err
	}
	log.LogInfof("MigrateConfigList success: cluster(%v) len(config)=%v", cluster, len(configList))
	return configList, nil
}

func (api *APIManager) GetVolumeMigrateConfig(cluster, volume string) (*cproto.MigrateConfig, error) {
	req := cutil.NewAPIRequest(http.MethodGet, fmt.Sprintf("http://%s%s", api.GetClusterInfo(cluster).FileMigrateHost, "/migrationConfig/list"))
	req.AddParam("cluster", cluster)
	req.AddParam("volume", volume)
	data, err := cutil.SendSimpleRequest(req, false)
	if err != nil {
		log.LogErrorf("GetVolumeMigrateConfig failed: cluster(%v) vol(%v) err(%v)", cluster, volume, err)
		return nil, err
	}
	configList := make([]*cproto.MigrateConfig, 0)
	if err = json.Unmarshal(data, &configList); err != nil {
		log.LogErrorf("GetVolumeMigrateConfig failed: cluster(%v) vol(%v) err(%v)", cluster, volume, err)
		return nil, err
	}
	if len(configList) != 1 {
		log.LogErrorf("GetVolumeMigrateConfig: wrong config list length, len(%v)", len(configList))
		return nil, nil
	}
	log.LogInfof("GetVolumeMigrateConfig success: cluster(%v) config(%v)", cluster, configList[0])
	return configList[0], nil

}

func (api *APIManager) CreateOrUpdateMigrateConfig(cluster, volume string, params map[string]string) error {
	req := cutil.NewAPIRequest(http.MethodGet, fmt.Sprintf("http://%s%s", api.GetClusterInfo(cluster).FileMigrateHost, "/migrationConfig/addOrUpdate"))
	req.AddParam("cluster", cluster)
	req.AddParam("volume", volume)
	for key, param := range params {
		switch param {
		case "true":
			param = "1"
		case "false":
			param = "0"
		}
		req.AddParam(key, param)
	}
	_, err := cutil.SendSimpleRequest(req, false)
	if err != nil {
		log.LogErrorf("UpdateMigrateConfig failed: cluster(%v) vol(%v) params(%v) err(%v)", cluster, params, err)
	}
	return err
}

func (api *APIManager) GetMetaAllPartitions(cluster, addr string) ([]*cproto.PartitionOnMeta, error) {
	host := fmt.Sprintf("%s:%s", strings.Split(addr, ":")[0], api.GetMetaNodeProf(cluster))
	req := cutil.NewAPIRequest(http.MethodGet, fmt.Sprintf("http://%s%s", host, "/getAllPartitions"))
	req.AddHeader("Accept", "application/json")

	body, err := cutil.SendSimpleRequest(req, false)
	if err != nil {
		return nil, err
	}
	all := make([]*cproto.PartitionOnMeta, 0)
	if err = json.Unmarshal(body, &all); err != nil {
		return nil, err
	}
	return all, err
}

func (api *APIManager) GetDataAllPartitions(cluster, addr string) ([]*cproto.PartitionOnData, error) {
	host := fmt.Sprintf("%s:%s", strings.Split(addr, ":")[0], api.GetDataNodeProf(cluster))
	req := cutil.NewAPIRequest(http.MethodGet, fmt.Sprintf("http://%s%s", host, "/partitions"))
	req.AddHeader("Accept", "application/json")

	body, err := cutil.SendSimpleRequest(req, false)
	if err != nil {
		return nil, err
	}
	all := new(cproto.AllDataPartitions)
	if err = json.Unmarshal(body, &all); err != nil {
		return nil, err
	}
	return all.Partitions, err
}
