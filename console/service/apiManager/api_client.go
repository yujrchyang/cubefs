package apiManager

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/console/cutil"
	"github.com/cubefs/cubefs/util/log"
	"net/http"
	"strconv"
	"time"

	cproto "github.com/cubefs/cubefs/console/proto"
)

const (
	AdminGetCluster   = "/admin/getCluster"
	AdminGetLimitInfo = "/admin/getLimitInfo"
	AdminSetNodeInfo  = "/admin/setNodeInfo"
	AdminGetALLVols   = "/admin/listVols"
	AdminGetVol       = "/admin/getVol"
	AdminCreateVol    = "/admin/createVol"
	AdminDeleteVol    = "/vol/delete"
	AdminUpdateVol    = "/vol/update"
	UsersOfVol        = "/vol/users"

	//node
	GetDataNode     = "/dataNode/get"
	GetMetaNode     = "/metaNode/get"
	MetaNodeOffline = "/metaNode/offline"
	DataNodeOffline = "/dataNode/offline"
	DiskOffLine     = "/disk/offline"
	ReloadPartition = "/reloadPartition"

	AdminGetMetaPartition     = "/metaPartition/get"
	AdminGetDataPartition     = "/dataPartition/get"
	AdminMetaPartitionOffline = "/metaPartition/offline"
	AdminDataPartitionOffline = "/dataPartition/offline"
	AdminLoadMetaPartition    = "/metaPartition/load"
	AdminLoadDataPartition    = "/dataPartition/load"
	AdminCreateMP             = "/metaPartition/create"
	AdminCreateDataPartition  = "/dataPartition/create"
	ClientDataPartition       = "/client/dataPartitions"
	ClientMetaPartition       = "/client/metaPartitions"

	AdminAddMetaReplica    = "/metaReplica/add"
	AdminDeleteMetaReplica = "/metaReplica/delete"

	AdminSetMetaNodeThreshold = "/threshold/set"
	BandwidthLimiterSet       = "/bwLimiter/set"
	GetAPIReqBwRateLimitInfo  = "/apiLimitInfo/get"
	AdminClusterFreeze        = "/cluster/freeze"

	AdminSetDiskUsage = "/diskUsage/set"
)

const (
	separator                   = ","
	releaseClientDefaultTimeout = 10 * time.Second
)

type ReleaseClient struct {
	masters    []string
	timeout    time.Duration
	leaderAddr string
	domain     string
	cluster    string

	datanodeProf string
	metanodeProf string
}

func NewReleaseClient(addrs []string, domain, cluster string, timeout time.Duration) *ReleaseClient {
	client := new(ReleaseClient)
	client.masters = addrs
	client.domain = domain
	client.cluster = cluster
	client.timeout = timeout
	return client
}

func (c *ReleaseClient) GetDomain() string {
	return c.domain
}

func (c *ReleaseClient) GetClusterView() (*cproto.ClusterView, error) {
	req := cutil.NewAPIRequest(http.MethodGet, fmt.Sprintf("http://%s%s", c.domain, AdminGetCluster))

	data, err := cutil.SendSimpleRequest(req, true)
	if err != nil {
		log.LogErrorf("ReleaseClient GetClusterView failed: err:%s", err.Error())
		return nil, err
	}

	cv := new(cproto.ClusterView)
	if err = json.Unmarshal(data, cv); err != nil {
		log.LogErrorf("ReleaseClient GetClusterView failed: %s", err.Error())
		return nil, err
	}

	return cv, nil
}

func (c *ReleaseClient) GetLimitInfo(vol string) (*cproto.LimitInfo, error) {
	args := make(map[string]string)
	args["name"] = vol

	req := cutil.NewAPIRequest(http.MethodPost, fmt.Sprintf("http://%s%s", c.domain, AdminGetLimitInfo))
	for key, value := range args {
		req.AddParam(key, value)
	}

	data, err := cutil.SendSimpleRequest(req, false)
	if err != nil {
		log.LogErrorf("ReleaseClient GetLimitInfo failed, vol(%v) err(%v)", vol, err)
		return nil, err
	}
	limitInfo := &cproto.LimitInfo{}
	if err = json.Unmarshal(data, limitInfo); err != nil {
		log.LogErrorf("ReleaseClient json Unmarshal limitInfo failed: vol(%s) err: %s", vol, err.Error())
		return nil, err
	}
	return limitInfo, nil
}

func (c *ReleaseClient) SetRatelimit(args map[string]string) error {
	req := cutil.NewAPIRequest(http.MethodPost, fmt.Sprintf("http://%s%s", c.domain, AdminSetNodeInfo))
	for key, value := range args {
		req.AddParam(key, value)
	}

	_, err := cutil.SendSimpleRequest(req, false)
	if err != nil {
		log.LogErrorf("ReleaseClient SetRatelimit failed, args(%v) err(%v)", args, err)
	}
	return err
}

func (c *ReleaseClient) GetAllVolList() ([]string, error) {
	req := cutil.NewAPIRequest(http.MethodPost, fmt.Sprintf("http://%s%s", c.domain, AdminGetALLVols))

	data, err := cutil.SendSimpleRequest(req, true)
	if err != nil {
		log.LogErrorf("ReleaseClient GetAllVolList failed: err(%v)", err)
		return nil, err
	}
	vols := make([]string, 0)
	if err = json.Unmarshal(data, &vols); err != nil {
		log.LogErrorf("ReleaseClient GetAllVolList json Unmarshal failed: %s", err.Error())
		return nil, err
	}
	return vols, nil
}

func (c *ReleaseClient) AdminGetVol(volName string) (*cproto.VolView, error) {
	req := cutil.NewAPIRequest(http.MethodGet, fmt.Sprintf("http://%s%s", c.domain, AdminGetVol))
	req.AddParam("name", volName)

	data, err := cutil.SendSimpleRequest(req, true)
	if err != nil {
		log.LogErrorf("ReleaseClient AdminGetVol failed: vol(%v)err(%v)", volName, err)
		return nil, err
	}
	volView := new(cproto.VolView)
	if err = json.Unmarshal(data, volView); err != nil {
		log.LogErrorf("ReleaseClient AdminGetVol json Unmarshal failed: vol(%v)err(%v)", volName, err)
		return nil, err
	}
	return volView, nil
}

func (c *ReleaseClient) DeleteVolume(volName, authKey string) error {
	req := cutil.NewAPIRequest(http.MethodPost, fmt.Sprintf("http://%s%s", c.domain, AdminDeleteVol))
	req.AddParam("name", volName)
	req.AddParam("authKey", authKey)

	data, err := cutil.SendSimpleRequest(req, true)
	if err != nil {
		log.LogErrorf("ReleaseClient DeleteVolume failed: err(%v)", err)
		return err
	}
	log.LogInfof("ReleaseClient DeleteVolume: name(%v) msg(%v)", volName, data)
	return nil
}

func (c *ReleaseClient) UpdateVolView(name, authKey string,
	capacity uint64,
	enableToken bool,
) error {
	req := cutil.NewAPIRequest(http.MethodPost, fmt.Sprintf("http://%s%s", c.domain, AdminUpdateVol))
	req.AddParam("name", name)
	req.AddParam("authKey", authKey)
	req.AddParam("capacity", strconv.FormatUint(capacity, 10))
	req.AddParam("enableToken", strconv.FormatBool(enableToken))

	data, err := cutil.SendSimpleRequest(req, true)
	if err != nil {
		log.LogErrorf("ReleaseClient UpdateVolView failed: vol(%v)err(%v)", name, err)
		return err
	}
	log.LogInfof("ReleaseClient UpdateVolView: name(%v) msg(%v)", name, data)
	return nil
}

func (c *ReleaseClient) CreateDataPartition(volume string, count int) error {
	req := cutil.NewAPIRequest(http.MethodPost, fmt.Sprintf("http://%s%s", c.domain, AdminCreateDataPartition))
	req.AddParam("name", volume)
	req.AddParam("count", strconv.Itoa(count))
	req.AddParam("type", "extent")

	data, err := cutil.SendSimpleRequest(req, true)
	if err != nil {
		log.LogErrorf("ReleaseClient: add dp failed: vol(%v)err(%v)", volume, err)
		return err
	}
	log.LogInfof("ReleaseClient add dp: name(%v) msg(%v)", volume, data)
	return nil
}

func (c *ReleaseClient) CreateMetaPartition(volume string, inodestart uint64) error {
	req := cutil.NewAPIRequest(http.MethodPost, fmt.Sprintf("http://%s%s", c.domain, AdminCreateMP))
	req.AddParam("name", volume)
	req.AddParam("start", strconv.FormatUint(inodestart, 10))

	data, err := cutil.SendSimpleRequest(req, true)
	if err != nil {
		log.LogErrorf("ReleaseClient: add mp failed: vol(%v)start(%v)err(%v)", volume, inodestart, err)
		return err
	}
	log.LogInfof("ReleaseClient add mp: name(%v)start(%v)msg(%v)", volume, inodestart, data)
	return nil
}

// 是否可以把默认的参数 放在这边 上层接口只要需要的参数即可调用
func (c *ReleaseClient) CreateVol(name, owner string, replicaNum int, capacity int,
) error {
	var (
		defaultVolType     = "extent" // extent && tiny
		defaultMpCount     = 1
		minWritableDPNum   = 5
		minWritableMPNum   = 1
		crossPod           = false
		autoRepairCrossPod = false
		enableToken        = false
		enableBitmap       = false
	)
	req := cutil.NewAPIRequest(http.MethodPost, fmt.Sprintf("http://%s%s", c.domain, AdminCreateVol))
	req.AddParam("name", name)
	req.AddParam("owner", owner)
	req.AddParam("replicas", strconv.Itoa(replicaNum))
	req.AddParam("type", defaultVolType)
	req.AddParam("capacity", strconv.Itoa(capacity))
	req.AddParam("mpCount", strconv.Itoa(defaultMpCount))
	req.AddParam("minWritableDp", strconv.Itoa(minWritableDPNum))
	req.AddParam("minWritableMp", strconv.Itoa(minWritableMPNum))
	req.AddParam("crossPod", strconv.FormatBool(crossPod))
	req.AddParam("autoRepair", strconv.FormatBool(autoRepairCrossPod))
	req.AddParam("enableToken", strconv.FormatBool(enableToken))
	req.AddParam("enableBitMapAllocator", strconv.FormatBool(enableBitmap))

	data, err := cutil.SendSimpleRequest(req, true)
	if err != nil {
		log.LogErrorf("ReleaseClient CreateVol failed: name(%v)owner(%v) err(%v)", name, owner, err)
		return err
	}
	log.LogInfof("ReleaseClient CreateVol: name(%v)owner(%v)capacity(%v) msg(%v)", name, owner, capacity, data)
	return nil
}

func (c *ReleaseClient) SetDiskUsageByKeyValue(args map[string]string) error {
	req := cutil.NewAPIRequest(http.MethodPost, fmt.Sprintf("http://%s%s", c.domain, AdminSetDiskUsage))
	for key, value := range args {
		req.AddParam(key, value)
	}

	data, err := cutil.SendSimpleRequest(req, true)
	if err != nil {
		log.LogErrorf("ReleaseClient SetDiskUsageByKeyValue failed: args(%v) err(%v)", args, err)
		return err
	}
	log.LogInfof("ReleaseClient SetDiskUsageByKeyValue: args(%v) msg(%v)", args, data)
	return nil
}

func (c *ReleaseClient) CreateVolByParamsMap(args map[string]string) error {
	req := cutil.NewAPIRequest(http.MethodPost, fmt.Sprintf("http://%s%s", c.domain, AdminCreateVol))
	for key, value := range args {
		req.AddParam(key, value)
	}

	data, err := cutil.SendSimpleRequest(req, true)
	if err != nil {
		log.LogErrorf("ReleaseClient CreateVolByParamsMap failed: args(%v) err(%v)", args, err)
		return err
	}
	log.LogInfof("ReleaseClient CreateVolByParamsMap: args(%v) msg(%v)", args, data)
	return nil

}

func (c *ReleaseClient) UpdateVolume(volName string, args map[string]string) error {
	req := cutil.NewAPIRequest(http.MethodPost, fmt.Sprintf("http://%s%s", c.domain, AdminUpdateVol))
	req.AddParam("name", volName)
	for key, value := range args {
		req.AddParam(key, value)
	}

	data, err := cutil.SendSimpleRequest(req, true)
	if err != nil {
		log.LogErrorf("ReleaseClient UpdateVolume failed: args(%v) err(%v)", args, err)
		return err
	}
	log.LogInfof("ReleaseClient UpdateVolume: args(%v) msg(%v)", args, data)
	return nil
}

func (c *ReleaseClient) GetMetaNode(addr string) (*cproto.MetaNode, error) {
	req := cutil.NewAPIRequest(http.MethodGet, fmt.Sprintf("http://%s%s", c.domain, GetMetaNode))
	req.AddParam("addr", addr)

	data, err := cutil.SendSimpleRequest(req, true)
	if err != nil {
		log.LogErrorf("ReleaseClient GetMetaNode failed: addr(%v) err(%v)", addr, err)
		return nil, err
	}

	nodeInfo := new(cproto.MetaNode)
	if err = json.Unmarshal(data, nodeInfo); err != nil {
		log.LogErrorf("ReleaseClient GetMetaNode json Unmarshal failed: addr(%v) %s", addr, err.Error())
		return nil, err
	}
	return nodeInfo, nil
}

func (c *ReleaseClient) GetDataNode(addr string) (*cproto.DataNode, error) {
	req := cutil.NewAPIRequest(http.MethodGet, fmt.Sprintf("http://%s%s", c.domain, GetDataNode))
	req.AddParam("addr", addr)

	data, err := cutil.SendSimpleRequest(req, true)
	if err != nil {
		log.LogErrorf("ReleaseClient GetDataNode failed: addr(%v) err(%v)", addr, err)
		return nil, err
	}

	nodeInfo := new(cproto.DataNode)
	if err = json.Unmarshal(data, nodeInfo); err != nil {
		log.LogErrorf("ReleaseClient GetDataNode json Unmarshal failed: addr(%s) err(%s)", addr, err.Error())
		return nil, err
	}
	return nodeInfo, nil
}

func (c *ReleaseClient) GetMetaPartition(mpID uint64) (*cproto.MetaPartition, error) {
	req := cutil.NewAPIRequest(http.MethodGet, fmt.Sprintf("http://%s%s", c.domain, AdminGetMetaPartition))
	req.AddParam("id", strconv.FormatUint(mpID, 10))

	data, err := cutil.SendSimpleRequest(req, true)
	if err != nil {
		log.LogErrorf("ReleaseClient GetMetaPartition failed: mpID(%v) err(%v)", mpID, err)
		return nil, err
	}

	partitionInfo := new(cproto.MetaPartition)
	if err = json.Unmarshal(data, partitionInfo); err != nil {
		log.LogErrorf("ReleaseClient GetMetaPartition json Unmarshal failed: mpID(%v) %s", mpID, err.Error())
		return nil, err
	}
	return partitionInfo, nil
}

func (c *ReleaseClient) GetDataPartition(dpID uint64) (*cproto.DataPartition, error) {
	req := cutil.NewAPIRequest(http.MethodGet, fmt.Sprintf("http://%s%s", c.domain, AdminGetDataPartition))
	req.AddParam("id", strconv.FormatUint(dpID, 10))

	data, err := cutil.SendSimpleRequest(req, true)
	if err != nil {
		log.LogErrorf("ReleaseClient GetDataPartition failed: dpID(%v) err(%v)", dpID, err)
		return nil, err
	}

	partitionInfo := new(cproto.DataPartition)
	if err = json.Unmarshal(data, partitionInfo); err != nil {
		log.LogErrorf("ReleaseClient GetDataPartition json Unmarshal failed: dpID(%v) %s", dpID, err.Error())
		return nil, err
	}
	return partitionInfo, nil
}

func (c *ReleaseClient) MetaNodeOffline(addr, dest string) error {
	req := cutil.NewAPIRequest(http.MethodPost, fmt.Sprintf("http://%s%s", c.domain, MetaNodeOffline))
	req.AddParam("addr", addr)
	req.AddParam("destAddr", dest)

	data, err := cutil.SendSimpleRequest(req, true)
	if err != nil {
		log.LogErrorf("ReleaseClient MetaNodeOffline failed: addr(%v)->dest(%v) err(%v)", addr, dest, err)
		return err
	}
	log.LogInfof("ReleaseClient MetaNodeOffline success: msg(%s)", string(data))
	return nil
}

func (c *ReleaseClient) DataNodeOffline(addr, dest string) error {
	req := cutil.NewAPIRequest(http.MethodPost, fmt.Sprintf("http://%s%s", c.domain, DataNodeOffline))
	req.AddParam("addr", addr)
	req.AddParam("destAddr", dest)

	data, err := cutil.SendSimpleRequest(req, true)
	if err != nil {
		log.LogErrorf("ReleaseClient DataNodeOffline failed: addr(%v)->dest(%v) err(%v)", addr, dest, err)
		return err
	}
	log.LogInfof("ReleaseClient DataNodeOffline success: msg(%s)", string(data))
	return nil
}

func (c *ReleaseClient) DiskOffline(addr, disk, dest string) error {
	req := cutil.NewAPIRequest(http.MethodPost, fmt.Sprintf("http://%s%s", c.domain, DiskOffLine))
	req.AddParam("addr", addr)
	req.AddParam("disk", disk)
	req.AddParam("destAddr", dest)

	data, err := cutil.SendSimpleRequest(req, true)
	if err != nil {
		log.LogErrorf("ReleaseClient DiskOffline failed: disk(%v) addr(%v)->dest(%v) err(%v)", disk, addr, dest, err)
		return err
	}
	log.LogInfof("ReleaseClient DiskOffline success: msg(%s)", string(data))
	return nil
}

func (c *ReleaseClient) MetaPartitionOffline(addr, volName string, mpID uint64) error {
	req := cutil.NewAPIRequest(http.MethodPost, fmt.Sprintf("http://%s%s", c.domain, AdminMetaPartitionOffline))
	req.AddParam("addr", addr)
	req.AddParam("name", volName)
	req.AddParam("id", strconv.FormatUint(mpID, 10))
	req.AddParam("destAddr", "")

	data, err := cutil.SendSimpleRequest(req, true)
	if err != nil {
		log.LogErrorf("ReleaseClient MetaPartitionOffline failed: mpID(%v)vol(%v) addr(%v)->dest(%v) err(%v)", mpID, volName, addr, "", err)
		return err
	}
	log.LogInfof("ReleaseClient MetaPartitionOffline success: msg(%s)", string(data))
	return nil
}

func (c *ReleaseClient) DataPartitionOffline(addr, volName string, dpID uint64) error {
	req := cutil.NewAPIRequest(http.MethodPost, fmt.Sprintf("http://%s%s", c.domain, AdminDataPartitionOffline))
	req.AddParam("addr", addr)
	req.AddParam("name", volName)
	req.AddParam("id", strconv.FormatUint(dpID, 10))
	req.AddParam("destAddr", "")

	data, err := cutil.SendSimpleRequest(req, true)
	if err != nil {
		log.LogErrorf("ReleaseClient DataPartitionOffline failed: dpID(%v)vol(%v) addr(%v)->dest(%v) err(%v)", dpID, volName, addr, "", err)
		return err
	}
	log.LogInfof("ReleaseClient DataPartitionOffline success: msg(%s)", string(data))
	return nil
}

func (c *ReleaseClient) LoadDataPartition(dpID uint64, volName string) error {
	req := cutil.NewAPIRequest(http.MethodPost, fmt.Sprintf("http://%s%s", c.domain, AdminLoadDataPartition))
	req.AddParam("id", strconv.FormatUint(dpID, 10))
	req.AddParam("name", volName)

	data, err := cutil.SendSimpleRequest(req, true)
	if err != nil {
		log.LogErrorf("ReleaseClient LoadDataPartition failed: dpID(%v)vol(%v) err(%v)", dpID, volName, err)
		return err
	}
	log.LogInfof("ReleaseClient LoadDataPartition success: msg(%s)", string(data))
	return nil
}

func (c *ReleaseClient) LoadMetaPartition(mpID uint64, volName string) error {
	req := cutil.NewAPIRequest(http.MethodPost, fmt.Sprintf("http://%s%s", c.domain, AdminLoadMetaPartition))
	req.AddParam("id", strconv.FormatUint(mpID, 10))
	req.AddParam("name", volName)

	data, err := cutil.SendSimpleRequest(req, true)
	if err != nil {
		log.LogErrorf("ReleaseClient LoadMetaPartition failed: mpID(%v)vol(%v) err(%v)", mpID, volName, err)
		return err
	}
	log.LogInfof("ReleaseClient LoadMetaPartition success: msg(%s)", string(data))
	return nil
}

func (c *ReleaseClient) AddMetaReplica(mpID uint64, addr string) error {
	req := cutil.NewAPIRequest(http.MethodPost, fmt.Sprintf("http://%s%s", c.domain, AdminAddMetaReplica))
	req.AddParam("id", strconv.FormatUint(mpID, 10))
	req.AddParam("addr", addr)

	data, err := cutil.SendSimpleRequest(req, true)
	if err != nil {
		log.LogErrorf("ReleaseClient AddMetaReplica failed: mpID(%v)addr(%v) err(%v)", mpID, addr, err)
		return err
	}
	log.LogInfof("ReleaseClient AddMetaReplica success: msg(%s)", string(data))
	return nil
}

func (c *ReleaseClient) DeleteMetaReplica(mpID uint64, addr string) error {
	req := cutil.NewAPIRequest(http.MethodPost, fmt.Sprintf("http://%s%s", c.domain, AdminDeleteMetaReplica))
	req.AddParam("id", strconv.FormatUint(mpID, 10))
	req.AddParam("addr", addr)

	data, err := cutil.SendSimpleRequest(req, true)
	if err != nil {
		log.LogErrorf("ReleaseClient DeleteMetaReplica failed: mpID(%v)addr(%v) err(%v)", mpID, addr, err)
		return err
	}
	log.LogInfof("ReleaseClient DeleteMetaReplica success: msg(%s)", string(data))
	return nil
}

func (c *ReleaseClient) SetMetaNodeThreshold(threshold float64) error {
	req := cutil.NewAPIRequest(http.MethodPost, fmt.Sprintf("http://%s%s", c.domain, AdminSetMetaNodeThreshold))
	req.AddParam("threshold", strconv.FormatFloat(threshold, 'f', 5, 64))

	data, err := cutil.SendSimpleRequest(req, true)
	if err != nil {
		log.LogErrorf("ReleaseClient SetMetaNodeThreshold failed: thresh(%v) err(%v)", threshold, err)
		return err
	}
	log.LogInfof("ReleaseClient SetMetaNodeThreshold success: msg(%s)", string(data))
	return nil
}

func (c *ReleaseClient) SetReadDirLimit(limit uint64) error {
	req := cutil.NewAPIRequest(http.MethodPost, fmt.Sprintf("http://%s%s", c.domain, AdminSetNodeInfo))
	req.AddParam("readDirLimit", strconv.FormatUint(limit, 10))

	data, err := cutil.SendSimpleRequest(req, true)
	if err != nil {
		log.LogErrorf("ReleaseClient SetReadDirLimit failed: limit(%v) err(%v)", limit, err)
		return err
	}
	log.LogInfof("ReleaseClient SetReadDirLimit success: msg(%s)", string(data))
	return nil
}

func (c *ReleaseClient) SetClusterFreeze(enableFreeze bool) error {
	req := cutil.NewAPIRequest(http.MethodPost, fmt.Sprintf("http://%s%s", c.domain, AdminClusterFreeze))
	req.AddParam("enable", strconv.FormatBool(enableFreeze))

	data, err := cutil.SendSimpleRequest(req, true)
	if err != nil {
		log.LogErrorf("ReleaseClient SetClusterFreeze failed: freeze(%v) err(%v)", enableFreeze, err)
		return err
	}
	log.LogInfof("ReleaseClient SetClusterFreeze success: msg(%s)", string(data))
	return nil
}

func (c *ReleaseClient) SetBandwidthLimiter(limit uint64) error {
	req := cutil.NewAPIRequest(http.MethodPost, fmt.Sprintf("http://%s%s", c.domain, BandwidthLimiterSet))
	req.AddParam("bw", strconv.FormatUint(limit, 10))

	data, err := cutil.SendSimpleRequest(req, true)
	if err != nil {
		log.LogErrorf("ReleaseClient SetBandwidthLimiter failed: limit(%v) err(%v)", limit, err)
		return err
	}
	log.LogInfof("ReleaseClient SetBandwidthLimiter success: msg(%s)", string(data))
	return nil
}

func (c *ReleaseClient) GetAPIReqBwRateLimitInfo() (map[uint8]int64, error) {
	req := cutil.NewAPIRequest(http.MethodPost, fmt.Sprintf("http://%s%s", c.domain, GetAPIReqBwRateLimitInfo))

	data, err := cutil.SendSimpleRequest(req, false)
	if err != nil {
		log.LogErrorf("ReleaseClient getAPIReqBWRateLimit failed: err(%v)", err)
		return nil, err
	}
	limitInfo := new(cproto.LimitInfo)
	if err = json.Unmarshal(data, limitInfo); err != nil {
		log.LogErrorf("ReleaseClient json Unmarshal limitInfo failed: err: %s", err.Error())
		return nil, err
	}
	return limitInfo.ApiReqBwRateLimitMap, nil
}

func (c *ReleaseClient) ClientDataPartitions(volume string) ([]*cproto.DataPartitionsView, error) {
	req := cutil.NewAPIRequest(http.MethodGet, fmt.Sprintf("http://%s%s", c.domain, ClientDataPartition))
	req.AddParam("name", volume)

	data, err := cutil.SendSimpleRequest(req, false)
	if err != nil {
		log.LogErrorf("ReleaseClient ClientDataPartitions failed: err(%v)", err)
		return nil, err
	}
	view := &cproto.DataPartitionsResp{}
	if err = json.Unmarshal(data, view); err != nil {
		log.LogErrorf("ReleaseClient ClientDataPartitions failed: err(%v)")
		return nil, err
	}
	return view.DataPartitions, nil
}

func (c *ReleaseClient) ClientMetaPartition(volume string) ([]*cproto.MetaPartitionView, error) {
	req := cutil.NewAPIRequest(http.MethodGet, fmt.Sprintf("http://%s%s", c.domain, ClientMetaPartition))
	req.AddParam("name", volume)

	data, err := cutil.SendSimpleRequest(req, false)
	if err != nil {
		log.LogErrorf("ReleaseClient ClientMetaPartition failed: err(%v)", err)
		return nil, err
	}
	view := make([]*cproto.MetaPartitionView, 0)
	if err = json.Unmarshal(data, &view); err != nil {
		log.LogErrorf("ReleaseClient ClientMetaPartition failed: err(%v)")
		return nil, err
	}
	return view, nil
}
