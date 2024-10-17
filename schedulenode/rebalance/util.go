package rebalance

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/http_client"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util/log"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"
)

func getDataHttpClient(nodeAddr, port string) *http_client.DataClient {
	strs := strings.Split(nodeAddr, ":")
	host := strs[0]
	return http_client.NewDataClient(fmt.Sprintf("%s:%s", host, port), false)
}

func checkRatio(highRatio, lowRatio, goalRatio float64) error {
	if highRatio < lowRatio {
		return ErrWrongRatio
	}
	if goalRatio > highRatio {
		return ErrWrongRatio
	}
	return nil
}

func getLiveReplicas(partition *proto.DataPartitionInfo, timeOutSec int64) (replicas []*proto.DataReplica) {
	replicas = make([]*proto.DataReplica, 0)
	for i := 0; i < len(partition.Replicas); i++ {
		replica := partition.Replicas[i]
		if isReplicaAlive(replica, timeOutSec) && hasHost(partition, replica.Addr) {
			replicas = append(replicas, replica)
		}
	}
	return
}

func isReplicaAlive(replica *proto.DataReplica, timeOutSec int64) (isAvailable bool) {
	if replica.Status != Unavailable && (time.Now().Unix()-replica.ReportTime <= timeOutSec) {
		isAvailable = true
	}
	return
}

func hasHost(partition *proto.DataPartitionInfo, addr string) (ok bool) {
	for _, host := range partition.Hosts {
		if host == addr {
			ok = true
			break
		}
	}
	return
}

func getZoneDataNodesByClusterName(cluster, zoneName string) (zoneDataNodes []string, err error) {
	if isRelease(cluster) {
		return nil, nil
	} else {
		client := master.NewMasterClient([]string{cluster}, false)
		return getZoneDataNodesByClient(client, nil, zoneName)
	}
}

func getZoneDataNodesByClient(mc *master.MasterClient, rc *releaseClient, zoneName string) (zoneDataNodes []string, err error) {
	zoneDataNodes = make([]string, 0)
	if mc != nil {
		topologyView, err := mc.AdminAPI().GetTopology()
		if err != nil {
			return nil, err
		}
		for _, zone := range topologyView.Zones {
			if zone.Name == zoneName {
				for _, nodeSetView := range zone.NodeSet {
					for _, dataNode := range nodeSetView.DataNodes {
						zoneDataNodes = append(zoneDataNodes, dataNode.Addr)
					}
				}
			}
		}
	}
	if rc != nil {
		clusterView, err := rc.AdminGetCluster()
		if err != nil {
			return nil, err
		}
		for _, datanode := range clusterView.DataNodes {
			zoneDataNodes = append(zoneDataNodes, datanode.Addr)
		}
	}
	return
}

func getZoneMetaNodes(mc *master.MasterClient, rc *releaseClient, zone string) (zoneMetaNodes []string, err error) {
	if mc != nil {
		topologyView, err := mc.AdminAPI().GetTopology()
		if err != nil {
			return nil, err
		}
		for _, zoneInfo := range topologyView.Zones {
			if zoneInfo.Name != zone {
				continue
			}
			zoneMetaNodes = make([]string, 0)
			for _, nodeSet := range zoneInfo.NodeSet {
				for _, node := range nodeSet.MetaNodes {
					zoneMetaNodes = append(zoneMetaNodes, node.Addr)
				}
			}
		}
	}
	if rc != nil {
		clusterView, err := rc.AdminGetCluster()
		if err != nil {
			return nil, err
		}
		for _, metaNode := range clusterView.MetaNodes {
			zoneMetaNodes = append(zoneMetaNodes, metaNode.Addr)
		}
	}
	return
}

func getDataPartitionByClient(mc *master.MasterClient, rc *releaseClient, dpID uint64, volName string) (info *proto.DataPartitionInfo, err error) {
	defer func() {
		if err != nil {
			if strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "not exists") {
				err = ErrDataPartitionNotFound
			}
		}
	}()

	if mc != nil {
		info, err = mc.AdminAPI().GetDataPartition(volName, dpID)
		return info, err
	}
	if rc != nil {
		info, err = rc.AdminGetDataPartition(volName, dpID)
		if info != nil {
			info.Hosts = info.DbBackHosts_
		}
		return info, err
	}
	return nil, fmt.Errorf("fatal error")
}

func deleteDataPartitionReplicaByClient(mc *master.MasterClient, rc *releaseClient, dpID uint64, addr string) (err error) {
	if mc != nil {
		err = mc.AdminAPI().DeleteDataReplica(dpID, addr)
	}
	if rc != nil {
		err = rc.DeleteDataReplica(dpID, addr)
	}
	return
}

func getStatusStr(status Status) string {
	switch status {
	case StatusStop:
		return "Stop"
	case StatusRunning:
		return "Running"
	case StatusTerminating:
		return "Terminating"
	default:
		return "None"
	}
}

func (rc *releaseClient) AdminGetCluster() (cv *ClusterView, err error) {
	req := rc.newAPIRequest(http.MethodGet, "/admin/getCluster")
	req.AddParam("ts", strconv.FormatInt(time.Now().Unix(), 10))
	body, err := rc.sendSimpleRequest(req)
	if err != nil {
		return nil, err
	}
	cv = new(ClusterView)
	if err = json.Unmarshal(body, &cv); err != nil {
		return nil, err
	}
	return
}

func (rc *releaseClient) GetVolumeSimpleInfo(volName string) (*proto.SimpleVolView, error) {
	req := rc.newAPIRequest(http.MethodGet, "/admin/getVol")
	req.AddParam("name", volName)
	body, err := rc.sendSimpleRequest(req)
	if err != nil {
		return nil, err
	}
	vv := new(proto.SimpleVolView)
	if err = json.Unmarshal(body, &vv); err != nil {
		return nil, err
	}
	return vv, nil
}

func (rc *releaseClient) AdminGetDataNode(addr string) (*DbBakDataNodeInfo, error) {
	req := rc.newAPIRequest(http.MethodGet, "/dataNode/get")
	req.AddParam("addr", addr)
	body, err := rc.sendSimpleRequest(req)
	if err != nil {
		return nil, err
	}
	nodeInfo := new(DbBakDataNodeInfo)
	if err = json.Unmarshal(body, &nodeInfo); err != nil {
		return nil, err
	}
	return nodeInfo, nil
}

func (rc *releaseClient) AdminGetMetaNode(addr string) (*proto.MetaNodeInfo, error) {
	req := rc.newAPIRequest(http.MethodGet, "/metaNode/get")
	req.AddParam("addr", addr)
	body, err := rc.sendSimpleRequest(req)
	if err != nil {
		return nil, err
	}
	nodeInfo := new(proto.MetaNodeInfo)
	if err = json.Unmarshal(body, &nodeInfo); err != nil {
		return nil, err
	}
	return nodeInfo, nil
}

func (rc *releaseClient) AdminGetDataPartition(volName string, id uint64) (*proto.DataPartitionInfo, error) {
	req := rc.newAPIRequest(http.MethodGet, "/dataPartition/get")
	req.AddParam("id", strconv.FormatUint(id, 10))
	req.AddParam("name", volName)
	data, err := rc.sendSimpleRequest(req)
	if err != nil {
		return nil, err
	}
	partitionInfo := new(proto.DataPartitionInfo)
	if err = json.Unmarshal(data, &partitionInfo); err != nil {
		return nil, err
	}
	return partitionInfo, nil
}

func (rc *releaseClient) GetMetaPartition(volName string, id uint64) (*proto.MetaPartitionInfo, error) {
	req := rc.newAPIRequest(http.MethodGet, "/metaPartition/get")
	req.AddParam("id", strconv.FormatUint(id, 10))
	req.AddParam("name", volName)
	data, err := rc.sendSimpleRequest(req)
	if err != nil {
		return nil, err
	}
	partitionInfo := new(proto.MetaPartitionInfo)
	if err = json.Unmarshal(data, &partitionInfo); err != nil {
		return nil, err
	}
	return partitionInfo, nil
}

func (rc *releaseClient) DataPartitionOffline(id uint64, addr, dest, volName string) error {
	req := rc.newAPIRequest(http.MethodGet, "/dataPartition/offline")
	req.AddParam("addr", addr)
	req.AddParam("dest", dest)
	req.AddParam("id", strconv.FormatUint(id, 10))
	req.AddParam("name", volName)
	_, err := rc.sendSimpleRequest(req)
	if err != nil {
		return err
	}
	return nil
}

func (rc *releaseClient) MetaPartitionOffline(id uint64, addr, dest, volName string) error {
	req := rc.newAPIRequest(http.MethodGet, "/metaPartition/offline")
	req.AddParam("addr", addr)
	req.AddParam("dest", dest)
	req.AddParam("id", strconv.FormatUint(id, 10))
	req.AddParam("name", volName)
	_, err := rc.sendSimpleRequest(req)
	if err != nil {
		return err
	}
	return nil
}

func (rc *releaseClient) AddDataReplica(id uint64, addr string) error {
	req := rc.newAPIRequest(http.MethodGet, "/dataReplica/add")
	req.AddParam("addr", addr)
	req.AddParam("id", strconv.FormatUint(id, 10))
	_, err := rc.sendSimpleRequest(req)
	if err != nil {
		return err
	}
	return nil
}

func (rc *releaseClient) DeleteDataReplica(id uint64, addr string) error {
	req := rc.newAPIRequest(http.MethodGet, "/dataReplica/delete")
	req.AddParam("addr", addr)
	req.AddParam("id", strconv.FormatUint(id, 10))
	_, err := rc.sendSimpleRequest(req)
	if err != nil {
		return err
	}
	return nil
}

type releaseClient struct {
	masters []string
	timeout time.Duration
	cluster string
}

func newReleaseClient(hosts []string, cluster string) *releaseClient {
	return &releaseClient{
		masters: hosts,
		cluster: cluster,
		timeout: 30 * time.Second,
	}
}

func (rc *releaseClient) newAPIRequest(method string, path string) *httpRequest {
	return &httpRequest{
		method: method,
		path:   path,
		params: make(map[string]string),
		header: make(map[string]string),
	}
}

func (rc *releaseClient) sendSimpleRequest(r *httpRequest) (data []byte, err error) {
	client := http.DefaultClient
	client.Timeout = rc.timeout

	url := mergeRequestUrl(fmt.Sprintf("http://%s%s", rc.masters[0], r.path), r.params)
	req, err := http.NewRequest(r.method, url, bytes.NewReader(r.body))
	if err != nil {
		log.LogErrorf("sendSimpleRequest: NewRequest err(%v) url(%v)", err, url)
		return
	}

	for key, value := range r.header {
		req.Header.Set(key, value)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Connection", "keep-alive")

	resp, err := client.Do(req)
	if err != nil {
		log.LogErrorf("sendSimpleRequest: http.DO err(%v) url(%v)", err, url)
		return
	}
	defer resp.Body.Close()
	if data, err = ioutil.ReadAll(resp.Body); err != nil {
		log.LogErrorf("sendSimpleRequest: ioutil.ReadAll err(%v) url(%v) data(%v)", err, url, string(data))
		return
	}
	if resp.StatusCode != http.StatusOK {
		// code不是200, 但err == nil的情况(server自己置的code)
		if err == nil {
			err = fmt.Errorf("code(%v) msg(%v)", resp.StatusCode, string(data))
		}
		log.LogErrorf("sendSimpleRequest: url(%v) err(%v)", url, err)
		return
	}
	if log.IsDebugEnabled() {
		log.LogDebugf("sendSimpleRequest success: url: %v", url)
	}
	return
}

func mergeRequestUrl(url string, params map[string]string) string {
	if params != nil && len(params) > 0 {
		buff := bytes.NewBuffer([]byte(url))
		isFirstParam := true
		for k, v := range params {
			if isFirstParam {
				buff.WriteString("?")
				isFirstParam = false
			} else {
				buff.WriteString("&")
			}
			buff.WriteString(k)
			buff.WriteString("=")
			buff.WriteString(v)
		}
		url = buff.String()
	}
	return url
}

type httpRequest struct {
	method string
	path   string
	params map[string]string
	header map[string]string
	body   []byte
}

func (r *httpRequest) AddParam(key, value string) {
	r.params[key] = value
}

func (r *httpRequest) AddHeader(key, value string) {
	r.header[key] = value
}

func (r *httpRequest) AddBody(body []byte) {
	r.body = body
}

func verifyCluster(cluster string) error {
	switch cluster {
	case SPARK, ELASTICDB, DBBAK, CFS_AMS_MCA, TEST, TestES, TestDbBak:
		return nil
	default:
		return fmt.Errorf("cluster:%v Not supported", cluster)
	}
}

func isRelease(cluster string) bool {
	if cluster == DBBAK || cluster == TestDbBak {
		return true
	}
	return false
}

func isSFXZone(zone string) bool {
	if strings.Contains(zone, "sfx") {
		return true
	}
	return false
}
