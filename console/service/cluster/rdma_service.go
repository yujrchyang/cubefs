package cluster

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/console/cutil"
	cproto "github.com/cubefs/cubefs/console/proto"
	api "github.com/cubefs/cubefs/console/service/apiManager"
	"github.com/cubefs/cubefs/util/log"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type RDMAService struct {
	api *api.APIManager
}

func NewRDMAService(api *api.APIManager) *RDMAService {
	return &RDMAService{
		api: api,
	}
}

func (rdma *RDMAService) GetClusterRDMAConf(cluster string) (*cproto.ClusterRDMAConfView, error) {
	clusterInfo := rdma.api.GetClusterInfo(cluster)
	clusterRDMAConf, err := GetClusterRDMAConf(cluster, clusterInfo.MasterDomain, "dataNode")
	if err != nil {
		return nil, err
	}
	view := &cproto.ClusterRDMAConfView{
		ClusterRDMAEnable: clusterRDMAConf.ClusterRDMA,
		ClusterRDMASend:   clusterRDMAConf.ClusterRDMASend,
		ReConnDelayTime:   clusterRDMAConf.RDMAReConnDelayTime,
	}
	return view, nil
}

func (rdma *RDMAService) GetRDMANodeList(cluster, nodeType string) ([]*cproto.RDMANodeView, error) {
	clusterInfo := rdma.api.GetClusterInfo(cluster)
	clusterRDMAConf, err := GetClusterRDMAConf(cluster, clusterInfo.MasterDomain, nodeType)
	if err != nil {
		return nil, err
	}

	nodeViewList := make([]*cproto.RDMANodeView, 0)
	for _, nodeInfo := range clusterRDMAConf.RDMANodeMap {
		var vendor string
		if nodeInfo.RDMAConf != nil && len(nodeInfo.RDMAConf.Vendor) >= 1 {
			vendor = nodeInfo.RDMAConf.Vendor[0]
		}
		nodeViewList = append(nodeViewList, &cproto.RDMANodeView{
			ID:              nodeInfo.ID,
			Addr:            nodeInfo.Addr,
			Pod:             nodeInfo.Pod,
			IsBond:          nodeInfo.IsBond,
			Vendor:          vendor,
			NodeRDMAService: nodeInfo.RDMAService,
			NodeRDMASend:    nodeInfo.RDMASend,
			NodeRDMARecv:    nodeInfo.RDMARecv,
			ReportTime:      nodeInfo.ReportTime.Format(time.DateTime),
		})
	}
	return nodeViewList, nil
}

func (rdma *RDMAService) GetRDMAVolumeList(cluster string) (*cproto.RDMAVolumeListResponse, error) {
	mc := rdma.api.GetMasterClient(cluster)
	if mc == nil {
		return nil, cproto.ErrUnSupportOperation
	}
	volInfos, err := mc.AdminAPI().ListVols("")
	if err != nil {
		log.LogErrorf("GetRDMAVolumeList: getVolList failed, cluster(%v) err(%v)", cluster, err)
		return nil, err
	}
	rdmaVols := make([]*cproto.RDMAVolumeView, 0)
	for _, volInfo := range volInfos {
		//if volInfo.RDMA {
		rdmaVols = append(rdmaVols, &cproto.RDMAVolumeView{
			Volume: volInfo.Name,
			//EnableRDMA: volInfo.RDMA,
		})
		//}
	}
	return &cproto.RDMAVolumeListResponse{
		Total: len(rdmaVols),
		Data:  rdmaVols,
	}, nil
}

func (rdma *RDMAService) GetRDMANodeView(cluster, node string) (*cproto.RDMANodeStatusView, error) {
	clusterInfo := rdma.api.GetClusterInfo(cluster)
	dhost := fmt.Sprintf("%s:%s", strings.Split(node, ":")[0], clusterInfo.DataProf)
	nodeRDMAStatus, err := getNodeRDMAStatus(dhost)
	if err != nil {
		return nil, err
	}
	nodeStats, err := getNodeStats(dhost)
	if err != nil {
		return nil, err
	}
	if nodeStats.RDMAConf == nil {
		nodeStats.RDMAConf = new(cproto.RDMAConf)
	}
	rdmaStatusView := &cproto.RDMANodeStatusView{
		NodeRDMAStatus: &cproto.NodeRDMAStatusView{
			Pod:                  nodeRDMAStatus.Pod,
			ClusterRDMAEnable:    nodeRDMAStatus.ClusterRDMA,
			ClusterRDMASend:      nodeRDMAStatus.ClusterRDMASend,
			NodeRDMAService:      nodeRDMAStatus.RDMAService,
			NodeRDMASend:         nodeRDMAStatus.RDMASend,
			NodeRDMARecv:         nodeRDMAStatus.RDMARecv,
			EnableSend:           nodeRDMAStatus.EnableSend,
			NodeCount:            nodeRDMAStatus.NodeCount,
			PermanentClosedCount: nodeRDMAStatus.PermanentClosedCount,
		},
		// RDMAConf 不能为nil
		NodeRDMAConf: &cproto.NodeRDMAConfView{
			FWVersion:     nodeStats.RDMAConf.FWVersion,
			DriverVersion: nodeStats.RDMAConf.DriverVersion,
			Vendor:        strings.Join(nodeStats.RDMAConf.Vendor, ","),
			SlaveName:     strings.Join(nodeStats.RDMAConf.SlaveName, ","),
		},
		ConnNodes: make([]*cproto.ConnNodeInfo, 0),
	}
	for addr, connNode := range nodeRDMAStatus.RdmaNodeAddrMap {
		connNode.Addr = addr
		rdmaStatusView.ConnNodes = append(rdmaStatusView.ConnNodes, connNode)
	}
	return rdmaStatusView, nil
}

func (rdma *RDMAService) SetClusterRDMAConf(cluster string, clusterRDMAEnable, clusterSendEnable bool, reConnIntervalSec int64) error {
	clusterInfo := rdma.api.GetClusterInfo(cluster)
	req := cutil.NewAPIRequest(http.MethodGet, fmt.Sprintf("http://%s%s", clusterInfo.MasterDomain, cproto.AdminSetRDMAConf))
	req.AddParam("setType", "cluster")
	req.AddParam("rdma", strconv.FormatBool(clusterRDMAEnable))
	req.AddParam("rdmaSend", strconv.FormatBool(clusterSendEnable))
	req.AddParam("rdmaReConnDelayTime", strconv.FormatInt(reConnIntervalSec, 10))

	data, err := cutil.SendSimpleRequest(req, false)
	if err != nil {
		return err
	}
	log.LogInfof("SetClusterRDMAConf success: cluster(%v) msg(%v)", cluster, string(data))
	return nil
}

func (rdma *RDMAService) SetNodeRDMAConf(cluster, addr, pod string, nodeEnable, nodeSendEnable, nodeRecvEnable bool, nodeType string) error {
	clusterInfo := rdma.api.GetClusterInfo(cluster)
	req := cutil.NewAPIRequest(http.MethodGet, fmt.Sprintf("http://%s%s", clusterInfo.MasterDomain, cproto.AdminSetRDMAConf))
	req.AddParam("setType", nodeType)
	// todo: 端口号是否会影响node存在校验
	req.AddParam("addr", addr)
	req.AddParam("podName", pod)
	req.AddParam("rdmaService", strconv.FormatBool(nodeEnable))
	req.AddParam("rdmaSend", strconv.FormatBool(nodeSendEnable))
	req.AddParam("rdmaRecv", strconv.FormatBool(nodeRecvEnable))

	data, err := cutil.SendSimpleRequest(req, false)
	if err != nil {
		return err
	}
	log.LogInfof("SetNodeRDMAConf success: cluster(%v) msg(%v)", cluster, string(data))
	return nil
}

func (rdma *RDMAService) BatchSetVolumeRDMAConf(cluster string, volList []string, enableRDMA bool) error {
	clusterInfo := rdma.api.GetClusterInfo(cluster)
	var errResult error
	for _, vol := range volList {
		err := setVolumeRDMAConf(cluster, clusterInfo.MasterDomain, vol, enableRDMA)
		if err != nil {
			// 最新的err 放在前面
			errResult = fmt.Errorf("%v, %v", err, errResult)
		}
	}
	return errResult
}

func (rdma *RDMAService) SetNodeSendEnable(cluster, node string, sendEnable bool, reason string) error {
	clusterInfo := rdma.api.GetClusterInfo(cluster)
	dhost := fmt.Sprintf("%s:%s", strings.Split(node, ":")[0], clusterInfo.DataProf)

	req := cutil.NewAPIRequest(http.MethodGet, fmt.Sprintf("http://%s%s", dhost, cproto.NodeSetRDMASendEnable))
	req.AddParam("enable", strconv.FormatBool(sendEnable))
	req.AddParam("reason", reason)

	data, err := cutil.SendSimpleRequest(req, false)
	if err != nil {
		return err
	}
	log.LogInfof("SetNodeSendEnable success: cluster(%v) node(%v) msg(%v)", cluster, node, string(data))
	return nil
}

func (rdma *RDMAService) BatchSetNodeConnEnable(cluster, host string, nodeList []string, connEnable bool, reason string) error {
	clusterInfo := rdma.api.GetClusterInfo(cluster)
	dhost := fmt.Sprintf("%s:%s", strings.Split(host, ":")[0], clusterInfo.DataProf)

	var errResult error
	for _, node := range nodeList {
		err := setNodeConnEnable(dhost, node, connEnable, reason)
		if err != nil {
			// 最新的err 放在前面
			errResult = fmt.Errorf("%v, %v", err, errResult)
		}
	}
	return errResult
}

func setNodeConnEnable(host, nodeAddr string, connEnable bool, reason string) error {
	req := cutil.NewAPIRequest(http.MethodGet, fmt.Sprintf("http://%s%s", host, cproto.NodeSetRDMAConnEnable))
	req.AddParam("enable", strconv.FormatBool(connEnable))
	req.AddParam("peerIP", nodeAddr)
	req.AddParam("reason", reason)

	data, err := cutil.SendSimpleRequest(req, false)
	if err != nil {
		return err
	}
	log.LogInfof("setNodeConnEnable success: node(%v) peer(%v) msg(%v)", host, nodeAddr, string(data))
	return nil
}

func setVolumeRDMAConf(cluster, masterAddr, volume string, enableRDMA bool) error {
	req := cutil.NewAPIRequest(http.MethodGet, fmt.Sprintf("http://%s%s", masterAddr, cproto.AdminSetRDMAConf))
	req.AddParam("setType", "vol")
	req.AddParam("volume", volume)
	req.AddParam("rdma", strconv.FormatBool(enableRDMA))

	data, err := cutil.SendSimpleRequest(req, false)
	if err != nil {
		return err
	}
	log.LogInfof("setVolumeRDMAConf success: cluster(%v) msg(%v)", cluster, string(data))
	return nil
}

func GetClusterRDMAConf(cluster, masterAddr, nodeType string) (clusterRDMAConfInfo *cproto.RDMAConfInfo, err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("getClusterRDMAConf failed: cluster(%s) err(%v)", cluster, err)
		}
	}()
	req := cutil.NewAPIRequest(http.MethodGet, fmt.Sprintf("http://%s%s", masterAddr, cproto.AdminRDMAConf))
	req.AddParam("nodeType", nodeType)

	data, err := cutil.SendSimpleRequest(req, false)
	if err != nil {
		return
	}
	clusterRDMAConfInfo = new(cproto.RDMAConfInfo)
	err = json.Unmarshal(data, &clusterRDMAConfInfo)
	return
}

func getNodeRDMAStatus(node string) (*cproto.NodeRDMAStatus, error) {
	req := cutil.NewAPIRequest(http.MethodGet, fmt.Sprintf("http://%s%s", node, cproto.NodeGetRDMAStatus))
	data, err := cutil.SendSimpleRequest(req, false)
	if err != nil {
		return nil, err
	}
	rdmaStatus := new(cproto.NodeRDMAStatus)
	err = json.Unmarshal(data, &rdmaStatus)
	return rdmaStatus, err
}

func getNodeStats(node string) (*cproto.NodeStats, error) {
	req := cutil.NewAPIRequest(http.MethodGet, fmt.Sprintf("http://%s%s", node, cproto.NodeGetStats))
	data, err := cutil.SendSimpleRequest(req, false)
	if err != nil {
		return nil, err
	}
	nodeStats := new(cproto.NodeStats)
	err = json.Unmarshal(data, &nodeStats)
	return nodeStats, err
}
