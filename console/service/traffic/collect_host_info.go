package traffic

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/console/cutil"
	"github.com/cubefs/cubefs/console/model"
	cproto "github.com/cubefs/cubefs/console/proto"
	api "github.com/cubefs/cubefs/console/service/apiManager"
	"github.com/cubefs/cubefs/sdk/http_client"
	"github.com/cubefs/cubefs/util/log"
)

const (
	hostInfoKeepDay    = 7
	getHostConcurrency = 30
)

type getClusterFunc func(cluster string) *model.ConsoleCluster

var (
	getCluster getClusterFunc
)

func CollectHostUsedInfo() {
	var (
		recordCh = make(chan []*model.ClusterHostInfo, 10)
		wg       = new(sync.WaitGroup)
	)
	go recordHostInfo(recordCh)

	sdk := api.GetSdkApiManager()
	getCluster = sdk.GetClusterInfo

	for _, cluster := range sdk.GetConsoleCluster() {
		wg.Add(1)
		go getHostInfo(wg, recordCh, cluster.ClusterName)
	}
	wg.Wait()
	close(recordCh)
}

func getHostInfo(wg *sync.WaitGroup, recordCh chan<- []*model.ClusterHostInfo, cluster string) {
	defer func() {
		wg.Done()
		// 10分钟一次 不打info日志
		cleanExpiredHostInfos(cluster)
	}()
	handleNodeUsageRatio(cluster, recordCh)
}

func handleNodeUsageRatio(cluster string, recordCh chan<- []*model.ClusterHostInfo) {
	sdk := api.GetSdkApiManager()
	datanodes := make(map[string][]string)
	metanodes := make(map[string][]string)
	if cproto.IsRelease(cluster) {
		rc := sdk.GetReleaseClient(cluster)
		clusterView, err := rc.GetClusterView()
		if err != nil {
			log.LogErrorf("handleNodeUsageRatio: getClusterView failed: cluster(%v) err(%v)", clusterView, err)
			return
		}
		zoneDatanodes := make([]string, 0)
		zoneMetanodes := make([]string, 0)
		for _, datanode := range clusterView.DataNodes {
			zoneDatanodes = append(zoneDatanodes, datanode.Addr)
		}
		for _, metanode := range clusterView.MetaNodes {
			zoneMetanodes = append(zoneMetanodes, metanode.Addr)
		}
		datanodes["all"] = zoneDatanodes
		metanodes["all"] = zoneMetanodes
	} else {
		mc := sdk.GetMasterClient(cluster)
		topology, err := mc.AdminAPI().GetTopology()
		if err != nil {
			log.LogErrorf("handleNodeUsageRatio: getClusterTopology failed: cluster(%v) err(%v)", cluster, err)
			return
		}
		for _, zone := range topology.Zones {
			zoneDataNodes := make([]string, 0)
			zoneMetaNodes := make([]string, 0)
			for _, nodeSet := range zone.NodeSet {
				for _, datanode := range nodeSet.DataNodes {
					zoneDataNodes = append(zoneDataNodes, datanode.Addr)
				}
				for _, metanode := range nodeSet.MetaNodes {
					zoneMetaNodes = append(zoneMetaNodes, metanode.Addr)
				}
			}
			datanodes[zone.Name] = zoneDataNodes
			metanodes[zone.Name] = zoneMetaNodes
		}
	}

	wg := new(sync.WaitGroup)
	wg.Add(2)

	go getNodeUsageInfo(wg, cluster, "data", datanodes, recordCh)
	go getNodeUsageInfo(wg, cluster, "meta", metanodes, recordCh)

	wg.Wait()
	return
}

func getNodeUsageInfo(fi *sync.WaitGroup, cluster, module string, zoneNode map[string][]string, recordCh chan<- []*model.ClusterHostInfo) {
	wg := new(sync.WaitGroup)
	for zoneName, nodeList := range zoneNode {
		zone := zoneName
		nodes := nodeList
		wg.Add(1)
		go func(ch chan<- []*model.ClusterHostInfo) {
			defer wg.Done()
			records := batchGetNode(cluster, zone, nodes, module)
			log.LogDebugf("test-append to chan: cluster(%v) module(%v) zone(%v) len(host)=%v len(records)=%v", cluster, module, zone, len(nodes), len(records))
			ch <- records
		}(recordCh)
	}
	wg.Wait()
	fi.Done()
}

func batchGetNode(cluster, zone string, nodeList []string, module string) []*model.ClusterHostInfo {
	var (
		nodeInfos = make([]*model.ClusterHostInfo, 0)
		wg        sync.WaitGroup
		mu        sync.Mutex
		ch        = make(chan struct{}, getHostConcurrency) // 10并发
	)
	for _, node := range nodeList {
		wg.Add(1)
		ch <- struct{}{}
		go func(addr string) {
			defer func() {
				<-ch
				wg.Done()
			}()
			var hostInfo *model.ClusterHostInfo
			var err error
			switch module {
			case "data":
				hostInfo, err = getCommonDataNodeInfo(cluster, zone, addr)
				if err != nil {
					log.LogErrorf("batchGetDatanode: zone(%v) addr(%v) err(%v)", zone, addr, err)
					return
				}

			case "meta":
				hostInfo, err = getCommonMetaNodeInfo(cluster, zone, addr)
				if err != nil {
					log.LogErrorf("batchGetDatanode: zone(%v) addr(%v) err(%v)", zone, addr, err)
					return
				}
			}
			mu.Lock()
			nodeInfos = append(nodeInfos, hostInfo)
			mu.Unlock()
		}(node)
	}
	wg.Wait()
	return nodeInfos
}

func recordHostInfo(ch <-chan []*model.ClusterHostInfo) {
	for {
		records, ok := <-ch
		if !ok {
			break
		}
		err := model.StoreHostInfoRecords(records)
		if err != nil {
			log.LogErrorf("recordHostInfo: store batch records failed: err(%v)", err)
		}
	}
	log.LogInfof("host信息记录完成，time: %v", time.Now())
}

func cleanExpiredHostInfos(cluster string) {
	timeStr := time.Now().AddDate(0, 0, -hostInfoKeepDay).Format(time.DateTime)
	model.CleanExpiredHosts(cluster, timeStr)
}

func getCommonDataNodeInfo(cluster, zone, addr string) (hostInfo *model.ClusterHostInfo, err error) {
	host := fmt.Sprintf("%s:%s", strings.Split(addr, ":")[0], getClusterNodeProf(cluster, "data"))
	dClient := http_client.NewDataClient(host, false)
	stats, err := dClient.GetDatanodeStats()
	if err != nil {
		log.LogErrorf("getCommonDataNodeInfo: node(%v) err(%v)", addr, err)
		return nil, err
	}
	if cproto.IsRelease(cluster) {
		stats.PartitionReports = stats.PartitionInfo
	}

	var used uint64
	for _, dpReport := range stats.PartitionReports {
		diskInfo := stats.DiskInfos[dpReport.DiskPath]
		if dpReport.IsSFX && diskInfo.CompressionRatio != 0 {
			used += dpReport.Used * 100 / uint64(diskInfo.CompressionRatio)
		} else {
			used += dpReport.Used
		}
	}
	hostInfo = &model.ClusterHostInfo{
		ClusterName:  cluster,
		ZoneName:     zone,
		Capacity:     formatByteToGB(stats.Total),
		Used:         formatByteToGB(used),
		UsedRatio:    float64(used) / float64(stats.Total),
		Host:         addr,
		ModuleType:   model.ModuleTypeDataNode,
		PartitionCnt: len(stats.PartitionReports),
		UpdateTime:   time.Now(),
	}
	return
}

func getCommonMetaNodeInfo(cluster, zone, addr string) (hostInfo *model.ClusterHostInfo, err error) {
	host := fmt.Sprintf("%s:%s", strings.Split(addr, ":")[0], getClusterNodeProf(cluster, "meta"))
	req := cutil.NewAPIRequest(http.MethodGet, fmt.Sprintf("http://%s%s", host, "/stat/info"))
	req.AddHeader("Accept", "application/json")

	body, err := cutil.SendSimpleRequest(req, false)
	if err != nil {
		return nil, err
	}
	statInfo := new(cproto.MetaNodeStatInfo)
	if err = json.Unmarshal(body, &statInfo); err != nil {
		return nil, err
	}

	hostInfo = &model.ClusterHostInfo{
		ClusterName:  cluster,
		ZoneName:     zone,
		Capacity:     formatByteToGB(statInfo.TotalMem),
		Used:         formatByteToGB(statInfo.UsedMem),
		UsedRatio:    statInfo.Ratio,
		Host:         addr,
		ModuleType:   model.ModuleTypeMetaNode,
		PartitionCnt: statInfo.MetaPartitionCount,
		InodeCount:   statInfo.InodeTotalCount,
		DentryCount:  statInfo.DentryTotalCount,
		UpdateTime:   time.Now(),
	}
	return
}

func getClusterNodeProf(cluster string, module string) string {
	clusterInfo := getCluster(cluster)
	if clusterInfo != nil {
		switch module {
		case "data":
			return clusterInfo.DataProf
		case "meta":
			return clusterInfo.MetaProf
		}
	}
	return ""
}

func GetHostUsageCurve(cluster string, moduleType int, addr string, start, end time.Time) ([]*cproto.ClusterResourceData, error) {
	records := make([]*cproto.ClusterResourceData, 0)
	if err := cutil.CONSOLE_DB.Table(model.ClusterHostInfo{}.TableName()).
		Select("round(unix_timestamp(update_time)+0) as date, capacity as total_gb, used as used_gb, used_ratio, partition_count, inode_count, dentry_count").
		Where("update_time >= ? AND update_time < ?", start, end).
		Where("cluster = ? AND module_type = ? AND addr = ?", cluster, moduleType, addr).
		Group("date").
		Scan(&records).Error; err != nil {
		log.LogErrorf("GetHostUsageCurve: %v %v %v, err: %v", cluster, moduleType, addr, err)
		return nil, err
	}
	for _, record := range records {
		record.UsedRatio = record.UsedRatio * 100
	}
	return records, nil
}
