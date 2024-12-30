package traffic

import (
	"github.com/cubefs/cubefs/console/model"
	api "github.com/cubefs/cubefs/console/service/apiManager"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util/log"
	"sync"
	"time"
)

func CollectHostUsedInfo() {
	// 统计集群中 所有节点的 使用量信息 1小时一次
	var (
		recordCh = make(chan []*model.ClusterHostInfo, 10)
		wg       = new(sync.WaitGroup)
	)
	go recordHostInfo(recordCh)

	sdk := api.GetSdkApiManager()
	clusters := sdk.GetConsoleCluster()
	for _, cluster := range clusters {
		wg.Add(1)
		go getHostInfo(wg, recordCh, cluster)
	}
	wg.Wait()
	close(recordCh)

	// clean expire info
	cleanExpiredHostInfos()
}

func getHostInfo(wg *sync.WaitGroup, recordCh chan<- []*model.ClusterHostInfo, cluster *model.ConsoleCluster) {
	defer wg.Done()
	if cluster.IsRelease {

	} else {
		handleNodeUsageRatio(cluster.ClusterName, recordCh)
	}
}

func handleNodeUsageRatio(cluster string, recordCh chan<- []*model.ClusterHostInfo) {
	sdk := api.GetSdkApiManager()
	mc := sdk.GetMasterClient(cluster)
	topology, err := mc.AdminAPI().GetTopology()
	if err != nil {
		log.LogErrorf("getHostInfo: getClusterTopology failed: cluster(%v) err(%v)", cluster, err)
		return
	}
	datanodes := make(map[string][]string)
	metanodes := make(map[string][]string)
	for _, zone := range topology.Zones {
		zoneDatanodes := make([]string, 0)
		zoneMetanodes := make([]string, 0)
		for _, nodeSet := range zone.NodeSet {
			for _, datanode := range nodeSet.DataNodes {
				zoneDatanodes = append(zoneDatanodes, datanode.Addr)
			}
			for _, metanode := range nodeSet.MetaNodes {
				zoneMetanodes = append(zoneMetanodes, metanode.Addr)
			}
		}
		datanodes[zone.Name] = zoneDatanodes
		metanodes[zone.Name] = zoneMetanodes
	}
	wg := new(sync.WaitGroup)
	wg.Add(2)
	// datanode
	go getDataNodeUsageInfo(wg, mc, cluster, datanodes, recordCh)
	// todo: metanode & flashnode

	wg.Wait()
	return
}

func getDataNodeUsageInfo(fi *sync.WaitGroup, mc *master.MasterClient, cluster string, zoneNode map[string][]string, recordCh chan<- []*model.ClusterHostInfo) {
	wg := new(sync.WaitGroup)
	for zoneName, nodeList := range zoneNode {
		zone := zoneName
		nodes := nodeList
		wg.Add(1)
		go func(ch chan<- []*model.ClusterHostInfo) {
			defer wg.Done()
			records := batchGetDatanode(mc, cluster, zone, nodes)
			ch <- records
		}(recordCh)
	}
	wg.Wait()
	fi.Done()
}

func batchGetDatanode(mc *master.MasterClient, cluster, zone string, nodeList []string) []*model.ClusterHostInfo {
	var (
		dataNodeInfo = make([]*model.ClusterHostInfo, 0)
		wg           sync.WaitGroup
		mu           sync.Mutex
		ch           = make(chan struct{}, 10) // 10并发
		update       = time.Now()
	)
	for _, node := range nodeList {
		wg.Add(1)
		ch <- struct{}{}
		go func(addr string) {
			defer func() {
				<-ch
				wg.Done()
			}()
			nodeInfo, err := mc.NodeAPI().GetDataNode(addr)
			if err != nil {
				log.LogErrorf("batchGetDatanode failed: zone(%v) addr(%v) err(%v)", zone, addr, err)
				return
			}
			record := &model.ClusterHostInfo{
				ClusterName: cluster,
				ZoneName:    zone,
				Capacity:    formatByteToGB(nodeInfo.Total),
				Used:        formatByteToGB(nodeInfo.Used),
				UsedRatio:   float64(nodeInfo.Used) / float64(nodeInfo.Total),
				HostAddr:    addr,
				ModuleType:  model.ModuleTypeDataNode,
				DpNum:       nodeInfo.DataPartitionCount,
				UpdateTime:  update,
			}
			mu.Lock()
			dataNodeInfo = append(dataNodeInfo, record)
			mu.Unlock()
		}(node)
	}
	return dataNodeInfo
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

func cleanExpiredHostInfos() {
	timeStr := time.Now().AddDate(0, 0, -1).Format(time.DateTime)
	err := model.CleanExpiredHosts(timeStr)
	if err != nil {
		log.LogWarnf("cleanExpiredHostInfos failed: %v", err)
	}
}
