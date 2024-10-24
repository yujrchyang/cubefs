package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/cubefs/cubefs/schedulenode/checktool/cfs"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util/log"
	"io/ioutil"
	"net/http"
	"os"
	"time"
)

var masterAddr = flag.String("master", "", "master address")
var concurrency = flag.Int("concurrency", 30, "concurrency of maximum offline partitions")
var disk = flag.String("disk", "", "if disk set, only offline this disk, if disk is bad")
var node = flag.String("node", "", "if node set, only offline bad disk on this node")

const (
	maxBadDataPartitionsCount = 200
)

func main() {
	flag.Parse()
	if *masterAddr != "cn.chubaofs.jd.local" && *masterAddr != "cn.elasticdb.jd.local" && *masterAddr != "sparkchubaofs.jd.local" {
		fmt.Printf("unsupported master address: %v", *masterAddr)
		return
	}
	if *concurrency > maxBadDataPartitionsCount {
		fmt.Printf("concurrency[%v] no more than[%v]", *concurrency, maxBadDataPartitionsCount)
		return
	}
	if *concurrency == 0 {
		fmt.Println("concurrency can not be 0")
		return
	}
	fmt.Printf("offline bad disk on master:%v\n", *masterAddr)
	client := master.NewMasterClient([]string{*masterAddr}, false)
	log.InitLog("/tmp", "disk_offline", log.DebugLevel, nil)
	defer func() {
		log.LogFlush()
	}()
	for {
		offlineBadDisk(client, *concurrency)
		time.Sleep(time.Minute * 8)
	}
}

func offlineBadDisk(client *master.MasterClient, concurrency int) {
	unavailDps, err := client.AdminAPI().GetUnavailablePartitions()
	if err != nil {
		log.LogErrorf("get unavailable dp failed, err:%v", err)
		return
	}
	if len(unavailDps) == 0 {
		fmt.Printf("master:%v no bad partitions found, disk[%v] node[%v]", client.Nodes()[0], *disk, *node)
		os.Exit(0)
	}
	msg := fmt.Sprintf("master[%v] unavailable dps[%v]", client.Nodes()[0], len(unavailDps))
	fmt.Println(msg)
	log.LogWarnf(msg)
	var badDPsCount int
	cv, err := getCluster(*masterAddr)
	if err != nil {
		return
	}
	for _, badPartitionView := range cv.BadPartitionIDs {
		badDPsCount += len(badPartitionView.PartitionIDs)
		if badPartitionView.PartitionID != 0 {
			badDPsCount++
		}
	}
	for _, migratedDataPartition := range cv.MigratedDataPartitions {
		badDPsCount += len(migratedDataPartition.PartitionIDs)
		if migratedDataPartition.PartitionID != 0 {
			badDPsCount++
		}
	}

	if badDPsCount >= concurrency {
		log.LogWarn(fmt.Sprintf("action[offlineDataPartition] host:%v badDPsCount:%v more than maxBadDataPartitionsCount:%v ",
			client.Nodes()[0], badDPsCount, concurrency))
		return
	}
	maxOfflineCount := concurrency - badDPsCount

	for dpID, badReplicas := range unavailDps {
		for addr, badDisk := range badReplicas {
			if (*disk != "" && badDisk != *disk) || (*node != "" && addr != *node) {
				log.LogWarnf("host[%v] dp[%v] addr[%v] diskPath[%v] no need offline, because only offline disk[%v] node[%v]", client.Nodes()[0], dpID, addr, badDisk, *disk)
				continue
			}
			if maxOfflineCount <= 0 {
				return
			}
			err = client.AdminAPI().DecommissionDataPartition(dpID, addr, "")
			if err != nil {
				log.LogErrorf("decommission failed, host[%v] dp[%v] addr[%v] badDisk[%v]", client.Nodes()[0], dpID, addr, badDisk)
			} else {
				log.LogWarnf("decommission success, host[%v] dp[%v] addr[%v] badDisk[%v]", client.Nodes()[0], dpID, addr, badDisk)
			}
			// 防御一下
			time.Sleep(time.Second * 2)
			maxOfflineCount--
		}
	}
}

func getCluster(host string) (cv *cfs.ClusterView, err error) {
	reqURL := fmt.Sprintf("http://%v/admin/getCluster", host)
	data, err := doRequest(reqURL, false)
	if err != nil {
		return
	}
	cv = &cfs.ClusterView{}
	if err = json.Unmarshal(data, cv); err != nil {
		log.LogErrorf("get cluster from %v failed ,data:%v,err:%v", host, string(data), err)
		return
	}
	cv.DataNodeStat = cv.DataNodeStatInfo
	cv.MetaNodeStat = cv.MetaNodeStatInfo
	log.LogInfof("action[getCluster],host[%v],len(VolStat)=%v,len(metaNodes)=%v,len(dataNodes)=%v",
		host, len(cv.VolStat), len(cv.MetaNodes), len(cv.DataNodes))
	return
}

func doRequest(reqUrl string, isReleaseCluster bool) (data []byte, err error) {
	var resp *http.Response
	client := http.Client{Timeout: time.Minute * 5}
	req, err := http.NewRequest(http.MethodGet, reqUrl, nil)
	if err != nil {
		log.LogErrorf("action[doRequest] reqRUL[%v] new request occurred err:%v\n", reqUrl, err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Connection", "close")
	if resp, err = client.Do(req); err != nil {
		log.LogErrorf("action[doRequest] reqRUL[%v] err:%v\n", reqUrl, err)
		return
	}
	defer resp.Body.Close()

	if data, err = ioutil.ReadAll(resp.Body); err != nil {
		log.LogErrorf("action[doRequest] reqRUL[%v] remoteAddr:%v,err:%v\n", reqUrl, resp.Request.RemoteAddr, err)
		if len(data) != 0 {
			log.LogErrorf("action[doRequest] ioutil.ReadAll data:%v", string(data))
		}
		return
	}
	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("action[doRequest] reqRUL[%v],statusCode[%v],body[%v]", reqUrl, resp.StatusCode, string(data))
		return
	}
	if !isReleaseCluster {
		reply := HTTPReply{}
		if err = json.Unmarshal(data, &reply); err != nil {
			log.LogErrorf("action[doRequest] reqRUL[%v] err:%v\n", reqUrl, err)
			return
		}
		data = reply.Data
		if len(data) <= 4 && string(data) == "null" && len(reply.Msg) != 0 {
			data = []byte(reply.Msg)
		}
	}
	return
}

// HTTPReply uniform response structure
type HTTPReply struct {
	Code int32           `json:"code"`
	Msg  string          `json:"msg"`
	Data json.RawMessage `json:"data"`
}
