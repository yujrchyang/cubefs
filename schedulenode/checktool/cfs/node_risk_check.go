package cfs

import (
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/http_client"
	"github.com/cubefs/cubefs/util/log"
	"sort"
	"strings"
	"sync"
	"time"
)

var riskCheck bool

func (s *ChubaoFSMonitor) scheduleToCheckDataNodeRiskData() {
	if !s.checkRiskFix {
		return
	}
	ticker := time.NewTicker(time.Duration(s.scheduleInterval) * time.Second)
	defer func() {
		ticker.Stop()
	}()
	s.checkDataNodeRiskData()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			if riskCheck {
				continue
			}
			s.checkDataNodeRiskData()
		}
	}
}

func (s *ChubaoFSMonitor) checkDataNodeRiskData() {
	riskCheck = true
	defer func() {
		riskCheck = false
	}()
	wg := sync.WaitGroup{}
	for _, host := range s.hosts {
		if host.isReleaseCluster {
			continue
		}
		wg.Add(1)
		go func(h *ClusterHost) {
			defer wg.Done()
			log.LogInfof("action[checkDataNodeRiskData] start check host:%v", h.host)
			checkDataNodeRiskData(h)
			log.LogInfof("action[checkDataNodeRiskData] finish check host:%v", h.host)
		}(host)
	}
	wg.Wait()
}

func checkDataNodeRiskData(host *ClusterHost) {
	var riskDataNodes []struct {
		FixRunning bool
		Addr       string
		RiskCount  int
	}
	cv, err := getCluster(host)
	if err != nil {
		log.LogErrorf("action[checkDataNodeRiskData] host:%v, err:%v", host.host, err)
		return
	}
	for _, node := range cv.DataNodes {
		if node.Status == false {
			log.LogWarnf("action[checkDataNodeRiskData] host:%v node:%v is inactive, skip check risk", host.host, node.Addr)
			continue
		}
		var partitions *proto.DataPartitions
		dHost := fmt.Sprintf("%v:%v", strings.Split(node.Addr, ":")[0], profPortMap[strings.Split(node.Addr, ":")[1]])
		dataClient := http_client.NewDataClient(dHost, false)
		partitions, err = dataClient.GetPartitionsFromNode()
		if err != nil {
			log.LogErrorf("action[checkDataNodeRiskData] host:%v, dn:%v, err:%v", host.host, node.Addr, err)
			continue
		}
		if partitions.RiskCount == 0 {
			continue
		}
		riskDataNodes = append(riskDataNodes, struct {
			FixRunning bool
			Addr       string
			RiskCount  int
		}{FixRunning: partitions.RiskFixerRunning, Addr: node.Addr, RiskCount: partitions.RiskCount})
		log.LogInfof("action[checkDataNodeRiskData] host:%v node:%v running:%v riskCount:%v", host.host, node.Addr, partitions.RiskFixerRunning, partitions.RiskCount)
	}
	log.LogInfof("action[checkDataNodeRiskData] host:%v total %v risk nodes, and start fix now", host.host, len(riskDataNodes))
	sort.Slice(riskDataNodes, func(i, j int) bool {
		return riskDataNodes[i].FixRunning && !riskDataNodes[j].FixRunning
	})
	for _, riskDataNode := range riskDataNodes {
		var partitions *proto.DataPartitions
		dHost := fmt.Sprintf("%v:%v", strings.Split(riskDataNode.Addr, ":")[0], profPortMap[strings.Split(riskDataNode.Addr, ":")[1]])
		dataClient := http_client.NewDataClient(dHost, false)
		if !riskDataNode.FixRunning {
			err = dataClient.StartRiskFix()
			if err != nil {
				log.LogErrorf("action[checkDataNodeRiskData] host:%v, node:%v, start risk fix err:%v", host.host, riskDataNode.Addr, err)
				continue
			}
		}
		//check risk fix running finish, fix one by one
		var count int
		for {
			count++
			time.Sleep(time.Minute)
			if partitions, err = dataClient.GetPartitionsFromNode(); err != nil {
				log.LogErrorf("action[checkDataNodeRiskData] host:%v, node:%v, GetPartitionsFromNode err:%v", host.host, riskDataNode.Addr, err)
				continue
			}
			log.LogInfof("action[checkDataNodeRiskData] host:%v, node:%v, check count:%v, risk count:%v, running:%v", host.host, riskDataNode.Addr, count, partitions.RiskFixerRunning, partitions.RiskCount)
			if partitions.RiskCount == 0 || !partitions.RiskFixerRunning {
				break
			}
		}
		log.LogInfof("action[checkDataNodeRiskData] host:%v, node:%v, check risk fix finished, check count:%v, risk count:%v, running:%v", host.host, riskDataNode.Addr, count, partitions.RiskFixerRunning, partitions.RiskCount)
	}
}
