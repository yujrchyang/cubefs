package cfs

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/schedulenode/common/cfs"
	"github.com/cubefs/cubefs/sdk/http_client"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/util/checktool"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"io/ioutil"
	"net"
	"net/http"
	"sort"
	"strings"
	"time"
)

func (s *ChubaoFSMonitor) scheduleToCheckNodesAlive() {
	ticker := time.NewTicker(time.Duration(s.scheduleInterval) * time.Second)
	defer func() {
		ticker.Stop()
	}()
	s.checkNodesAlive()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.checkNodesAlive()
		}
	}
}

func (s *ChubaoFSMonitor) checkNodesAlive() {
	for _, host := range s.hosts {
		checkNodeWg.Add(1)
		go func(host *ClusterHost) {
			defer checkNodeWg.Done()
			log.LogInfof("checkNodesAlive [%v] begin", host)
			startTime := time.Now()
			cv, err := getClusterByMasterNodes(host)
			if err != nil {
				log.LogErrorf("Domain[%v] get cluster by master nodes error, retry by domain, err:%v", host.host, err)
				cv, err = getCluster(host)
			}
			if err != nil {
				if isConnectionRefusedFailure(err) {
					msg := fmt.Sprintf("get cluster info from %v failed, err:%v ", host.host, err)
					checktool.WarnBySpecialUmpKey(UMPCFSClusterConnRefused, msg)
					return
				}
				msg := fmt.Sprintf("get cluster info from %v failed,err:%v ", host.host, err)
				checktool.WarnBySpecialUmpKey(UMPCFSNormalWarnKey, msg)
				return
			}
			cv.checkMetaNodeAlive(host)
			cv.checkDataNodeAlive(host, s)
			if s.checkFlashNode {
				cv.checkFlashNodeAlive(host)
			}
			cv.checkFlashNodeVersion(host, s.flashNodeValidVersions)
			cv.checkMetaNodeDiskStat(host, defaultMNDiskMinWarnSize)
			cv.checkMetaNodeDiskStatByMDCInfoFromSre(host, s)
			cv.checkMetaNodeFailedMetaPartitions(host)
			if time.Since(host.lastCleanExpiredMetaTime) > time.Hour*4 {
				host.lastCleanExpiredMetaTime = time.Now()
				cv.cleanExpiredMetaPartitions(host, s.ExpiredMetaRemainDaysCfg)
			}
			cv.checkMetaNodeRaftLogBackupAlive(host)
			host.warnInactiveNodesBySpecialUMPKey()
			log.LogInfof("checkNodesAlive [%v] end,cost[%v]", host, time.Since(startTime))
		}(host)
	}
	checkNodeWg.Wait()
}

func (cv *ClusterView) confirmCheckMetaNodeAlive(host *ClusterHost, enableWarn bool) (inactiveMetaNodes map[string]*DeadNode) {
	confirmThreshold := 5
	deadNodes := make([]MetaNodeView, 0)
	for _, mn := range cv.MetaNodes {
		if mn.Status == false {
			deadNodes = append(deadNodes, mn)
		}
	}
	if len(deadNodes) == 0 {
		if len(host.deadMetaNodes) != 0 {
			host.deadMetaNodes = make(map[string]*DeadNode, 0)
		}
		return
	}

	host.tokenLock.Lock()
	if host.tokenMap[metaNodeAliveRetryToken] > 0 && len(deadNodes) >= confirmThreshold {
		var confirmDeadCount int
		for _, mn := range deadNodes[:confirmThreshold] {
			if confirmMetaNodeActive(mn.Addr, host.isReleaseCluster) {
				log.LogWarnf("action[confirmCheckMetaNodeAlive] domain[%v] metanode[%v] maybe active", host.host, mn.Addr)
				continue
			}
			confirmDeadCount++
		}
		if confirmDeadCount < 2 {
			host.tokenMap[metaNodeAliveRetryToken] = host.tokenMap[metaNodeAliveRetryToken] - 1
			log.LogWarnf("action[checkMetaNodeAlive] confirm check metanode alive conflict with cluster view, please retry")
			host.tokenLock.Unlock()
			return
		}
	}
	host.tokenLock.Unlock()

	inactiveLen := len(deadNodes)
	inactiveMetaNodes = make(map[string]*DeadNode, 0)
	var (
		metaNode *DeadNode
		ok       bool
	)
	for _, mn := range deadNodes {
		metaNode, ok = host.deadMetaNodes[mn.Addr]
		if !ok {
			metaNode = &DeadNode{ID: mn.ID, Addr: mn.Addr, LastReportTime: time.Now()}
		}
		inactiveMetaNodes[mn.Addr] = metaNode
	}
	host.deadMetaNodes = inactiveMetaNodes
	log.LogWarnf("action[checkMetaNodeAlive] %v has %v inactive meta nodes %v", host.host, len(inactiveMetaNodes), deadNodes)
	if enableWarn {
		msg := fmt.Sprintf("%v has %v inactive meta nodes,some of which have been inactive for five minutes,", host, inactiveLen)
		host.doProcessAlarm(host.deadMetaNodes, msg, metaNodeType)
	}
	return
}

func (cv *ClusterView) checkMetaNodeAlive(host *ClusterHost) {
	inactiveMetaNodes := cv.confirmCheckMetaNodeAlive(host, true)
	if len(inactiveMetaNodes) == 0 {
		return
	}
	for addr, t := range host.offlineMetaNodesIn24Hour {
		if time.Since(t) > 24*time.Hour {
			delete(host.offlineMetaNodesIn24Hour, addr)
		}
	}
	if len(host.offlineMetaNodesIn24Hour) > defaultMaxOfflineMetaNodes {
		log.LogWarnf("action[checkMetaNodeAlive] %v has offline %v inactive meta nodes in latest 24 hours", host.host, defaultMaxOfflineMetaNodes)
		return
	}
	for _, inactiveMn := range inactiveMetaNodes {
		mn, err := getMetaNode(host, inactiveMn.Addr)
		if err != nil {
			return
		}
		if time.Since(mn.ReportTime) > 15*time.Minute && len(host.offlineMetaNodesIn24Hour) < defaultMaxOfflineMetaNodes {
			if isPhysicalMachineFailure(inactiveMn.Addr) {
				log.LogErrorf("action[isPhysicalMachineFailure] %v meta node:%v", host.host, inactiveMn.Addr)
				if host.host == "cn.chubaofs.jd.local" || host.host == "cn.chubaofs-seqwrite.jd.local" || host.host == "cn.elasticdb.jd.local" {
					offlineMetaNode(host, inactiveMn.Addr)
					host.lastTimeOfflineMetaNode = time.Now()
					host.offlineMetaNodesIn24Hour[inactiveMn.Addr] = time.Now()
				}
			}
		}
	}
}

func (ch *ClusterHost) doProcessAlarm(nodes map[string]*DeadNode, msg string, nodeType int) {
	var (
		inOneCycle bool
		needAlarm  bool
	)
	for _, dd := range nodes {
		inOneCycle = time.Since(dd.LastReportTime) < checktool.DefaultWarnInternal*time.Second
		if !inOneCycle {
			needAlarm = true
			msg = msg + dd.String() + "\n"
			dd.LastReportTime = time.Now()
		}
	}
	// 荷兰CFS 存在故障节点就执行普通告警
	if needAlarm && ch.host == "nl.chubaofs.jd.local" && len(nodes) > 0 {
		checktool.WarnBySpecialUmpKey(UMPCFSNormalWarnKey, msg)
	}
	if needAlarm && len(nodes) >= defaultMaxInactiveNodes {
		checktool.WarnBySpecialUmpKey(UMPKeyInactiveNodes, msg)
	}
	if len(nodes) == 1 {
		return
	}
	if len(nodes) <= defaultMaxInactiveNodes && nodeType == dataNodeType {
		ch.doProcessDangerousDp(nodes)
	}
	return
}

func (ch *ClusterHost) doProcessDangerousDp(nodes map[string]*DeadNode) {
	inOneCycle := time.Since(ch.lastTimeAlarmDP) < checktool.DefaultWarnInternal*time.Second
	if inOneCycle {
		return
	}
	sentryMap := make(map[uint64]int, 0)
	dangerDps := make([]uint64, 0)
	for _, dd := range nodes {
		dataNode, err := ch.getDataNode(dd.Addr)
		if err != nil {
			continue
		}
		for _, id := range dataNode.PersistenceDataPartitions {
			count, ok := sentryMap[id]
			if !ok {
				sentryMap[id] = 1
			} else {
				sentryMap[id] = count + 1
				dangerDps = append(dangerDps, id)
			}
		}
	}

	if len(dangerDps) != 0 {
		ips := ""
		for addr, _ := range nodes {
			ips += ips + addr + ","
		}
		msg := fmt.Sprintf("%v has %v inactive data nodes,ips[%v],dangerous data partitions[%v]", ch.host, len(nodes), ips, dangerDps)
		checktool.WarnBySpecialUmpKey(UMPKeyInactiveNodes, msg)
	}
	return
}

func (ch *ClusterHost) getDataNode(addr string) (dataNode *DataNode, err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("action[getDataNode] host[%v],addr[%v],err[%v]", ch.host, addr, err)
		}
	}()
	reqURL := fmt.Sprintf("http://%v/dataNode/get?addr=%v", ch.host, addr)
	data, err := doRequest(reqURL, ch.isReleaseCluster)
	if err != nil {
		return
	}
	dataNode = &DataNode{}
	if err = json.Unmarshal(data, dataNode); err != nil {
		return
	}
	return
}

func (cv *ClusterView) checkFlashNodeVersion(host *ClusterHost, expectVersion []string) {
	if len(expectVersion) == 0 {
		log.LogWarnf("checkFlashNodeVersion, no version specified in config:%v, skip check", cfgKeyFlashNodeValidVersions)
		return
	}
	port := host.getFlashNodeProfPort()
	for _, fn := range cv.FlashNodes {
		client := http_client.NewFlashClient(fmt.Sprintf("%v:%v", strings.Split(fn.Addr, ":")[0], port), false)
		version, err := client.GetVersion()
		if err != nil {
			log.LogErrorf("checkFlashNodeVersion, node:%v, err:%v", fn.Addr, err)
			continue
		}
		rightV := false
		for _, v := range expectVersion {
			if v == version.CommitID {
				rightV = true
			}
		}
		if !rightV {
			exporter.WarningBySpecialUMPKey(UMPCFSSparkFlashNodeVersionKey, fmt.Sprintf("flashnode[%v] invalid version[%v], expect[%v], has been automatically disabled", fn.Addr, version.CommitID, expectVersion))
			masterClient := master.NewMasterClient([]string{host.host}, false)
			var fnv *proto.FlashNodeViewInfo
			fnv, err = masterClient.NodeAPI().GetFlashNode(fn.Addr)
			if err != nil {
				log.LogErrorf("checkFlashNodeVersion, node:%v, err:%v", fn.Addr, err)
				continue
			}
			if fnv.IsEnable && time.Since(host.lastDisableFlashNodeTime) > time.Hour*6 {
				log.LogWarnf("set flashnode to inactive:%v, time:%v", fn.Addr, host.lastDisableFlashNodeTime)
				/*				err = masterClient.NodeAPI().SetFlashNodeState(fn.Addr, "false")
								if err != nil {
									log.LogErrorf("checkFlashNodeVersion, node:%v, err:%v", fn.Addr, err)
								}*/
				host.lastDisableFlashNodeTime = time.Now()
			}
		}
	}
}

func (cv *ClusterView) checkFlashNodeAlive(host *ClusterHost) {
	log.LogWarnf("action[checkFlashNodeAlive] domain[%v] begin check inactive flash nodes", host.host)
	deadNodes := make([]FlashNodeView, 0)
	for _, fn := range cv.FlashNodes {
		if fn.Status == false {
			deadNodes = append(deadNodes, fn)
		}
	}
	inactiveLen := len(deadNodes)
	if inactiveLen == 0 {
		if len(host.deadFlashNodes) != 0 {
			host.deadFlashNodes = make(map[string]*DeadNode, 0)
		}
		return
	}
	inactiveFlashNodes := make(map[string]*DeadNode, 0)
	var (
		flashNode *DeadNode
		ok        bool
	)
	for _, fn := range deadNodes {
		flashNode, ok = host.deadFlashNodes[fn.Addr]
		if !ok {
			flashNode = &DeadNode{Addr: fn.Addr, LastReportTime: time.Now()}
		}
		inactiveFlashNodes[fn.Addr] = flashNode
	}
	host.deadFlashNodes = inactiveFlashNodes
	log.LogWarnf("action[checkFlashNodeAlive] %v has %v inactive flash nodes", host.host, len(inactiveFlashNodes))
	msg := fmt.Sprintf("%v has %v inactive flash nodes,some of which have been inactive for 5 minutes,", host, inactiveLen)
	host.doProcessAlarm(host.deadFlashNodes, msg, flashNodeType)
	return
	// do not auto offline flashnodes
	/*	if len(inactiveFlashNodes) <= defaultMaxOfflineFlashNodesIn24Hour {
		for _, fn := range inactiveFlashNodes {
			flashNodeView, err := getFlashNode(host, fn.Addr)
			if err != nil {
				return
			}

			// 清理超过24小时的记录
			for key, t := range host.offlineFlashNodesIn24Hour {
				if time.Since(t) > 24*time.Hour {
					delete(host.offlineFlashNodesIn24Hour, key)
				}
			}

			if time.Since(flashNodeView.ReportTime) < 60*time.Minute {
				continue
			}

			if len(host.offlineFlashNodesIn24Hour) <= defaultMaxOfflineFlashNodesIn24Hour {
				host.offlineFlashNodesIn24Hour[flashNode.Addr] = time.Now()
				log.LogDebugf("action[checkFlashNodeAlive] offlineFlashNodesIn24Hour:%v", host.offlineFlashNodesIn24Hour)
				offlineFlashNode(host, flashNodeView.Addr)
			}
		}
	}*/
}

func confirmDataNodeActive(addr string, isDbback bool) (active bool) {
	var err error
	defer func() {
		if err != nil {
			log.LogErrorf("action[confirmDataNodeActive] addr[%v] err:%v", addr, err)
		}
	}()
	dataClient := http_client.NewDataClient(fmt.Sprintf("%v:%v", strings.Split(addr, ":")[0], profPortMap[strings.Split(addr, ":")[1]]), false)
	if isDbback {
		_, err = dataClient.GetDbbackDataNodeStats()
		if err != nil {
			return false
		}
	} else {
		_, err = dataClient.GetDatanodeStats()
		if err != nil {
			return false
		}
	}
	return true
}

func confirmMetaNodeActive(addr string, isDbback bool) (active bool) {

	var metaClient *meta.MetaHttpClient
	if isDbback {
		metaClient = meta.NewMetaHttpClient(fmt.Sprintf("%v:%v", strings.Split(addr, ":")[0], profPortMap[strings.Split(addr, ":")[1]]), false)
	} else {
		metaClient = meta.NewDBBackMetaHttpClient(fmt.Sprintf("%v:%v", strings.Split(addr, ":")[0], profPortMap[strings.Split(addr, ":")[1]]), false)
	}
	_, err := metaClient.GetStatInfo()
	if err != nil {
		return false
	}
	return true
}

func (cv *ClusterView) confirmCheckDataNodeAlive(host *ClusterHost, enableWarn bool) (inactiveDataNodes map[string]*DeadNode) {
	confirmThreshold := 5
	inactiveDataNodes = make(map[string]*DeadNode, 0)
	deadNodes := make([]DataNodeView, 0)
	for _, dn := range cv.DataNodes {
		if dn.Status == false {
			deadNodes = append(deadNodes, dn)
		}
	}
	if len(deadNodes) == 0 {
		if len(host.deadDataNodes) != 0 {
			host.deadDataNodes = make(map[string]*DeadNode, 0)
		}
		return
	}

	host.tokenLock.Lock()
	if host.tokenMap[dataNodeAliveRetryToken] > 0 && len(deadNodes) >= confirmThreshold {
		var confirmDeadCount int
		for _, dn := range deadNodes[:confirmThreshold] {
			if confirmDataNodeActive(dn.Addr, host.isReleaseCluster) {
				log.LogWarnf("action[confirmCheckDataNodeAlive] domain[%v] datanode[%v] maybe active", host.host, dn.Addr)
				continue
			}
			confirmDeadCount++
		}
		if confirmDeadCount < 2 {
			host.tokenMap[dataNodeAliveRetryToken] = host.tokenMap[dataNodeAliveRetryToken] - 1
			log.LogWarnf("action[checkDataNodeAlive] confirm check datanode alive conflict with cluster view, please retry")
			host.tokenLock.Unlock()
			return
		}
	}
	host.tokenLock.Unlock()

	inactiveLen := len(deadNodes)
	var (
		dataNode *DeadNode
		ok       bool
	)
	for _, dn := range deadNodes {
		dataNode, ok = host.deadDataNodes[dn.Addr]
		if !ok {
			dataNode = &DeadNode{Addr: dn.Addr, LastReportTime: time.Now()}
		}
		inactiveDataNodes[dn.Addr] = dataNode
	}
	host.deadDataNodes = inactiveDataNodes
	log.LogWarnf("action[checkDataNodeAlive] %v has %v inactive data nodes %v", host.host, len(inactiveDataNodes), deadNodes)
	if enableWarn {
		msg := fmt.Sprintf("%v has %v inactive data nodes,some of which have been inactive for five minutes,", host, inactiveLen)
		host.doProcessAlarm(host.deadDataNodes, msg, dataNodeType)
	}
	return
}

func (cv *ClusterView) checkDataNodeAlive(host *ClusterHost, s *ChubaoFSMonitor) {
	inactiveDataNodes := cv.confirmCheckDataNodeAlive(host, true)
	if len(inactiveDataNodes) == 0 {
		return
	}
	nodeZoneMap, err := getNodeToZoneMap(host)
	if err != nil {
		log.LogErrorf("action[getNodeToZoneMap] host[%v] err[%v]", host.host, err)
	}
	if len(inactiveDataNodes) == 1 {
		for _, dn := range inactiveDataNodes {
			dataNodeView, err := getDataNode(host, dn.Addr)
			if err != nil {
				return
			}
			if time.Since(dataNodeView.ReportTime) > 30*time.Minute {
				if isPhysicalMachineFailure(dn.Addr) {
					log.LogErrorf("action[isPhysicalMachineFailure],addr[%v],err[%v]", dn.Addr, err)
					//超过一定时间 尝试重启机器
					if host.host == "cn.chubaofs.jd.local" {
						//deprecated
						if err1 := s.checkThenRestartNode(dn.Addr, host.host); err1 != nil {
							log.LogErrorf("action[checkThenRestartNode] addr[%v] err1[%v]", dn.Addr, err1)
						}
					}
					/* 不再发起节点下线xbp单子
					var reqURL string
					if host.isReleaseCluster {
						reqURL = fmt.Sprintf("http://%v/dataNode/offline?addr=%v", host, deadNodes[0].Addr)
					} else {
						reqURL = fmt.Sprintf("http://%v/dataNode/decommission?addr=%v", host, deadNodes[0].Addr)
					}
					s.addDataNodeOfflineXBPTicket(deadNodes[0].Addr, reqURL, cv.Name, host.isReleaseCluster)
					*/
					// 清理超过24小时的记录
					for key, t := range host.offlineDataNodesIn24Hour {
						if time.Since(t) > 24*time.Hour {
							delete(host.offlineDataNodesIn24Hour, key)
						}
					}
					// 如果持续小于1个小时，不进行自动处理
					if time.Since(dataNodeView.ReportTime) < time.Hour {
						continue
					}
					// 判断24小时内下线的数目，符合要求则加入待执行下线磁盘的DataNode map
					if len(host.offlineDataNodesIn24Hour) < s.offlineDataNodeMaxCountIn24Hour {
						host.offlineDataNodesIn24Hour[dataNodeView.Addr] = time.Now()
						log.LogDebugf("action[checkDataNodeAlive] offlineDataNodesIn24Hour:%v", host.offlineDataNodesIn24Hour)
						if _, ok := host.inOfflineDiskDataNodes[dataNodeView.Addr]; !ok {
							host.inOfflineDiskDataNodes[dataNodeView.Addr] = dataNodeView.ReportTime
							log.LogDebugf("action[checkDataNodeAlive] inOfflineDiskDataNodes:%v", host.inOfflineDiskDataNodes)
						}
					}
					// 对待下线DataNode执行下线磁盘操作
					offlineBadDataNodeOneDisk(host)
					zoneName := nodeZoneMap[dataNodeView.Addr]
					if (isSSD(host.host, zoneName) && time.Since(dataNodeView.ReportTime) > time.Hour) ||
						time.Since(dataNodeView.ReportTime) > 8*time.Hour {
						if isSSD(host.host, zoneName) {
							offlineDataNode(host, dataNodeView.Addr)
							delete(host.inOfflineDiskDataNodes, dataNodeView.Addr)
							delete(s.lastCheckStartTime, dataNodeView.Addr)
							continue
						}
						if _, ok := host.inOfflineDiskDataNodes[dataNodeView.Addr]; ok {
							// 如果超过8小时 直接执行节点下线
							// 需要控制节点剩余DP数量，避免正在下线的DP数量过多
							badDPsCount, err1 := getBadPartitionIDsCount(host)
							if err1 != nil || badDPsCount > maxBadDataPartitionsCount {
								log.LogWarn(fmt.Sprintf("action[checkDataNodeAlive] getBadPartitionIDsCount host:%v badDPsCount:%v err:%v", host, badDPsCount, err1))
								continue
							}
							nodeView, err1 := getDataNode(host, dataNodeView.Addr)
							if err1 != nil {
								log.LogWarn(fmt.Sprintf("action[checkDataNodeAlive] getDataNode host:%v addr:%v err:%v", host, dataNodeView.Addr, err1))
								continue
							}
							badDPsCount += len(nodeView.PersistenceDataPartitions)
							if badDPsCount > maxBadDataPartitionsCount {
								continue
							}
							offlineDataNode(host, dataNodeView.Addr)
							delete(host.inOfflineDiskDataNodes, dataNodeView.Addr)
							delete(s.lastCheckStartTime, dataNodeView.Addr)
						}
					}
				}
			}
		}
	}
	return
}

func offlineBadDataNodeOneDisk(host *ClusterHost) {
	var diskPathsMap = map[int][]string{
		0: {"/data0", "/data6"},
		1: {"/data1", "/data7"},
		2: {"/data2", "/data8"},
		3: {"/data3", "/data9"},
		4: {"/data4", "/data10"},
		5: {"/data5", "/data11"},
	}
	var dbbakDiskPathsMap = map[int][]string{
		0: {"/cfsd0", "/cfsd6"},
		1: {"/cfsd1", "/cfsd7"},
		2: {"/cfsd2", "/cfsd8"},
		3: {"/cfsd3", "/cfsd9"},
		4: {"/cfsd4", "/cfsd10"},
		5: {"/cfsd5", "/cfsd11"},
	}
	for dataNodeAddr, lastOfflineDiskTime := range host.inOfflineDiskDataNodes {
		if time.Since(lastOfflineDiskTime) > time.Minute*30 {
			if !isPhysicalMachineFailure(dataNodeAddr) {
				delete(host.inOfflineDiskDataNodes, dataNodeAddr)
				continue
			}
			dataNodeView, err := getDataNode(host, dataNodeAddr)
			if err != nil {
				return
			}
			if time.Since(dataNodeView.ReportTime) < 5*time.Hour {
				delete(host.inOfflineDiskDataNodes, dataNodeAddr)
				continue
			}
			// 获取要被下线的磁盘地址 根据时间计算
			offlineDiskIndex := int((time.Since(dataNodeView.ReportTime) - 5*time.Hour) / (30 * time.Minute))
			diskPaths, ok := diskPathsMap[offlineDiskIndex%6]
			if !ok {
				continue
			}
			for _, diskPath := range diskPaths {
				offlineDataNodeDisk(host, dataNodeAddr, diskPath, false)
			}
			if host.isReleaseCluster {
				diskPaths, ok = dbbakDiskPathsMap[offlineDiskIndex%6]
				if !ok {
					continue
				}
				for _, diskPath := range diskPaths {
					offlineDataNodeDisk(host, dataNodeAddr, diskPath, false)
				}
			}
			host.inOfflineDiskDataNodes[dataNodeAddr] = time.Now()
		}
	}
}

func isPhysicalMachineFailure(addr string) (isPhysicalFailure bool) {
	var err error
	defer func() {
		if err != nil {
			log.LogWarnf("action[isPhysicalMachineFailure] node:%v err:%v isPhysicalFailure:%v", addr, err, isPhysicalFailure)
		}
	}()
	_, err = net.DialTimeout("tcp", addr, 3*time.Second)
	if err == nil {
		return false
	}
	oe, ok := err.(net.Error)
	if ok {
		isPhysicalFailure = oe.Timeout()
		return
	}
	return false
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

func doRequestWithTimeOut(reqUrl string, overtime time.Duration) (data []byte, err error) {
	if overtime < 0 {
		overtime = 5
	}
	var resp *http.Response
	client := http.Client{Timeout: overtime * time.Second}
	if resp, err = client.Get(reqUrl); err != nil {
		return
	}
	defer resp.Body.Close()

	if data, err = ioutil.ReadAll(resp.Body); err != nil {
		return
	}
	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("action[doRequestWithTimeOut] reqRUL[%v],statusCode[%v],body[%v]", reqUrl, resp.StatusCode, string(data))
		return
	}
	return
}

func getCluster(host *ClusterHost) (cv *ClusterView, err error) {
	reqURL := fmt.Sprintf("http://%v/admin/getCluster", host)
	data, err := doRequest(reqURL, host.isReleaseCluster)
	if err != nil {
		return
	}
	cv = &ClusterView{}
	if err = json.Unmarshal(data, cv); err != nil {
		log.LogErrorf("get cluster from %v failed ,data:%v,err:%v", host, string(data), err)
		return
	}
	if !host.isReleaseCluster {
		cv.DataNodeStat = cv.DataNodeStatInfo
		cv.MetaNodeStat = cv.MetaNodeStatInfo
		if cv.VolStat, err = GetVolStatFromVolList(host.host, host.isReleaseCluster); err != nil {
			return
		}
		if len(cv.VolStatInfo) == 0 {
			cv.VolStatInfo = cv.VolStat
		}
	}
	log.LogInfof("action[getCluster],host[%v],len(VolStat)=%v,len(metaNodes)=%v,len(dataNodes)=%v",
		host, len(cv.VolStat), len(cv.MetaNodes), len(cv.DataNodes))
	return
}

func getClusterByMasterNodes(host *ClusterHost) (cv *ClusterView, err error) {
	var data []byte
	if len(host.masterNodes) == 0 {
		return nil, fmt.Errorf("master nodes is empty")
	}
	for _, masterNode := range host.masterNodes {
		reqURL := fmt.Sprintf("http://%v/admin/getCluster", masterNode)
		data, err = doRequest(reqURL, host.isReleaseCluster)
		if err == nil {
			break
		}
	}
	if err != nil {
		return
	}
	cv = &ClusterView{}
	if err = json.Unmarshal(data, cv); err != nil {
		log.LogErrorf("action[getClusterByMasterNodes] from %v failed ,data:%v,err:%v", host, string(data), err)
		return
	}
	if !host.isReleaseCluster {
		cv.DataNodeStat = cv.DataNodeStatInfo
		cv.MetaNodeStat = cv.MetaNodeStatInfo
		if cv.VolStat, err = GetVolStatFromVolList(host.host, host.isReleaseCluster); err != nil {
			return
		}
		if len(cv.VolStatInfo) == 0 {
			cv.VolStatInfo = cv.VolStat
		}
	}
	log.LogInfof("action[getClusterByMasterNodes],host[%v],len(VolStat)=%v,len(metaNodes)=%v,len(dataNodes)=%v",
		host, len(cv.VolStat), len(cv.MetaNodes), len(cv.DataNodes))
	return
}

func GetVolStatFromVolList(host string, isReleaseCluster bool) (volStats []*VolSpaceStat, err error) {
	volInfos, err := GetVolList(host, isReleaseCluster)
	if err != nil {
		return
	}
	volStats = make([]*VolSpaceStat, 0)
	for _, volInfo := range volInfos {
		stat := &VolSpaceStat{
			Name: volInfo.Name,
		}
		if volInfo.TotalSize >= 0 {
			stat.TotalSize = uint64(volInfo.TotalSize)
		}
		if volInfo.UsedSize >= 0 {
			stat.UsedSize = uint64(volInfo.UsedSize)
		}
		stat.TotalGB = stat.TotalSize / GB
		stat.UsedGB = stat.UsedSize / GB
		if stat.TotalSize != 0 {
			stat.UsedRatio = fmt.Sprintf("%.3f", float64(stat.UsedSize)/float64(stat.TotalSize))
		}
		volStats = append(volStats, stat)
	}
	return
}

func GetVolList(host string, isReleaseCluster bool) (volInfos []VolInfo, err error) {
	reqURL := fmt.Sprintf("http://%v/vol/list", host)
	data, err := doRequest(reqURL, isReleaseCluster)
	if err != nil {
		return
	}
	if err = json.Unmarshal(data, &volInfos); err != nil {
		log.LogErrorf("get vol list %v failed ,data:%v,err:%v", host, string(data), err)
		return
	}
	log.LogInfof("action[GetVolList],host[%v],len(VolStat)=%v ", host, len(volInfos))
	return
}

func getNodeToZoneMap(host *ClusterHost) (nodeZoneMap map[string]string, err error) {
	topologyView, err := getTopology(host)
	if err != nil {
		return
	}
	nodeZoneMap = make(map[string]string, 0)
	for _, zoneView := range topologyView.Zones {
		for _, setView := range zoneView.NodeSet {
			for _, dn := range setView.DataNodes {
				nodeZoneMap[dn.Addr] = zoneView.Name
			}
			for _, mn := range setView.MetaNodes {
				nodeZoneMap[mn.Addr] = zoneView.Name
			}
		}
	}
	return
}

// dataNodeAliveRetryToken / metaNodeAliveRetryToken : 每间隔半小时，给meta/data分发重试token，当meta/data在检查节点存活时，如果有多节点出错，
// 在获得token的情况下，允许进行二次确认；没有token则不进行确认。主要为了避免master升级过程中的误报，增加一定的容错机制。
// 同时也为了避免因为容错机制和master心跳的判断之间存在分歧而导致master的有效报警被意外屏蔽。
// resetDbBackRecoverToken: 防止reset recover 过多过快，提高容错率
func (s *ChubaoFSMonitor) resetTokenMap() {
	for _, host := range s.hosts {
		host.initTokenMap()
	}
}

func (s *ChubaoFSMonitor) checkNodeSet() {
	var err error
	for _, host := range s.hosts {
		if host.isReleaseCluster {
			continue
		}
		var tv *TopologyView
		log.LogInfof("action[checkNodeSet] start check host:%v", host.host)
		tv, err = getTopology(host)
		if err != nil {
			log.LogErrorf("action[checkNodeSet] get topology failed, err:%v", err)
			continue
		}
		checkNodeSetLen(tv, host.host)
		log.LogInfof("action[checkNodeSet] finish check host:%v", host.host)
	}
	return
}

func checkNodeSetLen(tv *TopologyView, domain string) (badNodeSets, badMetaNodeSets []uint64) {
	badNodeSets = make([]uint64, 0)
	badMetaNodeSets = make([]uint64, 0)
	minDataNodeSetDiff := 32
	minMetaNodeSetDiff := 32
	for _, zoneView := range tv.Zones {
		var dataNodeSets = make([]struct {
			SetID uint64
			Num   int
		}, 0)
		var metaNodeSets = make([]struct {
			SetID uint64
			Num   int
		}, 0)
		for id, setView := range zoneView.NodeSet {
			if setView.DataNodeLen > 0 {
				dataNodeSets = append(dataNodeSets, struct {
					SetID uint64
					Num   int
				}{SetID: id, Num: setView.DataNodeLen})
			}
			if setView.MetaNodeLen > 0 {
				metaNodeSets = append(metaNodeSets, struct {
					SetID uint64
					Num   int
				}{SetID: id, Num: setView.MetaNodeLen})
			}
		}
		if len(dataNodeSets) >= 2 {
			sort.Slice(dataNodeSets, func(i, j int) bool {
				return dataNodeSets[i].Num < dataNodeSets[j].Num
			})
			if dataNodeSets[0].Num < dataNodeSets[1].Num*50/100 || dataNodeSets[1].Num-dataNodeSets[0].Num > minDataNodeSetDiff {
				msg := fmt.Sprintf("Domain[%v] zone[%v] data nodeset[%v] too small[%v]", domain, zoneView.Name, dataNodeSets[0].SetID, dataNodeSets[0].Num)
				exporter.WarningBySpecialUMPKey(UMPCFSNodeSetNumKey, msg)
				badNodeSets = append(badNodeSets, dataNodeSets[0].SetID)
			}
		}
		if len(metaNodeSets) >= 2 {
			sort.Slice(metaNodeSets, func(i, j int) bool {
				return metaNodeSets[i].Num < metaNodeSets[j].Num
			})
			if metaNodeSets[0].Num < metaNodeSets[1].Num*50/100 || metaNodeSets[1].Num-metaNodeSets[0].Num > minMetaNodeSetDiff {
				msg := fmt.Sprintf("Domain[%v] zone[%v] meta nodeset[%v] too small[%v]", domain, zoneView.Name, metaNodeSets[0].SetID, metaNodeSets[0].Num)
				exporter.WarningBySpecialUMPKey(UMPCFSNodeSetNumKey, msg)
				badMetaNodeSets = append(badMetaNodeSets, metaNodeSets[0].SetID)
			}
		}
	}
	return
}

func getTopology(host *ClusterHost) (tv *TopologyView, err error) {
	reqURL := fmt.Sprintf("http://%v/topo/get", host)
	data, err := doRequest(reqURL, host.isReleaseCluster)
	if err != nil {
		return
	}
	tv = &TopologyView{}
	if err = json.Unmarshal(data, tv); err != nil {
		log.LogErrorf("getTopology from %v failed ,data:%v,err:%v", host, string(data), err)
		return
	}
	return
}

func getMetaNode(host *ClusterHost, addr string) (mn *MetaNodeView, err error) {
	reqURL := fmt.Sprintf("http://%v/metaNode/get?addr=%v", host, addr)
	data, err := doRequest(reqURL, host.isReleaseCluster)
	if err != nil {
		return
	}
	mn = &MetaNodeView{}
	if err = json.Unmarshal(data, mn); err != nil {
		log.LogErrorf("get metaNode information from %v failed ,data:%v,err:%v", host, string(data), err)
		return
	}
	log.LogInfof("action[getMetaNode],host[%v],addr[%v],reportTime[%v]",
		host, addr, mn.ReportTime)
	return
}

func getDataNode(host *ClusterHost, addr string) (dn *DataNodeView, err error) {
	reqURL := fmt.Sprintf("http://%v/dataNode/get?addr=%v", host, addr)
	data, err := doRequest(reqURL, host.isReleaseCluster)
	if err != nil {
		return
	}
	dn = &DataNodeView{}
	if err = json.Unmarshal(data, dn); err != nil {
		log.LogErrorf("get getDataNode information from %v failed ,data:%v,err:%v", host, string(data), err)
		return
	}
	log.LogInfof("action[getDataNode],host[%v],addr[%v],reportTime[%v]",
		host, addr, dn.ReportTime)
	return
}

func offlineMetaPatition(host *ClusterHost, addr string, pid uint64) {
	var reqURL string
	if host.isReleaseCluster {
		mp, err := cfs.GetMetaPartition(host.host, pid, true)
		if err != nil {
			log.LogErrorf("action[offlineMetaPartition] reqURL[%v], get metapartition failed, err: %v", reqURL, err)
			return
		}
		reqURL = fmt.Sprintf("http://%v/metaPartition/offline?id=%v&addr=%v&name=%v", host, pid, addr, mp.VolName)
	} else {
		reqURL = fmt.Sprintf("http://%v/metaPartition/decommission?id=%v&addr=%v", host, pid, addr)
	}
	data, err := doRequest(reqURL, host.isReleaseCluster)
	if err != nil {
		log.LogErrorf("action[offlineMetaPartition] occurred err,url[%v],err %v", reqURL, err)
		return
	}
	msg := fmt.Sprintf("action[offlineMetaPartition] reqURL[%v],data[%v]", reqURL, string(data))
	checktool.WarnBySpecialUmpKey(UMPCFSNormalWarnKey, msg)
	return
}

func offlineMetaNode(host *ClusterHost, addr string) {
	var reqURL string
	if host.isReleaseCluster {
		reqURL = fmt.Sprintf("http://%v/metaNode/offline?addr=%v", host, addr)
	} else {
		reqURL = fmt.Sprintf("http://%v/metaNode/decommission?addr=%v", host, addr)
	}
	data, err := doRequest(reqURL, host.isReleaseCluster)
	if err != nil {
		log.LogErrorf("action[offlineMetaNode] occurred err,url[%v],err %v", reqURL, err)
		return
	}
	msg := fmt.Sprintf("action[offlineMetaNode] reqURL[%v],data[%v]", reqURL, string(data))
	checktool.WarnBySpecialUmpKey(UMPCFSNormalWarnKey, msg)
	return
}

func offlineDataNode(host *ClusterHost, addr string) {
	var reqURL string
	if host.isReleaseCluster {
		reqURL = fmt.Sprintf("http://%v/dataNode/offline?addr=%v", host, addr)
	} else {
		reqURL = fmt.Sprintf("http://%v/dataNode/decommission?addr=%v", host, addr)
	}
	data, err := doRequest(reqURL, host.isReleaseCluster)
	if err != nil {
		log.LogErrorf("action[offlineDataNode] occurred err,url[%v],err %v", reqURL, err)
		return
	}
	msg := fmt.Sprintf("action[offlineDataNode] reqURL[%v],data[%v]", reqURL, string(data))
	checktool.WarnBySpecialUmpKey(UMPCFSNormalWarnKey, msg)
	return
}

func (cv *ClusterView) checkMetaNodeDiskStat(host *ClusterHost, diskMinWarnSize int) {
	var port string
	// exclude hosts which have not update the meta node disk stat API
	if host.host == "cn.chubaofs-seqwrite.jd.local" {
		return
	}
	// set meta node port
	checkedCount := 0
	port = host.getMetaNodePProfPort()
	diskWarnNodes := make([]string, 0)
	for _, mn := range cv.MetaNodes {
		if mn.Status == false {
			continue
		}
		ipPort := strings.Split(mn.Addr, ":")
		isNeedTelAlarm, err := doCheckMetaNodeDiskStat(ipPort[0], port, host.isReleaseCluster, diskMinWarnSize)
		if err != nil {
			log.LogErrorf("action[checkMetaNodeDiskStat] host[%v] addr[%v] doCheckMetaNodeDiskStat err[%v]", host, mn.Addr, err)
			continue
		}
		checkedCount++
		if isNeedTelAlarm {
			diskWarnNodes = append(diskWarnNodes, mn.Addr)
		}
	}
	if len(diskWarnNodes) == 0 {
		log.LogInfof("action[checkMetaNodeDiskStat] host[%v] MN count[%v] checkedCount:%v diskStatWarnMetaNodes is 0", host, len(cv.MetaNodes), checkedCount)
		return
	}
	msg := fmt.Sprintf("%v has disk less than %vGB meta nodes:%v", host, diskMinWarnSize/GB, diskWarnNodes)
	if time.Since(host.metaNodeDiskUsedWarnTime) >= time.Minute*5 {
		checktool.WarnBySpecialUmpKey(UMPKeyMetaNodeDiskSpace, msg)
		host.metaNodeDiskUsedWarnTime = time.Now()
	} else {
		log.LogWarnf("action[checkMetaNodeDiskStat] :%v", msg)
	}
}

var (
	excludeCheckMetaNodeRaftLogBackupHosts = []string{"nl.chubaofs.ochama.com", "nl.chubaofs.jd.local"}
)

func (cv *ClusterView) checkMetaNodeRaftLogBackupAlive(host *ClusterHost) {
	for _, raftLogBackupHost := range excludeCheckMetaNodeRaftLogBackupHosts {
		if host.host == raftLogBackupHost {
			return
		}
	}
	raftLogBackupWarnMetaNodes := make([]string, 0)
	port := 15000
	for _, metaNode := range cv.MetaNodes {
		ipPort := strings.Split(metaNode.Addr, ":")
		isNeedAlarm, err := doCheckMetaNodeRaftLogBackupStat(ipPort[0], port)
		if err != nil {
			log.LogWarnf("action[checkMetaNodeRaftLogBackupAlive] host[%v] addr[%v] doCheckMetaNodeDiskStat err[%v]", host, metaNode.Addr, err)
		}
		if isNeedAlarm {
			raftLogBackupWarnMetaNodes = append(raftLogBackupWarnMetaNodes, metaNode.Addr)
		}
	}
	if len(raftLogBackupWarnMetaNodes) == 0 {
		return
	}
	msg := fmt.Sprintf("checkMetaNodeRaftLogBackupAlive: host[%v], fault count[%v] fault ips[%v]", host.host, len(raftLogBackupWarnMetaNodes), raftLogBackupWarnMetaNodes)
	checktool.WarnBySpecialUmpKey(UMPCFSRaftlogBackWarnKey, msg)
}

func (cv *ClusterView) checkMetaNodeFailedMetaPartitions(host *ClusterHost) {
	for _, metaNode := range cv.MetaNodes {
		ipPort := strings.Split(metaNode.Addr, ":")
		failedMpArr, err := doGetFailedMetaPartitions(ipPort[0], host.getMetaNodePProfPort(), host.isReleaseCluster)
		if err != nil {
			log.LogWarnf("action[checkMetaNodeFailedMetaPartitions] host[%v] addr[%v] doGetFailedMetaPartitions err[%v]", host, metaNode.Addr, err)
			continue
		}

		for _, mp := range failedMpArr {
			offlineMetaPatition(host, metaNode.Addr, mp)
		}
	}
}

func (cv *ClusterView) cleanExpiredMetaPartitions(host *ClusterHost, days int) {
	for _, metaNode := range cv.MetaNodes {
		ipPort := strings.Split(metaNode.Addr, ":")
		_, err := doCleanExpiredMetaPartitions(ipPort[0], host.getMetaNodePProfPort(), days, host.isReleaseCluster)
		if err != nil {
			log.LogWarnf("action[cleanExpiredMetaPartitions] host[%v] addr[%v] cleanExpiredMetaPartitions err[%v]", host, metaNode.Addr, err)
			continue
		}
	}
}

func doCheckMetaNodeDiskStat(ip, port string, isReleaseCluster bool, diskMinWarnSize int) (isNeedTelAlarm bool, err error) {
	type Disk struct {
		Path      string  `json:"Path"`
		Total     float64 `json:"Total"`
		Used      float64 `json:"Used"`
		Available float64 `json:"Available"`
	}
	// curl "http://172.26.36.130:17220/getDiskStat"
	reqURL := fmt.Sprintf("http://%v:%v/getDiskStat", ip, port)
	data, err := doRequest(reqURL, isReleaseCluster)
	if err != nil {
		return
	}
	disks := make([]Disk, 0)
	if err = json.Unmarshal(data, &disks); err != nil {
		return
	}
	for _, disk := range disks {
		if disk.Total > 0 && disk.Used/disk.Total < defaultMNDiskMinWarnRatio {
			continue
		}
		if disk.Available < float64(diskMinWarnSize) {
			isNeedTelAlarm = true
			break
		}
	}
	return
}

func doCheckMetaNodeRaftLogBackupStat(ip string, port int) (bool, error) {
	var returnErr error = nil
	for i := 0; i <= 10; i++ {
		reqURL := fmt.Sprintf("http://%v:%v/status", ip, port)
		data, err := doRequestWithTimeOut(reqURL, 3)
		if err != nil {
			// 端口递增检测
			port++
			returnErr = err
			continue
		}
		if string(data) == "running" {
			return false, nil
		} else {
			return true, err
		}
	}
	return true, returnErr
}

func doGetFailedMetaPartitions(ip string, port string, isReleaseDb bool) ([]uint64, error) {
	var returnErr error = nil
	var failedMpArr []uint64
	for i := 0; i <= 3; i++ {
		reqURL := fmt.Sprintf("http://%v:%v/getStartFailedPartitions", ip, port)
		data, err := doRequest(reqURL, isReleaseDb)
		if err != nil {
			returnErr = err
			continue
		}
		returnErr = json.Unmarshal(data, &failedMpArr)
		break
	}
	return failedMpArr, returnErr
}

func doCleanExpiredMetaPartitions(ip string, port string, days int, isReleaseDb bool) ([]uint64, error) {
	var returnErr error = nil
	failedMpArr := make([]uint64, 0)
	for i := 0; i <= 3; i++ {
		reqURL := fmt.Sprintf("http://%v:%v/cleanExpiredPartitions?Days=%v", ip, port, days)
		_, err := doRequest(reqURL, isReleaseDb)
		if err != nil {
			returnErr = err
			continue
		}
		break
	}
	return failedMpArr, returnErr
}

func (ch *ClusterHost) doProcessMetaNodeDiskStatAlarm(nodes map[string]*DeadNode, msg string) {
	var (
		inOneCycle bool
		needAlarm  bool
	)
	for _, dd := range nodes {
		inOneCycle = time.Since(dd.LastReportTime) < checktool.DefaultWarnInternal*time.Second
		if !inOneCycle {
			needAlarm = true
			msg = msg + dd.String() + "\n"
			dd.LastReportTime = time.Now()
		}
	}
	if needAlarm {
		checktool.WarnBySpecialUmpKey(UMPKeyMetaNodeDiskSpace, msg)
	}
	return
}

func isConnectionRefusedFailure(err error) bool {
	if strings.Contains(strings.ToLower(err.Error()), strings.ToLower(errorConnRefused)) {
		return true
	}
	return false
}

func (ch *ClusterHost) warnInactiveNodesBySpecialUMPKey() {
	if len(ch.deadDataNodes) == 0 && len(ch.deadMetaNodes) == 0 {
		return
	}
	//可能存在节点在线，但是因为机器负载大，进而导致没能按时给master上报心跳的情况，而这种情况运维也暂时无法处理
	//对于异常机器，检查进程是否存在，master视图异常但进程连续多次存在，才执行告警
	ch.checkDeadNodesProcessStatus(10)
	ch.doWarnInactiveNodesBySpecialUMPKey()
}

// 如果检测统计的ProcessStatusCount 大于 needWarnCount 会执行告警，约1分钟一次
func (ch *ClusterHost) checkDeadNodesProcessStatus(needWarnCount int) {
	// 检查进程的启动情况
	for _, deadNode := range ch.deadDataNodes {
		ch.checkDeadNodeStartStatus(deadNode, ch.getDataNodePProfPort(), needWarnCount)
	}
	for _, deadNode := range ch.deadMetaNodes {
		ch.checkDeadNodeStartStatus(deadNode, ch.getMetaNodePProfPort(), needWarnCount)
	}
}

func (ch *ClusterHost) checkDeadNodeStartStatus(deadNode *DeadNode, port string, needWarnCount int) {
	nodeStatus, err := checkNodeStartStatus(fmt.Sprintf("%v:%v", strings.Split(deadNode.Addr, ":")[0], port), 5)
	if err == nil && nodeStatus.Version != "" {
		//Version信息不为空时才认为是获取成功了
		deadNode.ProcessStatusCount++
		if deadNode.ProcessStatusCount >= needWarnCount {
			deadNode.IsNeedWarnBySpecialUMPKey = true
		}
	} else {
		deadNode.IsNeedWarnBySpecialUMPKey = true
	}
}

func (ch *ClusterHost) getDataNodePProfPort() (port string) {
	switch ch.host {
	case "id.chubaofs.jd.local", "th.chubaofs.jd.local":
		port = "17320"
	case "cn.chubaofs.jd.local", "cn.elasticdb.jd.local", "cn.chubaofs-seqwrite.jd.local", "idbbak.chubaofs.jd.local", "nl.chubaofs.jd.local", "nl.chubaofs.ochama.com":
		port = "6001"
	case "192.168.0.11:17010", "192.168.0.12:17010", "192.168.0.13:17010":
		port = "17320"
	default:
		port = "6001"
	}
	return
}

func getClusterName(domain string) (cluster string) {
	switch domain {
	case "nl.chubaofs.jd.local", "nl.chubaofs.ochama.com":
		cluster = "cfs_AMS_MCA"
	case "cn.chubaofs.jd.local", "sparkchubaofs.jd.local":
		cluster = "spark"
	case "cn.elasticdb.jd.local":
		cluster = "mysql"
	case "cn.chubaofs-seqwrite.jd.local":
		cluster = "cfs_dbBack"
	default:
		cluster = "unknown"
	}
	return
}

func (ch *ClusterHost) getFlashNodeProfPort() string {
	return "8001"
}

func (ch *ClusterHost) getMetaNodePProfPort() (port string) {
	switch ch.host {
	case "id.chubaofs.jd.local", "th.chubaofs.jd.local":
		port = "17220"
	case "cn.chubaofs.jd.local", "cn.elasticdb.jd.local", "cn.chubaofs-seqwrite.jd.local", "idbbak.chubaofs.jd.local", "nl.chubaofs.jd.local", "nl.chubaofs.ochama.com":
		port = "9092"
	case "192.168.0.11:17010", "192.168.0.12:17010", "192.168.0.13:17010":
		port = "17220"
	default:
		port = "9092"
	}
	return
}

func (ch *ClusterHost) doWarnInactiveNodesBySpecialUMPKey() {
	if time.Since(ch.lastTimeWarn) <= time.Minute*5 {
		return
	}
	inactiveDataNodeCount := 0
	inactiveMetaNodeCount := 0
	nodes := make([]string, 0)
	for _, deadNode := range ch.deadDataNodes {
		if deadNode.IsNeedWarnBySpecialUMPKey {
			nodes = append(nodes, deadNode.Addr)
			inactiveDataNodeCount++
		}
	}
	for _, deadNode := range ch.deadMetaNodes {
		if deadNode.IsNeedWarnBySpecialUMPKey {
			nodes = append(nodes, deadNode.Addr)
			inactiveMetaNodeCount++
		}
	}
	if inactiveDataNodeCount == 0 && inactiveMetaNodeCount == 0 {
		return
	}
	//如果有一台机器故障不告警, 两台需要判断是否是同一个机器
	//检查是不是机器异常，如果是机器异常才可以忽略
	if len(nodes) == 0 {
		return
	}
	if len(nodes) == 1 || len(nodes) == 2 && getIpFromIpPort(nodes[0]) == getIpFromIpPort(nodes[1]) {
		if isPhysicalMachineFailure(nodes[0]) {
			log.LogInfo(fmt.Sprintf("doWarnInactiveNodesBySpecialUMPKey isPhysicalMachineFailure ignore warn,cluster:%v, nodes:%v", ch.host, nodes))
			return
		}
		//一台机器，能连通的情况下，查询机器的启动时间，小于30分钟不进行告警
		nodeIp := getIpFromIpPort(nodes[0])
		totalStartupTime, err := GetNodeTotalStartupTime(nodeIp)
		if err == nil && totalStartupTime < MinUptimeThreshold {
			log.LogInfo(fmt.Sprintf("doWarnInactiveNodesBySpecialUMPKey cluster:%v,totalStartupTime:%v,ignore warn, nodes:%v", ch.host, totalStartupTime, nodes))
			return
		}
		if err != nil {
			deadNode, ok := ch.deadDataNodes[nodes[0]]
			if !ok {
				deadNode = ch.deadMetaNodes[nodes[0]]
			}
			if deadNode != nil && time.Since(deadNode.LastReportTime) < 45*time.Minute {
				log.LogErrorf("cluster:%v,GetNodeTotalStartupTime failed,ignore warn,totalStartupTime:%v,nodes:%v,err:%v", ch.host, totalStartupTime, nodes, err)
				return
			}
		}
	}
	if inactiveDataNodeCount <= 1 && inactiveMetaNodeCount <= 1 && ch.host != "cn.elasticdb.jd.local" {
		return
	}
	sb := new(strings.Builder)
	sb.WriteString(fmt.Sprintf("host:%v,", ch.host))
	if inactiveDataNodeCount != 0 {
		sb.WriteString(fmt.Sprintf("inactive DataNode总数:%v,", inactiveDataNodeCount))
	}
	if inactiveMetaNodeCount != 0 {
		sb.WriteString(fmt.Sprintf("inactive MetaNodes总数:%v,", inactiveMetaNodeCount))
	}
	sb.WriteString(fmt.Sprintf("详情:%v", nodes))
	ch.lastTimeWarn = time.Now()
	checktool.WarnBySpecialUmpKey(UMPCFSInactiveNodeWarnKey, sb.String())
}

// 获取 mdc 存入到数据库的日志 磁盘使用率 大于阈值 电话告警
func (cv *ClusterView) checkMetaNodeDiskStatByMDCInfoFromSre(host *ClusterHost, s *ChubaoFSMonitor) {
	if time.Since(host.metaNodeDiskRatioCheckTime) < time.Minute*5 {
		return
	}

	var (
		err             error
		dashboardMdcIps []string
		highRatioNodes  []string
	)
	defer func() {
		if err != nil {
			log.LogError(fmt.Sprintf("action[checkMetaNodeDiskStatByMDCInfoFromSre] err:%v", err))
		}
	}()
	if s.sreDB == nil {
		err = fmt.Errorf("sreDB is nil")
		return
	}
	/*select DISTINCT(ip) from tb_dashboard_mdc where origin='cfs' and (disk_path='/exportvolume' or disk_path='/export'
	or disk_path='/nvme') and fs_usage_percent > 70 and time_stamp >= now()-interval 20 minute;*/
	sqlStr := fmt.Sprintf(" select DISTINCT(ip) from `%s` where origin='cfs' and (disk_path='/exportvolume' or" +
		" disk_path='/export' or disk_path='/nvme') and fs_usage_percent > %v and time_stamp >= now()-interval 10 minute ",
		DashboardMdc{}.TableName(), s.metaNodeExportDiskUsedRatio)
	if err = s.sreDB.Raw(sqlStr).Scan(&dashboardMdcIps).Error; err != nil {
		return
	}

	log.LogInfo(fmt.Sprintf("action[checkMetaNodeDiskStatByMDCInfoFromSre] dashboardMdcIps:%v", dashboardMdcIps))
	nodeAddrMap := make(map[string]bool)
	for _, mn := range cv.MetaNodes {
		nodeAddrMap[strings.Split(mn.Addr, ":")[0]] = true
	}
	for _, ip := range dashboardMdcIps {
		if nodeAddrMap[ip] {
			highRatioNodes = append(highRatioNodes, ip)
		}
	}

	host.metaNodeDiskRatioCheckTime = time.Now()
	if len(highRatioNodes) == 0 {
		return
	}
	msg := fmt.Sprintf("%v has meta nodes export disk used ratio more than %v%%,detail:%v", host, s.metaNodeExportDiskUsedRatio, highRatioNodes)
	checktool.WarnBySpecialUmpKey(UMPKeyMetaNodeDiskRatio, msg)
}

func isSSD(host, zoneName string) bool {
	if host == "cn.elasticdb.jd.local" {
		return true
	}
	if host == "sparkchubaofs.jd.local" && strings.Contains(zoneName, "_ssd") {
		return true
	}
	return false
}

func getFlashNode(host *ClusterHost, addr string) (fn *FlashNodeView, err error) {
	reqURL := fmt.Sprintf("http://%v/flashNode/get?addr=%v", host, addr)
	data, err := doRequest(reqURL, host.isReleaseCluster)
	if err != nil {
		return
	}
	fn = &FlashNodeView{}
	if err = json.Unmarshal(data, fn); err != nil {
		log.LogErrorf("get getDataNode information from %v failed ,data:%v,err:%v", host, string(data), err)
		return
	}
	log.LogInfof("action[getDataNode],host[%v],addr[%v],reportTime[%v]",
		host, addr, fn.ReportTime)
	return
}

func offlineFlashNode(host *ClusterHost, addr string) {
	var reqURL string
	if host.isReleaseCluster {
		return
	} else {
		reqURL = fmt.Sprintf("http://%v/flashNode/decommission?addr=%v", host, addr)
	}
	data, err := doRequest(reqURL, host.isReleaseCluster)
	if err != nil {
		log.LogErrorf("action[offlineFlashNode] occurred err,url[%v],err %v", reqURL, err)
		return
	}
	msg := fmt.Sprintf("action[offlineFlashNode] reqURL[%v],data[%v]", reqURL, string(data))
	checktool.WarnBySpecialUmpKey(UMPCFSNormalWarnKey, msg)
	return
}
