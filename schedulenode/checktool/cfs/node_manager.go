package cfs

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/schedulenode/common/cfs"
	"github.com/cubefs/cubefs/sdk/http_client"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util/checktool"
	"github.com/cubefs/cubefs/util/log"
	"io/ioutil"
	"math"
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
					warnBySpecialUmpKeyWithPrefix(UMPCFSClusterConnRefused, msg)
					return
				}
				msg := fmt.Sprintf("get cluster info from %v failed,err:%v ", host.host, err)
				warnBySpecialUmpKeyWithPrefix(UMPCFSNormalWarnKey, msg)
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
			if !s.disableCleanExpiredMP {
				cv.cleanExpiredMetaPartitions(host, s.ExpiredMetaRemainDaysCfg)
			}
			if isProEnv() {
				cv.checkMetaNodeRaftLogBackupAlive(host)
			}
			host.warnInactiveNodesBySpecialUMPKey()
			log.LogInfof("checkNodesAlive [%v] end,cost[%v]", host, time.Since(startTime))
		}(host)
	}
	checkNodeWg.Wait()
}

// 检查metanode心跳卡顿情况，如果某个go协程持锁时间过长，可能导致心跳失败，这种情况需要单独报警
func (cv *ClusterView) checkMetaNodeStuckHeartbeat(host *ClusterHost, warn bool) {
	stuckNodes := make([]MetaNodeView, 0)
	deadNodes := make([]MetaNodeView, 0)
	maxCheckCount := math.MaxInt
	// TotalGB=0表示此时master可能正在切主，因此不需要检查所有节点是否卡顿，检查一部分即可
	if cv.MetaNodeStat.TotalGB == 0 {
		maxCheckCount = 16
	}
	for _, mn := range cv.MetaNodes {
		if mn.Status == false {
			if maxCheckCount == 0 {
				break
			}
			if (host.isReleaseCluster && isServerStartCompleted(mn.Addr)) || isServerAlreadyStart(mn.Addr, time.Minute*10) {
				stuckNodes = append(stuckNodes, mn)
			} else {
				deadNodes = append(deadNodes, mn)
			}
			maxCheckCount--
		}
	}
	if len(stuckNodes) == 0 {
		return
	}
	var masterLeaderChange string
	if cv.MetaNodeStat.TotalGB == 0 {
		masterLeaderChange = "master is changing leader,"
	}
	msg := fmt.Sprintf("%v %v has %v stuck heartbeat meta nodes, %v dead meta nodes, stuck nodes:%v, dead nodes:%v", host, masterLeaderChange, len(stuckNodes), len(deadNodes), stuckNodes, deadNodes)
	log.LogWarnf(msg)
	if len(stuckNodes) > 0 && warn {
		warnBySpecialUmpKeyWithPrefix(UMPKeyStuckNodes, msg)
	}
}

// confirmCheckMetaNodeAlive
// 1. 当master视图有非存活节点，则报警。
// 2. Master节点升级中常会误报，因此当非存活节点超过5个，需进行二次确认以避免误报
// 3. 同时为避免矫枉过正，当二次确认连续执行超过一定次数，则不再二次确认
// 4. 当token中还有令牌，则进行二次确认，如果确认为误报，则令牌减1，直到token为0。token每间隔半小时重置一次
// 5. token每间隔一定时间会重置
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
			if (host.isReleaseCluster && isServerStartCompleted(mn.Addr)) || isServerAlreadyStart(mn.Addr, time.Minute*2) {
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
	cv.checkMetaNodeStuckHeartbeat(host, true)
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
				if isDevEnv() || host.host == DomainSpark || host.host == DomainDbbak || host.host == DomainMysql {
					offlineMetaNode(host, inactiveMn.Addr)
					host.lastTimeOfflineMetaNode = time.Now()
					host.offlineMetaNodesIn24Hour[inactiveMn.Addr] = time.Now()
				}
			}
		}
	}
}

func (ch *ClusterHost) doProcessAlarm(nodes map[string]*DeadNode, msg string, nodeType int) {
	if len(nodes) == 0 {
		return
	}
	msg = msg + "{\n"
	for _, dd := range nodes {
		msg = msg + "  " + dd.String() + "\n"
	}
	msg = msg + "}"
	if isDevEnv() {
		warnBySpecialUmpKeyWithPrefix(UMPKeyInactiveNodes, msg)
	}

	// 荷兰CFS 存在故障节点就执行普通告警
	if ch.host == DomainNL && len(nodes) > 0 {
		warnBySpecialUmpKeyWithPrefix(UMPCFSNormalWarnKey, msg)
	}
	if len(nodes) >= defaultMaxInactiveNodes {
		warnBySpecialUmpKeyWithPrefix(UMPKeyInactiveNodes, msg)
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
	if time.Since(ch.lastTimeAlarmDP) < checktool.DefaultWarnInternal*time.Second {
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
			ips += addr + ","
		}
		ips = strings.TrimSuffix(ips, ",")
		msg := fmt.Sprintf("%v has %v inactive data nodes,ips[%v],dangerous data partitions[%v]", ch.host, len(nodes), ips, dangerDps)
		warnBySpecialUmpKeyWithPrefix(UMPKeyInactiveNodes, msg)
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
			warnBySpecialUmpKeyWithPrefix(UMPCFSSparkFlashNodeVersionKey, fmt.Sprintf("flashnode[%v] invalid version[%v], expect[%v], has been automatically disabled", fn.Addr, version.CommitID, expectVersion))
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

// confirmCheckDataNodeAlive
// 1. 当master视图有非存活节点，则报警。
// 2. Master节点升级中常会误报，因此当非存活节点超过5个，需进行二次确认以避免误报
// 3. 同时为避免矫枉过正，当二次确认连续执行超过一定次数，则不再二次确认
// 4. 当token中还有令牌，则进行二次确认，如果确认为误报，则令牌减1，直到token为0。token每间隔半小时重置一次
// 5. token每间隔一定时间会重置
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
			if (host.isReleaseCluster && isServerStartCompleted(dn.Addr)) || isServerAlreadyStart(dn.Addr, time.Minute*10) {
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

// checkDataNodeAlive
// autoOfflineThreshold - 一般不大于8小时
func (cv *ClusterView) checkDataNodeAlive(host *ClusterHost, s *ChubaoFSMonitor) {
	var (
		nodeZoneMap map[string]string
		zoneName    string
		err         error
	)
	inactiveDataNodes := cv.confirmCheckDataNodeAlive(host, true)
	if len(inactiveDataNodes) == 0 {
		return
	}
	// 只自动处理单节点故障场景
	if len(inactiveDataNodes) != 1 {
		return
	}
	var dn *DeadNode
	for _, deadNode := range inactiveDataNodes {
		dn = deadNode
		break
	}

	// 确认是否为物理机故障,如果故障恢复，应删除正在下线节点
	if !isPhysicalMachineFailure(dn.Addr) {
		return
	}

	if !host.isReleaseCluster {
		nodeZoneMap, err = getNodeToZoneMap(host)
		if err != nil {
			log.LogErrorf("action[checkDataNodeAlive] Domain[%v] err[%v]", host.host, err)
			return
		}
		zoneName = nodeZoneMap[dn.Addr]
	}

	// 确认故障持续时间是否达到下线阈值 - autoOfflineThreshold
	dataNodeView, err := getDataNode(host, dn.Addr)
	if err != nil {
		log.LogErrorf("action[checkDataNodeAlive] host[%v] err[%v]", host.host, err)
		return
	}
	offlineThreshold := getOfflineThreshold(s, isSSD(host.host, zoneName))
	log.LogInfof("action[checkDataNodeAlive] host[%v] addr[%v] zone[%v] inOfflineDataNodes[%v] autoOfflineThreshold[%v] offline pool size[%v]", host.host, dn.Addr, zoneName, len(host.inOfflineDataNodes), offlineThreshold, host.offlineDataNodeTokenPool.getSize())
	if time.Since(dataNodeView.ReportTime) < offlineThreshold {
		return
	}

	// 符合24小时内下线限制，并且且为新ip，则加入待下线DataNode Map
	if _, ok := host.inOfflineDataNodes[dataNodeView.Addr]; !ok {
		if len(host.inOfflineDataNodes) < host.offlineDataNodeTokenPool.getSize() && host.offlineDataNodeTokenPool.allow() {
			host.inOfflineDataNodes[dataNodeView.Addr] = dataNodeView.ReportTime
			log.LogDebugf("action[checkDataNodeAlive] inOfflineDataNodes:%v %v", len(host.inOfflineDataNodes), host.inOfflineDataNodes)
		} else {
			log.LogWarnf("action[checkDataNodeAlive] offline nodes[%v] in %v minutes reached max[%v]", len(host.inOfflineDataNodes), host.offlineDataNodeTokenPool.interval.Minutes(), host.offlineDataNodeTokenPool.getSize())
		}
	}

	// 变更时间：2024年10月21日
	// 变更事项：对故障DataNode只下线磁盘，不再用node下线，改为disk下线
	// 变更原因：ssd磁盘在宕机1小时后自动下线节点，导致修复暴涨，修复写造成了数据库集群网卡流入打满
	offlineBadDataNodeByDisk(s, host)
	return
}

// canOffline
// 避免正在下线的DP数量过多
func canOffline(host *ClusterHost) bool {
	maxBadPartition := maxBadDataPartitionsCount
	if host.host == DomainMysql {
		maxBadPartition = maxBadDataPartitionsCountMysql
	}
	badDPsCount, err := getBadPartitionIDsCount(host)
	if err != nil || badDPsCount > maxBadPartition {
		log.LogWarnf("action[canOffline] can not offline, host:%v badDPsCount:%v err:%v", host, badDPsCount, err)
		return false
	}
	return true
}

// offlineBadDatanodeByDisk
// 每间隔一定时间检查一次，如果满足下线条件，则下线1块磁盘
func offlineBadDataNodeByDisk(s *ChubaoFSMonitor, host *ClusterHost) {
	var (
		diskOfflineInterval  time.Duration
		diskPaths            []string
		nodeZoneMap          map[string]string
		autoOfflineThreshold time.Duration
		err                  error
	)

	if !host.isReleaseCluster {
		nodeZoneMap, err = getNodeToZoneMap(host)
		if err != nil {
			log.LogErrorf("action[offlineBadDataNodeByDisk] Domain[%v] err[%v]", host.host, err)
			return
		}
	}

	for dataNodeAddr, lastOfflineDiskTime := range host.inOfflineDataNodes {
		// mysql集群禁止自动下线，先电话通知，手动下线，等下线方案成熟后再改为自动下线
		if host.host == DomainMysql {
			warnBySpecialUmpKeyWithPrefix(UMPCFSMysqlInactiveNodeKey, fmt.Sprintf("Domain[%v] mysql inactive node[%v], please offline by tool", host.host, dataNodeAddr))
			continue
		}
		if host.host == DomainNL || host.host == DomainOchama {
			warnBySpecialUmpKeyWithPrefix(UMPCFSNLInactiveNodeKey, fmt.Sprintf("Domain[%v] inactive node[%v], please check", host.host, dataNodeAddr))
			continue
		}
		zoneName := nodeZoneMap[dataNodeAddr]
		// 下线最小时间间隔，ssd - 5分钟，hdd - 10分钟
		ssd := isSSD(host.host, zoneName)
		diskOfflineInterval = getOfflineInterval(s, ssd)
		autoOfflineThreshold = getOfflineThreshold(s, ssd)
		log.LogInfof("action[offlineBadDataNodeByDisk] host[%v] addr[%v] zone[%v] lastOfflineDiskTime[%v] diskOfflineInterval[%v] autoOfflineThreshold[%v]", host.host, dataNodeAddr, zoneName, lastOfflineDiskTime, diskOfflineInterval, autoOfflineThreshold)
		if time.Since(lastOfflineDiskTime) < diskOfflineInterval {
			log.LogWarnf("action[offlineBadDataNodeByDisk] host[%v] addr[%v] zone[%v] lastOfflineDiskTime[%v] sinceLastOffline[%v] diskOfflineInterval[%v] skip", host.host, dataNodeAddr, zoneName, lastOfflineDiskTime, time.Since(lastOfflineDiskTime), diskOfflineInterval)
			continue
		}
		// 再次确认是否为物理机故障
		if !isPhysicalMachineFailure(dataNodeAddr) {
			delete(host.inOfflineDataNodes, dataNodeAddr)
			continue
		}

		// 再次确认故障持续时间是否达到下线阈值 - autoOfflineThreshold
		var dataNodeView *DataNodeView
		dataNodeView, err = getDataNode(host, dataNodeAddr)
		if err != nil {
			log.LogErrorf("action[offlineBadDataNodeByDisk] Domain[%v] getDataNode failed, err:%v", host.host, err)
			continue
		}
		dataNodeReportTime := dataNodeView.ReportTime
		if time.Since(dataNodeReportTime) < autoOfflineThreshold {
			delete(host.inOfflineDataNodes, dataNodeAddr)
			continue
		}

		// 获取要被下线的磁盘地址 根据时间计算
		offlineDiskIndex := int((time.Since(dataNodeReportTime) - autoOfflineThreshold) / diskOfflineInterval)

		diskPaths = getDisks(host, dataNodeView)
		diskPath := diskPaths[offlineDiskIndex%len(diskPaths)]

		// 确认集群中正在下线dp的个数，判断是否可继续下线
		if !canOffline(host) {
			continue
		}
		offlineDataNodeDisk(host, dataNodeAddr, diskPath, false)
		host.inOfflineDataNodes[dataNodeAddr] = time.Now()
	}
}

func getOfflineThreshold(s *ChubaoFSMonitor, ssd bool) (autoOfflineThreshold time.Duration) {
	if ssd {
		if s.integerMap[cfgKeySSDDiskOfflineThreshold] > 0 {
			autoOfflineThreshold = time.Duration(s.integerMap[cfgKeySSDDiskOfflineThreshold]) * time.Minute
		} else {
			autoOfflineThreshold = 60 * time.Minute
		}
	} else {
		if s.integerMap[cfgKeyHDDDiskOfflineThreshold] > 0 {
			autoOfflineThreshold = time.Duration(s.integerMap[cfgKeyHDDDiskOfflineThreshold]) * time.Minute
		} else {
			autoOfflineThreshold = 300 * time.Minute
		}
	}
	return
}

func getOfflineInterval(s *ChubaoFSMonitor, ssd bool) (diskOfflineInterval time.Duration) {
	if ssd {
		if s.integerMap[cfgKeySSDDiskOfflineInterval] > 0 {
			diskOfflineInterval = time.Duration(s.integerMap[cfgKeySSDDiskOfflineInterval]) * time.Minute
		} else {
			diskOfflineInterval = 5 * time.Minute
		}
	} else {
		if s.integerMap[cfgKeyHDDDiskOfflineInterval] > 0 {
			diskOfflineInterval = time.Duration(s.integerMap[cfgKeyHDDDiskOfflineInterval]) * time.Minute
		} else {
			diskOfflineInterval = 20 * time.Minute
		}
	}
	return
}

func getDisks(host *ClusterHost, dataNodeView *DataNodeView) []string {
	dbbakDiskPaths1 := []string{
		0:  "/cfsd0",
		1:  "/cfsd6",
		2:  "/cfsd1",
		3:  "/cfsd7",
		4:  "/cfsd2",
		5:  "/cfsd8",
		6:  "/cfsd3",
		7:  "/cfsd9",
		8:  "/cfsd4",
		9:  "/cfsd10",
		10: "/cfsd5",
		11: "/cfsd11",
	}

	dbbakDiskPaths2 := []string{
		0:  "/data0",
		1:  "/data12",
		2:  "/data1",
		3:  "/data13",
		4:  "/data2",
		5:  "/data14",
		6:  "/data3",
		7:  "/data15",
		8:  "/data4",
		9:  "/data16",
		10: "/data5",
		11: "/data17",
		12: "/data6",
		13: "/data18",
		14: "/data7",
		15: "/data19",
		16: "/data8",
		17: "/data20",
		18: "/data9",
		19: "/data21",
		20: "/data10",
		21: "/data22",
		22: "/data11",
	}

	sparkDiskPaths := []string{
		0:  "/data0",
		1:  "/data8",
		2:  "/data1",
		3:  "/data9",
		4:  "/data2",
		5:  "/data10",
		6:  "/data3",
		7:  "/data11",
		8:  "/data4",
		9:  "/data12",
		10: "/data5",
		11: "/data13",
		12: "/data6",
		13: "/data14",
		14: "/data7",
		15: "/data15",
	}
	// 随机写集群, 从diskInfo获取实时磁盘列表
	if !host.isReleaseCluster {
		if len(dataNodeView.DiskInfos) == 0 {
			return sparkDiskPaths
		}
		disks := make([]string, 0)
		for disk := range dataNodeView.DiskInfos {
			disks = append(disks, disk)
		}
		sort.Slice(disks, func(i, j int) bool {
			return disks[i] < disks[j]
		})
		return disks
	}

	//顺序写集群
	var diskPath string
	var maxRetry = 10
	for _, dpID := range dataNodeView.PersistenceDataPartitions {
		maxRetry--
		if maxRetry == 0 {
			break
		}
		partition, err := cfs.GetDataPartition(host.host, dpID, host.isReleaseCluster)
		if err != nil {
			continue
		}
		for _, replica := range partition.Replicas {
			if replica.Addr == dataNodeView.Addr && replica.DiskPath != "" {
				diskPath = replica.DiskPath
				break
			}
		}
		if diskPath != "" {
			break
		}
	}
	if diskPath != "" && strings.Contains(diskPath, "data") {
		return dbbakDiskPaths2
	}
	return dbbakDiskPaths1
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
	return doRequestWithTimeout(reqUrl, isReleaseCluster, time.Minute*5)
}

func doRequestWithTimeout(reqUrl string, isReleaseCluster bool, timeout time.Duration) (data []byte, err error) {
	var resp *http.Response
	client := http.Client{Timeout: timeout}
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
				warnBySpecialUmpKeyWithPrefix(UMPCFSNodeSetNumKey, msg)
				badNodeSets = append(badNodeSets, dataNodeSets[0].SetID)
			}
		}
		if len(metaNodeSets) >= 2 {
			sort.Slice(metaNodeSets, func(i, j int) bool {
				return metaNodeSets[i].Num < metaNodeSets[j].Num
			})
			if metaNodeSets[0].Num < metaNodeSets[1].Num*50/100 || metaNodeSets[1].Num-metaNodeSets[0].Num > minMetaNodeSetDiff {
				msg := fmt.Sprintf("Domain[%v] zone[%v] meta nodeset[%v] too small[%v]", domain, zoneView.Name, metaNodeSets[0].SetID, metaNodeSets[0].Num)
				warnBySpecialUmpKeyWithPrefix(UMPCFSNodeSetNumKey, msg)
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
	warnBySpecialUmpKeyWithPrefix(UMPCFSNormalWarnKey, msg)
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
	warnBySpecialUmpKeyWithPrefix(UMPCFSNormalWarnKey, msg)
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
	warnBySpecialUmpKeyWithPrefix(UMPCFSNormalWarnKey, msg)
	return
}

func (cv *ClusterView) checkMetaNodeDiskStat(host *ClusterHost, diskMinWarnSize int) {
	// exclude hosts which have not update the meta node disk stat API
	if host.isReleaseCluster {
		return
	}
	// set meta node port
	checkedCount := 0
	diskWarnNodes := make([]string, 0)
	for _, mn := range cv.MetaNodes {
		if mn.Status == false {
			continue
		}
		ipPort := strings.Split(mn.Addr, ":")
		isNeedTelAlarm, err := doCheckMetaNodeDiskStat(ipPort[0], profPortMap[ipPort[1]], host.isReleaseCluster, diskMinWarnSize)
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
		warnBySpecialUmpKeyWithPrefix(UMPKeyMetaNodeDiskSpace, msg)
		host.metaNodeDiskUsedWarnTime = time.Now()
	} else {
		log.LogWarnf("action[checkMetaNodeDiskStat] :%v", msg)
	}
}

var (
	excludeCheckMetaNodeRaftLogBackupHosts = []string{DomainOchama, DomainNL}
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
			log.LogDebugf("action[checkMetaNodeRaftLogBackupAlive] host[%v] addr[%v] doCheckMetaNodeDiskStat err[%v]", host, metaNode.Addr, err)
		}
		if isNeedAlarm {
			raftLogBackupWarnMetaNodes = append(raftLogBackupWarnMetaNodes, metaNode.Addr)
		}
	}
	if len(raftLogBackupWarnMetaNodes) == 0 {
		return
	}
	msg := fmt.Sprintf("checkMetaNodeRaftLogBackupAlive: host[%v], fault count[%v]", host.host, len(raftLogBackupWarnMetaNodes))
	warnBySpecialUmpKeyWithPrefix(UMPCFSRaftlogBackWarnKey, msg)
}

func (cv *ClusterView) checkMetaNodeFailedMetaPartitions(host *ClusterHost) {
	for _, metaNode := range cv.MetaNodes {
		ipPort := strings.Split(metaNode.Addr, ":")
		failedMpArr, err := doGetFailedMetaPartitions(ipPort[0], profPortMap[ipPort[1]], host.isReleaseCluster)
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
	if time.Since(host.lastCleanExpiredMetaTime) < time.Hour*4 {
		return
	}

	host.lastCleanExpiredMetaTime = time.Now()
	for _, metaNode := range cv.MetaNodes {
		ipPort := strings.Split(metaNode.Addr, ":")
		_, err := doCleanExpiredMetaPartitions(ipPort[0], profPortMap[ipPort[1]], days, host.isReleaseCluster)
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
		ch.checkDeadNodeStartStatus(deadNode, needWarnCount)
	}
	for _, deadNode := range ch.deadMetaNodes {
		ch.checkDeadNodeStartStatus(deadNode, needWarnCount)
	}
}

func (ch *ClusterHost) checkDeadNodeStartStatus(deadNode *DeadNode, needWarnCount int) {
	ipPort := strings.Split(deadNode.Addr, ":")
	nodeStatus, err := checkNodeStartStatus(fmt.Sprintf("%v:%v", ipPort[0], profPortMap[ipPort[1]]), 5)
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

func getClusterName(domain string) (cluster string) {
	switch domain {
	case DomainNL, DomainOchama:
		cluster = ClusterNameAMS
	case DomainSpark:
		cluster = ClusterNameSpark
	case DomainMysql:
		cluster = ClusterNameMysql
	case DomainDbbak:
		cluster = ClusterNameDbbak
	default:
		cluster = "unknown"
	}
	return
}

func (ch *ClusterHost) getFlashNodeProfPort() string {
	return "8001"
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
	//如果inactive为1并且这台机器是物理故障则不告警（注意，相同ip的meta和data故障时，inactive可能为2）
	if len(nodes) == 0 {
		return
	}
	if len(nodes) == 1 || len(nodes) == 2 && getIpFromIpPort(nodes[0]) == getIpFromIpPort(nodes[1]) {
		if isPhysicalMachineFailure(nodes[0]) {
			log.LogInfo(fmt.Sprintf("doWarnInactiveNodesBySpecialUMPKey isPhysicalMachineFailure ignore warn,cluster:%v, nodes:%v", ch.host, nodes))
			return
		}
		//一台机器，能连通的情况下，查询机器的启动时间，小于30分钟不进行告警。（为什么？因为旧版本有异步加载，启动慢？）
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
	if inactiveDataNodeCount <= 1 && inactiveMetaNodeCount <= 1 && ch.host != DomainMysql {
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
	warnBySpecialUmpKeyWithPrefix(UMPCFSInactiveNodeWarnKey, sb.String())
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
	sqlStr := fmt.Sprintf(" select DISTINCT(ip) from `%s` where origin='cfs' and (disk_path='/exportvolume' or"+
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
	warnBySpecialUmpKeyWithPrefix(UMPKeyMetaNodeDiskRatio, msg)
}

// isSSD 此方法只用于区分线上spark，mysql，dbBack集群的机房介质信息
func isSSD(host, zoneName string) bool {
	if host == DomainMysql {
		return true
	}
	if host == DomainSpark && (strings.Contains(zoneName, "_ssd") || strings.Contains(zoneName, "_sfx")) {
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
	warnBySpecialUmpKeyWithPrefix(UMPCFSNormalWarnKey, msg)
	return
}
