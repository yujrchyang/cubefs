package cluster

import (
	"fmt"
	"github.com/cubefs/cubefs/console/model"
	cproto "github.com/cubefs/cubefs/console/proto"
	api "github.com/cubefs/cubefs/console/service/apiManager"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/unit"
	"math"
	"strings"
	"time"
)

const (
	writabledDataSpace = 100 * unit.GB
)

func FormatClusterSummaryView(api *api.APIManager, cluster *model.ConsoleCluster) *model.ClusterInfo {
	result := &model.ClusterInfo{
		ClusterName:   cluster.ClusterName,
		ClusterNameZH: cluster.ClusterNameZH,
	}
	if cluster.IsRelease {
		cv, err := api.GetClusterViewCacheRelease(cluster.ClusterName, false)
		if err != nil {
			log.LogWarnf("FormatClusterSummaryView: GetClusterViewCacheRelease failed: err(%v)", err)
			return result
		}
		result.VolNum = len(cv.VolStat)
		result.TotalTB = math.Trunc(float64(cv.DataNodeStat.TotalGB)/float64(1024)*100) / 100
		result.UsedTB = math.Trunc(float64(cv.DataNodeStat.UsedGB)/float64(1024)*100) / 100
		result.IncreaseTB = math.Trunc(float64(cv.DataNodeStat.IncreaseGB)/float64(1024)*100) / 100
	} else {
		cv, err := api.GetClusterViewCache(cluster.ClusterName, false)
		if err != nil {
			log.LogWarnf("FormatClusterSummaryView: GetClusterViewCacheRelease failed: err(%v)", err)
			return result
		}
		result.VolNum = cv.VolCount
		result.TotalTB = math.Trunc(float64(cv.DataNodeStatInfo.TotalGB)/float64(1024)*100) / 100
		result.UsedTB = math.Trunc(float64(cv.DataNodeStatInfo.UsedGB)/float64(1024)*100) / 100
		result.IncreaseTB = math.Trunc(float64(cv.DataNodeStatInfo.IncreasedGB)/float64(1024)*100) / 100
	}
	result.StateLevel = model.GetClusterHealthLevel(cluster.ClusterName)
	return result
}

func FormatConsoleClusterView(api *api.APIManager, cluster *model.ConsoleCluster) *cproto.ConsoleClusterView {
	var ccv *cproto.ConsoleClusterView
	if cluster.IsRelease {
		cv, err := api.GetClusterViewCacheRelease(cluster.ClusterName, false)
		if err != nil {
			log.LogWarnf("FormatConsoleClusterView: GetClusterViewCacheRelease failed: err(%v)", err)
			return nil
		}
		ccv = &cproto.ConsoleClusterView{
			VolumeCount:   int32(len(cv.VolStat)),
			MasterCount:   int32(len(strings.Split(api.GetClusterInfo(cluster.ClusterName).MasterAddrs, ","))),
			MetaNodeCount: int32(len(cv.MetaNodes)),
			DataNodeCount: int32(len(cv.DataNodes)),
			// 需要按vol 统计 或者接口请求
			DataPartitionCount: 0,
			MetaPartitionCount: 0,
			MaxMetaPartitionID: cv.MaxMetaPartitionID,
			MaxDataPartitionID: cv.MaxDataPartitionID,
			MaxMetaNodeID:      cv.MaxMetaNodeID,
			DataNodeBadDisks:   cv.DataNodeBadDisks,
			BadPartitionIDs:    cv.BadPartitionIDs,
			DataNodeStatInfo:   cv.DataNodeStat,
			MetaNodeStatInfo:   cv.MetaNodeStat,
		}
		return ccv
	} else {
		cv, err := api.GetClusterViewCache(cluster.ClusterName, false)
		if err != nil {
			log.LogWarnf("FormatConsoleClusterView: GetClusterViewCache failed: err(%v)", err)
			return nil
		}
		badDisks := make([]cproto.DataNodeBadDisksView, 0, len(cv.DataNodeBadDisks))
		for _, badDisk := range cv.DataNodeBadDisks {
			view := cproto.DataNodeBadDisksView{
				Addr:        badDisk.Addr,
				BadDiskPath: badDisk.BadDiskPath,
			}
			badDisks = append(badDisks, view)
		}
		badPartitions := make([]cproto.BadPartitionView, 0, len(cv.BadPartitionIDs))
		for _, badPartition := range cv.BadPartitionIDs {
			view := cproto.BadPartitionView{
				Path:        badPartition.Path,
				PartitionID: badPartition.PartitionID,
			}
			badPartitions = append(badPartitions, view)
		}

		ccv = &cproto.ConsoleClusterView{
			VolumeCount:        int32(cv.VolCount),
			MasterCount:        int32(len(strings.Split(api.GetClusterInfo(cluster.ClusterName).MasterAddrs, ","))),
			MetaNodeCount:      int32(len(cv.MetaNodes)),
			DataNodeCount:      int32(len(cv.DataNodes)),
			DataPartitionCount: 0,
			MetaPartitionCount: 0,
			MaxMetaPartitionID: cv.MaxMetaPartitionID,
			MaxDataPartitionID: cv.MaxDataPartitionID,
			MaxMetaNodeID:      cv.MaxMetaNodeID,
			DataNodeBadDisks:   badDisks,
			BadPartitionIDs:    badPartitions,
			ClientPkgAddr:      cv.ClientPkgAddr,
			DataNodeStatInfo: &cproto.DataNodeSpaceStat{
				TotalGB:    cv.DataNodeStatInfo.TotalGB,
				UsedGB:     cv.DataNodeStatInfo.UsedGB,
				IncreaseGB: cv.DataNodeStatInfo.IncreasedGB,
				UsedRatio:  cv.DataNodeStatInfo.UsedRatio,
			},
			MetaNodeStatInfo: &cproto.MetaNodeSpaceStat{
				TotalGB:    cv.MetaNodeStatInfo.TotalGB,
				UsedGB:     cv.MetaNodeStatInfo.UsedGB,
				IncreaseGB: cv.MetaNodeStatInfo.IncreasedGB,
				UsedRatio:  cv.MetaNodeStatInfo.UsedRatio,
			},
		}
		return ccv
	}
}

func FormatDataNodeView(nodeInfo interface{}, cluster string, rdmaConf *cproto.RDMAConfInfo) *cproto.NodeViewDetail {
	if cproto.IsRelease(cluster) {
		// 磁盘下线 手动输入磁盘路径
		info := nodeInfo.(*cproto.DataNode)
		detail := &cproto.NodeViewDetail{
			//ID:             info.ID,
			Addr:           info.Addr,
			ZoneName:       info.RackName,
			IsActive:       info.IsActive,
			Total:          FormatSize(info.Total),
			Used:           FormatSize(info.Used),
			Available:      FormatSize(info.Available),
			UsageRatio:     FormatRatio(info.Ratio),
			PartitionCount: info.DataPartitionCount,
			IsRDMA:         false,
			ReportTime:     formatTime(info.ReportTime),
		}
		detail.IsWritable = !info.ToBeOffline && info.Ratio <= 0.95 && info.Available > 0
		return detail
	} else {
		info := nodeInfo.(*proto.DataNodeInfo)
		detail := &cproto.NodeViewDetail{
			ID:             info.ID,
			Addr:           info.Addr,
			ZoneName:       info.ZoneName,
			IsActive:       info.IsActive,
			Total:          FormatSize(info.Total),
			Used:           FormatSize(info.Used),
			Available:      FormatSize(info.AvailableSpace),
			UsageRatio:     FormatRatio(info.UsageRatio),
			PartitionCount: info.DataPartitionCount,
			ReportTime:     formatTime(info.ReportTime),
		}
		//todo: 直接用上报的diskInfo
		detail.IsWritable = info.IsActive && !info.ToBeOffline && !info.ToBeMigrated && info.UsageRatio <= 0.95 && info.AvailableSpace > 0
		diskMap := make(map[string]bool)
		for _, rep := range info.DataPartitionReports {
			diskMap[rep.DiskPath] = true
		}
		for _, badDisk := range info.BadDisks {
			diskMap[badDisk] = true
		}
		disks := make([]string, 0, len(diskMap))
		for key := range diskMap {
			disks = append(disks, key)
		}
		detail.Disk = disks
		if rdmaConf != nil {
			var rdmaNodeMap = make(map[string]*cproto.NodeRDMAConf)
			for _, rdmaNode := range rdmaConf.RDMANodeMap {
				rdmaNodeMap[rdmaNode.Addr] = rdmaNode
			}
			if nodeRDMAInfo, ok := rdmaNodeMap[info.Addr]; ok {
				detail.IsRDMA = true
				detail.Pod = nodeRDMAInfo.Pod
				detail.NodeRDMAService = nodeRDMAInfo.RDMAService
				detail.NodeRDMASend = nodeRDMAInfo.RDMASend
				detail.NodeRDMARecv = nodeRDMAInfo.RDMARecv
			}
		}
		return detail
	}
}

func FormatMetaNodeView(nodeInfo interface{}, cluster string, rdmaConf *cproto.RDMAConfInfo) *cproto.NodeViewDetail {
	if cproto.IsRelease(cluster) {
		info := nodeInfo.(*cproto.MetaNode)
		detail := &cproto.NodeViewDetail{
			ID:             info.ID,
			Addr:           info.Addr,
			ZoneName:       info.RackName,
			IsActive:       info.IsActive,
			Total:          FormatSize(info.Total),
			Used:           FormatSize(info.Used),
			Available:      FormatSize(info.MaxMemAvailWeight),
			UsageRatio:     FormatRatio(info.Ratio),
			PartitionCount: info.MetaPartitionCount,
			IsRDMA:         false,
			ReportTime:     formatTime(info.ReportTime),
		}
		detail.IsWritable = float32(float64(info.Used)/float64(info.Total)) < info.Threshold
		return detail
	} else {
		info := nodeInfo.(*proto.MetaNodeInfo)
		detail := &cproto.NodeViewDetail{
			ID:             info.ID,
			Addr:           info.Addr,
			ZoneName:       info.ZoneName,
			IsActive:       info.IsActive,
			Total:          FormatSize(info.Total),
			Used:           FormatSize(info.Used),
			Available:      FormatSize(info.MaxMemAvailWeight),
			UsageRatio:     FormatRatio(info.Ratio),
			PartitionCount: uint32(info.MetaPartitionCount),
			ReportTime:     formatTime(info.ReportTime),
		}
		detail.IsWritable = float32(float64(info.Used)/float64(info.Total)) < info.Threshold
		if rdmaConf != nil {
			var rdmaNodeMap map[string]*cproto.NodeRDMAConf
			for _, rdmaNode := range rdmaConf.RDMANodeMap {
				rdmaNodeMap[rdmaNode.Addr] = rdmaNode
			}
			if nodeRDMAInfo, ok := rdmaNodeMap[info.Addr]; ok {
				detail.IsRDMA = true
				detail.Pod = nodeRDMAInfo.Pod
				detail.NodeRDMAService = nodeRDMAInfo.RDMAService
				detail.NodeRDMASend = nodeRDMAInfo.RDMASend
				detail.NodeRDMARecv = nodeRDMAInfo.RDMARecv
			}
		}
		return detail
	}
}

func FormatMetaPartitionView(partition interface{}, cluster string) *cproto.PartitionViewDetail {
	if cproto.IsRelease(cluster) {
		mp := partition.(*cproto.MetaPartition)
		view := &cproto.PartitionViewDetail{
			PartitionID: mp.PartitionID,
			VolName:     mp.VolName,
			Start:       mp.Start,
			End:         mp.End,
			InodeCount:  mp.InodeCount,
			DentryCount: mp.DentryCount,
			MaxExistIno: mp.MaxInodeID,
			Status:      formatPartitionStatus(mp.Status),
			IsRecover:   mp.IsRecover,
			Peers:       mp.Peers,
			Hosts:       FormatHost(mp.PersistenceHosts, make([]string, len(mp.PersistenceHosts))),
		}
		replicas := make([]*cproto.ReplicaViewDetail, 0, len(mp.Replicas))
		for _, replica := range mp.Replicas {
			info := &cproto.ReplicaViewDetail{
				Addr:        replica.Addr,
				Status:      formatPartitionStatus(replica.Status),
				IsLeader:    replica.IsLeader,
				Role:        formatReplicaRole(replica.IsLeader, false),
				ReportTime:  FormatTimestamp(replica.ReportTime),
				MaxInodeID:  replica.MaxInodeID,
				InodeCount:  replica.InodeCount,
				DentryCount: replica.DentryCount,
			}
			for _, loadResp := range mp.LoadResponse {
				if loadResp.Addr == replica.Addr {
					info.ApplyId = loadResp.ApplyID
				}
			}
			replicas = append(replicas, info)
		}
		view.Replicas = replicas
		view.ReplicaNum = int(mp.ReplicaNum)
		return view
	} else {
		mp := partition.(*proto.MetaPartitionInfo)
		view := &cproto.PartitionViewDetail{
			PartitionID:  mp.PartitionID,
			VolName:      mp.VolName,
			Start:        mp.Start,
			End:          mp.End,
			InodeCount:   mp.InodeCount,
			DentryCount:  mp.DentryCount,
			MaxExistIno:  mp.MaxExistIno,
			Status:       formatPartitionStatus(mp.Status),
			IsRecover:    mp.IsRecover,
			CreateTime:   FormatTimestamp(mp.CreateTime),
			Peers:        mp.Peers,
			Learners:     formatLearner(mp.Learners),
			Hosts:        FormatHost(mp.Hosts, mp.Zones),
			MissingNodes: formatMissInfo(mp.MissNodes, nil),
		}
		replicas := make([]*cproto.ReplicaViewDetail, 0, len(mp.Replicas))
		for _, replica := range mp.Replicas {
			info := &cproto.ReplicaViewDetail{
				Addr:       replica.Addr,
				Role:       formatReplicaRole(replica.IsLeader, replica.IsLearner),
				Status:     formatPartitionStatus(replica.Status),
				IsLeader:   replica.IsLeader,
				ReportTime: FormatTimestamp(replica.ReportTime),
				StoreMode:  formatStoreMode(uint8(replica.StoreMode)),
				IsLearner:  replica.IsLearner,
				IsRecover:  replica.IsRecover,
				ApplyId:    replica.ApplyId,
			}
			replicas = append(replicas, info)
		}
		view.Replicas = replicas
		view.ReplicaNum = int(mp.ReplicaNum)
		return view
	}
}

func FormatDataPartitionView(partition interface{}, cluster string) *cproto.PartitionViewDetail {
	if cproto.IsRelease(cluster) {
		dp := partition.(*cproto.DataPartition)
		view := &cproto.PartitionViewDetail{
			PartitionID:             dp.PartitionID,
			VolName:                 dp.VolName,
			Status:                  formatPartitionStatus(dp.Status),
			IsRecover:               dp.IsRecover,
			IsManual:                dp.IsManual,
			LastLoadTime:            FormatTimestamp(dp.LastLoadTime),
			Hosts:                   FormatHost(dp.PersistenceHosts, make([]string, len(dp.PersistenceHosts))),
			MissingNodes:            formatMissInfo(dp.MissNodes, nil),
			FilesWithMissingReplica: formatMissInfo(nil, dp.FileMissReplica),
		}
		replicas := make([]*cproto.ReplicaViewDetail, 0, len(dp.Replicas))
		for _, replica := range dp.Replicas {
			info := &cproto.ReplicaViewDetail{
				Addr:        replica.Addr,
				Role:        "-", // todo: 主cong
				Status:      formatPartitionStatus(replica.Status),
				ReportTime:  FormatTimestamp(replica.ReportTime),
				FileCount:   replica.FileCount,
				Total:       FormatSize(replica.Total),
				Used:        FormatSize(replica.Used),
				NeedCompare: replica.NeedCompare,
				DiskPath:    replica.DiskPath,
			}
			replicas = append(replicas, info)
		}
		view.Replicas = replicas
		view.ReplicaNum = int(dp.ReplicaNum)
		return view
	} else {
		dp := partition.(*proto.DataPartitionInfo)
		view := &cproto.PartitionViewDetail{
			PartitionID:             dp.PartitionID,
			VolName:                 dp.VolName,
			Status:                  formatPartitionStatus(dp.Status),
			IsRecover:               dp.IsRecover,
			IsManual:                dp.IsManual,
			EcMigrateStatus:         EcStatusMap[dp.EcMigrateStatus],
			CreateTime:              FormatTimestamp(dp.CreateTime),
			LastLoadTime:            FormatTimestamp(dp.LastLoadedTime),
			Peers:                   dp.Peers,
			Learners:                formatLearner(dp.Learners),
			Hosts:                   FormatHost(dp.Hosts, dp.Zones),
			MissingNodes:            formatMissInfo(dp.MissingNodes, nil),
			FilesWithMissingReplica: formatMissInfo(nil, dp.FilesWithMissingReplica),
		}
		replicas := make([]*cproto.ReplicaViewDetail, 0, len(dp.Replicas))
		for _, replica := range dp.Replicas {
			var zone string
			for i, host := range dp.Hosts {
				if replica.Addr == host {
					zone = dp.Zones[i]
				}
			}
			info := &cproto.ReplicaViewDetail{
				Addr:        replica.Addr,
				Zone:        zone,
				Status:      formatPartitionStatus(replica.Status),
				Role:        formatReplicaRole(replica.IsLeader, replica.IsLearner),
				IsLeader:    replica.IsLeader,
				IsLearner:   replica.IsLearner,
				IsRecover:   replica.IsRecover,
				ReportTime:  FormatTimestamp(replica.ReportTime),
				FileCount:   replica.FileCount,
				Total:       FormatSize(replica.Total),
				Used:        FormatSize(replica.Used),
				NeedCompare: replica.NeedsToCompare,
				MType:       replica.MType,
				DiskPath:    replica.DiskPath,
			}
			replicas = append(replicas, info)
		}
		view.Replicas = replicas
		view.ReplicaNum = int(dp.ReplicaNum)
		return view
	}
}

func FormatFlashGroup(view *proto.FlashGroupAdminView) *cproto.ClusterFlashGroupView {
	result := &cproto.ClusterFlashGroupView{
		ID:             view.ID,
		Slots:          view.Slots,
		Status:         view.Status.IsActive(),
		FlashNodeCount: view.FlashNodeCount,
	}
	return result
}

func FormatTimestamp(timeUnix int64) string {
	if timeUnix == 0 {
		return ""
	}
	return time.Unix(timeUnix, 0).Format("2006-01-02 15:04:05")
}

func formatTime(t time.Time) string {
	return t.Format("2006-01-02 15:04:05")
}

func formatPartitionStatus(status int8) string {
	switch status {
	case 1:
		return "ReadOnly"
	case 2:
		return "Writable"
	case -1:
		return "Unavailable"
	default:
		return "Unknown"
	}
}

func formatStoreMode(mode uint8) string {
	switch mode {
	case 1:
		return "Mem"
	case 2:
		return "RocksDB"
	case 3:
		return "Mem&Rocks"
	default:
		return "Unknown"
	}
}

func formatReplicaRole(isLeader, isLeaner bool) string {
	if isLeader {
		return "leader"
	} else if isLeaner {
		return "learner"
	} else {
		return "follower"
	}
}

func formatMissInfo(missNodes, missReplicaFiles map[string]int64) []*cproto.MissingInfo {
	res := make([]*cproto.MissingInfo, 0)
	// todo: ts时间戳单位 s级还是ms级
	if missNodes != nil {
		for missNode, ts := range missNodes {
			entry := &cproto.MissingInfo{
				Addr:      missNode,
				FoundTime: FormatTimestamp(ts),
			}
			res = append(res, entry)
		}
	}
	if missReplicaFiles != nil {
		for missReplicaFile, ts := range missReplicaFiles {
			entry := &cproto.MissingInfo{
				File:      missReplicaFile,
				FoundTime: FormatTimestamp(ts),
			}
			res = append(res, entry)
		}
	}
	return res
}

func FormatHost(hosts, zones []string) []*cproto.HostInfo {
	res := make([]*cproto.HostInfo, 0, len(zones))
	for i := 0; i < len(hosts); i++ {
		entry := &cproto.HostInfo{
			Zone: zones[i],
			Addr: hosts[i],
		}
		res = append(res, entry)
	}
	return res
}

func formatLearner(learners []proto.Learner) []*cproto.Learner {
	res := make([]*cproto.Learner, 0, len(learners))
	for _, learner := range learners {
		entry := &cproto.Learner{
			ID:            learner.ID,
			Addr:          learner.Addr,
			AutoProm:      learner.PmConfig.AutoProm,
			PromThreshold: learner.PmConfig.PromThreshold,
		}
		res = append(res, entry)
	}
	return res
}

var units = []string{"B", "KB", "MB", "GB", "TB", "PB"}
var step uint64 = 1024

func fixUnit(curSize uint64, curUnitIndex int) (newSize uint64, newUnitIndex int) {
	if curSize >= step && curUnitIndex < len(units)-1 {
		return fixUnit(curSize/step, curUnitIndex+1)
	}
	return curSize, curUnitIndex
}

func FormatSize(size uint64) string {
	fixedSize, fixedUnitIndex := fixUnit(size, 0)
	return fmt.Sprintf("%v %v", fixedSize, units[fixedUnitIndex])
}

func FormatRatio(ratio float64) string {
	return fmt.Sprintf("%.2f%%", ratio*100)
}

var EcStatusMap = map[uint8]string{
	0: "NotMigrate",
	1: "Migrating",
	2: "FinishEc",
	3: "RollBack",
	4: "OnlyEcExist",
	5: "MigrateFailed",
}
