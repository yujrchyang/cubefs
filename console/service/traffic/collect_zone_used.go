package traffic

import (
	"github.com/cubefs/cubefs/console/model"
	api "github.com/cubefs/cubefs/console/service/apiManager"
	"github.com/cubefs/cubefs/util/log"
	"sync"
	"time"
)

const (
	zoneInfoKeepDays = 30
)

// zone使用率信息 data/meta
func CollectZoneUsedInfo() {
	var (
		recordCh = make(chan []*model.ClusterZoneInfo, 10)
		wg       = new(sync.WaitGroup)
	)
	go recordZoneInfo(recordCh)

	sdk := api.GetSdkApiManager()
	for _, cluster := range sdk.GetConsoleCluster() {
		wg.Add(1)
		go getZoneUsedInfo(wg, recordCh, cluster)
	}
	wg.Wait()
	close(recordCh)

	cleanExpiredZoneInfos()
}

func getZoneUsedInfo(wg *sync.WaitGroup, recordCh chan<- []*model.ClusterZoneInfo, cluster *model.ConsoleCluster) {
	defer wg.Done()

	sdk := api.GetSdkApiManager()
	isHour := time.Now().Minute() == 0

	records := make([]*model.ClusterZoneInfo, 0)
	if cluster.IsRelease {
		rc := sdk.GetReleaseClient(cluster.ClusterName)
		cv, err := rc.GetClusterView()
		if err != nil {
			log.LogErrorf("getZoneUsed failed: cluster(%v) err(%v)", cluster.ClusterName, err)
			return
		}
		metaUsed := &model.ClusterZoneInfo{
			Cluster: cluster.ClusterName,
			Zone:    "all",
			Module:  "meta",
			TotalGB: cv.MetaNodeStat.TotalGB,
			UsedGB:  cv.MetaNodeStat.UsedGB,
			IsHour:  isHour,
		}
		dataUsed := &model.ClusterZoneInfo{
			Cluster: cluster.ClusterName,
			Zone:    "all",
			Module:  "data",
			TotalGB: cv.DataNodeStat.TotalGB,
			UsedGB:  cv.DataNodeStat.UsedGB,
			IsHour:  isHour,
		}
		records = append(records, metaUsed, dataUsed)
	} else {
		mc := sdk.GetMasterClient(cluster.ClusterName)
		cs, err := mc.AdminAPI().GetClusterStat()
		if err != nil {
			log.LogErrorf("getZoneUsed failed: cluster(%v) err(%v)", cluster.ClusterName, err)
			return
		}
		for zoneName, zoneStat := range cs.ZoneStatInfo {
			if zoneStat.MetaNodeStat != nil {
				metaUsed := &model.ClusterZoneInfo{
					Cluster: cluster.ClusterName,
					Zone:    zoneName,
					TotalGB: uint64(zoneStat.MetaNodeStat.Total),
					UsedGB:  uint64(zoneStat.MetaNodeStat.Used),
					Module:  "meta",
					IsHour:  isHour,
				}
				records = append(records, metaUsed)
			}
			if zoneStat.DataNodeStat != nil {
				dataUsed := &model.ClusterZoneInfo{
					Cluster: cluster.ClusterName,
					Zone:    zoneName,
					TotalGB: uint64(zoneStat.DataNodeStat.Total),
					UsedGB:  uint64(zoneStat.DataNodeStat.Used),
					Module:  "data",
					IsHour:  isHour,
				}
				records = append(records, dataUsed)
			}
		}
	}
	recordCh <- records
}

func recordZoneInfo(ch <-chan []*model.ClusterZoneInfo) {
	for {
		records, ok := <-ch
		if !ok {
			break
		}
		err := model.ClusterZoneInfo{}.StoreZoneInfoRecords(records)
		if err != nil {
			log.LogErrorf("recordZoneInfo: store batch records failed: err(%v)", err)
		}
	}
	log.LogInfof("zone使用信息记录完成，time: %v", time.Now())
}

func cleanExpiredZoneInfos() {
	timeStr := time.Now().AddDate(0, 0, -zoneInfoKeepDays).Format(time.DateTime)
	err := model.ClusterZoneInfo{}.CleanExpiredRecords(timeStr)
	if err != nil {
		log.LogWarnf("cleanExpiredZoneInfos failed: %v", err)
	}
}
