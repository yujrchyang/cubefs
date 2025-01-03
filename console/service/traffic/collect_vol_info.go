package traffic

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/console/cutil"
	"github.com/cubefs/cubefs/console/model"
	cproto "github.com/cubefs/cubefs/console/proto"
	api "github.com/cubefs/cubefs/console/service/apiManager"
	"github.com/cubefs/cubefs/util/log"
	"net/http"
	"strconv"
	"sync"
	"time"
)

const (
	volumeInfoKeepDay            = 2
	volumeHistoryKeepDay         = 30
	volumeClientCountQueryPeriod = time.Hour
)

func CollectVolumeInfo() {
	var (
		recordCh = make(chan []*model.ConsoleVolume, 10)
		wg       = new(sync.WaitGroup)
	)
	go recordVolumeInfo(recordCh)

	sdk := api.GetSdkApiManager()
	clusters := sdk.GetConsoleCluster()
	for _, cluster := range clusters {
		wg.Add(1)
		go getVolumeInfo(wg, recordCh, cluster.ClusterName, sdk)
	}
	wg.Wait()
	close(recordCh)
	for _, cluster := range clusters {
		cleanExpiredVolInfos(cluster.ClusterName)
	}
}

func recordVolumeInfo(ch <-chan []*model.ConsoleVolume) {
	for {
		records, ok := <-ch
		if !ok {
			break
		}
		err := model.StoreVolumeRecords(records)
		if err != nil {
			log.LogErrorf("recordVolumeInfo: store volume info records failed: cluster(%v) err(%v)", records[0].Cluster, err)
		}
	}
	if log.IsDebugEnabled() {
		log.LogDebugf("vol信息记录完成, time:%v", time.Now())
	}
}

func getVolumeInfo(wg *sync.WaitGroup, recordCh chan<- []*model.ConsoleVolume, cluster string, sdk *api.APIManager) {
	defer wg.Done()
	var (
		vols              = make([]*model.ConsoleVolume, 0)
		updateTime        = time.Now()
		volumeClientCount = make(map[string]uint64)
		volumeSourcePin   = make(map[string]*volSourcePinInfo)
	)

	// 1. 获取vol客户端连接
	volumeClientCountList, err := getClusterVolumeClientCnt(cluster)
	if err == nil {
		for _, entry := range volumeClientCountList {
			volumeClientCount[entry.VolumeName] = entry.Count
		}
	}
	// 2. 获取vol的source信息
	volumeSourcePinList, err := getVolSourceAndPinInfo(cluster)
	if err == nil {
		for _, entry := range volumeSourcePinList {
			volumeSourcePin[entry.VolumeName] = entry
		}
	}
	// 3. 从master拉取vol信息, todo: 并发获取vol信息
	if cproto.IsRelease(cluster) {
		rc := sdk.GetReleaseClient(cluster)
		clusterView, err := rc.GetClusterView()
		if err != nil {
			log.LogErrorf("getVolumeInfo failed: cluster(%v) err(%v)", cluster, err)
			return
		}
		for _, volStat := range clusterView.VolStat {
			vol := &model.ConsoleVolume{
				Cluster:     cluster,
				Volume:      volStat.Name,
				TotalGB:     volStat.TotalGB,
				UsedGB:      volStat.UsedGB,
				UpdateTime:  updateTime,
				ClientCount: int(volumeClientCount[volStat.Name]),
			}
			vol.UsedRatio, _ = strconv.ParseFloat(volStat.UsedRatio, 64)
			view, err := rc.AdminGetVol(volStat.Name)
			if err != nil {
				continue
			}
			vol.WritableDpNum = view.RwDpCnt
			vol.MpNum = view.MpCnt
			vol.WritableMpNum = view.RwMpCnt
			vol.InodeCount = view.InodeCount
			vol.DpReplicas = int(view.DpReplicaNum)
			if value, ok := volumeSourcePin[vol.Volume]; ok {
				vol.Source = value.Source
				vol.Pin = value.Pin
				vol.Department = value.Organization
				vol.CreateTime = value.CreateTime
			}
			// 如果获取vol信息失败的话，只存基本vol信息(不行 会造成曲线异常掉0)
			vols = append(vols, vol)
		}
	} else {
		mc := sdk.GetMasterClient(cluster)
		volInfos, err := mc.AdminAPI().ListVols("")
		if err != nil {
			log.LogErrorf("getVolumeInfo: getVolList failed, cluster(%v)err(%v)", cluster, err)
			return
		}
		for _, volInfo := range volInfos {
			vol := &model.ConsoleVolume{
				Cluster:     cluster,
				Volume:      volInfo.Name,
				TotalGB:     formatByteToGB(volInfo.TotalSize),
				UsedGB:      formatByteToGB(volInfo.UsedSize),
				UpdateTime:  updateTime,
				ClientCount: int(volumeClientCount[volInfo.Name]),
			}
			vol.UsedRatio = float64(vol.UsedGB) / float64(vol.TotalGB)
			view, err := mc.AdminAPI().GetVolumeSimpleInfo(volInfo.Name)
			if err != nil {
				continue
			}
			vol.WritableDpNum = view.RwDpCnt
			vol.MpNum = view.MpCnt
			vol.WritableMpNum = view.RwMpCnt
			vol.InodeCount = view.InodeCount
			vol.DpReplicas = int(view.DpReplicaNum)
			vol.Zone = view.ZoneName
			vol.CreateTime = view.CreateTime
			if value, ok := volumeSourcePin[vol.Volume]; ok {
				vol.Source = value.Source
				vol.Pin = value.Pin
				vol.Department = value.Organization
			}
			vols = append(vols, vol)
		}
	}
	recordCh <- vols
}

// 近1小时的客户端数量
func getClusterVolumeClientCnt(cluster string) (result []*cproto.VolumeClientCount, err error) {
	var (
		respData []byte
	)
	uri := fmt.Sprintf("http://%s%s", cutil.Global_CFG.HbaseQueryAddr, cproto.HbaseVolumeClientCountPath)
	req := cutil.NewAPIRequest(http.MethodGet, uri)
	startTimeStr := time.Now().Add(-volumeClientCountQueryPeriod).Format(cproto.TimeFormatCompact)
	req.AddParam("start", startTimeStr)
	req.AddParam("cluster", cluster)

	if respData, err = cutil.SendSimpleRequest(req, false); err != nil {
		log.LogErrorf("getClusterVolumeClientCount failed: cluster(%v) start(%v) err(%v)", cluster, startTimeStr, err)
		return nil, err
	}
	result = make([]*cproto.VolumeClientCount, 0)
	if err = json.Unmarshal(respData, &result); err != nil {
		log.LogErrorf("getClusterVolumeClientCount failed: cluster(%v) start(%v) err(%v)", cluster, startTimeStr, err)
		return nil, err
	}

	if log.IsDebugEnabled() {
		log.LogDebugf("getClusterVolumeClientCount success: cluster(%v) start(%v) len(%v)", cluster, startTimeStr, len(result))
	}
	return
}

type volSourcePinInfo struct {
	VolumeName   string `gorm:"column:volume_name"`
	Source       string `gorm:"column:source"`
	Pin          string `gorm:"column:pin"`
	CreateTime   string `gorm:"column:create_time"`
	Organization string `gorm:"column:organization_full_name"`
}

func getVolSourceAndPinInfo(cluster string) (records []*volSourcePinInfo, err error) {
	var convertClusterFunc = func(iCluster string) string {
		switch iCluster {
		case "mysql":
			return "elasticDB"
		case "cfs_dbBack":
			return "dbbak"
		case "cfs_test_cloud":
			return "test"
		default:
			return iCluster
		}
	}
	records = make([]*volSourcePinInfo, 0)
	err = cutil.MYSQL_DB.Raw("select volume.volume_name, volume.source, volume.pin, DATE_FORMAT(volume.create_time, '%Y-%m-%d %H:%i:%s') as create_time, user.organization_full_name from volume left join user on volume.pin = user.erp where volume.cluster = ?", convertClusterFunc(cluster)).
		Scan(&records).Error
	if err != nil {
		log.LogErrorf("getVolSourceAndPinInfo failed: cluster(%v) err(%v)", cluster, err)
	}
	return
}

func cleanExpiredVolInfos(cluster string) {
	end := time.Now().AddDate(0, 0, -volumeInfoKeepDay)
	start := end.Add(-1 * time.Hour)
	err := model.CleanExpiredVolumeInfo(start.Format(time.DateTime), end.Format(time.DateTime), cluster)
	if err == nil {
		log.LogInfof("cleanExpiredVolumeInfo success: cluster(%v)", cluster)
	}
}

// 将整点的数据 移到历史表
func MigrateVolumeHistoryData() {
	now := time.Now()
	date := time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), 0, 0, 0, time.Local)
	sdk := api.GetSdkApiManager()
	clusters := sdk.GetConsoleCluster()
	for _, cluster := range clusters {
		records, err := model.ConsoleVolume{}.LoadVolumeInfoByCluster(cluster.ClusterName, date)
		if err != nil {
			log.LogErrorf("MigrateVolumeHistoryData: load volume failed: cluster(%v) err(%v)", cluster, err)
			continue
		}
		model.StoreVolumeHistoryInfo(records)
		log.LogInfof("MigrateVolumeHistoryData: cluster(%v) time(%v)", cluster.ClusterName, date)
	}
	expired := date.AddDate(0, 0, -volumeHistoryKeepDay)
	for _, cluster := range clusters {
		if err := model.CleanExpiredVolumeHistoryInfo(cluster.ClusterName, expired.Format(time.DateTime)); err == nil {
			log.LogInfof("MigrateVolumeHistoryData: clean expired records success, cluster(%v) now(%v)", cluster, time.Now())
		}
	}
}

func formatByteToGB(byte uint64) uint64 {
	return byte / 1024 / 1024 / 1024
}
