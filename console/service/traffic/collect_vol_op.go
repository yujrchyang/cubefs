package traffic

import (
	"bufio"
	"fmt"
	api "github.com/cubefs/cubefs/console/service/apiManager"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/console/cutil"
	"github.com/cubefs/cubefs/console/model"
	"github.com/cubefs/cubefs/console/proto"
	"github.com/cubefs/cubefs/util/log"
)

const (
	DateFormatString = "2006-01-02"
	maxQueryRetryNum = 3
)

func GetVolList(cluster string, api *api.APIManager) []string {
	if proto.IsRelease(cluster) {
		client := api.GetReleaseClient(cluster)
		volumeList, err := client.GetAllVolList()
		if err != nil {
			log.LogErrorf("CollectVolumeOps: getVolList failed, cluster(%v) err(%v)", cluster, err)
			return nil
		}
		return volumeList
	} else {
		mc := api.GetMasterClient(cluster)
		volInfos, err := mc.AdminAPI().ListVols("")
		if err != nil {
			log.LogErrorf("CollectVolumeOps: getVolList failed: cluster(%v) err(%v)", cluster, err)
			return nil
		}
		volumeList := make([]string, 0, len(volInfos))
		for _, vol := range volInfos {
			volumeList = append(volumeList, vol.Name)
		}
		return volumeList
	}
}

func CollectVolumeOps() {
	var (
		recordCh = make(chan []*model.ConsoleVolumeOps, 10)
		wg       = new(sync.WaitGroup)
	)
	go recordVolumeOps(recordCh)

	sdk := api.GetSdkApiManager()
	clusters := sdk.GetConsoleCluster()
	for _, cluster := range clusters {
		wg.Add(1)
		go getVolumeOps(cluster.ClusterName, wg, recordCh)
	}
	wg.Wait()
	close(recordCh)

	cleanExpiredVolOps()
}

func recordVolumeOps(ch <-chan []*model.ConsoleVolumeOps) {
	for {
		records, ok := <-ch
		if !ok {
			break
		}
		model.BatchInsertVolumeOps(records)
	}
	log.LogInfof("僵尸vol记录完成, time:%v", time.Now())
}

func getVolumeOps(cluster string, wg *sync.WaitGroup, recordCh chan<- []*model.ConsoleVolumeOps) {
	defer wg.Done()
	// 1. 先查表 获取所有vol的基本信息
	now := time.Now()
	date := time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), 0, 0, 0, time.Local)
	consoleVols, err := model.ConsoleVolume{}.LoadVolumeInfoByCluster(cluster, date)
	if err != nil {
		log.LogErrorf("getVolumeOps: LoadClusterVolumes failed, cluster(%v) err(%v)", cluster, err)
		return
	}

	vols := make([]*model.ConsoleVolumeOps, 0, len(consoleVols))
	for _, info := range consoleVols {
		vol := &model.ConsoleVolumeOps{
			Cluster:    cluster,
			Volume:     info.Volume,
			Zone:       info.Zone,
			Source:     info.Source,
			TotalGB:    info.TotalGB,
			UsedGB:     info.UsedGB,
			OwnerId:    info.Pin,
			Department: info.Department,
			PeriodMode: int(model.ZombiePeriodDay),
		}
		vols = append(vols, vol)
	}
	// 僵尸vol
	req := &proto.QueryVolOpsRequest{
		Cluster: cluster,
		Period:  model.ZombiePeriodDay,
		Module:  proto.RoleNameDataNode,
		Action:  "",
	}
	volOps, err := queryVolumeOps(req)
	if err != nil {
		log.LogErrorf("getVolumeOps: QueryVolumeOps failed: req:%v, err:%v", req, err)
	}

	req.Module = proto.RoleNameMetaNode
	req.Action = "createInode"
	createOps, err := queryVolumeOps(req)
	if err != nil {
		log.LogErrorf("getVolumeOps: QueryVolumeOps failed: req:%v, err:%v", req, err)
	}

	req.Action = "evictInode"
	evictOps, err := queryVolumeOps(req)
	if err != nil {
		log.LogErrorf("getVolumeOps: QueryVolumeOps failed: req:%v, err:%v", req, err)
	}

	log.LogInfof("getVolOps: len(vol)=%v len(volOps)=%v len(createOps)=%v len(evictOps)=%v", len(vols), len(volOps), len(createOps), len(evictOps))
	for _, vol := range vols {
		vol.Ops = volOps[vol.Volume]
		vol.CreateInodeOps = createOps[vol.Volume]
		vol.EvictInodeOps = evictOps[vol.Volume]
		vol.CreateTime = time.Now()
	}
	recordCh <- vols
}

func cleanExpiredVolOps() {
	timeStr := time.Now().AddDate(0, 0, -volumeHistoryKeepDay*2).Format(time.DateTime)
	err := model.CleanExpiredVolumeOps(timeStr)
	if err != nil {
		log.LogWarnf("cleanExpiredVolOps failed: %v", err)
	}
}

func queryVolumeOps(request *proto.QueryVolOpsRequest) (volOps map[string]uint64, err error) {
	sqlLines := fmt.Sprintf(""+
		"select "+
		"volume_name, sum(total_count) as ops "+
		"from "+
		"chubaofs_monitor_data_storage.%s "+
		"where "+
		"time >= '%s' "+
		"AND cluster_name = '%s' "+
		"AND module = '%s' ",
		getVolMonitorTableName(request.Period), getVolMonitorStartTime(request.Period), request.Cluster, strings.ToLower(request.Module))
	if request.Action != "" {
		sqlLines += fmt.Sprintf(""+
			"AND action = '%s' ",
			request.Action)
	}
	sqlLines += "" +
		"group by " +
		"volume_name"

	for i := 0; i < maxQueryRetryNum; i++ {
		volOps, err = queryCK(sqlLines)
		if err == nil {
			break
		}
	}
	if err != nil {
		log.LogErrorf("QueryVolumeOps: cluster[%v] err: %v", request.Cluster, err)
	}
	return
}

func queryCK(sqlLines string) (volOps map[string]uint64, err error) {
	urlStr := fmt.Sprintf("http://%s:%s@%s/?query=%s",
		cutil.ClickHouseDBROnlyUser, cutil.ClickHouseDBPassword, cutil.ClickHouseDBHostAddr, url.QueryEscape(sqlLines))
	resp, err := http.Get(urlStr)
	if err != nil {
		log.LogErrorf("sendQueryRequest failed: err(%v) url(%s)", err, urlStr)
		return
	}
	defer resp.Body.Close()

	volOps = make(map[string]uint64, 0)
	s := bufio.NewScanner(resp.Body)
	for s.Scan() {
		line := s.Text()
		fields := strings.Split(line, "\t")
		if len(fields) != 2 {
			err = fmt.Errorf("queryScanner: wrong result format")
			return
		}
		ops, err := strconv.ParseUint(fields[1], 10, 64)
		if err != nil {
			log.LogWarnf("queryCKForVolOps: parse ops failed: vol(%v) err(%v)", fields[0], err)
			continue
		}
		volOps[fields[0]] = ops
	}
	return
}

func getVolMonitorStartTime(period model.ZombieVolPeriod) string {
	before := time.Now()
	if period == model.ZombiePeriodDay {
		start := time.Date(before.Year(), before.Month(), before.Day(), 0, 0, 0, 0, before.Location())
		return start.Format(time.DateTime)
	} else {
		start := time.Date(before.Year(), before.Month(), 1, 0, 0, 0, 0, before.Location())
		return start.Format(DateFormatString)
	}
}
func getVolMonitorTableName(period model.ZombieVolPeriod) string {
	switch period {
	case model.ZombiePeriodDay:
		return "chubaofs_volume_day_table_view_dist"
	case model.ZombiePeriodMonth:
		return "chubaofs_volume_month_table_view_dist"
	default:
		return ""
	}
}
