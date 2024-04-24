package cfs

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/http_client"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/util/checktool/ump"
	"github.com/cubefs/cubefs/util/log"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	UmpAppName               = "chubaofs-node"
	UmpPlatForm              = "jdos"
	ClusterNameSpark         = "spark"
	ClusterNameMysql         = "mysql"
	UmpKeyCheckDpStatusSpark = "spark_master_dp_check_status"
	UmpKeyCheckDpStatusMysql = "mysql_master_dp_check_status"
	DomainSpark              = "sparkchubaofs.jd.local"
	DomainMysql              = "cn.elasticdb.jd.local"
	DpReloadTaskInterval     = 300 // unit: second
	DpReloadReloadInterval   = 10  // unit: minute
	RegexpFailedDpInfo       = "\\[(.*?)\\]"
)

var ReloadedDPRecords map[string]*DPReloadRecord
var ClusterNames = []string{ClusterNameSpark, ClusterNameMysql}

type DPReloadRecord struct {
	Cluster        string
	VolName        string
	DpId           uint64
	Replica        string
	LastReloadTime int64
}

func NewDPReloadRecord(cluster, vol string, dpId uint64, replica string) *DPReloadRecord {
	return &DPReloadRecord{
		Cluster: cluster,
		VolName: vol,
		DpId:    dpId,
		Replica: replica,
	}
}

func (s *ChubaoFSMonitor) scheduleToReloadDP() {
	ReloadedDPRecords = make(map[string]*DPReloadRecord)
	s.reloadFailedDp()
	for {
		t := time.NewTimer(time.Duration(DpReloadTaskInterval) * time.Second)
		select {
		case <-s.ctx.Done():
			return
		case <-t.C:
			s.reloadFailedDp()
		}
	}
}

func (s *ChubaoFSMonitor) reloadFailedDp() {
	log.LogDebugf("action[reloadFailedDp] reload failed data partition task is running...")
	for _, cluster := range ClusterNames {
		umpKey := getClusterUmpKey(cluster)
		domain := getClusterDomain(cluster)
		if umpKey == "" || domain == "" {
			log.LogErrorf("invalid cluster name, get umpKey or domain name is empty, cluster(%v), umpKey(%v), domain(%v)", cluster, umpKey, domain)
			continue
		}
		s.doReload(umpKey, domain)
	}
}

func (s *ChubaoFSMonitor) doReload(umpKey, domainName string) {
	defer func() {
		if e := recover(); e != nil {
			log.LogErrorf("action[doReload] reload failed dp failed, umpKey(%v), domainName(%v), err(%v)", umpKey, domainName, e)
		}
	}()

	dpRecords, err := s.getLoadFailedRecordsFromUmp(umpKey)
	if err != nil {
		log.LogErrorf("action[doReload] get load failed dp records failed, umpKey(%v), err(%v)", umpKey, err.Error())
		return
	}
	if len(dpRecords) <= 0 {
		log.LogInfof("action[doReload] get load failed dp records is empty, umpKey(%v)", umpKey)
		return
	}
	for _, dpRecord := range dpRecords {
		// 同一个dp 10 分钟内只能reload一次
		key := fmt.Sprintf("%d_%s", dpRecord.DpId, dpRecord.Replica)
		if existedRecord, ok := ReloadedDPRecords[key]; ok {
			if time.Now().UnixMilli()-existedRecord.LastReloadTime <= DpReloadReloadInterval*60*1000 {
				log.LogDebugf("action[doReload] has reloaded in 10 minutes, dpId(%v), replica(%v), lastReloadTime(%v)",
					dpRecord.DpId, dpRecord.Replica, existedRecord.LastReloadTime)
				continue
			}
		}
		// reload data partition
		if err = reloadDataPartition(dpRecord, domainName); err != nil {
			log.LogErrorf("action[doReload] reload data partition failed, dpId(%v), replica(%v), err(%v)",
				dpRecord.DpId, dpRecord.Replica, err.Error())
			continue
		}
		dpRecord.LastReloadTime = time.Now().UnixMilli()
		ReloadedDPRecords[key] = dpRecord
	}

	// 从ReloadedDPRecords清理之前reload过且已经超过10分钟的dp
	for key, record := range ReloadedDPRecords {
		if time.Now().UnixMilli()-record.LastReloadTime > DpReloadReloadInterval*60*1000 {
			delete(ReloadedDPRecords, key)
		}
	}
}

func (s *ChubaoFSMonitor) getLoadFailedRecordsFromUmp(umpKey string) (dpRecords []*DPReloadRecord, err error) {
	var alarmRecords *ump.AlarmRecordResponse
	startTime := time.Now().UnixMilli() - DpReloadTaskInterval*2*1000 // 获取10分钟内的报警
	alarmRecords, err = s.umpClient.GetAlarmRecords(alarmRecordsMethod, UmpAppName, UmpPlatForm, umpKey, startTime, time.Now().UnixMilli())
	if err != nil {
		log.LogErrorf("action[getLoadFailedRecordsFromUmp] get failed dp info from ump failed, umpKey(%v), startTime(%v), err(%v)", umpKey, startTime, err.Error())
		return
	}
	if log.IsDebugEnabled() {
		data, _ := json.Marshal(alarmRecords)
		log.LogDebugf("action[getLoadFailedRecordsFromUmp], umpKey(%v), alarmRecords(%v)", umpKey, string(data))
	}

	for _, record := range alarmRecords.Records {
		var dpRecord *DPReloadRecord
		if dpRecord, err = ParseFailedDpInfo(record.Content); err != nil {
			log.LogErrorf("action[getLoadFailedRecordsFromUmp] parsed failed dp info failed, content(%v), err(%v)", record.Content, err.Error())
			return
		}
		if dpRecord != nil {
			dpRecords = append(dpRecords, dpRecord)
		}
	}
	return
}

func ParseFailedDpInfo(content string) (record *DPReloadRecord, err error) {
	log.LogInfof("action[parseFailedDpInfo] content:%v", content)
	if !strings.Contains(content, "load failed by datanode") {
		log.LogInfof("action[parseFailedDpInfo] invalid ump content, content(%v)", content)
		return
	}

	reg := regexp.MustCompile(RegexpFailedDpInfo)
	matches := reg.FindAllStringSubmatch(content, -1)
	if len(matches) < 4 {
		log.LogWarnf("action[parseFailedDpInfo] regexp match failed, invalid ump content, content:%v", content)
		return
	}
	var dpId uint64
	cluster := matches[0][1]
	volName := matches[1][1]
	dpIdStr := matches[2][1]
	replica := matches[3][1]
	if dpId, err = strconv.ParseUint(dpIdStr, 10, 64); err != nil {
		log.LogWarnf("action[parseFailedDpInfo] parse data partition id from ump content, dpIdStr:%v, err:%v", dpIdStr, err.Error())
		return
	}
	record = NewDPReloadRecord(cluster, volName, dpId, replica)
	return
}

func reloadDataPartition(dpRecord *DPReloadRecord, domainName string) (err error) {
	var (
		partitionM *proto.DataPartitionInfo
		partitionD *proto.DNDataPartitionInfo
		maxReplica *proto.DataReplica
	)
	dHost := fmt.Sprintf("%v:%v", strings.Split(dpRecord.Replica, ":")[0], profPortMap[strings.Split(dpRecord.Replica, ":")[1]])
	dataClient := http_client.NewDataClient(dHost, false)
	if partitionD, err = dataClient.GetPartitionFromNode(dpRecord.DpId); err == nil && partitionD.ID == dpRecord.DpId {
		log.LogInfof("action[reloadDataPartition] need not to reload, get data partition success from data node dpId(%v), dHost(%v)", dpRecord.DpId, dHost)
		return
	}

	masterClient := master.NewMasterClient([]string{domainName}, false)
	if partitionM, err = masterClient.AdminAPI().GetDataPartition("", dpRecord.DpId); err != nil {
		log.LogErrorf("action[reloadDataPartition] get data partition from master failed, dpId(%v), err(%v)", dpRecord.DpId, err.Error())
		return
	}
	maxReplica = partitionM.Replicas[0]
	partitionPath := fmt.Sprintf("datapartition_%v_%v", dpRecord.DpId, maxReplica.Total)

	var dirPath string
	for _, r := range partitionM.Replicas {
		if r.Addr == dpRecord.Replica {
			dirPath = r.DiskPath
			break
		}
	}
	if dirPath == "" {
		log.LogErrorf("action[reloadDataPartition] get dirPath is empty, dpId(%v), replica(%v)", dpRecord.DpId, dpRecord.Replica)
		return
	}

	for i := 0; i < 3; i++ {
		if err = dataClient.ReLoadPartition(partitionPath, dirPath); err == nil {
			log.LogInfof("action[reloadDataPartition] reload data partition success, dpId(%v), replica(%v), index(%v), partitionPath(%v), dirPath(%v)",
				dpRecord.DpId, dpRecord.Replica, i, partitionPath, dirPath)
			break
		} else {
			log.LogWarnf("action[reloadDataPartition] reload failed, dpId(%v), replica(%v), index(%v), err(%v)",
				dpRecord.DpId, dpRecord.Replica, i, err.Error())
		}
	}
	return
}

func getClusterUmpKey(clusterName string) string {
	switch clusterName {
	case ClusterNameSpark:
		return UmpKeyCheckDpStatusSpark
	case ClusterNameMysql:
		return UmpKeyCheckDpStatusMysql
	default:
		return ""
	}
}

func getClusterDomain(clusterName string) string {
	switch clusterName {
	case ClusterNameSpark:
		return DomainSpark
	case ClusterNameMysql:
		return DomainMysql
	default:
		return ""
	}
}
