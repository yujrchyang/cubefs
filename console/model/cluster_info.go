package model

import (
	"fmt"
	"github.com/cubefs/cubefs/console/cutil"
	"github.com/cubefs/cubefs/util/log"
	"time"
)

type RangeQueryRequest struct {
	Page     int
	PageSize int
}

const (
	AbnormalStatus = iota //异常
	ClearStatus           //异常消除
)

const (
	AbnormalAlarmLevelNormal = iota //正常 绿
	AbnormalAlarmLevelWarn          //普通 黄
	AbnormalAlarmLevelError         //问题 红
)

type ClusterInfo struct {
	ClusterName              string    `json:"clusterName" gorm:"column:cluster_name;comment:"`
	ClusterNameZH            string    `json:"clusterNameZH" gorm:"column:cluster_name_zh;comment:"`
	VolNum                   int       `json:"volumeNum" gorm:"column:volume_number;comment:"`
	TotalTB                  float64   `json:"totalTB" gorm:"column:total_tb;comment:"`
	UsedTB                   float64   `json:"usedTB" gorm:"column:used_tb;comment:"`
	IncreaseTB               float64   `json:"increaseTB" gorm:"column:increase_tb;comment:"`
	IsHighPerformanceCluster bool      `json:"isHighPerformanceCluster" gorm:"column:is_high_performance"`
	IsOverseaCluster         bool      `json:"isOverseaCluster" gorm:"column:is_oversea"`
	StateLevel               int       `json:"healthState" gorm:"-"`
	UpdateTime               time.Time `json:"-" gorm:"column:time;comment:"`
}

func (ClusterInfo) TableName() string {
	return "chubaofs_cluster_info_table"
}

func (table ClusterInfo) GetClusterListFromMysql(clusters []string) (infos []*ClusterInfo, err error) {
	infos = make([]*ClusterInfo, 0, len(clusters))
	dayTime, _ := time.Parse(time.DateTime, fmt.Sprintf("%s 00:00:00", time.Now().Format("2006-01-02")))
	if err = cutil.SRE_DB.Table(table.TableName()).Where("time >= ?", dayTime.Format(time.DateTime)).
		Where("cluster_name in (?)", clusters).Find(&infos).Error; err != nil {
		log.LogErrorf("GetClusterListFromMysql failed: clusters(%v) err(%v)", clusters, err)
	}
	return
}

func (table ClusterInfo) LoadClusterInfo(cluster string) (clusterInfo *ClusterInfo, err error) {
	clusterInfo = &ClusterInfo{}
	dayTime, _ := time.Parse(time.DateTime, fmt.Sprintf("%s 00:00:00", time.Now().Format("2006-01-02")))
	if err = cutil.SRE_DB.Table(table.TableName()).Where("time >= ?", dayTime.Format(time.DateTime)).
		Where("cluster_name = ?", cluster).Find(&clusterInfo).Error; err != nil {
		log.LogErrorf("LoadClusterInfo failed: cluster(%v) err(%v)", cluster, err)
	}
	return
}

// 集群健康度 报警信息表
type AbnormalRecord struct {
	ID                  uint      `gorm:"column:id;primary_key;AUTO_INCREMENT"`
	ClusterName         string    `json:"clusterId" form:"clusterId" gorm:"column:cluster_name;comment:"`
	IpAddr              string    `json:"IpAddr" gorm:"column:ip_addr;comment:异常对应的节点IP"`
	Role                string    `json:"role" gorm:"column:role;comment:告警角色 master、datanode、上传、下载、合图、LB"`
	AlarmType           int       `json:"alarmType" gorm:"column:alarm_type;comment:告警类型 主机/进程、坏盘、副本缺少、UMP"`
	AlarmTypeDes        string    `json:"alarmTypeDes" gorm:"column:alarm_type_des;comment:异常类型描述 对应于AlarmType 中文解释"`
	UmpKey              string    `json:"umpKey" gorm:"column:ump_key;comment:告警的key值,如果是ump告警此项信息不为空"`
	UmpKeyMonitorType   string    `json:"umpKeyMonitorType" gorm:"column:ump_key_monitor_type;comment:ump key的监控类型 方法、自定义、port"`
	MetricName          string    `json:"metricName" gorm:"column:metric_name;comment:ump告警的metric名称"`
	UmpAlarmLevel       string    `json:"umpAlarmLevel" gorm:"column:ump_alarm_level;comment:ump告警级别：WARNING、 CRITICAL、CLEARED"`
	AlarmData           string    `json:"alarmData" gorm:"column:alarm_data;comment:告警 异常原因,具体的原因信息, 坏盘保存disk path"`
	AlarmLevel          int       `json:"alarmLevel" gorm:"column:alarm_level;comment: 告警级别 2红 1黄 0绿 "`
	AlarmOrigin         string    `json:"alarmOrigin" gorm:"column:alarm_origin;comment:来源 UMP MDC 等拉取到的告警信息"`
	StartTime           time.Time `json:"startTime" gorm:"column:start_time;default:null;comment:异常告警开始时间"`
	EndTime             time.Time `json:"end_time" gorm:"column:end_time;default:null;comment:异常告警消除/处理时间"`
	Status              int       `json:"status" gorm:"column:status;comment:是否消除: 0 故障未恢复, 1故障恢复"`
	HandlerERP          string    `json:"handlerERP" gorm:"column:handler_erp;comment:处理人的ERP"`
	HandleTime          time.Time `json:"handleTime" gorm:"column:handle_time;default:null;comment:处理时间"`
	HandleType          string    `json:"handleType" gorm:"column:handle_type;comment:处理方式：各种详细msg"`
	Operation           int       `json:"operation" gorm:"column:operation;comment:操作"`
	IDC                 bool      `json:"idc" gorm:"column:idc;comment:是否进行IDC报障"`
	IDCOrderId          int       `json:"idcOrderId" gorm:"column:idc_order_id;comment:IDC报障工单号"`
	DataPartitionId     string    `json:"dataPartitionId" gorm:"column:data_partition_id;comment:缺少副本的data partition的id"`
	MetaPartitionId     string    `json:"metaPartitionId" gorm:"column:meta_partition_id;comment:缺少副本的meta partition的id"`
	MdcAlarmMetric      string    `json:"mdcAlarmMetric" gorm:"column:mdc_alarm_metric;"`
	MdcAlarmContainerId string    `json:"mdcAlarmContainerId" gorm:"column:mdc_alarm_container_id"` //todo:pod状态检查中的container id复用此处的字段
	DiskSnInfo          string    `json:"diskSnInfo" gorm:"column:disk_sn_info;"`                   // 坏盘对应的信息，原AlarmData用于唯一性判断，所以用新字段
	XBPTicketId         int       `json:"xbp_ticket_id" gorm:"column:xbp_ticket_id;comment:xbp_ticket_id"`
}

func (AbnormalRecord) TableName() string {
	return "chubaofs_alarm_table"
}

func (table AbnormalRecord) LoadAlarmRecordsWithOffset(searchCondition map[string]interface{}, rangeRequest *RangeQueryRequest) (r []*AbnormalRecord, total int64, err error) {
	limit := rangeRequest.PageSize
	if rangeRequest.PageSize == 0 {
		limit = 10
	}
	offset := rangeRequest.PageSize * (rangeRequest.Page - 1)
	r = make([]*AbnormalRecord, 0)
	if err = cutil.SRE_DB.Table(table.TableName()).Where(searchCondition).Count(&total).Error; err != nil {
		return nil, 0, fmt.Errorf("get alarm record count failed, err:%v", err)
	}

	if err = cutil.SRE_DB.Table(table.TableName()).Where(searchCondition).Order("start_time desc").
		Limit(limit).Offset(offset).Find(&r).Error; err != nil {
		err = fmt.Errorf("search alarm info with offset from model failed, err:%v", err)
		log.LogErrorf("LoadAlarmRecordsWithOffset: err: %v", err)
	}
	return
}

func (table AbnormalRecord) LoadHistoryAlarmRecords(cluster, role string) (r []*AbnormalRecord, err error) {
	r = make([]*AbnormalRecord, 0, 10)
	sqlStr := fmt.Sprintf("select * from %s where cluster_name='%s' AND role='%s' AND idc_order_id!=0 ORDER BY start_time DESC LIMIT 10",
		table.TableName(), cluster, role)
	if err = cutil.SRE_DB.Raw(sqlStr).Scan(&r).Error; err != nil {
		log.LogErrorf("LoadHistoryAlarmRecords: exec sql(%s) err(%v)", sqlStr, err)
		return
	}
	return
}

// 获取集群的健康度信息
func GetClusterHealthLevel(clusterName string) (clusterHealthLevel int) {
	var searchCondition = map[string]interface{}{
		"cluster_name": clusterName,
		"status":       AbnormalStatus,
	}

	alarmInfo, err := GetAlarmRecordsFromDatabase(searchCondition)
	if err != nil {
		return
	}
	for _, alarm := range alarmInfo {
		if alarm.AlarmLevel > clusterHealthLevel {
			clusterHealthLevel = alarm.AlarmLevel
		}
	}
	return
}

func GetAlarmRecordsFromDatabase(searchCondition map[string]interface{}) (alarmInfo []*AbnormalRecord, err error) {
	alarmInfo = make([]*AbnormalRecord, 0)
	err = cutil.SRE_DB.Table(AbnormalRecord{}.TableName()).Where(searchCondition).Find(&alarmInfo).Error
	if err != nil {
		errMsg := fmt.Sprintf("search alarm info from database failed, err:%v", err)
		log.LogError(errMsg)
		err = fmt.Errorf(errMsg)
		return
	}
	return
}

type ChubaofsClusterCapacityInMysql struct {
	ID          uint      `gorm:"primary_key;AUTO_INCREMENT"`
	ClusterName string    `gorm:"column:cluster_name"`
	RecordTime  time.Time `gorm:"column:record_time"`
	ZoneName    string    `gorm:"column:zone_name"`
	TotalGB     uint64    `gorm:"column:total_gb"`
	UsedGB      uint64    `gorm:"column:used_gb"`
	FileNum     uint64    `gorm:"column:total_file_num"`
	HourFlag    bool      `gorm:"column:is_hour"`
}

func (ChubaofsClusterCapacityInMysql) TableName() string {
	return "chubaofs_cluster_capacity_table"
}

type ChubaofsClusterInfoInMysql struct {
	StorageName              string    `gorm:"column:storage_name;comment:集群的类型：chubaofs/tfnode/flownode/image2.0;type:varchar(250);size:250;"`
	ClusterName              string    `gorm:"column:cluster_id"`
	ClusterNameZH            string    `gorm:"column:cluster_name"`
	Addr                     string    `gorm:"column:master_lb_addr"`
	MasterAddrs              string    `gorm:"column:master_addr"`
	IsContainObjectNode      bool      `gorm:"column:has_s3; comment:是否有s3;"`
	SystemName               string    `gorm:"column:upload_jdos;comment:chubaofs中,如果包含s3,这个字段记录的是jdos上的系统名称;"` //system name
	AppName                  string    `gorm:"column:cps_jdos;comment:chubaofs中,如果包含s3,这个字段记录的是jdos上的应用名称;;"`   //app names
	Creator                  string    `gorm:"column:creater;comment:创建者名称;type:varchar(250);size:250;"`
	CreateTime               time.Time `gorm:"column:create_time; default:null; comment:;type:datetime;"`
	UpdateTime               time.Time `gorm:"column:update_time; default:null; comment:;type:datetime;"`
	IsRelease                bool      `gorm:"column:is_release; comment:是否是release_db分支;"`
	DataNodeProfPort         uint16    `gorm:"column:data_node_prof_port; comment:data node的端口号;"`
	MetaNodeProfPort         uint16    `gorm:"column:meta_node_prof_port; comment:meta node的端口号;"`
	IsHighPerformanceCluster bool      `gorm:"column:is_high_performance;"`
	IsOverseaCluster         bool      `gorm:"column:is_oversea;"`
}

func (ChubaofsClusterInfoInMysql) TableName() string {
	return "tb_storage_cluster"
}

func (table ChubaofsClusterInfoInMysql) GetClusterInfoFromMysql(clusterName string) (info *ChubaofsClusterInfoInMysql, err error) {
	var clusterInfo ChubaofsClusterInfoInMysql
	err = cutil.SRE_DB.Table(table.TableName()).Where("storage_name = 'chubaofs' AND cluster_id = ? ",
		clusterName).First(&clusterInfo).Error
	if err != nil {
		err = fmt.Errorf("failed to look up the cluster %s information in model:%v", clusterName, err)
		return nil, err
	}
	return &clusterInfo, nil
}

type ChubaoFSZoneToRoomMapInMysql struct {
	ID                uint   `gorm:"column:id;primary_key;AUTO_INCREMENT"`
	ClusterName       string `json:"clusterId" form:"clusterId" gorm:"column:cluster_name;comment:集群名称"`
	ZoneName          string `json:"zoneName" gorm:"column:zone_name;comment:zone名, master视图获取到的"`
	IP                string `json:"ipAddr" gorm:"column:ip;comment:IP地址"`
	MachineRoomName   string `json:"machineRoomName" gorm:"column:machine_room_name;comment:机房简称"`
	MachineRoomNameZH string `json:"machineRoomNameZH" gorm:"column:machine_room_name_zh;comment:机房中文名"`
}

func (ChubaoFSZoneToRoomMapInMysql) TableName() string {
	return "chubaofs_zone_room_info"
}

func (table ChubaoFSZoneToRoomMapInMysql) GetZoneNameList(clusterName string, dataCenterName string) []string {
	zoneRoomInfo := make([]*ChubaoFSZoneToRoomMapInMysql, 0)
	if err := cutil.SRE_DB.Table(table.TableName()).
		Where("cluster_name = ? AND machine_room_name_zh = ?", clusterName, dataCenterName).Group("zone_name").
		Find(&zoneRoomInfo).Error; err != nil {
		return nil
	}
	zoneNameList := make([]string, 0)
	for _, info := range zoneRoomInfo {
		zoneNameList = append(zoneNameList, info.ZoneName)
	}
	return zoneNameList
}

type ConsoleCluster struct {
	ClusterName     string    `json:"clusterName" gorm:"column:cluster_name"`
	ClusterNameZH   string    `json:"clusterNameZH" gorm:"column:cluster_name_ZH"`
	MasterDomain    string    `json:"masterDomain" gorm:"column:master_domain"`
	MasterAddrs     string    `json:"masterAddrs" gorm:"column:master_addrs"`
	ObjectDomain    string    `json:"objectDomain" gorm:"column:s3_domain"`
	MetaProf        string    `json:"metaProf" gorm:"column:meta_prof"`
	DataProf        string    `json:"dataProf" gorm:"column:data_prof"`
	FlashProf       string    `json:"flashProf" gorm:"column:flash_prof"`
	IsRelease       bool      `json:"isRelease" gorm:"column:isRelease"`
	RebalanceHost   string    `json:"rebalanceHost" gorm:"column:rebalance_addr"`
	FileMigrateHost string    `json:"fileMigrateHost" gorm:"column:migrate_addr"`
	UpdateTime      time.Time `json:"-" gorm:"column:update_time"`
}

func (ConsoleCluster) TableName() string {
	return "console_cluster_info"
}

func (table ConsoleCluster) String() string {
	return fmt.Sprintf("%s", table.ClusterName)
}

func (table ConsoleCluster) LoadConsoleClusterList(cluster string) ([]*ConsoleCluster, error) {
	var res = make([]*ConsoleCluster, 0)
	db := cutil.CONSOLE_DB.Table(table.TableName())
	if cluster != "" {
		db.Where("cluster_name = ?", cluster)
	}
	if err := db.Scan(&res).Error; err != nil {
		return nil, err
	}
	return res, nil
}

func (table ConsoleCluster) InsertConsoleCluster(cluster *ConsoleCluster) error {
	if err := cutil.CONSOLE_DB.Table(table.TableName()).Create(&cluster).Error; err != nil {
		log.LogErrorf("InsertConsoleCluster failed: err: %v", err)
		return err
	}
	return nil
}

type ZoneSourceMapper struct {
	Id      uint64 `gorm:"column:id"`
	Zone    string `gorm:"column:zone"`
	Sources string `gorm:"column:sources"`
}

func (ZoneSourceMapper) TableName() string {
	return "zone_source_mapper"
}

func (z ZoneSourceMapper) GetSourcesInZone(zone []string) (re []*ZoneSourceMapper) {
	re = make([]*ZoneSourceMapper, 0)
	cutil.CONSOLE_DB.Table(z.TableName()).Where("zone IN ?", zone).
		Find(&re)
	return
}
