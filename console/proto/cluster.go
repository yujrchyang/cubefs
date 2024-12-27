package proto

import (
	"github.com/cubefs/cubefs/proto"
	"time"
)

const (
	BaseCroup             = "/chubaofs/"
	ClusterGroup          = BaseCroup + "cluster/"
	ClusterTopologyPath   = ClusterGroup + "topo"
	FlashNodeSetPingSort  = "/ping/set"
	FlashNodeSetStackRead = "/stack/set"
	FlashNodeSetTimeoutMs = "/singleContext/setTimeout"
)

const (
	All = "all"
)

// 角色名称
const (
	RoleNameMaster       = "Master"
	RoleNameDataNode     = "DataNode"
	RoleNameMetaNode     = "MetaNode"
	RoleNameObjectNode   = "ObjectNode"
	RoleNameLBNode       = "LBNode" //for health
	RoleNameObjectLBNode = "ObjectLBNode"
	RoleNameMasterLBNode = "MasterLBNode"
	RoleNameFlashNode    = "FlashNode"
)

// 异常类型 AlarmType
const (
	AbnormalTypeProcess = iota // 进程异常
	AbnormalTypeInternal
	AbnormalTypeBadDisk     // 坏盘 -- 具体的磁盘path
	AbnormalTypeMissReplica // vol 副本缺少，对应于具体的缺少的datanode节点 -- 单独的检查 处理逻辑
	AbnormalTypeUMP         // UMP -- 单独的检查 处理逻辑
	AbnormalTypePod         //容器异常
	AbnormalTypeMDC
	AbnormalTypeManual    //手工配置的告警
	AbnormalTypeHostFault //IDC机房巡检中的主机故障
)

var AlarmTypeDescMap = map[int]string{
	AbnormalTypeProcess:     "进程异常",
	AbnormalTypeInternal:    "节点内部状态异常",
	AbnormalTypeBadDisk:     "磁盘异常",
	AbnormalTypeMissReplica: "缺少副本",
	AbnormalTypeUMP:         "UMP平台告警",
	AbnormalTypePod:         "容器异常",
	AbnormalTypeMDC:         "MDC平台告警",
	AbnormalTypeManual:      "手工配置",
	AbnormalTypeHostFault:   "主机故障",
}

const (
	IDCHandle = iota
	EmergenceHandle
	UrlMDC
	OfflineHandle
	EnableHandle
	AlarmRemove
	StopHost
	RestartHost
)

type Operation struct {
	HandleType int    `json:"type"`
	HandleDesc string `json:"description"`
}

var Operations = []*Operation{
	OfflineOperation, EnableOperation, UrlMDCOperation, AlarmRemoveOperation,
}

var OfflineOperation = &Operation{
	HandleType: OfflineHandle,
	HandleDesc: "下线",
}

var EnableOperation = &Operation{
	HandleType: EnableHandle,
	HandleDesc: "拉起",
}

var UrlMDCOperation = &Operation{
	HandleType: UrlMDC,
	HandleDesc: "跳转MDC",
}

var AlarmRemoveOperation = &Operation{
	HandleType: AlarmRemove,
	HandleDesc: "消除告警",
}

var StopHostOperation = &Operation{
	HandleType: StopHost,
	HandleDesc: "主机关机",
}
var RestartHostOperation = &Operation{
	HandleType: RestartHost,
	HandleDesc: "主机重启",
}

var GlobalAlarmTypeToHandle = map[int][]*Operation{
	AbnormalTypeProcess: {
		OfflineOperation,
		AlarmRemoveOperation,
	},
	AbnormalTypeInternal: {
		OfflineOperation,
		AlarmRemoveOperation,
	},
	AbnormalTypeBadDisk: {
		OfflineOperation,
		AlarmRemoveOperation,
	},
	AbnormalTypeMissReplica: {
		OfflineOperation,
		AlarmRemoveOperation,
	},
	AbnormalTypeUMP: {
		AlarmRemoveOperation,
	},
	AbnormalTypePod: {
		EnableOperation,
		AlarmRemoveOperation,
	},
	AbnormalTypeMDC: {
		UrlMDCOperation,
		AlarmRemoveOperation,
	},
	//TODO:手工配置的告警都有哪些操作方法
	AbnormalTypeManual: {
		UrlMDCOperation,
		AlarmRemoveOperation,
	},
	AbnormalTypeHostFault: {
		StopHostOperation,
		RestartHostOperation,
		AlarmRemoveOperation,
	},
}

const (
	ResourceNoType int = iota //指定时间间隔
	ResourceLatestOneDay
	ResourceLatestOneWeek
	ResourceLatestOneMonth
	ResourceLatestThreeMonth
)

type CFSRoleHealthInfoResponse struct {
	ClusterName string               `json:"clusterId"`
	RoleName    string               `json:"roleName"`
	TotalNum    int64                `json:"totalNumber"`
	AlarmLevel  int                  `json:"alarmLevel"`
	AlarmList   []*CFSRoleHealthInfo `json:"alarmList"`
}
type CFSRoleHealthInfo struct {
	ID                   uint         `json:"Id"`
	IpAddr               string       `json:"IpAddr"`
	AlarmType            int          `json:"alarmType"`
	AlarmTypeDes         string       `json:"alarmTypeDes"`
	AlarmData            string       `json:"alarmData"`
	AlarmLevel           int          `json:"alarmLevel"`
	AlarmOrigin          string       `json:"alarmOrigin"`
	StartTime            string       `json:"startTime"`
	DurationTime         string       `json:"DurationTime"`
	EmergencyResponsible string       `json:"emergencyResponsible"`
	IDCStatus            bool         `json:"IDC"`
	Handles              []*Operation `json:"handle"`
	IDCHandleProcess     string       `json:"handleProcess"`
	OrderId              int          `json:"orderID"`
	IsInCluster          string       `json:"isInCluster"`
}
type ZoneInfo struct {
	ZoneName   string
	NodeNumber int
	IpList     []string
	Sources    string // separate by ,
}
type DataCenterInfo struct {
	DataCenterName    string
	DataCenterNodeNum int
	IpList            []string
}
type DataCenterInfoWithZoneInfo struct {
	DataCenterName    string
	DataCenterNodeNum int
	ZoneList          []*ZoneInfo
}
type RoleTopologyInfo struct {
	RoleName        string
	NodeTotalNumber int
	DataCenterInfo  []*DataCenterInfo
}
type RoleTopologyInfoWithZone struct {
	RoleName        string
	NodeTotalNumber int
	DataCenterInfo  []*DataCenterInfoWithZoneInfo
}

// 获取topo 是否可以从master拉取
type CFSSRETopology struct {
	ClusterName   string
	ClusterNameZH string
	Master        *RoleTopologyInfo         `json:"master"`
	DataNode      *RoleTopologyInfoWithZone `json:"dataNode"`
	MetaNode      *RoleTopologyInfoWithZone `json:"metaNode"`
	ObjectNode    *RoleTopologyInfo         `json:"objectNode"`
	LBNode        *RoleTopologyInfo         `json:"lbNode"`
	ObjectLBNode  *RoleTopologyInfo         `json:"objectLBNode"`
	MasterLBNode  *RoleTopologyInfo         `json:"masterLBNode"`
}

// 集群资源类信息获取的请求结构体
type CFSClusterResourceRequest struct {
	ClusterName    string `json:"clusterName"`
	DataCenterName string `json:"dataCenterName"`
	ZoneName       string `json:"zoneName"`
	IntervalType   int    `json:"internalType"` //0:选择的时间段; 1:近10分钟; 2:近1小时; 3:近24小时; 4:近7日
	StartTime      int64  `json:"startTime"`    //时间选择至少1天,最多不超过30天,毫秒级时间戳,数值类型
	EndTime        int64  `json:"endTime"`
}
type ClusterResourceData struct {
	Date      uint64  `json:"date"`
	TotalGB   uint64  `json:"totalGB"`
	UsedGB    uint64  `json:"usedGB"`
	UsedRatio float32 `json:"usedRatio"`
}

type ZoneUsageOverview struct {
	Date       uint64
	ZoneName   string
	TotalGB    uint64
	UsedGB     uint64
	UsageRatio float32
}

type ClusterFlashNodeView struct {
	ID           uint64 // nodeID
	Addr         string // 地址
	ReportTime   string
	IsActive     bool
	ZoneName     string
	FlashGroupID uint64 //fgID
	IsEnable     bool
	NodeLimit    string // 限速
	HitRate      string // 命中率
	Evicts       string // 过期
}

type FlashNodeListResponse struct {
	Data  []*ClusterFlashNodeView
	Total int
}

func NewFlashNodeListResponse(total int, data []*ClusterFlashNodeView) *FlashNodeListResponse {
	return &FlashNodeListResponse{
		Total: total,
		Data:  data,
	}
}

type ClusterFlashGroupView struct {
	ID             uint64
	Slots          []uint32
	Status         bool
	FlashNodeCount int
	//todo: hitRate string(host, not group)
}
type FlashGroupDetail struct {
	Host []string
	Zone []*ZoneInfo // zoneName hostCount ipList
}

type FlashGroupListResponse struct {
	Data  []*ClusterFlashGroupView
	Total int
}

func NewFlashGroupListResponse(total int, data []*ClusterFlashGroupView) *FlashGroupListResponse {
	return &FlashGroupListResponse{
		Data:  data,
		Total: total,
	}
}

type ConsoleClusterView struct {
	VolumeCount        int32
	MasterCount        int32
	MetaNodeCount      int32
	DataNodeCount      int32
	DataPartitionCount int32
	MetaPartitionCount int32
	MaxMetaPartitionID uint64
	MaxDataPartitionID uint64
	MaxMetaNodeID      uint64
	ClientPkgAddr      string
	DataNodeBadDisks   []DataNodeBadDisksView
	BadPartitionIDs    []BadPartitionView // 切片 release是uint64
	DataNodeStatInfo   *DataNodeSpaceStat // 这个字段命名 0701可以映射上去吗？
	MetaNodeStatInfo   *MetaNodeSpaceStat
}

type MasterView struct {
	Addr     string // 地址
	IsLeader bool   // 主节点
}

type MasterListResponse struct {
	Data  []*MasterView
	Total int
}

// meta/data 列表页
type NodeViewDetail struct {
	//common
	ID             uint64   // nodeID
	Addr           string   // 地址
	ZoneName       string   // zone
	IsActive       bool     // 状态
	Total          string   // 总容量
	Used           string   // 已使用
	Available      string   // 可使用, data
	UsageRatio     string   // 使用率
	PartitionCount uint32   // 分片数
	ReportTime     string   // 汇报时间
	IsWritable     bool     // 可写
	Disk           []string // 磁盘下线用
	// data
	RiskCount        int
	RiskFixerRunning bool
	// rdma
	IsRDMA          bool
	Pod             string
	NodeRDMAService bool
	NodeRDMASend    bool
	NodeRDMARecv    bool
}

// todo: 加上code，当且仅当code有值 才取数据
type NodeListResponse struct {
	Data  []*NodeViewDetail
	Total int
}

func NewNodeListResponse(total int, data []*NodeViewDetail) *NodeListResponse {
	return &NodeListResponse{
		Data:  data,
		Total: total,
	}
}

// dp/mp 列表页
type PartitionViewDetail struct {
	//common
	PartitionID  uint64               // 分片ID
	VolName      string               // 卷名
	Status       string               // 状态
	IsRecover    bool                 // isRecover
	CreateTime   string               // 创建时间， needFormat
	LastLoadTime string               // 最新加载时间， needFormat
	Replicas     []*ReplicaViewDetail // 副本数 - 可点击 []string
	Peers        []proto.Peer         // peer
	ReplicaNum   int                  // 副本数
	Leader       string               // leader地址
	// meta
	Start       uint64
	End         uint64
	InodeCount  uint64
	DentryCount uint64
	MaxExistIno uint64
	// data
	ExtentCount     uint64 // extent数(拿不着，是副本属性 不是dp属性)
	EcMigrateStatus string
	IsManual        bool

	Learners                []*Learner
	Hosts                   []*HostInfo
	MissingNodes            []*MissingInfo
	FilesWithMissingReplica []*MissingInfo
}
type HostInfo struct {
	Zone string
	Addr string
}
type Learner struct {
	ID            uint64
	Addr          string
	AutoProm      bool
	PromThreshold uint8
}
type MissingInfo struct {
	Addr      string
	File      string
	FoundTime string
}
type PartitionListResponse struct {
	Data  []*PartitionViewDetail
	Total int
}

func NewPartitionListResponse(total int, data []*PartitionViewDetail) *PartitionListResponse {
	return &PartitionListResponse{
		Data:  data,
		Total: total,
	}
}

// dp/mp 副本列表页
type ReplicaViewDetail struct {
	// common
	Addr       string
	Status     string // 1-ReadOnly 2-Writable -1:Unavailable 0-Unknown
	IsLeader   bool
	IsLearner  bool //  todo:用这两个字段形成角色(release 的datanode 是主备， 没有leader概念)
	IsRecover  bool
	Role       string
	ReportTime string
	// data
	Zone        string
	FileCount   uint32
	Total       string
	Used        string
	NeedCompare bool
	MType       string // hdd、ssd
	DiskPath    string
	// meta
	MaxInodeID  uint64
	InodeCount  uint64
	DentryCount uint64
	StoreMode   string // 1-Mem 2-RocksDB 3 Mem&Rocks
	ApplyId     uint64
}

type ZoneView struct {
	IPs []string
}

type CFSTopology struct {
	DataMap  map[string]*ZoneView
	MetaMap  map[string]*ZoneView
	FlashMap map[string]*ZoneView
}

type SourceUsageResponse struct {
	Data [][]*SourceUsedInfo
}

type SourceUsedInfo struct {
	Date   uint64 `gorm:"column:update_time"`
	Source string `gorm:"column:source"`
	UsedGB uint64 `gorm:"column:total_used"`
}

const (
	AdminSetRDMAConf      = "/rdmaConf/set"
	AdminRDMAConf         = "/rdma/conf"
	NodeSetRDMASendEnable = "/SetRDMASendEnable"
	NodeSetRDMAConnEnable = "/SetRDMAConnEnable"
	NodeGetRDMAStatus     = "/rdmaStatus"
	NodeGetStats          = "/stats"
	NodeGetAllRDMAConn    = "/GetAllRDMAConn"
)

type RDMAConfInfo struct {
	ClusterRDMA         bool
	ClusterRDMASend     bool
	RDMAReConnDelayTime int64
	RDMANodeMap         map[uint64]*NodeRDMAConf
}

type NodeRDMAConf struct {
	ID          uint64
	Addr        string
	ReportTime  time.Time
	Pod         string
	IsBond      bool
	RDMAConf    *RDMAConf
	RDMAService bool
	RDMASend    bool
	RDMARecv    bool
}

type RDMAConf struct {
	FWVersion     string
	DriverVersion string
	Vendor        []string
	SlaveName     []string
}

type NodeRDMAStatus struct {
	ClusterRDMA          bool
	ClusterRDMASend      bool
	Pod                  string
	RDMAService          bool
	RDMASend             bool
	RDMARecv             bool
	EnableSend           bool
	NodeCount            int
	PermanentClosedCount int
	RdmaNodeAddrMap      map[string]*ConnNodeInfo
}

type NodeStats struct {
	IsBond   bool
	RDMAConf *RDMAConf
}

/*      前端view       */
type ClusterRDMAConfView struct {
	ClusterRDMAEnable bool
	ClusterRDMASend   bool
	ReConnDelayTime   int64
}

type RDMANodeViewResponse struct {
	Total int
	Data  []*RDMANodeView
}

// rdma节点列表视图
type RDMANodeView struct {
	ID              uint64
	Addr            string
	Pod             string
	IsBond          bool
	Vendor          string
	NodeRDMAService bool
	NodeRDMASend    bool
	NodeRDMARecv    bool
	ReportTime      string
}

type RDMAVolumeListResponse struct {
	Total int
	Data  []*RDMAVolumeView
}

// rdma vol列表视图
type RDMAVolumeView struct {
	Volume     string
	EnableRDMA bool
}

// rdma节点详情视图
type RDMANodeStatusView struct {
	NodeRDMAStatus *NodeRDMAStatusView
	NodeRDMAConf   *NodeRDMAConfView
	ConnNodes      []*ConnNodeInfo
}

type NodeRDMAStatusView struct {
	Pod                  string
	ClusterRDMAEnable    bool
	ClusterRDMASend      bool
	NodeRDMAService      bool
	NodeRDMASend         bool
	NodeRDMARecv         bool
	EnableSend           bool
	NodeCount            int
	PermanentClosedCount int
}

type NodeRDMAConfView struct {
	FWVersion     string
	DriverVersion string
	Vendor        string
	SlaveName     string
}

type ConnNodeInfo struct {
	Addr            string
	EnableConn      bool `json:"Ok"`
	PermanentClosed bool
	ErrConnCount    int
	ReConnTime      string
}
