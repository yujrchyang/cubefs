package proto

import (
	"fmt"
	"github.com/cubefs/cubefs/console/model"
	"time"
)

const (
	HbaseVolumeClientCountPath = "/queryJson/cfsClientCount"
	HbaseVolumeClientListPath  = "/queryJson/cfsClientList"
)

const (
	TimeFormatCompact = "20060102150405"
)

const (
	ActionDrawLineDimension int32 = iota
	DiskDrawLineDimension
	ClientZoneLineDimension
)

type DataGranularity int

const (
	SecondGranularity DataGranularity = iota
	MinuteGranularity
	TenMinuteGranularity
)

const (
	IntervalTypeNull = iota
	IntervalTypeLatestTenMin
	IntervalTypeLatestOneHour
	IntervalTypeLatestOneDay
)

const (
	HostUsageCurveNoType int = iota
	HostUsageCurveOneHour
	HostUsageCurveSixHour
	HostUsageCurveOneDay
)

// 角色名称
const (
	ModuleDataNode   = "DataNode"
	ModuleMetaNode   = "MetaNode"
	ModuleObjectNode = "ObjectNode"
	ModuleFlashNode  = "FlashNode"
)

var OPMap = map[string][]string{
	ModuleDataNode: {
		"clientRead",
		"clientWrite",
		"read",
		"repairRead",
		"appendWrite",
		"overWrite",
		"repairWrite",
		"markDelete",
		"batchMarkDelete",
		"flushDelete",
		"diskIOCreate",
		"diskIOWrite",
		"diskIORead",
		"diskIORemove",
		"diskIOPunch",
		"diskIOSync",
	},
	ModuleMetaNode: {
		"createInode",
		"evictInode",
		"createDentry",
		"deleteDentry",
		"lookup",
		"readDir",
		"inodeGet",
		"batchInodeGet",
		"addExtents",
		"listExtents",
		"truncate",
		"insertExtent",
		"opCreateInode",
		"opEvictInode",
		"opCreateDentry",
		"opDeleteDentry",
		"opLookup",
		"opReadDir",
		"opInodeGet",
		"opBatchInodeGet",
		"opAddExtents",
		"opListExtents",
		"opTruncate",
		"opInsertExtent",
	},
	ModuleObjectNode: {
		"HeadObject",
		"GetObject",
		"PutObject",
		"ListObjects",
		"DeleteObject",
		"CopyObject",
		"CreateMultipartUpload",
		"UploadPart",
		"CompleteMultipartUpload",
		"AbortMultipartUpload",
		"ListMultipartUploads",
		"ListParts",
	},
	ModuleFlashNode: {
		"read",
		"prepare",
		"evict",
		"hit",
		"miss",
		"expire",
	},
}

type TrafficRequest struct {
	ClusterName   string `json:"clusterName"`
	IntervalType  int    `json:"intervalType"` // 1-10min 2-30min 3-1h
	Module        string `json:"module"`
	VolumeName    string `json:"volumeName"`
	OperationType string `json:"opType"`
	TopN          int    `json:"topN"`
	OrderBy       string `json:"orderBy"`
	IpAddr        string `json:"ipAddr"`
	PageSize      int    `json:"pageSize"`
	Page          int    `json:"page"`
	StartTime     int64  `json:"startTime"` // 秒级时间戳
	EndTime       int64  `json:"endTime"`
	Zone          string `json:"zone"` //zone
	Disk          string `json:"disk"` //磁盘
}

type FlowScheduleResult struct {
	VolumeName    string `json:"volume"`
	OperationType string `json:"action"`
	Time          string `json:"time"`
	IpAddr        string `json:"ip"`
	Zone          string `json:"zone"`
	Disk          string `json:"disk"`
	PartitionID   uint64 `json:"pid"`
	Count         uint64 `json:"total_count"`
	Size          uint64 `json:"total_size"` // 单位byte
	AvgSize       uint64 `json:"avg_size"`
	Max           uint64 `json:"max_latency"`
	Avg           uint64 `json:"avg"`
	Tp99          uint64 `json:"tp99"`
}

type TrafficResponse struct {
	Total int                   `json:"total"`
	Data  []*FlowScheduleResult `json:"results"`
}

type TrafficDetailsResponse struct {
	Data [][]*FlowScheduleResult `json:"results"`
}
type TrafficLatencyResponse struct {
	Data []*FlowScheduleResult `json:"results"`
}

// 不需要时间范围 展示所有的数据 时间倒排 limit 10000
type HistoryCurveRequest struct {
	Cluster      string
	Volume       string
	ZoneName     string
	IntervalType int
	Start        int64
	End          int64
}

type ZombieVolResponse struct {
	Total int64
	Data  []*model.ConsoleVolumeOps
}

type QueryVolOpsRequest struct {
	Cluster string
	Period  model.ZombieVolPeriod
	Action  string
	Module  string
}

type TopVolResponse struct {
	TopInode []*model.VolumeSummaryView
	TopUsed  []*model.VolumeSummaryView
}

type AbnormalVolResponse struct {
	Total            int64
	ZombieVolCount   int64
	NoDeleteVolCount int64
}

type HostUsageDetail struct {
	DataPartitions []*PartitionOnData
	MetaPartitions []*PartitionOnMeta
	VolInodeTotal  uint64
	VolDentryTotal uint64
	Total          int64
}

type AllDataPartitions struct {
	PartitionCount int                `json:"partitionCount"`
	Partitions     []*PartitionOnData `json:"partitions"`
}

type PartitionOnData struct {
	Pid      uint64   `json:"id"`
	Volume   string   `json:"Volname"`
	Size     uint64   `json:"size"`
	Used     uint64   `json:"used"`
	Status   int      `json:"status"`
	Path     string   `json:"path"`
	Replicas []string `json:"replicas"`
	Total    string
	UsedSize string
}

type PartitionOnMeta struct {
	Pid                uint64 `json:"pid"`
	Volume             string `json:"vol"`
	DentryCount        uint64 `json:"dentryCount"`
	InodeCount         uint64 `json:"inodeCount"`
	DeletedDentryCount uint64 `json:"delDentryCount"`
	DeletedInodeCount  uint64 `json:"delInodeCount"`
}

type MetaNodeStatInfo struct {
	Zone               string  `json:"zone"`
	TotalMem           uint64  `json:"totalMem"` //单位是什么
	UsedMem            uint64  `json:"usedMem"`
	Ratio              float64 `json:"ratio"`
	MetaPartitionCount int     `json:"metaPartitionCount"`
	DentryTotalCount   uint64  `json:"dentryTotalCount"`
	InodeTotalCount    uint64  `json:"inodeTotalCount"`
}

const (
	secondsForOneHour    = 60 * 60
	secondsForOneDay     = 24 * 60 * 60
	secondsForThreeMonth = 3 * 30 * secondsForOneDay
)

func ParseHistoryCurveRequestTime(interval int, startDate, endDate int64) (start, end time.Time, err error) {
	end = time.Now()
	switch interval {
	case ResourceNoType:
		if startDate == 0 && endDate == 0 {
			err = fmt.Errorf("start and end cannot both be 0")
			return
		}
		if endDate-startDate >= int64(secondsForThreeMonth) {
			err = fmt.Errorf("时间跨度不超过3个月！")
			return
		}
		if startDate == 0 {
			startDate = endDate - int64(secondsForOneDay)
		}
		if endDate == 0 {
			endDate = end.Unix()
		}
		if startDate == endDate {
			endDate = startDate + int64(secondsForOneDay)
		}
		end = time.Unix(endDate, 0)
		start = time.Unix(startDate, 0)

	case ResourceLatestOneDay:
		start = end.AddDate(0, 0, -1)

	case ResourceLatestOneWeek:
		start = end.AddDate(0, 0, -7)

	case ResourceLatestOneMonth:
		start = end.AddDate(0, -1, 0)

	default:
		err = fmt.Errorf("undefined interval type: %v", interval)
		return
	}
	end = time.Date(end.Year(), end.Month(), end.Day(), end.Hour(), end.Minute(), 0, 0, time.Local)
	start = time.Date(start.Year(), start.Month(), start.Day(), start.Hour(), start.Minute(), 0, 0, time.Local)
	return
}

func ParseHostCurveRequestTime(interval int, startDate, endDate int64) (start, end time.Time, err error) {
	end = time.Now()
	switch interval {
	case HostUsageCurveNoType:
		if startDate == 0 && endDate == 0 {
			err = fmt.Errorf("start and end cannot both be 0")
			return
		}
		if endDate-startDate >= int64(secondsForOneDay*3) {
			err = fmt.Errorf("时间跨度不超过3天！")
			return
		}
		if startDate == 0 {
			startDate = endDate - int64(secondsForOneHour)
		}
		if endDate == 0 {
			endDate = end.Unix()
		}
		if startDate == endDate {
			endDate = startDate + int64(secondsForOneHour)
		}
		end = time.Unix(endDate, 0)
		start = time.Unix(startDate, 0)

	case HostUsageCurveOneHour:
		start = end.Add(-1 * time.Hour)

	case HostUsageCurveSixHour:
		start = end.Add(-6 * time.Hour)

	case HostUsageCurveOneDay:
		start = end.AddDate(0, 0, -1)

	default:
		err = fmt.Errorf("undefined interval type: %v", interval)
		return
	}
	end = time.Date(end.Year(), end.Month(), end.Day(), end.Hour(), end.Minute(), 0, 0, time.Local)
	start = time.Date(start.Year(), start.Month(), start.Day(), start.Hour(), start.Minute(), 0, 0, time.Local)
	return
}
