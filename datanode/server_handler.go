package datanode

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/cubefs/cubefs/util/connman"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/multirate"

	"github.com/cubefs/cubefs/datanode/riskdata"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/repl"
	"github.com/cubefs/cubefs/storage"
	"github.com/cubefs/cubefs/util/cpu"
	"github.com/cubefs/cubefs/util/diskusage"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/ttlstore"
	"github.com/cubefs/cubefs/util/unit"
	raftProto "github.com/tiglabs/raft/proto"
)

func (s *DataNode) registerHandler() {
	http.HandleFunc(proto.VersionPath, func(w http.ResponseWriter, _ *http.Request) {
		version := proto.MakeVersion("DataNode")
		marshal, _ := json.Marshal(version)
		if _, err := w.Write(marshal); err != nil {
			log.LogErrorf("write version has err:[%s]", err.Error())
		}
	})
	http.HandleFunc("/disks", s.getDiskAPI)
	http.HandleFunc("/partitions", s.getPartitionsAPI)
	http.HandleFunc("/partition", s.getPartitionAPI)
	http.HandleFunc("/partitionSimple", s.getPartitionSimpleAPI)
	http.HandleFunc("/triggerPartitionError", s.triggerPartitionError)
	http.HandleFunc("/partitionRaftHardState", s.getPartitionRaftHardStateAPI)
	http.HandleFunc("/extent", s.getExtentAPI)
	http.HandleFunc("/block", s.getBlockCrcAPI)
	http.HandleFunc("/stats", s.getStatAPI)
	http.HandleFunc("/raftStatus", s.getRaftStatus)
	http.HandleFunc("/setAutoRepairStatus", s.setAutoRepairStatus)
	http.HandleFunc("/releasePartitions", s.releasePartitions)
	http.HandleFunc("/restorePartitions", s.restorePartitions)
	http.HandleFunc("/computeExtentMd5", s.getExtentMd5Sum)
	http.HandleFunc("/stat/info", s.getStatInfo)
	http.HandleFunc("/getReplBufferDetail", s.getReplProtocalBufferDetail)
	http.HandleFunc("/tinyExtentHoleInfo", s.getTinyExtentHoleInfo)
	http.HandleFunc("/playbackTinyExtentMarkDelete", s.playbackPartitionTinyDelete)
	http.HandleFunc("/stopPartition", s.stopPartition)
	http.HandleFunc("/reloadPartition", s.reloadPartition)
	http.HandleFunc("/moveExtent", s.moveExtentFile)
	http.HandleFunc("/moveExtentBatch", s.moveExtentFileBatch)
	http.HandleFunc("/repairExtent", s.repairExtent)
	http.HandleFunc("/repairExtentBatch", s.repairExtentBatch)
	http.HandleFunc("/extentCrc", s.getExtentCrc)
	http.HandleFunc("/fingerprint", s.getFingerprint)
	http.HandleFunc("/resetFaultOccurredCheckLevel", s.resetFaultOccurredCheckLevel)
	http.HandleFunc("/sfxStatus", s.getSfxStatus)
	http.HandleFunc("/getExtentLockInfo", s.getExtentLockInfo)
	http.HandleFunc("/transferDeleteV0", s.transferDeleteV0)
	http.HandleFunc("/risk/status", s.getRiskStatus)
	http.HandleFunc("/risk/startFix", s.startRiskFix)
	http.HandleFunc("/risk/stopFix", s.stopRiskFix)
	http.HandleFunc("/getDataPartitionViewCache", s.getDataPartitionViewCache)
	http.HandleFunc("/tinyExtents", s.getTinyExtents)
	http.HandleFunc("/getRecentTrashExtents", s.getRecentTrashExtents)
	http.HandleFunc("/getRecentDeleteExtents", s.getRecentDeleteExtents)
	http.HandleFunc("/batchRecoverTrashExtents", s.batchRecoverExtents)
	http.HandleFunc("/batchDeleteTrashExtents", s.batchDeleteTrashExtents)
	http.HandleFunc("/setSettings", s.setSettings)
	http.HandleFunc("/getSettings", s.getSettings)
}

// handler
func (s *DataNode) getDiskAPI(w http.ResponseWriter, r *http.Request) {
	disks := make([]interface{}, 0)
	for _, diskItem := range s.space.GetDisks() {
		disk := &struct {
			Path        string `json:"path"`
			Total       uint64 `json:"total"`
			Used        uint64 `json:"used"`
			Available   uint64 `json:"available"`
			Unallocated uint64 `json:"unallocated"`
			Allocated   uint64 `json:"allocated"`
			Status      int    `json:"status"`
			RestSize    uint64 `json:"restSize"`
			Partitions  int    `json:"partitions"`

			IsSFX             bool   `json:"isSFX"`
			PhysicalUsedRatio uint32 `json:"physicalUsedRatio"`
			CompressionRatio  uint32 `json:"compressionRatio"`

			// Limits
			ExecutingRepairTask          uint64 `json:"executing_repair_task"`
			FixTinyDeleteRecordLimit     uint64 `json:"fix_tiny_delete_record_limit"`
			ExecutingFixTinyDeleteRecord uint64 `json:"executing_fix_tiny_delete_record"`
		}{
			Path:        diskItem.Path,
			Total:       diskItem.Total,
			Used:        diskItem.Used,
			Available:   diskItem.Available,
			Unallocated: diskItem.Unallocated,
			Allocated:   diskItem.Allocated,
			Status:      diskItem.Status,
			RestSize:    diskItem.ReservedSpace,
			Partitions:  diskItem.PartitionCount(),

			IsSFX:             diskItem.IsSfx,
			PhysicalUsedRatio: diskItem.PhysicalUsedRatio,
			CompressionRatio:  diskItem.CompressionRatio,

			FixTinyDeleteRecordLimit:     diskItem.fixTinyDeleteRecordLimit,
			ExecutingFixTinyDeleteRecord: diskItem.executingFixTinyDeleteRecord,
			ExecutingRepairTask:          diskItem.executingRepairTask,
		}
		disks = append(disks, disk)
	}
	diskReport := &struct {
		Disks []interface{} `json:"disks"`
		Zone  string        `json:"zone"`
	}{
		Disks: disks,
		Zone:  s.zoneName,
	}
	s.buildSuccessResp(w, diskReport)
}

func (s *DataNode) batchDeleteTrashExtents(w http.ResponseWriter, r *http.Request) {
	var (
		keepTime uint64
		err      error
	)
	const (
		paramAuthKey       = "key"
		paramKeepTimeSec   = "keepTimeSec"
		defaultKeepTImeSec = 60 * 60 * 24 * 7
	)
	if err = r.ParseForm(); err != nil {
		err = fmt.Errorf("parse form fail: %v", err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	key := r.FormValue(paramAuthKey)
	if !matchKey(key) {
		err = fmt.Errorf("auth key not match: %v", key)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	keepTimeSec := r.FormValue(paramKeepTimeSec)
	if keepTimeSec == "" {
		keepTime = defaultKeepTImeSec
	} else {
		var keepTimeSecVal uint64
		if keepTimeSecVal, err = strconv.ParseUint(keepTimeSec, 10, 64); err != nil {
			s.buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("parse param %v failed: %v", paramKeepTimeSec, err))
			return
		}
		keepTime = keepTimeSecVal
	}
	releaseCount := uint64(0)
	for _, d := range s.space.disks {
		releaseCount += d.deleteTrash(keepTime)
	}
	s.buildSuccessResp(w, fmt.Sprintf("release trash extents, release extent count: %v: ", releaseCount))
}

func (s *DataNode) getPartitionsAPI(w http.ResponseWriter, r *http.Request) {
	partitions := make([]interface{}, 0)
	var (
		riskCount        int
		riskFixerRunning bool
		totalTrashCount  uint64
		totalTrashSize   uint64
	)
	s.space.WalkPartitions(func(dp *DataPartition) bool {
		partition := &struct {
			ID                    uint64                  `json:"id"`
			Size                  int                     `json:"size"`
			Used                  int                     `json:"used"`
			Status                int                     `json:"status"`
			Path                  string                  `json:"path"`
			Replicas              []string                `json:"replicas"`
			NeedServerFaultCheck  bool                    `json:"needServerFaultCheck"`
			ServerFaultCheckLevel FaultOccurredCheckLevel `json:"serverFaultCheckLevel"`
			RiskFixerStatus       *riskdata.FixerStatus   `json:"riskFixerStatus"`
			TrashCount            int                     `json:"trashCount"`
			TrashSize             uint64                  `json:"trashSize"`
		}{
			ID:                    dp.ID(),
			Size:                  dp.Size(),
			Used:                  dp.Used(),
			Status:                dp.Status(),
			Path:                  dp.Path(),
			Replicas:              dp.getReplicaClone(),
			ServerFaultCheckLevel: dp.serverFaultCheckLevel,
			RiskFixerStatus: func() *riskdata.FixerStatus {
				if fixer := dp.RiskFixer(); fixer != nil {
					status := fixer.Status()
					riskCount += status.Count
					if status.Running {
						riskFixerRunning = true
					}
					return status
				}
				return nil
			}(),
			TrashCount: dp.trashCount,
			TrashSize:  dp.trashSize,
		}
		partitions = append(partitions, partition)
		totalTrashCount += uint64(dp.trashCount)
		totalTrashSize += dp.trashSize
		return true
	})
	result := &struct {
		Partitions       []interface{} `json:"partitions"`
		PartitionCount   int           `json:"partitionCount"`
		RiskCount        int           `json:"riskCount"`
		RiskFixerRunning bool          `json:"riskFixerRunning"`
		TotalTrashCount  uint64        `json:"totalTrashCount"`
		TotalTrashSize   uint64        `json:"totalTrashSize"`
	}{
		Partitions:       partitions,
		PartitionCount:   len(partitions),
		RiskCount:        riskCount,
		RiskFixerRunning: riskFixerRunning,
		TotalTrashCount:  totalTrashCount,
		TotalTrashSize:   totalTrashSize,
	}
	s.buildSuccessResp(w, result)
}

func (s *DataNode) getPartitionAPI(w http.ResponseWriter, r *http.Request) {
	const (
		paramPartitionID = "id"
	)
	var (
		partitionID uint64
		files       []storage.ExtentInfoBlock
		err         error
		dpInfo      *DataPartitionViewInfo
	)
	if err = r.ParseForm(); err != nil {
		err = fmt.Errorf("parse form fail: %v", err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if partitionID, err = strconv.ParseUint(r.FormValue(paramPartitionID), 10, 64); err != nil {
		err = fmt.Errorf("parse param %v fail: %v", paramPartitionID, err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	partition := s.space.Partition(partitionID)
	if partition == nil {
		s.buildFailureResp(w, http.StatusNotFound, "partition not exist")
		return
	}
	if dpInfo, err = partition.getDataPartitionInfo(); err != nil {
		s.buildFailureResp(w, http.StatusNotFound, "data partition info get failed")
		return
	}
	if files, err = partition.ExtentStore().GetAllWatermarks(proto.AllExtentType, nil); err != nil {
		err = fmt.Errorf("get watermark fail: %v", err)
		s.buildFailureResp(w, http.StatusInternalServerError, err.Error())
		return
	}
	dpInfo.Files = files
	dpInfo.FileCount = len(files)
	s.buildSuccessResp(w, dpInfo)
}

func (s *DataNode) getPartitionSimpleAPI(w http.ResponseWriter, r *http.Request) {
	const (
		paramPartitionID = "id"
	)
	var (
		partitionID uint64
		err         error
		dpInfo      *DataPartitionViewInfo
	)
	if err = r.ParseForm(); err != nil {
		err = fmt.Errorf("parse form fail: %v", err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}

	if partitionID, err = strconv.ParseUint(r.FormValue(paramPartitionID), 10, 64); err != nil {
		err = fmt.Errorf("parse param %v fail: %v", paramPartitionID, err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	partition := s.space.Partition(partitionID)
	if partition == nil {
		s.buildFailureResp(w, http.StatusNotFound, "partition not exist")
		return
	}
	if dpInfo, err = partition.getDataPartitionInfo(); err != nil {
		s.buildFailureResp(w, http.StatusNotFound, "data partition info get failed")
		return
	}
	s.buildSuccessResp(w, dpInfo)
}

func (s *DataNode) triggerPartitionError(w http.ResponseWriter, r *http.Request) {
	const (
		paramPartitionID = "id"
		paramIsDiskError = "isDiskError"
		paramAuthCode    = "authCode"
	)
	var (
		err         error
		partitionID uint64
		authCode    string
		isDiskError bool
	)
	if err = r.ParseForm(); err != nil {
		err = fmt.Errorf("parse from fail: %v", err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if partitionID, err = strconv.ParseUint(r.FormValue(paramPartitionID), 10, 64); err != nil {
		err = fmt.Errorf("parse param %v fail: %v", paramPartitionID, err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	authCode = r.FormValue(paramAuthCode)
	if val := r.FormValue(paramIsDiskError); val != "" {
		var bVal bool
		if bVal, err = strconv.ParseBool(val); err != nil {
			s.buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("parse param %v fail: %v", paramIsDiskError, err))
			return
		}
		isDiskError = bVal
	}
	partition := s.space.Partition(partitionID)
	if partition == nil {
		s.buildFailureResp(w, http.StatusNotFound, "partition not exist")
		return
	}
	if authCode != hex.EncodeToString(md5.New().Sum([]byte(partition.volumeID))) {
		s.buildFailureResp(w, http.StatusBadRequest, "authCode mismatch")
		return
	}

	var partitionErr error
	if isDiskError {
		partitionErr = syscall.EIO
	} else {
		partitionErr = storage.NewParameterMismatchErr("parameter mismatch")
	}
	partition.checkIsPartitionError(partitionErr)
	s.buildSuccessResp(w, nil)
}

func (s *DataNode) getPartitionRaftHardStateAPI(w http.ResponseWriter, r *http.Request) {
	var (
		partitionID uint64
		err         error
	)
	if err = r.ParseForm(); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if partitionID, err = strconv.ParseUint(r.FormValue("partitionID"), 10, 64); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	var partition = s.space.Partition(partitionID)
	if partition == nil {
		s.buildFailureResp(w, http.StatusNotFound, "partition not exist")
		return
	}
	var hs raftProto.HardState
	if hs, err = partition.RaftHardState(); err != nil {
		s.buildFailureResp(w, http.StatusInternalServerError, err.Error())
		return
	}
	s.buildSuccessResp(w, hs)
	return
}

func (s *DataNode) getExtentAPI(w http.ResponseWriter, r *http.Request) {
	var (
		partitionID uint64
		extentID    int
		err         error
		extentInfo  *storage.ExtentInfoBlock
	)
	if err = r.ParseForm(); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if partitionID, err = strconv.ParseUint(r.FormValue("partitionID"), 10, 64); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if extentID, err = strconv.Atoi(r.FormValue("extentID")); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	partition := s.space.Partition(partitionID)
	if partition == nil {
		s.buildFailureResp(w, http.StatusNotFound, "partition not exist")
		return
	}
	if extentInfo, err = partition.ExtentStore().Watermark(uint64(extentID)); err != nil {
		s.buildFailureResp(w, 500, err.Error())
		return
	}

	s.buildSuccessResp(w, extentInfo)
	return
}

func (s *DataNode) getBlockCrcAPI(w http.ResponseWriter, r *http.Request) {
	var (
		partitionID uint64
		extentID    int
		err         error
		blocks      []*storage.BlockCrc
	)
	if err = r.ParseForm(); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if partitionID, err = strconv.ParseUint(r.FormValue("partitionID"), 10, 64); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if extentID, err = strconv.Atoi(r.FormValue("extentID")); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if proto.IsTinyExtent(uint64(extentID)) {
		s.buildFailureResp(w, http.StatusBadRequest, "can not query tiny extent")
		return
	}
	partition := s.space.Partition(partitionID)
	if partition == nil {
		s.buildFailureResp(w, http.StatusNotFound, "partition not exist")
		return
	}
	if blocks, err = partition.ExtentStore().ScanBlocks(uint64(extentID)); err != nil {
		s.buildFailureResp(w, 500, err.Error())
		return
	}

	s.buildSuccessResp(w, blocks)
	return
}

func (s *DataNode) getStatAPI(w http.ResponseWriter, r *http.Request) {
	response := &proto.DataNodeHeartbeatResponse{}
	s.buildHeartBeatResponse(response)

	s.buildSuccessResp(w, response)
}

func (s *DataNode) getRaftStatus(w http.ResponseWriter, r *http.Request) {
	const (
		paramRaftID = "raftID"
	)
	if err := r.ParseForm(); err != nil {
		err = fmt.Errorf("parse form fail: %v", err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	raftID, err := strconv.ParseUint(r.FormValue(paramRaftID), 10, 64)
	if err != nil {
		err = fmt.Errorf("parse param %v fail: %v", paramRaftID, err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	raftStatus := s.raftStore.RaftStatus(raftID)
	s.buildSuccessResp(w, raftStatus)
}

func (s *DataNode) setAutoRepairStatus(w http.ResponseWriter, r *http.Request) {
	const (
		paramAutoRepair = "autoRepair"
	)
	if err := r.ParseForm(); err != nil {
		err = fmt.Errorf("parse form fail: %v", err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	autoRepair, err := strconv.ParseBool(r.FormValue(paramAutoRepair))
	if err != nil {
		err = fmt.Errorf("parse param %v fail: %v", paramAutoRepair, err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	AutoRepairStatus = autoRepair
	s.buildSuccessResp(w, autoRepair)
}

/*
 * release the space of mark delete data partitions of the whole node.
 */
func (s *DataNode) releasePartitions(w http.ResponseWriter, r *http.Request) {
	const (
		paramAuthKey     = "key"
		paramKeepTimeSec = "keepTimeSec"

		defaultKeepTImeSec = 60 * 60 * 24 // 24 Hours
	)
	var (
		successVols []string
		failedVols  []string
		failedDisks []string
	)
	successVols = make([]string, 0)
	failedVols = make([]string, 0)
	failedDisks = make([]string, 0)
	if err := r.ParseForm(); err != nil {
		err = fmt.Errorf("parse form fail: %v", err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	key := r.FormValue(paramAuthKey)
	if !matchKey(key) {
		err := fmt.Errorf("auth key not match: %v", key)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	var keepTimeSec = defaultKeepTImeSec
	if keepTimeSecVal := r.FormValue(paramKeepTimeSec); len(keepTimeSecVal) > 0 {
		if pared, err := strconv.Atoi(keepTimeSecVal); err != nil {
			s.buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("parse param %v failed: %v", paramKeepTimeSec, err))
			return
		} else {
			keepTimeSec = pared
		}
	}

	// Format: expired_datapartition_{ID}_{Capacity}_{Timestamp}
	// Regexp: ^expired_datapartition_(\d)+_(\d)+_(\d)+$
	var regexpExpiredPartitionDirName = regexp.MustCompile("^expired_datapartition_(\\d)+_(\\d)+_(\\d)+$")
	var keepTimeDuring = time.Second * time.Duration(keepTimeSec)
	var now = time.Now()

	for _, d := range s.space.disks {
		fList, err := ioutil.ReadDir(d.Path)
		if err != nil {
			failedDisks = append(failedDisks, d.Path)
			continue
		}

		for _, fInfo := range fList {
			if !fInfo.IsDir() {
				continue
			}
			var filename = fInfo.Name()
			if !regexpExpiredPartitionDirName.MatchString(filename) {
				continue
			}
			var parts = strings.Split(filename, "_")
			var timestamp uint64
			if timestamp, err = strconv.ParseUint(parts[len(parts)-1], 10, 64); err != nil {
				failedVols = append(failedVols, d.Path+":"+fInfo.Name())
				continue
			}
			var expiredTime = time.Unix(int64(timestamp), 0)
			if expiredTime.Add(keepTimeDuring).After(now) {
				continue
			}
			err = os.RemoveAll(d.Path + "/" + fInfo.Name())
			if err != nil {
				failedVols = append(failedVols, d.Path+":"+fInfo.Name())
				continue
			}
			successVols = append(successVols, d.Path+":"+fInfo.Name())
		}
	}
	s.buildSuccessResp(w, fmt.Sprintf("release partitions, success partitions: %v, failed partitions: %v, failed disks: %v", successVols, failedVols, failedDisks))
}

func matchKey(key string) bool {
	return key == generateAuthKey()
}

func generateAuthKey() string {
	date := time.Now().Format("2006-01-02 15")
	h := md5.New()
	h.Write([]byte(date))
	cipherStr := h.Sum(nil)
	return hex.EncodeToString(cipherStr)
}

func (s *DataNode) restorePartitions(w http.ResponseWriter, r *http.Request) {
	const (
		paramAuthKey = "key"
		paramIdKey   = "id"
	)
	var (
		err         error
		all         bool
		successDps  []uint64
		failedDisks []string
		failedDps   []uint64
	)
	ids := make(map[uint64]bool)
	key := r.FormValue(paramAuthKey)
	if !matchKey(key) {
		err = fmt.Errorf("auth key not match: %v", key)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	idVal := r.FormValue(paramIdKey)
	if idVal == "all" {
		all = true
	} else {
		allId := strings.Split(idVal, ",")
		for _, val := range allId {
			id, err := strconv.ParseUint(val, 10, 64)
			if err != nil {
				continue
			}
			ids[id] = true
		}
	}
	failedDisks, failedDps, successDps = s.space.RestoreExpiredPartitions(all, ids)
	s.buildSuccessResp(w, fmt.Sprintf("restore partitions, success partitions: %v, failed partitions: %v, failed disks: %v", successDps, failedDps, failedDisks))
}

func (s *DataNode) getExtentMd5Sum(w http.ResponseWriter, r *http.Request) {
	const (
		paramPartitionID = "id"
		paramExtentID    = "extent"
		paramOffset      = "offset"
		paramSize        = "size"
	)
	var (
		err                                 error
		partitionID, extentID, offset, size uint64
		md5Sum                              string
	)
	if err = r.ParseForm(); err != nil {
		err = fmt.Errorf("parse form fail: %v", err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if partitionID, err = strconv.ParseUint(r.FormValue(paramPartitionID), 10, 64); err != nil {
		err = fmt.Errorf("parse param %v fail: %v", paramPartitionID, err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if extentID, err = strconv.ParseUint(r.FormValue(paramExtentID), 10, 64); err != nil {
		err = fmt.Errorf("parse param %v fail: %v", paramExtentID, err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if r.FormValue(paramOffset) != "" {
		if offset, err = strconv.ParseUint(r.FormValue(paramOffset), 10, 64); err != nil {
			err = fmt.Errorf("parse param %v fail: %v", paramOffset, err)
			s.buildFailureResp(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	if r.FormValue(paramSize) != "" {
		if size, err = strconv.ParseUint(r.FormValue(paramSize), 10, 64); err != nil {
			err = fmt.Errorf("parse param %v fail: %v", paramSize, err)
			s.buildFailureResp(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	partition := s.space.Partition(partitionID)
	if partition == nil {
		s.buildFailureResp(w, http.StatusNotFound, fmt.Sprintf("partition(%v) not exist", partitionID))
		return
	}
	if !partition.ExtentStore().IsExists(extentID) {
		s.buildFailureResp(w, http.StatusNotFound, fmt.Sprintf("partition(%v) extentID(%v) not exist", partitionID, extentID))
		return
	}
	md5Sum, err = partition.ExtentStore().ComputeMd5Sum(extentID, offset, size)
	if err != nil {
		s.buildFailureResp(w, http.StatusInternalServerError, fmt.Sprintf("partition(%v) extentID(%v) computeMD5 failed %v", partitionID, extentID, err))
		return
	}
	result := &struct {
		PartitionID uint64 `json:"PartitionID"`
		ExtentID    uint64 `json:"ExtentID"`
		Md5Sum      string `json:"md5"`
	}{
		PartitionID: partitionID,
		ExtentID:    extentID,
		Md5Sum:      md5Sum,
	}
	s.buildSuccessResp(w, result)
}

func (s *DataNode) getStatInfo(w http.ResponseWriter, r *http.Request) {
	if s.processStatInfo == nil {
		s.buildFailureResp(w, http.StatusBadRequest, "data node is initializing")
		return
	}
	//get process stat info
	cpuUsageList, maxCPUUsage := s.processStatInfo.GetProcessCPUStatInfo()
	_, memoryUsedGBList, maxMemoryUsedGB, maxMemoryUsage := s.processStatInfo.GetProcessMemoryStatInfo()
	//get disk info
	disks := s.space.GetDisks()
	diskList := make([]interface{}, 0, len(disks))
	for _, disk := range disks {
		diskTotal, err := diskusage.GetDiskTotal(disk.Path)
		if err != nil {
			diskTotal = disk.Total
		}
		diskInfo := &struct {
			Path          string  `json:"path"`
			TotalTB       float64 `json:"totalTB"`
			UsedGB        float64 `json:"usedGB"`
			UsedRatio     float64 `json:"usedRatio"`
			ReservedSpace uint    `json:"reservedSpaceGB"`
		}{
			Path:          disk.Path,
			TotalTB:       unit.FixedPoint(float64(diskTotal)/unit.TB, 1),
			UsedGB:        unit.FixedPoint(float64(disk.Used)/unit.GB, 1),
			UsedRatio:     unit.FixedPoint(float64(disk.Used)/float64(diskTotal), 1),
			ReservedSpace: uint(disk.ReservedSpace / unit.GB),
		}
		diskList = append(diskList, diskInfo)
	}
	result := &struct {
		Type           string         `json:"type"`
		Zone           string         `json:"zone"`
		Version        interface{}    `json:"versionInfo"`
		StartTime      string         `json:"startTime"`
		CPUUsageList   []float64      `json:"cpuUsageList"`
		MaxCPUUsage    float64        `json:"maxCPUUsage"`
		CPUCoreNumber  int            `json:"cpuCoreNumber"`
		MemoryUsedList []float64      `json:"memoryUsedGBList"`
		MaxMemoryUsed  float64        `json:"maxMemoryUsedGB"`
		MaxMemoryUsage float64        `json:"maxMemoryUsage"`
		DiskInfo       []interface{}  `json:"diskInfo"`
		ConnmanStats   *connman.Stats `json:"connmanStats"`
	}{
		Type:           ModuleName,
		Zone:           s.zoneName,
		Version:        proto.MakeVersion("DataNode"),
		StartTime:      s.processStatInfo.ProcessStartTime,
		CPUUsageList:   cpuUsageList,
		MaxCPUUsage:    maxCPUUsage,
		CPUCoreNumber:  cpu.GetCPUCoreNumber(),
		MemoryUsedList: memoryUsedGBList,
		MaxMemoryUsed:  maxMemoryUsedGB,
		MaxMemoryUsage: maxMemoryUsage,
		DiskInfo:       diskList,
		ConnmanStats:   gConnPool.Stats(),
	}
	s.buildSuccessResp(w, result)
}

func (s *DataNode) getReplProtocalBufferDetail(w http.ResponseWriter, r *http.Request) {
	allReplDetail := repl.GetReplProtocolDetail()
	s.buildSuccessResp(w, allReplDetail)
	return
}

func (s *DataNode) getTinyExtentHoleInfo(w http.ResponseWriter, r *http.Request) {
	var (
		partitionID uint64
		extentID    int
		err         error
	)
	if err = r.ParseForm(); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if partitionID, err = strconv.ParseUint(r.FormValue("partitionID"), 10, 64); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if extentID, err = strconv.Atoi(r.FormValue("extentID")); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	partition := s.space.Partition(partitionID)
	if partition == nil {
		s.buildFailureResp(w, http.StatusNotFound, "partition not exist")
		return
	}
	result, err := partition.getTinyExtentHoleInfo(uint64(extentID))
	if err != nil {
		s.buildFailureResp(w, http.StatusNotFound, err.Error())
		return
	}
	s.buildSuccessResp(w, result)
}

// 这个API用于回放指定Partition的TINYEXTENT_DELETE记录，为该Partition下的所有TINY EXTENT重新打洞
func (s *DataNode) playbackPartitionTinyDelete(w http.ResponseWriter, r *http.Request) {
	var (
		partitionID uint64
		count       uint64
		err         error
	)
	if err = r.ParseForm(); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if partitionID, err = strconv.ParseUint(r.FormValue("partitionID"), 10, 64); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	partition := s.space.Partition(partitionID)
	if partition == nil {
		s.buildFailureResp(w, http.StatusNotFound, "partition not exist")
		return
	}
	countStr := r.FormValue("count")
	if countStr != "" {
		if count, err = strconv.ParseUint(countStr, 10, 64); err != nil {
			s.buildFailureResp(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	store := partition.ExtentStore()
	if err = store.PlaybackTinyDelete(int64(count * storage.DeleteTinyRecordSize)); err != nil {
		s.buildFailureResp(w, http.StatusInternalServerError, err.Error())
		return
	}
	s.buildSuccessResp(w, nil)
}

func (s *DataNode) stopPartition(w http.ResponseWriter, r *http.Request) {
	var (
		partitionID uint64
		err         error
	)
	if err = r.ParseForm(); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if partitionID, err = strconv.ParseUint(r.FormValue("partitionID"), 10, 64); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}

	if err = s.stopPartitionById(partitionID); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}

	s.buildSuccessResp(w, nil)
}

func (s *DataNode) stopPartitionById(partitionID uint64) (err error) {
	partition := s.space.Partition(partitionID)
	if partition == nil {
		err = fmt.Errorf("partition[%d] not exist", partitionID)
		return
	}
	partition.Disk().space.DetachDataPartition(partition.ID())
	partition.Disk().DetachDataPartition(partition)
	partition.Stop()
	return nil
}

func (s *DataNode) reloadPartition(w http.ResponseWriter, r *http.Request) {
	var (
		partitionPath string
		disk          string
		err           error
	)
	if err = r.ParseForm(); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}

	if disk = r.FormValue("disk"); disk == "" {
		s.buildFailureResp(w, http.StatusBadRequest, "param disk is empty")
		return
	}

	if partitionPath = r.FormValue("partitionPath"); partitionPath == "" {
		s.buildFailureResp(w, http.StatusBadRequest, "param partitionPath is empty")
		return
	}

	if err = s.reloadPartitionByName(partitionPath, disk); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}

	s.buildSuccessResp(w, nil)
}

func (s *DataNode) reloadPartitionByName(partitionPath, disk string) (err error) {
	var (
		partition   *DataPartition
		partitionID uint64
	)

	var diskPath, ok = ParseDiskPath(disk)
	if !ok {
		err = fmt.Errorf("illegal disk path: %v", disk)
		return
	}

	var d, exists = s.space.GetDisk(diskPath.Path())
	if !exists {
		err = fmt.Errorf("disk no exists: %v", disk)
		return
	}

	if partitionID, _, err = unmarshalPartitionName(partitionPath); err != nil {
		err = fmt.Errorf("action[reloadPartition] unmarshal partitionName(%v) from disk(%v) err(%v) ",
			partitionPath, disk, err.Error())
		return
	}

	partition = s.space.Partition(partitionID)
	if partition != nil {
		err = fmt.Errorf("partition[%d] exist, can not reload", partitionID)
		return
	}

	if err = s.space.LoadPartition(d, partitionID, partitionPath, NormalRestorePartition); err != nil {
		return
	}
	return
}

func (s *DataNode) moveExtentFile(w http.ResponseWriter, r *http.Request) {
	const (
		paramPath        = "path"
		paramPartitionID = "partition"
		paramExtentID    = "extent"
	)
	var (
		err            error
		partitionID    uint64
		extentID       uint64
		pathStr        string
		partitionIDStr string
		extentIDStr    string
	)
	defer func() {
		if err != nil {
			s.buildFailureResp(w, http.StatusInternalServerError, err.Error())
			log.LogErrorf("move extent %s/%s failed:%s", pathStr, extentIDStr, err.Error())
		}
	}()

	pathStr = r.FormValue(paramPath)
	partitionIDStr = r.FormValue(paramPartitionID)
	extentIDStr = r.FormValue(paramExtentID)
	log.LogWarnf("move extent %s/%s begin", pathStr, extentIDStr)

	if len(partitionIDStr) == 0 || len(extentIDStr) == 0 || len(pathStr) == 0 {
		err = fmt.Errorf("need param [%s %s %s]", paramPath, paramPartitionID, paramExtentID)
		return
	}

	if partitionID, err = strconv.ParseUint(partitionIDStr, 10, 64); err != nil {
		err = fmt.Errorf("parse param %v fail: %v", paramPartitionID, err)
		return
	}
	if extentID, err = strconv.ParseUint(extentIDStr, 10, 64); err != nil {
		err = fmt.Errorf("parse param %v fail: %v", paramExtentID, err)
		return
	}

	if err = s.moveExtentFileToBackup(pathStr, partitionID, extentID); err != nil {
		return
	}

	log.LogWarnf("move extent %s/%s success", pathStr, extentIDStr)
	s.buildSuccessResp(w, nil)
	return
}

func (s *DataNode) moveExtentFileBatch(w http.ResponseWriter, r *http.Request) {
	const (
		paramPath        = "path"
		paramPartitionID = "partition"
		paramExtentID    = "extent"
	)
	var (
		err            error
		partitionID    uint64
		pathStr        string
		partitionIDStr string
		extentIDStr    string
		resultMap      map[uint64]string
	)
	defer func() {
		if err != nil {
			s.buildFailureResp(w, http.StatusInternalServerError, err.Error())
			log.LogWarnf("move extent batch partition:%s extents:%s failed:%s", pathStr, extentIDStr, err.Error())
		}
	}()

	resultMap = make(map[uint64]string)
	pathStr = r.FormValue(paramPath)
	partitionIDStr = r.FormValue(paramPartitionID)
	extentIDStr = r.FormValue(paramExtentID)
	extentIDArrayStr := strings.Split(extentIDStr, "-")
	log.LogWarnf("move extent batch partition:%s extents:%s begin", pathStr, extentIDStr)

	if len(partitionIDStr) == 0 || len(extentIDArrayStr) == 0 || len(pathStr) == 0 {
		err = fmt.Errorf("need param [%s %s %s]", paramPath, paramPartitionID, paramExtentID)
		return
	}

	if partitionID, err = strconv.ParseUint(partitionIDStr, 10, 64); err != nil {
		err = fmt.Errorf("parse param %v fail: %v", paramPartitionID, err)
		return
	}
	for _, idStr := range extentIDArrayStr {
		var ekId uint64
		if ekId, err = strconv.ParseUint(idStr, 10, 64); err != nil {
			err = fmt.Errorf("parse param %v fail: %v", paramExtentID, err)
			return
		}
		resultMap[ekId] = fmt.Sprintf("partition:%d extent:%d not start", partitionID, ekId)
	}

	for _, idStr := range extentIDArrayStr {
		ekId, _ := strconv.ParseUint(idStr, 10, 64)
		if err = s.moveExtentFileToBackup(pathStr, partitionID, ekId); err != nil {
			resultMap[ekId] = err.Error()
			log.LogErrorf("repair extent %s/%s failed", pathStr, idStr)
		} else {
			resultMap[ekId] = "OK"
			log.LogWarnf("repair extent %s/%s success", pathStr, idStr)
		}
	}
	err = nil
	log.LogWarnf("move extent batch partition:%s extents:%s success", pathStr, extentIDStr)
	s.buildSuccessResp(w, resultMap)
	return
}

func (s *DataNode) moveExtentFileToBackup(pathStr string, partitionID, extentID uint64) (err error) {
	const (
		repairDirStr = "repair_extents_backup"
	)
	rootPath := pathStr[:strings.LastIndex(pathStr, "/")]
	extentFilePath := path.Join(pathStr, strconv.Itoa(int(extentID)))
	now := time.Now()
	repairDirPath := path.Join(rootPath, repairDirStr, fmt.Sprintf("%d-%d-%d", now.Year(), now.Month(), now.Day()))
	os.MkdirAll(repairDirPath, 0655)
	fileInfo, err := os.Stat(repairDirPath)
	if err != nil || !fileInfo.IsDir() {
		err = fmt.Errorf("path[%s] is not exist or is not dir", repairDirPath)
		return
	}
	repairBackupFilePath := path.Join(repairDirPath, fmt.Sprintf("%d_%d_%d", partitionID, extentID, now.Unix()))
	log.LogWarnf("rename file[%s-->%s]", extentFilePath, repairBackupFilePath)
	if err = os.Rename(extentFilePath, repairBackupFilePath); err != nil {
		return
	}

	file, err := os.OpenFile(extentFilePath, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0666)
	if err != nil {
		return
	}
	file.Close()
	return nil
}

func (s *DataNode) repairExtent(w http.ResponseWriter, r *http.Request) {
	const (
		paramPath        = "path"
		paramPartitionID = "partition"
		paramExtentID    = "extent"
	)
	var (
		err              error
		partitionID      uint64
		extentID         uint64
		hasStopPartition bool
		pathStr          string
		partitionIDStr   string
		extentIDStr      string
		lastSplitIndex   int
	)
	defer func() {
		if err != nil {
			s.buildFailureResp(w, http.StatusInternalServerError, err.Error())
			log.LogErrorf("repair extent batch partition: %s extents[%s] failed:%s", pathStr, extentIDStr, err.Error())
		}

		if hasStopPartition {
			s.reloadPartitionByName(pathStr[lastSplitIndex+1:], pathStr[:lastSplitIndex])
		}

	}()

	hasStopPartition = false
	pathStr = r.FormValue(paramPath)
	partitionIDStr = r.FormValue(paramPartitionID)
	extentIDStr = r.FormValue(paramExtentID)

	if len(partitionIDStr) == 0 || len(extentIDStr) == 0 || len(pathStr) == 0 {
		err = fmt.Errorf("need param [%s %s %s]", paramPath, paramPartitionID, paramExtentID)
		return
	}

	if partitionID, err = strconv.ParseUint(partitionIDStr, 10, 64); err != nil {
		err = fmt.Errorf("parse param %v fail: %v", paramPartitionID, err)
		return
	}
	if extentID, err = strconv.ParseUint(extentIDStr, 10, 64); err != nil {
		err = fmt.Errorf("parse param %v fail: %v", paramExtentID, err)
		return
	}
	lastSplitIndex = strings.LastIndex(pathStr, "/")
	log.LogWarnf("repair extent %s/%s begin", pathStr, extentIDStr)

	if err = s.stopPartitionById(partitionID); err != nil {
		return
	}
	hasStopPartition = true

	if err = s.moveExtentFileToBackup(pathStr, partitionID, extentID); err != nil {
		return
	}

	hasStopPartition = false
	if err = s.reloadPartitionByName(pathStr[lastSplitIndex+1:], pathStr[:lastSplitIndex]); err != nil {
		return
	}
	log.LogWarnf("repair extent %s/%s success", pathStr, extentIDStr)
	s.buildSuccessResp(w, nil)
	return
}

func (s *DataNode) repairExtentBatch(w http.ResponseWriter, r *http.Request) {
	const (
		paramPath        = "path"
		paramPartitionID = "partition"
		paramExtentID    = "extent"
	)
	var (
		err              error
		partitionID      uint64
		hasStopPartition bool
		pathStr          string
		partitionIDStr   string
		extentIDStr      string
		resultMap        map[uint64]string
		lastSplitIndex   int
	)
	defer func() {
		if err != nil {
			s.buildFailureResp(w, http.StatusInternalServerError, err.Error())
			log.LogErrorf("repair extent batch partition: %s extents[%s] failed:%s", pathStr, extentIDStr, err.Error())
		}

		if hasStopPartition {
			s.reloadPartitionByName(pathStr[lastSplitIndex+1:], pathStr[:lastSplitIndex])
		}
	}()

	resultMap = make(map[uint64]string)
	hasStopPartition = false
	pathStr = r.FormValue(paramPath)
	partitionIDStr = r.FormValue(paramPartitionID)
	extentIDStr = r.FormValue(paramExtentID)
	extentIDArrayStr := strings.Split(extentIDStr, "-")

	if len(partitionIDStr) == 0 || len(extentIDArrayStr) == 0 || len(pathStr) == 0 {
		err = fmt.Errorf("need param [%s %s %s]", paramPath, paramPartitionID, paramExtentID)
		return
	}

	if partitionID, err = strconv.ParseUint(partitionIDStr, 10, 64); err != nil {
		err = fmt.Errorf("parse param %v fail: %v", paramPartitionID, err)
		return
	}
	for _, idStr := range extentIDArrayStr {
		var ekId uint64
		if ekId, err = strconv.ParseUint(idStr, 10, 64); err != nil {
			err = fmt.Errorf("parse param %v fail: %v", paramExtentID, err)
			return
		}
		resultMap[ekId] = fmt.Sprintf("partition:%d extent:%d not start", partitionID, ekId)
	}
	lastSplitIndex = strings.LastIndex(pathStr, "/")
	log.LogWarnf("repair extent batch partition: %s extents[%s] begin", pathStr, extentIDStr)

	if err = s.stopPartitionById(partitionID); err != nil {
		return
	}
	hasStopPartition = true

	for _, idStr := range extentIDArrayStr {
		ekId, _ := strconv.ParseUint(idStr, 10, 64)
		if err = s.moveExtentFileToBackup(pathStr, partitionID, ekId); err != nil {
			resultMap[ekId] = err.Error()
			log.LogErrorf("repair extent %s/%s failed:%s", pathStr, idStr, err.Error())
		} else {
			resultMap[ekId] = "OK"
			log.LogWarnf("repair extent %s/%s success", pathStr, idStr)
		}
	}
	err = nil
	hasStopPartition = false
	if err = s.reloadPartitionByName(pathStr[lastSplitIndex+1:], pathStr[:lastSplitIndex]); err != nil {
		return
	}
	log.LogWarnf("repair extent batch partition: %s extents[%s] success", pathStr, extentIDStr)
	s.buildSuccessResp(w, resultMap)
	return
}

func (s *DataNode) getExtentCrc(w http.ResponseWriter, r *http.Request) {
	var (
		partitionID uint64
		extentID    uint64
		err         error
	)
	if err = r.ParseForm(); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if partitionID, err = strconv.ParseUint(r.FormValue("partitionId"), 10, 64); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if extentID, err = strconv.ParseUint(r.FormValue("extentId"), 10, 64); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	log.LogDebugf("getExtentCrc partitionID(%v) extentID(%v)", partitionID, extentID)
	partition := s.space.Partition(partitionID)
	if partition == nil {
		s.buildFailureResp(w, http.StatusNotFound, "partition not exist")
		return
	}
	store := partition.ExtentStore()
	crc, err := store.GetExtentCrc(extentID)
	if err != nil {
		log.LogErrorf("GetExtentCrc err(%v)", err)
		s.buildFailureResp(w, http.StatusInternalServerError, err.Error())
		return
	}

	result := &struct {
		CRC uint32
	}{
		CRC: crc,
	}
	s.buildSuccessResp(w, result)
}

func (s *DataNode) getFingerprint(w http.ResponseWriter, r *http.Request) {
	var (
		partitionID uint64
		extentID    uint64
		strict      bool
		err         error
	)
	if err = r.ParseForm(); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if partitionID, err = strconv.ParseUint(r.FormValue("partitionId"), 10, 64); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if extentID, err = strconv.ParseUint(r.FormValue("extentId"), 10, 64); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	strict, _ = strconv.ParseBool(r.FormValue("strict"))
	partition := s.space.Partition(partitionID)
	if partition == nil {
		s.buildFailureResp(w, http.StatusNotFound, "partition not exist")
		return
	}
	store := partition.ExtentStore()

	var eib *storage.ExtentInfoBlock
	if eib, err = store.Watermark(extentID); err != nil {
		s.buildFailureResp(w, http.StatusInternalServerError, err.Error())
		return
	}
	var fingerprint storage.Fingerprint
	if fingerprint, err = store.Fingerprint(extentID, 0, int64(eib[storage.Size]), strict); err != nil {
		s.buildFailureResp(w, http.StatusInternalServerError, err.Error())
		return
	}

	result := &struct {
		Fingerprint storage.Fingerprint
	}{
		Fingerprint: fingerprint,
	}
	s.buildSuccessResp(w, result)
}

func (s *DataNode) resetFaultOccurredCheckLevel(w http.ResponseWriter, r *http.Request) {
	var (
		partitionID       uint64
		partitionStr      string
		level             uint64
		checkCorruptLevel FaultOccurredCheckLevel
		err               error
	)
	if err = r.ParseForm(); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if level, err = strconv.ParseUint(r.FormValue("level"), 10, 64); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	checkCorruptLevel, err = convertCheckCorruptLevel(level)
	if err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	partitionStr = r.FormValue("partitionID")
	if partitionStr != "" {
		if partitionID, err = strconv.ParseUint(partitionStr, 10, 64); err != nil {
			s.buildFailureResp(w, http.StatusBadRequest, err.Error())
			return
		}
		partition := s.space.Partition(partitionID)
		if partition == nil {
			s.buildFailureResp(w, http.StatusNotFound, "partition not exist")
			return
		}
		partition.setFaultOccurredCheckLevel(checkCorruptLevel)
	} else {
		s.space.WalkPartitions(func(partition *DataPartition) bool {
			partition.setFaultOccurredCheckLevel(checkCorruptLevel)
			return true
		})
	}
	s.buildSuccessResp(w, "success")
}

func (s *DataNode) getSfxStatus(w http.ResponseWriter, r *http.Request) {
	disks := make([]interface{}, 0)
	for _, diskItem := range s.space.GetDisks() {
		disk := &struct {
			Path              string `json:"path"`
			IsSfx             bool   `json:"IsSfx"`
			DevName           string `json:"devName"`
			PhysicalUsedRatio uint32 `json:"PhysicalUsedRatio"`
			CompressionRatio  uint32 `json:"CompressionRatio"`
		}{
			Path:              diskItem.Path,
			IsSfx:             diskItem.IsSfx,
			DevName:           diskItem.devName,
			PhysicalUsedRatio: diskItem.PhysicalUsedRatio,
			CompressionRatio:  diskItem.CompressionRatio,
		}
		if disk.IsSfx {
			disks = append(disks, disk)
		}
	}
	diskReport := &struct {
		Disks []interface{} `json:"disks"`
		Zone  string        `json:"zone"`
	}{
		Disks: disks,
		Zone:  s.zoneName,
	}
	s.buildSuccessResp(w, diskReport)
}

func (s *DataNode) getExtentLockInfo(w http.ResponseWriter, r *http.Request) {
	var (
		partitionID    uint64
		extentID       uint64
		extLockInfoMap = make(map[uint64]*proto.ExtentIdLockInfo)
		err            error
	)
	if err = r.ParseForm(); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if partitionID, err = strconv.ParseUint(r.FormValue("partitionID"), 10, 64); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if extentID, err = strconv.ParseUint(r.FormValue("extentID"), 10, 64); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	partition := s.space.Partition(partitionID)
	if partition == nil {
		s.buildFailureResp(w, http.StatusNotFound, "partition not exist")
		return
	}
	if extentID == 0 {
		partition.ExtentStore().RangeExtentLockInfo(func(key interface{}, value *ttlstore.Val) bool {
			eId := key.(uint64)
			extLockInfoMap[eId] = &proto.ExtentIdLockInfo{
				ExtentId:   eId,
				ExpireTime: value.GetExpirationTime(),
				TTL:        value.GetTTL(),
			}
			return true
		})
	} else {
		if value, ok := partition.ExtentStore().LoadExtentLockInfo(extentID); ok {
			extLockInfoMap[extentID] = &proto.ExtentIdLockInfo{
				ExtentId:   extentID,
				ExpireTime: value.GetExpirationTime(),
				TTL:        value.GetTTL(),
			}
		}
	}
	s.buildSuccessResp(w, extLockInfoMap)
	return
}

func (s *DataNode) startRiskFix(w http.ResponseWriter, r *http.Request) {
	s.space.WalkPartitions(func(partition *DataPartition) bool {
		if fixer := partition.RiskFixer(); fixer != nil {
			fixer.Start()
		}
		return true
	})
	s.buildSuccessResp(w, nil)
}

func (s *DataNode) stopRiskFix(w http.ResponseWriter, r *http.Request) {
	s.space.WalkPartitions(func(partition *DataPartition) bool {
		if fixer := partition.RiskFixer(); fixer != nil {
			fixer.Stop()
		}
		return true
	})
	s.buildSuccessResp(w, nil)
}

func (s *DataNode) getRiskStatus(w http.ResponseWriter, r *http.Request) {
	var result = &struct {
		Count   int  `json:"count"`
		Running bool `json:"running"`
	}{}
	s.space.WalkPartitions(func(partition *DataPartition) bool {
		if fixer := partition.RiskFixer(); fixer != nil {
			status := fixer.Status()
			result.Count += status.Count
			if status.Running {
				result.Running = true
			}
		}
		return true
	})
	s.buildSuccessResp(w, result)
}

func (s *DataNode) transferDeleteV0(w http.ResponseWriter, r *http.Request) {
	var (
		err          error
		partitionStr string
		partitionID  uint64
		failes       []uint64
		succeeds     []uint64
		archives     int
		lock         sync.Mutex
	)
	succeeds = make([]uint64, 0)
	failes = make([]uint64, 0)
	partitionStr = r.FormValue("partitionID")
	if partitionStr != "" {
		if partitionID, err = strconv.ParseUint(partitionStr, 10, 64); err != nil {
			s.buildFailureResp(w, http.StatusBadRequest, err.Error())
			return
		}
		partition := s.space.Partition(partitionID)
		if partition == nil {
			s.buildFailureResp(w, http.StatusNotFound, "partition not exist")
			return
		}
		archives, err = partition.extentStore.TransferDeleteV0()
		if err != nil {
			log.LogErrorf("transfer failed, partition: %v, err: %v", partitionID, err)
			s.buildFailureResp(w, http.StatusNotFound, err.Error())
			return
		}
		if archives > 0 {
			succeeds = append(succeeds, partitionID)
		}
	} else {
		s.transferDeleteLock.Lock()
		defer s.transferDeleteLock.Unlock()
		wg := sync.WaitGroup{}
		s.space.WalkDisks(func(disk *Disk) bool {
			wg.Add(1)
			go func() {
				defer wg.Done()
				disk.WalkPartitions(func(partition *DataPartition) bool {
					var archive int
					archive, err = partition.extentStore.TransferDeleteV0()
					if err != nil {
						lock.Lock()
						failes = append(failes, partition.partitionID)
						lock.Unlock()
						log.LogErrorf("transfer failed, partition: %v, err: %v", partitionID, err)
						return true
					}
					if archive == 0 {
						return true
					}
					lock.Lock()
					succeeds = append(succeeds, partition.partitionID)
					lock.Unlock()
					return true
				})
			}()
			return true
		})
		wg.Wait()
	}
	if len(succeeds) > 0 || len(failes) > 0 {
		s.buildSuccessResp(w, fmt.Sprintf("all partitions transfer finish, success: %v, failed: %v", succeeds, failes))
	} else {
		s.buildSuccessResp(w, "no partitions need to transfer")
	}
}

func (s *DataNode) getDataPartitionViewCache(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	volumeName := r.FormValue("volumeName")
	if volumeName == "" {
		s.buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("lack of params: volumeName"))
		return
	}
	dpIDStr := r.FormValue("dpID")
	if dpIDStr == "" {
		s.buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("lack of params: dpID"))
		return
	}
	dpID, err := strconv.ParseUint(dpIDStr, 10, 64)
	if err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	data := s.topoManager.GetPartitionFromCache(volumeName, dpID)
	s.buildSuccessResp(w, data)
	return
}

func (s *DataNode) getTinyExtents(w http.ResponseWriter, r *http.Request) {
	var (
		err         error
		partitionID uint64
	)
	var result = make([]struct {
		PartitionID          uint64   `json:"partitionID"`
		IsLeader             bool     `json:"isLeader"`
		AvailableCh          int      `json:"availableCh"`
		Volume               string   `json:"volume"`
		BrokenCh             int      `json:"brokenCh"`
		TotalTinyExtent      int      `json:"totalTinyExtent"`
		AvailableTinyExtents []uint64 `json:"availableTinyExtents"`
		BrokenTinyExtents    []uint64 `json:"brokenTinyExtents"`
	}, 0)

	if err = r.ParseForm(); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	if partitionStr := r.FormValue("partitionID"); partitionStr != "" {
		if partitionID, err = strconv.ParseUint(partitionStr, 10, 64); err != nil {
			s.buildFailureResp(w, http.StatusBadRequest, err.Error())
			return
		}
		partition := s.space.Partition(partitionID)
		if partition == nil {
			s.buildFailureResp(w, http.StatusNotFound, "partition not exist")
			return
		}
		avail := partition.extentStore.AvailableTinyExtentCnt()
		broken := partition.extentStore.BrokenTinyExtentCnt()
		result = append(result, struct {
			PartitionID          uint64   `json:"partitionID"`
			IsLeader             bool     `json:"isLeader"`
			AvailableCh          int      `json:"availableCh"`
			Volume               string   `json:"volume"`
			BrokenCh             int      `json:"brokenCh"`
			TotalTinyExtent      int      `json:"totalTinyExtent"`
			AvailableTinyExtents []uint64 `json:"availableTinyExtents"`
			BrokenTinyExtents    []uint64 `json:"brokenTinyExtents"`
		}{
			PartitionID:          partitionID,
			IsLeader:             partition.isReplLeader,
			AvailableCh:          avail,
			Volume:               partition.volumeID,
			BrokenCh:             broken,
			TotalTinyExtent:      avail + broken,
			AvailableTinyExtents: partition.extentStore.AvailableTinyExtents(),
			BrokenTinyExtents:    partition.extentStore.BrokenTinyExtents(),
		})
		s.buildSuccessResp(w, result)
		return
	}

	s.space.WalkPartitions(func(dp *DataPartition) bool {
		avail := dp.extentStore.AvailableTinyExtentCnt()
		broken := dp.extentStore.BrokenTinyExtentCnt()
		result = append(result, struct {
			PartitionID          uint64   `json:"partitionID"`
			IsLeader             bool     `json:"isLeader"`
			AvailableCh          int      `json:"availableCh"`
			Volume               string   `json:"volume"`
			BrokenCh             int      `json:"brokenCh"`
			TotalTinyExtent      int      `json:"totalTinyExtent"`
			AvailableTinyExtents []uint64 `json:"availableTinyExtents"`
			BrokenTinyExtents    []uint64 `json:"brokenTinyExtents"`
		}{
			PartitionID:          dp.partitionID,
			IsLeader:             dp.isReplLeader,
			AvailableCh:          avail,
			Volume:               dp.volumeID,
			BrokenCh:             broken,
			TotalTinyExtent:      avail + broken,
			AvailableTinyExtents: dp.extentStore.AvailableTinyExtents(),
			BrokenTinyExtents:    dp.extentStore.BrokenTinyExtents(),
		})
		return true
	})
	s.buildSuccessResp(w, result)
	return
}

func (s *DataNode) buildSuccessResp(w http.ResponseWriter, data interface{}) {
	s.buildJSONResp(w, http.StatusOK, data, "")
}

func (s *DataNode) buildFailureResp(w http.ResponseWriter, code int, msg string) {
	s.buildJSONResp(w, code, nil, msg)
}

// Create response for the API request.
func (s *DataNode) buildJSONResp(w http.ResponseWriter, code int, data interface{}, msg string) {
	var (
		jsonBody []byte
		err      error
	)
	w.WriteHeader(code)
	w.Header().Set("Content-Type", "application/json")
	body := struct {
		Code int         `json:"code"`
		Data interface{} `json:"data"`
		Msg  string      `json:"msg"`
	}{
		Code: code,
		Data: data,
		Msg:  msg,
	}
	if jsonBody, err = json.Marshal(body); err != nil {
		return
	}
	w.Write(jsonBody)
}

func (s *DataNode) getRecentTrashExtents(w http.ResponseWriter, r *http.Request) {
	var (
		partitionID uint64
		recentSec   int
		err         error
	)
	const (
		paramPartitionID = "id"
		paramRecentSec   = "recentSec"
		defaultRecentSec = 24 * 60 * 60
	)
	if err = r.ParseForm(); err != nil {
		err = fmt.Errorf("parse form fail: %v", err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}

	recent := r.FormValue(paramRecentSec)
	if recent == "all" {
		recentSec = 0
	} else if recent == "" {
		recentSec = defaultRecentSec
	} else {
		recentSec, err = strconv.Atoi(recent)
	}

	if partitionID, err = strconv.ParseUint(r.FormValue(paramPartitionID), 10, 64); err != nil {
		err = fmt.Errorf("parse param %v fail: %v", paramPartitionID, err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	partition := s.space.Partition(partitionID)
	if partition == nil {
		s.buildFailureResp(w, http.StatusNotFound, "partition not exist")
		return
	}
	type TrashResult struct {
		PartitionID uint64            `json:"partitionID"`
		TotalSize   uint64            `json:"totalSize"`
		Extents     map[uint64]uint64 `json:"extents"`
	}
	rst := &TrashResult{
		PartitionID: partitionID,
	}
	rst.Extents, rst.TotalSize = partition.ExtentStore().GetRecentTrashExtents(recentSec)
	s.buildSuccessResp(w, rst)
}

func (s *DataNode) getRecentDeleteExtents(w http.ResponseWriter, r *http.Request) {
	var (
		partitionID uint64
		recentSec   int
		err         error
	)
	const (
		paramPartitionID = "id"
		paramRecentSec   = "recentSec"
		defaultRecentSec = 24 * 60 * 60
	)
	if err = r.ParseForm(); err != nil {
		err = fmt.Errorf("parse form fail: %v", err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}

	recent := r.FormValue(paramRecentSec)
	if recent == "all" {
		recentSec = 0
	} else if recent == "" {
		recentSec = defaultRecentSec
	} else {
		recentSec, err = strconv.Atoi(recent)
	}

	if partitionID, err = strconv.ParseUint(r.FormValue(paramPartitionID), 10, 64); err != nil {
		err = fmt.Errorf("parse param %v fail: %v", paramPartitionID, err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	partition := s.space.Partition(partitionID)
	if partition == nil {
		s.buildFailureResp(w, http.StatusNotFound, "partition not exist")
		return
	}
	type RecentDeleteResult struct {
		PartitionID uint64           `json:"partitionID"`
		Extents     map[uint64]int64 `json:"extents"`
	}
	recentDelete := &RecentDeleteResult{
		PartitionID: partitionID,
	}
	recentDelete.Extents = partition.ExtentStore().GetRecentDeleteExtents(recentSec)
	s.buildSuccessResp(w, recentDelete)
}

func (s *DataNode) batchRecoverExtents(w http.ResponseWriter, r *http.Request) {
	type RecoverExtentRequest struct {
		Partition uint64   `json:"partition"`
		Extents   []uint64 `json:"extents"`
	}
	var (
		req *RecoverExtentRequest
		err error
	)
	defer func() {
		if err != nil {
			log.LogErrorf("action[batchRecoverExtents] failed, err:%v", err)
		}
	}()
	if err = r.ParseForm(); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(r.Body)
	if err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}

	req = new(RecoverExtentRequest)
	if err = json.Unmarshal(buf.Bytes(), &req); err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}

	partition := s.space.Partition(req.Partition)
	if partition == nil {
		err = fmt.Errorf("partition[%d] not exist, can not recover", req.Partition)
		s.buildFailureResp(w, http.StatusInternalServerError, err.Error())
		return
	}

	if len(req.Extents) == 0 {
		err = fmt.Errorf("no extents in request")
		s.buildFailureResp(w, http.StatusInternalServerError, err.Error())
		return
	}
	if !partition.ExtentStore().IsFinishLoad() {
		err = fmt.Errorf("partition[%d] not finish load", req.Partition)
		s.buildFailureResp(w, http.StatusInternalServerError, err.Error())
		return
	}
	recovered := 0
	failed := make([]uint64, 0)
	for _, extent := range req.Extents {
		err = partition.limit(context.Background(), proto.OpRecoverTrashExtent_, 0, multirate.FlowDisk)
		if err != nil {
			log.LogErrorf("action[batchRecoverExtents] partition(%v) extent(%v) failed, err:%v", req.Partition, extent, err)
			failed = append(failed, extent)
			continue
		}
		tp := exporter.NewModuleTP(proto.GetOpMsgExtend(proto.OpRecoverTrashExtent_))
		if partition.ExtentStore().RecoverTrashExtent(extent) {
			recovered++
		} else {
			failed = append(failed, extent)
		}
		tp.Set(nil)
	}
	s.buildSuccessResp(w, fmt.Sprintf("partition(%v) extents recover total(%v) recovered(%v) failed(%v)", req.Partition, len(req.Extents), recovered, failed))
}

type SwitchCollectionPara struct {
	DisableBlackList       string `json:"disableBlackList,omitempty"`
	DisableAutoDeleteTrash string `json:"disableAutoDeleteTrash,omitempty"`
}

func (s *DataNode) setSettings(w http.ResponseWriter, r *http.Request) {
	err := r.ParseForm()
	if err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}

	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(r.Body)
	if err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}

	paras := new(SwitchCollectionPara)
	err = json.Unmarshal(buf.Bytes(), paras)
	if err != nil {
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}

	if paras.DisableBlackList != "" {
		var isDisable bool
		isDisable, err = strconv.ParseBool(paras.DisableBlackList)
		if err != nil {
			s.buildFailureResp(w, http.StatusBadRequest, err.Error())
			return
		}
		if err = s.settings.Set(SettingKeyDisableBlackList, strconv.FormatBool(isDisable)); err != nil {
			s.buildFailureResp(w, http.StatusInternalServerError, err.Error())
			return
		}
		if gConnPool != nil {
			gConnPool.DisableBlackList(isDisable)
		}
	}
	if paras.DisableAutoDeleteTrash != "" {
		var isDisable bool
		isDisable, err = strconv.ParseBool(paras.DisableAutoDeleteTrash)
		if err != nil {
			s.buildFailureResp(w, http.StatusBadRequest, err.Error())
			return
		}
		if err = s.settings.Set(SettingKeyDisableAutoDeleteTrash, strconv.FormatBool(isDisable)); err != nil {
			s.buildFailureResp(w, http.StatusInternalServerError, err.Error())
			return
		}
	}
	s.buildSuccessResp(w, fmt.Sprintf("set switch collection success"))
}

func (s *DataNode) getSettings(w http.ResponseWriter, r *http.Request) {
	var settings = make(map[string]string)
	s.settings.Walk(func(key, value string) bool {
		settings[key] = value
		return true
	})
	s.buildSuccessResp(w, settings)
}

func (s *DataNode) releaseTrashExtents(w http.ResponseWriter, r *http.Request) {
	var (
		keepTime uint64
		err      error
	)
	const (
		paramAuthKey       = "key"
		paramKeepTimeSec   = "keepTimeSec"
		defaultKeepTImeSec = 60 * 60 * 24 * 7
	)
	if err = r.ParseForm(); err != nil {
		err = fmt.Errorf("parse form fail: %v", err)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	key := r.FormValue(paramAuthKey)
	if !matchKey(key) {
		err = fmt.Errorf("auth key not match: %v", key)
		s.buildFailureResp(w, http.StatusBadRequest, err.Error())
		return
	}
	keepTimeSec := r.FormValue(paramKeepTimeSec)
	if keepTimeSec == "" {
		keepTime = defaultKeepTImeSec
	} else {
		var keepTimeSecVal uint64
		if keepTimeSecVal, err = strconv.ParseUint(keepTimeSec, 10, 64); err != nil {
			s.buildFailureResp(w, http.StatusBadRequest, fmt.Sprintf("parse param %v failed: %v", paramKeepTimeSec, err))
			return
		}
		keepTime = keepTimeSecVal
	}
	releaseCount := uint64(0)
	for _, d := range s.space.disks {
		for _, partition := range d.partitionMap {
			releaseCount += partition.batchDeleteTrashExtents(keepTime)
		}
	}
	s.buildSuccessResp(w, fmt.Sprintf("release trash extents, release extent count: %v: ", releaseCount))
}
