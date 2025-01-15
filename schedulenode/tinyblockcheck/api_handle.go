package tinyblck

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/schedulenode/common"
	"github.com/cubefs/cubefs/util/log"
	"net/http"
	"path"
	"strconv"
	"time"
)

func (w *TinyBlockCheckWorker) registerHandle() {
	http.HandleFunc(proto.Version, func(w http.ResponseWriter, r *http.Request) {
		version := proto.MakeVersion("blck")
		marshal, _ := json.Marshal(version)
		if _, err := w.Write(marshal); err != nil {
			log.LogErrorf("write version has err:[%s]", err.Error())
		}
		return
	})
	http.HandleFunc("/runTask", w.runCheckTask)
	http.HandleFunc("/checkTinySize", w.checkTinySize)
	//http.HandleFunc("/calcSize", w.calcGarbageBlockSize)
	//http.HandleFunc("/validGarbageBlocks", w.validGarbageBlocks)
	//http.HandleFunc("/cleanGarbageBlocks", w.cleanGarbageBlocks)
	http.HandleFunc("/getParams", w.getParameters)
	http.HandleFunc("/updateParams", w.updateParameters)
	//http.HandleFunc("/dumpMetaExtents", w.dumpMetaExtentsMap)
	//http.HandleFunc("/runTaskByMetaDumpFile", w.runTaskByMetaDumpFile)
}

func (w *TinyBlockCheckWorker) runCheckTask(respWriter http.ResponseWriter, r *http.Request) {
	var err error
	resp := common.NewAPIResponse(http.StatusOK, "OK")
	defer func() {
		data, _ := resp.Marshal()
		if _, err = respWriter.Write(data); err != nil {
			log.LogErrorf("[runCheckTask] response %s", err)
		}
	}()

	if err = r.ParseForm(); err != nil {
		resp.Code = http.StatusBadRequest
		resp.Msg = err.Error()
		return
	}
	//parse cluster, volName
	clusterName := r.FormValue("clusterName")
	masterClient, ok := w.mcw[clusterName]
	if !ok {
		resp.Code = http.StatusBadRequest
		resp.Msg = fmt.Sprintf("%s not exist in clusters config", clusterName)
		return
	}

	volName := r.FormValue("volName")
	if volName == "" {
		resp.Code = http.StatusBadRequest
		resp.Msg = fmt.Sprintf("volName is needed")
		return
	}

	needClean, _ := strconv.ParseBool(r.FormValue("needClean"))

	var safeCleanInterval uint64
	safeCleanInterval, err = strconv.ParseUint(r.FormValue("safeCleanIntervalSecond"), 10, 64)
	if err != nil {
		safeCleanInterval = uint64(DefaultSafeCleanInterval)
	}

	//needSendEmail, _ := strconv.ParseBool(r.FormValue("sendEmail"))

	go func() {
		task := &proto.Task{
			TaskType:      proto.WorkerTypeTinyBlockCheck,
			Cluster:       clusterName,
			VolName:       volName,
			WorkerAddr:    w.LocalIp,
		}
		exportDir := path.Join(w.exportDir, clusterName, fmt.Sprintf("%s_%s", volName, time.Now().Format(proto.TimeFormat2)))
		blckTask := NewTinyBlockCheckTask(task, masterClient, needClean, true, int64(safeCleanInterval), exportDir)
		blckTask.RunTinyExtentsCheck()
	}()
}

func (w *TinyBlockCheckWorker) checkTinySize(respWriter http.ResponseWriter, r *http.Request) {
	var err error
	resp := common.NewAPIResponse(http.StatusOK, "OK")
	defer func() {
		data, _ := resp.Marshal()
		if _, err = respWriter.Write(data); err != nil {
			log.LogErrorf("[checkTinySize] response %s", err)
		}
	}()

	if err = r.ParseForm(); err != nil {
		resp.Code = http.StatusBadRequest
		resp.Msg = err.Error()
		return
	}
	//parse cluster, volName
	clusterName := r.FormValue("clusterName")
	masterClient, ok := w.mcw[clusterName]
	if !ok {
		resp.Code = http.StatusBadRequest
		resp.Msg = fmt.Sprintf("%s not exist in clusters config", clusterName)
		return
	}

	volName := r.FormValue("volName")
	if volName == "" {
		resp.Code = http.StatusBadRequest
		resp.Msg = fmt.Sprintf("volName is needed")
		return
	}

	go func() {
		task := &proto.Task{
			TaskType:      proto.WorkerTypeTinyBlockCheck,
			Cluster:       clusterName,
			VolName:       volName,
			WorkerAddr:    w.LocalIp,
		}
		exportDir := path.Join(w.exportDir, clusterName, fmt.Sprintf("%s_%s", volName, time.Now().Format(proto.TimeFormat2)))
		blckTask := NewTinyBlockCheckTask(task, masterClient, false, true, int64(DefaultSafeCleanInterval), exportDir)
		blckTask.RunTinyExtentsSizeCheck()
	}()
}



//
//func (w *TinyBlockCheckWorker) calcGarbageBlockSize(respWriter http.ResponseWriter, r *http.Request) {
//	var err error
//	resp := common.NewAPIResponse(http.StatusOK, "OK")
//	defer func() {
//		data, _ := resp.Marshal()
//		if _, err = respWriter.Write(data); err != nil {
//			log.LogErrorf("[calcGarbageBlockSize] response %s", err)
//		}
//	}()
//
//	if err = r.ParseForm(); err != nil {
//		resp.Code = http.StatusBadRequest
//		resp.Msg = err.Error()
//		return
//	}
//	//parse cluster, volName
//	clusterName := r.FormValue("clusterName")
//	masterClient, ok := w.mcw[clusterName]
//	if !ok {
//		resp.Code = http.StatusBadRequest
//		resp.Msg = fmt.Sprintf("%s not exist in clusters config", clusterName)
//		return
//	}
//
//	volName := r.FormValue("volName")
//	if volName == "" {
//		resp.Code = http.StatusBadRequest
//		resp.Msg = fmt.Sprintf("volName is needed")
//		return
//	}
//
//	exportDir := r.FormValue("exportDir")
//	if exportDir == "" {
//		resp.Code = http.StatusBadRequest
//		resp.Msg = fmt.Sprintf("export dir is needed")
//		return
//	}
//
//	task := &proto.Task{
//		TaskType:      proto.WorkerTypeTinyBlockCheck,
//		Cluster:       clusterName,
//		VolName:       volName,
//		WorkerAddr:    w.LocalIp,
//	}
//	blckTask := NewTinyBlockCheckTask(task, masterClient, false, true, DefaultSafeCleanInterval, exportDir)
//	err = blckTask.parseVolumeGarbageBlocksInfo()
//	if err != nil {
//		resp.Code = http.StatusBadRequest
//		resp.Msg = fmt.Sprintf("parse garbage block size failed")
//		return
//	}
//
//	err = blckTask.calcGarbageBlockSize()
//	if err != nil {
//		resp.Code = http.StatusBadRequest
//		resp.Msg = fmt.Sprintf("parse garbage block size failed")
//		return
//	}
//	resp.Data = &struct {
//		GarbageSize uint64 `json:"size"`
//	}{
//		GarbageSize: blckTask.garbageSize,
//	}
//	return
//}
//
//func (w *TinyBlockCheckWorker) validGarbageBlocks(respWriter http.ResponseWriter, r *http.Request) {
//	var err error
//	resp := common.NewAPIResponse(http.StatusOK, "OK")
//	defer func() {
//		data, _ := resp.Marshal()
//		if _, err = respWriter.Write(data); err != nil {
//			log.LogErrorf("[runCheckTask] response %s", err)
//		}
//	}()
//
//	if err = r.ParseForm(); err != nil {
//		resp.Code = http.StatusBadRequest
//		resp.Msg = err.Error()
//		return
//	}
//	//parse cluster, volName
//	clusterName := r.FormValue("clusterName")
//	masterClient, ok := w.mcw[clusterName]
//	if !ok {
//		resp.Code = http.StatusBadRequest
//		resp.Msg = fmt.Sprintf("%s not exist in clusters config", clusterName)
//		return
//	}
//
//	volName := r.FormValue("volName")
//	if volName == "" {
//		resp.Code = http.StatusBadRequest
//		resp.Msg = fmt.Sprintf("volName is needed")
//		return
//	}
//
//	exportDir := r.FormValue("exportDir")
//	if exportDir == "" {
//		resp.Code = http.StatusBadRequest
//		resp.Msg = fmt.Sprintf("export dir is needed")
//		return
//	}
//
//	task := &proto.Task{
//		TaskType:      proto.WorkerTypeTinyBlockCheck,
//		Cluster:       clusterName,
//		VolName:       volName,
//		WorkerAddr:    w.LocalIp,
//	}
//	blckTask := NewTinyBlockCheckTask(task, masterClient, false, true, DefaultSafeCleanInterval, exportDir)
//	err = blckTask.parseVolumeGarbageBlocksInfo()
//	if err != nil {
//		resp.Code = http.StatusBadRequest
//		resp.Msg = fmt.Sprintf("parse garbage block size failed")
//		return
//	}
//
//	if err = blckTask.getExtentsByMPs(); err != nil {
//		log.LogErrorf("[doCheckGarbage] get cluster[%s] volume[%s] extents from mp failed:%v",
//			blckTask.Cluster, blckTask.VolName, err)
//		return
//	}
//
//	checkErrorBlocks := make(map[uint64][]uint64)
//	for dpID, garbageBlockBitSet := range blckTask.garbageBlocks {
//		metaExtentsBitSet, has := blckTask.metaExtentsMap[dpID]
//		if has {
//			validBitSet := garbageBlockBitSet.And(metaExtentsBitSet)
//			if validBitSet.IsNil() {
//				continue
//			}
//			log.LogInfof("validGarbageBlocks dataPartition(%v) with wrong garbage blocks")
//			checkErrorBlocks[dpID] = make([]uint64, 0)
//			for index := 0; index < validBitSet.MaxNum(); index++ {
//				if validBitSet.Get(index) {
//					checkErrorBlocks[dpID] = append(checkErrorBlocks[dpID], uint64(index))
//					log.LogInfof("validGarbageBlocks dataPartition(%v) extentID(%v) still reference by meta data ",
//						dpID, index)
//				}
//			}
//		}
//	}
//	if len(checkErrorBlocks) != 0 {
//		resp.Data = checkErrorBlocks
//	}
//	return
//}
//
//func (w *TinyBlockCheckWorker) cleanGarbageBlocks(respWriter http.ResponseWriter, r *http.Request) {
//	var err error
//	resp := common.NewAPIResponse(http.StatusOK, "OK")
//	defer func() {
//		data, _ := resp.Marshal()
//		if _, err = respWriter.Write(data); err != nil {
//			log.LogErrorf("[runCheckTask] response %s", err)
//		}
//	}()
//
//	if err = r.ParseForm(); err != nil {
//		resp.Code = http.StatusBadRequest
//		resp.Msg = err.Error()
//		return
//	}
//	//parse cluster, volName
//	clusterName := r.FormValue("clusterName")
//	masterClient, ok := w.mcw[clusterName]
//	if !ok {
//		resp.Code = http.StatusBadRequest
//		resp.Msg = fmt.Sprintf("%s not exist in clusters config", clusterName)
//		return
//	}
//
//	volName := r.FormValue("volName")
//	if volName == "" {
//		resp.Code = http.StatusBadRequest
//		resp.Msg = fmt.Sprintf("volName is needed")
//		return
//	}
//
//	exportDir := r.FormValue("exportDir")
//	if exportDir == "" {
//		resp.Code = http.StatusBadRequest
//		resp.Msg = fmt.Sprintf("export dir is needed")
//		return
//	}
//
//	task := &proto.Task{
//		TaskType:      proto.WorkerTypeTinyBlockCheck,
//		Cluster:       clusterName,
//		VolName:       volName,
//		WorkerAddr:    w.LocalIp,
//	}
//	blckTask := NewTinyBlockCheckTask(task, masterClient, false, true, DefaultSafeCleanInterval, exportDir)
//	err = blckTask.parseVolumeGarbageBlocksInfo()
//	if err != nil {
//		resp.Code = http.StatusBadRequest
//		resp.Msg = fmt.Sprintf("parse garbage block size failed: %v", err)
//		return
//	}
//
//	err = blckTask.doCleanGarbage()
//	if err != nil {
//		resp.Code = http.StatusBadRequest
//		resp.Msg = fmt.Sprintf("clean garbage block failed: %v", err)
//		return
//	}
//
//	return
//}

func (w *TinyBlockCheckWorker) updateParameters(respWriter http.ResponseWriter, r *http.Request) {
	resp := common.NewAPIResponse(http.StatusOK, "success")
	defer func() {
		data, _ := resp.Marshal()
		if _, err := respWriter.Write(data); err != nil {
			log.LogErrorf("[removeCluster] response %s", err)
		}
	}()

	err := r.ParseForm()
	if err != nil {
		resp.Code = http.StatusBadRequest
		resp.Msg = err.Error()
		return
	}
	newParallelMPCnt, _ := strconv.ParseInt(r.FormValue("parallelMPCnt"), 10, 32)
	newParallelInodeCnt, _ := strconv.ParseInt(r.FormValue("parallelInodeCnt"), 10, 32)
	newRatio, _ := strconv.ParseFloat(r.FormValue("ratio"), 64)
	newMaxCheckSize, _ := strconv.ParseUint(r.FormValue("maxCheckSize"), 10, 64)
	newMinCheckSize, _ := strconv.ParseUint(r.FormValue("minCheckSize"), 10, 64)

	if newParallelMPCnt != 0 && int32(newParallelMPCnt) != parallelMpCnt.Load() {
		parallelMpCnt.Store(int32(newParallelMPCnt))
	}

	if newParallelInodeCnt != 0 && int32(newParallelInodeCnt) != parallelInodeCnt.Load() {
		parallelInodeCnt.Store(int32(newParallelInodeCnt))
	}

	if newRatio > 0 && newRatio < 1 && newRatio != checkSizeRatio.Load() {
		checkSizeRatio.Store(newRatio)
	}

	if newMaxCheckSize > 0 && newMaxCheckSize != maxCheckSize.Load() {
		maxCheckSize.Store(newMaxCheckSize)
	}

	if newMinCheckSize > 0 && newMinCheckSize != minCheckSize.Load() {
		minCheckSize.Store(newMinCheckSize)
	}

	resp.Data = &struct {
		ParallelMPCount    int32   `json:"parallelMPCnt"`
		ParallelInodeCount int32   `json:"parallelInodeCnt"`
		Ratio              float64 `json:"ratio"`
		MaxCheckSize       uint64  `json:"maxCheckSize"`
		MinCheckSize       uint64  `json:"minCheckSize"`
	}{
		ParallelMPCount:    parallelMpCnt.Load(),
		ParallelInodeCount: parallelInodeCnt.Load(),
		Ratio:              checkSizeRatio.Load(),
		MaxCheckSize:       maxCheckSize.Load(),
		MinCheckSize:       minCheckSize.Load(),
	}
	return
}

func (w *TinyBlockCheckWorker) getParameters(respWriter http.ResponseWriter, r *http.Request) {
	resp := common.NewAPIResponse(http.StatusOK, "success")
	defer func() {
		data, _ := resp.Marshal()
		if _, err := respWriter.Write(data); err != nil {
			log.LogErrorf("[getParameters] response %s", err)
		}
	}()
	resp.Data = &struct {
		ParallelMPCount    int32   `json:"parallelMPCnt"`
		ParallelInodeCount int32   `json:"parallelInodeCnt"`
		Ratio              float64 `json:"ratio"`
		MaxCheckSize       uint64  `json:"maxCheckSize"`
		MinCheckSize       uint64  `json:"minCheckSize"`
	}{
		ParallelMPCount:    parallelMpCnt.Load(),
		ParallelInodeCount: parallelInodeCnt.Load(),
		Ratio:              checkSizeRatio.Load(),
		MaxCheckSize:       maxCheckSize.Load(),
		MinCheckSize:       minCheckSize.Load(),
	}
	return
}
//
//func (w *TinyBlockCheckWorker) dumpMetaExtentsMap(respWriter http.ResponseWriter, r *http.Request) {
//	var err error
//	resp := common.NewAPIResponse(http.StatusOK, "OK")
//	defer func() {
//		data, _ := resp.Marshal()
//		if _, err = respWriter.Write(data); err != nil {
//			log.LogErrorf("[runCheckTask] response %s", err)
//		}
//	}()
//
//	if err = r.ParseForm(); err != nil {
//		resp.Code = http.StatusBadRequest
//		resp.Msg = err.Error()
//		return
//	}
//	//parse cluster, volName
//	clusterName := r.FormValue("clusterName")
//	masterClient, ok := w.mcw[clusterName]
//	if !ok {
//		resp.Code = http.StatusBadRequest
//		resp.Msg = fmt.Sprintf("%s not exist in clusters config", clusterName)
//		return
//	}
//
//	volName := r.FormValue("volName")
//	if volName == "" {
//		resp.Code = http.StatusBadRequest
//		resp.Msg = fmt.Sprintf("volName is needed")
//		return
//	}
//
//	exportDir := r.FormValue("exportDir")
//	if exportDir == "" {
//		resp.Code = http.StatusBadRequest
//		resp.Msg = fmt.Sprintf("export dir is needed")
//		return
//	}
//
//	task := &proto.Task{
//		TaskType:      proto.WorkerTypeTinyBlockCheck,
//		Cluster:       clusterName,
//		VolName:       volName,
//		WorkerAddr:    w.LocalIp,
//	}
//	blckTask := NewTinyBlockCheckTask(task, masterClient, false, true, DefaultSafeCleanInterval, "")
//	blckTask.metaExportDir = exportDir
//	err = blckTask.getExtentsByMPs()
//	if err != nil {
//		resp.Code = http.StatusBadRequest
//		resp.Msg = fmt.Sprintf("parse garbage block size failed: %v", err)
//		return
//	}
//
//	blckTask.dumpMetExtentInfo()
//	return
//}
//
//func (w *TinyBlockCheckWorker) runTaskByMetaDumpFile(respWriter http.ResponseWriter, r *http.Request) {
//	var err error
//	resp := common.NewAPIResponse(http.StatusOK, "OK")
//	defer func() {
//		data, _ := resp.Marshal()
//		if _, err = respWriter.Write(data); err != nil {
//			log.LogErrorf("[runCheckTask] response %s", err)
//		}
//	}()
//
//	if err = r.ParseForm(); err != nil {
//		resp.Code = http.StatusBadRequest
//		resp.Msg = err.Error()
//		return
//	}
//	//parse cluster, volName
//	clusterName := r.FormValue("clusterName")
//	masterClient, ok := w.mcw[clusterName]
//	if !ok {
//		resp.Code = http.StatusBadRequest
//		resp.Msg = fmt.Sprintf("%s not exist in clusters config", clusterName)
//		return
//	}
//
//	volName := r.FormValue("volName")
//	if volName == "" {
//		resp.Code = http.StatusBadRequest
//		resp.Msg = fmt.Sprintf("volName is needed")
//		return
//	}
//
//	metaExportDir := r.FormValue("metaExportDir")
//	if metaExportDir == "" {
//		resp.Code = http.StatusBadRequest
//		resp.Msg = fmt.Sprintf("metaExportDir is needed")
//		return
//	}
//
//	var safeCleanInterval uint64
//	safeCleanInterval, err = strconv.ParseUint(r.FormValue("safeCleanIntervalSecond"), 10, 64)
//	if err != nil {
//		safeCleanInterval = uint64(DefaultSafeCleanInterval)
//	}
//
//	needSendEmail, _ := strconv.ParseBool(r.FormValue("sendEmail"))
//
//	go func() {
//		task := &proto.Task{
//			TaskType:      proto.WorkerTypeTinyBlockCheck,
//			Cluster:       clusterName,
//			VolName:       volName,
//			WorkerAddr:    w.LocalIp,
//		}
//		exportDir := path.Join(w.exportDir, clusterName, fmt.Sprintf("%s_%s", volName, time.Now().Format(proto.TimeFormat2)))
//		blckTask := NewTinyBlockCheckTask(task, masterClient, false, true, int64(safeCleanInterval), exportDir)
//		blckTask.metaExportDir = metaExportDir
//		blckTask.checkByMetaDumpFile = true
//		blckTask.RunOnce()
//		notifyServer := notify.NewNotify(w.NotifyConfig)
//		notifyServer.SetAlarmEmails(w.mailTo)
//		notifyServer.SetAlarmErps(w.alarmErps)
//		if needSendEmail && len(blckTask.garbageBlocks) != 0 {
//			notifyServer.AlarmToEmailWithHtmlContent("ChubaoFS 废块检查结果通知", blckTask.formatGarbageBlockInfoEmailContent())
//		}
//	}()
//}