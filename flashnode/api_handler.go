// Copyright 2018 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package flashnode

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/cpu"
	"github.com/cubefs/cubefs/util/diskusage"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/ping"
	"github.com/cubefs/cubefs/util/unit"
	"net/http"
	"strconv"
)

// register the APIs
func (f *FlashNode) registerAPIHandler() (err error) {
	http.HandleFunc(proto.VersionPath, f.getVersion)
	http.HandleFunc("/stat", f.getCacheStatHandler)
	http.HandleFunc("/keys", f.getKeysHandler)
	http.HandleFunc("/evictVol", f.evictVolumeCacheHandler)
	http.HandleFunc("/evictAll", f.evictAllCacheHandler)
	http.HandleFunc("/evictInode", f.evictInodeHandler)
	http.HandleFunc("/stat/info", f.getStatInfo)
	http.HandleFunc("/stack/set", f.setStackEnable)
	return
}

func (f *FlashNode) getVersion(w http.ResponseWriter, _ *http.Request) {
	version := proto.MakeVersion("FlashNode")
	version.Version = NodeLatestVersion
	marshal, _ := json.Marshal(version)
	if _, err := w.Write(marshal); err != nil {
		log.LogErrorf("write version has err:[%s]", err.Error())
	}
}

func (f *FlashNode) getCacheStatHandler(w http.ResponseWriter, r *http.Request) {
	sendOkReply(w, r, &proto.HTTPReply{Code: http.StatusOK, Data: &proto.FlashNodeStat{
		NodeLimit:          f.nodeLimit,
		VolLimit:           f.volLimitMap,
		CacheStatus:        f.cacheEngine.Status(),
		EnableStack:        f.cacheEngine.GetCacheStackEnable(),
		EnablePing:         ping.GetPingEnable(),
		CacheReadTimeoutMs: gSingleContext.GetTimeoutMs(),
	}, Msg: "ok"})
	return
}

func (f *FlashNode) getKeysHandler(w http.ResponseWriter, r *http.Request) {
	sendOkReply(w, r, &proto.HTTPReply{Code: http.StatusOK, Data: f.cacheEngine.Keys(), Msg: "ok"})
	return
}

func (f *FlashNode) evictVolumeCacheHandler(w http.ResponseWriter, r *http.Request) {
	var (
		volume     string
		failedKeys []interface{}
	)
	r.ParseForm()
	volume = r.FormValue(VolumePara)
	if volume == "" {
		sendErrReply(w, r, &proto.HTTPReply{Code: http.StatusBadRequest, Msg: "volume name can not be empty"})
		return
	}
	failedKeys = f.cacheEngine.EvictVolumeCache(volume)
	sendOkReply(w, r, &proto.HTTPReply{Code: http.StatusOK, Data: failedKeys, Msg: "ok"})
	return
}

func (f *FlashNode) evictAllCacheHandler(w http.ResponseWriter, r *http.Request) {
	f.cacheEngine.EvictAllCache()
	sendOkReply(w, r, &proto.HTTPReply{Code: http.StatusOK, Msg: "ok"})
	return
}

func (f *FlashNode) evictInodeHandler(w http.ResponseWriter, r *http.Request) {
	var (
		volume     string
		inode      uint64
		err        error
		failedKeys []interface{}
	)
	r.ParseForm()
	volume = r.FormValue(VolumePara)
	if volume == "" {
		sendErrReply(w, r, &proto.HTTPReply{Code: http.StatusBadRequest, Msg: "volume name can not be empty"})
		return
	}
	inodeStr := r.FormValue(InodePara)
	if inodeStr == "" {
		sendErrReply(w, r, &proto.HTTPReply{Code: http.StatusBadRequest, Msg: "inode can not be empty"})
		return
	}
	inode, err = strconv.ParseUint(inodeStr, 10, 64)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: http.StatusBadRequest, Msg: err.Error()})
		return
	}
	failedKeys = f.cacheEngine.EvictInodeCache(volume, inode)
	sendOkReply(w, r, &proto.HTTPReply{Code: http.StatusOK, Data: failedKeys, Msg: "ok"})
}

func (f *FlashNode) stopHandler(w http.ResponseWriter, r *http.Request) {
	f.stopServer()
	sendOkReply(w, r, &proto.HTTPReply{Code: http.StatusOK, Msg: "ok"})
	return
}

func (f *FlashNode) getStatInfo(w http.ResponseWriter, r *http.Request) {
	if f.processStatInfo == nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: http.StatusInternalServerError, Msg: "flash node is not initialized"})
		return
	}
	//get process stat info
	cpuUsageList, maxCPUUsage := f.processStatInfo.GetProcessCPUStatInfo()
	_, memoryUsedGBList, maxMemoryUsedGB, maxMemoryUsage := f.processStatInfo.GetProcessMemoryStatInfo()

	diskTotal, err := diskusage.GetDiskTotal(f.tmpfsPath)
	if err != nil {
		diskTotal = f.total
	}

	tmpfs := &struct {
		Path     string  `json:"path"`
		TotalGB  float64 `json:"totalGB"`
		MaxUseGB float64 `json:"maxUseGB"`
	}{
		Path:     f.tmpfsPath,
		TotalGB:  unit.FixedPoint(float64(diskTotal)/unit.GB, 1),
		MaxUseGB: unit.FixedPoint(float64(f.cacheEngine.Status().MaxAlloc)/unit.GB, 1),
	}

	result := &struct {
		Type           string      `json:"type"`
		Zone           string      `json:"zone"`
		Version        interface{} `json:"versionInfo"`
		StartTime      string      `json:"startTime"`
		CPUUsageList   []float64   `json:"cpuUsageList"`
		MaxCPUUsage    float64     `json:"maxCPUUsage"`
		CPUCoreNumber  int         `json:"cpuCoreNumber"`
		MemoryUsedList []float64   `json:"memoryUsedGBList"`
		MaxMemoryUsed  float64     `json:"maxMemoryUsedGB"`
		MaxMemoryUsage float64     `json:"maxMemoryUsage"`
		TmpfsInfo      interface{} `json:"tmpfsInfo"`
	}{
		Type:           ModuleName,
		Zone:           f.zoneName,
		Version:        proto.MakeVersion("FlashNode"),
		StartTime:      f.processStatInfo.ProcessStartTime,
		CPUUsageList:   cpuUsageList,
		MaxCPUUsage:    maxCPUUsage,
		CPUCoreNumber:  cpu.GetCPUCoreNumber(),
		MemoryUsedList: memoryUsedGBList,
		MaxMemoryUsed:  maxMemoryUsedGB,
		MaxMemoryUsage: maxMemoryUsage,
		TmpfsInfo:      tmpfs,
	}
	sendOkReply(w, r, &proto.HTTPReply{Code: http.StatusOK, Data: result})
}

func (f *FlashNode) setStackEnable(w http.ResponseWriter, r *http.Request) {
	val := r.FormValue("enable")
	if val == "" {
		sendErrReply(w, r, &proto.HTTPReply{Code: http.StatusInternalServerError, Msg: "parameter enable is empty"})
		return
	}
	enable, err := strconv.ParseBool(val)
	if err != nil {
		sendErrReply(w, r, &proto.HTTPReply{Code: http.StatusInternalServerError, Msg: err.Error()})
		return
	}
	if f.cacheEngine == nil {
		if err != nil {
			sendErrReply(w, r, &proto.HTTPReply{Code: http.StatusInternalServerError, Msg: "cache engine is nil"})
			return
		}
	}
	f.cacheEngine.SetCacheStackEnable(enable)
	sendOkReply(w, r, &proto.HTTPReply{Code: http.StatusOK, Data: fmt.Sprintf("set stack enable to:%v success", enable)})
}

func sendOkReply(w http.ResponseWriter, r *http.Request, httpReply *proto.HTTPReply) (err error) {
	reply, err := json.Marshal(httpReply)
	if err != nil {
		log.LogErrorf("fail to marshal http reply[%v]. URL[%v],remoteAddr[%v] err:[%v]", httpReply, r.URL, r.RemoteAddr, err)
		http.Error(w, "fail to marshal http reply", http.StatusBadRequest)
		return
	}
	w.Header().Set("content-type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(reply)))
	if _, err = w.Write(reply); err != nil {
		log.LogErrorf("fail to write http reply len[%d].URL[%v],remoteAddr[%v] err:[%v]", len(reply), r.URL, r.RemoteAddr, err)
		return
	}
	log.LogInfof("URL[%v],remoteAddr[%v],response ok", r.URL, r.RemoteAddr)
	return
}

func sendErrReply(w http.ResponseWriter, r *http.Request, httpReply *proto.HTTPReply) {
	log.LogInfof("URL[%v],remoteAddr[%v],response err[%v]", r.URL, r.RemoteAddr, httpReply)
	reply, err := json.Marshal(httpReply)
	if err != nil {
		log.LogErrorf("fail to marshal http reply[%v]. URL[%v],remoteAddr[%v] err:[%v]", httpReply, r.URL, r.RemoteAddr, err)
		http.Error(w, "fail to marshal http reply", http.StatusBadRequest)
		return
	}
	w.Header().Set("content-type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(reply)))
	if _, err = w.Write(reply); err != nil {
		log.LogErrorf("fail to write http reply len[%d].URL[%v],remoteAddr[%v] err:[%v]", len(reply), r.URL, r.RemoteAddr, err)
	}
	return
}
