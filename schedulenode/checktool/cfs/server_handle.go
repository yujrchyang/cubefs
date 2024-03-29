package cfs

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/util/log"
	"net/http"
	"strconv"
	"strings"
)

func registerChubaoFSHighLoadNodeSolver(s *ChubaoFSHighLoadNodeSolver) {
	if s != nil {
		http.HandleFunc("/checkToolHighLoadNodeSolver", s.highLoadNodeSolverHandle)
		http.HandleFunc("/checkToolHighLoadNodeSolverCritical", s.highLoadNodeSolverCriticalHandle)
	}
}

func registerChubaoFSDPReleaserServer(dpReleaser *ChubaoFSDPReleaser) {
	if dpReleaser != nil {
		http.HandleFunc("/getChubaoFSDPReleaser", dpReleaser.getChubaoFSDPReleaser)
		http.HandleFunc("/setChubaoFSDPReleaser", dpReleaser.setChubaoFSDPReleaser)
	}
}

func parseEnable(r *http.Request) (enable bool, err error) {
	if err = r.ParseForm(); err != nil {
		return
	}
	var value string
	if value = r.FormValue("enable"); value == "" {
		err = fmt.Errorf("ParaEnableNotFound")
		return
	}
	if enable, err = strconv.ParseBool(value); err != nil {
		return
	}
	return
}

func BuildSuccessResp(w http.ResponseWriter, r *http.Request, data interface{}) {
	buildJSONResp(w, r, http.StatusOK, data, "")
}

func BuildFailureResp(w http.ResponseWriter, r *http.Request, code int, msg string) {
	buildJSONResp(w, r, code, nil, msg)
}

func BuildJSONRespWithDiffCode(w http.ResponseWriter, httpCode, code int, data interface{}, msg string) {
	var (
		jsonBody []byte
		err      error
	)
	w.WriteHeader(httpCode)
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

func (s *ChubaoFSMonitor) registerHandler() {
	http.HandleFunc("dp/release", s.releaseDp)
}

func (s *ChubaoFSMonitor) releaseDp(w http.ResponseWriter, r *http.Request) {
	var err error
	if err = r.ParseForm(); err != nil {
		err = fmt.Errorf("parse form fail: %v", err)
		BuildFailureResp(w, r, http.StatusBadRequest, err.Error())
		return
	}
	host := r.FormValue("host")
	if strings.TrimSpace(host) == "" {
		err = fmt.Errorf("host can't be empty")
		BuildFailureResp(w, r, http.StatusBadRequest, err.Error())
		return
	}
	if err = s.dpReleaser.releaseDataNodeDpOnHost(host); err != nil {
		BuildFailureResp(w, r, http.StatusBadRequest, err.Error())
		return
	}
	buildJSONResp(w, r, http.StatusOK, nil, "start dp release success")
}

// Create response for the API request.
func buildJSONResp(w http.ResponseWriter, r *http.Request, code int, data interface{}, msg string) {
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
	if _, err = w.Write(jsonBody); err != nil {
		log.LogErrorf("write response failed,err:%v,url:%v", err, r.URL.String())
	}
}
