package cutil

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cubefs/cubefs/util/log"
	"io/ioutil"
	"net/http"
	"time"
)

const (
	DefaultRequestTimeOut = time.Duration(60 * time.Second)
)

var (
	ErrHttpStatusCode = errors.New("status code not OK")
)

type HTTPReply struct {
	Code int32           `json:"code"`
	Msg  string          `json:"msg"`
	Data json.RawMessage `json:"data"`
}

type HttpRequest struct {
	method string
	path   string
	params map[string]string
	header map[string]string
	body   []byte
}

func (r *HttpRequest) AddParam(key, value string) {
	r.params[key] = value
}

func (r *HttpRequest) AddHeader(key, value string) {
	r.header[key] = value
}

func (r *HttpRequest) AddBody(body []byte) {
	r.body = body
}

func NewAPIRequest(method string, path string) *HttpRequest {
	return &HttpRequest{
		method: method,
		path:   path,
		params: make(map[string]string),
		header: make(map[string]string),
	}
}

func SendSimpleRequest(r *HttpRequest, isRelease bool) (data []byte, err error) {
	client := http.DefaultClient
	client.Timeout = DefaultRequestTimeOut

	url := mergeRequestUrl(r.path, r.params)
	req, err := http.NewRequest(r.method, url, bytes.NewReader(r.body))
	if err != nil {
		log.LogErrorf("sendSimpleRequest: NewRequest err(%v) url(%v)", err, url)
		return
	}

	for key, value := range r.header {
		req.Header.Set(key, value)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Connection", "keep-alive")

	resp, err := client.Do(req)
	if err != nil {
		log.LogErrorf("sendSimpleRequest: http.DO err(%v) url(%v)", err, url)
		return
	}
	defer resp.Body.Close()
	if data, err = ioutil.ReadAll(resp.Body); err != nil {
		log.LogErrorf("sendSimpleRequest: ioutil.ReadAll err(%v) url(%v) data(%v)", err, url, string(data))
		return
	}
	if resp.StatusCode != http.StatusOK {
		// code不是200, 但err == nil的情况(server自己置的code)
		if err == nil {
			err = fmt.Errorf("%v, msg: %v", ErrHttpStatusCode, string(data))
		}
		log.LogErrorf("sendSimpleRequest: url(%v) err(%v) code(%v)", url, err, resp.StatusCode)
		return
	}
	if !isRelease {
		reply := HTTPReply{}
		if err = json.Unmarshal(data, &reply); err != nil {
			log.LogErrorf("sendSimpleRequest: Unmarshal err(%v) url(%v)", err, url)
			return
		}
		// 有的接口 code会返回200 有的接口为其他值
		if reply.Code != http.StatusOK && reply.Code != 0 {
			return nil, fmt.Errorf("sendSimpleRequest: reply.Code != 0, reply:%v", reply)
		}
		data = reply.Data
	}
	if log.IsDebugEnabled() {
		log.LogDebugf("sendSimpleRequest success: url: %v", url)
	}
	return
}

func mergeRequestUrl(url string, params map[string]string) string {
	if params != nil && len(params) > 0 {
		buff := bytes.NewBuffer([]byte(url))
		isFirstParam := true
		for k, v := range params {
			if isFirstParam {
				buff.WriteString("?")
				isFirstParam = false
			} else {
				buff.WriteString("&")
			}
			buff.WriteString(k)
			buff.WriteString("=")
			buff.WriteString(v)
		}
		url = buff.String()
	}
	return url
}

func SendHttpReply(w http.ResponseWriter, r *http.Request, reply *HTTPReply) {
	log.LogDebugf("URL[%v],remoteAddr[%v],response[%v]", r.URL, r.RemoteAddr, reply)
	data, err := json.Marshal(reply)
	if err != nil {
		http.Error(w, "fail to marshal http reply", http.StatusInternalServerError)
		return
	}
	w.Header().Set("content-type", "application/json")
	if _, err = w.Write(data); err != nil {
		log.LogErrorf("fail to write http reply len[%d].URL[%v],remoteAddr[%v] err:[%v]", len(data), r.URL, r.RemoteAddr, err)
	}
	return
}

func BuildHttpReply(code int, msg string) *HTTPReply {
	return &HTTPReply{
		Code: int32(code),
		Msg:  msg,
	}
}
