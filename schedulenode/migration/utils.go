package migration

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
)

func UmpKeySuffix(migType string, action string) string {
	return fmt.Sprintf("%v_%v", migType, action)
}

func CalcAuthKey(key string) (authKey string) {
	h := md5.New()
	_, _ = h.Write([]byte(key))
	cipherStr := h.Sum(nil)
	return strings.ToLower(hex.EncodeToString(cipherStr))
}

func workerTypeKey(workerType proto.WorkerType, cluster, volume string) string {
	return fmt.Sprintf("%v,%v,%v", workerType, cluster, volume)
}

func SendReply(w http.ResponseWriter, r *http.Request, httpReply *proto.HTTPReply) {
	reply, err := json.Marshal(httpReply)
	if err != nil {
		log.LogErrorf("fail to marshal http reply[%v]. URL[%v],remoteAddr[%v] err:[%v]", httpReply, r.URL, r.RemoteAddr, err)
		http.Error(w, "fail to marshal http reply", http.StatusBadRequest)
		return
	}

	w.Header().Set("content-type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(reply)))

	if _, err = w.Write(reply); err != nil {
		log.LogErrorf("fail to write http reply[%s] len[%d].URL[%v],remoteAddr[%v] err:[%v]", string(reply), len(reply), r.URL, r.RemoteAddr, err)
	}

	log.LogInfof("URL[%v], remoteAddr[%v], response[%v]", r.URL, r.RemoteAddr, string(reply[:]))
	return
}

func DoGet(reqURL string) (reply *proto.QueryHTTPResult, err error) {
	resp, err := http.Get(reqURL)
	if err != nil {
		return
	}
	body, err := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	if err != nil {
		return
	}
	if resp.StatusCode != http.StatusOK {
		err = errors.NewErrorf("compact request: failed, response status code(%v) is not ok, url(%v)", resp.StatusCode, reqURL)
		return
	}
	reply = &proto.QueryHTTPResult{}
	if err = json.Unmarshal(body, reply); err != nil {
		return
	}
	return
}

func GenStopUrl(ipPort, path, cluster, vol string) string {
	url := fmt.Sprintf("http://%v%v?%v=%v&%v=%v",
		ipPort, path, ClusterKey, cluster, VolNameKey, vol)
	return url
}
