package service

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/console/cutil"
	"github.com/cubefs/cubefs/console/model"
	"github.com/cubefs/cubefs/console/proto"
	"github.com/cubefs/cubefs/util/log"
	"net/http"
	"os"
	"strings"
	"time"
)

// 卷的客户端数量 列表 版本信息(上报的表)
type VolumeClientService struct {
	clientListAddr string
}

// hbase的addr 配置文件 path路由定义
func NewVolumeVersionService(cfg *cutil.ConsoleConfig) *VolumeClientService {
	vs := new(VolumeClientService)
	vs.clientListAddr = cfg.HbaseQueryAddr
	return vs
}

func (vs *VolumeClientService) GetVolClientCount(cluster, volume string) (uint64, error) {
	return getVolumeIPCount(cluster, volume)
}

func (vs *VolumeClientService) GetVolClientList(cluster, volume string) ([]string, error) {
	return getVolumeIPList(cluster, volume)
}

func (vs *VolumeClientService) GetVolClientListCSV(cluster, volume string) (string, error) {
	return _getVolumeClientCSV(cluster, volume)
}

func (vs *VolumeClientService) GetVolumeClientListUrl(cluster, volume string) (url string, err error) {
	ips, err := getVolumeIPList(cluster, volume)
	if err != nil {
		return
	}
	filename := proto.ConsoleVolume + "/" + fmt.Sprintf("%s-%s-%s", time.Now().Format(proto.TimeFormatCompact), cluster, volume)
	var content strings.Builder
	for _, ip := range ips {
		content.WriteString(ip + "\n")
	}
	reader := strings.NewReader(content.String())
	url, err = cutil.CFS_S3.GetDownloadPresignUrl(filename, reader)
	return
}

// 版本上报的客户端version 具体信息
func (vs *VolumeClientService) GetVolClientVersionList(cluster, volume string) ([]*model.VolumeClientVersion, error) {
	table := model.VolumeClientVersion{}
	return table.GetVolClientList(volume)
}

func getVolumeIPCount(cluster, volume string) (uint64, error) {
	var (
		url      string
		respData []byte
		err      error
	)
	url = fmt.Sprintf("http://%s%s", cutil.Global_CFG.HbaseQueryAddr, proto.HbaseVolumeClientListPath)

	req := cutil.NewAPIRequest(http.MethodGet, url)
	endTime := time.Now()
	req.AddParam("cluster", cluster)
	req.AddParam("volume", volume)
	req.AddParam("end", endTime.Format(proto.TimeFormatCompact))
	req.AddParam("start", endTime.Add(-24*time.Hour).Format(proto.TimeFormatCompact))
	if respData, err = cutil.SendSimpleRequest(req, false); err != nil {
		log.LogErrorf("getVolumeIPList failed: cluster[%v] volume[%v] err[%v]", cluster, volume, err)
		return 0, err
	}
	// {"code":0,"msg":null,"data":[{"VOLUME_NAME":"dcc_3vol","COUNT":205}]}
	res := make([]*proto.VolumeClientCount, 0)
	if err = json.Unmarshal(respData, &res); err != nil {
		log.LogErrorf("getVolumeIPCount failed: cluster[%v] volume[%v] Unmarshal resp err[%v]", cluster, volume, err)
		return 0, err
	}
	var volIPCount = new(proto.VolumeClientCount)
	if len(res) > 0 {
		volIPCount = res[0]
	}
	return volIPCount.Count, nil
}

func getVolumeIPList(cluster, volume string) (ipList []string, err error) {
	var (
		url      string
		respData []byte
	)
	url = fmt.Sprintf("http://api.storage.hbase.jd.local/queryJson/cfsClientList")

	req := cutil.NewAPIRequest(http.MethodGet, url)
	endTime := time.Now()
	req.AddParam("cluster", cluster)
	req.AddParam("volume", volume)
	req.AddParam("end", endTime.Format(proto.TimeFormatCompact))
	req.AddParam("start", endTime.Add(-24*time.Hour).Format(proto.TimeFormatCompact))
	if respData, err = cutil.SendSimpleRequest(req, false); err != nil {
		log.LogErrorf("getVolumeIPList failed: cluster[%v] volume[%v] err[%v]", cluster, volume, err)
		return
	}

	var volIPList []*proto.VolumeIPInfo
	if err = json.Unmarshal(respData, &volIPList); err != nil {
		log.LogErrorf("getVolumeIPList failed: cluster[%v] volume[%v] Unmarshal resp err[%v]", cluster, volume, err)
		return
	}

	ipList = make([]string, 0)
	for _, ipInfo := range volIPList {
		ipList = append(ipList, ipInfo.ClientIP)
	}
	if log.IsDebugEnabled() {
		log.LogDebugf("getVolumeIPList success: cluster[%v] volume[%v] cost[%v]", cluster, volume, time.Now().Sub(endTime))
	}
	return
}

func _getVolumeClientCSV(cluster, volume string) (filename string, err error) {
	ipList, err := getVolumeIPList(cluster, volume)
	if err != nil {
		return
	}

	filename = "./" + fmt.Sprintf("%s-%s-%s", time.Now().Format(proto.TimeFormatCompact), cluster, volume)
	dst, _ := os.Create(filename)
	var content strings.Builder
	for _, ip := range ipList {
		content.WriteString(ip + "\n")
	}
	_, err = dst.Write([]byte(content.String()))
	if err != nil {
		return
	}
	dst.Close()
	log.LogInfof("_getVolumeClientCSV: success write file: %v", filename)
	return filename, nil
}
