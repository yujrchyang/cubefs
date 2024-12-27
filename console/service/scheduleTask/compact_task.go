package scheduleTask

import (
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/console/cutil"
	"github.com/cubefs/cubefs/console/model"
	cproto "github.com/cubefs/cubefs/console/proto"
	api "github.com/cubefs/cubefs/console/service/apiManager"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
	"io/ioutil"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	defaultVolConcurrency   = 2
	defaultMpConcurrency    = 5
	defaultInodeConcurrency = 20
	defaultEkMinLength      = 10
	defaultEkMaxAvgSize     = 64   // ek max avg size, uint MB
	defaultInodeMinSize     = 1024 // inode min size, uint Byte
	defaultSizeRange        = "1,128"
)

type CompactWorker struct {
	api *api.APIManager //调用master接口用
}

func NewCompactWorker(api *api.APIManager) *CompactWorker {
	compact := &CompactWorker{
		api: api,
	}
	return compact
}

func (c *CompactWorker) ListCompactVol(cluster string, status *bool) []*proto.DataMigVolume {
	mc := c.api.GetMasterClient(cluster)
	vols, err := mc.AdminAPI().ListCompactVolumes()
	if err != nil {
		log.LogErrorf("ListCompactVol failed: cluster(%v) status(%v) err(%v)", cluster, status, err)
		return nil
	}
	sort.Slice(vols, func(i, j int) bool { return vols[i].Name < vols[j].Name })
	if status == nil {
		return vols
	}
	result := make([]*proto.DataMigVolume, 0)

	for _, cVol := range vols {
		if *status {
			if cVol.CompactTag != proto.CompactOpen {
				continue
			}
		} else {
			if cVol.CompactTag == proto.CompactOpen {
				continue
			}
		}
		result = append(result, cVol)
	}
	return result
}

func (c *CompactWorker) CheckCompactVolList(cluster string) []*proto.DataMigVolume {
	var (
		err  error
		vols []*proto.VolInfo
		res  = make([]*proto.DataMigVolume, 0)
	)
	mc := c.api.GetMasterClient(cluster)
	vols, err = mc.AdminAPI().ListVols("")
	if err != nil {
		log.LogErrorf("CheckCompactVolList failed: cluster(%v) err(%v)", cluster, err)
		return nil
	}
	for _, vol := range vols {
		volInfo, _ := mc.AdminAPI().GetVolumeSimpleInfo(vol.Name)
		if volInfo.CrossRegionHAType != proto.CrossRegionHATypeQuorum && !volInfo.ForceROW {
			continue
		}
		res = append(res, convertCompactVolInfo(vol))
	}
	return res
}

func convertCompactVolInfo(vol *proto.VolInfo) *proto.DataMigVolume {
	return &proto.DataMigVolume{
		Name:       vol.Name,
		Owner:      vol.Owner,
		Status:     vol.Status,
		TotalSize:  vol.TotalSize,
		UsedSize:   vol.UsedSize,
		CreateTime: vol.CreateTime,
		ForceROW:   vol.ForceROW,
		CompactTag: proto.CompactTag(vol.CompactTag),
	}
}

func (c *CompactWorker) BatchOpenCompactVol(cluster string, vols []*proto.DataMigVolume) error {
	mc := c.api.GetMasterClient(cluster)
	var errMsg string
	for _, vol := range vols {
		authKey := cutil.CalcAuthKey(vol.Owner)
		_, err := mc.AdminAPI().SetCompact(vol.Name, proto.CompactOpenName, authKey)
		if err != nil {
			errMsg += fmt.Sprintf("*** %s", vol.Name)
		}
	}
	if errMsg != "" {
		errMsg += "***,open compact field"
	} else {
		return nil
	}
	log.LogErrorf("BatchOpenCompactVol: cluster(%s) errMsg: %s", cluster, errMsg)
	return errors.New(errMsg)
}

func (c *CompactWorker) BatchCloseCompactVol(cluster string, vols []*proto.DataMigVolume) error {
	// 得是在compact中有的（list列表）
	mc := c.api.GetMasterClient(cluster)
	var errMsg string
	for _, vol := range vols {
		authKey := cutil.CalcAuthKey(vol.Owner)
		_, err := mc.AdminAPI().SetCompact(vol.Name, proto.CompactCloseName, authKey)
		if err != nil {
			errMsg += fmt.Sprintf("*** %s", vol.Name)
		}
	}
	if errMsg != "" {
		errMsg += "***, close compact failed"
	} else {
		return nil
	}
	log.LogErrorf("BatchCloseCompactVol: cluster(%s) errMsg: %s", cluster, errMsg)
	return errors.New(errMsg)
}

type fragView struct {
	volume    string
	mpID      uint64
	inode     uint64
	ekLength  uint64
	ekAvgSize uint64
	inodeSize uint64
}

func (c *CompactWorker) CheckFrag(recordID uint64, request *model.CheckFragRequest) {
	log.LogInfof("CheckFrag: start task id(%v)", recordID)
	mc := c.api.GetMasterClient(request.Cluster)
	var mps []*proto.MetaPartitionView
	var err error
	if mps, err = mc.ClientAPI().GetMetaPartitions(request.Volume); err != nil {
		log.LogErrorf("CheckFrag: get vol(%s) mp failed, err(%v)", request.Volume, err)
		return
	}
	checkMps(mc, mps, request)

	link, err := saveCheckResultToS3(request)
	if err != nil {
		log.LogErrorf("CheckFrag: save result file to s3 failed, req(%v) err(%v)", request, err)
		return
	}
	table := model.CheckCompactFragRecord{}
	table.UpdateRecord(recordID, link)
	log.LogInfof("CheckFrag: update record success, id(%v) request(%v) link(%v)", recordID, request, link)
	return
}

func checkMps(mc *master.MasterClient, mps []*proto.MetaPartitionView, request *model.CheckFragRequest) {
	sizeRanges := sizeRangeStrToInt(request.SizeRange)
	var ekInfosMutex sync.Mutex
	ekbaseInfos := make([]*ekBaseInfo, len(sizeRanges)+1)
	for i := 0; i < len(sizeRanges)+1; i++ {
		ekbaseInfos[i] = &ekBaseInfo{}
	}
	ek := &ekInfo{
		mpCount: len(mps),
	}
	var wg sync.WaitGroup // mp 并发
	var ch = make(chan struct{}, request.MpConcurrency)
	for _, mp := range mps {
		wg.Add(1)
		ch <- struct{}{}
		go func(mp *proto.MetaPartitionView, ek *ekInfo, baseInfos []*ekBaseInfo, mutex *sync.Mutex) {
			defer func() {
				wg.Done()
				<-ch
			}()
			var (
				mpInfo *proto.MetaPartitionInfo
				err    error
			)
			if mpInfo, err = mc.ClientAPI().GetMetaPartition(mp.PartitionID, ""); err != nil {
				log.LogErrorf("checkMps: getMetaPartition failed, vol(%s) mp(%v) err(%v)", request.Volume, mp.PartitionID, err)
				return
			}
			leaderAddr := getLeaderAddr(mpInfo.Replicas)
			var leaderNodeInfo *proto.MetaNodeInfo
			if leaderNodeInfo, err = mc.NodeAPI().GetMetaNode(leaderAddr); err != nil {
				log.LogErrorf("checkMps: getMetaNode failed: vol(%s) mp(%v) addr(%v) err(%v)", request.Volume, mp.PartitionID, leaderAddr, err)
				return
			}
			if leaderNodeInfo.ProfPort == "" {
				leaderNodeInfo.ProfPort = "9092"
			}
			leaderIpPort := strings.Split(leaderNodeInfo.Addr, ":")[0] + ":" + leaderNodeInfo.ProfPort
			metaAdminApi := meta.NewMetaHttpClient(leaderIpPort, false)
			var inodeIds *proto.MpAllInodesId
			if inodeIds, err = metaAdminApi.ListAllInodesId(mp.PartitionID, 0, 0, 0); err != nil {
				log.LogErrorf("checkMps: listAllInodes failed: vol(%s) mp(%v) leader(%s) err(%v)", request.Volume, mp.PartitionID, leaderIpPort, err)
				return
			}
			inodeInfoCheck(mp.PartitionID, inodeIds.Inodes, leaderIpPort, request, sizeRanges, ek, baseInfos, mutex)
		}(mp, ek, ekbaseInfos, &ekInfosMutex)
	}
	wg.Wait()
	// check Frag file
	if request.SaveFragment {
		// 先根据sizeRange写一个头，再写内容
		printFragmentToFile(request, sizeRanges, ekbaseInfos, ek)
	}
}

func inodeInfoCheck(mpID uint64, inodes []uint64, leader string, request *model.CheckFragRequest, sizeRanges []int,
	ek *ekInfo, baseInfos []*ekBaseInfo, mutex *sync.Mutex) {

	var mu sync.Mutex
	var wg sync.WaitGroup
	var ch = make(chan struct{}, request.InodeConcurrency)
	for _, inode := range inodes {
		wg.Add(1)
		ch <- struct{}{}
		go func(inode uint64, ek *ekInfo, baseInfos []*ekBaseInfo, mutex *sync.Mutex, stdMutex *sync.Mutex) {
			defer func() {
				wg.Done()
				<-ch
			}()
			var (
				extentInfo *proto.GetExtentsResponse
				err        error
			)
			if extentInfo, err = getInodeEkInfo(mpID, inode, leader); err != nil {
				log.LogErrorf("inodeInfoCheck: getInodeEkInfo failed, mp(%v) ino(%v) leader(%v) err(%v)", mpID, inode, leader, err)
				return
			}
			if extentInfo == nil || len(extentInfo.Extents) == 0 {
				return
			}
			// stdout的内容 写到一个文件中， 加锁保证并发
			std := printOverlapInode(leader, request.Volume, mpID, inode, extentInfo)
			extLength := len(extentInfo.Extents)
			addEKData(request.Volume, uint64(extLength), extentInfo.Size, inode, sizeRanges, ek, baseInfos, mutex)
			if extentInfo.Size <= request.InodeMinSize {
				return
			}
			ekAvgSize := extentInfo.Size / uint64(extLength)
			if uint64(extLength) > request.EkMinLength && ekAvgSize < request.EkMaxAvgSize {
				mutex.Lock()
				ek.needCompactInodeCount++
				ek.needCompactEkCount += uint64(extLength)
				mutex.Unlock()
				std.WriteString(formatCompactCheckFragView(request.Volume, mpID, inode, extLength, ekAvgSize, extentInfo.Size))
			}
			// std 写到文件中
			stdMutex.Lock()
			stdoutToFile(request, std)
			stdMutex.Unlock()
		}(inode, ek, baseInfos, mutex, &mu)
	}
	wg.Wait()
}

func addEKData(volName string, extLength uint64, inodeSize uint64, inodeId uint64, sizeRanges []int,
	ek *ekInfo, baseInfos []*ekBaseInfo, mutex *sync.Mutex) {
	mutex.Lock()
	defer mutex.Unlock()

	index := calSizeRangeIndex(inodeSize, sizeRanges)
	baseInfos[index].inodeCount++
	baseInfos[index].totalEk += extLength
	baseInfos[index].totalInodeSize += inodeSize

	if ek.maxEkLen < extLength {
		ek.maxEkLen = extLength
		ek.maxEkLenInodeId = inodeId
		ek.maxEkLenInodeSize = inodeSize
	}
}

func getInodeEkInfo(mpID uint64, inode uint64, leader string) (re *proto.GetExtentsResponse, err error) {
	url := fmt.Sprintf("http://%s/getExtentsByInode?pid=%d&ino=%d", leader, mpID, inode)
	httpClient := http.Client{Timeout: 10 * time.Second}
	resp, err := httpClient.Get(url)
	if err != nil {
		return
	}
	respData, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}
	var data []byte
	if data, err = parseResp(respData); err != nil {
		return
	}
	if len(data) == 0 {
		return nil, nil
	}
	re = &proto.GetExtentsResponse{}
	if err = json.Unmarshal(data, &re); err != nil {
		return
	}
	if re == nil {
		err = fmt.Errorf("get %s fails, data: %s", url, string(data))
		return
	}
	return
}

func getInode(leaderIpPort string, mpId, ino uint64) (inodeInfoView *proto.InodeInfoView, err error) {
	httpClient := http.Client{Timeout: 10 * time.Second}
	resp, err := httpClient.Get(fmt.Sprintf("http://%s/getInode?pid=%d&ino=%d", leaderIpPort, mpId, ino))
	if err != nil {
		return
	}
	all, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}
	value := make(map[string]interface{})
	err = json.Unmarshal(all, &value)
	if err != nil {
		return
	}
	if value["msg"] != "Ok" {
		return
	}
	data := value["data"].(map[string]interface{})
	dataInfo := data["info"].(map[string]interface{})
	inodeInfoView = &proto.InodeInfoView{
		Ino:         uint64(dataInfo["ino"].(float64)),
		PartitionID: mpId,
		At:          dataInfo["at"].(string),
		Ct:          dataInfo["ct"].(string),
		Mt:          dataInfo["mt"].(string),
		Nlink:       uint64(dataInfo["nlink"].(float64)),
		Size:        uint64(dataInfo["sz"].(float64)),
		Gen:         uint64(dataInfo["gen"].(float64)),
		Gid:         uint64(dataInfo["gid"].(float64)),
		Uid:         uint64(dataInfo["uid"].(float64)),
		Mode:        uint64(dataInfo["mode"].(float64)),
	}
	return
}

type ekBaseInfo struct {
	inodeCount     uint64
	totalEk        uint64
	totalInodeSize uint64
	avgEk          uint64
	avgInodeSize   uint64
	avgEkSize      uint64
}
type ekInfo struct {
	mpCount               int
	needCompactInodeCount uint64
	needCompactEkCount    uint64
	maxEkLen              uint64
	maxEkLenInodeId       uint64
	maxEkLenInodeSize     uint64
}

func sizeRangeStrToInt(sizeRange string) (sizeRanges []int) {
	// 提取数值
	for _, sr := range strings.Split(sizeRange, ",") {
		if sr != "" {
			v, _ := strconv.Atoi(sr)
			if v > 0 {
				sizeRanges = append(sizeRanges, v)
			}
		}
	}
	// 去重
	m := make(map[int]struct{})
	for _, v := range sizeRanges {
		m[v] = struct{}{}
	}
	sizeRanges = sizeRanges[:0]
	for k := range m {
		sizeRanges = append(sizeRanges, k)
	}
	// 排序
	sort.Slice(sizeRanges, func(i, j int) bool {
		return sizeRanges[i] < sizeRanges[j]
	})
	return
}

func getLeaderAddr(replicas []*proto.MetaReplicaInfo) (leaderAddr string) {
	for _, replica := range replicas {
		if replica.IsLeader {
			leaderAddr = replica.Addr
			break
		}
	}
	return
}

func calSizeRangeIndex(inodeSize uint64, sizeRanges []int) int {
	if inodeSize == 0 {
		return -1
	}
	if len(sizeRanges) == 1 {
		if inodeSize <= uint64(sizeRanges[0])*1024*1024 {
			return 0
		}
		if inodeSize > uint64(sizeRanges[0])*1024*1024 {
			return 1
		}
	}
	for i, sr := range sizeRanges {
		if i == 0 {
			if inodeSize <= uint64(sr)*1024*1024 {
				return 0
			}
			if inodeSize > uint64(sr)*1024*1024 && inodeSize <= uint64(sizeRanges[i+1])*1024*1024 {
				return 1
			}
		} else if i == len(sizeRanges)-1 {
			if inodeSize > uint64(sr)*1024*1024 {
				return i + 1
			}
			if inodeSize > uint64(sizeRanges[i-1])*1024*1024 && inodeSize <= uint64(sr)*1024*1024 {
				return i
			}
		} else {
			if inodeSize <= uint64(sr)*1024*1024 && inodeSize > uint64(sizeRanges[i-1])*1024*1024 {
				return i
			}
		}
	}
	return 0
}

func parseResp(resp []byte) (data []byte, err error) {
	var body = &struct {
		Code int32           `json:"code"`
		Msg  string          `json:"msg"`
		Data json.RawMessage `json:"data"`
	}{}
	if err = json.Unmarshal(resp, &body); err != nil {
		return
	}
	data = body.Data
	return
}

func printFragmentToFile(request *model.CheckFragRequest, sizeRanges []int, baseEks []*ekBaseInfo, ek *ekInfo) {
	// 先写title
	var groupStr string
	if len(sizeRanges) == 0 {
		groupStr += fmt.Sprintf("inodeCount,avgEk,avgEkSize,avgInodeSize")
	} else if len(sizeRanges) == 1 {
		groupStr += fmt.Sprintf("\"(0,%vMB]\" inodeCount,\"(0,%vMB]\" avgEk,\"(0,%vMB]\" avgEkSize,\"(0,%vMB]\" avgInodeSize,", sizeRanges[0], sizeRanges[0], sizeRanges[0], sizeRanges[0])
		groupStr += fmt.Sprintf("\"(%vMB,∞)\" inodeCount,\"(%vMB,∞)\" avgEk,\"(%vMB,∞)\" avgEkSize,\"(%vMB,∞)\" avgInodeSize", sizeRanges[0], sizeRanges[0], sizeRanges[0], sizeRanges[0])
	} else {
		for i, sizeRange := range sizeRanges {
			if i == 0 {
				groupStr += fmt.Sprintf("\"(0,%vMB]\" inodeCount,\"(0,%vMB]\" avgEk,\"(0,%vMB]\" avgEkSize,\"(0,%vMB]\" avgInodeSize,", sizeRange, sizeRange, sizeRange, sizeRange)
			} else if i < len(sizeRanges)-1 {
				groupStr += fmt.Sprintf("\"(%v,%vMB]\" inodeCount,\"(%v,%vMB]\" avgEk,\"(%v,%vMB]\" avgEkSize,\"(%v,%vMB]\" avgInodeSize,", sizeRanges[i-1], sizeRange, sizeRanges[i-1], sizeRange, sizeRanges[i-1], sizeRange, sizeRanges[i-1], sizeRange)
			} else {
				groupStr += fmt.Sprintf("\"(%v,%vMB]\" inodeCount,\"(%v,%vMB]\" avgEk,\"(%v,%vMB]\" avgEkSize,\"(%v,%vMB]\" avgInodeSize,", sizeRanges[i-1], sizeRange, sizeRanges[i-1], sizeRange, sizeRanges[i-1], sizeRange, sizeRanges[i-1], sizeRange)
				groupStr += fmt.Sprintf("\"(%vMB,∞)\" inodeCount,\"(%vMB,∞)\" avgEk,\"(%vMB,∞)\" avgEkSize,\"(%vMB,∞)\" avgInodeSize", sizeRange, sizeRange, sizeRange, sizeRange)
			}
		}
	}

	var (
		totalInodeCount uint64
		totalEk         uint64
	)
	for _, baseEk := range baseEks {
		if baseEk.inodeCount == 0 {
			continue
		}
		totalInodeCount += baseEk.inodeCount
		totalEk += baseEk.totalEk
		baseEk.avgEk = baseEk.totalEk / baseEk.inodeCount
		baseEk.avgInodeSize = baseEk.totalInodeSize / baseEk.inodeCount
		baseEk.avgEkSize = baseEk.totalInodeSize / baseEk.totalEk
		if totalInodeCount == 0 {
			return
		}
	}
	var std strings.Builder
	if ek.needCompactInodeCount == 0 {
		std.WriteString(fmt.Sprintf("needCompactInodeCount:%v needCompactEkCount:%v\n", ek.needCompactInodeCount, ek.needCompactEkCount))
	} else {
		std.WriteString(fmt.Sprintf("needCompactInodeCount:%v needCompactEkCount:%v needCompactAvgEk:%v\n", ek.needCompactInodeCount, ek.needCompactEkCount, ek.needCompactEkCount/ek.needCompactInodeCount))
	}
	fragmentTitle := fmt.Sprintf("volume,mpCount,totalInodeCount,totalEk,avgEk,maxEkLenInodeId,maxEkLen,maxEkLenInodeSize,maxLenAvgEkSize,fragment level,%v\n", groupStr)
	std.WriteString(fragmentTitle)

	var ekBaseInfoStr string
	for i, baseInfo := range baseEks {
		base := fmt.Sprintf("%v,%v,%v,%v", baseInfo.inodeCount, baseInfo.avgEk, baseInfo.avgEkSize, baseInfo.avgInodeSize)
		if i != len(baseEks)-1 {
			ekBaseInfoStr += base + ","
		} else {
			ekBaseInfoStr += base
		}
	}
	maxLenAvgEkSize := ek.maxEkLenInodeSize / ek.maxEkLen
	fragmentLevel := calFragmentLevel(ek.maxEkLen, maxLenAvgEkSize)
	volumeFrgResult := fmt.Sprintf("%v,%v,%v,%v,%v,%v,%v,%v,%v,%v,%v\n",
		request.Volume, ek.mpCount, totalInodeCount, totalEk, totalEk/totalInodeCount, ek.maxEkLenInodeId, ek.maxEkLen, ek.maxEkLenInodeSize, maxLenAvgEkSize, fragmentLevel, ekBaseInfoStr)
	std.WriteString(volumeFrgResult)
	// 追加写到文件
	stdoutToFile(request, &std)
}

func printOverlapInode(leader string, vol string, mpID, inode uint64, extentInfo *proto.GetExtentsResponse) *strings.Builder {
	if extentInfo == nil {
		return nil
	}
	// inode 各自写各自的
	var sb *strings.Builder
	for i, extent := range extentInfo.Extents {
		if i >= len(extentInfo.Extents)-1 {
			break
		}
		if extent.FileOffset+uint64(extent.Size) > extentInfo.Extents[i+1].FileOffset {
			var ct, mt string
			var inodeInfo *proto.InodeInfoView
			var err error
			inodeInfo, err = getInode(leader, mpID, inode)
			if inodeInfo != nil && err == nil {
				ct = inodeInfo.Ct
				mt = inodeInfo.Mt
			}
			// volume mpId inodeId ct mt preEkEnd nextEkOffset
			sb.WriteString(fmt.Sprintf("%v,%v,%v,%v,%v,%v,%v \n", vol, mpID, inode, ct, mt, extent.FileOffset+uint64(extent.Size), extentInfo.Extents[i+1].FileOffset))
			break
		}
	}
	return sb
}

var checkFragViewTableRowPattern = "%-63v    %-8v    %-8v    %-8v    %-8v    %-8v\n"

func formatCompactCheckFragView(volName string, mpId, inodeId uint64, extLength int, ekAvgSize uint64, inodeSize uint64) string {
	return fmt.Sprintf(checkFragViewTableRowPattern, volName, mpId, inodeId, extLength, ekAvgSize, inodeSize)
}

func stdoutToFile(request *model.CheckFragRequest, sb *strings.Builder) {
	filename := cproto.ConsoleSchedule + "/" + fmt.Sprintf("compact-%s-%s", request.Cluster, request.Volume)
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.LogErrorf("stdoutToFile open file(%v) failed, err(%v)", filename, err)
		return
	}
	_, err = f.Write([]byte(sb.String()))
	if err != nil {
		log.LogErrorf("stdoutToFile write file(%v) failed, err(%v)", filename, err)
		return
	}
	f.Close()
	return
}

func saveCheckResultToS3(request *model.CheckFragRequest) (link string, err error) {

	var filename string
	needSave := request.SaveOverlap || request.SaveFragment
	if needSave {
		var f *os.File
		filename = cproto.ConsoleSchedule + "/" + fmt.Sprintf("compact-%s-%s", request.Cluster, request.Volume)
		f, err = os.OpenFile(filename, os.O_RDONLY, 0666)
		if err != nil {
			return
		}
		link, err = cutil.CFS_S3.GetDownloadPresignUrl(filename, f)
	}
	log.LogInfof("saveCheckResultToS3: needSave(%v) file(%v) link(%v)", needSave, filename, link)
	return
}

func calFragmentLevel(maxEkLen, maxLenAvgEkSize uint64) string {
	if maxEkLen > 100 && maxLenAvgEkSize < 1*1024*1024 {
		return "高"
	} else if maxEkLen > 100 && maxLenAvgEkSize < 5*1024*1024 {
		return "中"
	} else {
		return "低"
	}
}
