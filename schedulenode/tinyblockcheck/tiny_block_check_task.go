package tinyblck

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/metanode"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/storage"
	"github.com/cubefs/cubefs/util/bitset"
	"github.com/cubefs/cubefs/util/connpool"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/topology"
	"github.com/cubefs/cubefs/util/unit"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"path"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"
)

var gConnPool = connpool.NewConnectPool()

type TinyBlockCheckTask struct {
	*proto.Task
	mpParaCnt              int
	safeCleanInterval      int64
	garbageSize            uint64
	exportDir              string
	needClean              bool
	needDumpResult         bool
	tinyExtentsHolesMap    map[uint64][]*ExtentHolesInfo
	dataTinyExtentsMap     map[uint64][]*bitset.ByteSliceBitSet
	metaTinyExtentsMap     map[uint64][]*bitset.ByteSliceBitSet
	garbageTinyBlocks      map[uint64][]*bitset.ByteSliceBitSet
	metaTinyExtentsSizeMap map[uint64][]uint64
	dataTinyExtentsSizeMap map[uint64]uint64
	tinyGarbageBlocks      map[string][]*ExtentInfo
	masterClient           *master.MasterClient
	metaExportDir          string
	checkByMetaDumpFile    bool
}

func NewTinyBlockCheckTask(task *proto.Task, mc *master.MasterClient, needClean, needDumpResult bool, safeCleanIntervalSecond int64, exportDir string) *TinyBlockCheckTask {
	tinyBlckTask := new(TinyBlockCheckTask)
	tinyBlckTask.Task = task
	tinyBlckTask.masterClient = mc
	tinyBlckTask.needClean = needClean
	tinyBlckTask.needDumpResult = needDumpResult
	tinyBlckTask.exportDir = exportDir
	tinyBlckTask.safeCleanInterval = safeCleanIntervalSecond
	return tinyBlckTask
}

func (t *TinyBlockCheckTask) getDataPartitionView(dpID uint64, dataNodeAddr string) (dpView *DataPartitionView, err error) {
	var (
		resp *http.Response
		respData []byte
		client = &http.Client{}
	)
	client.Timeout = 60 * time.Second
	url := fmt.Sprintf("http://%s:%v/partition?id=%v", strings.Split(dataNodeAddr, ":")[0], t.masterClient.DataNodeProfPort, dpID)
	resp, err = client.Get(url)
	if err != nil {
		log.LogErrorf("get url(%s) failed, cluster:%s, error:%v", url, t.Cluster, err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("invalid status code: %v %v", url, resp.StatusCode)
		return
	}

	respData, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		err = errors.NewErrorf("read all response body failed: %v", err)
		return
	}

	body := &struct {
		Code int32           `json:"code"`
		Msg  string          `json:"msg"`
		Data json.RawMessage `json:"data"`
	}{}

	if err = json.Unmarshal(respData, &body); err != nil {
		log.LogErrorf("Unmarshal resp data failed, cluster(%s), url(%s) err(%v)", t.Cluster, url, err)
		return
	}

	if body.Code != 200 {
		err = fmt.Errorf("resp code not ok")
		log.LogErrorf("resp code not ok, cluster(%s), url(%s) code(%v)", t.Cluster, url, body.Code)
		return
	}

	dpView = new(DataPartitionView)
	if t.masterClient.IsDbBack {
		dbBackDataPartitionView := new(proto.DataPartitionViewDbBack)
		if err = json.Unmarshal(body.Data, dbBackDataPartitionView); err != nil {
			log.LogErrorf("Unmarshal data partition view failed, cluster(%s), url(%s) err(%v)", t.Cluster, url, err)
			return
		}
		dpView.VolName = dbBackDataPartitionView.VolName
		dpView.ID = uint64(dbBackDataPartitionView.ID)
		dpView.FileCount = dbBackDataPartitionView.FileCount
		for _, file := range dbBackDataPartitionView.Files {
			extentInfo := storage.ExtentInfoBlock{}
			extentInfo[proto.FileID] = file.FileId
			extentInfo[proto.Size] = file.Size
			extentInfo[proto.Crc] = uint64(file.Crc)
			extentInfo[proto.ModifyTime] = uint64(file.ModTime.Unix())
			dpView.Files = append(dpView.Files, extentInfo)
		}
		return
	}

	if err = json.Unmarshal(body.Data, dpView); err != nil {
		log.LogErrorf("Unmarshal data partition view failed, cluster(%s), url(%s) err(%v)", t.Cluster, url, err)
		return
	}

	return
}

func (t *TinyBlockCheckTask)checkNodeStatus(ip, profPort string) (startComplete bool) {
	client := &http.Client{}
	client.Timeout = time.Second * 60
	url := fmt.Sprintf("http://%s:%v/status", ip, profPort)
	resp, err := client.Get(url)
	if err != nil {
		log.LogErrorf("get url(%s) failed, cluster:%s, error:%v", url, t.Cluster, err)
		return
	}
	defer resp.Body.Close()

	var respData []byte
	respData, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		log.LogErrorf("read all response body failed: %v", err)
		return
	}

	body := &struct {
		StartComplete bool `json:"StartComplete"`
	}{}
	if err = json.Unmarshal(respData, &body); err != nil {
		log.LogErrorf("Unmarshal resp data failed, cluster(%s), url(%s) err(%v)", t.Cluster, url, err)
		return
	}
	startComplete = body.StartComplete
	return
}

func (t *TinyBlockCheckTask) getExtentHoles(dpID, extentID uint64, dataNodeAddr string) (extentHolesInfo *ExtentHolesInfo, err error) {
	var (
		resp     *http.Response
		respData []byte
		client   = &http.Client{}
	)
	client.Timeout = 60 * time.Second
	url := fmt.Sprintf("http://%s:%v/tinyExtentHoleInfo?partitionID=%v&extentID=%v",
		strings.Split(dataNodeAddr, ":")[0], t.masterClient.DataNodeProfPort, dpID, extentID)
	resp, err = client.Get(url)
	if err != nil {
		log.LogErrorf("get url(%s) failed, cluster:%s, error:%v", url, t.Cluster, err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		err = fmt.Errorf("invalid status code: %v %v", url, resp.StatusCode)
		return
	}
	respData, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		err = errors.NewErrorf("read all response body failed: %v", err)
		return
	}
	body := &struct {
		Code int32           `json:"code"`
		Msg  string          `json:"msg"`
		Data json.RawMessage `json:"data"`
	}{}
	if err = json.Unmarshal(respData, &body); err != nil {
		log.LogErrorf("Unmarshal resp data failed, cluster(%s), url(%s) err(%v)", t.Cluster, url, err)
		return
	}
	if body.Code != 200 {
		err = fmt.Errorf("resp code not ok")
		log.LogErrorf("resp code not ok, cluster(%s), url(%s) code(%v)", t.Cluster, url, body.Code)
		return
	}
	extentHolesInfo = new(ExtentHolesInfo)
	if err = json.Unmarshal(body.Data, extentHolesInfo); err != nil {
		log.LogErrorf("Unmarshal data partition view failed, cluster(%s), url(%s) err(%v)", t.Cluster, url, err)
		return
	}
	return
}

func (t *TinyBlockCheckTask) getTinyExtentsByDataPartition(dpId uint64, dataNodeAddr string) (extentsInfo []storage.ExtentInfoBlock, err error) {
	var dpView *DataPartitionView
	dpView, err = t.getDataPartitionView(dpId, dataNodeAddr)
	if err != nil {
		log.LogErrorf("action[getExtentsByDataPartition] get cluster[%s] data partition[id: %v, addr: %s] view failed:%v",
			t.Cluster, dpId, dataNodeAddr, err)
		return
	}
	extentsInfo = make([]storage.ExtentInfoBlock, 0, len(dpView.Files))
	for _, ex := range dpView.Files {
		if !t.masterClient.IsDbBack && proto.IsTinyExtent(ex[storage.FileID]) {
			extentsInfo = append(extentsInfo, ex)
		}
	}
	return
}

//func (t *TinyBlockCheckTask) getTinyExtentsHoles(dpId uint64, dataNodeAddr string) (extentsInfo []storage.ExtentInfoBlock, err error) {
//	var dpView *DataPartitionView
//	dpView, err = t.getDataPartitionView(dpId, dataNodeAddr)
//	if err != nil {
//		log.LogErrorf("action[getExtentsByDataPartition] get cluster[%s] data partition[id: %v, addr: %s] view failed:%v",
//			t.Cluster, dpId, dataNodeAddr, err)
//		return
//	}
//	extentsInfo = make([]storage.ExtentInfoBlock, 0, len(dpView.Files))
//	for _, ex := range dpView.Files {
//		if !t.masterClient.IsDbBack && proto.IsTinyExtent(ex[storage.FileID]) {
//			extentsInfo = append(extentsInfo, ex)
//		}
//	}
//	return
//}
//
//func (t *TinyBlockCheckTask) getExtentsInfoByDataPartition(dpId uint64, dataNodeAddr string, skipTiny bool) (extentsInfo []*ExtentInfo, err error) {
//	var dpView *DataPartitionView
//	dpView, err = t.getDataPartitionView(dpId, dataNodeAddr)
//	if err != nil {
//		log.LogErrorf("action[getExtentsByDataPartition] get cluster[%s] data partition[id: %v, addr: %s] view failed:%v",
//			t.Cluster, dpId, dataNodeAddr, err)
//		return
//	}
//	extentsInfo = make([]*ExtentInfo, 0, len(dpView.Files))
//	for _, ex := range dpView.Files {
//		if skipTiny && ((!t.masterClient.IsDbBack && proto.IsTinyExtent(ex[storage.FileID])) || (t.masterClient.IsDbBack && ex[storage.FileID] >= 50000000)) {
//			continue
//		}
//		extentsInfo = append(extentsInfo, &ExtentInfo{
//			DataPartitionID: dpId,
//			ExtentID:        ex[storage.FileID],
//			Size:            uint32(ex[storage.Size]),
//		})
//	}
//	return
//}

func (t *TinyBlockCheckTask) getTinyExtentsFromMetaPartition(mpId uint64, leaderAddr string, ExtentInfoCh chan *ExtentInfo) (err error) {
	if t.masterClient.IsDbBack {
		return
	}
	var resp *proto.MpAllInodesId
	var retryCnt = 5
	var metaHttpClient *meta.MetaHttpClient
	metaHttpClient = meta.NewMetaHttpClient(fmt.Sprintf("%s:%v", strings.Split(leaderAddr, ":")[0], t.masterClient.MetaNodeProfPort), false)
	for retryCnt > 0 {
		err = nil
		resp, err = metaHttpClient.ListAllInodesIDAndDelInodesID(mpId, 0, 0, 0)
		if err == nil {
			break
		}
		time.Sleep(time.Second*5)
		retryCnt--
	}
	if err != nil {
		log.LogErrorf("action[getExtentsFromMetaPartition] get cluster[%s] mp[%v] all inode info failed:%v",
			t.Cluster, mpId, err)
		return
	}

	inodeIDCh := make(chan uint64, 256)
	go func() {
		for _, inoID := range resp.Inodes {
			inodeIDCh <- inoID
		}
		for _, delInodeID := range resp.DelInodes {
			inodeIDCh <- delInodeID
		}
		close(inodeIDCh)
	}()

	errorCh := make(chan error, resp.Count)
	defer func() {
		close(errorCh)
	}()
	var wg sync.WaitGroup
	inodeConcurrency := parallelInodeCnt.Load()
	for i := int32(0); i < inodeConcurrency ; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				inodeID, ok := <- inodeIDCh
				if !ok {
					break
				}
				var eksResp *proto.GetExtentsResponse
				var errInfo error
				var retryCount = 5
				for retryCount > 0 {
					errInfo = nil
					eksResp, errInfo = metaHttpClient.GetExtentKeyByDelInodeId(mpId, inodeID)
					if errInfo == nil {
						break
					}
					time.Sleep(time.Second*5)
					retryCount--
				}
				if errInfo != nil {
					errorCh <- errInfo
					log.LogErrorf("action[getExtentsFromMetaPartition] get cluster[%s] mp[%v] extent " +
						"key by inode id[%v] failed: %v", t.Cluster, mpId, inodeID, errInfo)
					continue
				}
				//log.LogDebugf("inode id:%v, eks cnt:%v", inodeID, len(eksResp.Extents))
				for _, ek := range eksResp.Extents {
					if !proto.IsTinyExtent(ek.ExtentId) {
						continue
					}
					ExtentInfoCh <- &ExtentInfo{
						DataPartitionID: ek.PartitionId,
						ExtentID:        ek.ExtentId,
						ExtentOffset:    ek.ExtentOffset,
						Size:            ek.Size,
					}
				}
			}
		}()
	}
	wg.Wait()

	select {
	case <-errorCh:
		err = errors.NewErrorf("get extent key by inode id failed")
		return
	default:
	}
	return
}

func batchDeleteExtent(host string, dp *topology.DataPartition, eks []*proto.MetaDelExtentKey) (err error) {
	var conn *net.TCPConn
	conn, err = gConnPool.GetConnect(host)
	defer func() {
		if err != nil {
			gConnPool.PutConnect(conn, true)
		} else {
			gConnPool.PutConnect(conn, false)
		}
	}()

	if err != nil {
		err = errors.NewErrorf("get conn from pool failed:%v, data node host:%s, data partition id:%v", err, host, dp.PartitionID)
		return
	}
	packet := metanode.NewPacketToBatchDeleteExtent(context.Background(), dp, eks)
	if err = packet.WriteToConn(conn, proto.WriteDeadlineTime*10); err != nil {
		err = errors.NewErrorf("write to dataNode %s, %s", packet.GetUniqueLogId(), err.Error())
		return
	}
	if err = packet.ReadFromConn(conn, proto.ReadDeadlineTime*10); err != nil {
		err = errors.NewErrorf("read response from dataNode %s, %s", packet.GetUniqueLogId(), err.Error())
		return
	}
	if packet.ResultCode != proto.OpOk {
		err = errors.NewErrorf("batch delete extent %s response: %s", packet.GetUniqueLogId(), packet.GetResultMsg())
	}
	return
}

//func (t *TinyBlockCheckTask) doCleanGarbage() (err error) {
//	var failedCnt int
//	for dpID, blockInfo := range t.gar {
//		maxNum := blockInfo.MaxNum()
//		extentsID := make([]uint64, 0, maxNum)
//		for index := 0; index <= maxNum; index++ {
//			if blockInfo.Get(index) {
//				extentsID = append(extentsID, uint64(index))
//			}
//		}
//
//		var dpInfo *proto.DataPartitionInfo
//		if dpInfo, err = t.masterClient.AdminAPI().GetDataPartition(t.VolName, dpID); err != nil || len(dpInfo.Hosts) == 0 {
//			log.LogErrorf("[doCleanGarbage] get cluster[%s] volume[%s] dp[%v] host failed:%v",
//				t.Cluster, t.VolName, dpID, err)
//			failedCnt++
//			continue
//		}
//
//		dp := &topology.DataPartition{
//			PartitionID: dpID,
//			Hosts:       dpInfo.Hosts,
//		}
//
//		var eks []*proto.MetaDelExtentKey
//		for _, extentID := range extentsID {
//			ek := &proto.MetaDelExtentKey{
//				ExtentKey: proto.ExtentKey{
//					PartitionId:  dpID,
//					ExtentId:     extentID,
//				},
//			}
//			eks = append(eks, ek)
//		}
//		if len(eks) == 0 {
//			continue
//		}
//
//		startIndex := 0
//		for startIndex < len(eks) {
//			endIndex := startIndex + 1024
//			if endIndex > len(eks) {
//				endIndex = len(eks)
//			}
//			log.LogInfof("[doCleanGarbage] start batch delete extent, host addr[%s], partition id[%v], " +
//				"extents is %v", dpInfo.Hosts[0], dpID, extentsID[startIndex:endIndex])
//			if err = batchDeleteExtent(dpInfo.Hosts[0], dp, eks[startIndex: endIndex]); err != nil {
//				log.LogErrorf("batch delete extent failed:%v", err)
//				failedCnt++
//				break
//			}
//			startIndex = endIndex
//		}
//		log.LogInfof("[doCleanGarbage] batch delete extent finish, partition id:%v", dpID)
//	}
//	log.LogInfof("[doCleanGarbage] totalCount:%v, failedCount:%v", len(t.garbageBlocks), failedCnt)
//	return
//}

func (t *TinyBlockCheckTask) init() (err error) {
	if t.exportDir == "" {
		return
	}

	if err = os.RemoveAll(t.exportDir); err != nil {
		return
	}

	if err = os.MkdirAll(t.exportDir, 0666); err != nil {
		return
	}

	return
}

func (t *TinyBlockCheckTask) isMetaOutVolume() bool {
	if  t.VolName == "jss-online" || t.VolName == "jss-online-ssd" || t.VolName == "ofw-cof-bak" ||
		t.VolName == "ofw-cof" || t.VolName == "ofw-cof-yc" {
		return true
	}
	return false
}

func (t *TinyBlockCheckTask) reCheckTinyGarbageBlocks() (err error) {
	if t.masterClient.IsDbBack {
		return
	}

	var checkGarbageWithError = false
	defer func() {
		if checkGarbageWithError {
			err = fmt.Errorf("check garbage with error")
			return
		}
	}()
	var mps []*proto.MetaPartitionView
	mps, err = t.masterClient.ClientAPI().GetMetaPartitions(t.VolName)
	if err != nil {
		log.LogErrorf("action[getExtentsByMPs] get cluster[%s] volume[%s] meta partitions "+
			"failed: %v", t.Cluster, t.VolName, err)
		return
	}
	errorCh := make(chan error, len(mps))
	defer func() {
		close(errorCh)
	}()
	var resultWaitGroup sync.WaitGroup
	extCh := make(chan *ExtentInfo, 1024)
	resultWaitGroup.Add(1)
	go func() {
		defer resultWaitGroup.Done()
		for {
			e := <-extCh
			if e == nil {
				break
			}
			if !proto.IsTinyExtent(e.ExtentID) {
				continue
			}

			if _, ok := t.tinyGarbageBlocks[fmt.Sprintf("%v_%v", e.DataPartitionID, e.ExtentID)]; !ok {
				continue
			}

			tinyGarbageBlocks := t.tinyGarbageBlocks[fmt.Sprintf("%v_%v", e.DataPartitionID, e.ExtentID)]
			for _, tinyGarbageBlock := range tinyGarbageBlocks {
				if (e.ExtentOffset >= tinyGarbageBlock.ExtentOffset &&
					e.ExtentOffset < tinyGarbageBlock.ExtentOffset + uint64(tinyGarbageBlock.Size)) ||
					(e.ExtentOffset + uint64(e.Size) > tinyGarbageBlock.ExtentOffset &&
						e.ExtentOffset + uint64(e.Size) <=  tinyGarbageBlock.ExtentOffset + uint64(tinyGarbageBlock.Size)) {
					checkGarbageWithError = true
					log.LogCriticalf("mistake check extents: %v", e)
				}

			}

		}
	}()
	mpIDCh := make(chan uint64, 512)
	go func() {
		defer close(mpIDCh)
		for _, mp := range mps {
			mpIDCh <- mp.PartitionID
		}
	}()
	var wg sync.WaitGroup
	mpConcurrency := parallelMpCnt.Load()
	for index := int32(0); index < mpConcurrency; index++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				mpID, ok := <-mpIDCh
				if !ok {
					break
				}
				log.LogInfof("action[getExtentsByMPs] start get %s %s metaPartition(%v) inode", t.Cluster, t.VolName, mpID)
				metaPartitionInfo, errGetMPInfo := t.masterClient.ClientAPI().GetMetaPartition(mpID, t.VolName)
				if errGetMPInfo != nil {
					errorCh <- errGetMPInfo
					log.LogErrorf("action[getExtentsByMPs] get cluster[%s] volume[%s] mp[%v] info failed: %v",
						t.Cluster, t.VolName, mpID, errGetMPInfo)
					continue
				}
				var (
					maxInodeCount uint64 = 0
					leaderAddr           = ""
				)
				for _, replica := range metaPartitionInfo.Replicas {
					if replica.InodeCount >= maxInodeCount {
						maxInodeCount = replica.InodeCount
						leaderAddr = replica.Addr
					}
				}
				startComplete := t.checkNodeStatus(strings.Split(leaderAddr, ":")[0], strconv.FormatInt(int64(t.masterClient.MetaNodeProfPort), 10))
				if !startComplete {
					errorCh <- fmt.Errorf("meta node %s start incomplete", leaderAddr)
					continue
				}
				if errorInfo := t.getTinyExtentsFromMetaPartition(mpID, leaderAddr, extCh); errorInfo != nil {
					errorCh <- errorInfo
					log.LogErrorf("action[getExtentsByMPs] get cluster[%s] volume [%s] extent id list "+
						"from mp[%v] failed: %v", t.Cluster, t.VolName, mpID, errorInfo)
					continue
				}
			}
		}()
	}
	wg.Wait()
	close(extCh)
	resultWaitGroup.Wait()
	select {
	case e := <-errorCh:
		//meta info must be complete
		log.LogErrorf("get extent id list from meta partition failed:%v", e)
		err = errors.NewErrorf("get extent id list from meta partition failed:%v", e)
		return
	default:
	}
	log.LogInfof("meta extent map count:%v", len(t.metaTinyExtentsMap))
	return
}
func (t *TinyBlockCheckTask) checkTinyGarbageBlocks() {
	t.garbageTinyBlocks = make(map[uint64][]*bitset.ByteSliceBitSet, 0)
	for dpid, tinyExtentsArr := range t.dataTinyExtentsMap {
		log.LogInfof("start check dp tiny garage, id: %v", dpid)
		if _, ok := t.metaTinyExtentsMap[dpid]; !ok {
			log.LogInfof("dp not exist in meta tiny extents map, id: %v", dpid)
			t.garbageTinyBlocks[dpid] = tinyExtentsArr
			continue
		}
		t.garbageTinyBlocks[dpid] = make([]*bitset.ByteSliceBitSet, proto.TinyExtentCount)
		for index, tinyExtentBitSet := range tinyExtentsArr {
			t.garbageTinyBlocks[dpid][index] = tinyExtentBitSet.Xor(t.metaTinyExtentsMap[dpid][index]).And(tinyExtentBitSet)
		}
	}
}
//check tiny extent garbage one by one
func (t *TinyBlockCheckTask) checkTinyGarbageBlocksOneByOne() {
	t.tinyGarbageBlocks = make(map[string][]*ExtentInfo)
	var volumeTinyGarbageSize uint64
	for dpID, tinyExtentsHoles := range t.tinyExtentsHolesMap {
		log.LogInfof("start check cluster[%s] volume[%s] partitionID[%v] garbage extents", t.Cluster, t.VolName, dpID)
		if _, ok := t.metaTinyExtentsMap[dpID]; !ok {
			log.LogInfof("cluster[%s] volume[%s] partitionID[%v] not exist in meta tiny extents map, skip check", t.Cluster, t.VolName, dpID)
			continue
		}
		dataPartitionGarbageSize := uint64(0)
		filePath := path.Join(t.exportDir, fmt.Sprintf("%d", dpID))
		fp, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC,  0666)
		if err != nil {
			log.LogErrorf("open file %s failed: %v", filePath, err)
			return
		}
		_, err = fp.WriteString(garbageTinyExtentDumpHeader)
		if err != nil {
			_ = fp.Close()
			return
		}
		for index, holesInfo := range tinyExtentsHoles {
			extentID := index+1
			log.LogInfof("start check cluster[%s] volume[%s] partitionID[%v] extentID[%v] garbage blocks", t.Cluster, t.VolName, dpID, extentID)
			if holesInfo == nil {
				continue
			}
			//bitCount := tinyExtentsBlockCount(holesInfo.ExtentSize)
			bitSet := bitset.NewByteSliceBitSet()
			bitSet.FillAll()
			for _, hole := range holesInfo.Holes {
				start := int(hole.Offset / proto.PageSize)
				if start >= bitSet.Cap() {
					continue
				}
				count := int(hole.Size / proto.PageSize)
				end := start+count
				if start+count >= bitSet.Cap() {
					end = bitSet.Cap()
				}
				for i := start; i < end; i++ {
					bitSet.Clear(i)
				}
			}
			var tinyGarbageBlocksBitSet *bitset.ByteSliceBitSet
			if t.metaTinyExtentsMap[dpID][index] == nil {
				tinyGarbageBlocksBitSet = bitSet
			} else {
				tinyGarbageBlocksBitSet = bitSet.Xor(t.metaTinyExtentsMap[dpID][index]).And(bitSet)
			}
			var extentGarbageSize uint32
			tinyGarbageBlocks := getTinyGarbageBlocks(dpID, uint64(extentID), tinyGarbageBlocksBitSet)
			if extentGarbageSize, err = dumpTinyGarbageBlockInfoToFile(fp, tinyGarbageBlocks); err != nil {
				_ = fp.Close()
				return
			}
			t.tinyGarbageBlocks[fmt.Sprintf("%v_%v", dpID, extentID)] = tinyGarbageBlocks
			log.LogInfof("cluster[%s] volume[%s] partitionID[%v] extentID[%v] garbage size: %v", t.Cluster, t.VolName, dpID, extentID, extentGarbageSize)
			dataPartitionGarbageSize += uint64(extentGarbageSize)
		}
		volumeTinyGarbageSize += dataPartitionGarbageSize
		_ = fp.Close()
	}
}

func getTinyGarbageBlocks(dpID, extentID uint64, garbageBitSet *bitset.ByteSliceBitSet) (garbageBlocks []*ExtentInfo) {
	garbageTinyBlockIndex := make([]int, 0)
	for index := 0; index < garbageBitSet.Cap(); index++ {
		if garbageBitSet.Get(index) {
			garbageTinyBlockIndex = append(garbageTinyBlockIndex, index)
		}
	}
	if len(garbageTinyBlockIndex) == 0 {
		return
	}
	garbageBlocks = make([]*ExtentInfo, 0)
	preIndex := garbageTinyBlockIndex[0]
	startIndex := garbageTinyBlockIndex[0]
	var extentOffset uint64
	var size uint32
	extentOffset = uint64(startIndex*proto.PageSize)
	size = proto.PageSize
	for i := 1; i < len(garbageTinyBlockIndex); i++ {
		if garbageTinyBlockIndex[i] == preIndex+1 {
			size += proto.PageSize
		} else {
			garbageBlocks = append(garbageBlocks, &ExtentInfo{
				DataPartitionID: dpID,
				ExtentID:        extentID,
				ExtentOffset:    extentOffset,
				Size:            size,
			})
			extentOffset = uint64(garbageTinyBlockIndex[i]*proto.PageSize)
			size = proto.PageSize
		}
		preIndex = garbageTinyBlockIndex[i]
	}
	garbageBlocks = append(garbageBlocks, &ExtentInfo{
		DataPartitionID: dpID,
		ExtentID:        extentID,
		ExtentOffset:    extentOffset,
		Size:            size,
	})
	return
}

func (t *TinyBlockCheckTask) getTinyExtentsByMPs() (err error) {
	if t.masterClient.IsDbBack {
		return
	}
	t.metaTinyExtentsMap = make(map[uint64][]*bitset.ByteSliceBitSet, 0)
	var mps []*proto.MetaPartitionView
	mps, err = t.masterClient.ClientAPI().GetMetaPartitions(t.VolName)
	if err != nil {
		log.LogErrorf("action[getExtentsByMPs] get cluster[%s] volume[%s] meta partitions "+
			"failed: %v", t.Cluster, t.VolName, err)
		return
	}
	errorCh := make(chan error, len(mps))
	defer func() {
		close(errorCh)
	}()
	var resultWaitGroup sync.WaitGroup
	extCh := make(chan *ExtentInfo, 1024)
	resultWaitGroup.Add(1)
	go func() {
		defer resultWaitGroup.Done()
		for {
			e := <-extCh
			if e == nil {
				break
			}
			if !proto.IsTinyExtent(e.ExtentID) {
				continue
			}
			if _, ok := t.metaTinyExtentsMap[e.DataPartitionID]; !ok {
				log.LogInfof("init cluster[%v] volume[%v] partitionID(%v) tiny extents list", t.Cluster, t.VolName, e.DataPartitionID)
				t.metaTinyExtentsMap[e.DataPartitionID] = make([]*bitset.ByteSliceBitSet, proto.TinyExtentCount)
			}
			if t.metaTinyExtentsMap[e.DataPartitionID][e.ExtentID-1] == nil {
				log.LogInfof("init cluster[%v] volume[%v] partitionID(%v) extentID(%v) tiny extents bitmap",
					t.Cluster, t.VolName, e.DataPartitionID, e.ExtentID)
				t.metaTinyExtentsMap[e.DataPartitionID][e.ExtentID-1] = bitset.NewByteSliceBitSet()
			}
			extentEnd := e.ExtentOffset + uint64(e.Size)
			if extentEnd > DefMaxCheckSize {
				extentEnd = DefMaxCheckSize
			}
			if extentEnd%proto.PageSize != 0 {
				extentEnd += proto.PageSize - (extentEnd%proto.PageSize)
			}
			extentStart := e.ExtentOffset
			if e.ExtentOffset%proto.PageSize != 0 {
				extentStart -= e.ExtentOffset%proto.PageSize
			}
			for offset := extentStart; offset < extentEnd; offset += proto.PageSize {
				index := offset/proto.PageSize
				t.metaTinyExtentsMap[e.DataPartitionID][e.ExtentID-1].Set(int(index))
			}
		}
	}()
	mpIDCh := make(chan uint64, 512)
	go func() {
		defer close(mpIDCh)
		for _, mp := range mps {
			mpIDCh <- mp.PartitionID
		}
	}()
	var wg sync.WaitGroup
	mpConcurrency := parallelMpCnt.Load()
	for index := int32(0); index < mpConcurrency; index++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				mpID, ok := <-mpIDCh
				if !ok {
					break
				}
				log.LogInfof("action[getExtentsByMPs] start get %s %s metaPartition(%v) inode", t.Cluster, t.VolName, mpID)
				metaPartitionInfo, errGetMPInfo := t.masterClient.ClientAPI().GetMetaPartition(mpID, t.VolName)
				if errGetMPInfo != nil {
					errorCh <- errGetMPInfo
					log.LogErrorf("action[getExtentsByMPs] get cluster[%s] volume[%s] mp[%v] info failed: %v",
						t.Cluster, t.VolName, mpID, errGetMPInfo)
					continue
				}
				var (
					maxInodeCount uint64 = 0
					leaderAddr           = ""
				)
				for _, replica := range metaPartitionInfo.Replicas {
					if replica.InodeCount >= maxInodeCount {
						maxInodeCount = replica.InodeCount
						leaderAddr = replica.Addr
					}
				}
				startComplete := t.checkNodeStatus(strings.Split(leaderAddr, ":")[0], strconv.FormatInt(int64(t.masterClient.MetaNodeProfPort), 10))
				if !startComplete {
					errorCh <- fmt.Errorf("meta node %s start incomplete", leaderAddr)
					continue
				}
				if errorInfo := t.getTinyExtentsFromMetaPartition(mpID, leaderAddr, extCh); errorInfo != nil {
					errorCh <- errorInfo
					log.LogErrorf("action[getExtentsByMPs] get cluster[%s] volume [%s] extent id list "+
						"from mp[%v] failed: %v", t.Cluster, t.VolName, mpID, errorInfo)
					continue
				}
			}
		}()
	}
	wg.Wait()
	close(extCh)
	resultWaitGroup.Wait()
	select {
	case e := <-errorCh:
		//meta info must be complete
		log.LogErrorf("get extent id list from meta partition failed:%v", e)
		err = errors.NewErrorf("get extent id list from meta partition failed:%v", e)
		return
	default:
	}
	log.LogInfof("meta extent map count:%v", len(t.metaTinyExtentsMap))
	return
}

func tinyExtentsBlockCount(size uint64) (count int) {
	if size%proto.PageSize == 0 {
		count = int(size / proto.PageSize)
	} else {
		count = int(size/proto.PageSize + 1)
	}
	return
}

//为保证数据安全性，保留最后16M数据不做检查(16M为可调参数)
//避免内存问题，分批检查dp（按照tiny extent检查大小为1T计算，每个分片64个tiny extent使用的bitmap占用2G内存）
func (t *TinyBlockCheckTask) getTinyExtentsByDPs(dps []*proto.DataPartitionResponse) (err error) {
	var (
		wg             sync.WaitGroup
		extentsMapLock sync.Mutex
		dpViewCh       = make(chan *proto.DataPartitionResponse, 10)
	)
	t.tinyExtentsHolesMap = make(map[uint64][]*ExtentHolesInfo, len(dps))
	go func() {
		defer close(dpViewCh)
		for _, dp := range dps {
			dpViewCh <- dp
		}
	}()
	for index := 0; index < 10; index++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				dp, ok := <-dpViewCh
				if !ok {
					break
				}
				if dp == nil {
					continue
				}

				if len(dp.Hosts) == 0 {
					continue
				}

				if startComplete := t.checkNodeStatus(strings.Split(dp.Hosts[0], ":")[0], strconv.Itoa(int(t.masterClient.DataNodeProfPort))); !startComplete {
					log.LogErrorf("data node %s start incomplete", dp.Hosts[0])
					continue
				}
				extentsMapLock.Lock()
				if _, has := t.tinyExtentsHolesMap[dp.PartitionID]; !has {
					t.tinyExtentsHolesMap[dp.PartitionID] = make([]*ExtentHolesInfo, proto.TinyExtentCount)
				}
				extentsMapLock.Unlock()

				var tinyExtentsInfo []storage.ExtentInfoBlock
				tinyExtentsInfo, err = t.getTinyExtentsByDataPartition(dp.PartitionID, dp.Hosts[0])
				if err != nil {
					log.LogErrorf("action[getExtentsByDPs] get cluster[%s] volume[%s] extent id list "+
						"from dp[%v:%s] failed: %v",
						t.Cluster, t.VolName, dp.PartitionID, dp.Hosts[0], err)
					continue
				}
				if len(tinyExtentsInfo) == 0 {
					continue
				}

				for _, tinyExtentInfo := range tinyExtentsInfo {
					extentID := tinyExtentInfo[storage.FileID]
					if extentID > proto.TinyExtentCount {
						continue
					}
					extentSize := tinyExtentInfo[storage.Size]
					if extentSize > unit.TB {
						log.LogInfof("cluster[%s] volume[%s] partitionID[%v]  tinyExtent[%v] size[%v] more than 1TB", t.Cluster, t.VolName, dp.PartitionID, extentID, extentSize)
						extentSize = unit.TB
					}
					checkSize := extentSize - DefReservedSize
					//get first host extent holes
					if len(dp.Hosts) == 0 {
						log.LogInfof("cluster[%s] volume[%s] partitionID[%v] with zero hosts", t.Cluster, t.VolName, dp.PartitionID)
						continue
					}
					host := dp.Hosts[0]
					var extentHolesInfo *ExtentHolesInfo
					if extentHolesInfo, err = t.getExtentHoles(dp.PartitionID, extentID, host); err != nil {
						log.LogErrorf("get cluster[%s] volume[%s] extent[%v] holes from"+
							"dp[%v:%s] failed: %v", t.Cluster, t.VolName, extentID, dp.PartitionID, host, err)
						break
					}
					extentHolesInfo.ExtentSize = checkSize
					extentsMapLock.Lock()
					t.tinyExtentsHolesMap[dp.PartitionID][extentID-1] = extentHolesInfo
					extentsMapLock.Unlock()
				}
			}
		}()
	}
	wg.Wait()
	return
}
func (t *TinyBlockCheckTask) doCheckTinyExtentsGarbage() (err error) {
	defer func() {
		if r := recover(); r != nil {
			log.LogCriticalf("recover panic: %s", string(debug.Stack()))
			return
		}
	}()
	//timer := time.NewTimer(time.Duration(t.safeCleanInterval) * time.Second)
	var dpsView *proto.DataPartitionsView
	dpsView, err = t.masterClient.ClientAPI().GetDataPartitions(t.VolName, nil)
	if err != nil {
		log.LogErrorf("action[doCheckGarbageInBatches] get cluster[%s] volume[%s] data partition failed: %v",
			t.Cluster, t.VolName, err)
		return
	}
	if err = t.getTinyExtentsByDPs(dpsView.DataPartitions); err != nil {
		log.LogErrorf("[doCheckGarbage] get cluster[%s] volume[%s] extents from dp failed:%v",
			t.Cluster, t.VolName, err)
		return
	}
	log.LogInfof("get cluster[%s] volume[%s] extents from dp finished", t.Cluster, t.VolName)
	//select {
	//case <-timer.C:
	//}
	log.LogInfof("start get cluster[%s] volume[%s] meta extents info from mp", t.Cluster, t.VolName)
	if err = t.getTinyExtentsByMPs(); err != nil {
		log.LogErrorf("[doCheckGarbage] get cluster[%s] volume[%s] extents from mp failed:%v",
			t.Cluster, t.VolName, err)
		return
	}
	log.LogInfof("start check cluster[%s] volume[%s] tiny extents garbage", t.Cluster, t.VolName)
	t.checkTinyGarbageBlocksOneByOne()

	if err = t.reCheckTinyGarbageBlocks(); err != nil {
		log.LogErrorf("[doCheckGarbage] recheck %s %s tiny garbage block failed:%v",
			t.Cluster, t.VolName, err)
		return
	}
	//todo:clean
	return
}
var garbageTinyExtentDumpFormat = "    %-15v    %-15v    %-15v    %-15v\n"
var garbageTinyExtentDumpHeader = fmt.Sprintf(garbageTinyExtentDumpFormat, "DataPartitionID", "ExtentID", "ExtentOffset", "Size")
func dumpTinyGarbageBlockInfoToFile(fp *os.File, tinyGarbageBlocks []*ExtentInfo) (garbageSize uint32, err error) {
	for _, extent := range tinyGarbageBlocks {
		garbageSize += extent.Size
		log.LogInfof("tiny garbage block: %v_%v_%v_%v", extent.DataPartitionID, extent.ExtentID, extent.ExtentOffset, extent.Size)
		_, err = fp.WriteString(fmt.Sprintf(garbageTinyExtentDumpFormat, extent.DataPartitionID,
			extent.ExtentID, extent.ExtentOffset, extent.Size))
		if err != nil {
			return
		}
	}
	return
}
func (t *TinyBlockCheckTask) dumpTinyGarbageBlock() {
	if t.exportDir == "" {
		return
	}
	volumeTinyGarbageSize := uint64(0)
	for dpID, garbageTinyExtents := range t.garbageTinyBlocks {
		dataPartitionGarbageSize := uint64(0)
		filePath := path.Join(t.exportDir, fmt.Sprintf("%d", dpID))
		fp, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
		if err != nil {
			log.LogErrorf("open file %s failed: %v", filePath, err)
			return
		}
		_, err = fp.WriteString(garbageTinyExtentDumpHeader)
		if err != nil {
			log.LogErrorf("write file %s failed: %v", filePath, err)
			_ = fp.Close()
			return
		}
		log.LogInfof(" %s %s data partition %v tiny garbage blocks result dump to %s", t.Cluster, t.VolName, dpID, filePath)
		for i, garbageBlocks := range garbageTinyExtents {
			tinyGarbageBlocks := getTinyGarbageBlocks(dpID, uint64(i+1), garbageBlocks)
			var extentGarbageSize uint32
			if extentGarbageSize, err = dumpTinyGarbageBlockInfoToFile(fp, tinyGarbageBlocks); err != nil {
				_ = fp.Close()
				return
			}
			log.LogInfof("%s %s data partition %v extent %v garbage size: %v", t.Cluster, t.VolName, dpID, i+1, extentGarbageSize)
			dataPartitionGarbageSize += uint64(extentGarbageSize)
		}
		volumeTinyGarbageSize += dataPartitionGarbageSize
		_ = fp.Close()
	}
	log.LogInfof("%s %s total garbage size: %v", t.Cluster, t.VolName, volumeTinyGarbageSize)
	return
}
func (t *TinyBlockCheckTask) doCleanTinyGarbage() (err error) {
	return
}

func (t *TinyBlockCheckTask) parseDataPartitionEmptyExtents(fileName string, parseMetaDir bool) (dpID uint64, emptyExtentsMap map[uint64]byte, err error) {
	dpID, err = strconv.ParseUint(fileName, 10, 64)
	if err != nil {
		return
	}
	var filePath string
	if parseMetaDir{
		filePath = path.Join(t.metaExportDir, fileName)
	} else {
		filePath = path.Join(t.exportDir, fileName)
	}
	fp, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		return
	}
	defer fp.Close()
	data, err := ioutil.ReadAll(fp)
	if err != nil || len(data) == 0 {
		return
	}
	emptyExtentsMap = make(map[uint64]byte, 0)
	extentsIDStr := strings.Split(string(data), "\n")
	for _, extentIDStr := range extentsIDStr {
		var extentID uint64
		extentID, err = strconv.ParseUint(extentIDStr, 10, 64)
		if err != nil {
			continue
		}
		emptyExtentsMap[extentID] = 0
	}
	return
}
func (t *TinyBlockCheckTask) getTinyExtentsSizeByMPs() (err error) {
	if t.masterClient.IsDbBack {
		return
	}
	t.metaTinyExtentsSizeMap = make(map[uint64][]uint64, 0)
	var mps []*proto.MetaPartitionView
	mps, err = t.masterClient.ClientAPI().GetMetaPartitions(t.VolName)
	if err != nil {
		log.LogErrorf("action[getExtentsByMPs] get cluster[%s] volume[%s] meta partitions "+
			"failed: %v", t.Cluster, t.VolName, err)
		return
	}
	errorCh := make(chan error, len(mps))
	defer func() {
		close(errorCh)
	}()
	var resultWaitGroup sync.WaitGroup
	extCh := make(chan *ExtentInfo, 1024)
	resultWaitGroup.Add(1)
	go func() {
		defer resultWaitGroup.Done()
		for {
			e := <-extCh
			if e == nil {
				break
			}
			if !proto.IsTinyExtent(e.ExtentID) {
				continue
			}
			if e.ExtentID > 64 {
				continue
			}
			if _, ok := t.metaTinyExtentsSizeMap[e.DataPartitionID]; !ok {
				t.metaTinyExtentsSizeMap[e.DataPartitionID] = make([]uint64, proto.TinyExtentCount)
			}
			t.metaTinyExtentsSizeMap[e.DataPartitionID][e.ExtentID-1] += uint64(e.Size)
		}
	}()
	mpIDCh := make(chan uint64, 512)
	go func() {
		defer close(mpIDCh)
		for _, mp := range mps {
			mpIDCh <- mp.PartitionID
		}
	}()
	var wg sync.WaitGroup
	mpConcurrency := parallelMpCnt.Load()
	for index := int32(0); index < mpConcurrency; index++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				mpID, ok := <-mpIDCh
				if !ok {
					break
				}
				log.LogInfof("action[getExtentsByMPs] start get %s %s metaPartition(%v) inode", t.Cluster, t.VolName, mpID)
				metaPartitionInfo, errGetMPInfo := t.masterClient.ClientAPI().GetMetaPartition(mpID, t.VolName)
				if errGetMPInfo != nil {
					errorCh <- errGetMPInfo
					log.LogErrorf("action[getExtentsByMPs] get cluster[%s] volume[%s] mp[%v] info failed: %v",
						t.Cluster, t.VolName, mpID, errGetMPInfo)
					continue
				}
				var (
					maxInodeCount uint64 = 0
					leaderAddr           = ""
				)
				for _, replica := range metaPartitionInfo.Replicas {
					if replica.InodeCount >= maxInodeCount {
						maxInodeCount = replica.InodeCount
						leaderAddr = replica.Addr
					}
				}
				if errorInfo := t.getTinyExtentsFromMetaPartition(mpID, leaderAddr, extCh); errorInfo != nil {
					errorCh <- errorInfo
					log.LogErrorf("action[getExtentsByMPs] get cluster[%s] volume [%s] extent id list "+
						"from mp[%v] failed: %v", t.Cluster, t.VolName, mpID, errorInfo)
					continue
				}
			}
		}()
	}
	wg.Wait()
	close(extCh)
	resultWaitGroup.Wait()
	select {
	case e := <-errorCh:
		//meta info must be complete
		log.LogErrorf("get extent id list from meta partition failed:%v", e)
		err = errors.NewErrorf("get extent id list from meta partition failed:%v", e)
		return
	default:
	}
	for dpID, extentsSizeArr := range t.metaTinyExtentsSizeMap {
		dpTinyExtentsTotalSize := uint64(0)
		for index, size := range extentsSizeArr {
			dpTinyExtentsTotalSize += size
			log.LogCriticalf("MetaNode dpID: %v, extentID: %v, size: %v", dpID, index+1, size)
		}
		log.LogCriticalf("MetaNode dpID: %v, tinyTotalSize: %v", dpID, dpTinyExtentsTotalSize)
	}
	for dpID, dataTinyExtentsTotalSize := range t.dataTinyExtentsSizeMap {
		extentsSizeArr, ok := t.metaTinyExtentsSizeMap[dpID]
		if ok {
			dpTinyExtentsTotalSize := uint64(0)
			for index, size := range extentsSizeArr {
				dpTinyExtentsTotalSize += size
				log.LogCriticalf("MetaNode dpID: %v, extentID: %v, size: %v", dpID, index+1, size)
			}
			log.LogCriticalf("%s %s %v check result, metaSize: %v, dataSize: %v", t.Cluster, t.VolName, dpID, dpTinyExtentsTotalSize, dataTinyExtentsTotalSize)
		}  else {
			log.LogCriticalf("%s %s %v check result, metaSize: 0, dataSize: %v", t.Cluster, t.VolName, dpID, dataTinyExtentsTotalSize)
		}
	}
	return
}
func (t *TinyBlockCheckTask) getTinyExtentsSizeByDPs(dps []*proto.DataPartitionResponse) (err error) {
	var (
		wg             sync.WaitGroup
		mutex          sync.Mutex
		dpViewCh       = make(chan *proto.DataPartitionResponse, 10)
	)
	t.dataTinyExtentsSizeMap = make(map[uint64]uint64, 0)
	go func() {
		defer close(dpViewCh)
		for _, dp := range dps {
			dpViewCh <- dp
		}
	}()
	for index := 0; index < 10; index++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				dp, ok := <-dpViewCh
				if !ok {
					break
				}
				if dp == nil {
					continue
				}
				var dpTinyExtentTotalSize = uint64(0)
				for _, host := range dp.Hosts {
					totalSize := uint64(0)
					var tinyExtentsInfo []storage.ExtentInfoBlock
					tinyExtentsInfo, err = t.getTinyExtentsByDataPartition(dp.PartitionID, host)
					if err != nil {
						log.LogErrorf("get cluster[%s] volume[%s] extent id list "+
							"from dp[%v:%s] failed: %v",
							t.Cluster, t.VolName, dp.PartitionID, host, err)
						continue
					}
					if len(tinyExtentsInfo) == 0 {
						continue
					}
					for _, tinyExtentInfo := range tinyExtentsInfo {
						extentID := tinyExtentInfo[storage.FileID]
						if extentID > 64 {
							continue
						}
						var holesInfo *ExtentHolesInfo
						if holesInfo, err = t.getExtentHoles(dp.PartitionID, extentID, host); err != nil {
							log.LogErrorf("get cluster[%s] volume[%s] extent[%v] holes from"+
								"dp[%v:%s] failed: %v", t.Cluster, t.VolName, extentID, dp.PartitionID, host, err)
							continue
						}
						totalSize += holesInfo.ExtentAvaliSize
						log.LogCriticalf("DataNode dpID: %v, extentID: %v, host: %s, extentSize: %v", dp.PartitionID, extentID, host, holesInfo.ExtentAvaliSize)
					}
					if totalSize > dpTinyExtentTotalSize {
						dpTinyExtentTotalSize = totalSize
					}
				}
				mutex.Lock()
				t.dataTinyExtentsSizeMap[dp.PartitionID] = dpTinyExtentTotalSize
				mutex.Unlock()
			}
		}()
	}
	wg.Wait()
	return
}

func (t *TinyBlockCheckTask) doCheckTinyExtentsSize() (err error) {
	var dpsView *proto.DataPartitionsView
	dpsView, err = t.masterClient.ClientAPI().GetDataPartitions(t.VolName, nil)
	if err != nil {
		log.LogErrorf("get cluster[%s] volume[%s] data partition failed: %v",
			t.Cluster, t.VolName, err)
		return
	}
	if err = t.getTinyExtentsSizeByDPs(dpsView.DataPartitions); err != nil {
		log.LogErrorf("get cluster[%s] volume[%s] extents from dp failed:%v",
			t.Cluster, t.VolName, err)
		return
	}
	if err = t.getTinyExtentsSizeByMPs(); err != nil {
		log.LogErrorf("get cluster[%s] volume[%s] extents from mp failed:%v",
			t.Cluster, t.VolName, err)
		return
	}
	return
}

func (t *TinyBlockCheckTask) RunTinyExtentsCheck() {
	if t.masterClient.IsDbBack {
		log.LogInfof("BlockCheckTask RunTinyExtentsCheck not support dbBak cluster")
		return
	}
	log.LogInfof("BlockCheckTask RunTinyExtentsCheck %s %s start check", t.Cluster, t.VolName)
	if err := t.init(); err != nil {
		log.LogErrorf("BlockCheckTask RunTinyExtentsCheck, %s %s init bock check task failed: %v", t.Cluster, t.VolName, err)
		return
	}
	err := t.doCheckTinyExtentsGarbage()
	if err != nil {
		log.LogErrorf("BlockCheckTask RunTinyExtentsCheck, %s %s check garbage block failed: %v", t.Cluster, t.VolName, err)
		return
	}
	//if t.needDumpResult && t.exportDir != "" {
	//	t.dumpTinyGarbageBlock()
	//}
	if !t.needClean {
		return
	}
	//if err = t.doCleanTinyGarbage(); err != nil {
	//	log.LogErrorf("BlockCheckTask RunTinyExtentsCheck, %s %s clean garbage block failed: %v", t.Cluster, t.VolName, err)
	//	return
	//}
	log.LogInfof("BlockCheckTask RunTinyExtentsCheck %s %s check finish", t.Cluster, t.VolName)
}

func (t *TinyBlockCheckTask) RunTinyExtentsSizeCheck() {
	if t.masterClient.IsDbBack {
		log.LogInfof("BlockCheckTask RunTinyExtentsSizeCheck not support dbBak cluster")
		return
	}
	log.LogInfof("BlockCheckTask RunTinyExtentsSizeCheck %s %s start check", t.Cluster, t.VolName)
	if err := t.init(); err != nil {
		log.LogErrorf("BlockCheckTask RunTinyExtentsSizeCheck, %s %s init bock check task failed: %v", t.Cluster, t.VolName, err)
		return
	}
	err := t.doCheckTinyExtentsSize()
	if err != nil {
		log.LogErrorf("BlockCheckTask RunTinyExtentsSizeCheck, %s %s check garbage block failed: %v", t.Cluster, t.VolName, err)
		return
	}
	log.LogInfof("BlockCheckTask RunTinyExtentsSizeCheck %s %s check finish", t.Cluster, t.VolName)
}
//
//func markDeleteExtentDBBack(host string, dp *topology.DataPartition, ek *proto.MetaDelExtentKey) (err error) {
//	return
//	var conn *net.TCPConn
//	conn, err = gConnPool.GetConnect(host)
//	defer func() {
//		if err != nil {
//			gConnPool.PutConnect(conn, true)
//		} else {
//			gConnPool.PutConnect(conn, false)
//		}
//	}()
//	if err != nil {
//		err = errors.NewErrorf("get conn from pool failed:%v, data node host:%s, data partition id:%v", err, host, dp.PartitionID)
//		return
//	}
//	packet := metanode.NewExtentDeletePacketDBBack(dp, ek)
//	if err = packet.WriteToConn(conn); err != nil {
//		err = errors.NewErrorf("write to dataNode %s, %s", packet.GetUniqueLogId(), err.Error())
//		return
//	}
//	if err = packet.ReadFromConn(conn, proto.ReadDeadlineTime); err != nil {
//		err = errors.NewErrorf("read response from dataNode %s, %s", packet.GetUniqueLogId(), err.Error())
//		return
//	}
//	if packet.ResultCode != proto.OpOk {
//		err = errors.NewErrorf("batch delete extent %s response: %s", packet.GetUniqueLogId(), packet.GetResultMesg())
//	}
//	return
//}