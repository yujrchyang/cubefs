package datanode

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/datanode/mock"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/storage"
	"github.com/cubefs/cubefs/util/statistics"
	"golang.org/x/net/context"
	"hash/crc32"
	"math/rand"
	"os"
	"path"
	"reflect"
	"sync"
	"testing"
	"time"
)

var fakeNode *fakeDataNode

const (
	mockDataTcpPort1 = 17017
	mockDataID1      = 1
	mockDataTcpPort2 = 17018
	mockDataID2      = 2
	mockDataTcpPort3 = 17019
	mockDataID3      = 3
	pageSize         = 4096
)

func init() {
	rand.Seed(time.Now().UnixNano())
	getLocalIp()
	fmt.Println("init datanode")
	mock.NewMockMaster()
	if err := FakeDirCreate(); err != nil {
		panic(err.Error())
	}
	fakeNode = newFakeDataNode()
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func TestUnmarshalRandWriteRaftLog(t *testing.T) {
	data := RandStringRunes(100 * 1024)
	item := GetRandomWriteOpItem()
	item.data = ([]byte)(data)
	item.opcode = proto.OpRandomWrite
	item.extentID = 2
	item.offset = 100
	item.size = 100 * 1024
	item.crc = crc32.ChecksumIEEE(item.data)
	oldMarshalResult, err := MarshalRandWriteRaftLog(item.opcode, item.extentID, item.offset, item.size, item.data, item.crc)
	if err != nil {
		t.Logf("MarshalRandWriteRaftLog Item(%v) failed (%v)", item, err)
		t.FailNow()
	}

	newOpItem, err := UnmarshalRandWriteRaftLog(oldMarshalResult, true)
	if err != nil {
		t.Logf("UnMarshalRandWriteRaftLog Item(%v) failed (%v)", item, err)
		t.FailNow()
	}
	if item.opcode != newOpItem.opcode || item.extentID != newOpItem.extentID || item.offset != newOpItem.offset ||
		item.size != newOpItem.size || item.crc != newOpItem.crc || bytes.Compare(([]byte)(data), newOpItem.data) != 0 {
		t.Logf("UnMarshalRandWriteRaftLog Item(%v) newItem(%v) failed ", item, err)
		t.FailNow()
	}
}

func TestUnmarshalRandWriteRaftLogV3(t *testing.T) {
	data := RandStringRunes(100 * 1024)
	item := GetRandomWriteOpItem()
	item.data = make([]byte, len(data)+proto.RandomWriteRaftLogV3HeaderSize)
	copy(item.data[proto.RandomWriteRaftLogV3HeaderSize:], data)
	item.opcode = proto.OpRandomWrite
	item.extentID = 2
	item.offset = 100
	item.size = 100 * 1024
	item.crc = crc32.ChecksumIEEE(item.data[proto.RandomWriteRaftLogV3HeaderSize:])

	oldMarshalResult, err := MarshalRandWriteRaftLogV3(item.opcode, item.extentID, item.offset, item.size, item.data, item.crc)
	if err != nil {
		t.Logf("MarshalRandWriteRaftLog Item(%v) failed (%v)", item, err)
		t.FailNow()
	}

	newOpItem, err := UnmarshalRandWriteRaftLog(oldMarshalResult, true)
	if err != nil {
		t.Logf("UnMarshalRandWriteRaftLog Item(%v) failed (%v)", item, err)
		t.FailNow()
	}
	if item.opcode != newOpItem.opcode || item.extentID != newOpItem.extentID || item.offset != newOpItem.offset ||
		item.size != newOpItem.size || item.crc != newOpItem.crc || bytes.Compare(([]byte)(data), newOpItem.data) != 0 {
		t.Logf("UnMarshalRandWriteRaftLog Item(%v) newItem(%v) failed ", item, err)
		t.FailNow()
	}
}

func TestBinaryUnmarshalRandWriteRaftLogV3(t *testing.T) {
	data := RandStringRunes(100 * 1024)
	item := GetRandomWriteOpItem()
	item.data = make([]byte, len(data)+proto.RandomWriteRaftLogV3HeaderSize)
	copy(item.data[proto.RandomWriteRaftLogV3HeaderSize:], data)
	item.opcode = proto.OpRandomWrite
	item.extentID = 2
	item.offset = 100
	item.size = 100 * 1024
	item.crc = crc32.ChecksumIEEE(item.data[proto.RandomWriteRaftLogV3HeaderSize:])

	oldMarshalResult, err := MarshalRandWriteRaftLogV3(item.opcode, item.extentID, item.offset, item.size, item.data, item.crc)
	if err != nil {
		t.Logf("MarshalRandWriteRaftLog Item(%v) failed (%v)", item, err)
		t.FailNow()
	}

	newOpItem, err := BinaryUnmarshalRandWriteRaftLogV3(oldMarshalResult)
	if err != nil {
		t.Logf("BinaryUnmarshalRandWriteRaftLogV3 Item(%v) failed (%v)", item, err)
		t.FailNow()
	}
	if item.opcode != newOpItem.opcode || item.extentID != newOpItem.extentID || item.offset != newOpItem.offset ||
		item.size != newOpItem.size || item.crc != newOpItem.crc || bytes.Compare(([]byte)(data), newOpItem.data) != 0 {
		t.Logf("BinaryUnmarshalRandWriteRaftLogV3 Item(%v) newItem(%v) failed ", item, err)
		t.FailNow()
	}
}

func TestUnmarshalOldVersionRaftLog(t *testing.T) {
	data := RandStringRunes(100 * 1024)
	item := GetRandomWriteOpItem()
	item.data = make([]byte, len(data))
	copy(item.data[:], data)
	item.opcode = proto.OpRandomWrite
	item.extentID = 2
	item.offset = 100
	item.size = 100 * 1024
	item.crc = crc32.ChecksumIEEE(item.data[proto.RandomWriteRaftLogV3HeaderSize:])
	v, err := MarshalOldVersionRandWriteOpItemForTest(item)
	if err != nil {
		t.Logf("MarshalOldVersionRandWriteOpItemForTest Item(%v) failed (%v)", item, err)
		t.FailNow()
	}
	raftOpItem := new(RaftCmdItem)
	raftOpItem.V = v
	raftOpItem.Op = uint32(item.opcode)
	raftOpItemBytes, err := json.Marshal(raftOpItem)
	if err != nil {
		t.Logf("json Marshal raftOpItem(%v) failed (%v)", raftOpItem, err)
		t.FailNow()
	}
	newOpItem, err := UnmarshalOldVersionRaftLog(raftOpItemBytes)
	if err != nil {
		t.Logf("UnmarshalOldVersionRaftLog Item(%v) failed (%v)", item, err)
		t.FailNow()
	}

	if item.opcode != newOpItem.opcode || item.extentID != newOpItem.extentID || item.offset != newOpItem.offset ||
		item.size != newOpItem.size || item.crc != newOpItem.crc || bytes.Compare(([]byte)(data), newOpItem.data) != 0 {
		t.Logf("UnmarshalOldVersionRaftLog Item(%v) newItem(%v) failed ", item, err)
		t.FailNow()
	}
}

func MarshalOldVersionRandWriteOpItemForTest(item *rndWrtOpItem) (result []byte, err error) {
	buff := bytes.NewBuffer(make([]byte, 0))
	if err = binary.Write(buff, binary.BigEndian, item.extentID); err != nil {
		return
	}
	if err = binary.Write(buff, binary.BigEndian, item.offset); err != nil {
		return
	}
	if err = binary.Write(buff, binary.BigEndian, item.size); err != nil {
		return
	}
	if err = binary.Write(buff, binary.BigEndian, item.crc); err != nil {
		return
	}
	if _, err = buff.Write(item.data); err != nil {
		return
	}
	result = buff.Bytes()
	return
}

func TestValidateCRC(t *testing.T) {
	dataNodeAddr1 := "192.168.0.31"
	dataNodeAddr2 := "192.168.0.32"
	dataNodeAddr3 := "192.168.0.33"

	dataNode1Extents := make([]storage.ExtentInfoBlock, 0)
	dataNode1Extents = append(dataNode1Extents, newExtentInfoForTest(1, 11028, 1776334861))
	dataNode1Extents = append(dataNode1Extents, newExtentInfoForTest(1028, 11028, 1776334861))
	dataNode2Extents := make([]storage.ExtentInfoBlock, 0)
	dataNode2Extents = append(dataNode2Extents, newExtentInfoForTest(1, 11028, 1776334861))
	dataNode2Extents = append(dataNode2Extents, newExtentInfoForTest(1028, 11028, 1776334861))
	dataNode3Extents := make([]storage.ExtentInfoBlock, 0)
	dataNode3Extents = append(dataNode3Extents, newExtentInfoForTest(1, 11028, 1776334861))
	dataNode3Extents = append(dataNode3Extents, newExtentInfoForTest(1028, 11028, 1776334861))

	validateCRCTasks := make([]*DataPartitionValidateCRCTask, 0, 3)
	validateCRCTasks = append(validateCRCTasks, NewDataPartitionValidateCRCTask(dataNode1Extents, dataNodeAddr1, dataNodeAddr1))
	validateCRCTasks = append(validateCRCTasks, NewDataPartitionValidateCRCTask(dataNode2Extents, dataNodeAddr2, dataNodeAddr1))
	validateCRCTasks = append(validateCRCTasks, NewDataPartitionValidateCRCTask(dataNode3Extents, dataNodeAddr3, dataNodeAddr1))

	dp := &DataPartition{
		partitionID: 1,
	}
	dp.validateCRC(validateCRCTasks)
}

func TestCheckNormalExtentFile_differentCrc(t *testing.T) {
	dp := &DataPartition{
		partitionID: 1,
	}
	extentInfos := make([]storage.ExtentInfoBlock, 0, 3)
	extentInfos = append(extentInfos, newExtentInfoForTest(1028, 11028, 1776334861))
	extentInfos = append(extentInfos, newExtentInfoForTest(1028, 11028, 1776334862))
	extentInfos = append(extentInfos, newExtentInfoForTest(1028, 11028, 1776334863))
	extentReplicaSource := make(map[int]string, 3)
	extentReplicaSource[0] = "192.168.0.31"
	extentReplicaSource[1] = "192.168.0.32"
	extentReplicaSource[2] = "192.168.0.33"
	crcLocAddrMap := make(map[uint64][]string)
	for i, extentInfo := range extentInfos {
		crcLocAddrMap[extentInfo[storage.Crc]] = append(crcLocAddrMap[extentInfo[storage.Crc]], extentReplicaSource[i])
	}
	extentCrcInfo, crcNotEqual := dp.checkNormalExtentFile(extentInfos, extentReplicaSource)
	if crcNotEqual != true {
		t.Errorf("action[TestCheckNormalExtentFile_differentCrc] failed, result[%v] expect[%v]", crcNotEqual, true)
	}
	if reflect.DeepEqual(crcLocAddrMap, extentCrcInfo.CrcLocAddrMap) == false {
		t.Errorf("action[TestCheckNormalExtentFile_differentCrc] failed, result[%v] expect[%v]", extentCrcInfo.CrcLocAddrMap, crcLocAddrMap)
	}
}

func TestCheckNormalExtentFile_SameCrc(t *testing.T) {
	dp := &DataPartition{
		partitionID: 1,
	}
	extentInfos := make([]storage.ExtentInfoBlock, 0, 3)
	extentInfos = append(extentInfos, newExtentInfoForTest(1028, 11028, 1776334865))
	extentInfos = append(extentInfos, newExtentInfoForTest(1028, 11028, 1776334865))
	extentInfos = append(extentInfos, newExtentInfoForTest(1028, 11028, 1776334865))
	extentReplicaSource := make(map[int]string, 3)
	extentReplicaSource[0] = "192.168.0.31"
	extentReplicaSource[1] = "192.168.0.32"
	extentReplicaSource[2] = "192.168.0.33"
	_, crcNotEqual := dp.checkNormalExtentFile(extentInfos, extentReplicaSource)
	if crcNotEqual != false {
		t.Errorf("action[TestCheckNormalExtentFile_SameCrc] failed, result[%v] expect[%v]", crcNotEqual, false)
	}
}

func TestCheckNormalExtentFile_OnlyOneReplica(t *testing.T) {
	dp := &DataPartition{
		partitionID: 1,
	}
	extentInfos := make([]storage.ExtentInfoBlock, 0, 3)
	extentInfos = append(extentInfos, newExtentInfoForTest(1028, 11028, 1776334865))
	extentReplicaSource := make(map[int]string, 3)
	extentReplicaSource[0] = "192.168.0.31"
	_, crcNotEqual := dp.checkNormalExtentFile(extentInfos, extentReplicaSource)
	if crcNotEqual != false {
		t.Errorf("action[TestCheckNormalExtentFile_OnlyOneReplica] failed, result[%v] expect[%v]", crcNotEqual, false)
	}
}

func TestCheckTinyExtentFile_DifferentCrc(t *testing.T) {
	dp := &DataPartition{
		partitionID: 1,
	}
	extentInfos := make([]storage.ExtentInfoBlock, 0, 3)
	extentInfos = append(extentInfos, newExtentInfoForTest(30, 30000, 1776334861))
	extentInfos = append(extentInfos, newExtentInfoForTest(30, 30000, 1776334862))
	extentInfos = append(extentInfos, newExtentInfoForTest(30, 30000, 1776334863))
	extentReplicaSource := make(map[int]string, 3)
	extentReplicaSource[0] = "192.168.0.31"
	extentReplicaSource[1] = "192.168.0.32"
	extentReplicaSource[2] = "192.168.0.33"
	_, crcNotEqual := dp.checkTinyExtentFile(extentInfos, extentReplicaSource)
	if crcNotEqual != true {
		t.Errorf("action[TestCheckTinyExtentFile_DifferentCrc] failed, result[%v] expect[%v]", crcNotEqual, true)
	}
}

func TestCheckTinyExtentFile_SameCrc(t *testing.T) {
	dp := &DataPartition{
		partitionID: 1,
	}
	extentInfos := make([]storage.ExtentInfoBlock, 0, 3)
	extentInfos = append(extentInfos, newExtentInfoForTest(30, 30000, 1776334865))
	extentInfos = append(extentInfos, newExtentInfoForTest(30, 30000, 1776334865))
	extentInfos = append(extentInfos, newExtentInfoForTest(30, 30000, 1776334865))
	extentReplicaSource := make(map[int]string, 3)
	extentReplicaSource[0] = "192.168.0.31"
	extentReplicaSource[1] = "192.168.0.32"
	extentReplicaSource[2] = "192.168.0.33"
	_, crcNotEqual := dp.checkTinyExtentFile(extentInfos, extentReplicaSource)
	if crcNotEqual != false {
		t.Errorf("action[TestCheckTinyExtentFile_SameCrc] failed, result[%v] expect[%v]", crcNotEqual, false)
	}
}

func TestCheckTinyExtentFile_OnlyOneReplica(t *testing.T) {
	dp := &DataPartition{
		partitionID: 1,
	}
	extentInfos := make([]storage.ExtentInfoBlock, 0, 3)
	extentInfos = append(extentInfos, newExtentInfoForTest(30, 30000, 1776334865))
	extentReplicaSource := make(map[int]string, 3)
	extentReplicaSource[0] = "192.168.0.31"
	_, crcNotEqual := dp.checkTinyExtentFile(extentInfos, extentReplicaSource)
	if crcNotEqual != false {
		t.Errorf("action[TestCheckTinyExtentFile_OnlyOneReplica] failed, result[%v] expect[%v]", crcNotEqual, false)
	}
}

func TestCheckTinyExtentFile_DiffSize(t *testing.T) {
	dp := &DataPartition{
		partitionID: 1,
	}
	extentInfos := make([]storage.ExtentInfoBlock, 0, 3)
	extentInfos = append(extentInfos, newExtentInfoForTest(30, 11301, 1776334861))
	extentInfos = append(extentInfos, newExtentInfoForTest(30, 11302, 1776334862))
	extentInfos = append(extentInfos, newExtentInfoForTest(30, 11303, 1776334863))
	extentReplicaSource := make(map[int]string, 3)
	extentReplicaSource[0] = "192.168.0.31"
	extentReplicaSource[1] = "192.168.0.32"
	extentReplicaSource[2] = "192.168.0.33"
	_, crcNotEqual := dp.checkTinyExtentFile(extentInfos, extentReplicaSource)
	if crcNotEqual != false {
		t.Errorf("action[TestCheckTinyExtentFile_DiffSize] failed, result[%v] expect[%v]", crcNotEqual, false)
	}
}

func newExtentInfoForTest(fileID, size, crc uint64) storage.ExtentInfoBlock {
	return storage.ExtentInfoBlock{
		storage.FileID: fileID,
		storage.Size:   size,
		storage.Crc:    crc,
	}
}

func TestIsGetConnectError(t *testing.T) {
	err := fmt.Errorf("rand str %v op", errorGetConnectMsg)
	connectError := isGetConnectError(err)
	if !connectError {
		t.Errorf("action[TestIsGetConnectError] failed, isGetConnectError expect[%v] actual[%v]", true, connectError)
	}
	err = fmt.Errorf("rand str abc op")
	connectError = isGetConnectError(err)
	if connectError {
		t.Errorf("action[TestIsGetConnectError] failed, isGetConnectError expect[%v] actual[%v]", false, connectError)
	}
}

func TestIsConnectionRefusedFailure(t *testing.T) {
	err := fmt.Errorf("rand str %v op", errorConnRefusedMsg)
	connectRefusedFailure := isConnectionRefusedFailure(err)
	if !connectRefusedFailure {
		t.Errorf("action[TestIsConnectionRefusedFailure] failed, isConnectionRefusedFailure expect[%v] actual[%v]", true, connectRefusedFailure)
	}
	err = fmt.Errorf("rand str abc op")
	connectRefusedFailure = isConnectionRefusedFailure(err)
	if connectRefusedFailure {
		t.Errorf("action[TestIsConnectionRefusedFailure] failed, isGetConnectError expect[%v] actual[%v]", false, connectRefusedFailure)
	}
}

func TestIsIOTimeoutFailure(t *testing.T) {
	err := fmt.Errorf("rand str %v op", errorIOTimeoutMsg)
	ioTimeoutFailure := isIOTimeoutFailure(err)
	if !ioTimeoutFailure {
		t.Errorf("action[TestIsIOTimeoutFailure] failed, isIOTimeoutFailure expect[%v] actual[%v]", true, ioTimeoutFailure)
	}
	err = fmt.Errorf("rand str abc op")
	ioTimeoutFailure = isIOTimeoutFailure(err)
	if ioTimeoutFailure {
		t.Errorf("action[TestIsIOTimeoutFailure] failed, isIOTimeoutFailure expect[%v] actual[%v]", false, ioTimeoutFailure)
	}
}

func TestGetRemoteExtentInfoForValidateCRCWithRetry(t *testing.T) {
	var (
		ctx         = context.Background()
		tcp         = mock.NewMockTcp(mockDataTcpPort1)
		err         error
		extentFiles []storage.ExtentInfoBlock
	)
	err = tcp.Start()
	if err != nil {
		t.Fatalf("start mock tcp server failed: %v", err)
	}
	defer tcp.Stop()
	dp := &DataPartition{
		partitionID: 10,
	}
	mock.ReplyGetRemoteExtentInfoForValidateCRCCount = GetRemoteExtentInfoForValidateCRCRetryTimes
	targetHost := fmt.Sprintf(":%v", mockDataTcpPort1)
	if extentFiles, err = dp.getRemoteExtentInfoForValidateCRCWithRetry(ctx, targetHost); err == nil {
		t.Error("action[getRemoteExtentInfoForValidateCRCWithRetry] err should not be nil")
	}
	if extentFiles, err = dp.getRemoteExtentInfoForValidateCRCWithRetry(ctx, targetHost); err != nil {
		t.Errorf("action[getRemoteExtentInfoForValidateCRCWithRetry] err:%v", err)
	}
	if uint64(len(extentFiles)) != mock.LocalCreateExtentId {
		t.Errorf("action[getRemoteExtentInfoForValidateCRCWithRetry] extents length expect[%v] actual[%v]", mock.LocalCreateExtentId, len(extentFiles))
	}
}

func TestValidateCrc(t *testing.T) {
	var (
		err         error
		dp          *DataPartition
		count       uint64 = 100
		testBaseDir        = path.Join(os.TempDir(), t.Name())
		extents     []storage.ExtentInfoBlock
	)
	_ = os.RemoveAll(testBaseDir)
	dp = createDataPartition(1, count, testBaseDir, t)
	defer func() {
		_ = os.RemoveAll(testBaseDir)
	}()
	storage.ValidateCrcInterval = 5
	time.Sleep(time.Second*time.Duration(storage.ValidateCrcInterval) + 1)
	defer func() {
		storage.ValidateCrcInterval = int64(20 * storage.RepairInterval)
	}()
	testCases := []struct {
		name  string
		class int
	}{
		{name: "getLocalExtentInfoForValidateCRC", class: 0},
		{name: "runValidateCRC", class: 1},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			switch testCase.class {
			case 0:
				if extents, err = dp.getLocalExtentInfoForValidateCRC(); err != nil {
					t.Errorf("action[getLocalExtentInfoForValidateCRC] err:%v", err)
				}
				if uint64(len(extents)) != count {
					t.Errorf("action[getLocalExtentInfoForValidateCRC] extents length expect[%v] actual[%v]", count, len(extents))
				}
				var dp2 *DataPartition
				if dp2, err = initDataPartition(testBaseDir, 2, false); err != nil {
					t.Errorf("init data partition err:%v", err)
				}
				if extents, err = dp2.getLocalExtentInfoForValidateCRC(); err == nil || err.Error() != "partition is loadding" {
					t.Errorf("action[getLocalExtentInfoForValidateCRC], err should not be equal to nil or err[%v]", err)
				}
			case 1:
				tcp := mock.NewMockTcp(mockDataTcpPort1)
				err = tcp.Start()
				if err != nil {
					t.Fatalf("start mock tcp server failed: %v", err)
				}
				defer tcp.Stop()
				mock.ReplyGetRemoteExtentInfoForValidateCRCCount = 0
				dp.runValidateCRC(context.Background())
			}
		})
	}
}

func TestGetLocalExtentInfo(t *testing.T) {
	var (
		err         error
		dp          *DataPartition
		count       uint64 = 100
		testBaseDir        = path.Join(os.TempDir(), t.Name())
	)
	_ = os.RemoveAll(testBaseDir)
	dp = createDataPartition(1, count, testBaseDir, t)
	defer func() {
		_ = os.RemoveAll(testBaseDir)
	}()

	tinyExtents := []uint64{1, 2, 3, 10}
	var extents []storage.ExtentInfoBlock
	var leaderTinyDeleteRecordFileSize int64
	if extents, _, err = dp.getLocalExtentInfo(proto.TinyExtentType, tinyExtents); err != nil {
		t.Fatalf("get local extent info, extentType:%v err:%v", proto.TinyExtentType, err)
	}
	if len(extents) != len(tinyExtents) {
		t.Fatalf("tiny extent count expect:%v, actual:%v", len(tinyExtents), len(extents))
	}
	if leaderTinyDeleteRecordFileSize, err = dp.extentStore.LoadTinyDeleteFileOffset(); err != nil {
		t.Fatalf("get leaderTinyDeleteRecordFileSize, extentType:%v err:%v", proto.TinyExtentType, err)
	}
	if leaderTinyDeleteRecordFileSize != int64(24) {
		t.Fatalf("leaderTinyDeleteRecordFileSize expect:%v, actual:%v", 24, leaderTinyDeleteRecordFileSize)
	}
	if extents, _, err = dp.getLocalExtentInfo(proto.NormalExtentType, tinyExtents); err != nil {
		t.Fatalf("get local extent info, extentType:%v err:%v", proto.TinyExtentType, err)
	}
	if len(extents) != int(count) {
		t.Fatalf("tiny extent count expect:%v, actual:%v", count, len(extents))
	}

	if dp, err = initDataPartition(testBaseDir, 2, false); err != nil {
		t.Fatalf("init data partition err:%v", err)
	}
	if _, _, err = dp.getLocalExtentInfo(proto.NormalExtentType, tinyExtents); err == nil {
		t.Fatalf("get local extent info, extentType:%v err equal nil", proto.TinyExtentType)
	}
}

func TestGetRemoteExtentInfo(t *testing.T) {
	ctx := context.Background()
	tcp := mock.NewMockTcp(mockDataTcpPort1)
	err := tcp.Start()
	if err != nil {
		t.Fatalf("start mock tcp server failed: %v", err)
	}
	defer tcp.Stop()
	// use v3
	mock.SupportedGetAllWatermarksVersion = 3
	dp := &DataPartition{
		partitionID: 10,
	}
	tinyExtents := []uint64{1, 2, 3, 10}
	targetHost := fmt.Sprintf(":%v", mockDataTcpPort1)
	extentFiles, _, err := dp.getRemoteExtentInfo(ctx, proto.TinyExtentType, tinyExtents, targetHost)
	if err != nil {
		t.Fatalf("get remote extent info by v3 type:%v failed:%v", proto.TinyExtentType, err)
	}
	if len(extentFiles) != len(tinyExtents) {
		t.Fatalf("get remote extent info by v3 type:%v tiny extent count expect:%v actual:%v", proto.TinyExtentType, len(tinyExtents), len(extentFiles))
	}
	extentFiles, _, err = dp.getRemoteExtentInfo(ctx, proto.NormalExtentType, tinyExtents, targetHost)
	if err != nil {
		t.Fatalf("get remote extent info by v3 type:%v failed:%v", proto.NormalExtentType, err)
	}
	if len(extentFiles) != mock.RemoteNormalExtentCount {
		t.Fatalf("get remote extent info by v3 type:%v normal extent count expect:%v actual:%v", proto.NormalExtentType, mock.RemoteNormalExtentCount, len(extentFiles))
	}

	// use v2
	mock.SupportedGetAllWatermarksVersion = 2
	extentFiles, _, err = dp.getRemoteExtentInfo(ctx, proto.TinyExtentType, tinyExtents, targetHost)
	if err != nil {
		t.Fatalf("get remote extent info by v2 type:%v failed:%v", proto.TinyExtentType, err)
	}
	if len(extentFiles) != len(tinyExtents) {
		t.Fatalf("get remote extent info by v2 type:%v tiny extent count expect:%v actual:%v", proto.TinyExtentType, len(tinyExtents), len(extentFiles))
	}
	extentFiles, _, err = dp.getRemoteExtentInfo(ctx, proto.NormalExtentType, tinyExtents, targetHost)
	if err != nil {
		t.Fatalf("get remote extent info by v2 type:%v failed:%v", proto.NormalExtentType, err)
	}
	if len(extentFiles) != mock.RemoteNormalExtentCount {
		t.Fatalf("get remote extent info by v2 type:%v normal extent count expect:%v actual:%v", proto.NormalExtentType, mock.RemoteNormalExtentCount, len(extentFiles))
	}
	// use v1
	mock.SupportedGetAllWatermarksVersion = 1
	extentFiles, _, err = dp.getRemoteExtentInfo(ctx, proto.TinyExtentType, tinyExtents, targetHost)
	if err != nil {
		t.Fatalf("get remote extent info by v1 type:%v failed:%v", proto.TinyExtentType, err)
	}
	if len(extentFiles) != len(tinyExtents) {
		t.Fatalf("get remote extent info by v1 type:%v tiny extent count expect:%v actual:%v", proto.TinyExtentType, len(tinyExtents), len(extentFiles))
	}
	extentFiles, _, err = dp.getRemoteExtentInfo(ctx, proto.NormalExtentType, tinyExtents, targetHost)
	if err != nil {
		t.Fatalf("get remote extent info by v1 type:%v failed:%v", proto.NormalExtentType, err)
	}
	if len(extentFiles) != mock.RemoteNormalExtentCount {
		t.Fatalf("get remote extent info by v1 type:%v normal extent count expect:%v actual:%v", proto.NormalExtentType, mock.RemoteNormalExtentCount, len(extentFiles))
	}
}

func TestBuildDataPartitionRepairTask_TinyExtent(t *testing.T) {
	var (
		err                  error
		dp                   *DataPartition
		localExtentFileCount uint64 = 100
		testBaseDir                 = path.Join(os.TempDir(), t.Name())
		ctx                         = context.Background()
	)
	tcp := mock.NewMockTcp(mockDataTcpPort1)
	err = tcp.Start()
	if err != nil {
		t.Fatalf("start mock tcp server failed: %v", err)
	}
	defer tcp.Stop()
	_ = os.RemoveAll(testBaseDir)
	dp = createDataPartition(1, localExtentFileCount, testBaseDir, t)
	defer func() {
		_ = os.RemoveAll(testBaseDir)
	}()
	var repairTasks []*DataPartitionRepairTask
	tinyExtents := []uint64{1, 2, 3, 10}
	if repairTasks, err = dp.buildDataPartitionRepairTask(ctx, dp.replicas, proto.TinyExtentType, tinyExtents); err != nil {
		t.Fatalf("build data partition repair task err:%v", err)
	}
	if len(repairTasks[0].extents) != len(tinyExtents) {
		t.Fatalf("repairTasks[0] tiny extent count expect:%v, actual:%v", len(tinyExtents), len(repairTasks[0].extents))
	}
	if repairTasks[0].LeaderTinyDeleteRecordFileSize != int64(24) {
		t.Fatalf("repairTasks[0] leaderTinyDeleteRecordFileSize expect:%v, actual:%v", 24, repairTasks[0].LeaderTinyDeleteRecordFileSize)
	}
	if len(repairTasks[1].extents) != len(tinyExtents) {
		t.Fatalf("repairTasks[1] tiny extent info type:%v tiny extent count expect:%v actual:%v", proto.TinyExtentType, len(tinyExtents), len(repairTasks[1].extents))
	}
	_, brokenTinyExtents := dp.prepareRepairTasks(repairTasks)
	results := [][3]uint64{
		{uint64(len(tinyExtents)), 0, uint64(len(tinyExtents))},
		{0, 0, 0},
		{0, 0, 0},
	}
	for i, task := range repairTasks {
		if results[i][0] != uint64(len(task.ExtentsToBeRepairedSource)) || results[i][1] != uint64(len(task.ExtentsToBeCreated)) || results[i][2] != uint64(len(task.ExtentsToBeRepaired)) {
			t.Fatalf("repairTasks[%v] result not match, expect:%v actual:%v", i, results[i],
				[3]int{len(task.ExtentsToBeRepairedSource), len(task.ExtentsToBeCreated), len(task.ExtentsToBeRepaired)})
		}
	}
	if len(brokenTinyExtents) != len(tinyExtents) {
		t.Fatalf("brokenTinyExtents count expect:%v, actual:%v", len(tinyExtents), len(brokenTinyExtents))
	}
	if len(repairTasks[0].ExtentsToBeRepairedSource) != len(tinyExtents) {
		t.Fatalf("repairTasks[0] ExtentsToBeRepairedSource count expect:%v, actual:%v", len(tinyExtents), len(repairTasks[0].ExtentsToBeRepairedSource))
	}
	if len(repairTasks[0].ExtentsToBeCreated) != 0 {
		t.Fatalf("repairTasks[0] v count expect:%v, actual:%v", 0, len(repairTasks[0].ExtentsToBeCreated))
	}
	if len(repairTasks[0].ExtentsToBeRepaired) != len(tinyExtents) {
		t.Fatalf("repairTasks[0] ExtentsToBeRepaired count expect:%v, actual:%v", len(tinyExtents), len(repairTasks[0].ExtentsToBeRepaired))
	}
	dp.DoRepairOnLeaderDisk(ctx, repairTasks[0])
	for _, extentId := range tinyExtents {
		if !dp.ExtentStore().IsExists(extentId) {
			t.Fatalf("tiny extent(%v) should exist", extentId)
		}
		size, _ := dp.ExtentStore().LoadExtentWaterMark(extentId)
		expectSize := extentId * 1024
		if expectSize%pageSize != 0 {
			expectSize = expectSize + (pageSize - expectSize%pageSize)
		}
		if size != int64(expectSize) {
			t.Fatalf("repaired tiny extent(%v) size expect:%v actual:%v", extentId, expectSize, size)
		}
	}
	if err = dp.notifyFollowersToRepair(ctx, repairTasks); err != nil {
		t.Fatalf("notify extent repair should not have err, but there are err:%v", err)
	}
}

func TestBuildDataPartitionRepairTask_NormalExtent(t *testing.T) {
	var (
		err                  error
		dp                   *DataPartition
		localExtentFileCount uint64 = 100
		testBaseDir                 = path.Join(os.TempDir(), t.Name())
		ctx                         = context.Background()
	)
	tcp := mock.NewMockTcp(mockDataTcpPort1)
	err = tcp.Start()
	if err != nil {
		t.Fatalf("start mock tcp server failed: %v", err)
	}
	defer tcp.Stop()
	_ = os.RemoveAll(testBaseDir)
	dp = createDataPartition(1, localExtentFileCount, testBaseDir, t)
	defer func() {
		_ = os.RemoveAll(testBaseDir)
	}()
	var repairTasks []*DataPartitionRepairTask
	var tinyExtents []uint64
	if repairTasks, err = dp.buildDataPartitionRepairTask(ctx, dp.replicas, proto.NormalExtentType, tinyExtents); err != nil {
		t.Fatalf("build data partition repair task err:%v", err)
	}
	if len(repairTasks[0].extents) != int(localExtentFileCount) {
		t.Fatalf("repairTasks[0] noraml extent count expect:%v, actual:%v", localExtentFileCount, len(repairTasks[0].extents))
	}
	if repairTasks[0].LeaderTinyDeleteRecordFileSize != int64(24) {
		t.Fatalf("repairTasks[0] leaderTinyDeleteRecordFileSize expect:%v, actual:%v", 24, repairTasks[0].LeaderTinyDeleteRecordFileSize)
	}
	_, _ = dp.prepareRepairTasks(repairTasks)
	remoteRepairCount := localExtentFileCount - (mock.RemoteNormalExtentCount - 1)
	results := [][3]uint64{
		{mock.RemoteNormalExtentCount, 1, mock.RemoteNormalExtentCount},
		{remoteRepairCount, remoteRepairCount, remoteRepairCount},
		{remoteRepairCount, remoteRepairCount, remoteRepairCount},
	}
	for i, task := range repairTasks {
		if results[i][0] != uint64(len(task.ExtentsToBeRepairedSource)) || results[i][1] != uint64(len(task.ExtentsToBeCreated)) || results[i][2] != uint64(len(task.ExtentsToBeRepaired)) {
			t.Fatalf("repairTasks[%v] result not match, expect:%v actual:%v", i, results[i],
				[3]int{len(task.ExtentsToBeRepairedSource), len(task.ExtentsToBeCreated), len(task.ExtentsToBeRepaired)})
		}
	}
	dp.DoRepairOnLeaderDisk(ctx, repairTasks[0])
	if !dp.ExtentStore().IsExists(mock.LocalCreateExtentId) {
		t.Fatalf("normal extent(%v) should exist", mock.LocalCreateExtentId)
	}
	// 比较创建并修复后extent水位
	size, _ := dp.ExtentStore().LoadExtentWaterMark(mock.LocalCreateExtentId)
	if size != int64(mock.LocalCreateExtentId*1024) {
		t.Fatalf("created normal extent(%v) size expect:%v actual:%v", mock.LocalCreateExtentId, mock.LocalCreateExtentId*1024, size)
	}
	// 比较修复后extent水位
	for i := 0; i < mock.RemoteNormalExtentCount-1; i++ {
		extentId := uint64(i + 1 + proto.TinyExtentCount)
		size, _ = dp.ExtentStore().LoadExtentWaterMark(extentId)
		if size != int64(extentId*1024) {
			t.Fatalf("repaired normal extent(%v) size expect:%v actual:%v", extentId, extentId*1024, size)
		}
	}
	if err = dp.notifyFollowersToRepair(ctx, repairTasks); err != nil {
		t.Fatalf("notify extent repair should not have err, but there are err:%v", err)
	}
}

func TestRepair(t *testing.T) {
	var (
		err                  error
		dp                   *DataPartition
		localExtentFileCount uint64 = 100
		testBaseDir                 = path.Join(os.TempDir(), t.Name())
		ctx                         = context.Background()
	)
	tcp := mock.NewMockTcp(mockDataTcpPort1)
	err = tcp.Start()
	if err != nil {
		t.Fatalf("start mock tcp server failed: %v", err)
	}
	defer tcp.Stop()
	_ = os.RemoveAll(testBaseDir)
	dp = createDataPartition(1, localExtentFileCount, testBaseDir, t)
	defer func() {
		_ = os.RemoveAll(testBaseDir)
	}()
	var brokenTinyExtents []uint64
	for i := 1; i <= proto.TinyExtentCount; i++ {
		brokenTinyExtents = append(brokenTinyExtents, uint64(i))
	}
	dp.repair(ctx, proto.TinyExtentType)
	for _, extentId := range brokenTinyExtents {
		if !dp.ExtentStore().IsExists(extentId) {
			t.Fatalf("tiny extent(%v) should exist", extentId)
		}
		size, _ := dp.ExtentStore().LoadExtentWaterMark(extentId)
		expectSize := extentId * 1024
		if expectSize%pageSize != 0 {
			expectSize = expectSize + (pageSize - expectSize%pageSize)
		}
		if size != int64(expectSize) {
			t.Fatalf("repaired tiny extent(%v) size expect:%v actual:%v", extentId, expectSize, size)
		}
	}
	dp.repair(ctx, proto.NormalExtentType)
	if !dp.ExtentStore().IsExists(mock.LocalCreateExtentId) {
		t.Fatalf("normal extent(%v) should exist", mock.LocalCreateExtentId)
	}
	// 比较创建并修复后extent水位
	size, _ := dp.ExtentStore().LoadExtentWaterMark(mock.LocalCreateExtentId)
	if size != int64(mock.LocalCreateExtentId*1024) {
		t.Fatalf("created normal extent(%v) size expect:%v actual:%v", mock.LocalCreateExtentId, mock.LocalCreateExtentId*1024, size)
	}
	// 比较修复后extent水位
	for i := 0; i < mock.RemoteNormalExtentCount-1; i++ {
		extentId := uint64(i + 1 + proto.TinyExtentCount)
		size, _ = dp.ExtentStore().LoadExtentWaterMark(extentId)
		if size != int64(extentId*1024) {
			t.Fatalf("repaired normal extent(%v) size expect:%v actual:%v", extentId, extentId*1024, size)
		}
	}
}

func TestDoStreamExtentFixRepairOnFollowerDisk(t *testing.T) {
	var (
		err                  error
		dp                   *DataPartition
		localExtentFileCount uint64 = 100
		testBaseDir                 = path.Join(os.TempDir(), t.Name())
		ctx                         = context.Background()
	)
	tcp := mock.NewMockTcp(mockDataTcpPort1)
	err = tcp.Start()
	if err != nil {
		t.Fatalf("start mock tcp server failed: %v", err)
	}
	defer tcp.Stop()
	_ = os.RemoveAll(testBaseDir)
	dp = createDataPartition(1, localExtentFileCount, testBaseDir, t)
	defer func() {
		_ = os.RemoveAll(testBaseDir)
	}()
	var repairTasks []*DataPartitionRepairTask
	var tinyExtents []uint64
	if repairTasks, err = dp.buildDataPartitionRepairTask(ctx, dp.replicas, proto.NormalExtentType, tinyExtents); err != nil {
		t.Fatalf("build data partition repair task err:%v", err)
	}
	_, _ = dp.prepareRepairTasks(repairTasks)
	wg := new(sync.WaitGroup)
	for _, extentInfo := range repairTasks[0].ExtentsToBeRepaired {
		wg.Add(1)
		source := repairTasks[0].ExtentsToBeRepairedSource[extentInfo[storage.FileID]]
		go func(info storage.ExtentInfoBlock) {
			defer wg.Done()
			dp.doStreamExtentFixRepairOnFollowerDisk(ctx, info, []string{source})
		}(extentInfo)
	}
	wg.Wait()
	// 比较修复后extent水位
	for i := 0; i < mock.RemoteNormalExtentCount-1; i++ {
		extentId := uint64(i + 1 + proto.TinyExtentCount)
		size, _ := dp.ExtentStore().LoadExtentWaterMark(extentId)
		if size != int64(extentId*1024) {
			t.Fatalf("repaired normal extent(%v) size expect:%v actual:%v", extentId, extentId*1024, size)
		}
	}
}

func createDataPartition(partitionId, normalExtentCount uint64, baseDir string, t *testing.T) (dp *DataPartition) {
	var (
		ctx = context.Background()
		err error
	)
	if err = os.MkdirAll(baseDir, os.ModePerm); err != nil {
		t.Fatalf("prepare test base dir:%v err: %v", baseDir, err)
	}

	if dp, err = initDataPartition(baseDir, partitionId, true); err != nil {
		t.Fatalf("init data partition err:%v", err)
	}
	// create normal extent
	var (
		data                      = []byte{1, 2, 3, 4, 5, 6}
		size                      = int64(len(data))
		crc                       = crc32.ChecksumIEEE(data)
		extentID           uint64 = proto.TinyExtentCount + 1
		deleteTinyExtentId uint64 = 1
	)
	for ; extentID <= proto.TinyExtentCount+normalExtentCount; extentID++ {
		if err = dp.extentStore.Create(extentID, 0, true); err != nil {
			t.Fatalf("extent store create normal extent err:%v", err)
		}
		if err = dp.extentStore.Write(ctx, extentID, 0, size, data, crc, storage.AppendWriteType, false); err != nil {
			t.Fatalf("extent store write normal extent err:%v", err)
		}
	}
	if uint64(dp.GetExtentCount()) != proto.TinyExtentCount+normalExtentCount {
		t.Fatalf("get extent count expect:%v, actual:%v", proto.TinyExtentCount+normalExtentCount, dp.GetExtentCount())
	}
	// waiting time for modification is greater than 10s
	time.Sleep(time.Second * 11)
	dp.extentStore.RecordTinyDelete(deleteTinyExtentId, 1, 2)
	return
}

func initDataPartition(rootDir string, partitionID uint64, isCreatePartition bool) (partition *DataPartition, err error) {
	var (
		partitionSize = 128849018880
	)
	dataPath := path.Join(rootDir, fmt.Sprintf(DataPartitionPrefix+"_%v_%v", partitionID, partitionSize))
	host := fmt.Sprintf(":%v", mockDataTcpPort1)
	partition = &DataPartition{
		volumeID:                "test-vol",
		clusterID:               "test-cluster",
		partitionID:             partitionID,
		path:                    dataPath,
		partitionSize:           partitionSize,
		replicas:                []string{host, host, host},
		repairPropC:             make(chan struct{}, 1),
		updateVolInfoPropC:      make(chan struct{}, 1),
		stopC:                   make(chan bool, 0),
		stopRaftC:               make(chan uint64, 0),
		snapshot:                make([]*proto.File, 0),
		partitionStatus:         proto.ReadWrite,
		DataPartitionCreateType: 0,
		monitorData:             statistics.InitMonitorData(statistics.ModelDataNode),
		persistSync:             make(chan struct{}, 1),
		inRepairExtents:         make(map[uint64]struct{}),
		applyStatus:             NewWALApplyStatus(),
		config: &dataPartitionCfg{
			VolHAType: proto.DefaultCrossRegionHAType,
		},
	}
	d := new(Disk)
	partition.disk = d
	partition.extentStore, err = storage.NewExtentStore(partition.path, partitionID, partitionSize, CacheCapacityPerPartition, nil, isCreatePartition, storage.IOInterceptors{})
	return
}

