package migration

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	util_sdk "github.com/cubefs/cubefs/cli/cmd/util/sdk"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data"
	"github.com/cubefs/cubefs/sdk/http_client"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/util/log"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
	"hash/crc32"
	"io"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

const (
	clusterName   = "chubaofs01"
	ltptestVolume = "ltptest"
	ltptestMaster = "192.168.0.11:17010,192.168.0.12:17010,192.168.0.13:17010"
	size10M       = 10 * 1024 * 1024
	size128M      = 128 * 1024 * 1024
	size512M      = 512 * 1024 * 1024
)

var (
	vol  *VolumeInfo
	mpOp *MigrateTask
)

func init() {
	vol = &VolumeInfo{}
	vol.Name = ltptestVolume
	nodes := strings.Split(ltptestMaster, ",")
	mcc := &ControlConfig{}
	err := vol.Init(clusterName, ltptestVolume, nodes, mcc, data.Normal)
	if err != nil {
		panic(fmt.Sprintf("vol.Init, err:%v", err))
	}
	mpOp = &MigrateTask{
		vol: vol,
		mc:  master.NewMasterClient(strings.Split(ltptestMaster, ","), false),
	}
}

func getInodeATimePolicies(cluster, volName string) (layerPolicies []interface{}, exist bool) {
	exist = true
	lp := &proto.LayerPolicyInodeATime{
		ClusterId:  clusterName,
		VolumeName: ltptestVolume,
		OriginRule: "inodeAccessTime:sec:15:hdd",
		TimeType:   3,
		// TimeType = 1, inode should be migrated if inode access time is earlier then this value
		// TimeType = 2, inode should be migrated if the days since inode be accessed is more then this value
		TimeValue:    15,
		TargetMedium: 2,
	}
	layerPolicies = append(layerPolicies, lp)
	return
}

var clusterInfo = &ClusterInfo{
	Name:         clusterName,
	MasterClient: master.NewMasterClient(strings.Split(ltptestMaster, ","), false),
}

func TestSetInodeMigDirection(t *testing.T) {
	vol.GetLayerPolicies = getInodeATimePolicies
	mpOperation := &MigrateTask{
		vol:  vol,
		mpId: 1,
	}
	inodeOperation := &MigrateInode{
		mpOp: mpOperation,
		inodeInfo: &proto.InodeInfo{
			AccessTime: time.Now().Add(time.Second * -100),
			ModifyTime: time.Now().Add(time.Second * -100),
		},
	}
	var (
		migDir MigrateDirection
		err    error
	)
	if migDir, err = inodeOperation.mpOp.getInodeMigDirection(inodeOperation.inodeInfo); err != nil {
		assert.FailNow(t, err.Error())
		return
	}
	assert.Equal(t, SSDTOHDDFILEMIGRATE, migDir)
}

func TestGetSSSDEkSegment(t *testing.T) {
	inodeOperation := new(MigrateInode)
	inodeOperation.migDirection = SSDTOHDDFILEMIGRATE
	inodeOperation.vol = &VolumeInfo{
		GetDpMediumType: func(cluster, volName string, dpId uint64) (mediumType string) {
			if dpId == 0 {
				return proto.MediumSSDName
			}
			if dpId == 6 || dpId == 7 {
				return proto.MediumHDDName
			}
			if dpId < 1000 {
				if dpId%5 == 0 {
					return proto.MediumHDDName
				} else {
					return proto.MediumSSDName
				}
			} else {
				if dpId%3 == 0 {
					return proto.MediumHDDName
				} else {
					return proto.MediumSSDName
				}
			}
		},
	}

	expectSsdEkCnt := 0
	var (
		fileOffset uint64 = 0
		size       uint32 = 2
	)
	for i := 0; i < 10000; i++ {
		var curFileOffset uint64
		if i%3 == 0 {
			curFileOffset = fileOffset + 1
		} else {
			curFileOffset = fileOffset
		}
		fileOffset = curFileOffset + uint64(size)
		//fmt.Printf("%v %v:%v\n", i, curFileOffset, size)
		inodeOperation.extents = append(inodeOperation.extents, proto.ExtentKey{
			PartitionId: uint64(i),
			ExtentId:    100,
			FileOffset:  curFileOffset,
			Size:        size,
		})
		mediumType := inodeOperation.vol.GetDpMediumType("", "", uint64(i))
		if mediumType == proto.MediumSSDName {
			expectSsdEkCnt++
		}
	}
	actualSsdEkCnt := 0
	inodeOperation.lastMigEkIndex = 0
	for inodeOperation.lastMigEkIndex < len(inodeOperation.extents) {
		_ = inodeOperation.LookupEkSegment()
		//fmt.Printf("%v:%v\n", inodeOperation.startIndex, inodeOperation.endIndex)
		for i := inodeOperation.startIndex; i < inodeOperation.endIndex; i++ {
			actualSsdEkCnt++
			mediumType := inodeOperation.vol.GetDpMediumType("", "", uint64(i))
			if mediumType != proto.MediumSSDName {
				assert.FailNow(t, fmt.Sprintf("dpId %v mediumType should be %v", i, proto.MediumSSDName))
			}
		}
	}
	//assert.Equalf(t, expectSsdEkCnt, actualSsdEkCnt, "ssd ek count incorrect")
}

func TestLookupMaxSegment(t *testing.T) {
	inodeOperation := new(MigrateInode)
	inodeOperation.migDirection = COMPACTFILEMIGRATE
	inodeOperation.vol = &VolumeInfo{
		GetDpMediumType: func(cluster, volName string, dpId uint64) (mediumType string) {
			return proto.MediumSSDName
		}}
	inodeOperation.extents = append(inodeOperation.extents, proto.ExtentKey{
		PartitionId: uint64(1),
		ExtentId:    100,
		Size:        1 * 1024 * 1024,
	}, proto.ExtentKey{
		PartitionId: uint64(2),
		ExtentId:    100,
		Size:        1 * 1024 * 1024,
	}, proto.ExtentKey{
		PartitionId: uint64(3),
		ExtentId:    100,
		Size:        1 * 1024 * 1024,
	}, proto.ExtentKey{
		PartitionId: uint64(1),
		ExtentId:    100,
		Size:        125 * 1024 * 1024,
	}, proto.ExtentKey{
		PartitionId: uint64(1),
		ExtentId:    100,
		Size:        1 * 1024 * 1024,
	}, proto.ExtentKey{
		PartitionId: uint64(2),
		ExtentId:    100,
		Size:        1 * 1024 * 1024,
	}, proto.ExtentKey{
		PartitionId: uint64(3),
		ExtentId:    100,
		Size:        1 * 1024 * 1024,
	})
	inodeOperation.extentMaxIndex = make(map[string]int, 0)
	inodeOperation.initExtentMaxIndex()
	fmt.Println(inodeOperation.extentMaxIndex)
	if err := inodeOperation.LookupEkSegment(); err != nil {
		assert.FailNow(t, err.Error())
	}
	fmt.Printf("startIndex:%v endIndex:%v\n", inodeOperation.startIndex, inodeOperation.endIndex)
}

func TestLockAndUnlockExtent(t *testing.T) {
	_, ec, _ := creatHelper(t)
	var (
		willLockedEks []proto.ExtentKey
		dpId          uint64 = 1
	)
	for i := proto.TinyExtentStartID; i <= proto.TinyExtentCount; i++ {
		willLockedEks = append(willLockedEks, proto.ExtentKey{
			PartitionId: dpId,
			ExtentId:    uint64(i),
		})
	}
	_ = ec.LockExtent(context.Background(), willLockedEks, lockTime)
	mc := master.NewMasterClient(strings.Split(ltptestMaster, ","), false)
	dpView, _ := mc.ClientAPI().GetDataPartitions(ltptestVolume, []uint64{})
	var dpHosts []string
	for _, partition := range dpView.DataPartitions {
		if partition.PartitionID == dpId {
			dpHosts = partition.Hosts
		}
	}
	for i, host := range dpHosts {
		dpHosts[i] = fmt.Sprintf("%v:%v", strings.Split(host, ":")[0], 17320)
	}
	for _, hostAddr := range dpHosts {
		dataClient := http_client.NewDataClient(hostAddr, false)
		info, err := dataClient.GetExtentLockInfo(dpId, 0)
		if err != nil {
			assert.FailNowf(t, "GetExtentLockInfo failed", "dpId:%v hostAddr:%v, err:%v", dpId, hostAddr, err)
		}
		for i := proto.TinyExtentStartID; i <= proto.TinyExtentCount; i++ {
			_, ok := info[strconv.Itoa(i)]
			assert.Equal(t, true, ok)
		}
	}
	_ = ec.UnlockExtent(context.Background(), willLockedEks)
	for _, hostAddr := range dpHosts {
		dataClient := http_client.NewDataClient(hostAddr, false)
		info, err := dataClient.GetExtentLockInfo(dpId, 0)
		if err != nil {
			assert.FailNowf(t, "GetExtentLockInfo failed", "dpId:%v hostAddr:%v, err:%v", dpId, hostAddr, err)
		}
		assert.Equal(t, 0, len(info))
	}
}

func TestReadExtentAllHost(t *testing.T) {
	setVolForceRow(true)
	defer setVolForceRow(false)

	testFile := "/cfs/mnt/TestReadExtentAllHost"
	file, _ := os.Create(testFile)
	defer func() {
		file.Close()
		os.Remove(testFile)
		log.LogFlush()
	}()

	mw, ec, _ := creatHelper(t)
	stat := getFileStat(t, testFile)
	ctx := context.Background()
	defer func() {
		ec.Close(ctx)
	}()
	writeRowFileBySdk(t, ctx, stat.Ino, size10M, ec)
	_, _, extents, _ := mw.GetExtents(ctx, stat.Ino)

	for _, ek := range extents {
		allEkData, err := ec.ReadExtentAllHost(context.Background(), 1, ek, int(ek.ExtentOffset), int(ek.Size))
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		var crcs = make([]uint32, 0)
		for _, d := range allEkData {
			crc := crc32.ChecksumIEEE(d)
			crcs = append(crcs, crc)
		}
		if len(crcs) < 2 {
			continue
		}
		crc0 := crcs[0]
		for i := 1; i < len(crcs); i++ {
			assert.Equal(t, crcs[i], crc0)
		}
	}
}

func TestCheckReplicaCrcValid(t *testing.T) {
	setVolForceRow(true)
	defer setVolForceRow(false)

	testFile := "/cfs/mnt/TestReadExtentAllHost"
	file, _ := os.Create(testFile)
	defer func() {
		file.Close()
		os.Remove(testFile)
		log.LogFlush()
	}()

	mw, ec, _ := creatHelper(t)
	stat := getFileStat(t, testFile)
	inodeInfo := &proto.InodeInfo{
		Inode: stat.Ino,
	}
	mpOp.vol.NormalDataClient = ec
	mpOp.vol.MetaClient = mw
	ctx := context.Background()
	writeRowFileByMountDir(size512M, testFile)
	_, _, extents, _ := mpOp.vol.GetMetaClient().GetExtents(ctx, stat.Ino)
	cmpInode := &proto.InodeExtents{
		Inode:   inodeInfo,
		Extents: extents,
	}
	mpOp.task = &proto.Task{TaskType: proto.WorkerTypeCompact}
	subTask, err := NewMigrateInode(mpOp, cmpInode)
	if err != nil {
		assert.FailNow(t, err.Error())
		return
	}
	if err = subTask.Init(); err != nil {
		assert.FailNow(t, err.Error())
		return
	}
	f, _ := os.Open(testFile)
	defer f.Close()
	crc := crc32.NewIEEE()
	buf := make([]byte, 1024)
	for {
		n, err := f.Read(buf)
		if err != nil && err != io.EOF {
			panic(err)
		}
		if n > 0 {
			crc.Write(buf[:n])
		}
		if err == io.EOF {
			break
		}
	}
	subTask.migDataCrc = crc.Sum32()
	subTask.startIndex = 0
	subTask.endIndex = len(subTask.extents)
	replicaCrc, err := subTask.replicaCrc()
	if err != nil {
		assert.FailNow(t, err.Error())
	}
	if err = subTask.checkReplicaCrcValid(replicaCrc); err != nil {
		assert.FailNow(t, err.Error())
	}
}

func TestCheckReplicaCrcValid2(t *testing.T) {
	setVolForceRow(true)
	defer setVolForceRow(false)

	testFile := "/cfs/mnt/TestReadExtentAllHost"
	file, _ := os.Create(testFile)
	defer func() {
		file.Close()
		os.Remove(testFile)
		log.LogFlush()
	}()

	mw, ec, _ := creatHelper(t)
	stat := getFileStat(t, testFile)
	inodeInfo := &proto.InodeInfo{
		Inode: stat.Ino,
	}
	mpOp.vol.NormalDataClient = ec
	mpOp.vol.MetaClient = mw
	ctx := context.Background()
	writeRowFileByMountDir(size512M, testFile)
	_, _, extents, _ := mpOp.vol.GetMetaClient().GetExtents(ctx, stat.Ino)
	cmpInode := &proto.InodeExtents{
		Inode:   inodeInfo,
		Extents: extents,
	}
	mpOp.task = &proto.Task{TaskType: proto.WorkerTypeCompact}
	subTask, err := NewMigrateInode(mpOp, cmpInode)
	if err != nil {
		assert.FailNow(t, err.Error())
		return
	}
	if err = subTask.Init(); err != nil {
		assert.FailNow(t, err.Error())
		return
	}
	subTask.migDirection = SSDTOHDDFILEMIGRATE
	_ = subTask.OpenFile()
	subTask.startIndex = 0
	subTask.endIndex = len(subTask.extents)
	err = subTask.ReadAndWriteEkData()
	if err != nil {
		assert.FailNow(t, err.Error())
	}
	replicaCrc, err := subTask.replicaCrc()
	if err != nil {
		assert.FailNow(t, err.Error())
	}
	if err = subTask.checkReplicaCrcValid(replicaCrc); err != nil {
		assert.FailNow(t, err.Error())
	}
}

func TestDeleteOldExtents(t *testing.T) {
	inodeOperation := &MigrateInode{
		inodeInfo: &proto.InodeInfo{
			Inode: 10,
		},
		extents: []proto.ExtentKey{
			{
				PartitionId: 1,
				ExtentId:    1,
			},
			{
				PartitionId: 3,
				ExtentId:    1,
			},
			{
				PartitionId: 2,
				ExtentId:    1,
			},
			{
				PartitionId: 2,
				ExtentId:    1,
			},
			{
				PartitionId: 2,
				ExtentId:    1,
			},
		},
	}
	inodeOperation.startIndex = 0
	inodeOperation.endIndex = 2
	inodeOperation.mpOp = mpOp
	inodeOperation.deleteOldExtents(inodeOperation.extents[inodeOperation.startIndex:inodeOperation.endIndex])
}

var (
//inodeOp *MigrateInode
)

func TestCalcCmpExtents(t *testing.T) {
	inodeInfo := &proto.InodeInfo{
		Inode: 100,
	}
	cmpInode := &proto.InodeExtents{
		Inode: inodeInfo,
	}
	offsetSizes := [][2]int{
		{0, 10},            // 0
		{10, 1048566},      // 1
		{1048576, 10},      // 2
		{1048586, 1048566}, // 3
		{2097152, 10},      // 4
		{2097162, 1048566}, // 5
		{3145728, 10},      // 6
		{3145738, 1048566}, // 7
		{4194304, 10},      // 8
		{4194314, 1048566}, // 9
		{5242880, 10},      // 10
		{5242890, 1048566}, // 11
		{6291456, 10},      // 12
		{6291466, 1048566}, // 13
		{7340032, 10},      // 14
		{7340042, 1048566}, // 15
		{8388608, 10},      // 16
		{8388618, 1048566}, // 17
		{9437184, 10},      // 18
		{9437194, 1048566}, // 19
	}
	cmpInode.Extents = make([]proto.ExtentKey, len(offsetSizes))
	for i, offsetSize := range offsetSizes {
		cmpInode.Extents[i] = proto.ExtentKey{FileOffset: uint64(offsetSize[0]), Size: uint32(offsetSize[1])}
	}
	// mpId uint64, inode *proto.InodeExtents, vol *CompactVolumeInfo
	var (
		inodeOperation *MigrateInode
		err            error
	)
	mpOp.task = &proto.Task{TaskType: proto.WorkerTypeCompact}
	mpOp.vol.GetDpMediumType = func(cluster, volName string, dpId uint64) (mediumType string) {
		return proto.MediumSSDName
	}
	inodeOperation, err = NewMigrateInode(mpOp, cmpInode)
	if err != nil {
		assert.FailNow(t, err.Error())
		return
	}
	_ = inodeOperation.Init()
	inodeOperation.migDirection = COMPACTFILEMIGRATE
	results := [][3]int{
		{0, 20, int(LockExtents)},
		{0, 20, int(InodeMigStopped)},
		{0, 20, int(InodeMigStopped)},
	}
	index := 0
	for {
		_ = inodeOperation.LookupEkSegment()
		if !(results[index][0] == inodeOperation.startIndex &&
			results[index][1] == inodeOperation.endIndex &&
			results[index][2] == int(inodeOperation.stage)) {
			fmt.Printf("TestCalcCmpExtents result mismatch, startIndex:endIndex:stage expect:%v,%v,%v, actual:%v,%v,%v\n",
				results[index][0], results[index][1], migrateInodeStage(results[index][2]), inodeOperation.startIndex, inodeOperation.endIndex, inodeOperation.stage)
			t.Fatalf("TestCalcCmpExtents result mismatch, startIndex:endIndex:stage expect:%v,%v,%v, actual:%v,%v,%v",
				results[index][0], results[index][1], migrateInodeStage(results[index][2]), inodeOperation.startIndex, inodeOperation.endIndex, inodeOperation.stage)
		}
		index++
		if inodeOperation.stage == InodeMigStopped {
			break
		}
	}
}

func TestInitTask(t *testing.T) {
	inodeInfo := &proto.InodeInfo{
		Inode: 100,
	}
	cmpInode := &proto.InodeExtents{
		Inode:   inodeInfo,
		Extents: []proto.ExtentKey{},
	}
	mpOp.task = &proto.Task{TaskType: proto.WorkerTypeCompact}
	subTask, err := NewMigrateInode(mpOp, cmpInode)
	if err != nil {
		assert.FailNow(t, err.Error())
		return
	}
	subTask.Init()
	if subTask.statisticsInfo.MigEkCnt != 0 {
		t.Fatalf("inode task Initial CmpEkCnt expect:%v, actual:%v", 0, subTask.statisticsInfo.MigEkCnt)
	}
	if subTask.statisticsInfo.MigCnt != 0 {
		t.Fatalf("inode task Initial CmpCnt expect:%v, actual:%v", 0, subTask.statisticsInfo.MigCnt)
	}
	if subTask.stage != OpenFile {
		t.Fatalf("inode task Initial stage expect:%v, actual:%v", OpenFile, subTask.stage)
	}
}

func TestOpenFile(t *testing.T) {
	testFilePath := "/cfs/mnt/TestOpenFile"
	file, _ := os.Create(testFilePath)
	defer func() {
		file.Close()
		os.Remove(testFilePath)
		log.LogFlush()
	}()
	mw, ec, _ := creatHelper(t)
	ctx := context.Background()
	defer func() {
		if err := ec.Close(ctx); err != nil {
			t.Errorf("close ExtentClient failed: err(%v), vol(%v)", err, ltptestVolume)
		}
		if err := mw.Close(); err != nil {
			t.Errorf("close MetaWrapper failed: err(%v), vol(%v)", err, ltptestVolume)
		}
	}()
	stat := getFileStat(t, testFilePath)
	_ = ec.OpenStream(stat.Ino, false)

	bytes := make([]byte, 512)
	_, _, err := ec.Write(ctx, stat.Ino, 0, bytes, false)
	if err != nil {
		t.Fatalf("ec.write failed: err(%v)", err)
	}
	inodeInfo := &proto.InodeInfo{
		Inode: stat.Ino,
	}
	cmpInode := &proto.InodeExtents{
		Inode:   inodeInfo,
		Extents: []proto.ExtentKey{},
	}
	mpOp.vol.NormalDataClient = ec
	mpOp.task = &proto.Task{TaskType: proto.WorkerTypeCompact}
	subTask, _ := NewMigrateInode(mpOp, cmpInode)
	_ = subTask.Init()
	err = subTask.OpenFile()
	if err != nil {
		t.Fatalf("inode task OpenFile failed: err(%v) inodeId(%v)", err, stat.Ino)
	}
	if subTask.stage != LookupEkSegment {
		t.Fatalf("inode task OpenFile stage expect:%v, actual:%v", LookupEkSegment, subTask.stage)
	}

	var notExistInoId uint64 = 200
	inodeInfo = &proto.InodeInfo{
		Inode: notExistInoId,
	}
	cmpInode = &proto.InodeExtents{
		Inode:   inodeInfo,
		Extents: []proto.ExtentKey{},
	}
	subTask, _ = NewMigrateInode(mpOp, cmpInode)
	_ = subTask.Init()
	err = subTask.OpenFile()
	if err == nil {
		t.Fatalf("inode task OpenFile should hava error, but it does not hava, inodeId(%v)", notExistInoId)
	}
}

func TestReadAndWriteEkData(t *testing.T) {
	setVolForceRow(true)
	defer setVolForceRow(false)

	testFile := "/cfs/mnt/TestReadEkData"
	file, _ := os.Create(testFile)
	defer func() {
		file.Close()
		os.Remove(testFile)
		log.LogFlush()
	}()

	mw, ec, _ := creatHelper(t)
	stat := getFileStat(t, testFile)
	inodeInfo := &proto.InodeInfo{
		Inode: stat.Ino,
	}
	mpOp.vol.NormalDataClient = ec
	mpOp.vol.MetaClient = mw
	ctx := context.Background()
	writeRowFileBySdk(t, ctx, stat.Ino, size10M, mpOp.vol.NormalDataClient)
	_, _, extents, _ := mpOp.vol.GetMetaClient().GetExtents(ctx, stat.Ino)
	cmpInode := &proto.InodeExtents{
		Inode:   inodeInfo,
		Extents: extents,
	}
	mpOp.task = &proto.Task{TaskType: proto.WorkerTypeCompact}
	subTask, _ := NewMigrateInode(mpOp, cmpInode)
	_ = subTask.Init()
	_ = subTask.OpenFile()
	for i := 0; i < len(extents); i++ {
		subTask.startIndex = i
		subTask.endIndex = len(extents) - 1
		err := subTask.ReadAndWriteEkData()
		if err != nil {
			cmpEksCnt := subTask.endIndex - subTask.startIndex + 1
			if len(subTask.newEks) >= cmpEksCnt {
				t.Logf("%v", err)
				continue
			}
			t.Fatalf("inode task ReadEkData failed: err(%v)", err)
		}
		if subTask.stage != MetaMergeExtents {
			t.Fatalf("inode task Initial stage expect:%v, actual:%v", MetaMergeExtents, subTask.stage)
		}
	}
}

func TestReadAndWriteEkData2(t *testing.T) {
	setVolForceRow(true)
	defer setVolForceRow(false)

	testFile := "/cfs/mnt/TestWriteMergeExtentData"
	file, _ := os.Create(testFile)
	defer func() {
		file.Close()
		os.Remove(testFile)
		log.LogFlush()
	}()
	mw, ec, _ := creatHelper(t)
	stat := getFileStat(t, testFile)
	inodeInfo := &proto.InodeInfo{
		Inode: stat.Ino,
	}
	mpOp.vol.NormalDataClient = ec
	mpOp.vol.MetaClient = mw
	ctx := context.Background()
	writeRowFileBySdk(t, ctx, stat.Ino, size10M, mpOp.vol.NormalDataClient)
	_, _, extents, _ := mpOp.vol.GetMetaClient().GetExtents(ctx, stat.Ino)
	cmpInode := &proto.InodeExtents{
		Inode:   inodeInfo,
		Extents: extents,
	}
	mpOp.task = &proto.Task{TaskType: proto.WorkerTypeCompact}
	subTask, _ := NewMigrateInode(mpOp, cmpInode)
	subTask.startIndex = 0
	subTask.endIndex = len(extents)
	_ = subTask.Init()
	_ = subTask.OpenFile()
	err := subTask.ReadAndWriteEkData()
	if err != nil {
		cmpEksCnt := subTask.endIndex - subTask.startIndex + 1
		if len(subTask.newEks) >= cmpEksCnt {
			t.Logf("%v", err)
			return
		}
	}
	startOffset := subTask.extents[subTask.startIndex].FileOffset
	size := subTask.extents[subTask.endIndex-1].FileOffset + uint64(subTask.extents[subTask.endIndex-1].Size) - startOffset
	if subTask.newEks[0].FileOffset != extents[subTask.startIndex].FileOffset {
		t.Fatalf("inode task WriteMergeExtentData FileOffset expect:%v, actual:%v", extents[subTask.startIndex].FileOffset, subTask.newEks[0].FileOffset)
	}
	var cmpSize uint32
	for _, newEk := range subTask.newEks {
		cmpSize += newEk.Size
	}
	if uint64(cmpSize) != size {
		t.Fatalf("inode task WriteMergeExtentData Size expect:%v, actual:%v", size, cmpSize)
	}
	if err != nil {
		t.Fatalf("inode task WriteMergeExtentData failed: err(%v)", err)
	}
	if subTask.stage != MetaMergeExtents {
		t.Fatalf("inode task Initial stage expect:%v, actual:%v", MetaMergeExtents, subTask.stage)
	}
}

func TestMetaMergeExtents(t *testing.T) {
	setVolForceRow(true)
	defer setVolForceRow(false)

	testFile := "/cfs/mnt/TestWriteMergeExtentData"
	file, _ := os.Create(testFile)
	defer func() {
		file.Close()
		os.Remove(testFile)
		log.LogFlush()
	}()
	mw, ec, _ := creatHelper(t)
	stat := getFileStat(t, testFile)
	inodeInfo := &proto.InodeInfo{
		Inode: stat.Ino,
	}
	mpOp.vol.NormalDataClient = ec
	mpOp.vol.SetMetaClient(mw)
	ctx := context.Background()
	writeRowFileBySdk(t, ctx, stat.Ino, size10M, mpOp.vol.NormalDataClient)
	gen, _, extents, _ := mpOp.vol.GetMetaClient().GetExtents(ctx, stat.Ino)
	//fmt.Printf("before merge gen:%v\n", gen)
	cmpInode := &proto.InodeExtents{
		Inode:   inodeInfo,
		Extents: extents,
	}
	// 根据inode获取mpId，再根据mpId获取info
	_, mpID, err := util_sdk.LocateInode(stat.Ino, mpOp.mc, ltptestVolume)
	if err != nil {
		t.Fatalf("LocateInode(%v) info failed, err(%v)", stat.Ino, err)
	}
	mpOp.mpId = mpID
	var mpInfo *proto.MetaPartitionInfo
	cMP := &meta.MetaPartition{PartitionID: mpID}
	mpInfo, err = mpOp.mc.ClientAPI().GetMetaPartition(mpID, "")
	if err != nil {
		t.Fatalf("get meta partition(%v) info failed, err(%v)", mpID, err)
	}
	for _, replica := range mpInfo.Replicas {
		cMP.Members = append(cMP.Members, replica.Addr)
		if replica.IsLeader {
			cMP.LeaderAddr = proto.NewAtomicString(replica.Addr)
		}
		if replica.IsLearner {
			cMP.Learners = append(cMP.Learners, replica.Addr)
		}
	}
	cMP.Status = mpInfo.Status
	cMP.Start = mpInfo.Start
	cMP.End = mpInfo.End
	mpOp.mpInfo = cMP
	mpOp.leader = cMP.GetLeaderAddr()
	err = mpOp.GetProfPort()
	if err != nil {
		t.Fatalf("GetProfPort(%v) info failed, err(%v)", mpID, err)
	}
	mpOp.task = &proto.Task{TaskType: proto.WorkerTypeCompact}
	subTask, _ := NewMigrateInode(mpOp, cmpInode)
	remainEkCnt := 2
	subTask.startIndex = 0
	subTask.endIndex = len(extents) - remainEkCnt
	_ = subTask.Init()
	_ = subTask.OpenFile()
	err = subTask.ReadAndWriteEkData()
	if err != nil {
		assert.FailNow(t, err.Error())
		return
	}
	afterCompactEkLen := len(subTask.newEks) + remainEkCnt
	err = subTask.MetaMergeExtents()
	if err != nil {
		t.Fatalf("inode task MetaMergeExtents failed: err(%v)", err)
	}
	gen, _, extents, _ = mpOp.vol.GetMetaClient().GetExtents(ctx, stat.Ino)
	fmt.Printf("after merge gen:%v\n", gen)
	if len(extents) != afterCompactEkLen {
		t.Fatalf("inode task MetaMergeExtents failed: extents length, expect:%v, actual:%v", afterCompactEkLen, len(extents))
	}
	if subTask.stage != LookupEkSegment {
		t.Fatalf("inode task Initial stage expect:%v, actual:%v", LookupEkSegment, subTask.stage)
	}
}

func TestMetaMergeExtentsError(t *testing.T) {
	setVolForceRow(true)
	defer setVolForceRow(false)

	testFile := "/cfs/mnt/TestWriteMergeExtentData"
	file, _ := os.Create(testFile)
	defer func() {
		file.Close()
		os.Remove(testFile)
		log.LogFlush()
	}()
	mw, ec, _ := creatHelper(t)
	stat := getFileStat(t, testFile)
	inodeInfo := &proto.InodeInfo{
		Inode: stat.Ino,
	}
	mpOp.vol.NormalDataClient = ec
	mpOp.vol.MetaClient = mw
	ctx := context.Background()
	writeRowFileBySdk(t, ctx, stat.Ino, size10M, mpOp.vol.NormalDataClient)
	_, _, extents, _ := mpOp.vol.GetMetaClient().GetExtents(ctx, stat.Ino)
	cmpInode := &proto.InodeExtents{
		Inode:   inodeInfo,
		Extents: extents,
	}
	mpOp.task = &proto.Task{TaskType: proto.WorkerTypeCompact}
	subTask, _ := NewMigrateInode(mpOp, cmpInode)
	subTask.startIndex = 0
	subTask.endIndex = len(extents) - 2
	_ = subTask.Init()
	_ = subTask.OpenFile()
	_ = subTask.ReadAndWriteEkData()
	// modify file
	_, _, _ = ec.Write(ctx, stat.Ino, 0, []byte{1, 2, 3, 4, 5}, false)
	if err := ec.Flush(ctx, stat.Ino); err != nil {
		t.Fatalf("Flush failed, err(%v)", err)
	}
	err := subTask.MetaMergeExtents()
	if err == nil {
		t.Fatalf("inode task MetaMergeExtents should hava error, but it does not hava, inodeId(%v)", stat.Ino)
	}
}

func TestWriteFileByRow(t *testing.T) {
	setVolForceRow(true)
	defer setVolForceRow(false)

	testFile := "/cfs/mnt/TestWriteFileByRow"
	file, _ := os.Create(testFile)
	defer func() {
		file.Close()
		os.Remove(testFile)
		log.LogFlush()
	}()

	mw, ec, _ := creatHelper(t)
	stat := getFileStat(t, testFile)
	mpOp.vol.NormalDataClient = ec
	mpOp.vol.MetaClient = mw
	ctx := context.Background()
	writeRowFileBySdk(t, ctx, stat.Ino, size10M, mpOp.vol.NormalDataClient)
}

func TestCompareReplicasInodeEksEqual(t *testing.T) {
	setVolForceRow(true)
	defer setVolForceRow(false)

	testFile := "/cfs/mnt/TestCompareReplicasInodeEksEqual"
	file, _ := os.Create(testFile)
	defer func() {
		file.Close()
		os.Remove(testFile)
		log.LogFlush()
	}()

	stat := getFileStat(t, testFile)
	inodeInfo := &proto.InodeInfo{
		Inode: stat.Ino,
	}
	mw, ec, _ := creatHelper(t)
	mpOp.vol.NormalDataClient = ec
	mpOp.vol.MetaClient = mw
	ctx := context.Background()
	writeRowFileBySdk(t, ctx, stat.Ino, size128M, mpOp.vol.GetDataClient())
	mps, err := clusterInfo.MasterClient.ClientAPI().GetMetaPartitions(ltptestVolume)
	if err != nil {
		assert.FailNowf(t, err.Error(), "", "")
	}
	var (
		members []string
		mpId    uint64
		leader  string
	)
	for _, mp := range mps {
		if inodeInfo.Inode >= mp.Start && inodeInfo.Inode < mp.End {
			mpId = mp.PartitionID
			members = mp.Members
			leader = mp.LeaderAddr
			break
		}
	}
	if len(members) < 3 {
		assert.FailNowf(t, "", "the members of mpId[%v] less than three", mpId)
	}
	mpOp.mpId = mpId
	mpOp.mpInfo = &meta.MetaPartition{
		Members: members,
	}
	var leaderNodeInfo *proto.MetaNodeInfo
	if leaderNodeInfo, err = clusterInfo.MasterClient.NodeAPI().GetMetaNode(leader); err != nil {
		assert.FailNowf(t, err.Error(), "", "")
		return
	}
	mpOp.profPort = leaderNodeInfo.ProfPort
	writeRowFileBySdk(t, ctx, stat.Ino, size10M, mpOp.vol.GetDataClient())
	cmpInode := &proto.InodeExtents{
		Inode: inodeInfo,
	}
	mpOp.task = &proto.Task{TaskType: proto.WorkerTypeCompact}
	subTask, _ := NewMigrateInode(mpOp, cmpInode)
	isEqual := subTask.compareReplicasInodeEksEqual()
	assert.True(t, isEqual, "")
}

func writeRowFileByMountDir(size int, filePath string) {
	file, _ := os.Create(filePath)
	defer func() {
		file.Close()
	}()
	bufStr := strings.Repeat("A", size)
	bytes := []byte(bufStr)
	_, _ = file.WriteAt(bytes, 0)
	mStr := strings.Repeat("b", 10)
	mBytes := []byte(mStr)
	for i := 0; i < size; i++ {
		remain := i % (1024 * 1024)
		if remain == 0 {
			_, _ = file.WriteAt(mBytes, int64(i))
		}
	}
	_ = file.Sync()
	return
}

func writeRowFileBySdk(t *testing.T, ctx context.Context, inoId uint64, size int, ec *data.ExtentClient) {
	bufStr := strings.Repeat("A", size)
	bytes := []byte(bufStr)
	if err := ec.OpenStream(inoId, false); err != nil {
		t.Fatalf("writeRowFileBySdk OpenStream failed: inodeId(%v), err(%v)", inoId, err)
	}
	_, _, err := ec.Write(ctx, inoId, 0, bytes, false)
	if err != nil {
		t.Fatalf("writeRowFileBySdk SyncWrite failed: inodeId(%v), err(%v)", inoId, err)
	}
	mStr := strings.Repeat("b", 10)
	mBytes := []byte(mStr)
	for i := 0; i < size; i++ {
		remain := i % (1024 * 1024)
		if remain == 0 {
			_, _, _ = ec.Write(ctx, inoId, uint64(i), mBytes, false)
		}
	}
	if err = ec.Flush(ctx, inoId); err != nil {
		t.Fatalf("Flush failed, err(%v)", err)
	}
}

func creatHelper(t *testing.T) (mw *meta.MetaWrapper, ec *data.ExtentClient, err error) {
	if mw, err = meta.NewMetaWrapper(&meta.MetaConfig{
		Volume:        ltptestVolume,
		Masters:       strings.Split(ltptestMaster, ","),
		ValidateOwner: true,
		Owner:         ltptestVolume,
	}); err != nil {
		t.Fatalf("NewMetaWrapper failed: err(%v) vol(%v)", err, ltptestVolume)
	}
	if ec, err = data.NewExtentClient(&data.ExtentConfig{
		Volume:            ltptestVolume,
		Masters:           strings.Split(ltptestMaster, ","),
		FollowerRead:      false,
		OnInsertExtentKey: mw.InsertExtentKey,
		OnGetExtents:      mw.GetExtents,
		OnTruncate:        mw.Truncate,
		TinySize:          -1,
	}, nil); err != nil {
		t.Fatalf("NewExtentClient failed: err(%v), vol(%v)", err, ltptestVolume)
	}
	return mw, ec, nil
}

func setVolForceRow(forceRow bool) {
	mc := getMasterClient()
	vv, _ := mc.AdminAPI().GetVolumeSimpleInfo(ltptestVolume)
	_ = mc.AdminAPI().UpdateVolume(vv.Name, vv.Capacity, int(vv.DpReplicaNum), int(vv.MpReplicaNum), int(vv.TrashRemainingDays),
		int(vv.DefaultStoreMode), vv.FollowerRead, vv.VolWriteMutexEnable, vv.NearRead, vv.Authenticate, vv.EnableToken, vv.AutoRepair,
		forceRow, vv.IsSmart, vv.EnableWriteCache, calcAuthKey(vv.Owner), vv.ZoneName, fmt.Sprintf("%v,%v", vv.MpLayout.PercentOfMP, vv.MpLayout.PercentOfReplica), strings.Join(vv.SmartRules, ","),
		uint8(vv.OSSBucketPolicy), uint8(vv.CrossRegionHAType), vv.ExtentCacheExpireSec, vv.CompactTag, vv.DpFolReadDelayConfig.DelaySummaryInterval, vv.FolReadHostWeight, 0, 0, 0, vv.UmpCollectWay, -1, -1, false,
		"", false, false, 0, false, vv.ConnConfig.ReadTimeoutNs, vv.ConnConfig.WriteTimeoutNs, 0, 0)
}

func calcAuthKey(key string) (authKey string) {
	h := md5.New()
	_, _ = h.Write([]byte(key))
	cipherStr := h.Sum(nil)
	return strings.ToLower(hex.EncodeToString(cipherStr))
}

func getFileStat(t *testing.T, file string) *syscall.Stat_t {
	info, err := os.Stat(file)
	if err != nil {
		t.Fatalf("Get Stat failed: err(%v) file(%v)", err, file)
	}
	return info.Sys().(*syscall.Stat_t)
}

func getMasterClient() *master.MasterClient {
	masterClient := master.NewMasterClient(strings.Split(ltptestMaster, ","), false)
	return masterClient
}
