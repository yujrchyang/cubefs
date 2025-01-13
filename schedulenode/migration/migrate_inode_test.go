package migration

import (
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/cubefs/cubefs/sdk/s3"

	util_sdk "github.com/cubefs/cubefs/cli/cmd/util/sdk"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data"
	"github.com/cubefs/cubefs/sdk/http_client"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/util/log"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
)

const (
	clusterName   = "chubaofs01"
	ltptestVolume = "ltptest"
	ltptestMaster = "192.168.0.11:17010,192.168.0.12:17010,192.168.0.13:17010"
	size1M        = 1 * 1024 * 1024
	size5M        = 5 * 1024 * 1024
	size10M       = 10 * 1024 * 1024
	size128M      = 128 * 1024 * 1024
	size512M      = 512 * 1024 * 1024
)

var (
	vol  *VolumeInfo
	mpOp *MigrateTask
)

func init() {
	vol = &VolumeInfo{
		GetMigrationConfig: func(cluster, volName string) (volumeConfig proto.MigrationConfig) {
			return proto.MigrationConfig{
				Region:    region,
				Endpoint:  endPoint,
				AccessKey: accessKey,
				SecretKey: secretKey,
				Bucket:    bucketName,
			}
		},
	}
	vol.ClusterName = clusterName
	vol.Name = ltptestVolume
	nodes := strings.Split(ltptestMaster, ",")
	err := vol.Init(nodes, data.Normal)
	if err != nil {
		panic(fmt.Sprintf("vol.Init, err:%v", err))
	}
	mpOp = &MigrateTask{
		vol: vol,
		mc:  master.NewMasterClient(strings.Split(ltptestMaster, ","), false),
	}
	_, err = log.InitLog("/cfs/log", "unittest", log.DebugLevel, nil)
	if err != nil {
		panic(fmt.Sprintf("init log in /cfs/log failed"))
	}
}

var clusterInfo = &ClusterInfo{
	Name:         clusterName,
	MasterClient: master.NewMasterClient(strings.Split(ltptestMaster, ","), false),
}

type LayerPolicyMeta struct {
	TimeType     int8
	TimeValue    int64
	TargetMedium proto.MediumType
}

func TestSetInodeMigDirection(t *testing.T) {
	layerPolicyMetas := []LayerPolicyMeta{
		{
			TimeType:     proto.InodeAccessTimeTypeSec,
			TimeValue:    15,
			TargetMedium: proto.MediumHDD,
		},
		{
			TimeType:     proto.InodeAccessTimeTypeDays,
			TimeValue:    15,
			TargetMedium: proto.MediumHDD,
		},
		{
			TimeType:     proto.InodeAccessTimeTypeTimestamp,
			TimeValue:    15,
			TargetMedium: proto.MediumHDD,
		},
		{
			TimeType:     proto.InodeAccessTimeTypeSec,
			TimeValue:    15,
			TargetMedium: proto.MediumS3,
		},
		{
			TimeType:     proto.InodeAccessTimeTypeDays,
			TimeValue:    15,
			TargetMedium: proto.MediumS3,
		},
		{
			TimeType:     proto.InodeAccessTimeTypeTimestamp,
			TimeValue:    15,
			TargetMedium: proto.MediumS3,
		},
	}
	for _, policyMeta := range layerPolicyMetas {
		vol.GetLayerPolicies = func(cluster, volName string) (layerPolicies []interface{}, exist bool) {
			exist = true
			lp := &proto.LayerPolicyInodeATime{
				TimeType:     policyMeta.TimeType,
				TimeValue:    policyMeta.TimeValue,
				TargetMedium: policyMeta.TargetMedium,
			}
			layerPolicies = append(layerPolicies, lp)
			return
		}
		mpOperation := &MigrateTask{
			vol:  vol,
			mpId: 1,
		}
		inodeOperation := &MigrateInode{
			mpOp: mpOperation,
			inodeInfo: &proto.InodeInfo{
				AccessTime: proto.CubeFSTime(0),
				ModifyTime: proto.CubeFSTime(0),
			},
		}
		var (
			migDir MigrateDirection
			err    error
		)
		if migDir, err = inodeOperation.mpOp.getInodeMigDirection(inodeOperation.inodeInfo); err != nil {
			assert.FailNow(t, err.Error())
		}
		if policyMeta.TargetMedium == proto.MediumHDD {
			assert.Equal(t, SSDToHDDFileMigrate, migDir)
		}
		if policyMeta.TargetMedium == proto.MediumS3 {
			assert.Equal(t, S3FileMigrate, migDir)
		}
		inodeOperation = &MigrateInode{
			mpOp: mpOperation,
			inodeInfo: &proto.InodeInfo{
				AccessTime: proto.CubeFSTime(time.Now().Unix()),
				ModifyTime: proto.CubeFSTime(time.Now().Unix()),
			},
		}
		if migDir, err = inodeOperation.mpOp.getInodeMigDirection(inodeOperation.inodeInfo); err != nil {
			assert.FailNow(t, err.Error())
		}
		if policyMeta.TargetMedium == proto.MediumHDD {
			assert.Equal(t, HDDToSSDFileMigrate, migDir)
		}
		if policyMeta.TargetMedium == proto.MediumS3 {
			assert.Equal(t, ReverseS3FileMigrate, migDir)
		}
	}
}

func TestGetSSSDEkSegment(t *testing.T) {
	inodeOperation := new(MigrateInode)
	inodeOperation.migDirection = SSDToHDDFileMigrate
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
	inodeOperation.migDirection = CompactFileMigrate
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
	expectExtentMaxIndex := make(map[string]int, 0)
	expectExtentMaxIndex["1#100"] = 4
	expectExtentMaxIndex["2#100"] = 5
	expectExtentMaxIndex["3#100"] = 6
	for key, value := range inodeOperation.extentMaxIndex {
		assert.Equal(t, expectExtentMaxIndex[key], value)
	}
	if err := inodeOperation.LookupEkSegment(); err != nil {
		assert.FailNow(t, err.Error())
	}
	assert.Equal(t, 0, inodeOperation.startIndex)
	assert.Equal(t, 7, inodeOperation.endIndex)
}

func TestLookupEkS3MigDirection(t *testing.T) {
	inodeOperation := new(MigrateInode)
	inodeOperation.migDirection = S3FileMigrate
	inodeOperation.vol = &VolumeInfo{
		GetDpMediumType: func(cluster, volName string, dpId uint64) (mediumType string) {
			return proto.MediumSSDName
		}}
	inodeOperation.extents = append(inodeOperation.extents, proto.ExtentKey{
		PartitionId: uint64(1),
		ExtentId:    100,
		Size:        1 * 1024 * 1024,
		CRC:         0,
	}, proto.ExtentKey{
		PartitionId: uint64(2),
		ExtentId:    100,
		Size:        1 * 1024 * 1024,
		CRC:         0,
	}, proto.ExtentKey{
		PartitionId: uint64(3),
		ExtentId:    100,
		Size:        1 * 1024 * 1024,
		CRC:         0,
	})
	inodeOperation.extentMaxIndex = make(map[string]int, 0)
	inodeOperation.initExtentMaxIndex()
	expectExtentMaxIndex := make(map[string]int, 0)
	expectExtentMaxIndex["1#100"] = 0
	expectExtentMaxIndex["2#100"] = 1
	expectExtentMaxIndex["3#100"] = 2
	for key, value := range inodeOperation.extentMaxIndex {
		assert.Equal(t, expectExtentMaxIndex[key], value)
	}
	if err := inodeOperation.LookupEkSegment(); err != nil {
		assert.FailNow(t, err.Error())
	}
	assert.Equal(t, 0, inodeOperation.startIndex)
	assert.Equal(t, 3, inodeOperation.endIndex)
	inodeOperation.extents[1].CRC = 1
	inodeOperation.lastMigEkIndex = 0
	if err := inodeOperation.LookupEkSegment(); err != nil {
		assert.FailNow(t, err.Error())
	}
	assert.Equal(t, 0, inodeOperation.startIndex)
	assert.Equal(t, 1, inodeOperation.endIndex)
	if err := inodeOperation.LookupEkSegment(); err != nil {
		assert.FailNow(t, err.Error())
	}
	assert.Equal(t, 2, inodeOperation.startIndex)
	assert.Equal(t, 3, inodeOperation.endIndex)
	inodeOperation.migDirection = ReverseS3FileMigrate
	inodeOperation.lastMigEkIndex = 0
	inodeOperation.startIndex = 0
	inodeOperation.endIndex = 0
	inodeOperation.extents[0].CRC = 1
	inodeOperation.extents[1].CRC = 1
	inodeOperation.extents[2].CRC = 1
	if err := inodeOperation.LookupEkSegment(); err != nil {
		assert.FailNow(t, err.Error())
	}
	assert.Equal(t, 0, inodeOperation.startIndex)
	assert.Equal(t, 3, inodeOperation.endIndex)
	inodeOperation.lastMigEkIndex = 0
	inodeOperation.startIndex = 0
	inodeOperation.endIndex = 0
	inodeOperation.extents[1].CRC = 0
	if err := inodeOperation.LookupEkSegment(); err != nil {
		assert.FailNow(t, err.Error())
	}
	assert.Equal(t, 0, inodeOperation.startIndex)
	assert.Equal(t, 1, inodeOperation.endIndex)
	if err := inodeOperation.LookupEkSegment(); err != nil {
		assert.FailNow(t, err.Error())
	}
	assert.Equal(t, 2, inodeOperation.startIndex)
	assert.Equal(t, 3, inodeOperation.endIndex)
}

func TestLookupEkHddMigDirection(t *testing.T) {
	inodeOperation := new(MigrateInode)
	inodeOperation.migDirection = SSDToHDDFileMigrate
	inodeOperation.vol = &VolumeInfo{
		GetDpMediumType: func(cluster, volName string, dpId uint64) (mediumType string) {
			return proto.MediumSSDName
		}}
	inodeOperation.extents = append(inodeOperation.extents, proto.ExtentKey{
		PartitionId: uint64(1),
		ExtentId:    100,
		Size:        1 * 1024 * 1024,
		CRC:         0,
	}, proto.ExtentKey{
		PartitionId: uint64(2),
		ExtentId:    100,
		Size:        1 * 1024 * 1024,
		CRC:         0,
	}, proto.ExtentKey{
		PartitionId: uint64(3),
		ExtentId:    100,
		Size:        1 * 1024 * 1024,
		CRC:         0,
	}, proto.ExtentKey{
		PartitionId: uint64(3),
		ExtentId:    101,
		Size:        128 * 1024 * 1024,
		CRC:         0,
	})
	if err := inodeOperation.LookupEkSegment(); err != nil {
		assert.FailNow(t, err.Error())
	}
	assert.Equal(t, 0, inodeOperation.startIndex)
	assert.Equal(t, 3, inodeOperation.endIndex)
	if err := inodeOperation.LookupEkSegment(); err != nil {
		assert.FailNow(t, err.Error())
	}
	assert.Equal(t, 3, inodeOperation.startIndex)
	assert.Equal(t, 4, inodeOperation.endIndex)
	inodeOperation.vol = &VolumeInfo{
		GetDpMediumType: func(cluster, volName string, dpId uint64) (mediumType string) {
			return proto.MediumHDDName
		}}
	inodeOperation.lastMigEkIndex = 0
	inodeOperation.startIndex = 0
	inodeOperation.endIndex = 0
	if err := inodeOperation.LookupEkSegment(); err != nil {
		assert.FailNow(t, err.Error())
	}
	assert.Equal(t, 0, inodeOperation.startIndex)
	assert.Equal(t, 0, inodeOperation.endIndex)
	inodeOperation.migDirection = HDDToSSDFileMigrate
	inodeOperation.lastMigEkIndex = 0
	inodeOperation.startIndex = 0
	inodeOperation.endIndex = 0
	if err := inodeOperation.LookupEkSegment(); err != nil {
		assert.FailNow(t, err.Error())
	}
	assert.Equal(t, 0, inodeOperation.startIndex)
	assert.Equal(t, 3, inodeOperation.endIndex)
	if err := inodeOperation.LookupEkSegment(); err != nil {
		assert.FailNow(t, err.Error())
	}
	assert.Equal(t, 3, inodeOperation.startIndex)
	assert.Equal(t, 4, inodeOperation.endIndex)
}

func TestLookupEkSegmentNoTinyExtent(t *testing.T) {
	inodeOperation := new(MigrateInode)
	inodeOperation.migDirection = SSDToHDDFileMigrate
	inodeOperation.vol = &VolumeInfo{
		GetDpMediumType: func(cluster, volName string, dpId uint64) (mediumType string) {
			return proto.MediumSSDName
		}}
	inodeOperation.extents = append(inodeOperation.extents, proto.ExtentKey{
		PartitionId: uint64(1),
		ExtentId:    1,
		Size:        1 * 1024 * 1024,
	}, proto.ExtentKey{
		PartitionId: uint64(2),
		ExtentId:    100,
		Size:        1 * 1024 * 1024,
	}, proto.ExtentKey{
		PartitionId: uint64(3),
		ExtentId:    3,
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
		ExtentId:    5,
		Size:        1 * 1024 * 1024,
	})
	if err := inodeOperation.LookupEkSegment(); err != nil {
		assert.FailNow(t, err.Error())
	}
	assert.Equal(t, LockExtents, inodeOperation.stage)
	assert.Equal(t, 1, inodeOperation.startIndex)
	assert.Equal(t, 2, inodeOperation.endIndex)
	if err := inodeOperation.LookupEkSegment(); err != nil {
		assert.FailNow(t, err.Error())
	}
	assert.Equal(t, LockExtents, inodeOperation.stage)
	assert.Equal(t, 3, inodeOperation.startIndex)
	assert.Equal(t, 6, inodeOperation.endIndex)
	if err := inodeOperation.LookupEkSegment(); err != nil {
		assert.FailNow(t, err.Error())
	}
	assert.Equal(t, InodeMigStopped, inodeOperation.stage)
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
	logDir := "/cfs/log/migrate"
	log.InitLog(logDir, "test", log.DebugLevel, nil)

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
		log.LogFlush()
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
	mpOp.vol.ControlConfig = &ControlConfig{
		DirectWrite: true,
	}
	ctx := context.Background()
	err := writeRowFileByMountDir(size512M, testFile)
	if err != nil {
		assert.FailNow(t, err.Error())
		return
	}
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
	migEks := subTask.extents[subTask.startIndex:subTask.endIndex]
	replicaCrc, err := subTask.getReplicaDataCRC(migEks)
	if err != nil {
		assert.FailNow(t, err.Error())
	}
	if err = subTask.checkReplicaCRCValid(replicaCrc, replicaCrc); err != nil {
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
	mpOp.vol.ControlConfig = &ControlConfig{
		DirectWrite: true,
	}
	ctx := context.Background()
	err := writeRowFileByMountDir(size512M, testFile)
	if err != nil {
		assert.FailNow(t, err.Error())
		return
	}
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
	subTask.migDirection = SSDToHDDFileMigrate
	_ = subTask.OpenFile()
	subTask.startIndex = 0
	subTask.endIndex = len(subTask.extents)
	err = subTask.ReadFromDataNodeAndWriteToDataNode()
	if err != nil {
		assert.FailNow(t, err.Error())
	}
	migEks := subTask.extents[subTask.startIndex:subTask.endIndex]
	oldReplicaCrc, err := subTask.getReplicaDataCRC(migEks)
	if err != nil {
		assert.FailNow(t, err.Error())
	}
	copyNewEks := make([]proto.ExtentKey, len(subTask.newEks))
	for i, ek := range subTask.newEks {
		copyNewEks[i] = *ek
	}
	newReplicaCrc, err := subTask.getReplicaDataCRC(copyNewEks)
	if err != nil {
		assert.FailNow(t, err.Error())
	}
	if err = subTask.checkReplicaCRCValid(oldReplicaCrc, newReplicaCrc); err != nil {
		assert.FailNow(t, err.Error())
	}
}

func TestCheckS3CrcValid(t *testing.T) {
	tests := []struct {
		name     string
		fileSize int
	}{
		{"1M", size1M},
		{"2M", size1M * 2},
		{"5M", size5M},
		{"6M", size5M + size1M},
		{"128M", size128M},
		{"129M", size128M + size1M},
		{"512M", size512M},
		{"1024M", size512M * 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			readDatanodeWriteS3(tt.fileSize, t)
		})
	}
}

func readDatanodeWriteS3(fileSize int, t *testing.T) {
	setVolForceRow(true)
	defer setVolForceRow(false)

	testFile := fmt.Sprintf("/cfs/mnt/TestReadWriteS3_%v", fileSize)
	file, _ := os.Create(testFile)
	defer func() {
		file.Close()
		os.Remove(testFile)
		log.LogFlush()
	}()
	subTask := createMigrateInode(fileSize, testFile, t)

	mpOp.vol.GetLayerPolicies = func(cluster, volName string) (layerPolicies []interface{}, exist bool) {
		exist = true
		lp := &proto.LayerPolicyInodeATime{
			ClusterId:    clusterName,
			VolumeName:   ltptestVolume,
			OriginRule:   "inodeAccessTime:sec:15:s3",
			TimeType:     1,
			TimeValue:    15,
			TargetMedium: 5,
		}
		layerPolicies = append(layerPolicies, lp)
		return
	}
	if err := subTask.Init(); err != nil {
		assert.FailNow(t, err.Error())
	}
	_ = subTask.OpenFile()
	for i := range subTask.extents {
		subTask.startIndex = i
		subTask.endIndex = i + 1
		err := subTask.ReadFromDataNodeAndWriteToS3()
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		migEks := subTask.extents[subTask.startIndex:subTask.endIndex]
		oldReplicaCrc, err := subTask.getReplicaDataCRC(migEks)
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		newEks := make([]proto.ExtentKey, len(subTask.newEks))
		for i, ek := range subTask.newEks {
			newEks[i] = *ek
		}
		assert.Equal(t, migEks[0].FileOffset, newEks[i].FileOffset)
		assert.Equal(t, migEks[0].PartitionId, newEks[i].PartitionId)
		assert.Equal(t, migEks[0].ExtentId, newEks[i].ExtentId)
		assert.Equal(t, migEks[0].Size, newEks[i].Size)
		newReplicaCrc, err := subTask.getS3DataCRC([]proto.ExtentKey{newEks[i]})
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		if err = subTask.checkReplicaCRCValid(oldReplicaCrc, newReplicaCrc); err != nil {
			assert.FailNow(t, err.Error())
		}
	}
	// test ReadS3AndWriteToDataNode
	subTask.extents = nil
	for _, ek := range subTask.newEks {
		subTask.extents = append(subTask.extents, *ek)
	}
	subTask.newEks = nil
	subTask.migDirection = ReverseS3FileMigrate
	for i := range subTask.extents {
		subTask.startIndex = i
		subTask.endIndex = i + 1
		newEksLen := len(subTask.newEks)
		err := subTask.ReadFromS3AndWriteToDataNode()
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		migEks := subTask.extents[subTask.startIndex:subTask.endIndex]
		oldReplicaCrc, err := subTask.getS3DataCRC(migEks)
		newEks := make([]proto.ExtentKey, len(subTask.newEks))
		for i, ek := range subTask.newEks {
			newEks[i] = *ek
		}
		assert.Equal(t, migEks[0].FileOffset, newEks[newEksLen].FileOffset)
		var newEkSize uint32
		for _, ek := range newEks[newEksLen:] {
			newEkSize += ek.Size
		}
		assert.Equal(t, migEks[0].Size, newEkSize)
		newReplicaCrc, err := subTask.getReplicaDataCRC(newEks[newEksLen:])
		if err != nil {
			assert.FailNow(t, err.Error())
		}
		if err = subTask.checkReplicaCRCValid(oldReplicaCrc, newReplicaCrc); err != nil {
			assert.FailNow(t, err.Error())
		}
	}
}

func createMigrateInode(fileSize int, testFile string, t *testing.T) (inodeOp *MigrateInode) {
	mw, ec, _ := creatHelper(t)
	stat := getFileStat(t, testFile)
	inodeInfo := &proto.InodeInfo{
		Inode: stat.Ino,
	}
	mpOp.vol.NormalDataClient = ec
	mpOp.vol.S3Client = s3.NewS3Client(region, endPoint, accessKey, secretKey, false)
	mpOp.vol.Bucket = ltptestVolume
	mpOp.vol.MetaClient = mw
	mpOp.vol.ControlConfig = &ControlConfig{
		DirectWrite: true,
	}

	ctx := context.Background()
	err := writeRowFileByMountDir(fileSize, testFile)
	if err != nil {
		assert.FailNow(t, err.Error())
		return
	}
	_, _, extents, _ := mpOp.vol.GetMetaClient().GetExtents(ctx, stat.Ino)
	migInode := &proto.InodeExtents{
		Inode:   inodeInfo,
		Extents: extents,
	}
	_, mpID, err := util_sdk.LocateInode(stat.Ino, mpOp.mc, ltptestVolume)
	if err != nil {
		t.Fatalf("LocateInode(%v) info failed, err(%v)", stat.Ino, err)
	}
	mpOp.mpId = mpID
	err = mpOp.GetMpInfo()
	if err != nil {
		assert.FailNow(t, err.Error())
	}
	err = mpOp.GetProfPort()
	if err != nil {
		assert.FailNow(t, err.Error())
	}
	mpOp.task = &proto.Task{TaskType: proto.WorkerTypeInodeMigration}
	inodeOp, err = NewMigrateInode(mpOp, migInode)
	if err != nil {
		assert.FailNow(t, err.Error())
	}
	return
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
	err := inodeOperation.deleteOldExtents(inodeOperation.extents[inodeOperation.startIndex:inodeOperation.endIndex])
	assert.Nil(t, err)
}

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
	inodeOperation.migDirection = CompactFileMigrate
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
	assert.Equal(t, subTask.migDirection, CompactFileMigrate)
	assert.Equal(t, subTask.statisticsInfo.MigEkCnt, uint64(0))
	assert.Equal(t, subTask.statisticsInfo.MigCnt, uint64(0))
	assert.Equal(t, subTask.stage, OpenFile)
}

func TestInitFileMigrate(t *testing.T) {
	testFile := fmt.Sprintf("/cfs/mnt/TestReadWriteS3_%v", size1M)
	file, _ := os.Create(testFile)
	defer func() {
		file.Close()
		os.Remove(testFile)
		log.LogFlush()
	}()
	migInode := createMigrateInode(size1M, testFile, t)

	mpOp.vol.GetLayerPolicies = func(cluster, volName string) (layerPolicies []interface{}, exist bool) {
		exist = true
		lp := &proto.LayerPolicyInodeATime{
			OriginRule:   "inodeAccessTime:xxx:15:s3",
			TimeType:     proto.InodeAccessTimeTypeReserved,
			TimeValue:    15,
			TargetMedium: proto.MediumS3,
		}
		layerPolicies = append(layerPolicies, lp)
		return
	}
	err := migInode.initFileMigrate()
	if err != nil {
		assert.FailNow(t, err.Error())
	}
	assert.Equal(t, NoneFileMigrate, migInode.migDirection)
	assert.Equal(t, InodeMigStopped, migInode.stage)
	mpOp.vol.GetLayerPolicies = func(cluster, volName string) (layerPolicies []interface{}, exist bool) {
		exist = true
		lp := &proto.LayerPolicyInodeATime{
			OriginRule:   "inodeAccessTime:timestamp:0:hdd",
			TimeType:     proto.InodeAccessTimeTypeTimestamp,
			TimeValue:    0,
			TargetMedium: proto.MediumHDD,
		}
		layerPolicies = append(layerPolicies, lp)
		return
	}
	mpOp.vol.GetMigrationConfig = func(cluster, volName string) (volumeConfig proto.MigrationConfig) {
		return proto.MigrationConfig{
			MigrationBack: noMigrationBack,
		}
	}
	err = migInode.initFileMigrate()
	if err != nil {
		assert.FailNow(t, err.Error())
	}
	assert.Equal(t, HDDToSSDFileMigrate, migInode.migDirection)
	assert.Equal(t, InodeMigStopped, migInode.stage)
	mpOp.vol.GetLayerPolicies = func(cluster, volName string) (layerPolicies []interface{}, exist bool) {
		exist = true
		lp := &proto.LayerPolicyInodeATime{
			OriginRule:   "inodeAccessTime:timestamp:32526344848:hdd",
			TimeType:     proto.InodeAccessTimeTypeTimestamp,
			TimeValue:    32526344848,
			TargetMedium: proto.MediumHDD,
		}
		layerPolicies = append(layerPolicies, lp)
		return
	}
	err = migInode.initFileMigrate()
	if err != nil {
		assert.FailNow(t, err.Error())
	}
	assert.Equal(t, SSDToHDDFileMigrate, migInode.migDirection)
	mpOp.vol.GetLayerPolicies = func(cluster, volName string) (layerPolicies []interface{}, exist bool) {
		exist = true
		lp := &proto.LayerPolicyInodeATime{
			OriginRule:   "inodeAccessTime:timestamp:32526344848:s3",
			TimeType:     proto.InodeAccessTimeTypeTimestamp,
			TimeValue:    32526344848,
			TargetMedium: proto.MediumS3,
		}
		layerPolicies = append(layerPolicies, lp)
		return
	}
	err = migInode.initFileMigrate()
	if err != nil {
		assert.FailNow(t, err.Error())
	}
	assert.Equal(t, S3FileMigrate, migInode.migDirection)
	mpOp.vol.GetLayerPolicies = func(cluster, volName string) (layerPolicies []interface{}, exist bool) {
		exist = true
		lp := &proto.LayerPolicyInodeATime{
			OriginRule:   "inodeAccessTime:timestamp:0:s3",
			TimeType:     proto.InodeAccessTimeTypeTimestamp,
			TimeValue:    0,
			TargetMedium: proto.MediumS3,
		}
		layerPolicies = append(layerPolicies, lp)
		return
	}
	err = migInode.initFileMigrate()
	if err != nil {
		assert.FailNow(t, err.Error())
	}
	assert.Equal(t, ReverseS3FileMigrate, migInode.migDirection)
	assert.Equal(t, InodeMigStopped, migInode.stage)
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
	_ = ec.OpenStream(stat.Ino, false, false)

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
	mpOp.vol.ControlConfig = &ControlConfig{
		DirectWrite: true,
	}
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
	mpOp.vol.ControlConfig = &ControlConfig{
		DirectWrite: true,
	}
	subTask, _ := NewMigrateInode(mpOp, cmpInode)
	_ = subTask.Init()
	_ = subTask.OpenFile()
	for i := 0; i < len(extents); i++ {
		subTask.startIndex = i
		subTask.endIndex = len(extents) - 1
		err := subTask.ReadFromDataNodeAndWriteToDataNode()
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
	err := subTask.ReadFromDataNodeAndWriteToDataNode()
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
	_, _, extents, _ := mpOp.vol.GetMetaClient().GetExtents(ctx, stat.Ino)
	cmpInode := &proto.InodeExtents{
		Inode:   inodeInfo,
		Extents: extents,
	}
	// 根据inode获取mpId，再根据mpId获取info
	_, mpID, err := util_sdk.LocateInode(stat.Ino, mpOp.mc, ltptestVolume)
	if err != nil {
		assert.FailNow(t, err.Error())
		return
	}
	mpOp.mpId = mpID
	var mpInfo *proto.MetaPartitionInfo
	cMP := &meta.MetaPartition{PartitionID: mpID}
	mpInfo, err = mpOp.mc.ClientAPI().GetMetaPartition(mpID, ltptestVolume)
	if err != nil {
		assert.FailNow(t, err.Error())
		return
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
		assert.FailNow(t, err.Error())
		return
	}
	mpOp.task = &proto.Task{TaskType: proto.WorkerTypeCompact}
	subTask, _ := NewMigrateInode(mpOp, cmpInode)
	subTask.startIndex = 0
	subTask.endIndex = len(extents)
	_ = subTask.Init()
	_ = subTask.OpenFile()
	subTask.searchMaxExtentIndex(subTask.startIndex, &subTask.endIndex)
	err = subTask.ReadFromDataNodeAndWriteToDataNode()
	if err != nil {
		assert.FailNow(t, err.Error())
		return
	}
	afterCompactEkLen := len(subTask.newEks)
	err = subTask.MetaMergeExtents()
	assert.Nil(t, err)
	_, _, extents, _ = mpOp.vol.GetMetaClient().GetExtents(ctx, stat.Ino)
	assert.Equal(t, len(extents), afterCompactEkLen)
	assert.Equal(t, LookupEkSegment, subTask.stage)
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
	_, mpID, err := util_sdk.LocateInode(stat.Ino, mpOp.mc, ltptestVolume)
	if !assert.NoError(t, err) {
		return
	}
	mpOp.mpId = mpID
	var mpInfo *proto.MetaPartitionInfo
	cMP := &meta.MetaPartition{PartitionID: mpID}
	mpInfo, err = mpOp.mc.ClientAPI().GetMetaPartition(mpID, ltptestVolume)
	if !assert.NoError(t, err) {
		return
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
	_ = subTask.ReadFromDataNodeAndWriteToDataNode()
	// modify file
	_, _, _ = ec.Write(ctx, stat.Ino, 0, []byte{1, 2, 3, 4, 5}, false)
	if err := ec.Flush(ctx, stat.Ino); err != nil {
		t.Fatalf("Flush failed, err(%v)", err)
	}
	err = subTask.MetaMergeExtents()
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
	writeRowFileBySdk(t, ctx, stat.Ino, size128M, mpOp.vol.DataClient)
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
	writeRowFileBySdk(t, ctx, stat.Ino, size10M, mpOp.vol.DataClient)
	cmpInode := &proto.InodeExtents{
		Inode: inodeInfo,
	}
	mpOp.task = &proto.Task{TaskType: proto.WorkerTypeCompact}
	subTask, _ := NewMigrateInode(mpOp, cmpInode)
	isEqual := subTask.compareReplicasInodeEksEqual()
	assert.True(t, isEqual, "")
}

func TestInitExtentMaxIndex(t *testing.T) {
	items := [][]uint64{{1, 1}, {1, 1}, {2, 1}, {9, 10}, {1, 1}, {2, 1}, {3, 1}, {9, 10}, {7, 6}}
	migInode := &MigrateInode{
		extentMaxIndex: make(map[string]int),
	}
	for _, item := range items {
		migInode.extents = append(migInode.extents, proto.ExtentKey{
			PartitionId: item[0],
			ExtentId:    item[1],
		})
	}
	migInode.initExtentMaxIndex()
	assert.Equal(t, 4, migInode.extentMaxIndex[dpIdExtentIdKey(1, 1)])
	assert.Equal(t, 5, migInode.extentMaxIndex[dpIdExtentIdKey(2, 1)])
	assert.Equal(t, 6, migInode.extentMaxIndex[dpIdExtentIdKey(3, 1)])
	assert.Equal(t, 7, migInode.extentMaxIndex[dpIdExtentIdKey(9, 10)])
	assert.Equal(t, 8, migInode.extentMaxIndex[dpIdExtentIdKey(7, 6)])
}

func TestSearchMaxExtentIndex(t *testing.T) {
	items := [][]uint64{{1, 1}, {1, 1}, {2, 1}, {9, 10}, {1, 1}, {2, 1}, {3, 1}, {9, 10}, {7, 6}}
	migInode := &MigrateInode{
		extentMaxIndex: make(map[string]int),
	}
	for _, item := range items {
		migInode.extents = append(migInode.extents, proto.ExtentKey{
			PartitionId: item[0],
			ExtentId:    item[1],
		})
	}
	migInode.initExtentMaxIndex()
	var start, end = 0, 1
	migInode.searchMaxExtentIndex(start, &end)
	assert.Equal(t, 8, end)
	end = 2
	migInode.searchMaxExtentIndex(start, &end)
	assert.Equal(t, 8, end)
	end = 3
	migInode.searchMaxExtentIndex(start, &end)
	assert.Equal(t, 8, end)
	end = 4
	migInode.searchMaxExtentIndex(start, &end)
	assert.Equal(t, 8, end)
	end = 5
	migInode.searchMaxExtentIndex(start, &end)
	assert.Equal(t, 8, end)
	end = 6
	migInode.searchMaxExtentIndex(start, &end)
	assert.Equal(t, 8, end)
	end = 7
	migInode.searchMaxExtentIndex(start, &end)
	assert.Equal(t, 8, end)
	end = 8
	migInode.searchMaxExtentIndex(start, &end)
	assert.Equal(t, 8, end)
	end = 9
	migInode.searchMaxExtentIndex(start, &end)
	assert.Equal(t, 9, end)
}

func TestCheckEkSegmentHasHole(t *testing.T) {
	items := [][]uint64{{1, 1}, {2, 2}, {4, 1}, {9, 10}, {19, 1}, {20, 1}, {22, 5}, {27, 10}, {37, 6}}
	migInode := &MigrateInode{}
	for _, item := range items {
		migInode.extents = append(migInode.extents, proto.ExtentKey{
			FileOffset: item[0],
			Size:       uint32(item[1]),
		})
	}
	var start, end = 0, 1
	fileOffset, hasHold := migInode.checkEkSegmentHasHole(start, end)
	assert.Equal(t, uint64(0), fileOffset)
	assert.Equal(t, false, hasHold)
	start, end = 0, 2
	fileOffset, hasHold = migInode.checkEkSegmentHasHole(start, end)
	assert.Equal(t, uint64(0), fileOffset)
	assert.Equal(t, false, hasHold)
	start, end = 0, 3
	fileOffset, hasHold = migInode.checkEkSegmentHasHole(start, end)
	assert.Equal(t, uint64(0), fileOffset)
	assert.Equal(t, false, hasHold)
	start, end = 0, 4
	fileOffset, hasHold = migInode.checkEkSegmentHasHole(start, end)
	assert.Equal(t, uint64(9), fileOffset)
	assert.Equal(t, true, hasHold)
	start, end = 0, 5
	fileOffset, hasHold = migInode.checkEkSegmentHasHole(start, end)
	assert.Equal(t, uint64(9), fileOffset)
	assert.Equal(t, true, hasHold)
	start, end = 0, 9
	fileOffset, hasHold = migInode.checkEkSegmentHasHole(start, end)
	assert.Equal(t, uint64(9), fileOffset)
	assert.Equal(t, true, hasHold)
}

func TestCheckEkSegmentHasTinyExtent(t *testing.T) {
	items := []uint64{100, 101, 105, 200, 300, 60, 900, 35}
	migInode := &MigrateInode{}
	for _, item := range items {
		migInode.extents = append(migInode.extents, proto.ExtentKey{
			ExtentId: item,
		})
	}
	var start, end = 0, 1
	tinyExtentId, hasTinyExtent := migInode.checkEkSegmentHasTinyExtent(start, end)
	assert.Equal(t, uint64(0), tinyExtentId)
	assert.Equal(t, false, hasTinyExtent)
	start, end = 0, 2
	tinyExtentId, hasTinyExtent = migInode.checkEkSegmentHasTinyExtent(start, end)
	assert.Equal(t, uint64(0), tinyExtentId)
	assert.Equal(t, false, hasTinyExtent)
	start, end = 2, 5
	tinyExtentId, hasTinyExtent = migInode.checkEkSegmentHasTinyExtent(start, end)
	assert.Equal(t, uint64(0), tinyExtentId)
	assert.Equal(t, false, hasTinyExtent)
	start, end = 2, 6
	tinyExtentId, hasTinyExtent = migInode.checkEkSegmentHasTinyExtent(start, end)
	assert.Equal(t, uint64(60), tinyExtentId)
	assert.Equal(t, true, hasTinyExtent)
	start, end = 2, 7
	tinyExtentId, hasTinyExtent = migInode.checkEkSegmentHasTinyExtent(start, end)
	assert.Equal(t, uint64(60), tinyExtentId)
	assert.Equal(t, true, hasTinyExtent)
	start, end = 2, 8
	tinyExtentId, hasTinyExtent = migInode.checkEkSegmentHasTinyExtent(start, end)
	assert.Equal(t, uint64(60), tinyExtentId)
	assert.Equal(t, true, hasTinyExtent)
	start, end = 7, 8
	tinyExtentId, hasTinyExtent = migInode.checkEkSegmentHasTinyExtent(start, end)
	assert.Equal(t, uint64(35), tinyExtentId)
	assert.Equal(t, true, hasTinyExtent)
}

func TestCheckEkSegmentHasS3Extent(t *testing.T) {
	migInode := &MigrateInode{}
	extentCnt := 10
	s3Index1 := 5
	s3Index2 := 9
	for i := 0; i < extentCnt; i++ {
		if i == s3Index1 || i == s3Index2 {
			migInode.extents = append(migInode.extents, proto.ExtentKey{
				CRC: uint32(proto.S3Extent),
			})
		} else {
			migInode.extents = append(migInode.extents, proto.ExtentKey{
				CRC: uint32(proto.CubeFSExtent),
			})
		}
	}
	var start, end = 0, s3Index1
	hasS3Extent := migInode.checkEkSegmentHasS3Extent(start, end)
	assert.Equal(t, false, hasS3Extent)
	start, end = s3Index1, 6
	hasS3Extent = migInode.checkEkSegmentHasS3Extent(start, end)
	assert.Equal(t, true, hasS3Extent)
	start, end = 6, s3Index2
	hasS3Extent = migInode.checkEkSegmentHasS3Extent(start, end)
	assert.Equal(t, false, hasS3Extent)
	start, end = s3Index2, 10
	hasS3Extent = migInode.checkEkSegmentHasS3Extent(start, end)
	assert.Equal(t, true, hasS3Extent)
	start, end = 0, extentCnt
	hasS3Extent = migInode.checkEkSegmentHasS3Extent(start, end)
	assert.Equal(t, true, hasS3Extent)
}

func TestGetDelExtentKeys(t *testing.T) {
	items := [][]uint64{{1, 1}, {1, 1}, {2, 1}, {9, 10}, {1, 1}, {2, 1}, {3, 1}, {9, 10}, {7, 6}}
	migInode := &MigrateInode{
		extentMaxIndex: make(map[string]int),
	}
	for _, item := range items {
		migInode.extents = append(migInode.extents, proto.ExtentKey{
			PartitionId: item[0],
			ExtentId:    item[1],
		})
	}
	migInode.initExtentMaxIndex()
	var start, end = 0, 1
	migInode.searchMaxExtentIndex(start, &end)
	migInode.endIndex = end
	migEks := migInode.extents[start:end]
	canDeleteEks := migInode.getDelExtentKeys(migEks)
	assert.Equal(t, len(migEks), len(canDeleteEks))
	assert.Equal(t, 8, len(canDeleteEks))
	migInode.extents = append(migInode.extents, proto.ExtentKey{
		PartitionId: 1,
		ExtentId:    1,
	})
	start, end = 0, 8
	migInode.endIndex = end
	migEks = migInode.extents[start:end]
	canDeleteEks = migInode.getDelExtentKeys(migEks)
	assert.Equal(t, 5, len(canDeleteEks))
	assert.NotEqual(t, len(canDeleteEks), len(migEks))
	migInode.initExtentMaxIndex()
	migInode.searchMaxExtentIndex(start, &end)
	migInode.endIndex = end
	assert.Equal(t, 10, end)
	migEks = migInode.extents[start:end]
	canDeleteEks = migInode.getDelExtentKeys(migEks)
	assert.Equal(t, len(migEks), len(canDeleteEks))
	assert.Equal(t, 10, len(canDeleteEks))
}

func writeRowFileByMountDir(size int, filePath string) (err error) {
	file, _ := os.Create(filePath)
	defer func() {
		file.Close()
	}()
	var randomBytes []byte
	randomBytes, err = generateRandomBytes(size)
	if err != nil {
		return
	}
	_, _ = file.WriteAt(randomBytes, 0)
	var mBytes []byte
	mBytes, err = generateRandomBytes(10)
	if err != nil {
		return
	}
	for i := 0; i < size; i++ {
		remain := i % (1024 * 1024)
		if remain == 0 {
			_, _ = file.WriteAt(mBytes, int64(i))
		}
	}
	_ = file.Sync()
	return
}

func generateRandomBytes(n int) ([]byte, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return nil, err
	}
	return b, nil
}

func writeRowFileBySdk(t *testing.T, ctx context.Context, inoId uint64, size int, ec *data.ExtentClient) {
	bytes, err := generateRandomBytes(size)
	if err != nil {
		t.Fatalf("generateRandomString failed: size(%v), err(%v)", size, err)
	}
	if err := ec.OpenStream(inoId, false, false); err != nil {
		t.Fatalf("writeRowFileBySdk OpenStream failed: inodeId(%v), err(%v)", inoId, err)
	}
	_, _, err = ec.Write(ctx, inoId, 0, bytes, false)
	if err != nil {
		t.Fatalf("writeRowFileBySdk SyncWrite failed: inodeId(%v), err(%v)", inoId, err)
	}
	mBytes, err := generateRandomBytes(10)
	if err != nil {
		t.Fatalf("generateRandomString failed: size(%v), err(%v)", size, err)
	}
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
		"", false, false, 0, false, 0,
		vv.ConnConfig.ReadTimeoutNs, vv.ConnConfig.WriteTimeoutNs, 0, 0, false, false, false, 0, 0, proto.PersistenceMode_Nil)
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

/*
func TestNewMigrateInode(t *testing.T) {
	mpOp := &MigrateTask{vol: &VolumeInfo{Name: "testVol"}}
	inode := &proto.InodeExtents{Inode: &proto.InodeInfo{Inode: 1}, Extents: []proto.ExtentKey{}}

	// 正常情况
	migInode, err := NewMigrateInode(mpOp, inode)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if migInode == nil {
		t.Fatal("Expected non-nil MigrateInode, got nil")
	}

	// 测试传入的 inode 为 nil
	_, err = NewMigrateInode(mpOp, nil)
	if err == nil {
		t.Fatal("Expected error for nil inode, got nil")
	}

	// 测试传入的 mpOp 为 nil
	_, err = NewMigrateInode(nil, inode)
	if err == nil {
		t.Fatal("Expected error for nil mpOp, got nil")
	}
}

func TestMigrateInode_Init(t *testing.T) {
	mpOp := &MigrateTask{vol: &VolumeInfo{Name: "testVol"}, task: &proto.Task{TaskType: proto.WorkerTypeInodeMigration}}
	inode := &proto.InodeExtents{Inode: &proto.InodeInfo{Inode: 1}, Extents: []proto.ExtentKey{}}
	migInode, _ := NewMigrateInode(mpOp, inode)

	// 正常情况
	err := migInode.Init()
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// 测试 Compact 任务类型
	mpOp.task.TaskType = proto.WorkerTypeCompact
	err = migInode.Init()
	if err != nil {
		t.Fatalf("Expected no error for compact task, got %v", err)
	}

	// 测试 initFileMigrate 返回错误
	mpOp.task.TaskType = proto.WorkerTypeInodeMigration
	migInode.vol = nil // 强制返回错误
	err = migInode.Init()
	if err == nil {
		t.Fatal("Expected error for nil volume, got nil")
	}
}

func TestMigrateInode_LookupEkSegment(t *testing.T) {
	mpOp := &MigrateTask{vol: &VolumeInfo{Name: "testVol"}, task: &proto.Task{TaskType: proto.WorkerTypeInodeMigration}}
	inode := &proto.InodeExtents{Inode: &proto.InodeInfo{Inode: 1}, Extents: []proto.ExtentKey{
		{FileOffset: 0, Size: 1024, PartitionId: 1, ExtentId: 1, CRC: uint32(proto.CubeFSExtent)},
		{FileOffset: 1024, Size: 1024, PartitionId: 1, ExtentId: 2, CRC: uint32(proto.CubeFSExtent)},
	}}
	migInode, _ := NewMigrateInode(mpOp, inode)

	// 正常情况
	err := migInode.LookupEkSegment()
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// 测试无效的 migDirection
	migInode.migDirection = InvalidMigrateDirection
	err = migInode.LookupEkSegment()
	if err == nil {
		t.Fatal("Expected error for invalid migDirection, got nil")
	}

	// 测试 extents 为空
	migInode.extents = []proto.ExtentKey{}
	err = migInode.LookupEkSegment()
	if err == nil {
		t.Fatal("Expected error for empty extents, got nil")
	}

	// 测试 extents 中有空洞
	migInode.extents = []proto.ExtentKey{
		{FileOffset: 0, Size: 1024, PartitionId: 1, ExtentId: 1, CRC: uint32(proto.CubeFSExtent)},
		{FileOffset: 2048, Size: 1024, PartitionId: 1, ExtentId: 2, CRC: uint32(proto.CubeFSExtent)},
	}
	err = migInode.LookupEkSegment()
	if err == nil {
		t.Fatal("Expected error for hole in extents, got nil")
	}
}

func TestMigrateInode_LockExtents(t *testing.T) {
	mpOp := &MigrateTask{vol: &VolumeInfo{Name: "testVol"}, task: &proto.Task{TaskType: proto.WorkerTypeInodeMigration}}
	inode := &proto.InodeExtents{Inode: &proto.InodeInfo{Inode: 1}, Extents: []proto.ExtentKey{
		{FileOffset: 0, Size: 1024, PartitionId: 1, ExtentId: 1, CRC: uint32(proto.CubeFSExtent)},
	}}
	migInode, _ := NewMigrateInode(mpOp, inode)

	// 正常情况
	err := migInode.LockExtents()
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// 测试 ReverseS3FileMigrate 情况
	migInode.migDirection = ReverseS3FileMigrate
	err = migInode.LockExtents()
	if err != nil {
		t.Fatalf("Expected no error for ReverseS3FileMigrate, got %v", err)
	}

	// 测试锁定失败的情况
	migInode.extentClient = nil // 强制返回错误
	err = migInode.LockExtents()
	if err == nil {
		t.Fatal("Expected error for nil extentClient, got nil")
	}
}

func TestMigrateInode_ReadAndWriteDataNode(t *testing.T) {
	mpOp := &MigrateTask{vol: &VolumeInfo{Name: "testVol"}, task: &proto.Task{TaskType: proto.WorkerTypeInodeMigration}}
	inode := &proto.InodeExtents{Inode: &proto.InodeInfo{Inode: 1}, Extents: []proto.ExtentKey{
		{FileOffset: 0, Size: 1024, PartitionId: 1, ExtentId: 1, CRC: uint32(proto.CubeFSExtent)},
	}}
	migInode, _ := NewMigrateInode(mpOp, inode)

	// 正常情况
	err := migInode.ReadAndWriteDataNode()
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// 测试读写失败的情况
	migInode.extentClient = nil // 强制返回错误
	err = migInode.ReadAndWriteDataNode()
	if err == nil {
		t.Fatal("Expected error for nil extentClient, got nil")
	}

	// 测试 CRC 校验失败的情况
	migInode.migDataCrc = 0 // 强制 CRC 校验失败
	err = migInode.ReadAndWriteDataNode()
	if err == nil {
		t.Fatal("Expected error for CRC mismatch, got nil")
	}
}

func TestMigrateInode_MetaMergeExtents(t *testing.T) {
	mpOp := &MigrateTask{vol: &VolumeInfo{Name: "testVol"}, task: &proto.Task{TaskType: proto.WorkerTypeInodeMigration}}
	inode := &proto.InodeExtents{Inode: &proto.InodeInfo{Inode: 1}, Extents: []proto.ExtentKey{
		{FileOffset: 0, Size: 1024, PartitionId: 1, ExtentId: 1, CRC: uint32(proto.CubeFSExtent)},
	}}
	migInode, _ := NewMigrateInode(mpOp, inode)

	// 正常情况
	err := migInode.MetaMergeExtents()
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// 测试合并失败的情况
	migInode.vol.MetaClient = nil // 强制返回错误
	err = migInode.MetaMergeExtents()
	if err == nil {
		t.Fatal("Expected error for nil metaClient, got nil")
	}

	// 测试 extents 为空的情况
	migInode.extents = []proto.ExtentKey{}
	err = migInode.MetaMergeExtents()
	if err == nil {
		t.Fatal("Expected error for empty extents, got nil")
	}
}

func TestMigrateInode_checkEkSegmentHasHole(t *testing.T) {
	mpOp := &MigrateTask{vol: &VolumeInfo{Name: "testVol"}, task: &proto.Task{TaskType: proto.WorkerTypeInodeMigration}}
	inode := &proto.InodeExtents{Inode: &proto.InodeInfo{Inode: 1}, Extents: []proto.ExtentKey{
		{FileOffset: 0, Size: 1024, PartitionId: 1, ExtentId: 1, CRC: uint32(proto.CubeFSExtent)},
		{FileOffset: 2048, Size: 1024, PartitionId: 1, ExtentId: 2, CRC: uint32(proto.CubeFSExtent)},
	}}
	migInode, _ := NewMigrateInode(mpOp, inode)

	// 测试有空洞的情况
	fileOffset, hasHole := migInode.checkEkSegmentHasHole(0, 2)
	if !hasHole {
		t.Fatal("Expected hole, got no hole")
	}
	if fileOffset != 2048 {
		t.Fatalf("Expected fileOffset 2048, got %v", fileOffset)
	}

	// 测试没有空洞的情况
	migInode.extents = []proto.ExtentKey{
		{FileOffset: 0, Size: 1024, PartitionId: 1, ExtentId: 1, CRC: uint32(proto.CubeFSExtent)},
		{FileOffset: 1024, Size: 1024, PartitionId: 1, ExtentId: 2, CRC: uint32(proto.CubeFSExtent)},
	}
	fileOffset, hasHole = migInode.checkEkSegmentHasHole(0, 2)
	if hasHole {
		t.Fatal("Expected no hole, got hole")
	}
}

func TestMigrateInode_checkEkSegmentHasTinyExtent(t *testing.T) {
	mpOp := &MigrateTask{vol: &VolumeInfo{Name: "testVol"}, task: &proto.Task{TaskType: proto.WorkerTypeInodeMigration}}
	inode := &proto.InodeExtents{Inode: &proto.InodeInfo{Inode: 1}, Extents: []proto.ExtentKey{
		{FileOffset: 0, Size: 1024, PartitionId: 1, ExtentId: 1, CRC: uint32(proto.CubeFSExtent)},
		{FileOffset: 1024, Size: 1024, PartitionId: 1, ExtentId: 2, CRC: uint32(proto.CubeFSExtent)},
	}}
	migInode, _ := NewMigrateInode(mpOp, inode)

	// 测试有 TinyExtent 的情况
	tinyExtentId, hasTinyExtent := migInode.checkEkSegmentHasTinyExtent(0, 2)
	if !hasTinyExtent {
		t.Fatal("Expected TinyExtent, got no TinyExtent")
	}
	if tinyExtentId != 2 {
		t.Fatalf("Expected TinyExtentId 2, got %v", tinyExtentId)
	}

	// 测试没有 TinyExtent 的情况
	migInode.extents = []proto.ExtentKey{
		{FileOffset: 0, Size: 1024, PartitionId: 1, ExtentId: 1, CRC: uint32(proto.CubeFSExtent)},
		{FileOffset: 1024, Size: 1024, PartitionId: 1, ExtentId: 2, CRC: uint32(proto.CubeFSExtent)},
	}
	tinyExtentId, hasTinyExtent = migInode.checkEkSegmentHasTinyExtent(0, 2)
	if hasTinyExtent {
		t.Fatal("Expected no TinyExtent, got TinyExtent")
	}
}

func TestMigrateInode_checkEkSegmentHasS3Extent(t *testing.T) {
	mpOp := &MigrateTask{vol: &VolumeInfo{Name: "testVol"}, task: &proto.Task{TaskType: proto.WorkerTypeInodeMigration}}
	inode := &proto.InodeExtents{Inode: &proto.InodeInfo{Inode: 1}, Extents: []proto.ExtentKey{
		{FileOffset: 0, Size: 1024, PartitionId: 1, ExtentId: 1, CRC: uint32(proto.CubeFSExtent)},
		{FileOffset: 1024, Size: 1024, PartitionId: 1, ExtentId: 2, CRC: uint32(proto.S3Extent)},
	}}
	migInode, _ := NewMigrateInode(mpOp, inode)

	// 测试有 S3Extent 的情况
	hasS3Extent := migInode.checkEkSegmentHasS3Extent(0, 2)
	if !hasS3Extent {
		t.Fatal("Expected S3Extent, got no S3Extent")
	}

	// 测试没有 S3Extent 的情况
	migInode.extents = []proto.ExtentKey{
		{FileOffset: 0, Size: 1024, PartitionId: 1, ExtentId: 1, CRC: uint32(proto.CubeFSExtent)},
		{FileOffset: 1024, Size: 1024, PartitionId: 1, ExtentId: 2, CRC: uint32(proto.CubeFSExtent)},
	}
	hasS3Extent = migInode.checkEkSegmentHasS3Extent(0, 2)
	if hasS3Extent {
		t.Fatal("Expected no S3Extent, got S3Extent")
	}
}

func TestMigrateInode_checkEkSegmentHasCubeFSExtent(t *testing.T) {
	mpOp := &MigrateTask{vol: &VolumeInfo{Name: "testVol"}, task: &proto.Task{TaskType: proto.WorkerTypeInodeMigration}}
	inode := &proto.InodeExtents{Inode: &proto.InodeInfo{Inode: 1}, Extents: []proto.ExtentKey{
		{FileOffset: 0, Size: 1024, PartitionId: 1, ExtentId: 1, CRC: uint32(proto.CubeFSExtent)},
		{FileOffset: 1024, Size: 1024, PartitionId: 1, ExtentId: 2, CRC: uint32(proto.S3Extent)},
	}}
	migInode, _ := NewMigrateInode(mpOp, inode)

	// 测试有 CubeFSExtent 的情况
	hasCubeFSExtent := migInode.checkEkSegmentHasCubeFSExtent(0, 2)
	if !hasCubeFSExtent {
		t.Fatal("Expected CubeFSExtent, got no CubeFSExtent")
	}

	// 测试没有 CubeFSExtent 的情况
	migInode.extents = []proto.ExtentKey{
		{FileOffset: 0, Size: 1024, PartitionId: 1, ExtentId: 1, CRC: uint32(proto.S3Extent)},
		{FileOffset: 1024, Size: 1024, PartitionId: 1, ExtentId: 2, CRC: uint32(proto.S3Extent)},
	}
	hasCubeFSExtent = migInode.checkEkSegmentHasCubeFSExtent(0, 2)
	if hasCubeFSExtent {
		t.Fatal("Expected no CubeFSExtent, got CubeFSExtent")
	}
}
*/
