package migration

import (
	"encoding/json"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"math"
	"net"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/sdk/mysql"
	"github.com/cubefs/cubefs/util/connpool"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/unit"
	"golang.org/x/net/context"
)

const (
	limitSize            = 128              // 迁移的ek链片段数据最大大小MB
	lockTime             = 3 * 24 * 60 * 60 //s
	reserveTime          = 30               //s
	maxConsumeTime       = lockTime - reserveTime
	afterLockSleepTime   = 10 //s
	retryUnlockExtentCnt = 3

	UploadByPartNumOfParallel = 10
)

var (
	gConnPool = connpool.NewConnectPool()
)

type MigrateInode struct {
	inodeInfo      *proto.InodeInfo
	extents        []proto.ExtentKey
	stage          migrateInodeStage
	name           string
	mpOp           *MigrateTask
	extentMaxIndex map[string]int // key:partitionId#extentId
	lastMigEkIndex int
	startIndex     int
	endIndex       int
	buff           []byte
	newEks         []*proto.ExtentKey
	firstMig       bool
	limitSize      uint32
	limitCnt       uint16
	vol            *VolumeInfo
	statisticsInfo MigrateRecord
	lastUpdateTime int64
	rwStartTime    time.Time
	migDataCrc     uint32
	migDirection   MigrateDirection
	extentClient   *data.ExtentClient
	DirectWrite    bool
}

func NewMigrateInode(mpOp *MigrateTask, inode *proto.InodeExtents) (inodeOp *MigrateInode, err error) {
	inodeOp = &MigrateInode{
		mpOp:           mpOp,
		vol:            mpOp.vol,
		inodeInfo:      inode.Inode,
		extents:        inode.Extents,
		extentMaxIndex: make(map[string]int, 0),
		firstMig:       true,
		statisticsInfo: MigrateRecord{MigInodeCnt: 1},
		name:           fmt.Sprintf("%s_%d_%d", mpOp.vol.Name, mpOp.mpId, inode.Inode.Inode),
		DirectWrite:    mpOp.vol.ControlConfig.DirectWrite,
	}
	return
}

func (migInode *MigrateInode) RunOnce() (finished bool, err error) {
	metrics := exporter.NewModuleTP(UmpKeySuffix(FileMig, RunInodeTask))
	defer metrics.Set(err)

	defer func() {
		migInode.vol.DelInodeRunningCnt(migInode.inodeInfo.Inode)
	}()
	if !migInode.vol.AddInodeRunningCnt(migInode.inodeInfo.Inode) {
		return
	}
	defer func() {
		migInode.vol.UpdateVolLastTime()
		migInode.mpOp.UpdateStatisticsInfo(migInode.statisticsInfo)
	}()
	for err == nil {
		if !migInode.vol.IsRunning() {
			log.LogDebugf("inode fileMigrate stop because vol(%v) be stopped, ino(%v) inode.stage(%v)", migInode.vol.Name, migInode.name, migInode.stage)
			migInode.stage = InodeMigStopped
		}
		log.LogDebugf("inode runonce taskType(%v) ino(%v) inode.stage(%v) startEndIndex(%v:%v)", migInode.mpOp.task.TaskType, migInode.name, migInode.stage, migInode.startIndex, migInode.endIndex)
		switch migInode.stage {
		case Init:
			err = migInode.Init()
		case OpenFile:
			err = migInode.OpenFile()
		case LookupEkSegment:
			err = migInode.LookupEkSegment()
		case LockExtents:
			err = migInode.LockExtents()
		case CheckCanMigrate:
			err = migInode.checkCanMigrate()
		case ReadAndWriteEkData:
			err = migInode.ReadAndWriteData()
		case MetaMergeExtents:
			err = migInode.MetaMergeExtents()
		case InodeMigStopped:
			migInode.MigTaskCloseStream()
			finished = true
			return
		default:
			err = nil
			return
		}
		migInode.UpdateTime()
	}
	return
}

func (migInode *MigrateInode) Init() (err error) {
	metrics := exporter.NewModuleTP(UmpKeySuffix(FileMig, migInode.stage.String()))
	defer metrics.Set(err)

	defer func() {
		if err != nil {
			log.LogErrorf("Init inode[%v] %v:%v", migInode.name, InodeInitTaskFailed, err.Error())
			migInode.DealActionErr(InodeInitTaskCode, err)
			return
		}
	}()
	if migInode.mpOp.task.TaskType == proto.WorkerTypeCompact {
		migInode.migDirection = CompactFileMigrate
		migInode.extentClient = migInode.vol.DataClient
	} else {
		err = migInode.initFileMigrate()
	}
	if err != nil || migInode.stage == InodeMigStopped {
		return
	}
	migInode.initExtentMaxIndex()
	migInode.lastMigEkIndex = 0
	migInode.stage = OpenFile
	log.LogDebugf("[inode fileMigrate] init task success ino(%v)", migInode.name)
	return
}

func (migInode *MigrateInode) initFileMigrate() (err error) {
	if migInode.migDirection, err = migInode.mpOp.getInodeMigDirection(migInode.inodeInfo); err != nil {
		return
	}
	if err = migInode.setInodeAttrMaxTime(); err != nil {
		return
	}
	migConfig := migInode.vol.GetMigrationConfig(migInode.vol.ClusterName, migInode.vol.Name)
	isMigBack := migConfig.MigrationBack == migrationBack
	switch migInode.migDirection {
	case SSDToHDDFileMigrate:
		migInode.extentClient = migInode.vol.DataClient
	case HDDToSSDFileMigrate:
		if isMigBack {
			migInode.extentClient = migInode.vol.NormalDataClient
		} else {
			// do not migrate back, stop
			migInode.stage = InodeMigStopped
		}
	case S3FileMigrate:
		migInode.extentClient = migInode.vol.NormalDataClient
	case ReverseS3FileMigrate:
		if isMigBack {
			migInode.extentClient = migInode.vol.NormalDataClient
		} else {
			// do not migrate back, stop
			migInode.stage = InodeMigStopped
		}
	default:
		migInode.stage = InodeMigStopped
	}
	return
}

func (migInode *MigrateInode) OpenFile() (err error) {
	metrics := exporter.NewModuleTP(UmpKeySuffix(FileMig, migInode.stage.String()))
	defer metrics.Set(err)

	defer func() {
		if err != nil {
			log.LogErrorf("OpenFile inode[%v] %v:%v", migInode.name, InodeOpenFailed, err.Error())
			migInode.DealActionErr(InodeOpenFailedCode, err)
			return
		}
		log.LogDebugf("[inode fileMigrate] open file success ino(%v)", migInode.name)
		migInode.stage = LookupEkSegment
	}()
	if err = migInode.extentClient.OpenStream(migInode.inodeInfo.Inode, false, false); err != nil {
		return
	}
	if err = migInode.extentClient.RefreshExtentsCache(context.Background(), migInode.inodeInfo.Inode); err != nil {
		migInode.MigTaskCloseStream()
	}
	return
}

func (migInode *MigrateInode) LookupEkSegment() (err error) {
	metrics := exporter.NewModuleTP(UmpKeySuffix(FileMig, migInode.stage.String()))
	defer metrics.Set(err)

	defer func() {
		if err != nil {
			log.LogErrorf("LookupEkSegment inode[%v] %v:%v", migInode.name, InodeLookupEkFailed, err.Error())
			migInode.MigTaskCloseStream()
			migInode.DealActionErr(InodeLookupEkCode, err)
			return
		}
		if migInode.migDirection == CompactFileMigrate {
			if migInode.endIndex-migInode.startIndex != 1 {
				return
			}
			if migInode.endIndex >= len(migInode.extents) {
				migInode.stage = InodeMigStopped
			} else {
				migInode.stage = LookupEkSegment
			}
		}
	}()
	var (
		start         int
		end           int
		migSize       uint64
		migCnt        uint64
		findFirstSSd  bool
		checkHole     bool
		srcMediumType string
		eks           = migInode.extents
	)
	switch migInode.migDirection {
	case CompactFileMigrate:
		srcMediumType = NoneMediumType
	case HDDToSSDFileMigrate:
		srcMediumType = proto.MediumHDDName
	case SSDToHDDFileMigrate:
		srcMediumType = proto.MediumSSDName
	case S3FileMigrate:
		srcMediumType = NoneMediumType
	case ReverseS3FileMigrate:
		srcMediumType = proto.MediumS3Name
	default:
		err = fmt.Errorf("migrate direction invalid(%v)", migInode.migDirection)
		return err
	}
	for i := migInode.lastMigEkIndex; i < len(migInode.extents); i++ {
		migInode.lastMigEkIndex = i + 1
		ek := eks[i]
		var candidateMediumType string
		if srcMediumType != NoneMediumType {
			switch srcMediumType {
			case proto.MediumS3Name:
				candidateMediumType = strings.ToLower(proto.ExtentType(ek.CRC).String())
			default:
				candidateMediumType = migInode.vol.GetDpMediumType(migInode.vol.ClusterName, migInode.vol.Name, ek.PartitionId)
				candidateMediumType = strings.ToLower(candidateMediumType)
			}
		}
		log.LogDebugf("lookup inode(%v) srcMediumType(%v) candidateMediumType(%v) ek(%v)", migInode.name, srcMediumType, candidateMediumType, ek)
		if !proto.IsTinyExtent(ek.ExtentId) && ((srcMediumType == NoneMediumType && ek.CRC == uint32(proto.CubeFSExtent)) || candidateMediumType == srcMediumType) {
			if !findFirstSSd {
				findFirstSSd = true
				start = i
			} else {
				checkHole = true
			}
			end = i + 1
			// check the hole
			if checkHole && end <= len(migInode.extents) {
				if eks[i-1].FileOffset+uint64(eks[i-1].Size) < ek.FileOffset {
					err = fmt.Errorf("lookup ino:%v fileOffset:%v migType:%v has a hole", migInode.name, ek.FileOffset, migInode.migDirection)
					return
				}
			}
			migCnt++
			migSize += uint64(ek.Size)
			if migSize > uint64(limitSize*1024*1024) {
				end -= 1
				migInode.lastMigEkIndex -= 1
				migCnt--
				migSize -= uint64(ek.Size)
				break
			}
		} else {
			if start < end {
				break
			}
		}
	}
	if end == 0 {
		migInode.stage = InodeMigStopped
		return
	}
	migInode.searchMaxExtentIndex(start, &end)
	if fileOffset, hasHole := migInode.checkEkSegmentHasHole(start, end); hasHole {
		err = fmt.Errorf("checkEkSegmentHasHole ino:%v fileOffset:%v migDirection:%v has a hole", migInode.name, fileOffset, migInode.migDirection)
		return
	}
	if tinyExtentId, hasTinyExtent := migInode.checkEkSegmentHasTinyExtent(start, end); hasTinyExtent {
		err = fmt.Errorf("checkEkSegmentHasTinyExtent ino:%v tinyExtentId:%v migDirection:%v can't have tiny extent", migInode.name, tinyExtentId, migInode.migDirection)
		return
	}
	if migInode.migDirection != ReverseS3FileMigrate && migInode.checkEkSegmentHasS3Extent(start, end) {
		err = fmt.Errorf("checkEkSegmentHasS3Extent ino:%v ek start:%v end:%v migDirection:%v can't have s3 extent", migInode.name, start, end, migInode.migDirection)
		return
	}
	if migInode.migDirection == ReverseS3FileMigrate && migInode.checkEkSegmentHasCubeFSExtent(start, end) {
		err = fmt.Errorf("checkEkSegmentHasCubeFSExtent ino:%v ek start:%v end:%v migDirection:%v can't have cubeFS extent", migInode.name, start, end, migInode.migDirection)
		return
	}
	migInode.startIndex = start
	migInode.endIndex = end

	if migInode.startIndex >= migInode.endIndex {
		migInode.stage = InodeMigStopped
	} else {
		migInode.stage = LockExtents
		log.LogDebugf("LookupEkSegment ino:%v startIndex:%v endIndex:%v migType:%v", migInode.name, migInode.startIndex, migInode.endIndex, migInode.migDirection)
	}
	return
}

func (migInode *MigrateInode) LockExtents() (err error) {
	metrics := exporter.NewModuleTP(UmpKeySuffix(FileMig, migInode.stage.String()))
	defer metrics.Set(err)

	defer func() {
		if err != nil {
			log.LogErrorf("LockExtents inode[%v] %v:%v", migInode.name, InodeLockExtentFailed, err.Error())
			migInode.MigTaskCloseStream()
			migInode.DealActionErr(InodeLockExtentFailedCode, err)
			return
		}
	}()
	if migInode.migDirection == ReverseS3FileMigrate {
		// 从s3回迁到datanode不需要加锁
		migInode.stage = CheckCanMigrate
		return
	}
	willLockedEks := migInode.extents[migInode.startIndex:migInode.endIndex]
	err = migInode.extentClient.LockExtent(context.Background(), willLockedEks, lockTime)
	log.LogInfof("LockExtents inode[%v] willLockedEks:%v lockTime:%v err:%v", migInode.name, willLockedEks, lockTime, err)
	if err == nil {
		// extent锁定后延迟
		time.Sleep(afterLockSleepTime * time.Second)
		migInode.stage = CheckCanMigrate
	}
	return
}

func (migInode *MigrateInode) UnlockExtents() {
	var err error
	defer func() {
		if err != nil {
			log.LogErrorf("UnlockExtents inode[%v] %v:%v", migInode.name, InodeUnlockExtentFailed, err.Error())
			migInode.DealActionErr(InodeLockExtentFailedCode, err)
			return
		}
	}()
	if migInode.migDirection == ReverseS3FileMigrate {
		return
	}
	willUnlockedEks := migInode.extents[migInode.startIndex:migInode.endIndex]
	for i := 0; i < retryUnlockExtentCnt; i++ {
		if err = migInode.extentClient.UnlockExtent(context.Background(), willUnlockedEks); err != nil {
			continue
		}
		break
	}
	log.LogInfof("UnlockExtents inode[%v] willUnlockedEks:%v err:%v", migInode.name, willUnlockedEks, err)
	return
}

func (migInode *MigrateInode) checkCanMigrate() (err error) {
	metrics := exporter.NewModuleTP(UmpKeySuffix(FileMig, migInode.stage.String()))
	defer metrics.Set(err)

	if migInode.mpOp.task.TaskType != proto.WorkerTypeInodeMigration {
		migInode.stage = ReadAndWriteEkData
		return
	}

	defer func() {
		if err != nil {
			migInode.UnlockExtents()
			migInode.MigTaskCloseStream()
			migInode.DealActionErr(InodeCheckInodeFailedCode, err)
			log.LogErrorf("checkCanMigrate inode[%v] %v:%v", migInode.name, InodeCheckInodeFailed, err.Error())
			return
		}
		migInode.stage = ReadAndWriteEkData
	}()
	var (
		inodeInfo    *proto.InodeInfo
		migDirection MigrateDirection
	)
	if inodeInfo, err = migInode.getInodeInfo(); err != nil {
		return
	}
	if err = migInode.mpOp.getInodeInfoMaxTime(inodeInfo); err != nil {
		return
	}
	if migDirection, err = migInode.mpOp.getInodeMigDirection(inodeInfo); err != nil {
		return
	}
	if migDirection != migInode.migDirection {
		err = fmt.Errorf("inode cannot migrate, because different migrate direction init migDirection:%v now migDirection:%v atime:%v mtime:%v", migInode.migDirection, migDirection, inodeInfo.AccessTime, inodeInfo.ModifyTime)
		return
	}
	return
}

func (migInode *MigrateInode) ReadAndWriteData() (err error) {
	switch migInode.migDirection {
	case SSDToHDDFileMigrate, HDDToSSDFileMigrate, CompactFileMigrate:
		err = migInode.ReadAndWriteDataNode()
	case S3FileMigrate:
		err = migInode.ReadDataNodeAndWriteToS3()
	case ReverseS3FileMigrate:
		err = migInode.ReadS3AndWriteToDataNode()
	default:
		err = fmt.Errorf("migrate direction:%v invalid", migInode.migDirection)
	}
	return
}

func (migInode *MigrateInode) ReadDataNodeAndWriteToS3() (err error) {
	metrics := exporter.NewModuleTP(UmpKeySuffix(FileMig, migInode.stage.String()+"_s3"))
	defer metrics.Set(err)

	if migInode.vol.S3Client == nil {
		err = fmt.Errorf("ReadDataNodeAndWriteToS3 inode[%v] migDirection[%v] %v", migInode.name, migInode.migDirection, "s3 client is nil")
		log.LogErrorf(err.Error())
		return
	}

	migInode.rwStartTime = time.Now()

	defer migInode.handleError("ReadDataNodeAndWriteToS3", &err)

	offset := migInode.extents[migInode.startIndex].FileOffset
	totalSize := migInode.extents[migInode.endIndex-1].FileOffset + uint64(migInode.extents[migInode.endIndex-1].Size) - offset

	minBlockSize := 5 * unit.MB
	partCount := int(math.Ceil(float64(totalSize) / float64(minBlockSize)))
	chunks := make([][]byte, partCount)
	s3Key := proto.GenS3Key(migInode.vol.ClusterName, migInode.vol.Name, migInode.inodeInfo.Inode, migInode.extents[migInode.startIndex].PartitionId, migInode.extents[migInode.startIndex].ExtentId)

	if totalSize <= uint64(minBlockSize) {
		if err = migInode.readAndWriteSmallDataToS3(offset, totalSize, s3Key, chunks); err != nil {
			return
		}
	} else {
		if err = migInode.readAndWriteLargeDataToS3(offset, totalSize, minBlockSize, partCount, s3Key, chunks); err != nil {
			return
		}
	}

	var writeTotal int64
	if writeTotal, err = migInode.vol.S3Client.GetObjectContentLength(context.Background(), migInode.vol.Bucket, s3Key); err != nil {
		return
	}

	newEk := &proto.ExtentKey{
		FileOffset:   offset,
		PartitionId:  migInode.extents[migInode.startIndex].PartitionId,
		ExtentId:     migInode.extents[migInode.startIndex].ExtentId,
		ExtentOffset: 0,
		Size:         uint32(totalSize),
		CRC:          uint32(proto.S3Extent),
	}
	migInode.newEks = append(migInode.newEks, newEk)

	crc := crc32.NewIEEE()
	for _, chunk := range chunks {
		_, _ = crc.Write(chunk)
	}
	migInode.migDataCrc = crc.Sum32()

	log.LogDebugf("ReadDataNodeAndWriteToS3 ino(%v) s3Key(%v) totalSize(%v) writeTotal(%v) readRange(%v:%v) s3Key(%v) newEks(%v)\n",
		migInode.name, s3Key, totalSize, writeTotal, migInode.startIndex, migInode.endIndex, s3Key, migInode.newEks)
	if totalSize != uint64(writeTotal) {
		err = fmt.Errorf("ReadDataNodeAndWriteToS3 compare equal ino(%v) s3Key(%v) totalSize(%v) but write size(%v)", migInode.name, s3Key, totalSize, writeTotal)
		return
	}

	return
}

func (migInode *MigrateInode) readAndWriteSmallDataToS3(offset, totalSize uint64, s3Key string, chunks [][]byte) error {
	ctx := context.Background()
	buff := make([]byte, totalSize)
	readN, _, err := migInode.extentClient.Read(ctx, migInode.inodeInfo.Inode, buff, offset, int(totalSize))
	if err != nil && err != io.EOF {
		return err
	}
	if readN <= 0 {
		return fmt.Errorf("ReadDataNodeAndWriteToS3 read small data from datanode extent ino(%v) s3Key(%v), totalSize(%v), readN(%v), readOffset(%v), readSize(%v)",
			migInode.name, s3Key, totalSize, readN, offset, totalSize)
	}
	if err = migInode.vol.S3Client.PutObject(ctx, migInode.vol.Bucket, s3Key, buff[:readN]); err != nil {
		return err
	}
	chunks[0] = buff[:readN]
	log.LogDebugf("ReadDataNodeAndWriteToS3 write small data ino(%v) s3Key(%v), totalSize(%v), readN(%v), readOffset(%v), readSize(%v)",
		migInode.name, s3Key, totalSize, readN, offset, readN)
	return nil
}

func (migInode *MigrateInode) readAndWriteLargeDataToS3(offset, totalSize uint64, minBlockSize, partCount int, s3Key string, chunks [][]byte) error {
	ctx := context.Background()
	chunksMutex := sync.Mutex{}
	err := migInode.vol.S3Client.UploadByPart(ctx, migInode.vol.Bucket, s3Key, partCount, UploadByPartNumOfParallel, func(index int) (data []byte, err error) {
		readSize := minBlockSize
		if index == partCount-1 {
			readSize = int(totalSize - uint64(index*minBlockSize))
		}
		readOffset := offset + uint64(index*minBlockSize)
		buff := make([]byte, readSize)
		readN, _, err := migInode.extentClient.Read(ctx, migInode.inodeInfo.Inode, buff, readOffset, readSize)
		if err != nil && err != io.EOF {
			return nil, err
		}
		if readN <= 0 {
			return nil, fmt.Errorf("ReadDataNodeAndWriteToS3 read big data from datanode extent ino(%v) s3Key(%v), totalSize(%v) partIndex(%v), readN(%v), readOffset(%v), readSize(%v)",
				migInode.name, s3Key, totalSize, index, readN, readOffset, readSize)
		}
		chunksMutex.Lock()
		chunks[index] = buff[:readN]
		chunksMutex.Unlock()
		log.LogDebugf("ReadDataNodeAndWriteToS3 write big data ino(%v) s3Key(%v), totalSize(%v) partIndex(%v), readN(%v), readOffset(%v), readSize(%v)",
			migInode.name, s3Key, totalSize, index, readN, readOffset, readSize)
		return buff[:readN], nil
	})
	return err
}

func (migInode *MigrateInode) ReadS3AndWriteToDataNode() (err error) {
	metrics := exporter.NewModuleTP(UmpKeySuffix(FileMig, migInode.stage.String()+"_reverseS3"))
	defer metrics.Set(err)

	if migInode.vol.S3Client == nil {
		err = fmt.Errorf("ReadS3AndWriteToDataNode inode[%v] migDirection[%v] %v", migInode.name, migInode.migDirection, "s3 client is nil")
		log.LogErrorf(err.Error())
		return
	}

	migInode.rwStartTime = time.Now()

	defer migInode.handleError("ReadS3AndWriteToDataNode", &err)

	fileOffset := migInode.extents[migInode.startIndex].FileOffset
	totalSize := migInode.extents[migInode.endIndex-1].FileOffset + uint64(migInode.extents[migInode.endIndex-1].Size) - fileOffset

	var (
		writeTotal int
		crc        = crc32.NewIEEE()
		ctx        = context.Background()
	)

	for _, ek := range migInode.extents[migInode.startIndex:migInode.endIndex] {
		err = migInode.ProcessReadAndWriteData(ctx, ek, fileOffset, uint64(ek.Size), crc, &writeTotal)
		if err != nil {
			return err
		}
	}

	migInode.migDataCrc = crc.Sum32()
	log.LogDebugf("ReadS3AndWriteToDataNode ino(%v) totalSize(%v) writeTotal(%v) readRange(%v:%v)", migInode.name, totalSize, writeTotal, migInode.startIndex, migInode.endIndex)

	if totalSize != uint64(writeTotal) {
		err = fmt.Errorf("ReadS3AndWriteToDataNode compare equal ino(%v) totalSize(%v) but write size(%v)", migInode.name, totalSize, writeTotal)
		return err
	}

	return nil
}

func (migInode *MigrateInode) handleError(action string, err *error) {
	if *err != nil {
		migInode.UnlockExtents()
		migInode.MigTaskCloseStream()
		migInode.DealActionErr(InodeReadFailedCode, *err)
		log.LogErrorf("%v inode[%v] %v:%v", action, migInode.name, InodeReadAndWriteFailed, (*err).Error())
		return
	}
	migInode.stage = MetaMergeExtents
}

func (migInode *MigrateInode) ProcessReadAndWriteData(ctx context.Context, ek proto.ExtentKey, fileOffset uint64, size uint64, crc hash.Hash32, writeTotal *int) error {
	var (
		firstWrite = true
		readSize   uint64 = unit.BlockSize
		buff              = make([]byte, readSize)
		writeFileOffset   = fileOffset
		readStartOffset   uint64
		totalSize         uint64
		dp                *data.DataPartition
		newEk             *proto.ExtentKey
	)

	switch migInode.migDirection {
	case ReverseS3FileMigrate:
		readStartOffset, totalSize =  ek.ExtentOffset, uint64(ek.Size)
	default:
		readStartOffset, totalSize =  fileOffset, size
	}

	readOffset := readStartOffset

	for {
		remainingSize := totalSize - (readOffset - readStartOffset)
		if remainingSize <= 0 {
			break
		}

		if remainingSize < readSize {
			readSize = remainingSize
		}

		readN, err := migInode.readData(ctx, ek, readOffset, readSize, buff)
		if err != nil {
			return err
		}

		if readN > 0 {
			_, _ = crc.Write(buff[:readN])
			err = migInode.writeData(ctx, &dp, &newEk, &firstWrite, &writeFileOffset, buff[:readN], writeTotal)
			if err != nil {
				return err
			}

			readOffset += uint64(readN)
		}
	}

	return nil
}

func (migInode *MigrateInode) readData(ctx context.Context, ek proto.ExtentKey, readOffset, readSize uint64, buff []byte) (readN int, err error) {
	if migInode.migDirection == ReverseS3FileMigrate {
		readN, err = migInode.readDataFromS3(ctx, ek.PartitionId, ek.ExtentId, readOffset, readSize, buff)
	} else {
		readN, _, err = migInode.extentClient.Read(ctx, migInode.inodeInfo.Inode, buff, readOffset, int(readSize))
	}
	return
}

func (migInode *MigrateInode) writeData(ctx context.Context, dp **data.DataPartition, newEk **proto.ExtentKey, firstWrite *bool, writeFileOffset *uint64, buff []byte, writeTotal *int) error {
	var (
		writeN            int
		extentWriteOffset int
		err               error
	)

	if *firstWrite {
		*dp, writeN, *newEk, err = migInode.extentClient.SyncWrite(ctx, migInode.inodeInfo.Inode, *writeFileOffset, buff, migInode.DirectWrite)
		if err != nil {
			return err
		}
		if !checkMigDirectionMediumTypeIsMatch(migInode, *dp) {
			return fmt.Errorf("ReadAndWriteDataNode syncWrite dpId(%v) incorrect medium type, volume(%v) mp(%v) inode(%v) migType(%v)",
				(*dp).PartitionID, migInode.vol.Name, migInode.mpOp.mpId, migInode.inodeInfo.Inode, migInode.migDirection)
		}
		migInode.newEks = append(migInode.newEks, *newEk)
		*firstWrite = false
	} else {
		writeN, err = migInode.extentClient.SyncWriteToSpecificExtent(ctx, *dp, migInode.inodeInfo.Inode, *writeFileOffset, extentWriteOffset, buff, int((*newEk).ExtentId), migInode.DirectWrite)
		if err != nil {
			log.LogWarnf("ReadS3AndWriteToDataNode syncWriteToSpecificExtent ino(%v), err(%v)", migInode.name, err)
			*dp, writeN, *newEk, err = migInode.extentClient.SyncWrite(ctx, migInode.inodeInfo.Inode, *writeFileOffset, buff, migInode.DirectWrite)
			extentWriteOffset = 0
			if err != nil {
				return err
			}
			migInode.newEks = append(migInode.newEks, *newEk)
			err = migInode.checkNewEkCountValid()
			if err != nil {
				return err
			}
		} else {
			(*newEk).Size += uint32(writeN)
		}
	}

	*writeFileOffset += uint64(writeN)
	*writeTotal += writeN

	return nil
}


func (migInode *MigrateInode) readDataFromS3(ctx context.Context, partitionId, extentId, readOffset, readSize uint64, buff []byte) (readN int, err error) {
	s3Key := proto.GenS3Key(migInode.vol.ClusterName, migInode.vol.Name, migInode.inodeInfo.Inode, partitionId, extentId)
	readN, err = migInode.vol.S3Client.GetObject(ctx, migInode.vol.Bucket, s3Key, readOffset, readSize, buff)
	return
}

func (migInode *MigrateInode) ReadAndWriteDataNode() (err error) {
	metrics := exporter.NewModuleTP(UmpKeySuffix(FileMig, migInode.stage.String()))
	defer metrics.Set(err)

	migInode.rwStartTime = time.Now()

	defer migInode.handleError("ReadAndWriteDataNode", &err)

	fileOffset := migInode.extents[migInode.startIndex].FileOffset
	totalSize := migInode.extents[migInode.endIndex-1].FileOffset + uint64(migInode.extents[migInode.endIndex-1].Size) - fileOffset

	var (
		writeTotal int
		crc        = crc32.NewIEEE()
		ctx        = context.Background()
	)
	err = migInode.ProcessReadAndWriteData(ctx, proto.ExtentKey{}, fileOffset, totalSize, crc, &writeTotal)
	if err != nil {
		return err
	}
	migInode.migDataCrc = crc.Sum32()
	log.LogDebugf("ReadAndWriteDataNode ino(%v) totalSize(%v) writeTotal(%v) readRange(%v:%v)", migInode.name, totalSize, writeTotal, migInode.startIndex, migInode.endIndex)
	if totalSize != uint64(writeTotal) {
		err = fmt.Errorf("ReadAndWriteDataNode compare equal ino(%v) totalSize(%v) but write size(%v)", migInode.name, totalSize, writeTotal)
		return
	}
	return
}

func (migInode *MigrateInode) MetaMergeExtents() (err error) {
	metrics := exporter.NewModuleTP(UmpKeySuffix(FileMig, migInode.stage.String()))
	defer metrics.Set(err)

	var (
		migEks = migInode.extents[migInode.startIndex:migInode.endIndex]
		newEks = make([]proto.ExtentKey, len(migInode.newEks))
	)
	for i, ek := range migInode.newEks {
		newEks[i] = *ek
	}
	migInode.newEks = nil
	defer func() {
		if err != nil {
			log.LogErrorf("inode[%v] %v:%v", migInode.name, InodeMergeFailed, err.Error())
			migInode.MigTaskCloseStream()
			migInode.DealActionErr(InodeMergeFailedCode, err)
			return
		}
		migInode.stage = LookupEkSegment
	}()
	if !migInode.compareReplicasInodeEksEqual() {
		migInode.UnlockExtents()
		err = fmt.Errorf("unequal extensions between mp[%v] inode[%v] replicas", migInode.mpOp.mpId, migInode.inodeInfo.Inode)
		return
	}
	// 检查迁移的extent是否可以删除
	if ok, canDeleteExtentKeys := migInode.checkMigExtentCanDelete(migEks); !ok {
		migInode.UnlockExtents()
		err = fmt.Errorf("checkMigExtentCanDelete ino:%v rawEks length:%v delEks length:%v rawEks(%v) canDeleteExtentKeys(%v)", migInode.name, len(migEks), len(canDeleteExtentKeys), migEks, canDeleteExtentKeys)
		return
	}
	var (
		beforeReplicateDataCRC []uint32
		afterReplicateDataCRC []uint32
	)
	beforeReplicateDataCRC, afterReplicateDataCRC, err = migInode.getMigBeforeAfterDataCRC(migEks, newEks)
	if err != nil {
		migInode.UnlockExtents()
		return
	}
	// sdk读数据crc、副本迁移前的数据crc、新写数据crc之间比对
	if err = migInode.checkReplicaCRCValid(beforeReplicateDataCRC, afterReplicateDataCRC); err != nil {
		migInode.UnlockExtents()
		return
	}
	// 读写的时间 超过（锁定时间-预留时间），放弃修改ek链
	consumedTime := time.Since(migInode.rwStartTime)
	if consumedTime >= maxConsumeTime*time.Second {
		migInode.UnlockExtents()
		migInode.stage = LookupEkSegment
		log.LogWarnf("before MetaMergeExtents ino(%v) has been consumed time(%v) readRange(%v:%v), but maxConsumeTime(%v)",
			migInode.name, consumedTime, migInode.startIndex, migInode.endIndex, maxConsumeTime*time.Second)
		return
	}
	err = migInode.vol.MetaClient.InodeMergeExtents_ll(context.Background(), migInode.inodeInfo.Inode, migEks, newEks, proto.FileMigMergeEk)
	if err != nil {
		// merge fail, delete new create extents
		//migInode.deleteNewExtents(copyNewEks)
		return
	}
	// merge success, delete old extents
	err = migInode.deleteOldExtents(migEks)
	if err == nil {
		migInode.UnlockExtents()
	} else {
		warnMsg := fmt.Sprintf("migration cluster(%v) volume(%v) mp(%v) inode(%v) workerIp(%v) delete old extents fail",
			migInode.vol.ClusterName, migInode.vol.Name, migInode.mpOp.mpId, migInode.inodeInfo.Inode, localIp)
		exporter.WarningBySpecialUMPKey(fmt.Sprintf("%v_%v_warning", proto.RoleDataMigWorker, "deleteOldExtents"), warnMsg)
		log.LogErrorf("%v migEks(%v) newEks(%v) err(%v)", warnMsg, migEks, newEks, err)
	}
	log.LogDebugf("InodeMergeExtents_ll success ino(%v) oldEks(%v) newEks(%v)", migInode.name, migEks, newEks)
	migInode.addInodeMigrateLog(migEks, newEks)
	migInode.SummaryStatisticsInfo(newEks)
	return
}
func (migInode *MigrateInode) getMigBeforeAfterDataCRC(migEks, newEks []proto.ExtentKey) (oldReplicateDataCRC, newReplicateDataCRC []uint32, err error) {
	switch migInode.migDirection {
	case HDDToSSDFileMigrate, SSDToHDDFileMigrate, CompactFileMigrate:
		if oldReplicateDataCRC, err = migInode.getReplicaDataCRC(migEks); err != nil {
			return
		}
		if newReplicateDataCRC, err = migInode.getReplicaDataCRC(newEks); err != nil {
			return
		}
	case S3FileMigrate:
		if oldReplicateDataCRC, err = migInode.getReplicaDataCRC(migEks); err != nil {
			return
		}
		if newReplicateDataCRC, err = migInode.getS3DataCRC(newEks); err != nil {
			return
		}
	case ReverseS3FileMigrate:
		if oldReplicateDataCRC, err = migInode.getS3DataCRC(migEks); err != nil {
			return
		}
		if newReplicateDataCRC, err = migInode.getReplicaDataCRC(newEks); err != nil {
			return
		}
	}
	return
}

func (migInode *MigrateInode) checkMigExtentCanDelete(migEks []proto.ExtentKey) (ok bool, canDeleteExtentKeys []proto.ExtentKey) {
	canDeleteExtentKeys = migInode.getDelExtentKeys(migEks)
	if len(migEks) == len(canDeleteExtentKeys) {
		ok = true
	}
	return
}

func (migInode *MigrateInode) addInodeMigrateLog(oldEks, newEks []proto.ExtentKey) {
	var err error
	defer func() {
		if r := recover(); r != nil {
			stack := make([]byte, 1024*8)
			length := runtime.Stack(stack, false)
			log.LogWarnf(fmt.Sprintf("addInodeMigrateLog occurred panic: %v: %s\n", err, stack[:length]))
		}
		if err != nil {
			log.LogErrorf("addInodeMigrateLog err:%v", err)
		}
	}()
	var (
		oldEksByte []byte
		newEksByte []byte
	)
	if oldEksByte, err = json.Marshal(oldEks); err != nil {
		return
	}
	if newEksByte, err = json.Marshal(newEks); err != nil {
		return
	}
	err = mysql.AddInodeMigrateLog(migInode.mpOp.task, migInode.inodeInfo.Inode, string(oldEksByte), string(newEksByte), len(oldEks), len(newEks))
	return
}

func (migInode *MigrateInode) SummaryStatisticsInfo(newEks []proto.ExtentKey) {
	migInode.statisticsInfo.MigCnt += 1
	migInode.statisticsInfo.MigEkCnt += uint64(migInode.endIndex - migInode.startIndex)
	migInode.statisticsInfo.NewEkCnt += uint64(len(newEks))
	var migSize uint32
	for _, newEk := range newEks {
		migSize += newEk.Size
	}
	migInode.statisticsInfo.MigSize += uint64(migSize)
}

func (migInode *MigrateInode) compareReplicasInodeEksEqual() bool {
	var (
		wg           sync.WaitGroup
		mu           sync.Mutex
		inodeExtents []*proto.GetExtentsResponse
		members      = migInode.mpOp.mpInfo.Members
	)

	for _, member := range members {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			ipPort := fmt.Sprintf("%v:%v", strings.Split(addr, ":")[0], migInode.mpOp.profPort)
			metaHttpClient := meta.NewMetaHttpClient(ipPort, false)
			getExtentsResp, err := metaHttpClient.GetExtentKeyByInodeId(migInode.mpOp.mpId, migInode.inodeInfo.Inode)
			if err == nil && getExtentsResp != nil {
				mu.Lock()
				inodeExtents = append(inodeExtents, getExtentsResp)
				mu.Unlock()
			} else {
				log.LogErrorf("GetExtentsNoModifyAccessTime ino(%v) err(%v)", migInode.name, err)
			}
		}(member)
	}
	wg.Wait()

	if len(inodeExtents) != len(members) {
		return false
	}

	firstExtents := inodeExtents[0].Extents
	firstSize := inodeExtents[0].Size

	for i := 1; i < len(inodeExtents); i++ {
		if inodeExtents[i].Size != firstSize || len(inodeExtents[i].Extents) != len(firstExtents) || !reflect.DeepEqual(inodeExtents[i].Extents, firstExtents) {
			return false
		}
	}

	return true
}

func (migInode *MigrateInode) getReplicaDataCRC(extentKeys []proto.ExtentKey) (replicateCrc []uint32, err error) {
	defer func() {
		if r := recover(); r != nil {
			stack := make([]byte, 1024*8)
			length := runtime.Stack(stack, false)
			log.LogWarnf(fmt.Sprintf("getReplicaDataCRC occurred panic: %v: %s\n", err, stack[:length]))
			err = fmt.Errorf("getReplicaDataCRC occurred panic:%v", r)
		}
	}()

	var (
		allReplicateEkData [][]byte
		replicateHash32    []hash.Hash32
	)

	for _, ek := range extentKeys {
		offset := ek.ExtentOffset
		totalSize := uint64(ek.Size)
		readOffset := offset
		var readSize uint64

		remainingSize := totalSize
		for remainingSize > 0 {
			readSize = unit.BlockSize
			if remainingSize < readSize {
				readSize = remainingSize
			}

			allReplicateEkData, err = migInode.extentClient.ReadExtentAllHost(context.Background(), migInode.inodeInfo.Inode, ek, int(readOffset), int(readSize))
			if err != nil && err != io.EOF {
				return nil, err
			}

			if len(replicateHash32) == 0 {
				replicateHash32 = make([]hash.Hash32, len(allReplicateEkData))
				for i := range allReplicateEkData {
					replicateHash32[i] = crc32.NewIEEE()
				}
			}

			if len(allReplicateEkData) != len(replicateHash32) {
				return nil, fmt.Errorf("getReplicaDataCRC inode:%v readOffset:%v readSize:%v, allReplicateEkData length is %v but replicateHash32 length is %v err:%v",
					migInode.name, readOffset, readSize, len(allReplicateEkData), len(replicateHash32), err)
			}

			for i, d := range allReplicateEkData {
				if _, err = replicateHash32[i].Write(d); err != nil {
					return nil, err
				}
			}

			readOffset += readSize
			remainingSize -= readSize
		}
	}

	replicateCrc = make([]uint32, 0, len(replicateHash32))
	for _, hash32 := range replicateHash32 {
		replicateCrc = append(replicateCrc, hash32.Sum32())
	}
	return
}

func (migInode *MigrateInode) getS3DataCRC(extentKeys []proto.ExtentKey) (s3DataCrc []uint32, err error) {
	defer func() {
		if r := recover(); r != nil {
			stack := make([]byte, 1024*8)
			length := runtime.Stack(stack, false)
			log.LogWarnf(fmt.Sprintf("getReplicaDataCRC occurred panic: %v: %s\n", err, stack[:length]))
			err = fmt.Errorf("getReplicaDataCRC occurred panic:%v", r)
		}
	}()
	var replicateHash32 = crc32.NewIEEE()
	for _, ek := range extentKeys {
		offset := ek.ExtentOffset
		totalSize := uint64(ek.Size)
		readOffset := offset
		var readSize   uint64

		remainingSize := totalSize
		for remainingSize > 0 {
			readSize = unit.BlockSize
			if remainingSize < readSize {
				readSize = remainingSize
			}
			s3Key := proto.GenS3Key(migInode.vol.ClusterName, migInode.vol.Name, migInode.inodeInfo.Inode, ek.PartitionId, ek.ExtentId)
			buff := make([]byte, readSize)
			var readN int
			readN, err = migInode.vol.S3Client.GetObject(context.Background(), migInode.vol.Bucket, s3Key, readOffset, readSize, buff)
			if err != nil {
				return
			}
			_, err = replicateHash32.Write(buff[:readN])
			if err != nil {
				return
			}
			readOffset += readSize
			remainingSize -= readSize
		}
	}
	s3DataCrc = []uint32{replicateHash32.Sum32()}
	return
}

func (migInode *MigrateInode) checkReplicaCRCValid(oldReplicateCrc, newReplicateCrc []uint32) (err error) {
	switch migInode.migDirection {
	case S3FileMigrate:
		if len(oldReplicateCrc) <= 1 {
			err = fmt.Errorf("old replicate count is %v migDirection:%v", len(oldReplicateCrc), migInode.migDirection)
			return err
		}
		if len(newReplicateCrc) == 0 {
			err = fmt.Errorf("new replicate count is %v migDirection:%v", len(newReplicateCrc), migInode.migDirection)
			return err
		}
	case ReverseS3FileMigrate:
		if len(oldReplicateCrc) == 0 {
			err = fmt.Errorf("old replicate count is %v migDirection:%v", len(oldReplicateCrc), migInode.migDirection)
			return err
		}
		if len(newReplicateCrc) <= 1 {
			err = fmt.Errorf("new replicate count is %v migDirection:%v", len(newReplicateCrc), migInode.migDirection)
			return err
		}
	default:
		if len(oldReplicateCrc) <= 1 {
			err = fmt.Errorf("old replicate count is %v migDirection:%v", len(oldReplicateCrc), migInode.migDirection)
			return err
		}
		if len(newReplicateCrc) <= 1 {
			err = fmt.Errorf("new replicate count is %v migDirection:%v", len(newReplicateCrc), migInode.migDirection)
			return err
		}
	}
	oldCrc0 := oldReplicateCrc[0]
	for i := 1; i < len(oldReplicateCrc); i++ {
		if oldReplicateCrc[i] != oldCrc0 {
			err = fmt.Errorf("old replicate crc no equal migDirection:%v crc0:%v crc%v:%v", migInode.migDirection, oldCrc0, i, oldReplicateCrc[i])
			return err
		}
	}
	if oldCrc0 != migInode.migDataCrc {
		err = fmt.Errorf("old replicate and migDataCrc are not equal migDirection:%v replciateCrc:%v migDataCrc:%v", migInode.migDirection, oldReplicateCrc, migInode.migDataCrc)
		return err
	}
	newCrc0 := newReplicateCrc[0]
	for i := 1; i < len(newReplicateCrc); i++ {
		if newReplicateCrc[i] != newCrc0 {
			err = fmt.Errorf("new replicate crc no equal migDirection:%v crc0:%v crc%v:%v", migInode.migDirection, newCrc0, i, newReplicateCrc[i])
			return err
		}
	}
	if newCrc0 != migInode.migDataCrc {
		err = fmt.Errorf("new replicate and migDataCrc are not equal migDirection:%v replciateCrc:%v migDataCrc:%v", migInode.migDirection, newReplicateCrc, migInode.migDataCrc)
		return err
	}
	return nil
}

func (migInode *MigrateInode) deleteOldExtents(extentKeys []proto.ExtentKey) (err error) {
	if migInode.migDirection == ReverseS3FileMigrate {
		return
	}
	// 检查这些extentKey是否还在后面还未迁移的ek中
	canDeleteExtentKeys := migInode.getDelExtentKeys(extentKeys)
	if len(extentKeys) != len(canDeleteExtentKeys) {
		log.LogWarnf("deleteOldExtents warn ino:%v rawEks length:%v delEks length:%v rawEks(%v) canDeleteExtentKeys(%v)", migInode.name, len(extentKeys), len(canDeleteExtentKeys), extentKeys, canDeleteExtentKeys)
	}
	var (
		dpIdEksMap = make(map[uint64][]proto.ExtentKey)
	)
	for _, metaDelExtentKey := range canDeleteExtentKeys {
		dpId := metaDelExtentKey.PartitionId
		dpIdEksMap[dpId] = append(dpIdEksMap[dpId], metaDelExtentKey)
	}
	for dpId, eks := range dpIdEksMap {
		err = retryDeleteExtents(migInode.mpOp.mc, dpId, eks, migInode.inodeInfo.Inode)
		if err != nil {
			log.LogErrorf("deleteOldExtents ino:%v partitionId:%v extentKeys:%v err:%v", migInode.name, dpId, eks, err)
			return
		}
		log.LogInfof("deleteOldExtents ino:%v partitionId:%v extentKeys:%v success", migInode.name, dpId, eks)
	}
	return
}

func retryDeleteExtents(mc *master.MasterClient, partitionId uint64, eks []proto.ExtentKey, inodeId uint64) (err error) {
	retryNum := 5
	for i := 0; i < retryNum; i++ {
		if err = deleteExtents(mc, partitionId, eks, inodeId); err != nil {
			continue
		}
		break
	}
	return
}

func deleteExtents(mc *master.MasterClient, partitionId uint64, eks []proto.ExtentKey, inodeId uint64) (err error) {
	var partition *proto.DataPartitionInfo
	partition, err = mc.AdminAPI().GetDataPartition("", partitionId)
	if err != nil {
		err = errors.NewErrorf("get data partition:%v, err:%v", partitionId, err)
		return
	}
	var (
		conn *net.TCPConn
	)
	conn, err = gConnPool.GetConnect(partition.Hosts[0])
	defer func() {
		if err != nil {
			gConnPool.PutConnect(conn, true)
		} else {
			gConnPool.PutConnect(conn, false)
		}
	}()
	if err != nil {
		err = errors.NewErrorf("get conn from pool partition:%v, err:%v", partitionId, err)
		return
	}
	dp := &DataPartition{
		PartitionID: partitionId,
		Hosts:       partition.Hosts,
	}
	inodeEks := make([]proto.InodeExtentKey, len(eks))
	for i, ek := range eks {
		if ek.PartitionId != partitionId {
			err = errors.NewErrorf("deleteExtents do batchDelete on partition:%v but unexpect extentKey:%v", partitionId, ek)
			return err
		}
		inodeEks[i] = proto.InodeExtentKey{ExtentKey: proto.ExtentKey{
			FileOffset:   ek.FileOffset,
			PartitionId:  ek.PartitionId,
			ExtentId:     ek.ExtentId,
			ExtentOffset: ek.ExtentOffset,
			Size:         ek.Size,
			CRC:          ek.CRC,
		},
			InodeId: inodeId,
		}
	}
	packet := NewPacketToBatchDeleteExtent(context.Background(), dp, inodeEks)
	if err = packet.WriteToConn(conn, proto.WriteDeadlineTime); err != nil {
		err = errors.NewErrorf("deleteExtents write to dataNode %v, err:%v", packet.GetUniqueLogId(), err)
		return
	}
	if err = packet.ReadFromConn(conn, proto.ReadDeadlineTime*10); err != nil {
		err = errors.NewErrorf("deleteExtents read response from dataNode %v, err:%v", packet.GetUniqueLogId(), err)
		return
	}
	if packet.ResultCode != proto.OpOk {
		err = errors.NewErrorf("deleteExtents %v response: %v", packet.GetUniqueLogId(), packet.GetResultMsg())
	}
	return
}

func (migInode *MigrateInode) getDelExtentKeys(extentKeys []proto.ExtentKey) []proto.ExtentKey {
	extentKeyMap := make(map[string][]proto.ExtentKey)
	for _, ek := range extentKeys {
		key := dpIdExtentIdKey(ek.PartitionId, ek.ExtentId)
		extentKeyMap[key] = append(extentKeyMap[key], ek)
	}
	cannotDeleteExtentMap := make(map[string]bool)
	for _, ek := range migInode.extents[:migInode.startIndex] {
		key := dpIdExtentIdKey(ek.PartitionId, ek.ExtentId)
		cannotDeleteExtentMap[key] = true
	}
	for _, ek := range migInode.extents[migInode.endIndex:] {
		key := dpIdExtentIdKey(ek.PartitionId, ek.ExtentId)
		cannotDeleteExtentMap[key] = true
	}
	var canDeleteExtentKeys []proto.ExtentKey
	for key, eks := range extentKeyMap {
		if !cannotDeleteExtentMap[key] {
			canDeleteExtentKeys = append(canDeleteExtentKeys, eks...)
		}
	}
	return canDeleteExtentKeys
}

func (migInode *MigrateInode) getInodeInfo() (inodeInfo *proto.InodeInfo, err error) {
	ipPort := fmt.Sprintf("%v:%v", strings.Split(migInode.mpOp.leader, ":")[0], migInode.mpOp.profPort)
	metaHttpClient := meta.NewMetaHttpClient(ipPort, false)
	inodeInfo, err = metaHttpClient.GetInodeInfo(migInode.mpOp.mpId, migInode.inodeInfo.Inode)
	return
}

func (migInode *MigrateInode) searchMaxExtentIndex(start int, end *int) {
	if *end < len(migInode.extents) {
		maxIndex := 0
		for _, ek := range migInode.extents[start:*end] {
			key := dpIdExtentIdKey(ek.PartitionId, ek.ExtentId)
			index := migInode.extentMaxIndex[key]
			if index > maxIndex {
				maxIndex = index
			}
		}
		if maxIndex >= *end {
			log.LogDebugf("extentMaxIndex ino:%v before reset start:%v end:%v maxIndex:%v", migInode.name, start, *end, maxIndex)
			*end = maxIndex + 1
			migInode.lastMigEkIndex = *end
		} else {
			log.LogDebugf("extentMaxIndex ino:%v after reset start:%v end:%v maxIndex:%v", migInode.name, start, *end, maxIndex)
			return
		}
		migInode.searchMaxExtentIndex(start, end)
	}
}

func (migInode *MigrateInode) checkEkSegmentHasHole(start, end int) (fileOffset uint64, hasHold bool) {
	if end-start <= 1 {
		return 0, false
	}
	for i, ek := range migInode.extents[start:end] {
		if i < end-start-1 {
			if ek.FileOffset+uint64(ek.Size) < migInode.extents[i+1].FileOffset {
				fileOffset = migInode.extents[i+1].FileOffset
				hasHold = true
				return
			}
		}
	}
	return
}

func (migInode *MigrateInode) checkEkSegmentHasTinyExtent(start, end int) (tinyExtentId uint64, hasTinyExtent bool) {
	for _, ek := range migInode.extents[start:end] {
		if proto.IsTinyExtent(ek.ExtentId) {
			tinyExtentId = ek.ExtentId
			hasTinyExtent = true
			return
		}
	}
	return
}

func (migInode *MigrateInode) checkEkSegmentHasS3Extent(start, end int) (hasS3Extent bool) {
	for _, ek := range migInode.extents[start:end] {
		if ek.CRC == uint32(proto.S3Extent) {
			hasS3Extent = true
			return
		}
	}
	return
}

func (migInode *MigrateInode) checkEkSegmentHasCubeFSExtent(start, end int) (hasCubeFSExtent bool) {
	for _, ek := range migInode.extents[start:end] {
		if ek.CRC == uint32(proto.CubeFSExtent) {
			hasCubeFSExtent = true
			return
		}
	}
	return
}

func (migInode *MigrateInode) DealActionErr(errCode int, err error) {
	if err == nil {
		return
	}
	migInode.statisticsInfo.MigErrCode = errCode
	migInode.statisticsInfo.MigErrCnt += 1
	migInode.statisticsInfo.MigErrMsg = err.Error()
	return
}

func (migInode *MigrateInode) MigTaskCloseStream() {
	metrics := exporter.NewModuleTP(UmpKeySuffix(FileMig, migInode.stage.String()))
	defer metrics.Set(nil)

	if migInode.extentClient == nil {
		return
	}
	if err := migInode.extentClient.CloseStream(context.Background(), migInode.inodeInfo.Inode); err != nil {
		log.LogErrorf("MigTaskCloseStream ino(%v) err(%v)", migInode.name, err)
	}
}

func (migInode *MigrateInode) initExtentMaxIndex() {
	for i, ek := range migInode.extents {
		key := dpIdExtentIdKey(ek.PartitionId, ek.ExtentId)
		migInode.extentMaxIndex[key] = i
	}
}

func dpIdExtentIdKey(partitionId, extentId uint64) string {
	return fmt.Sprintf("%v#%v", partitionId, extentId)
}

func (migInode *MigrateInode) UpdateTime() {
	if time.Now().Unix()-migInode.lastUpdateTime > 10*60 {
		if updateErr := mysql.UpdateTaskUpdateTime(migInode.mpOp.task.TaskId); updateErr != nil {
			if !strings.Contains(updateErr.Error(), "affected rows less then one") {
				log.LogErrorf("UpdateTaskUpdateTime to mysql failed, tasks(%v), err(%v)", migInode.mpOp.task, updateErr)
			}
		}
		migInode.lastUpdateTime = time.Now().Unix()
	}
}

func (migInode *MigrateInode) setInodeAttrMaxTime() (err error) {
	var (
		wg                                          sync.WaitGroup
		mu                                          sync.Mutex
		inodeInfoViews                              []*proto.InodeInfo
		members                                     = migInode.mpOp.mpInfo.Members
		maxAccessTime, maxModifyTime, maxCreateTime proto.CubeFSTime
	)
	for _, member := range members {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			ipPort := fmt.Sprintf("%v:%v", strings.Split(addr, ":")[0], migInode.mpOp.profPort)
			metaHttpClient := meta.NewMetaHttpClient(ipPort, false)
			inodeInfoView, errInternal := metaHttpClient.GetInodeInfo(migInode.mpOp.mpId, migInode.inodeInfo.Inode)
			if errInternal == nil && inodeInfoView != nil {
				mu.Lock()
				inodeInfoViews = append(inodeInfoViews, inodeInfoView)
				mu.Unlock()
			} else {
				err = errInternal
				log.LogErrorf("GetInodeInfo mpId(%v) ino(%v) err(%v)", migInode.mpOp.mpId, migInode.inodeInfo.Inode, errInternal)
			}
		}(member)
	}
	wg.Wait()
	if len(inodeInfoViews) != len(members) {
		return err
	}
	for _, inodeInfoView := range inodeInfoViews {
		if inodeInfoView.AccessTime.After(maxAccessTime) {
			maxAccessTime = inodeInfoView.AccessTime
		}
		if inodeInfoView.ModifyTime.After(maxModifyTime) {
			maxModifyTime = inodeInfoView.ModifyTime
		}
		if inodeInfoView.CreateTime.After(maxCreateTime) {
			maxCreateTime = inodeInfoView.CreateTime
		}
	}
	migInode.inodeInfo.AccessTime = maxAccessTime
	migInode.inodeInfo.ModifyTime = maxModifyTime
	migInode.inodeInfo.CreateTime = maxCreateTime
	return
}

func (migInode *MigrateInode) checkNewEkCountValid() (err error) {
	switch migInode.migDirection {
	case CompactFileMigrate:
		migEksCnt := migInode.endIndex - migInode.startIndex
		if len(migInode.newEks) >= migEksCnt {
			err = fmt.Errorf("ReadAndWriteDataNode new create extent ino(%v) newEks length(%v) is greater than or equal to oldEks length(%v)",
				migInode.name, len(migInode.newEks), migInode)
		}
	}
	return
}

func checkMigDirectionMediumTypeIsMatch(migInode *MigrateInode, dp *data.DataPartition) bool {
	switch migInode.migDirection {
	case SSDToHDDFileMigrate:
		return dp.MediumType == proto.MediumHDDName
	case HDDToSSDFileMigrate:
		return dp.MediumType == proto.MediumSSDName
	case CompactFileMigrate, S3FileMigrate, ReverseS3FileMigrate:
		return true
	}
	return false
}
