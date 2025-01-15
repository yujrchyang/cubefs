package migration

import (
	"fmt"
	"hash/crc32"
	"math"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/sdk/mysql"
	"github.com/cubefs/cubefs/util/connpool"
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
	if mpOp == nil {
		err = fmt.Errorf("MigrateTask should not be nil")
		return
	}
	if inode == nil {
		err = fmt.Errorf("InodeExtents should not be nil")
		return
	}
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
	if mpOp.vol.VolId == 0 {
		err = fmt.Errorf("new migrate inode volume(%v) volId(%v) mpId(%v) inodeId(%v) volId should not be 0",
			mpOp.vol.Name, mpOp.vol.VolId, mpOp.mpId, inode.Inode.Inode)
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
			if err != nil {
				migInode.UnlockExtents()
			}
		case ReadAndWriteEkData:
			err = migInode.ReadAndWriteData()
			if err != nil {
				migInode.UnlockExtents()
			}
		case MetaMergeExtents:
			err = migInode.MetaMergeExtents()
			if !isNotUnlockExtentErr(err) {
				migInode.UnlockExtents()
			}
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

func isNotUnlockExtentErr(err error) bool {
	return err != nil && (strings.Contains(err.Error(), MetaMergeFailed) || strings.Contains(err.Error(), DeleteOldExtentFailed))
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
	switch migInode.mpOp.task.TaskType {
	case proto.WorkerTypeCompact:
		migInode.migDirection = CompactFileMigrate
		migInode.extentClient = migInode.vol.DataClient
	case proto.WorkerTypeInodeMigration:
		err = migInode.initFileMigrate()
	default:
		err = fmt.Errorf("task type(%v) invaild", migInode.mpOp.task.TaskType)
		return
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
		migInode.extentClient = migInode.vol.WriteToHddDataClient // 读，只写HDD dp
	case HDDToSSDFileMigrate:
		if isMigBack {
			migInode.extentClient = migInode.vol.DataClient // 读，只写SSD dp
		} else {
			// do not migrate back, stop
			migInode.stage = InodeMigStopped
		}
	case S3FileMigrate:
		migInode.extentClient = migInode.vol.DataClient // 只使用读
	case ReverseS3FileMigrate:
		if isMigBack {
			migInode.extentClient = migInode.vol.DataClient // 只使用写
		} else {
			// do not migrate back, stop
			migInode.stage = InodeMigStopped
		}
	default:
		migInode.stage = InodeMigStopped
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
		}
		handleCompactFileMigrate(migInode)
	}()

	srcMediumType := getSrcMediumType(migInode.migDirection)
	if srcMediumType == "" {
		err = fmt.Errorf("migrate direction invalid(%v)", migInode.migDirection)
		return err
	}

	start, end, migSize, migCnt, findFirstSSd := 0, 0, uint64(0), uint64(0), false
	for i := migInode.lastMigEkIndex; i < len(migInode.extents); i++ {
		migInode.lastMigEkIndex = i + 1
		ek := migInode.extents[i]

		candidateMediumType := getCandidateMediumType(migInode, srcMediumType, ek)
		log.LogDebugf("lookup inode(%v) srcMediumType(%v) candidateMediumType(%v) ek(%v)", migInode.name, srcMediumType, candidateMediumType, ek)

		if !isValidExtent(ek, srcMediumType, candidateMediumType) {
			if start < end {
				break
			}
			continue
		}

		if !findFirstSSd {
			findFirstSSd = true
			start = i
		}

		end = i + 1
		if err := checkForHole(migInode, i, start, end); err != nil {
			return err
		}

		migCnt++
		migSize += uint64(ek.Size)
		if migSize > uint64(limitSize*1024*1024) {
			end--
			migInode.lastMigEkIndex--
			migCnt--
			migSize -= uint64(ek.Size)
			break
		}
	}

	if end == 0 {
		migInode.stage = InodeMigStopped
		return
	}

	migInode.searchMaxExtentIndex(start, &end)
	if err := validateEkSegment(migInode, start, end); err != nil {
		return err
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

func handleCompactFileMigrate(migInode *MigrateInode) {
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
		err = migInode.ReadFromDataNodeAndWriteToDataNode()
	case S3FileMigrate:
		err = migInode.ReadFromDataNodeAndWriteToS3()
	case ReverseS3FileMigrate:
		err = migInode.ReadFromS3AndWriteToDataNode()
	default:
		err = fmt.Errorf("migrate direction:%v invalid", migInode.migDirection)
	}
	return
}

func (migInode *MigrateInode) ReadFromDataNodeAndWriteToDataNode() (err error) {
	metrics := exporter.NewModuleTP(UmpKeySuffix(FileMig, migInode.stage.String()))
	defer metrics.Set(err)

	migInode.rwStartTime = time.Now()

	defer migInode.handleError("ReadFromDataNodeAndWriteToDataNode", &err)

	fileOffset := migInode.extents[migInode.startIndex].FileOffset
	totalSize := migInode.extents[migInode.endIndex-1].FileOffset + uint64(migInode.extents[migInode.endIndex-1].Size) - fileOffset

	var (
		writeTotal int
		crc        = crc32.NewIEEE()
		ctx        = context.Background()
	)
	err = migInode.doReadAndWriteTo(ctx, proto.ExtentKey{}, fileOffset, totalSize, crc, &writeTotal)
	if err != nil {
		return err
	}
	migInode.migDataCrc = crc.Sum32()
	log.LogDebugf("ReadFromDataNodeAndWriteToDataNode ino(%v) totalSize(%v) writeTotal(%v) readRange(%v:%v)", migInode.name, totalSize, writeTotal, migInode.startIndex, migInode.endIndex)
	if totalSize != uint64(writeTotal) {
		err = fmt.Errorf("ReadFromDataNodeAndWriteToDataNode compare equal ino(%v) totalSize(%v) but write size(%v)", migInode.name, totalSize, writeTotal)
		return
	}
	return
}

func (migInode *MigrateInode) ReadFromDataNodeAndWriteToS3() (err error) {
	metrics := exporter.NewModuleTP(UmpKeySuffix(FileMig, migInode.stage.String()+"_s3"))
	defer metrics.Set(err)

	if migInode.vol.S3Client == nil {
		err = fmt.Errorf("ReadFromDataNodeAndWriteToS3 inode[%v] migDirection[%v] %v", migInode.name, migInode.migDirection, "s3 client is nil")
		log.LogErrorf(err.Error())
		return
	}

	migInode.rwStartTime = time.Now()

	defer migInode.handleError("ReadFromDataNodeAndWriteToS3", &err)

	offset := migInode.extents[migInode.startIndex].FileOffset
	totalSize := migInode.extents[migInode.endIndex-1].FileOffset + uint64(migInode.extents[migInode.endIndex-1].Size) - offset

	minBlockSize := 5 * unit.MB
	partCount := int(math.Ceil(float64(totalSize) / float64(minBlockSize)))
	chunks := make([][]byte, partCount)
	s3Key := proto.GenS3Key(migInode.vol.ClusterName, migInode.vol.Name, migInode.vol.VolId, migInode.inodeInfo.Inode, migInode.extents[migInode.startIndex].PartitionId, migInode.extents[migInode.startIndex].ExtentId)

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

	if totalSize != uint64(writeTotal) {
		err = fmt.Errorf("ReadFromDataNodeAndWriteToS3 compare equal ino(%v) s3Key(%v) totalSize(%v) but write size(%v)", migInode.name, s3Key, totalSize, writeTotal)
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

	log.LogDebugf("ReadFromDataNodeAndWriteToS3 ino(%v) s3Key(%v) totalSize(%v) writeTotal(%v) readRange(%v:%v) s3Key(%v) newEks(%v)\n",
		migInode.name, s3Key, totalSize, writeTotal, migInode.startIndex, migInode.endIndex, s3Key, migInode.newEks)

	return
}

func (migInode *MigrateInode) ReadFromS3AndWriteToDataNode() (err error) {
	metrics := exporter.NewModuleTP(UmpKeySuffix(FileMig, migInode.stage.String()+"_reverseS3"))
	defer metrics.Set(err)

	if migInode.vol.S3Client == nil {
		err = fmt.Errorf("ReadFromS3AndWriteToDataNode inode[%v] migDirection[%v] %v", migInode.name, migInode.migDirection, "s3 client is nil")
		log.LogErrorf(err.Error())
		return
	}

	migInode.rwStartTime = time.Now()

	defer migInode.handleError("ReadFromS3AndWriteToDataNode", &err)

	fileOffset := migInode.extents[migInode.startIndex].FileOffset
	totalSize := migInode.extents[migInode.endIndex-1].FileOffset + uint64(migInode.extents[migInode.endIndex-1].Size) - fileOffset

	var (
		writeTotal int
		crc        = crc32.NewIEEE()
		ctx        = context.Background()
	)

	for _, ek := range migInode.extents[migInode.startIndex:migInode.endIndex] {
		err = migInode.doReadAndWriteTo(ctx, ek, fileOffset, uint64(ek.Size), crc, &writeTotal)
		if err != nil {
			return err
		}
	}

	migInode.migDataCrc = crc.Sum32()
	log.LogDebugf("ReadFromS3AndWriteToDataNode ino(%v) totalSize(%v) writeTotal(%v) readRange(%v:%v)", migInode.name, totalSize, writeTotal, migInode.startIndex, migInode.endIndex)

	if totalSize != uint64(writeTotal) {
		err = fmt.Errorf("ReadFromS3AndWriteToDataNode compare equal ino(%v) totalSize(%v) but write size(%v)", migInode.name, totalSize, writeTotal)
		return err
	}

	return nil
}

func (migInode *MigrateInode) handleError(action string, err *error) {
	if *err != nil {
		migInode.MigTaskCloseStream()
		migInode.DealActionErr(InodeReadFailedCode, *err)
		log.LogErrorf("%v inode[%v] %v:%v", action, migInode.name, InodeReadAndWriteFailed, (*err).Error())
		return
	}
	migInode.stage = MetaMergeExtents
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
		err = fmt.Errorf("unequal extensions between mp[%v] inode[%v] replicas", migInode.mpOp.mpId, migInode.inodeInfo.Inode)
		return
	}
	if ok, canDeleteExtentKeys := migInode.checkMigExtentCanDelete(migEks); !ok {
		err = fmt.Errorf("checkMigExtentCanDelete ino:%v rawEks length:%v delEks length:%v rawEks(%v) canDeleteExtentKeys(%v)", migInode.name, len(migEks), len(canDeleteExtentKeys), migEks, canDeleteExtentKeys)
		return
	}

	beforeReplicateDataCRC, afterReplicateDataCRC, err := migInode.getMigBeforeAfterDataCRC(migEks, newEks)
	if err != nil {
		return
	}

	if err = migInode.checkReplicaCRCValid(beforeReplicateDataCRC, afterReplicateDataCRC); err != nil {
		return
	}
	// 读写的时间 超过（锁定时间-预留时间），放弃修改ek链
	consumedTime := time.Since(migInode.rwStartTime)
	if consumedTime >= maxConsumeTime*time.Second {
		msg := fmt.Sprintf("before MetaMergeExtents ino(%v) has been consumed time(%v) readRange(%v:%v), but maxConsumeTime(%v)",
			migInode.name, consumedTime, migInode.startIndex, migInode.endIndex, maxConsumeTime*time.Second)
		err = fmt.Errorf(msg)
		return
	}
	err = migInode.vol.MetaClient.InodeMergeExtents_ll(context.Background(), migInode.inodeInfo.Inode, migEks, newEks, proto.FileMigMergeEk)
	if err != nil {
		// meta merge failed, do not unlock extent
		err = fmt.Errorf("%v, err:%v", MetaMergeFailed, err)
		return
	}

	err = migInode.deleteOldExtents(migEks)
	if err != nil {
		migInode.deleteOldExtentsFailedAlarm(err, migEks, newEks)
		// merge success, but delete old extents failed, do not unlock extent
		err = fmt.Errorf("%v, err:%v", DeleteOldExtentFailed, err)
	}
	log.LogDebugf("InodeMergeExtents_ll success ino(%v) oldEks(%v) newEks(%v)", migInode.name, migEks, newEks)
	migInode.addInodeMigrateLog(migEks, newEks)
	migInode.SummaryStatisticsInfo(newEks)
	return
}

func (migInode *MigrateInode) deleteOldExtentsFailedAlarm(err error, migEks, newEks []proto.ExtentKey) {
	warnMsg := fmt.Sprintf("migration cluster(%v) volume(%v) mp(%v) inode(%v) workerIp(%v) delete old extents fail",
		migInode.vol.ClusterName, migInode.vol.Name, migInode.mpOp.mpId, migInode.inodeInfo.Inode, localIp)
	exporter.WarningBySpecialUMPKey(fmt.Sprintf("%v_%v_warning", proto.RoleDataMigWorker, "deleteOldExtents"), warnMsg)
	log.LogErrorf("%v migEks(%v) newEks(%v) err(%v)", warnMsg, migEks, newEks, err)
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
