package migcore

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

const (
	noMigrationBack = 0
	migrationBack   = 1
)

var (
	gConnPool = connpool.NewConnectPool()
)

type MigInode struct {
	inodeInfo      *proto.InodeInfo
	extents        []proto.ExtentKey
	stage          MigInodeStage
	name           string
	mpId           uint64
	task           MigTask
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
	statisticsInfo MigRecord
	lastUpdateTime int64
	rwStartTime    time.Time
	migDataCrc     uint32
	migDirection   MigDirection
	extentClient   *data.ExtentClient
	DirectWrite    bool
}

func NewMigrateInode(task MigTask, inode *proto.InodeExtents) (inodeOp *MigInode, err error) {
	if task == nil {
		err = fmt.Errorf("task should not be nil")
		return
	}
	if inode == nil {
		err = fmt.Errorf("InodeExtents should not be nil")
		return
	}
	inodeOp = &MigInode{
		//mpOp:           mpOp,
		task:           task,
		mpId:           task.GetMpId(),
		vol:            task.GetVol(),
		inodeInfo:      inode.Inode,
		extents:        inode.Extents,
		extentMaxIndex: make(map[string]int, 0),
		firstMig:       true,
		statisticsInfo: MigRecord{MigInodeCnt: 1},
		name:           fmt.Sprintf("%s_%d_%d", task.GetVol().Name, task.GetMpId(), inode.Inode.Inode),
		DirectWrite:    task.GetVol().ControlConfig.DirectWrite,
	}
	if inodeOp.vol.VolId == 0 {
		err = fmt.Errorf("new migrate inode volume(%v) volId(%v) mpId(%v) inodeId(%v) volId should not be 0",
			inodeOp.vol.Name, inodeOp.vol.VolId, inodeOp.mpId, inode.Inode.Inode)
	}
	return
}

func (mi *MigInode) RunOnce() (finished bool, err error) {
	metrics := exporter.NewModuleTP(UmpKeySuffix(FileMig, RunInodeTask))
	defer metrics.Set(err)

	defer func() {
		mi.vol.DelInodeRunningCnt(mi.inodeInfo.Inode)
	}()
	if !mi.vol.AddInodeRunningCnt(mi.inodeInfo.Inode) {
		return
	}
	defer func() {
		mi.vol.UpdateVolLastTime()
		mi.task.UpdateStatisticsInfo(mi.statisticsInfo)
	}()
	for err == nil {
		if !mi.vol.IsRunning() {
			log.LogDebugf("inode fileMigrate stop because vol(%v) be stopped, ino(%v) inode.stage(%v)", mi.vol.Name, mi.name, mi.stage)
			mi.stage = InodeMigStopped
		}
		log.LogDebugf("inode runonce taskType(%v) ino(%v) inode.stage(%v) startEndIndex(%v:%v)", mi.task.GetTaskType(), mi.name, mi.stage, mi.startIndex, mi.endIndex)
		switch mi.stage {
		case Init:
			err = mi.Init()
		case OpenFile:
			err = mi.OpenFile()
		case LookupEkSegment:
			err = mi.LookupEkSegment()
		case LockExtents:
			err = mi.LockExtents()
		case CheckCanMigrate:
			err = mi.checkCanMigrate()
			if err != nil {
				mi.UnlockExtents()
			}
		case ReadAndWriteEkData:
			err = mi.ReadAndWriteData()
			if err != nil {
				mi.UnlockExtents()
			}
		case MetaMergeExtents:
			err = mi.MetaMergeExtents()
			if !isNotUnlockExtentErr(err) {
				mi.UnlockExtents()
			}
		case InodeMigStopped:
			mi.MigTaskCloseStream()
			finished = true
			return
		default:
			err = nil
			return
		}
		mi.UpdateTime()
	}
	return
}

func isNotUnlockExtentErr(err error) bool {
	return err != nil && (strings.Contains(err.Error(), MetaMergeFailed) || strings.Contains(err.Error(), DeleteOldExtentFailed))
}

func (mi *MigInode) Init() (err error) {
	metrics := exporter.NewModuleTP(UmpKeySuffix(FileMig, mi.stage.String()))
	defer metrics.Set(err)

	defer func() {
		if err != nil {
			log.LogErrorf("Init inode[%v] %v:%v", mi.name, InodeInitTaskFailed, err.Error())
			mi.DealActionErr(InodeInitTaskCode, err)
			return
		}
	}()
	switch mi.task.GetTaskType() {
	case proto.WorkerTypeCompact:
		mi.migDirection = CompactFileMigrate
		mi.extentClient = mi.vol.DataClient
	case proto.WorkerTypeInodeMigration:
		err = mi.initFileMigrate()
	default:
		err = fmt.Errorf("task type(%v) invaild", mi.task.GetTaskType())
		return
	}
	if err != nil || mi.stage == InodeMigStopped {
		return
	}
	mi.initExtentMaxIndex()
	mi.lastMigEkIndex = 0
	mi.stage = OpenFile
	log.LogDebugf("[inode fileMigrate] init task success ino(%v)", mi.name)
	return
}

func (mi *MigInode) OpenFile() (err error) {
	metrics := exporter.NewModuleTP(UmpKeySuffix(FileMig, mi.stage.String()))
	defer metrics.Set(err)

	defer func() {
		if err != nil {
			log.LogErrorf("OpenFile inode[%v] %v:%v", mi.name, InodeOpenFailed, err.Error())
			mi.DealActionErr(InodeOpenFailedCode, err)
			return
		}
		log.LogDebugf("[inode fileMigrate] open file success ino(%v)", mi.name)
		mi.stage = LookupEkSegment
	}()
	if err = mi.extentClient.OpenStream(mi.inodeInfo.Inode, false, false); err != nil {
		return
	}
	if err = mi.extentClient.RefreshExtentsCache(context.Background(), mi.inodeInfo.Inode); err != nil {
		mi.MigTaskCloseStream()
	}
	return
}

func (mi *MigInode) initFileMigrate() (err error) {
	if mi.migDirection, err = mi.task.GetInodeMigDirection(mi.inodeInfo); err != nil {
		return
	}
	if err = mi.setInodeAttrMaxTime(); err != nil {
		return
	}
	migConfig := mi.vol.GetMigrationConfig(mi.vol.ClusterName, mi.vol.Name)
	isMigBack := migConfig.MigrationBack == migrationBack
	switch mi.migDirection {
	case SSDToHDDFileMigrate:
		mi.extentClient = mi.vol.WriteToHddDataClient // 读，只写HDD dp
	case HDDToSSDFileMigrate:
		if isMigBack {
			mi.extentClient = mi.vol.DataClient // 读，只写SSD dp
		} else {
			// do not migrate back, stop
			mi.stage = InodeMigStopped
		}
	case S3FileMigrate:
		mi.extentClient = mi.vol.DataClient // 只使用读
	case ReverseS3FileMigrate:
		if isMigBack {
			mi.extentClient = mi.vol.DataClient // 只使用写
		} else {
			// do not migrate back, stop
			mi.stage = InodeMigStopped
		}
	default:
		mi.stage = InodeMigStopped
	}
	return
}

func (mi *MigInode) LookupEkSegment() (err error) {
	metrics := exporter.NewModuleTP(UmpKeySuffix(FileMig, mi.stage.String()))
	defer metrics.Set(err)

	defer func() {
		if err != nil {
			log.LogErrorf("LookupEkSegment inode[%v] %v:%v", mi.name, InodeLookupEkFailed, err.Error())
			mi.MigTaskCloseStream()
			mi.DealActionErr(InodeLookupEkCode, err)
		}
		handleCompactFileMigrate(mi)
	}()

	srcMediumType := getSrcMediumType(mi.migDirection)
	if srcMediumType == "" {
		err = fmt.Errorf("migrate direction invalid(%v)", mi.migDirection)
		return err
	}

	start, end, migSize, migCnt, findFirstSSd := 0, 0, uint64(0), uint64(0), false
	for i := mi.lastMigEkIndex; i < len(mi.extents); i++ {
		mi.lastMigEkIndex = i + 1
		ek := mi.extents[i]

		candidateMediumType := getCandidateMediumType(mi, srcMediumType, ek)
		log.LogDebugf("lookup inode(%v) srcMediumType(%v) candidateMediumType(%v) ek(%v)", mi.name, srcMediumType, candidateMediumType, ek)

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
		if err := checkForHole(mi, i, start, end); err != nil {
			return err
		}

		migCnt++
		migSize += uint64(ek.Size)
		if migSize > uint64(limitSize*1024*1024) {
			end--
			mi.lastMigEkIndex--
			migCnt--
			migSize -= uint64(ek.Size)
			break
		}
	}

	if end == 0 {
		mi.stage = InodeMigStopped
		return
	}

	mi.searchMaxExtentIndex(start, &end)
	if err = validateEkSegment(mi, start, end); err != nil {
		return err
	}

	mi.startIndex = start
	mi.endIndex = end

	if mi.startIndex >= mi.endIndex {
		mi.stage = InodeMigStopped
	} else {
		mi.stage = LockExtents
		log.LogDebugf("LookupEkSegment ino:%v startIndex:%v endIndex:%v migType:%v", mi.name, mi.startIndex, mi.endIndex, mi.migDirection)
	}

	return
}

func handleCompactFileMigrate(migInode *MigInode) {
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

func (mi *MigInode) LockExtents() (err error) {
	metrics := exporter.NewModuleTP(UmpKeySuffix(FileMig, mi.stage.String()))
	defer metrics.Set(err)

	defer func() {
		if err != nil {
			log.LogErrorf("LockExtents inode[%v] %v:%v", mi.name, InodeLockExtentFailed, err.Error())
			mi.MigTaskCloseStream()
			mi.DealActionErr(InodeLockExtentFailedCode, err)
			return
		}
	}()
	if mi.migDirection == ReverseS3FileMigrate {
		// 从s3回迁到datanode不需要加锁
		mi.stage = CheckCanMigrate
		return
	}
	willLockedEks := mi.extents[mi.startIndex:mi.endIndex]
	err = mi.extentClient.LockExtent(context.Background(), willLockedEks, lockTime)
	log.LogInfof("LockExtents inode[%v] willLockedEks:%v lockTime:%v err:%v", mi.name, willLockedEks, lockTime, err)
	if err == nil {
		// extent锁定后延迟
		time.Sleep(afterLockSleepTime * time.Second)
		mi.stage = CheckCanMigrate
	}
	return
}

func (mi *MigInode) UnlockExtents() {
	var err error
	defer func() {
		if err != nil {
			log.LogErrorf("UnlockExtents inode[%v] %v:%v", mi.name, InodeUnlockExtentFailed, err.Error())
			mi.DealActionErr(InodeLockExtentFailedCode, err)
			return
		}
	}()
	if mi.migDirection == ReverseS3FileMigrate {
		return
	}
	willUnlockedEks := mi.extents[mi.startIndex:mi.endIndex]
	for i := 0; i < retryUnlockExtentCnt; i++ {
		if err = mi.extentClient.UnlockExtent(context.Background(), willUnlockedEks); err != nil {
			continue
		}
		break
	}
	log.LogInfof("UnlockExtents inode[%v] willUnlockedEks:%v err:%v", mi.name, willUnlockedEks, err)
	return
}

func (mi *MigInode) checkCanMigrate() (err error) {
	metrics := exporter.NewModuleTP(UmpKeySuffix(FileMig, mi.stage.String()))
	defer metrics.Set(err)

	if mi.task.GetTaskType() != proto.WorkerTypeInodeMigration {
		mi.stage = ReadAndWriteEkData
		return
	}

	defer func() {
		if err != nil {
			mi.MigTaskCloseStream()
			mi.DealActionErr(InodeCheckInodeFailedCode, err)
			log.LogErrorf("checkCanMigrate inode[%v] %v:%v", mi.name, InodeCheckInodeFailed, err.Error())
			return
		}
		mi.stage = ReadAndWriteEkData
	}()
	var (
		inodeInfo    *proto.InodeInfo
		migDirection MigDirection
	)
	if inodeInfo, err = mi.getInodeInfo(); err != nil {
		return
	}
	if err = mi.task.GetInodeInfoMaxTime(inodeInfo); err != nil {
		return
	}
	if migDirection, err = mi.task.GetInodeMigDirection(inodeInfo); err != nil {
		return
	}
	if migDirection != mi.migDirection {
		err = fmt.Errorf("inode cannot migrate, because different migrate direction init migDirection:%v now migDirection:%v atime:%v mtime:%v", mi.migDirection, migDirection, inodeInfo.AccessTime, inodeInfo.ModifyTime)
		return
	}
	return
}

func (mi *MigInode) ReadAndWriteData() (err error) {
	switch mi.migDirection {
	case SSDToHDDFileMigrate, HDDToSSDFileMigrate, CompactFileMigrate:
		err = mi.ReadFromDataNodeAndWriteToDataNode()
	case S3FileMigrate:
		err = mi.ReadFromDataNodeAndWriteToS3()
	case ReverseS3FileMigrate:
		err = mi.ReadFromS3AndWriteToDataNode()
	default:
		err = fmt.Errorf("migrate direction:%v invalid", mi.migDirection)
	}
	return
}

func (mi *MigInode) ReadFromDataNodeAndWriteToDataNode() (err error) {
	metrics := exporter.NewModuleTP(UmpKeySuffix(FileMig, mi.stage.String()))
	defer metrics.Set(err)

	mi.rwStartTime = time.Now()

	defer mi.handleError("ReadFromDataNodeAndWriteToDataNode", &err)

	fileOffset := mi.extents[mi.startIndex].FileOffset
	totalSize := mi.extents[mi.endIndex-1].FileOffset + uint64(mi.extents[mi.endIndex-1].Size) - fileOffset

	var (
		writeTotal int
		crc        = crc32.NewIEEE()
		ctx        = context.Background()
	)
	err = mi.doReadAndWriteTo(ctx, proto.ExtentKey{}, fileOffset, totalSize, crc, &writeTotal)
	if err != nil {
		return err
	}
	mi.migDataCrc = crc.Sum32()
	log.LogDebugf("ReadFromDataNodeAndWriteToDataNode ino(%v) totalSize(%v) writeTotal(%v) readRange(%v:%v)", mi.name, totalSize, writeTotal, mi.startIndex, mi.endIndex)
	if totalSize != uint64(writeTotal) {
		err = fmt.Errorf("ReadFromDataNodeAndWriteToDataNode compare equal ino(%v) totalSize(%v) but write size(%v)", mi.name, totalSize, writeTotal)
		return
	}
	return
}

func (mi *MigInode) ReadFromDataNodeAndWriteToS3() (err error) {
	metrics := exporter.NewModuleTP(UmpKeySuffix(FileMig, mi.stage.String()+"_s3"))
	defer metrics.Set(err)

	if mi.vol.S3Client == nil {
		err = fmt.Errorf("ReadFromDataNodeAndWriteToS3 inode[%v] migDirection[%v] %v", mi.name, mi.migDirection, "s3 client is nil")
		log.LogErrorf(err.Error())
		return
	}

	mi.rwStartTime = time.Now()

	defer mi.handleError("ReadFromDataNodeAndWriteToS3", &err)

	offset := mi.extents[mi.startIndex].FileOffset
	totalSize := mi.extents[mi.endIndex-1].FileOffset + uint64(mi.extents[mi.endIndex-1].Size) - offset

	minBlockSize := 5 * unit.MB
	partCount := int(math.Ceil(float64(totalSize) / float64(minBlockSize)))
	chunks := make([][]byte, partCount)

	// 使用ek链片段中的第一个normalExtent创建s3 extentKey
	// 如果不存在normalExtent，借助datanode创建一个并且删除掉
	normalExtent, exist := mi.findEkSegmentFirstNormalExtent()
	if !exist {
		log.LogWarnf("ReadFromDataNodeAndWriteToS3 did not find normal extent ino(%v) readRange(%v:%v) migEks(%v)",
			mi.name, mi.startIndex, mi.endIndex, mi.extents[mi.startIndex:mi.endIndex])
		normalExtent, err = mi.createAndDeleteNormalExtentForGenS3Key()
		if err != nil {
			return
		}
	}

	s3Key := proto.GenS3Key(mi.vol.ClusterName, mi.vol.Name, mi.vol.VolId, mi.inodeInfo.Inode, normalExtent.PartitionId, normalExtent.ExtentId)

	if totalSize <= uint64(minBlockSize) {
		if err = mi.readAndWriteSmallDataToS3(offset, totalSize, s3Key, chunks); err != nil {
			return
		}
	} else {
		if err = mi.readAndWriteLargeDataToS3(offset, totalSize, minBlockSize, partCount, s3Key, chunks); err != nil {
			return
		}
	}

	var writeTotal int64
	if writeTotal, err = mi.vol.S3Client.GetObjectContentLength(context.Background(), mi.vol.Bucket, s3Key); err != nil {
		return
	}

	if totalSize != uint64(writeTotal) {
		err = fmt.Errorf("ReadFromDataNodeAndWriteToS3 compare equal ino(%v) s3Key(%v) totalSize(%v) but write size(%v)",
			mi.name, s3Key, totalSize, writeTotal)
		return
	}

	newEk := &proto.ExtentKey{
		FileOffset:   offset,
		PartitionId:  normalExtent.PartitionId,
		ExtentId:     normalExtent.ExtentId,
		ExtentOffset: 0,
		Size:         uint32(totalSize),
		CRC:          uint32(proto.S3Extent),
	}
	mi.newEks = append(mi.newEks, newEk)

	crc := crc32.NewIEEE()
	for _, chunk := range chunks {
		_, _ = crc.Write(chunk)
	}
	mi.migDataCrc = crc.Sum32()

	log.LogDebugf("ReadFromDataNodeAndWriteToS3 ino(%v) s3Key(%v) totalSize(%v) writeTotal(%v) readRange(%v:%v) s3Key(%v) newEks(%v)\n",
		mi.name, s3Key, totalSize, writeTotal, mi.startIndex, mi.endIndex, s3Key, mi.newEks)

	return
}

func (mi *MigInode) createAndDeleteNormalExtentForGenS3Key() (normalExtent proto.ExtentKey, err error) {
	partitionId := mi.extents[mi.startIndex].PartitionId
	extID, err := createNormalExtent(mi.task.GetMasterClient(), mi.vol.Name, partitionId, mi.inodeInfo.Inode)
	if err != nil {
		return
	}
	log.LogDebugf("create normal extent partitionId(%v) extentId(%v)", partitionId, extID)
	// creat extent successfully and delete this extent
	eks := []proto.ExtentKey{
		{PartitionId: partitionId, ExtentId: extID},
	}
	err = retryDeleteExtents(mi.task.GetMasterClient(), mi.vol.Name, partitionId, eks, mi.inodeInfo.Inode)
	if err != nil {
		// delete failed, no impact on migration
		log.LogWarnf("deleteNormalExtent ino(%v) partitionId(%v) extentKeys(%v) err(%v)", mi.name, partitionId, eks, err)
		err = nil
	}
	normalExtent.PartitionId = partitionId
	normalExtent.ExtentId = extID
	return
}

func (mi *MigInode) findEkSegmentFirstNormalExtent() (normalExtent proto.ExtentKey, exist bool) {
	migEks := mi.extents[mi.startIndex:mi.endIndex]
	for _, ek := range migEks {
		if ek.ExtentId == 0 {
			continue
		}
		if !proto.IsTinyExtent(ek.ExtentId) {
			return ek, true
		}
	}
	return proto.ExtentKey{}, false
}

func (mi *MigInode) ReadFromS3AndWriteToDataNode() (err error) {
	metrics := exporter.NewModuleTP(UmpKeySuffix(FileMig, mi.stage.String()+"_reverseS3"))
	defer metrics.Set(err)

	if mi.vol.S3Client == nil {
		err = fmt.Errorf("ReadFromS3AndWriteToDataNode inode[%v] migDirection[%v] %v", mi.name, mi.migDirection, "s3 client is nil")
		log.LogErrorf(err.Error())
		return
	}

	mi.rwStartTime = time.Now()

	defer mi.handleError("ReadFromS3AndWriteToDataNode", &err)

	fileOffset := mi.extents[mi.startIndex].FileOffset
	totalSize := mi.extents[mi.endIndex-1].FileOffset + uint64(mi.extents[mi.endIndex-1].Size) - fileOffset

	var (
		writeTotal int
		crc        = crc32.NewIEEE()
		ctx        = context.Background()
	)

	for _, ek := range mi.extents[mi.startIndex:mi.endIndex] {
		err = mi.doReadAndWriteTo(ctx, ek, fileOffset, uint64(ek.Size), crc, &writeTotal)
		if err != nil {
			return err
		}
	}

	mi.migDataCrc = crc.Sum32()
	log.LogDebugf("ReadFromS3AndWriteToDataNode ino(%v) totalSize(%v) writeTotal(%v) readRange(%v:%v)", mi.name, totalSize, writeTotal, mi.startIndex, mi.endIndex)

	if totalSize != uint64(writeTotal) {
		err = fmt.Errorf("ReadFromS3AndWriteToDataNode compare equal ino(%v) totalSize(%v) but write size(%v)", mi.name, totalSize, writeTotal)
		return err
	}

	return nil
}

func (mi *MigInode) handleError(action string, err *error) {
	if *err != nil {
		mi.MigTaskCloseStream()
		mi.DealActionErr(InodeReadFailedCode, *err)
		log.LogErrorf("%v inode[%v] %v:%v", action, mi.name, InodeReadAndWriteFailed, (*err).Error())
		return
	}
	mi.stage = MetaMergeExtents
}

func (mi *MigInode) MetaMergeExtents() (err error) {
	metrics := exporter.NewModuleTP(UmpKeySuffix(FileMig, mi.stage.String()))
	defer metrics.Set(err)

	var (
		migEks = mi.extents[mi.startIndex:mi.endIndex]
		newEks = make([]proto.ExtentKey, len(mi.newEks))
	)
	for i, ek := range mi.newEks {
		newEks[i] = *ek
	}
	mi.newEks = nil
	defer func() {
		if err != nil {
			log.LogErrorf("inode[%v] %v:%v", mi.name, InodeMergeFailed, err.Error())
			mi.MigTaskCloseStream()
			mi.DealActionErr(InodeMergeFailedCode, err)
			return
		}
		mi.stage = LookupEkSegment
	}()
	if !mi.compareReplicasInodeEksEqual() {
		err = fmt.Errorf("unequal extensions between mp[%v] inode[%v] replicas", mi.mpId, mi.inodeInfo.Inode)
		return
	}
	if ok, canDeleteExtentKeys := mi.checkMigExtentCanDelete(migEks); !ok {
		err = fmt.Errorf("checkMigExtentCanDelete ino:%v rawEks length:%v delEks length:%v rawEks(%v) canDeleteExtentKeys(%v)", mi.name, len(migEks), len(canDeleteExtentKeys), migEks, canDeleteExtentKeys)
		return
	}

	beforeReplicateDataCRC, afterReplicateDataCRC, err := mi.getMigBeforeAfterDataCRC(migEks, newEks)
	if err != nil {
		return
	}

	if err = mi.checkReplicaCRCValid(beforeReplicateDataCRC, afterReplicateDataCRC); err != nil {
		return
	}
	// 读写的时间 超过（锁定时间-预留时间），放弃修改ek链
	consumedTime := time.Since(mi.rwStartTime)
	if consumedTime >= maxConsumeTime*time.Second {
		msg := fmt.Sprintf("before MetaMergeExtents ino(%v) has been consumed time(%v) readRange(%v:%v), but maxConsumeTime(%v)",
			mi.name, consumedTime, mi.startIndex, mi.endIndex, maxConsumeTime*time.Second)
		err = fmt.Errorf(msg)
		return
	}
	err = mi.vol.MetaClient.InodeMergeExtents_ll(context.Background(), mi.inodeInfo.Inode, migEks, newEks, proto.FileMigMergeEk)
	if err != nil {
		// meta merge failed, do not unlock extent
		err = fmt.Errorf("%v, err:%v", MetaMergeFailed, err)
		return
	}

	err = mi.deleteOldExtents(migEks)
	if err != nil {
		mi.deleteOldExtentsFailedAlarm(err, migEks, newEks)
		// merge success, but delete old extents failed, do not unlock extent
		err = fmt.Errorf("%v, err:%v", DeleteOldExtentFailed, err)
	}
	log.LogDebugf("InodeMergeExtents_ll success ino(%v) oldEks(%v) newEks(%v)", mi.name, migEks, newEks)
	mi.addInodeMigrateLog(migEks, newEks)
	mi.SummaryStatisticsInfo(newEks)
	return
}

func (mi *MigInode) deleteOldExtentsFailedAlarm(err error, migEks, newEks []proto.ExtentKey) {
	warnMsg := fmt.Sprintf("migration cluster(%v) volume(%v) mp(%v) inode(%v) workerIp(%v) delete old extents fail",
		mi.vol.ClusterName, mi.vol.Name, mi.mpId, mi.inodeInfo.Inode, mi.task.GetLocalIp())
	exporter.WarningBySpecialUMPKey(fmt.Sprintf("%v_%v_warning", proto.RoleDataMigWorker, "deleteOldExtents"), warnMsg)
	log.LogErrorf("%v migEks(%v) newEks(%v) err(%v)", warnMsg, migEks, newEks, err)
}

func (mi *MigInode) compareReplicasInodeEksEqual() bool {
	var (
		wg           sync.WaitGroup
		mu           sync.Mutex
		inodeExtents []*proto.GetExtentsResponse
		members      = mi.task.GetMpInfo().Members
	)

	for _, member := range members {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			ipPort := fmt.Sprintf("%v:%v", strings.Split(addr, ":")[0], mi.task.GetProfPort())
			metaHttpClient := meta.NewMetaHttpClient(ipPort, false)
			getExtentsResp, err := metaHttpClient.GetExtentKeyByInodeId(mi.mpId, mi.inodeInfo.Inode)
			if err == nil && getExtentsResp != nil {
				mu.Lock()
				inodeExtents = append(inodeExtents, getExtentsResp)
				mu.Unlock()
			} else {
				fmt.Printf("getExtentsResp:%v\n", err)
				log.LogErrorf("GetExtentsNoModifyAccessTime ino(%v) err(%v)", mi.name, err)
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

func (mi *MigInode) MigTaskCloseStream() {
	metrics := exporter.NewModuleTP(UmpKeySuffix(FileMig, mi.stage.String()))
	defer metrics.Set(nil)

	if mi.extentClient == nil {
		return
	}
	if err := mi.extentClient.CloseStream(context.Background(), mi.inodeInfo.Inode); err != nil {
		log.LogErrorf("MigTaskCloseStream ino(%v) err(%v)", mi.name, err)
	}
}

func dpIdExtentIdKey(partitionId, extentId uint64) string {
	return fmt.Sprintf("%v#%v", partitionId, extentId)
}

func (mi *MigInode) UpdateTime() {
	if time.Now().Unix()-mi.lastUpdateTime > 10*60 {
		if updateErr := mysql.UpdateTaskUpdateTime(mi.task.GetTaskId()); updateErr != nil {
			if !strings.Contains(updateErr.Error(), "affected rows less then one") {
				log.LogErrorf("UpdateTaskUpdateTime to mysql failed, tasks(%v), err(%v)", mi.task, updateErr)
			}
		}
		mi.lastUpdateTime = time.Now().Unix()
	}
}
