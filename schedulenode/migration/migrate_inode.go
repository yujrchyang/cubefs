package migration

import (
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"net"
	"reflect"
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
	limitSize            = 128    // 迁移的ek链片段数据最大大小MB
	lockTime             = 5 * 60 //s
	reserveTime          = 30     //s
	maxConsumeTime       = lockTime - reserveTime
	afterLockSleepTime   = 10 //s
	retryUnlockExtentCnt = 3
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
	}
	return
}

func (migInode *MigrateInode) RunOnce() (finished bool, err error) {
	metrics := exporter.NewModuleTP(UmpKeySuffix(FileMig, RunInodeTask))
	defer metrics.Set(err)

	defer func() {
		migInode.vol.DelInodeRunningCnt()
	}()
	if !migInode.vol.AddInodeRunningCnt() {
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
			err = migInode.ReadAndWriteEkData()
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
			log.LogErrorf("inode[%v] %v:%v", migInode.name, InodeInitTaskFailed, err.Error())
			migInode.DealActionErr(InodeInitTaskCode, err)
			return
		}
	}()
	var isMigBack bool
	if migInode.mpOp.task.TaskType == proto.WorkerTypeInodeMigration {
		var migDir MigrateDirection
		if migDir, err = migInode.mpOp.getInodeMigDirection(migInode.inodeInfo); err != nil {
			return
		}
		migInode.migDirection = migDir
		if err = migInode.setInodeAttrMaxTime(); err != nil {
			return
		}
		isMigBack = migInode.vol.GetMigrationConfig(migInode.vol.ClusterName, migInode.vol.Name).MigrationBack == migrationBack
	} else {
		migInode.migDirection = COMPACTFILEMIGRATE
	}
	if migInode.migDirection == HDDTOSSDFILEMIGRATE {
		if isMigBack {
			migInode.extentClient = migInode.vol.NormalDataClient
		} else {
			migInode.stage = InodeMigStopped
			return
		}
	} else {
		migInode.extentClient = migInode.vol.DataClient
	}
	migInode.initExtentMaxIndex()
	migInode.lastMigEkIndex = 0
	log.LogDebugf("[inode fileMigrate] init task success ino(%v)", migInode.name)
	migInode.stage = OpenFile
	return
}

func (migInode *MigrateInode) OpenFile() (err error) {
	metrics := exporter.NewModuleTP(UmpKeySuffix(FileMig, migInode.stage.String()))
	defer metrics.Set(err)

	defer func() {
		if err != nil {
			log.LogErrorf("inode[%v] %v:%v", migInode.name, InodeOpenFailed, err.Error())
			migInode.DealActionErr(InodeOpenFailedCode, err)
			return
		}
		log.LogDebugf("[inode fileMigrate] open file success ino(%v)", migInode.name)
		migInode.stage = LookupEkSegment
	}()
	if err = migInode.extentClient.OpenStream(migInode.inodeInfo.Inode, false); err != nil {
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
			log.LogErrorf("inode[%v] %v:%v", migInode.name, InodeLookupEkFailed, err.Error())
			migInode.MigTaskCloseStream()
			migInode.DealActionErr(InodeLookupEkCode, err)
			return
		}
		if migInode.migDirection == COMPACTFILEMIGRATE {
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
	case COMPACTFILEMIGRATE:
		srcMediumType = NoneMediumType
	case HDDTOSSDFILEMIGRATE:
		srcMediumType = proto.MediumHDDName
	case SSDTOHDDFILEMIGRATE:
		srcMediumType = proto.MediumSSDName
	default:
		err = fmt.Errorf("migrate direction invalid(%v)", migInode.migDirection)
		return err
	}
	for i := migInode.lastMigEkIndex; i < len(migInode.extents); i++ {
		migInode.lastMigEkIndex = i + 1
		ek := eks[i]
		var candidateMediumType string
		if srcMediumType != NoneMediumType {
			candidateMediumType = migInode.vol.GetDpMediumType(migInode.vol.ClusterName, migInode.vol.Name, ek.PartitionId)
			candidateMediumType = strings.ToLower(candidateMediumType)
		}
		log.LogDebugf("lookup inode(%v) srcMediumType(%v) candidateMediumType(%v)", migInode.name, srcMediumType, candidateMediumType)
		if !proto.IsTinyExtent(ek.ExtentId) && (srcMediumType == NoneMediumType || candidateMediumType == srcMediumType) {
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
		err = fmt.Errorf("checkEkSegmentHasHole ino:%v fileOffset:%v migType:%v has a hole", migInode.name, fileOffset, migInode.migDirection)
		return
	}
	if tinyExtentId, hasTinyExtent := migInode.checkEkSegmentHasTinyExtent(start, end); hasTinyExtent {
		err = fmt.Errorf("checkEkSegmentHasTinyExtent ino:%v tinyExtentId:%v  migType:%v has tiny extent", migInode.name, tinyExtentId, migInode.migDirection)
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
			log.LogErrorf("inode[%v] %v:%v", migInode.name, InodeLockExtentFailed, err.Error())
			migInode.MigTaskCloseStream()
			migInode.DealActionErr(InodeLockExtentFailedCode, err)
			return
		}
		// extent锁定后延迟
		time.Sleep(afterLockSleepTime * time.Second)
		migInode.stage = CheckCanMigrate
	}()
	willLockedEks := migInode.extents[migInode.startIndex:migInode.endIndex]
	err = migInode.extentClient.LockExtent(context.Background(), willLockedEks, lockTime)
	log.LogInfof("LockExtents inode[%v] willLockedEks:%v lockTime:%v err:%v", migInode.name, willLockedEks, lockTime, err)
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
			log.LogErrorf("inode[%v] %v:%v", migInode.name, InodeCheckInodeFailed, err.Error())
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
		err = fmt.Errorf("check inode cannot migrate inconsistent migrate type, atime:%v mtime:%v migType:%v", inodeInfo.AccessTime, inodeInfo.ModifyTime, migDirection)
		return
	}
	return
}

func (migInode *MigrateInode) ReadAndWriteEkData() (err error) {
	metrics := exporter.NewModuleTP(UmpKeySuffix(FileMig, migInode.stage.String()))
	defer metrics.Set(err)

	migInode.rwStartTime = time.Now()
	defer func() {
		if err != nil {
			migInode.UnlockExtents()
			migInode.MigTaskCloseStream()
			migInode.DealActionErr(InodeReadFailedCode, err)
			log.LogErrorf("inode[%v] %v:%v", migInode.name, InodeReadAndWriteFailed, err.Error())
			return
		}
		migInode.stage = MetaMergeExtents
	}()
	offset := migInode.extents[migInode.startIndex].FileOffset
	totalSize := migInode.extents[migInode.endIndex-1].FileOffset + uint64(migInode.extents[migInode.endIndex-1].Size) - offset
	migEksCnt := migInode.endIndex - migInode.startIndex
	var (
		firstWrite = true
		dp         *data.DataPartition
		newEk      *proto.ExtentKey
		readN      int
		writeN     int
		readOffset = offset
		readSize   uint64
		buff       = make([]byte, unit.BlockSize)
		write      int
		writeTotal int
		crc        = crc32.NewIEEE()
		ctx        = context.Background()
	)
	for {
		readSize = uint64(len(buff))
		if totalSize-(readOffset-offset) <= 0 {
			break
		}
		if totalSize-(readOffset-offset) < readSize {
			readSize = totalSize - (readOffset - offset)
		}
		readN, _, err = migInode.extentClient.Read(ctx, migInode.inodeInfo.Inode, buff, readOffset, int(readSize))
		if err != nil && err != io.EOF {
			return
		}
		if readN > 0 {
			_, _ = crc.Write(buff[:readN])
			if firstWrite {
				dp, writeN, newEk, err = migInode.extentClient.SyncWrite(ctx, migInode.inodeInfo.Inode, readOffset, buff[:readN])
				if err != nil {
					return
				}
				if !(migInode.migDirection == SSDTOHDDFILEMIGRATE && dp.MediumType == proto.MediumHDDName ||
					migInode.migDirection == HDDTOSSDFILEMIGRATE && dp.MediumType == proto.MediumSSDName ||
					migInode.migDirection == COMPACTFILEMIGRATE) {
					return fmt.Errorf("SyncWrite dpId(%v) incorrect medium type, volume(%v) mp(%v) inode(%v) migType(%v)",
						dp.PartitionID, migInode.vol.Name, migInode.mpOp.mpId, migInode.inodeInfo.Inode, migInode.migDirection)
				}
				migInode.newEks = append(migInode.newEks, newEk)
				firstWrite = false
			} else {
				writeN, err = migInode.extentClient.SyncWriteToSpecificExtent(ctx, dp, migInode.inodeInfo.Inode, readOffset, write, buff[:readN], int(newEk.ExtentId))
				if err != nil {
					log.LogWarnf("ReadAndWriteEkData syncWriteToSpecificExtent ino(%v), err(%v)", migInode.name, err)
					dp, writeN, newEk, err = migInode.extentClient.SyncWrite(ctx, migInode.inodeInfo.Inode, readOffset, buff[:readN])
					write = 0
					if err != nil {
						return
					}
					migInode.newEks = append(migInode.newEks, newEk)
					if migInode.migDirection == COMPACTFILEMIGRATE && len(migInode.newEks) >= migEksCnt {
						err = fmt.Errorf("ReadAndWriteEkData new create extent ino(%v) newEks length(%v) is greater than or equal to oldEks length(%v)", migInode.name, len(migInode.newEks), migInode)
						return
					}
				} else {
					newEk.Size += uint32(writeN)
				}
			}
			readOffset += uint64(readN)
			write += writeN
			writeTotal += writeN
			log.LogDebugf("ReadAndWriteEkData write data ino(%v), totalSize(%v), readN(%v), readOffset(%v), write(%v), dpId(%v), extId(%v)",
				migInode.name, totalSize, readN, readOffset, write, dp.PartitionID, newEk.ExtentId)
		}
		if err == io.EOF {
			err = nil
			break
		}
	}
	migInode.migDataCrc = crc.Sum32()
	log.LogDebugf("ino(%v) totalSize(%v) writeTotal(%v) readRange(%v:%v)\n", migInode.name, totalSize, writeTotal, migInode.startIndex, migInode.endIndex)
	if totalSize != uint64(writeTotal) {
		err = fmt.Errorf("ReadAndWriteEkData compare equal ino(%v) totalSize(%v) but write size(%v)", migInode.name, totalSize, writeTotal)
		return
	}
	return
}

func (migInode *MigrateInode) MetaMergeExtents() (err error) {
	metrics := exporter.NewModuleTP(UmpKeySuffix(FileMig, migInode.stage.String()))
	defer metrics.Set(err)

	var (
		migEks     = migInode.extents[migInode.startIndex:migInode.endIndex]
		copyNewEks = make([]proto.ExtentKey, len(migInode.newEks))
	)
	for i, ek := range migInode.newEks {
		copyNewEks[i] = *ek
	}
	migInode.newEks = nil
	defer func() {
		// 解锁extent
		migInode.UnlockExtents()
		if err != nil {
			log.LogErrorf("inode[%v] %v:%v", migInode.name, InodeMergeFailed, err.Error())
			migInode.MigTaskCloseStream()
			migInode.DealActionErr(InodeMergeFailedCode, err)
			return
		}
		migInode.stage = LookupEkSegment
	}()
	if migInode.mpOp.isRecover() {
		err = fmt.Errorf("mp[%v] is recovering", migInode.mpOp.mpId)
		return
	}
	if !migInode.compareReplicasInodeEksEqual() {
		err = fmt.Errorf("unequal extensions between mp[%v] inode[%v] replicas", migInode.mpOp.mpId, migInode.inodeInfo.Inode)
		return
	}
	// 分别从三副本中读取数据和新写的的数据进行crc比较
	var replicateCrc []uint32
	if replicateCrc, err = migInode.replicaCrc(); err != nil {
		return err
	}
	if err = migInode.checkReplicaCrcValid(replicateCrc); err != nil {
		return err
	}
	// 读写的时间 超过（锁定时间-预留时间），放弃修改ek链
	consumedTime := time.Since(migInode.rwStartTime)
	if consumedTime >= maxConsumeTime*time.Second {
		migInode.stage = LookupEkSegment
		log.LogWarnf("before MetaMergeExtents ino(%v) has been consumed time(%v) readRange(%v:%v), but maxConsumeTime(%v)", migInode.name, consumedTime, migInode.startIndex, migInode.endIndex, maxConsumeTime*time.Second)
		return
	}
	log.LogDebugf("before MetaMergeExtents ino(%v) has been consumed time(%v) readRange(%v:%v) will be merged", migInode.name, consumedTime, migInode.startIndex, migInode.endIndex)
	err = migInode.vol.MetaClient.InodeMergeExtents_ll(context.Background(), migInode.inodeInfo.Inode, migEks, copyNewEks, proto.FileMigMergeEk)
	if err != nil {
		// merge fail, delete new create extents
		//migInode.deleteNewExtents(copyNewEks)
		return
	}
	// merge success, delete old extents
	migInode.deleteOldExtents(migEks)
	log.LogDebugf("InodeMergeExtents_ll success ino(%v) oldEks(%v) newEks(%v)", migInode.name, migEks, copyNewEks)
	migInode.statisticsInfo.MigCnt += 1
	migInode.statisticsInfo.MigEkCnt += uint64(migInode.endIndex - migInode.startIndex)
	migInode.statisticsInfo.NewEkCnt += uint64(len(copyNewEks))
	var migSize uint32
	for _, newEk := range copyNewEks {
		migSize += newEk.Size
	}
	migInode.statisticsInfo.MigSize += uint64(migSize)
	return
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

func (migInode *MigrateInode) replicaCrc() (replicateCrc []uint32, err error) {
	defer func() {
		if r := recover(); r != nil {
			log.LogWarnf("replicaCrc occurred panic: %v", r)
			err = fmt.Errorf("replicaCrc occurred panic:%v", r)
		}
	}()
	migEks := migInode.extents[migInode.startIndex:migInode.endIndex]

	var (
		allReplicateEkData [][]byte
	)
	var replicateHash32 = make([]hash.Hash32, 0)
	for _, ek := range migEks {
		var (
			offset     = ek.ExtentOffset
			totalSize  = uint64(ek.Size)
			readOffset = offset
			readSize   uint64
		)
		for {
			readSize = unit.BlockSize
			if totalSize-(readOffset-offset) <= 0 {
				break
			}
			if totalSize-(readOffset-offset) < readSize {
				readSize = totalSize - (readOffset - offset)
			}
			allReplicateEkData, err = migInode.extentClient.ReadExtentAllHost(context.Background(), migInode.inodeInfo.Inode, ek, int(readOffset), int(readSize))
			if err != nil && err != io.EOF {
				return
			}
			if len(replicateHash32) == 0 {
				replicateHash32 = make([]hash.Hash32, len(allReplicateEkData))
				for i := range allReplicateEkData {
					replicateHash32[i] = crc32.NewIEEE()
				}
			}
			for i, d := range allReplicateEkData {
				_, err = replicateHash32[i].Write(d)
				if err != nil {
					return
				}
			}
			readOffset += readSize
		}
	}
	for _, hash32 := range replicateHash32 {
		replicateCrc = append(replicateCrc, hash32.Sum32())
	}
	return
}

func (migInode *MigrateInode) checkReplicaCrcValid(replicateCrc []uint32) (err error) {
	if len(replicateCrc) <= 1 {
		err = fmt.Errorf("dp replicate count is %v", len(replicateCrc))
		return err
	}
	crc0 := replicateCrc[0]
	for i := 1; i < len(replicateCrc); i++ {
		if replicateCrc[i] != crc0 {
			err = fmt.Errorf("replicate crc no equal, crc0:%v crc%v:%v", crc0, i, replicateCrc[i])
			return err
		}
	}
	if crc0 != migInode.migDataCrc {
		err = fmt.Errorf("replicate and migDataCrc are not equal, replciateCrc:%v migDataCrc:%v", replicateCrc, migInode.migDataCrc)
		return err
	}
	return nil
}

func (migInode *MigrateInode) deleteOldExtents(extentKeys []proto.ExtentKey) {
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
		err := retryDeleteExtents(migInode.mpOp.mc, dpId, eks, migInode.inodeInfo.Inode)
		if err != nil {
			log.LogErrorf("deleteOldExtents ino:%v partitionId:%v extentKeys:%v err:%v", migInode.name, dpId, eks, err)
			continue
		}
		log.LogInfof("deleteOldExtents ino:%v partitionId:%v extentKeys:%v success", migInode.name, dpId, eks)
	}
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
			inodeInfoView, err := metaHttpClient.GetInodeInfo(migInode.mpOp.mpId, migInode.inodeInfo.Inode)
			if err == nil && inodeInfoView != nil {
				mu.Lock()
				inodeInfoViews = append(inodeInfoViews, inodeInfoView)
				mu.Unlock()
			} else {
				log.LogErrorf("GetInodeNoModifyAccessTime mpId(%v) ino(%v) err(%v)", migInode.mpOp.mpId, migInode.inodeInfo.Inode, err)
			}
		}(member)
	}
	wg.Wait()
	if len(inodeInfoViews) != len(members) {
		return fmt.Errorf("there is a mp replica but no response")
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
