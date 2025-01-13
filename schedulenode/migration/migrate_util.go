package migration

import (
	"encoding/json"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"net"
	"runtime"
	"strings"
	"sync"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/data"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/sdk/mysql"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/unit"
	"golang.org/x/net/context"
)

func (migInode *MigrateInode) DealActionErr(errCode int, err error) {
	if err == nil {
		return
	}
	migInode.statisticsInfo.MigErrCode = errCode
	migInode.statisticsInfo.MigErrCnt += 1
	migInode.statisticsInfo.MigErrMsg = err.Error()
	return
}

func (migInode *MigrateInode) initExtentMaxIndex() {
	for i, ek := range migInode.extents {
		key := dpIdExtentIdKey(ek.PartitionId, ek.ExtentId)
		migInode.extentMaxIndex[key] = i
	}
}

func getSrcMediumType(migDirection MigrateDirection) string {
	switch migDirection {
	case CompactFileMigrate, S3FileMigrate:
		return NoneMediumType
	case HDDToSSDFileMigrate:
		return proto.MediumHDDName
	case SSDToHDDFileMigrate:
		return proto.MediumSSDName
	case ReverseS3FileMigrate:
		return proto.MediumS3Name
	default:
		return ""
	}
}

func getCandidateMediumType(migInode *MigrateInode, srcMediumType string, ek proto.ExtentKey) string {
	if srcMediumType == NoneMediumType {
		return ""
	}
	if srcMediumType == proto.MediumS3Name {
		return strings.ToLower(proto.ExtentType(ek.CRC).String())
	}
	return strings.ToLower(migInode.vol.GetDpMediumType(migInode.vol.ClusterName, migInode.vol.Name, ek.PartitionId))
}

func isValidExtent(ek proto.ExtentKey, srcMediumType, candidateMediumType string) bool {
	return !proto.IsTinyExtent(ek.ExtentId) && ((srcMediumType == NoneMediumType && ek.CRC == uint32(proto.CubeFSExtent)) || candidateMediumType == srcMediumType)
}

func checkForHole(migInode *MigrateInode, i, start, end int) error {
	if start < end && i > 0 {
		prevEk := migInode.extents[i-1]
		currEk := migInode.extents[i]
		if prevEk.FileOffset+uint64(prevEk.Size) < currEk.FileOffset {
			return fmt.Errorf("lookup ino:%v fileOffset:%v migType:%v has a hole", migInode.name, currEk.FileOffset, migInode.migDirection)
		}
	}
	return nil
}

func validateEkSegment(migInode *MigrateInode, start, end int) error {
	if fileOffset, hasHole := migInode.checkEkSegmentHasHole(start, end); hasHole {
		return fmt.Errorf("checkEkSegmentHasHole ino:%v fileOffset:%v migDirection:%v has a hole", migInode.name, fileOffset, migInode.migDirection)
	}
	if tinyExtentId, hasTinyExtent := migInode.checkEkSegmentHasTinyExtent(start, end); hasTinyExtent {
		return fmt.Errorf("checkEkSegmentHasTinyExtent ino:%v tinyExtentId:%v migDirection:%v can't have tiny extent", migInode.name, tinyExtentId, migInode.migDirection)
	}
	if migInode.migDirection != ReverseS3FileMigrate && migInode.checkEkSegmentHasS3Extent(start, end) {
		return fmt.Errorf("checkEkSegmentHasS3Extent ino:%v ek start:%v end:%v migDirection:%v can't have s3 extent", migInode.name, start, end, migInode.migDirection)
	}
	if migInode.migDirection == ReverseS3FileMigrate && migInode.checkEkSegmentHasCubeFSExtent(start, end) {
		return fmt.Errorf("checkEkSegmentHasCubeFSExtent ino:%v ek start:%v end:%v migDirection:%v can't have cubeFS extent", migInode.name, start, end, migInode.migDirection)
	}
	return nil
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

func (migInode *MigrateInode) doReadAndWriteTo(ctx context.Context, ek proto.ExtentKey, fileOffset uint64, size uint64, crc hash.Hash32, writeTotal *int) error {
	var (
		firstWrite             = true
		readSize        uint64 = unit.BlockSize
		buff                   = make([]byte, readSize)
		writeFileOffset        = fileOffset
		extentWriteOffset      int
		readStartOffset uint64
		totalSize       uint64
		dp              *data.DataPartition
		newEk           *proto.ExtentKey
	)

	switch migInode.migDirection {
	case ReverseS3FileMigrate:
		readStartOffset, totalSize = ek.ExtentOffset, uint64(ek.Size)
	default:
		readStartOffset, totalSize = fileOffset, size
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
			err = migInode.writeToDataNode(ctx, &dp, &newEk, &firstWrite, &writeFileOffset, &extentWriteOffset, buff[:readN], writeTotal)
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

func (migInode *MigrateInode) readDataFromS3(ctx context.Context, partitionId, extentId, readOffset, readSize uint64, buff []byte) (readN int, err error) {
	s3Key := proto.GenS3Key(migInode.vol.ClusterName, migInode.vol.Name, migInode.inodeInfo.Inode, partitionId, extentId)
	readN, err = migInode.vol.S3Client.GetObject(ctx, migInode.vol.Bucket, s3Key, readOffset, readSize, buff)
	return
}

func (migInode *MigrateInode) writeToDataNode(ctx context.Context, dp **data.DataPartition, newEk **proto.ExtentKey, firstWrite *bool, writeFileOffset *uint64, extentWriteOffset *int, buff []byte, writeTotal *int) error {
	var (
		writeN            int
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
		writeN, err = migInode.extentClient.SyncWriteToSpecificExtent(ctx, *dp, migInode.inodeInfo.Inode, *writeFileOffset, *extentWriteOffset, buff, int((*newEk).ExtentId), migInode.DirectWrite)
		if err != nil {
			log.LogWarnf("ReadS3AndWriteToDataNode syncWriteToSpecificExtent ino(%v), err(%v)", migInode.name, err)
			*dp, writeN, *newEk, err = migInode.extentClient.SyncWrite(ctx, migInode.inodeInfo.Inode, *writeFileOffset, buff, migInode.DirectWrite)
			*extentWriteOffset = 0
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

	*extentWriteOffset += writeN
	*writeFileOffset += uint64(writeN)
	*writeTotal += writeN

	return nil
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
		var readSize uint64

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
		err = retryDeleteExtents(migInode.mpOp.mc, migInode.vol.Name, dpId, eks, migInode.inodeInfo.Inode)
		if err != nil {
			log.LogErrorf("deleteOldExtents ino:%v partitionId:%v extentKeys:%v err:%v", migInode.name, dpId, eks, err)
			return
		}
		log.LogInfof("deleteOldExtents ino:%v partitionId:%v extentKeys:%v success", migInode.name, dpId, eks)
	}
	return
}

func (migInode *MigrateInode) getInodeInfo() (inodeInfo *proto.InodeInfo, err error) {
	ipPort := fmt.Sprintf("%v:%v", strings.Split(migInode.mpOp.leader, ":")[0], migInode.mpOp.profPort)
	metaHttpClient := meta.NewMetaHttpClient(ipPort, false)
	inodeInfo, err = metaHttpClient.GetInodeInfo(migInode.mpOp.mpId, migInode.inodeInfo.Inode)
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

func (migInode *MigrateInode) checkNewEkCountValid() (err error) {
	switch migInode.migDirection {
	case CompactFileMigrate:
		migEksCnt := migInode.endIndex - migInode.startIndex
		if len(migInode.newEks) >= migEksCnt {
			err = fmt.Errorf("ReadAndWriteDataNode new create extent ino(%v) newEks length(%v) is greater than or equal to oldEks length(%v)",
				migInode.name, len(migInode.newEks), migEksCnt)
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

func (migInode *MigrateInode) checkMigExtentCanDelete(migEks []proto.ExtentKey) (ok bool, canDeleteExtentKeys []proto.ExtentKey) {
	canDeleteExtentKeys = migInode.getDelExtentKeys(migEks)
	if len(migEks) == len(canDeleteExtentKeys) {
		ok = true
	}
	return
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

func retryDeleteExtents(mc *master.MasterClient, volume string, partitionId uint64, eks []proto.ExtentKey, inodeId uint64) (err error) {
	retryNum := 5
	for i := 0; i < retryNum; i++ {
		if err = deleteExtents(mc, volume, partitionId, eks, inodeId); err != nil {
			continue
		}
		break
	}
	return
}

func deleteExtents(mc *master.MasterClient, volume string, partitionId uint64, eks []proto.ExtentKey, inodeId uint64) (err error) {
	var partition *proto.DataPartitionInfo
	partition, err = mc.AdminAPI().GetDataPartition(volume, partitionId)
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
