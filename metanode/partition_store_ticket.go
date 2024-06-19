// Copyright 2018 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package metanode

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/unit"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
)

type storeMsg struct {
	command       uint32
	applyIndex    uint64
	snap          Snapshot
	reqTree       *BTree
	allocatorSnap *inoAllocatorV1
}

func (mp *metaPartition) updateRaftStorageParam() {
	nodeCfg := getGlobalConfNodeInfo()
	logSize := defRaftLogSize
	logCap := defRaftLogCap
	if nodeCfg.raftLogSizeFromMaster > 0 {
		logSize = nodeCfg.raftLogSizeFromMaster * unit.MB
	}

	if nodeCfg.raftLogSizeFromLoc  > 0 {
		logSize = nodeCfg.raftLogSizeFromLoc * unit.MB
	}

	if nodeCfg.raftLogCapFromMaster > 0 {
		logCap = nodeCfg.raftLogCapFromMaster
	}

	if nodeCfg.raftLogCapFromLoc  > 0 {
		logCap = nodeCfg.raftLogCapFromLoc
	}

	raftPartition := mp.raftPartition
	if raftPartition == nil {
		return
	}

	if logSize != 0 && logSize != raftPartition.GetWALFileSize() &&
		logSize >= (proto.MinMetaRaftLogSize * unit.MB ) && logSize <= (proto.MaxMetaRaftLogSize * unit.MB) {
		raftPartition.SetWALFileSize(logSize)
		log.LogWarnf("[updateRaftStorageParam] partitionId=%d: File size :%d MB", mp.config.PartitionId, logSize / unit.MB)
	}

	if logCap != 0 && logCap != raftPartition.GetWALFileCacheCapacity() && logCap >= proto.MinMetaRaftLogCap {
		raftPartition.SetWALFileCacheCapacity(logCap)
		log.LogWarnf("[updateRaftStorageParam] partitionId=%d: File Cap :%d ", mp.config.PartitionId, logCap)
	}

	return
}

func (mp *metaPartition) startSchedule(curIndex uint64) {
	timer := time.NewTimer(time.Hour * 24 * 365)
	timer.Stop()
	timerCursor := time.NewTimer(intervalToSyncCursor)
	timerSyncReqRecordsEvictTimestamp := time.NewTimer(time.Second * 5)
	storeTicker := time.NewTicker(intervalDumpSnap)
	cancelFreezeBitMapAllocateTicker := time.NewTicker(intervalToCheckCancelFreeze)
	dumpFunc := func(msg *storeMsg) {
		defer func() {
			mp.manager.tokenM.ReleaseToken(mp.config.PartitionId)
			atomic.StoreInt64(&mp.lastDumpTime, time.Now().Unix())
		}()
		log.LogWarnf("[beforMetaPartitionStore] partitionId=%d: nowAppID"+
			"=%d, applyID=%d", mp.config.PartitionId, curIndex,
			msg.applyIndex)
		if err := mp.store(msg); err == nil {
			// truncate raft log
			if mp.raftPartition != nil {
				mp.updateRaftStorageParam()
				mp.raftPartition.Truncate(curIndex)
				log.LogWarnf("[afterMetaPartitionStore] partitionId=%d: nowAppID"+
					"=%d, applyID=%d", mp.config.PartitionId, curIndex,
					msg.applyIndex)
				curIndex = msg.applyIndex
			} else {
				// maybe happen when start load dentry
				log.LogWarnf("[startSchedule] raftPartition is nil so skip" +
					" truncate raft log")
			}
			if msg.snap != nil {
				msg.snap.Close()
			}

		} else {
			// retry again
			mp.storeChan <- msg
			err = errors.NewErrorf("[startSchedule]: dump partition id=%d: %v", mp.config.PartitionId, err.Error())
			log.LogErrorf(err.Error())
			exporter.WarningAppendKey(PartitionDumpSnapFailed, err.Error())
		}

		if _, ok := mp.IsLeader(); ok {
			timer.Reset(intervalToPersistData)
		}
	}
	go func(stopC chan bool) {
		var maxMsg *storeMsg
		var maxIndex uint64
		for {
			select {
			case <-stopC:
				timer.Stop()
				timerCursor.Stop()
				storeTicker.Stop()
				mp.manager.tokenM.ReleaseToken(mp.config.PartitionId)

				return

			case <-storeTicker.C:
				if time.Now().Unix() - atomic.LoadInt64(&mp.lastDumpTime) < int64(intervalToPersistData / time.Second){
					continue
				}

				if !mp.manager.tokenM.GetRunToken(mp.config.PartitionId) {
					continue
				}

				if maxMsg != nil {
					msg := maxMsg
					go dumpFunc(msg)
					maxMsg = nil
				} else {
					//no dump exe, release token
					mp.manager.tokenM.ReleaseToken(mp.config.PartitionId)
				}
			case msg := <-mp.storeChan:
				switch msg.command {
				case startStoreTick:
					timer.Reset(intervalToPersistData)
				case stopStoreTick:
					timer.Stop()
				case opFSMStoreTick:
					if curIndex >= msg.applyIndex || maxIndex > msg.applyIndex {
						if msg.snap != nil {
							msg.snap.Close()
						}
						continue
					}

					if maxMsg != nil && maxMsg.snap != nil {
						maxMsg.snap.Close()
					}
					maxMsg = msg
					maxIndex = msg.applyIndex
				case resetStoreTick:
					if _, ok := mp.IsLeader(); ok {
						timer.Reset(intervalToPersistData)
					}
				}
			case <-timer.C:
				if mp.applyID <= curIndex {
					timer.Reset(intervalToPersistData)
					continue
				}
				if _, err := mp.submit(context.Background(), opFSMStoreTick, "", nil, nil); err != nil {
					log.LogErrorf("[startSchedule] raft submit: %s", err.Error())
					if _, ok := mp.IsLeader(); ok {
						timer.Reset(intervalToPersistData)
					}
				}
			case <-timerCursor.C:
				if _, ok := mp.IsLeader(); !ok {
					timerCursor.Reset(intervalToSyncCursor)
					continue
				}
				cursorBuf := make([]byte, 8)
				binary.BigEndian.PutUint64(cursorBuf, mp.config.Cursor)
				if _, err := mp.submit(context.Background(), opFSMSyncCursor, "", cursorBuf, nil); err != nil {
					log.LogErrorf("[startSchedule] raft submit: %s", err.Error())
				}
				timerCursor.Reset(intervalToSyncCursor)
			case <- timerSyncReqRecordsEvictTimestamp.C:
				if _, ok := mp.IsLeader(); !ok {
					timerSyncReqRecordsEvictTimestamp.Reset(intervalToSyncEvictReqRecords)
					continue
				}
				evictTimestamp := mp.reqRecords.GetEvictTimestamp()
				evictTimestampBuff := make([]byte, 8)
				binary.BigEndian.PutUint64(evictTimestampBuff, uint64(evictTimestamp))
				if _, err := mp.submit(context.Background(), opFSMSyncEvictReqRecords, "", evictTimestampBuff, nil); err != nil {
					log.LogErrorf("[startSchedule] raft submit: %s", err.Error())
				}
				timerSyncReqRecordsEvictTimestamp.Reset(intervalToSyncEvictReqRecords)
			case <- cancelFreezeBitMapAllocateTicker.C:
				if _, ok := mp.IsLeader(); !ok {
					continue
				}

				mp.cancelFreezeBitmapAllocator()
			}
		}
	}(mp.stopC)
}

func (mp *metaPartition) getTrashCleanInterval() (interval time.Duration) {
	interval = defIntervalToCleanTrash
	if nodeInfo.trashCleanInterval != 0 {
		interval = time.Duration(nodeInfo.trashCleanInterval) * time.Minute
	}
	if mp.config.TrashCleanInterval != 0 {
		interval = time.Duration(mp.config.TrashCleanInterval) * time.Minute
	}
	return
}

func (mp *metaPartition) startCleanTrashScheduler() {
	cleanTrashTimer := time.NewTimer(mp.getTrashCleanInterval())
	go func(stopC chan bool) {
		for {
			select {
			case <-stopC:
				cleanTrashTimer.Stop()
				return
			case <-cleanTrashTimer.C:
				cleanTrashTimer.Reset(mp.getTrashCleanInterval())
				if _, ok := mp.IsLeader(); !ok {
					continue
				}

				if mp.trashExpiresFirstUpdateTime.IsZero() {
					log.LogDebugf("mp[%v] trashExpiresFirstUpdateTime no update", mp.config.PartitionId)
					continue
				}

				if time.Since(mp.trashExpiresFirstUpdateTime) < (intervalToUpdateAllVolsConf + intervalToUpdateVolTrashExpires) {
					log.LogDebugf("mp[%v] since trashExpiresFirstUpdateTime less than %v",
						mp.config.PartitionId, intervalToUpdateAllVolsConf+ intervalToUpdateVolTrashExpires)
					continue
				}
				err := mp.CleanExpiredDeletedDentry()
				if err != nil {
					log.LogErrorf("[CleanExpiredDeletedDentry], vol: %v, error: %s", mp.config.VolName, err.Error())
				}
				err = mp.CleanExpiredDeletedINode()
				if err != nil {
					log.LogErrorf("[CleanExpiredDeletedINode], vol: %v, error: %s", mp.config.VolName, err.Error())
				}

			}
		}
	}(mp.stopC)
}

func (mp *metaPartition) getTrashDaysByVol(vol string) (days int32) {
	if volTopo := mp.topoManager.GetVolume(vol); volTopo.Config() == nil {
		days = -1
	} else {
		days = volTopo.Config().GetTrashDays()
	}
	return
}

func (mp *metaPartition) startUpdatePartitionConfigScheduler() {
	for {
		if mp.config.TrashRemainingDays > -1 {
			break
		}

		mp.config.TrashRemainingDays = mp.getTrashDaysByVol(mp.config.VolName)
		if mp.config.TrashRemainingDays == -1 {
			log.LogWarnf("[startUpdateTrashDaysScheduler], Vol: %v, PartitionID: %v", mp.config.VolName, mp.config.PartitionId)
			time.Sleep(time.Second)
			continue
		}
		break
	}
	ticker := time.NewTicker(intervalToUpdateVolTrashExpires)
	go func(stopC chan bool) {
		for {
			select {
			case <-stopC:
				ticker.Stop()
				return
			case <-ticker.C:
				if mp.trashExpiresFirstUpdateTime.IsZero() {
					mp.trashExpiresFirstUpdateTime = time.Now()
				}
				volTopo := mp.topoManager.GetVolume(mp.config.VolName)
				if volTopo.Config() == nil {
					continue
				}
				conf := volTopo.Config()
				mp.config.TrashRemainingDays = conf.GetTrashDays()
				mp.config.ChildFileMaxCount = conf.GetChildFileMaxCount()
				mp.config.TrashCleanInterval = conf.GetTrashCleanInterval()
				mp.config.EnableRemoveDupReq = conf.GetEnableRemoveDupReqFlag()
				mp.updateMetaPartitionInodeAllocatorState(conf.GetEnableBitMapFlag())
				log.LogDebugf("Vol: %v, PartitionID: %v, trash-days: %v, childFileMaxCount: %v, trashCleanInterval: %vMin",
					mp.config.VolName, mp.config.PartitionId, mp.config.TrashRemainingDays, mp.config.ChildFileMaxCount, mp.config.TrashCleanInterval)
			}
		}
	}(mp.stopC)
}

func (mp *metaPartition) updateMetaPartitionInodeAllocatorState(enable bool) {
	if enable {
		//if cursor >= end, status will load from dump file, if status == init, set status -> available
		//cursor < end && status == init, keep status in init
		status := mp.inodeIDAllocator.GetStatus()
		if mp.config.Cursor >= mp.config.End && status == allocatorStatusInit {
			_ = mp.inodeIDAllocator.SetStatus(allocatorStatusAvailable)
		}
		//if status := mp.inodeIDAllocator.GetStatus(); status == allocatorStatusFrozen {
		//	log.LogDebugf("bitmap allocator already frozen, partitionID: %v", mp.config.PartitionId)
		//	return
		//}
		//_ = mp.inodeIDAllocator.SetStatus(allocatorStatusAvailable)
	} else {
		_ = mp.inodeIDAllocator.SetStatus(allocatorStatusUnavailable)
	}
}

func (mp *metaPartition) stop() (err error) {
	mp.stopLock.Lock()
	defer mp.stopLock.Unlock()
	if mp.stopChState == mpStopChStoppedState {
		err = fmt.Errorf("PartitionID: %v stop chan already closed", mp.config.PartitionId)
		return
	}
	if mp.stopC != nil {
		close(mp.stopC)
	}
	mp.stopChState = mpStopChStoppedState
	return
}
