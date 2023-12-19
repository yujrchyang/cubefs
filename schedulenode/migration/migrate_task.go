package migration

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/sdk/master"
	"github.com/cubefs/cubefs/sdk/meta"
	"github.com/cubefs/cubefs/sdk/mysql"
	"github.com/cubefs/cubefs/util/exporter"
	"github.com/cubefs/cubefs/util/log"
	"golang.org/x/net/context"
)

type MigrateTask struct {
	sync.RWMutex
	wg          sync.WaitGroup
	mpId        uint64
	name        string
	stage       MigrateTaskStage
	task        *proto.Task
	vol         *VolumeInfo
	mc          *master.MasterClient
	mpInfo      *meta.MetaPartition
	leader      string
	profPort    string
	migEkCnt    uint64
	newEkCnt    uint64
	migInodeCnt uint64
	migCnt      uint64
	migSize     uint64
	migErrCnt   uint64
	migErrMsg   map[int]string
	inodes      []uint64
	last        int
}

func NewMigrateTask(task *proto.Task, masterClient *master.MasterClient, vol *VolumeInfo) (mpOp *MigrateTask) {
	mpOp = &MigrateTask{mpId: task.MpId, mc: masterClient, vol: vol, task: task}
	mpOp.name = fmt.Sprintf("%s#%s#%d", task.Cluster, task.VolName, task.MpId)
	mpOp.migErrMsg = make(map[int]string, 0)
	return
}

func (migTask *MigrateTask) RunOnce() (finished bool, err error) {
	metrics := exporter.NewModuleTP(UmpKeySuffix(FileMig, RunMpTask))
	defer metrics.Set(err)

	defer func() {
		migTask.vol.DelMPRunningCnt(migTask.mpId)
	}()
	if !migTask.vol.AddMPRunningCnt(migTask.mpId) {
		return true, nil
	}
	if len(migTask.inodes) == 0 {
		migTask.last = 0
		migTask.stage = GetMPInfo
	}
	defer func() {
		if migTask.migInodeCnt <= 0 {
			return
		}
		var msg strings.Builder
		for _, errMsg := range migTask.migErrMsg {
			msg.WriteString(errMsg + "##")
		}
		if err := mysql.AddCompactSummary(migTask.task, migTask.migEkCnt, migTask.newEkCnt, migTask.migInodeCnt, migTask.migCnt, migTask.migSize, migTask.migErrCnt, msg.String()); err != nil {
			log.LogErrorf("AddCompactSummary add tasks to mysql failed, tasks(%v), err(%v)", migTask.task, err)
		}
	}()
	for err == nil {
		if !migTask.vol.IsRunning() {
			log.LogDebugf("cmpMpTask compact cancel because vol(%v) be stopped, cmpMpTask.Name(%v) cmpMpTask.stage(%v)", migTask.vol.GetName(), migTask.name, migTask.stage)
			migTask.stage = Stopped
		}
		log.LogDebugf("cmpMpTask runonce taskType(%v) cmpMpTask.Name(%v) cmpMpTask.stage(%v)", migTask.task.TaskType, migTask.name, migTask.stage)

		switch migTask.stage {
		case GetMPInfo:
			err = migTask.GetMpInfo()
		case GetMNProfPort:
			err = migTask.GetProfPort()
		case ListAllIno:
			err = migTask.ListAllIno()
		case GetInodes:
			err = migTask.MigrateInode()
		case WaitSubTask:
			err = migTask.WaitMigSubTask()
		case Stopped:
			migTask.StopMig()
			finished = true
			return
		default:
			err = nil
			return
		}
	}
	return
}

func (migTask *MigrateTask) GetMpInfo() (err error) {
	metrics := exporter.NewModuleTP(UmpKeySuffix(FileMig, migTask.stage.String()))
	defer metrics.Set(err)
	defer func() {
		if err != nil {
			log.LogErrorf("mp(%v) stage(%v) failed, err:%v", migTask.mpId, migTask.stage, err)
			return
		}
		migTask.stage = GetMNProfPort
	}()
	mpInfo, err := migTask.mc.ClientAPI().GetMetaPartition(migTask.mpId, "")
	if err != nil {
		log.LogErrorf("get meta partition(%v) info failed, err:%v", migTask.mpId, err)
		return
	}
	if mpInfo.IsRecover {
		err = fmt.Errorf("mp(%v) is recovering", migTask.mpId)
		return
	}
	mp := &meta.MetaPartition{
		PartitionID: migTask.mpId,
	}
	for _, replica := range mpInfo.Replicas {
		mp.Members = append(mp.Members, replica.Addr)
		if replica.IsLeader {
			mp.LeaderAddr = proto.NewAtomicString(replica.Addr)
		}
		if replica.IsLearner {
			mp.Learners = append(mp.Learners, replica.Addr)
		}
	}
	mp.Status = mpInfo.Status
	mp.Start = mpInfo.Start
	mp.End = mpInfo.End

	migTask.Lock()
	defer migTask.Unlock()
	migTask.mpInfo = mp
	leaderAddr := mp.GetLeaderAddr()
	if leaderAddr == "" {
		return fmt.Errorf("get metapartition(%v) no leader", migTask.mpId)
	}
	migTask.leader = leaderAddr
	return
}

func (migTask *MigrateTask) GetProfPort() (err error) {
	metrics := exporter.NewModuleTP(UmpKeySuffix(FileMig, migTask.stage.String()))
	defer metrics.Set(err)
	defer func() {
		if err != nil {
			log.LogErrorf("MP(%v) stage(%v) failed:%s", migTask.mpId, migTask.stage, err)
			return
		}
		migTask.stage = ListAllIno
	}()

	var leaderNodeInfo *proto.MetaNodeInfo
	if leaderNodeInfo, err = migTask.mc.NodeAPI().GetMetaNode(migTask.leader); err != nil {
		log.LogErrorf("get metaNode leader(%v) info failed:%v", migTask.leader, err)
		return
	}

	migTask.Lock()
	defer migTask.Unlock()
	migTask.profPort = leaderNodeInfo.ProfPort
	migTask.leader = strings.Split(leaderNodeInfo.Addr, ":")[0] + ":" + migTask.profPort
	return
}

func (migTask *MigrateTask) ListAllIno() (err error) {
	metrics := exporter.NewModuleTP(UmpKeySuffix(FileMig, migTask.stage.String()))
	defer metrics.Set(err)

	defer func() {
		if err != nil {
			migTask.stage = Stopped
			log.LogErrorf("[ListAllIno] list all inodes failed, mpName(%v) mpId(%v) state(%v) err(%v)", migTask.name, migTask.mpId, migTask.stage, err)
			return
		}
		if len(migTask.inodes) == 0 {
			migTask.stage = Stopped
			return
		}
		migTask.stage = GetInodes
	}()

	metaAdminApi := meta.NewMetaHttpClient(migTask.leader, false)
	resp, err := metaAdminApi.ListAllInodesId(migTask.mpId, 0, 0, 0)
	if err != nil {
		return
	}
	migTask.Lock()
	defer migTask.Unlock()
	migTask.inodes = migTask.inodes[:0]
	migTask.inodes = append(migTask.inodes, resp.Inodes...)
	return
}

func (migTask *MigrateTask) MigrateInode() (err error) {
	metrics := exporter.NewModuleTP(UmpKeySuffix(FileMig, migTask.stage.String()))
	defer metrics.Set(err)
	defer func() {
		if err != nil {
			migTask.resetInode()
			migTask.stage = Stopped
			return
		}
		migTask.stage = WaitSubTask
	}()
	if migTask.isRecover() {
		err = fmt.Errorf("mp[%v] is recovering", migTask.mpId)
		return
	}
	end := migTask.last + migTask.vol.GetInodeCheckStep()
	if end > len(migTask.inodes) {
		end = len(migTask.inodes)
	}
	var (
		inodes                                      = migTask.inodes[migTask.last:end]
		maxIno                               uint64 = 0
		inodeConcurrentPerMP                        = migTask.vol.GetInodeConcurrentPerMP()
		minEkLen, minInodeSize, maxEkAvgSize        = migTask.vol.GetInodeFilterParams()
	)
	migInodes, err := migTask.vol.GetMetaClient().GetInodeExtents_ll(context.Background(), migTask.mpId, inodes, inodeConcurrentPerMP, minEkLen, minInodeSize, maxEkAvgSize)
	if err != nil {
		log.LogErrorf("[MigInodes] migTask.vol.metaClient.GetInodeExtents_ll: mpName(%v) mpId(%v) state(%v) err(%v)", migTask.name, migTask.mpId, migTask.stage, err)
		return
	}
	for _, migInode := range migInodes {
		isMatch := migTask.checkMatchMigDir(migInode.Inode.Inode)
		log.LogDebugf("check match migration dir, migTask.Name(%v) taskType(%v) inode(%v) isMatch(%v)", migTask.name, migTask.task.TaskType, migInode.Inode, isMatch)
		if !isMatch {
			continue
		}
		var inodeOp *MigrateInode
		if inodeOp, err = NewMigrateInode(migTask, migInode); err != nil {
			log.LogErrorf("NewMigrateInode migTask.Name(%v) inode(%v) err(%v)", migTask.name, migInode.Inode, err)
			continue
		}
		migTask.wg.Add(1)
		go func(inodeOp *MigrateInode) {
			defer migTask.wg.Done()
			_, subTaskErr := inodeOp.RunOnce()
			if subTaskErr != nil {
				log.LogErrorf("[subTask] RunOnce, mpName(%v) mpId(%v) state(%v) err(%v)", migTask.name, migTask.mpId, migTask.stage, subTaskErr)
			}
		}(inodeOp)

		if migInode.Inode.Inode > maxIno {
			maxIno = migInode.Inode.Inode
		}
	}
	migTask.wg.Wait()
	migTask.setLastCursor(len(migInodes), end, maxIno)
	return
}

func (migTask *MigrateTask) checkMatchMigDir(inode uint64) (isMatch bool) {
	if migTask.task.TaskType != proto.WorkerTypeInodeMigration {
		return true
	}
	_, ok1 := migTask.vol.inodeFilter.Load(rootDir)
	if ok1 {
		return true
	}
	_, ok2 := migTask.vol.inodeFilter.Load(inode)
	if ok2 {
		return true
	}
	return false
}

func (migTask *MigrateTask) setLastCursor(migInodeCnt, end int, maxIno uint64) {
	if migInodeCnt == 0 {
		migTask.last = end
	} else {
		for ; migTask.last < len(migTask.inodes); migTask.last++ {
			if migTask.inodes[migTask.last] >= maxIno {
				//next run will start at this point
				break
			}
		}
		migTask.last++
	}
}

func (migTask *MigrateTask) WaitMigSubTask() (err error) {
	metrics := exporter.NewModuleTP(UmpKeySuffix(FileMig, migTask.stage.String()))
	defer metrics.Set(err)
	if migTask.last >= len(migTask.inodes) {
		migTask.resetInode()
		migTask.stage = Stopped
	} else {
		migTask.stage = GetInodes
	}
	return
}

func (migTask *MigrateTask) resetInode() {
	migTask.last = 0
	migTask.inodes = migTask.inodes[:0]
}

func (migTask *MigrateTask) StopMig() {
	migTask.resetInode()
}

func (migTask *MigrateTask) isRecover() bool {
	mpInfo, err := migTask.mc.ClientAPI().GetMetaPartition(migTask.mpId, "")
	return err != nil || mpInfo.IsRecover
}

func (migTask *MigrateTask) getInodeInfoMaxTime(inodeInfo *proto.InodeInfo) (err error) {
	var (
		wg                                          sync.WaitGroup
		mu                                          sync.Mutex
		inodeInfoViews                              []*proto.InodeInfo
		members                                     = migTask.mpInfo.Members
		maxAccessTime, maxModifyTime, maxCreateTime proto.CubeFSTime
	)
	for _, member := range members {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			ipPort := fmt.Sprintf("%v:%v", strings.Split(addr, ":")[0], migTask.profPort)
			metaHttpClient := meta.NewMetaHttpClient(ipPort, false)
			inodeInfoView, err := metaHttpClient.GetInodeInfo(migTask.mpId, inodeInfo.Inode)
			if err == nil && inodeInfoView != nil {
				mu.Lock()
				inodeInfoViews = append(inodeInfoViews, inodeInfoView)
				mu.Unlock()
			} else {
				log.LogErrorf("GetInodeNoModifyAccessTime mpId(%v) ino(%v) err(%v)", migTask.mpId, inodeInfo.Inode, err)
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
	inodeInfo.AccessTime = maxAccessTime
	inodeInfo.ModifyTime = maxModifyTime
	inodeInfo.CreateTime = maxCreateTime
	return
}

func (migTask *MigrateTask) getInodeMigDirection(inodeInfo *proto.InodeInfo) (migDirection MigrateDirection, err error) {
	vol := migTask.vol
	policies, exist := vol.GetLayerPolicies(vol.ClusterName, vol.Name)
	migDirection = HDDTOSSDFILEMIGRATE
	if !exist {
		err = fmt.Errorf("getLayerPolicies does not exist, cluster(%v) volume(%v) mp(%v)", vol.ClusterName, vol.Name, migTask.mpId)
		return
	}
	for _, policy := range policies {
		policyInodeATime, ok := policy.(*proto.LayerPolicyInodeATime)
		if !ok {
			err = fmt.Errorf("getLayerPolicies is invalid, cluster(%v) volume(%v) mp(%v)", vol.ClusterName, vol.Name, migTask.mpId)
			break
		}
		if policyInodeATime.TimeType == proto.InodeAccessTimeTypeTimestamp {
			if int64(inodeInfo.AccessTime) < policyInodeATime.TimeValue &&
				int64(inodeInfo.ModifyTime) < policyInodeATime.TimeValue {
				migDirection = SSDTOHDDFILEMIGRATE
				break
			}
		}
		if policyInodeATime.TimeType == proto.InodeAccessTimeTypeDays {
			if time.Now().Unix()-int64(inodeInfo.AccessTime) > policyInodeATime.TimeValue*24*60*60 &&
				time.Now().Unix()-int64(inodeInfo.ModifyTime) > policyInodeATime.TimeValue*24*60*60 {
				migDirection = SSDTOHDDFILEMIGRATE
				break
			}
		}
		if policyInodeATime.TimeType == proto.InodeAccessTimeTypeSec {
			if time.Now().Unix()-int64(inodeInfo.AccessTime) > policyInodeATime.TimeValue &&
				time.Now().Unix()-int64(inodeInfo.ModifyTime) > policyInodeATime.TimeValue {
				migDirection = SSDTOHDDFILEMIGRATE
				break
			}
		}
	}
	return
}

func (migTask *MigrateTask) UpdateStatisticsInfo(info MigrateRecord) {
	migTask.Lock()
	defer migTask.Unlock()
	migTask.migCnt += info.MigCnt
	migTask.migInodeCnt += info.MigInodeCnt
	migTask.migEkCnt += info.MigEkCnt
	migTask.newEkCnt += info.NewEkCnt
	migTask.migSize += info.MigSize
	migTask.migErrCnt += info.MigErrCnt
	if info.MigErrCode >= InodeOpenFailedCode && info.MigErrCode <= InodeMergeFailedCode {
		migTask.migErrMsg[info.MigErrCode] = info.MigErrMsg
	}
}

func (migTask *MigrateTask) GetTaskType() proto.WorkerType {
	return migTask.task.TaskType
}
