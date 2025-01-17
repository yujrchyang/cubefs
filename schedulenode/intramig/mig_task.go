package intramig

import (
	"fmt"
	cm "github.com/cubefs/cubefs/schedulenode/common"
	"github.com/cubefs/cubefs/schedulenode/migcore"
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

type MigTask struct {
	sync.RWMutex
	mpId        uint64
	name        string
	stage       MigTaskStage
	task        *proto.Task
	vol         *migcore.VolumeInfo
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
	localIp     string

	stopC       chan struct{}
	migInodeLimiter *cm.ConcurrencyLimiter
}

func NewMigTask(localIp string, task *proto.Task, masterClient *master.MasterClient, vol *migcore.VolumeInfo) (mpOp *MigTask) {
	mpOp = &MigTask{localIp: localIp, mpId: task.MpId, mc: masterClient, vol: vol, task: task}
	mpOp.name = fmt.Sprintf("%s#%s#%d", task.Cluster, task.VolName, task.MpId)
	mpOp.migErrMsg = make(map[int]string, 0)
	mpOp.stopC = make(chan struct{})
	mpOp.migInodeLimiter = cm.NewConcurrencyLimiter(DefaultInodeConcurrent)
	return
}

func (mt *MigTask) updateInodeMigParallel(num *int32) {
	go func() {
		ticker := time.NewTicker(time.Minute)
		for {
			select {
			case <- ticker.C:
				if *num <= 0 || *num == mt.migInodeLimiter.Limit() {
					continue
				}
				mt.migInodeLimiter.Reset(*num)
				log.LogInfof("migInodeLimiter reset to %v", *num)
			case <- mt.stopC:
				return
			}
		}
	}()
}

func (mt *MigTask) RunOnce() (finished bool, err error) {
	metrics := exporter.NewModuleTP(migcore.UmpKeySuffix(migcore.FileMig, migcore.RunMpTask))
	defer metrics.Set(err)

	defer func() {
		close(mt.stopC)
		mt.vol.DelMPRunningCnt(mt.mpId)
	}()
	if !mt.vol.AddMPRunningCnt(mt.mpId) {
		return true, nil
	}
	if len(mt.inodes) == 0 {
		mt.last = 0
		mt.stage = GetMPInfo
	}
	defer func() {
		if mt.migInodeCnt <= 0 {
			return
		}
		var msg strings.Builder
		for _, errMsg := range mt.migErrMsg {
			msg.WriteString(errMsg + "##")
		}
		if err := mysql.AddCompactSummary(mt.task, mt.migEkCnt, mt.newEkCnt, mt.migInodeCnt, mt.migCnt, mt.migSize, mt.migErrCnt, msg.String()); err != nil {
			log.LogErrorf("AddCompactSummary add tasks to mysql failed, tasks(%v), err(%v)", mt.task, err)
		}
	}()
	for err == nil {
		if !mt.vol.IsRunning() {
			log.LogDebugf("cmpMpTask compact cancel because vol(%v) be stopped, cmpMpTask.Name(%v) cmpMpTask.stage(%v)", mt.vol.GetName(), mt.name, mt.stage)
			mt.stage = Stopped
		}
		log.LogDebugf("cmpMpTask runonce taskType(%v) cmpMpTask.Name(%v) cmpMpTask.stage(%v)", mt.task.TaskType, mt.name, mt.stage)

		switch mt.stage {
		case GetMPInfo:
			err = mt.SetMpInfo()
		case GetMNProfPort:
			err = mt.SetProfPort()
		case ListAllIno:
			err = mt.ListAllIno()
		case GetInodes:
			err = mt.MigrateInode()
		case WaitSubTask:
			err = mt.WaitMigSubTask()
		case Stopped:
			mt.StopMig()
			finished = true
			return
		default:
			err = nil
			return
		}
	}
	return
}

func (mt *MigTask) SetMpInfo() (err error) {
	metrics := exporter.NewModuleTP(migcore.UmpKeySuffix(migcore.FileMig, mt.stage.String()))
	defer metrics.Set(err)
	defer func() {
		if err != nil {
			log.LogErrorf("mp(%v) stage(%v) failed, err:%v", mt.mpId, mt.stage, err)
			return
		}
		mt.stage = GetMNProfPort
	}()
	mpInfo, err := mt.mc.ClientAPI().GetMetaPartition(mt.mpId, mt.vol.Name)
	if err != nil {
		log.LogErrorf("get meta partition(%v) info failed, err:%v", mt.mpId, err)
		return
	}
	if mpInfo.IsRecover {
		err = fmt.Errorf("mp(%v) is recovering", mt.mpId)
		return
	}
	mp := &meta.MetaPartition{
		PartitionID: mt.mpId,
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

	mt.Lock()
	defer mt.Unlock()
	mt.mpInfo = mp
	leaderAddr := mp.GetLeaderAddr()
	if leaderAddr == "" {
		return fmt.Errorf("get metapartition(%v) no leader", mt.mpId)
	}
	mt.leader = leaderAddr
	return
}

func (mt *MigTask) SetProfPort() (err error) {
	metrics := exporter.NewModuleTP(migcore.UmpKeySuffix(migcore.FileMig, mt.stage.String()))
	defer metrics.Set(err)
	defer func() {
		if err != nil {
			log.LogErrorf("MP(%v) stage(%v) failed:%s", mt.mpId, mt.stage, err)
			return
		}
		mt.stage = ListAllIno
	}()

	var leaderNodeInfo *proto.MetaNodeInfo
	if leaderNodeInfo, err = mt.mc.NodeAPI().GetMetaNode(mt.leader); err != nil {
		log.LogErrorf("get metaNode leader(%v) info failed:%v", mt.leader, err)
		return
	}

	mt.Lock()
	defer mt.Unlock()
	mt.profPort = leaderNodeInfo.ProfPort
	mt.leader = strings.Split(leaderNodeInfo.Addr, ":")[0] + ":" + mt.profPort
	return
}

func (mt *MigTask) ListAllIno() (err error) {
	metrics := exporter.NewModuleTP(migcore.UmpKeySuffix(migcore.FileMig, mt.stage.String()))
	defer metrics.Set(err)

	defer func() {
		if err != nil {
			mt.stage = Stopped
			log.LogErrorf("[ListAllIno] list all inodes failed, mpName(%v) mpId(%v) state(%v) err(%v)", mt.name, mt.mpId, mt.stage, err)
			return
		}
		if len(mt.inodes) == 0 {
			mt.stage = Stopped
			return
		}
		mt.stage = GetInodes
	}()

	metaAdminApi := meta.NewMetaHttpClient(mt.leader, false)
	resp, err := metaAdminApi.ListAllInodesId(mt.mpId, 0, 0, 0)
	if err != nil {
		return
	}
	mt.Lock()
	defer mt.Unlock()
	mt.inodes = mt.inodes[:0]
	mt.inodes = append(mt.inodes, resp.Inodes...)
	return
}

func (mt *MigTask) MigrateInode() (err error) {
	metrics := exporter.NewModuleTP(migcore.UmpKeySuffix(migcore.FileMig, mt.stage.String()))
	defer metrics.Set(err)
	defer func() {
		if err != nil {
			mt.resetInode()
			mt.stage = Stopped
			return
		}
		mt.stage = WaitSubTask
	}()
	end := mt.last + mt.vol.GetInodeCheckStep()
	if end > len(mt.inodes) {
		end = len(mt.inodes)
	}
	var (
		inodes                                      = mt.inodes[mt.last:end]
		maxIno                               uint64 = 0
		inodeConcurrentPerMP                        = mt.vol.GetInodeConcurrentPerMP()
		minEkLen, minInodeSize, maxEkAvgSize        = mt.vol.GetInodeFilterParams()
	)
	migInodes, err := mt.vol.GetMetaClient().GetInodeExtents_ll(context.Background(), mt.mpId, inodes, inodeConcurrentPerMP, minEkLen, minInodeSize, maxEkAvgSize)
	if err != nil {
		log.LogErrorf("[MigInodes] migTask.vol.metaClient.GetInodeExtents_ll: mpName(%v) mpId(%v) state(%v) err(%v)", mt.name, mt.mpId, mt.stage, err)
		return
	}
	log.LogDebugf("migrate inode range mpName(%v) mpId(%v) last:end[%v:%v]", mt.name, mt.mpId, mt.last, end)
	for _, migInode := range migInodes {
		isMatch := mt.checkMatchMigDir(migInode.Inode.Inode)
		log.LogDebugf("check match migration dir, migTask.Name(%v) taskType(%v) inode(%v) isMatch(%v)", mt.name, mt.task.TaskType, migInode.Inode, isMatch)
		if !isMatch {
			continue
		}
		var inodeOp *migcore.MigInode
		if inodeOp, err = migcore.NewMigrateInode(mt, migInode); err != nil {
			log.LogErrorf("NewMigrateInode migTask.Name(%v) inode(%v) err(%v)", mt.name, migInode.Inode, err)
			continue
		}
		mt.migInodeLimiter.Add()
		go func(inodeOp *migcore.MigInode) {
			defer func() {
				mt.migInodeLimiter.Done()
			}()
			_, subTaskErr := inodeOp.RunOnce()
			if subTaskErr != nil {
				log.LogErrorf("[subTask] RunOnce, mpName(%v) mpId(%v) state(%v) err(%v)", mt.name, mt.mpId, mt.stage, subTaskErr)
			}
		}(inodeOp)

		if migInode.Inode.Inode > maxIno {
			maxIno = migInode.Inode.Inode
		}
	}
	mt.setLastCursor(len(migInodes), end, maxIno)
	return
}

func (mt *MigTask) checkMatchMigDir(inode uint64) (isMatch bool) {
	if mt.task.TaskType != proto.WorkerTypeInodeMigration {
		return true
	}
	_, ok1 := mt.vol.InodeFilter.Load(migcore.RootDir)
	if ok1 {
		return true
	}
	_, ok2 := mt.vol.InodeFilter.Load(inode)
	if ok2 {
		return true
	}
	return false
}

func (mt *MigTask) setLastCursor(migInodeCnt, end int, maxIno uint64) {
	if migInodeCnt == 0 {
		mt.last = end
	} else {
		for ; mt.last < len(mt.inodes); mt.last++ {
			if mt.inodes[mt.last] >= maxIno {
				//next run will start at this point
				break
			}
		}
		mt.last++
	}
}

func (mt *MigTask) WaitMigSubTask() (err error) {
	metrics := exporter.NewModuleTP(migcore.UmpKeySuffix(migcore.FileMig, mt.stage.String()))
	defer metrics.Set(err)
	if mt.last >= len(mt.inodes) {
		mt.resetInode()
		mt.stage = Stopped
	} else {
		mt.stage = GetInodes
	}
	return
}

func (mt *MigTask) resetInode() {
	mt.last = 0
	mt.inodes = mt.inodes[:0]
}

func (mt *MigTask) StopMig() {
	mt.resetInode()
}

func (mt *MigTask) GetInodeInfoMaxTime(inodeInfo *proto.InodeInfo) (err error) {
	var (
		wg                                          sync.WaitGroup
		mu                                          sync.Mutex
		inodeInfoViews                              []*proto.InodeInfo
		members                                     = mt.mpInfo.Members
		maxAccessTime, maxModifyTime, maxCreateTime proto.CubeFSTime
	)
	for _, member := range members {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			ipPort := fmt.Sprintf("%v:%v", strings.Split(addr, ":")[0], mt.profPort)
			metaHttpClient := meta.NewMetaHttpClient(ipPort, false)
			inodeInfoView, err := metaHttpClient.GetInodeInfo(mt.mpId, inodeInfo.Inode)
			if err == nil && inodeInfoView != nil {
				mu.Lock()
				inodeInfoViews = append(inodeInfoViews, inodeInfoView)
				mu.Unlock()
			} else {
				log.LogErrorf("GetInodeNoModifyAccessTime mpId(%v) ino(%v) err(%v)", mt.mpId, inodeInfo.Inode, err)
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

func (mt *MigTask) GetInodeMigDirection(inodeInfo *proto.InodeInfo) (migDir migcore.MigDirection, err error) {
	vol := mt.vol
	if vol == nil {
		err = fmt.Errorf("mig task name(%v) vol should not be nil", mt.name)
		return
	}
	policies, exist := vol.GetLayerPolicies(vol.ClusterName, vol.Name)
	if !exist {
		err = fmt.Errorf("getLayerPolicies does not exist, cluster(%v) volume(%v) mp(%v)", vol.ClusterName, vol.Name, mt.mpId)
		return
	}
	for _, policy := range policies {
		policyInodeATime, ok := policy.(*proto.LayerPolicyInodeATime)
		if !ok {
			err = fmt.Errorf("getLayerPolicies is invalid, cluster(%v) volume(%v) mp(%v)", vol.ClusterName, vol.Name, mt.mpId)
			break
		}
		if policyInodeATime.TimeType == proto.InodeAccessTimeTypeTimestamp {
			isToColdMedium := migcore.TimeStampAgo(inodeInfo, policyInodeATime)
			migDir = migcore.ConvertMigrateDirection(policyInodeATime, isToColdMedium)
			if isToColdMedium {
				break
			}
		}
		if policyInodeATime.TimeType == proto.InodeAccessTimeTypeDays {
			isToColdMedium := migcore.DaysAgo(inodeInfo, policyInodeATime)
			migDir = migcore.ConvertMigrateDirection(policyInodeATime, isToColdMedium)
			if isToColdMedium {
				break
			}
		}
		if policyInodeATime.TimeType == proto.InodeAccessTimeTypeSec {
			isToColdMedium := migcore.SecondsAgo(inodeInfo, policyInodeATime)
			migDir = migcore.ConvertMigrateDirection(policyInodeATime, isToColdMedium)
			if isToColdMedium {
				break
			}
		}
	}
	return
}

func (mt *MigTask) UpdateStatisticsInfo(info migcore.MigRecord) {
	mt.Lock()
	defer mt.Unlock()
	mt.migCnt += info.MigCnt
	mt.migInodeCnt += info.MigInodeCnt
	mt.migEkCnt += info.MigEkCnt
	mt.newEkCnt += info.NewEkCnt
	mt.migSize += info.MigSize
	mt.migErrCnt += info.MigErrCnt
	if info.MigErrCode >= migcore.InodeOpenFailedCode && info.MigErrCode <= migcore.InodeMergeFailedCode {
		mt.migErrMsg[info.MigErrCode] = info.MigErrMsg
	}
}

func (mt *MigTask) GetTaskType() proto.WorkerType {
	return mt.task.TaskType
}

func(mt *MigTask) GetTaskId () uint64 {
	return mt.task.TaskId
}

func(mt *MigTask) GetMasterClient () *master.MasterClient {
	return mt.mc
}

func(mt *MigTask) GetProfPort() string {
	return mt.profPort
}

func(mt *MigTask) GetMpInfo() *meta.MetaPartition {
	return mt.mpInfo
}

func(mt *MigTask) GetLocalIp() string {
	return mt.localIp
}

func(mt *MigTask) GetVol() *migcore.VolumeInfo {
	return mt.vol
}

func(mt *MigTask) GetMpId() uint64 {
	return mt.mpId
}

func(mt *MigTask) GetMpLeader() string {
	return mt.leader
}

func(mt *MigTask) GetRawTask() *proto.Task {
	return mt.task
}

func(mt *MigTask) String () string {
	return fmt.Sprintf("%v", mt.task)
}
