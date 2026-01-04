// Copyright 2022 The CubeFS Authors.
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

package volumemgr

import (
	"container/list"
	"context"
	"math"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/cubefs/cubefs/blobstore/common/codemode"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

const (
	NoDiskLoadThreshold = int(^uint(0) >> 1)
	healthiestScore     = 0
)

type allocConfig struct {
	allocatableDiskLoadThreshold int
	allocFactor                  int
	allocatableSize              uint64
	codeModes                    map[codemode.CodeMode]codeModeConf
	shardNum                     int
}

type idleItem struct {
	head    *list.List
	element *list.Element
}

type idleVolumes struct {
	m                 map[proto.Vid]idleItem // 根据 vid 管理的空闲 volume map
	allocatableShards []*list.List           // shardNum 个链表
	notAllocatable    *list.List             // 不可分配的 volume 链表
	shardNum          int                    // 用来分组管理空闲 volume 的 shard，不是 EC 切片的 shard
	sync.RWMutex
}

// 返回指定 shard 的所有空闲 volume
func (i *idleVolumes) getOneShardIdles(shardId int) []*volume {
	i.RLock()
	shardId = shardId % i.shardNum
	ret := make([]*volume, 0, i.allocatableShards[shardId].Len())
	head := i.allocatableShards[shardId].Front()
	for head != nil {
		ret = append(ret, head.Value.(*volume))
		head = head.Next()
	}
	i.RUnlock()
	return ret
}

func (i *idleVolumes) statAllocatableNum() int {
	i.RLock()
	defer i.RUnlock()
	return len(i.m) - i.notAllocatable.Len()
}

func (i *idleVolumes) addAllocatable(vol *volume) {
	i.Lock()
	if item, ok := i.m[vol.vid]; ok {
		item.head.Remove(item.element)
	}
	idx := int(vol.vid) % i.shardNum
	e := i.allocatableShards[idx].PushFront(vol)
	i.m[vol.vid] = idleItem{element: e, head: i.allocatableShards[idx]}
	i.Unlock()
}

func (i *idleVolumes) addNotAllocatable(vol *volume) {
	i.Lock()
	if item, ok := i.m[vol.vid]; ok {
		item.head.Remove(item.element)
	}
	e := i.notAllocatable.PushFront(vol)
	i.m[vol.vid] = idleItem{element: e, head: i.notAllocatable}
	i.Unlock()
}

func (i *idleVolumes) delete(vid proto.Vid) {
	i.Lock()
	if item, ok := i.m[vid]; ok {
		item.head.Remove(item.element)
		delete(i.m, vid)
	}
	i.Unlock()
}

func (i *idleVolumes) get(vid proto.Vid) (vol *volume) {
	i.RLock()
	if item, ok := i.m[vid]; ok {
		vol = item.element.Value.(*volume)
	}
	i.RUnlock()
	return vol
}

func (i *idleVolumes) allocFromOptions(optionalVids []proto.Vid, count int) (succeed []proto.Vid) {
	i.Lock()
	defer i.Unlock()
	for _, vid := range optionalVids {
		if item, ok := i.m[vid]; ok {
			item.head.Remove(item.element)
			delete(i.m, vid)
			succeed = append(succeed, vid)
			if len(succeed) >= count {
				return
			}
		}
	}
	return
}

type volumeMap map[proto.Vid]*volume

type activeVolumes struct {
	allocatorVols map[string]volumeMap
	diskLoad      map[proto.DiskID]int
	sync.RWMutex
}

// volume allocator, use for allocating volume
type volumeAllocator struct {
	// idle volumes
	idles map[codemode.CodeMode]*idleVolumes
	// actives volumes
	actives *activeVolumes

	allocConfig
}

type sortVid []vidLoad

type vidLoad struct {
	vid    proto.Vid
	load   int
	health int
}

func (v sortVid) Len() int           { return len(v) }
func (v sortVid) Swap(i, j int)      { v[i], v[j] = v[j], v[i] }
func (v sortVid) Less(i, j int) bool { return v[i].health > v[j].health || v[i].load < v[j].load }

func newVolumeAllocator(cfg allocConfig) *volumeAllocator {
	idles := make(map[codemode.CodeMode]*idleVolumes)
	for _, modeConf := range cfg.codeModes {
		allocatableShard := make([]*list.List, cfg.shardNum)
		for i := 0; i < cfg.shardNum; i++ {
			allocatableShard[i] = list.New()
		}
		idles[modeConf.mode] = &idleVolumes{
			m:                 make(map[proto.Vid]idleItem),
			allocatableShards: allocatableShard,
			shardNum:          cfg.shardNum,
			notAllocatable:    list.New(),
		}
	}
	return &volumeAllocator{
		idles: idles,
		actives: &activeVolumes{
			allocatorVols: make(map[string]volumeMap),
			diskLoad:      make(map[proto.DiskID]int),
		},
		allocConfig: cfg,
	}
}

// volume free size or volume health change event callback, check if move volume into idle's allocatable head
func (a *volumeAllocator) VolumeFreeHealthCallback(ctx context.Context, vol *volume) error {
	allocatableScoreThreshold := a.codeModes[vol.volInfoBase.CodeMode].tactic.PutQuorum - a.getShardNum(vol.volInfoBase.CodeMode)
	if vol.canAlloc(a.allocatableSize, allocatableScoreThreshold) {
		a.idles[vol.volInfoBase.CodeMode].addAllocatable(vol)
	}
	return nil
}

// volume status change event callback, idle change should Insert into volume allocator's idle head
func (a *volumeAllocator) VolumeStatusIdleCallback(ctx context.Context, vol *volume) error {
	span := trace.SpanFromContextSafe(ctx)
	allocatableScoreThreshold := a.codeModes[vol.volInfoBase.CodeMode].tactic.PutQuorum - a.getShardNum(vol.volInfoBase.CodeMode)
	span.Debugf("vid: %d set status idle callback, status is %d,free is %d,health is %d", vol.vid, vol.volInfoBase.Status, vol.volInfoBase.Free, vol.volInfoBase.HealthScore)
	if vol.canAlloc(a.allocatableSize, allocatableScoreThreshold) {
		a.idles[vol.volInfoBase.CodeMode].addAllocatable(vol)
	} else {
		a.idles[vol.volInfoBase.CodeMode].addNotAllocatable(vol)
	}

	if vol.token != nil {
		host, _, err := proto.DecodeToken(vol.token.tokenID)
		if err != nil {
			span.Errorf("decode token error,%s", vol.token.String())
			return err
		}
		a.removeAllocatedVolumes(vol.vid, host)
	}
	return nil
}

// volume status change event callback, active change should delete from volume allocator's idle head
// and Insert into allocated head
func (a *volumeAllocator) VolumeStatusActiveCallback(ctx context.Context, vol *volume) error {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("vid: %d set status active callback, status is %d", vol.vid, vol.volInfoBase.Status)
	host, _, err := proto.DecodeToken(vol.token.tokenID)
	if err != nil {
		span.Errorf("decode token error,%s", vol.token.String())
		return err
	}
	a.insertAllocatedVolumes(vol, host)
	a.idles[vol.volInfoBase.CodeMode].delete(vol.vid)
	return nil
}

// volume status change event callback, lock change should delete from volume allocator's idle head
func (a *volumeAllocator) VolumeStatusLockCallback(ctx context.Context, vol *volume) error {
	a.idles[vol.volInfoBase.CodeMode].delete(vol.vid)
	return nil
}

// Insert a volume into volume allocator's idles head
// please ensure that this volume must be idle status
func (a *volumeAllocator) Insert(v *volume, mode codemode.CodeMode) {
	a.idles[mode].addAllocatable(v)
}

// PreAlloc select volumes which can alloc
// 1. when EnableDiskLoad=false, all volume will range by health, the healthier volume will range in front of the optional head
// 2. when EnableDiskLoad=true, if do not hash enough volumes to alloc ,
//  1. first add disk's load and retry, each time add one until disk's load equal to diskLoadThreshold will set EnableDiskLoad=false
//  2. second minus volume score and retry , each time minus one until volume's score equal to scoreThreshold
//
// 挑选可以分配使用的 volume
//  1. 如果不考虑磁盘负载，则只考虑 volume 的健康程度，优先返回更健康的 volume
//  2. 如果考虑磁盘负载，当没有足够的 volume 用来分配时：
//     a. 首先增加磁盘负载并重试，每次增加 1，直到磁盘负载达到 diskLoadThreshold 为止，此时 EnableDiskLoad 将被设置为 false
//     b. 其次减去 volume 得分并重试，每次减去 1，直到 volume 得分等于 scoreThreshold 阈值
func (a *volumeAllocator) PreAlloc(ctx context.Context, mode codemode.CodeMode, count int) ([]proto.Vid, int) {
	span := trace.SpanFromContextSafe(ctx)
	// 先获取对应模式的所有空闲 volume
	idleVolumes := a.idles[mode]
	// 如果为空（即当下没有空闲的 volume）直接返回
	if idleVolumes == nil {
		return nil, 0
	}
	// shardNum - 在配置文件中指定，如果不指定则使用默认值 16
	shardIdx := rand.Intn(idleVolumes.shardNum)
	startIdx := shardIdx
	isLastShard := false

	// 一个 volume 可写的 chunk 个数一定要大于等于 PutQuorum
	allocatableScoreThreshold := a.codeModes[mode].tactic.PutQuorum - a.getShardNum(mode)
	// 检查是否要考虑磁盘负载，默认不考虑
	isEnableDiskLoad := a.isEnableDiskLoad()
	// 健康程度的得分阈值，初始值为 0
	// 如果后续分配不出来会一次递减，直至等于 allocatableScoreThreshold
	scoreThreshold := healthiestScore
	// diskLoadThreshold start half of allocatableDiskLoadThreshold,avoid loop too much times
	diskLoadThreshold := a.allocatableDiskLoadThreshold / 2
	// optionalVids include all volume id which satisfied with our condition(idle/enough free size/health/not over disk load)
	// all vid will range by health, the healthier volume will range in front of the optional head
	optionalVids := make([]proto.Vid, 0)
	var assignable []*volume

GetOneShardAgain:
	// 先获取 shardIdx 对应的空闲 shard 链表
	allIdles := idleVolumes.getOneShardIdles(shardIdx)
	// 检查是否遍历完了所有 shard
	if (shardIdx+1)%idleVolumes.shardNum == startIdx {
		isLastShard = true
		// if last shard has none volume, will put assignable to allIdles, for adjust disk threshold
		if len(allIdles) == 0 {
			allIdles = assignable
			assignable = assignable[:0]
		}
	}

RETRY:
	span.Debugf("prealloc volume length is %d,isEnableDiskLoad:%v", len(allIdles), isEnableDiskLoad)
	now := time.Now()
	for idx, volume := range allIdles {
		volume.lock.RLock()
		// 如果满足以下条件则插入到 optionalVids 中
		// 1. volume 是空闲状态
		// 2. volume 的空闲容量大于 allocatableSize
		// 3. volume 的健康状态大于等于 scoreThreshold
		// 4. 在需要考虑磁盘负载的情况下磁盘负载不超过 diskLoadThreshold
		// 如果不满足以上条件但满足以下条件，则插入到 notAllocatable 链表中
		// 1. volume 的空闲容量小于 allocatableSize 或 volume 的健康状态小于 scoreThreshold
		// 2. volume 是空闲状态
		// 其他情况放入 assignable 中备用
		if volume.canAlloc(a.allocatableSize, scoreThreshold) && (!isEnableDiskLoad || !a.isOverload(volume.vUnits, diskLoadThreshold)) {
			optionalVids = append(optionalVids, volume.vid)
			// only insufficient free size or unhealthy volume move to temporary head,
			// ignore over diskLoad volume
		} else if !volume.canAlloc(a.allocatableSize, allocatableScoreThreshold) && volume.canInsert() {
			// 空闲容量不够或者健康分低的则加入到不能分配的链表中
			idleVolumes.addNotAllocatable(volume)
		} else {
			// 其余条件的加入到 assignable 中
			assignable = append(assignable, volume)
		}
		volume.lock.RUnlock()

		// 如果选到了足够的 volume 则跳出循环
		if len(optionalVids) >= a.allocFactor*count {
			break
		}

		// go to the end, first retry with high disk load volume
		// second  lower health score volume
		// 如果不是最后一组 volume 则通过下面的 goto GetOneShardAgain 尝试下一组
		// 如果已经是最后一组 shard 的最后一个 volume 了
		if isLastShard && idx == len(allIdles)-1 {
			span.Infof("assignable volume length is %d", len(assignable))
			// assignable 为 0 表示没有可以尝试的 volume 了，直接返回
			if len(assignable) == 0 {
				span.Warnf("has no assignable volume,enableDiskLoad:%v,diskLoadThreshold:%d", isEnableDiskLoad, diskLoadThreshold)
				break
			}
			// 1. 如果开启了磁盘负载选项并且小于阈值，那么逐步调大允许的范围
			// 2. 如果已经超过阈值，则直接关闭磁盘负载选项，不检查这一项
			// 3. 如果磁盘负载也关闭了，那么逐步减小得分阈值
			// 4. 如果得分的阈值已经小于可分配的阈值了，则使用 assignable 最最后一次尝试
			if isEnableDiskLoad && diskLoadThreshold < a.allocatableDiskLoadThreshold {
				// When diskLoad exceeds the threshold, retry 3 times at most
				diskLoadThreshold += int(math.Ceil(float64(a.allocatableDiskLoadThreshold) / 6.0))
			} else if isEnableDiskLoad {
				isEnableDiskLoad = false
			} else if scoreThreshold > allocatableScoreThreshold {
				scoreThreshold -= 1
			}
			allIdles = assignable
			assignable = assignable[:0]
			goto RETRY
		}
	}
	// 如果本批次的 volume 没有选够则递增 shard 查看下一组
	if len(optionalVids) < a.allocFactor*count {
		shardIdx = (shardIdx + 1) % idleVolumes.shardNum
		if startIdx != shardIdx {
			goto GetOneShardAgain
		}
	}
	span.Infof("optional vids length is %d, vids is %v", len(optionalVids), optionalVids)

	// 根据健康状态排序
	optionalVids = a.sortVidByHealthAndDiskLoad(mode, optionalVids)
	ret := idleVolumes.allocFromOptions(optionalVids, count)
	span.Debugf("preAlloc volume cost time:%v", time.Since(now))
	return ret, diskLoadThreshold
}

// StatAllocatable return allocatable volume num about every kind of code mode
func (a *volumeAllocator) StatAllocatable() (ret map[codemode.CodeMode]int) {
	allocVolNum := make(map[codemode.CodeMode]int)
	for mode := range a.idles {
		allocVolNum[mode] = a.idles[mode].statAllocatableNum()
	}
	return allocVolNum
}

func (a *volumeAllocator) GetExpiredVolumes() (expiredVids []proto.Vid) {
	a.actives.RLock()
	actives := make([]*volume, 0)
	for _, m := range a.actives.allocatorVols {
		for _, vol := range m {
			actives = append(actives, vol)
		}
	}
	a.actives.RUnlock()

	for _, vol := range actives {
		vol.lock.RLock()
		if vol.isExpired() {
			expiredVids = append(expiredVids, vol.vid)
		}
		vol.lock.RUnlock()
	}
	return
}

func (a *volumeAllocator) LisAllocatedVolumesByHost(host string) (ret []*volume) {
	a.actives.RLock()
	volM, ok := a.actives.allocatorVols[host]
	if !ok {
		a.actives.RUnlock()
		return nil
	}
	a.actives.RUnlock()

	for _, volume := range volM {
		ret = append(ret, volume)
	}

	return
}

func (a *volumeAllocator) insertAllocatedVolumes(v *volume, host string) {
	a.actives.Lock()
	volM, ok := a.actives.allocatorVols[host]
	if !ok {
		volM = make(volumeMap)
		a.actives.allocatorVols[host] = volM
	}
	volM[v.vid] = v

	for _, unit := range v.vUnits {
		a.actives.diskLoad[unit.vuInfo.DiskID]++
	}
	a.actives.Unlock()
}

func (a *volumeAllocator) removeAllocatedVolumes(vid proto.Vid, host string) {
	a.actives.Lock()
	volM, ok := a.actives.allocatorVols[host]
	if ok {
		vol, ok := volM[vid]
		if ok {
			for _, unit := range vol.vUnits {
				a.actives.diskLoad[unit.vuInfo.DiskID]--
			}
		}
		delete(volM, vid)
	}
	a.actives.Unlock()
}

func (a *volumeAllocator) isOverload(vUnits []*volumeUnit, diskLoadThreshold int) bool {
	a.actives.RLock()
	defer a.actives.RUnlock()

	for _, unit := range vUnits {
		if a.actives.diskLoad[unit.vuInfo.DiskID] > diskLoadThreshold {
			return true
		}
	}
	return false
}

func (a *volumeAllocator) isEnableDiskLoad() bool {
	return a.allocatableDiskLoadThreshold != NoDiskLoadThreshold
}

func (a *volumeAllocator) getShardNum(mode codemode.CodeMode) int {
	modeConf := a.codeModes[mode]
	return modeConf.tactic.N + modeConf.tactic.M + modeConf.tactic.L
}

func (a *volumeAllocator) sortVidByHealthAndDiskLoad(mode codemode.CodeMode, vids []proto.Vid) (ret []proto.Vid) {
	if len(vids) <= 1 {
		return vids
	}

	var (
		arrVids sortVid
		diskIDs []proto.DiskID
	)
	for _, vid := range vids {
		volume := a.idles[mode].get(vid)
		if volume != nil {
			health, diskLoad := 0, 0
			volume.withRLocked(func() error {
				health = volume.volInfoBase.HealthScore
				return nil
			})
			vl := vidLoad{vid, 0, health}
			if !a.isEnableDiskLoad() {
				arrVids = append(arrVids, vl)
				continue
			}

			volume.withRLocked(func() error {
				if cap(diskIDs) == 0 {
					diskIDs = make([]proto.DiskID, 0, len(volume.vUnits))
				}
				for _, unit := range volume.vUnits {
					diskIDs = append(diskIDs, unit.vuInfo.DiskID)
				}
				return nil
			})
			a.actives.RLock()
			for _, diskID := range diskIDs {
				diskLoad += a.actives.diskLoad[diskID]
			}
			a.actives.RUnlock()
			vl.load = diskLoad
			arrVids = append(arrVids, vl)
			diskIDs = diskIDs[:0]
		}
	}
	sort.Sort(arrVids)
	ret = make([]proto.Vid, 0, len(arrVids))
	for _, arrVid := range arrVids {
		ret = append(ret, arrVid.vid)
	}

	return ret
}
