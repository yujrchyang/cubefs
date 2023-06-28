package metanode

import (
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/bitmap"
	"sync"
	"time"
)

const (
	allocatorStatusUnavailable int8 = 0
	allocatorStatusInit        int8 = 1
	allocatorStatusAvailable   int8 = 2
	allocatorStatusFrozen      int8 = 3
	bitPerU64                       = 64
)

type inoAllocatorV1 struct {
	mu               sync.RWMutex
	Bits             bitmap.U64BitMap `json:"-"`
	Start            uint64           `json:"start"`
	End              uint64           `json:"end"`
	Cnt              uint64           `json:"count"`
	Used             uint64           `json:"used"`
	BitCursor        int              `json:"lastBitIndex"`
	Status           int8             `json:"status"`
	Version          uint64           `json:"version"`
	BitsSnap         bitmap.U64BitMap `json:"-"`
	FreezeTime       int64            `json:"-"`
	CancelFreezeTime int64            `json:"-"`
}

func (allocator *inoAllocatorV1) String() string {
	allocator.mu.RLock()
	defer allocator.mu.RUnlock()
	return fmt.Sprintf("Start: %v, End: %v, Count: %v, Used: %v, Status: %v", allocator.Start, allocator.End, allocator.Cnt, allocator.Used, allocator.Status)
}

func NewInoAllocatorV1(start, end uint64) *inoAllocatorV1 {
	if end <= start {
		panic(fmt.Errorf("error inode section, start: %v, end: %v", start, end))
	}

	cnt := end - start
	if cnt > proto.DefaultMetaPartitionInodeIDStep {
		cnt = proto.DefaultMetaPartitionInodeIDStep
	}
	allocator := &inoAllocatorV1{
		Cnt:   cnt,
		Start: start,
		End:   start + cnt,
	}
	bitArrayLen := cnt / bitPerU64
	if cnt%bitPerU64 != 0 {
		bitArrayLen += 1
	}
	allocator.Bits = make([]uint64, bitArrayLen)
	allocator.BitsSnap = make([]uint64, bitArrayLen)
	allocator.Status = allocatorStatusUnavailable

	totalBits := bitArrayLen * bitPerU64
	for overBitIndex := cnt; overBitIndex < totalBits; overBitIndex++ {
		allocator.Bits.SetBit(int(overBitIndex))
		allocator.BitsSnap.SetBit(int(overBitIndex))
	}
	allocator.BitCursor = 0
	return allocator
}

func (allocator *inoAllocatorV1) GetAllocatorSnapInfo() (bits bitmap.U64BitMap, cancelFreezeTime int64, allocatorStatus int8) {
	allocator.mu.RLock()
	defer allocator.mu.RUnlock()

	return allocator.BitsSnap.Copy(), allocator.CancelFreezeTime, allocator.Status
}

func (allocator *inoAllocatorV1) FreezeAllocator(frozenDuration time.Duration) {
	allocator.mu.Lock()
	defer allocator.mu.Unlock()

	if allocator.Status == allocatorStatusUnavailable {
		return
	}

	allocator.Status = allocatorStatusFrozen           //置状态
	allocator.BitsSnap = allocator.Bits.Copy()         //打快照

	curTime := time.Now()
	allocator.FreezeTime = curTime.Unix()
	allocator.CancelFreezeTime = curTime.Add(frozenDuration).Unix()
}

func (allocator *inoAllocatorV1) CancelFreezeAllocator() {
	allocator.mu.Lock()
	defer allocator.mu.Unlock()

	if allocator.Status != allocatorStatusFrozen {
		return
	}

	if allocator.CancelFreezeTime > time.Now().Unix() {
		return
	}

	allocator.Status = allocatorStatusAvailable //解除冻结
	allocator.FreezeTime = 0
	allocator.CancelFreezeTime = 0              //清除解除冻结时间
	allocator.BitCursor = 0                     //重置cursor
}

func (allocator *inoAllocatorV1) ResetCancelFreezeTime(newCancelFreezeTime int64) {
	allocator.mu.Lock()
	defer allocator.mu.Unlock()

	if allocator.Status != allocatorStatusFrozen {
		return
	}

	if allocator.CancelFreezeTime < newCancelFreezeTime {
		allocator.CancelFreezeTime = newCancelFreezeTime
	}
	return
}

func (allocator *inoAllocatorV1) GetAllocatorFreezeState() (status int8, freezeTime, cancelFreezeTime int64) {
	allocator.mu.RLock()
	defer allocator.mu.RUnlock()

	return allocator.Status, allocator.FreezeTime, allocator.CancelFreezeTime
}

func (allocator *inoAllocatorV1) AllocateId() (id uint64, needFreeze bool, err error) {
	allocator.mu.Lock()
	defer allocator.mu.Unlock()
	if allocator.Status != allocatorStatusAvailable {
		return 0, false, fmt.Errorf("allocator not start")
	}

	findToEnd := false
	freeIndex := 0
	if freeIndex, findToEnd, err = allocator.BitsSnap.GetFirstFreeBit(allocator.BitCursor, false); err != nil {
		if findToEnd {
			needFreeze = true
		}
		return
	}
	allocator.BitsSnap.SetBit(freeIndex)
	allocator.Used++
	allocator.BitCursor = freeIndex
	id = allocator.Start + uint64(freeIndex)
	return
}

func (allocator *inoAllocatorV1) SetId(id uint64) {
	if id >= allocator.End {
		return
	}
	allocator.mu.Lock()
	defer allocator.mu.Unlock()
	if allocator.Status == allocatorStatusUnavailable {
		return
	}
	bitIndex := int(id - allocator.Start)
	if allocator.Bits.IsBitFree(bitIndex) {
		allocator.Bits.SetBit(bitIndex)
		allocator.Used++
	}

	if allocator.BitsSnap.IsBitFree(bitIndex) {
		allocator.BitsSnap.SetBit(bitIndex)
		allocator.BitCursor = bitIndex
	}
}

func (allocator *inoAllocatorV1) ClearId(id uint64) {
	if id >= allocator.End {
		return
	}
	allocator.mu.Lock()
	defer allocator.mu.Unlock()
	if allocator.Status == allocatorStatusUnavailable {
		return
	}
	bitIndex := int(id - allocator.Start)
	if !allocator.Bits.IsBitFree(bitIndex) {
		allocator.Bits.ClearBit(bitIndex)
		allocator.Used--
	}
}

func (allocator *inoAllocatorV1) ResetBitCursorToEnd() {
	allocator.mu.Lock()
	defer allocator.mu.Unlock()

	if allocator.Status == allocatorStatusUnavailable {
		return
	}

	allocator.BitCursor = int(allocator.End - allocator.Start - 1)
}

func (allocator *inoAllocatorV1) GetUsed() uint64 {
	allocator.mu.RLock()
	defer allocator.mu.RUnlock()
	return allocator.Used
}

func (allocator *inoAllocatorV1) GetFree() uint64 {
	allocator.mu.RLock()
	defer allocator.mu.RUnlock()
	if allocator.Cnt < allocator.Used {
		return 0
	}
	return allocator.Cnt - allocator.Used
}

func (allocator *inoAllocatorV1) ReleaseBitMapMemory() {
	if allocator.Cnt != 0 {
		allocator.Bits = make([]uint64, 0)
		allocator.BitsSnap = make([]uint64, 0)
		allocator.Cnt = 0
		allocator.Used = 0
		allocator.End = 0
		allocator.Start = 0
		allocator.FreezeTime = 0
		allocator.CancelFreezeTime = 0
	}
}

func (allocator *inoAllocatorV1) changeStatusToUnavailable() (err error) {
	allocator.Status = allocatorStatusUnavailable
	allocator.ReleaseBitMapMemory()
	return
}

func (allocator *inoAllocatorV1) changeStatusToInit() (err error) {
	if allocator.Status == allocatorStatusAvailable {
		return fmt.Errorf("can not change status available to init")
	}

	allocator.Status = allocatorStatusInit
	return
}

func (allocator *inoAllocatorV1) changeStatusToAvailable() (err error) {
	if allocator.Status == allocatorStatusUnavailable {
		return fmt.Errorf("can not change status unavailable to available")
	}

	allocator.Status = allocatorStatusAvailable
	return
}

func (allocator *inoAllocatorV1) changeStatusToFrozen() (err error) {
	if allocator.Status == allocatorStatusUnavailable {
		return fmt.Errorf("allocator can not be freezed when status is unavailable")
	}
	allocator.Status = allocatorStatusFrozen
	return
}

func (allocator *inoAllocatorV1) SetStatus(newStatus int8) (err error) {
	if newStatus > allocatorStatusAvailable || newStatus < allocatorStatusUnavailable {
		err = fmt.Errorf("unknown status %v", newStatus)
		return
	}
	allocator.mu.Lock()
	defer allocator.mu.Unlock()

	switch newStatus {
	case allocatorStatusUnavailable:
		err = allocator.changeStatusToUnavailable()
	case allocatorStatusInit:
		err = allocator.changeStatusToInit()
	case allocatorStatusAvailable:
		err = allocator.changeStatusToAvailable()
	case allocatorStatusFrozen:
		err = allocator.changeStatusToFrozen()
	default:
		err = fmt.Errorf("unknown new status:%d", newStatus)
	}
	return
}

func (allocator *inoAllocatorV1) SetStatusToFrozen() (alreadyFrozen bool, err error) {
	allocator.mu.Lock()
	defer allocator.mu.Unlock()

	if allocator.Status == allocatorStatusFrozen {
		alreadyFrozen = true
		return
	}

	err = allocator.changeStatusToFrozen()
	return
}

func (allocator *inoAllocatorV1) GetStatus() int8 {
	allocator.mu.RLock()
	defer allocator.mu.RUnlock()
	return allocator.Status
}

func (allocator *inoAllocatorV1) ResetLastBitIndex() {
	allocator.mu.RLock()
	defer allocator.mu.RUnlock()
	allocator.BitCursor = 0
}

func (allocator *inoAllocatorV1) GetUsedInos() []uint64 {
	allocator.mu.RLock()
	defer allocator.mu.RUnlock()

	usedInos := make([]uint64, 0, allocator.Cnt)
	for id := allocator.Start; id < allocator.End; id++ {
		if allocator.Bits.IsBitFree(int(id - allocator.Start)) {
			continue
		}
		usedInos = append(usedInos, id)
	}
	return usedInos
}

func (allocator *inoAllocatorV1) GetUsedInosBitMap() []uint64 {
	allocator.mu.RLock()
	defer allocator.mu.RUnlock()

	return allocator.Bits
}