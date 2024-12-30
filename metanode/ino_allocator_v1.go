package metanode

import (
	"encoding/binary"
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
	marshalBinaryBaseDataLen        = 44
	bitmapCursorStart               = 2
)

var (
	doubleAllocateError = fmt.Errorf("double allocate")
	doubleAllocateWarningKey = "DoubleAllocate"
)

type inoAllocatorV1 struct {
	mu               sync.RWMutex
	Bits       bitmap.U64BitMap `json:"-"`
	Start      uint64           `json:"start"`
	End        uint64           `json:"end"`
	Cnt        uint64           `json:"count"`
	Used       uint64           `json:"used"`
	BitCursor  int              `json:"lastBitIndex"`
	Status     int8             `json:"status"`
	Version    uint64           `json:"version"`
	BitsSnap   bitmap.U64BitMap `json:"-"`
	FreezeTime int64            `json:"freezeTime"`
	ActiveTime int64            `json:"activeTime"`
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

	allocator.BitCursor = bitmapCursorStart
	//occupied 0 and 1 permanent
	for index := 0; index < bitmapCursorStart; index++ {
		allocator.Bits.SetBit(index)
		allocator.BitsSnap.SetBit(index)
	}
	allocator.Used = 2
	return allocator
}

func (allocator *inoAllocatorV1) GetAllocatorSnapInfo() (bits bitmap.U64BitMap, activeTime int64, allocatorStatus int8) {
	allocator.mu.RLock()
	defer allocator.mu.RUnlock()

	return allocator.BitsSnap.Copy(), allocator.ActiveTime, allocator.Status
}

func (allocator *inoAllocatorV1) FreezeAllocator(freezeTime, activeTime int64) {
	allocator.mu.Lock()
	defer allocator.mu.Unlock()

	if allocator.Status == allocatorStatusUnavailable {
		return
	}

	allocator.Status = allocatorStatusFrozen   //置状态
	allocator.BitsSnap = allocator.Bits.Copy() //打快照

	allocator.FreezeTime = freezeTime
	allocator.ActiveTime = activeTime
}

func (allocator *inoAllocatorV1) Active(force bool) {
	allocator.mu.Lock()
	defer allocator.mu.Unlock()

	if allocator.Status != allocatorStatusFrozen {
		return
	}

	if !force && allocator.ActiveTime > time.Now().Unix() {
		return
	}

	allocator.Status = allocatorStatusAvailable //激活分配器
	allocator.FreezeTime = 0
	allocator.ActiveTime = 0 //清除激活时间
	allocator.BitCursor = bitmapCursorStart  //重置cursor
}

func (allocator *inoAllocatorV1) ResetActiveTime(newActiveime int64) {
	allocator.mu.Lock()
	defer allocator.mu.Unlock()

	if allocator.Status != allocatorStatusFrozen {
		return
	}

	if allocator.ActiveTime < newActiveime {
		allocator.ActiveTime = newActiveime
	}
	return
}

func (allocator *inoAllocatorV1) GetAllocatorFreezeState() (status int8, freezeTime, activeTime int64) {
	allocator.mu.RLock()
	defer allocator.mu.RUnlock()

	return allocator.Status, allocator.FreezeTime, allocator.ActiveTime
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
	if !allocator.Bits.IsBitFree(freeIndex) {
		err = fmt.Errorf("%s: %v", doubleAllocateError, freeIndex)
		return
	}
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
	if id >= allocator.End || id < bitmapCursorStart {
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
		if allocator.Used > 0 {
			allocator.Used--
		}
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
		allocator.ActiveTime = 0
	}
}

func (allocator *inoAllocatorV1) changeStatusToUnavailable() (err error) {
	if allocator.Status == allocatorStatusUnavailable {
		return
	}
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

func (allocator *inoAllocatorV1) GetUsedInos() []uint64 {
	allocator.mu.RLock()
	defer allocator.mu.RUnlock()

	if allocator.Status == allocatorStatusUnavailable {
		return nil
	}

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

func (allocator *inoAllocatorV1) GenAllocatorSnap() *inoAllocatorV1 {
	allocator.mu.RLock()
	defer allocator.mu.RUnlock()

	if allocator.Status == allocatorStatusInit || allocator.Status == allocatorStatusUnavailable {
		//未启用时不做dump
		return nil
	}

	return &inoAllocatorV1{
		Start:      allocator.Start,
		End:        allocator.End,
		Cnt:        allocator.Cnt,
		Used:       allocator.Used,
		BitCursor:  allocator.BitCursor,
		Status:     allocator.Status,
		Version:    allocator.Version,
		BitsSnap:   allocator.BitsSnap.Copy(),
		FreezeTime: allocator.FreezeTime,
		ActiveTime: allocator.ActiveTime,
	}
}

func (allocator *inoAllocatorV1) MarshalBinary() []byte {
	allocator.mu.RLock()
	defer allocator.mu.RUnlock()

	//version, used, cursor, status, freezeTime, activeTime, reserved filed, bitmapCount, bitmap
	dataLen := marshalBinaryBaseDataLen + len(allocator.BitsSnap)*8
	data := make([]byte, dataLen)

	offset := 0
	binary.BigEndian.PutUint64(data[offset:offset+Uint64Size], allocator.Version)
	offset += Uint64Size
	binary.BigEndian.PutUint32(data[offset:offset+Uint32Size], uint32(allocator.BitCursor))
	offset += Uint32Size
	data[offset] = byte(allocator.Status)
	offset += Uint32Size
	binary.BigEndian.PutUint64(data[offset:offset+Uint64Size], uint64(allocator.FreezeTime))
	offset += Uint64Size
	binary.BigEndian.PutUint64(data[offset:offset+Uint64Size], uint64(allocator.ActiveTime))
	offset += Uint64Size
	offset += Uint64Size //reserved filed
	binary.BigEndian.PutUint32(data[offset:offset+Uint32Size], uint32(len(allocator.BitsSnap)))
	offset += Uint32Size
	allocator.BitsSnap.Range(func(value uint64) bool {
		binary.BigEndian.PutUint64(data[offset:offset+Uint64Size], value)
		offset += Uint64Size
		return true
	})
	return data
}

func (allocator *inoAllocatorV1) UnmarshalBinary(data []byte) (err error) {
	allocator.mu.Lock()
	defer allocator.mu.Unlock()

	offset := 0
	allocator.Version = binary.BigEndian.Uint64(data[offset:offset+Uint64Size])
	offset += Uint64Size
	allocator.BitCursor = int(binary.BigEndian.Uint32(data[offset:offset+Uint32Size]))
	offset += Uint32Size
	allocator.Status = int8(data[offset])
	offset += Uint32Size
	allocator.FreezeTime = int64(binary.BigEndian.Uint64(data[offset:offset+Uint64Size]))
	offset += Uint64Size
	allocator.ActiveTime = int64(binary.BigEndian.Uint64(data[offset:offset+Uint64Size]))
	offset += Uint64Size
	offset += Uint64Size
	bitmapLen := binary.BigEndian.Uint32(data[offset:offset+Uint32Size])
	offset += Uint32Size
	allocator.BitsSnap = bitmap.NewU64BitMap(bitmapLen)
	err = allocator.BitsSnap.FillByBinaryData(data[offset:])
	if err != nil {
		return fmt.Errorf("fill bitmap snap by binary data failed: %v", err)
	}
	return
}
