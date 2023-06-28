package metanode

import (
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/assert"
	"math"
	"math/rand"
	"sort"
	"testing"
	"time"
)


func TestInoAllocatorV1_MaxId(t *testing.T) {
	allocator := NewInoAllocatorV1(2000001, 4000000)
	allocator.SetStatus(allocatorStatusInit)
	allocator.SetStatus(allocatorStatusAvailable)
	allocator.SetId(4000000 - 1)
	_, needFreeze, _ := allocator.AllocateId()
	assert.Equal(t, true, needFreeze)
	allocator.FreezeAllocator(0)
	allocator.CancelFreezeAllocator()
	id, _, _ := allocator.AllocateId()
	assert.Equal(t, uint64(2000001), id, "expect id is 2000001")
	id, _, _ = allocator.AllocateId()
	assert.Equal(t, uint64(2000002), id, "expect id is 2000002")
	allocator.SetId(4000000 - 1)
	allocator.ClearId(4000000)
	_, needFreeze, _ = allocator.AllocateId()
	assert.Equal(t, true, needFreeze)
	allocator.FreezeAllocator(0)
	allocator.CancelFreezeAllocator()
	id, _, _ = allocator.AllocateId()
	assert.Equal(t, uint64(2000003), id, "expect id is 2000003")
}

func TestInoAllocatorV1_MaxCost(t *testing.T) {
	allocator := NewInoAllocatorV1(0, 1<<24 + uint64(rand.Int()) % 64)
	allocator.SetStatus(allocatorStatusInit)
	allocator.SetStatus(allocatorStatusAvailable)
	//set all 1
	for i := 0; i < len(allocator.Bits); i++ {
		allocator.Bits[i] = math.MaxUint64
		allocator.BitsSnap[i] = math.MaxUint64
	}

	allocator.ClearId(3)
	allocator.BitsSnap.ClearBit(3)
	allocator.BitCursor = 4
	start := time.Now()
	id, _, err := allocator.AllocateId()
	if err != nil {
		t.Fatalf("allocate id failed")
		return
	}
	cost := time.Since(start)
	assert.Equal(t, uint64(3), id, "expect allocate id:3")
	t.Logf("allocate id:%d, max cost:%v", id, cost)

	allocator.BitCursor = 256
	id, _, err = allocator.AllocateId()
	if err == nil {
		t.Fatalf("allocate id failed, expect err, but now success.id:%d", id)
		return
	}
	allocator.ClearId(allocator.Cnt - 1)
	allocator.BitsSnap.ClearBit(int(allocator.Cnt - 1))
	id, _, err = allocator.AllocateId()
	if err != nil {
		t.Logf(allocator.Bits.GetU64BitInfo(len(allocator.Bits) - 1))
		t.Fatalf("allocate id failed")
		return
	}
	assert.Equal(t, allocator.Cnt - 1, id, "expect allocate id:16777215")
	t.Logf("allocate max id:%d, cnt:%d, end:%d ", id, allocator.Cnt, allocator.End)
}

func TestInoAllocatorV1_NotU64Len(t *testing.T) {
	allocator := NewInoAllocatorV1(0, 1<<24 + 1)
	allocator.SetStatus(allocatorStatusInit)
	//set all 1

	allocator.SetId(3)

	t.Logf(allocator.Bits.GetU64BitInfo(0))
	t.Logf(allocator.Bits.GetU64BitInfo(len(allocator.Bits) - 1))
}

func TestInoAllocatorV1_U64Len(t *testing.T) {
	allocator := NewInoAllocatorV1(0, 1<<24)
	allocator.SetStatus(allocatorStatusInit)
	//set all 1

	allocator.SetId(3)
	t.Logf(allocator.Bits.GetU64BitInfo(0))
	t.Logf(allocator.Bits.GetU64BitInfo(len(allocator.Bits) - 1))
}

func InoAlloterv1UsedCnt(t *testing.T, allocator *inoAllocatorV1) {
	for i := uint64(0) ; i < 100; i++ {
		allocator.SetId(i * 100 + i)
	}
	if allocator.GetUsed() != 100 {
		t.Fatalf("allocate 100, but record:%d, cap:%d", allocator.GetUsed(), allocator.Cnt)
	}

	for i := uint64(0); i < 100; i++ {
		if allocator.Bits.IsBitFree(int((i * 100 + i - allocator.Start) % allocator.Cnt)) {
			t.Fatalf("id allocator:%d but now free, cap:%d", i * 100 + i, allocator.Cnt)
		}
	}

	for i := uint64(0) ; i < 100; i++ {
		allocator.ClearId(i * 100 + i)
	}
	if allocator.GetUsed() != 0 {
		t.Fatalf("allocate 0, but record:%d, cap:%d", allocator.GetUsed(), allocator.Cnt)
	}
	for i := uint64(0); i < 100; i++ {
		if !allocator.Bits.IsBitFree(int((i * 100 + i - allocator.Start) % allocator.Cnt)) {
			t.Fatalf("id allocator:%d but now free, cap:%d", i * 100 + i, allocator.Cnt)
		}
	}
}

func TestInoAllocatorV1_UsedCnt(t *testing.T) {
	allocator  := NewInoAllocatorV1(0, 1<<24 + 1)
	//set all 1
	allocator1 := NewInoAllocatorV1(0, 1<<24)
	allocator.SetStatus(allocatorStatusInit)
	allocator1.SetStatus(allocatorStatusInit)
	InoAlloterv1UsedCnt(t, allocator)
	InoAlloterv1UsedCnt(t, allocator1)
}

func InoAlloterv1Allocate(t *testing.T, allocator *inoAllocatorV1, start uint64) {
	allocator.SetId(start)
	allocator.BitsSnap.SetBit(int(start - allocator.Start))
	for i := uint64(0) ; i < 100; i++ {
		id, _, _ := allocator.AllocateId()
		allocator.SetId(id)
	}
	t.Logf(allocator.Bits.GetU64BitInfo(int(start / 64)))
	t.Logf(allocator.Bits.GetU64BitInfo(int(start / 64 + 1)))
	for i := uint64(0); i < 100; i++ {
		if allocator.Bits.IsBitFree(int((i + 1 + start) % allocator.Cnt)) {
			t.Fatalf("id allocator:%d but now free, cap:%d", i, allocator.Cnt)
		}
	}

	for i := uint64(0) ; i < 100; i++ {
		allocator.ClearId((i + 1 + start)%allocator.Cnt)
	}

	t.Logf(allocator.Bits.GetU64BitInfo(int(start / 64)))
	t.Logf(allocator.Bits.GetU64BitInfo(int(start / 64 + 1)))
	for i := uint64(0); i < 100; i++ {
		if !allocator.Bits.IsBitFree(int((i + 1 + start) % allocator.Cnt)) {
			t.Fatalf("id allocator:%d but now free, cap:%d", i, allocator.Cnt)
		}
	}
	return
}

func TestInoAllocatorV1_Allocate(t *testing.T) {
	allocator  := NewInoAllocatorV1(0, 1<<24 + 1)
	//set all
	allocator1 := NewInoAllocatorV1(0, 1<<24)
	allocator.SetStatus(allocatorStatusInit)
	allocator.SetStatus(allocatorStatusAvailable)
	allocator1.SetStatus(allocatorStatusInit)
	allocator1.SetStatus(allocatorStatusAvailable)
	InoAlloterv1Allocate(t, allocator, uint64(rand.Int()) % allocator.Cnt)
	InoAlloterv1Allocate(t, allocator1, uint64(rand.Int()) % allocator1.Cnt)
}

func TestInoAllocatorV1_StTest(t *testing.T) {
	var err error
	allocator := NewInoAllocatorV1(0, 1<<24)
	//stopped
	err = allocator.SetStatus(allocatorStatusAvailable)
	if err == nil {
		t.Fatalf("expect err, but now nil")
	}
	t.Logf("stat stopped-->started :%v", err)

	err = allocator.SetStatus(allocatorStatusUnavailable)
	if err != nil {
		t.Fatalf("expect nil, but err:%v", err.Error())
	}
	t.Logf("stat stopped-->stopped")

	err = allocator.SetStatus(allocatorStatusInit)
	if err != nil {
		t.Fatalf("expect nil, but err:%v", err.Error())
	}
	t.Logf("stat stopped-->init")

	//init
	err = allocator.SetStatus(allocatorStatusUnavailable)
	if err != nil {
		t.Fatalf("expect nil, but err:%v", err.Error())
	}
	t.Logf("stat init-->stopped")
	allocator.SetStatus(allocatorStatusInit)

	err = allocator.SetStatus(allocatorStatusAvailable)
	if err != nil {
		t.Fatalf("expect nil, but err:%v", err.Error())
	}
	t.Logf("stat init-->start")

	//start
	err = allocator.SetStatus(allocatorStatusAvailable)
	if err != nil {
		t.Fatalf("expect nil, but err:%v", err.Error())
	}
	t.Logf("stat start-->start")

	err = allocator.SetStatus(allocatorStatusInit)
	if err == nil {
		t.Fatalf("expect err, but now nil")
	}
	t.Logf("stat started-->init :%v", err)

	err = allocator.SetStatus(allocatorStatusUnavailable)
	if err != nil {
		t.Fatalf("expect nil, but err:%v", err.Error())
	}
	t.Logf("stat start-->stopped")
}

func TestInoAllocatorV1_AllocateIdBySnap(t *testing.T) {
	allocator := NewInoAllocatorV1(0, proto.DefaultMetaPartitionInodeIDStep)
	_ = allocator.SetStatus(allocatorStatusInit)
	_ = allocator.SetStatus(allocatorStatusAvailable)

	cnt := 100
	occupiedIDs := make([]uint64, 0, cnt)
	rand.Seed(time.Now().UnixMicro())
	for index := 1; index <= cnt; index++ {
		id := uint64(rand.Intn(int(allocator.Cnt)))
		occupiedIDs = append(occupiedIDs, id)
		allocator.SetId(id)
	}

	sort.Slice(occupiedIDs, func(i, j int) bool {
		return occupiedIDs[i] < occupiedIDs[j]
	})

	FreezeAllocator(allocator)
	if _, _, err := allocator.AllocateId(); err == nil {
		t.Fatalf("allocator has been freezed, expect allocate failed")
		return
	}

	releaseIDs := make([]uint64, 0)
	for index, id := range occupiedIDs {
		if index % 3 == 0 {
			allocator.ClearId(id)
			releaseIDs = append(releaseIDs, id)
		}
	}

	for _, releaseID := range releaseIDs {
		assert.Equal(t, true, allocator.Bits.IsBitFree(int(releaseID)), fmt.Sprintf("%v expect release in allocator bits", releaseID))
		assert.Equal(t, false, allocator.BitsSnap.IsBitFree(int(releaseID)), fmt.Sprintf("%v expect has been occupied", releaseID))
	}

	if !waitAllocatorCancelFreeze(allocator) {
		t.Fatalf("allocator cancel freeze failed")
		return
	}

	allocateCnt := 0
	for index := uint64(0); index < allocator.Cnt; index++ {
		allocateID, needFreeze, err := allocator.AllocateId()
		if err != nil {
			if needFreeze{
				break
			}
			t.Fatalf("allocate failed:%v", err)
			return
		}

		allocateCnt++
		for _, id := range occupiedIDs {
			if allocateID == id {
				t.Fatalf("occupied id(%v) has been allocated, unexpect", id)
			}
		}
		allocator.SetId(allocateID)
	}

	assert.Equal(t, proto.DefaultMetaPartitionInodeIDStep - uint64(1+cnt), uint64(allocateCnt))

	FreezeAllocator(allocator)
	if _, _, err := allocator.AllocateId(); err == nil {
		t.Fatalf("allocator has been freezed, expect allocate failed")
		return
	}

	//release all
	for index := uint64(0); index < allocator.Cnt; index++ {
		allocator.ClearId(index)
	}

	if !waitAllocatorCancelFreeze(allocator) {
		t.Fatalf("allocator cancel freeze failed")
		return
	}

	for index := uint64(0); index < allocator.Cnt; index++ {
		allocateID, needFreeze, err := allocator.AllocateId()
		if err != nil {
			if needFreeze{
				break
			}
			t.Fatalf("allocate failed:%v", err)
			return
		}

		find := false
		for _, id := range releaseIDs {
			if allocateID == id {
				find = true
				break
			}
		}
		if !find {
			t.Fatalf("%v still occupied, but has been allocated, already relase IDs%v", allocateID, releaseIDs)
		}
	}

	return
}

func FreezeAllocator(allocator *inoAllocatorV1) {
	allocator.FreezeAllocator(time.Second*5)
	go func() {
		intervalCheckCancelFreeze := time.Second*1
		timer := time.NewTimer(intervalCheckCancelFreeze)
		for {
			select {
			case <- timer.C:
				timer.Reset(intervalCheckCancelFreeze)
				if time.Now().Before(time.Unix(allocator.CancelFreezeTime, 0)) {
					continue
				}
				allocator.CancelFreezeAllocator()
				return
			}
		}
	}()
}

func waitAllocatorCancelFreeze(allocator *inoAllocatorV1) (cancelFreezeSuccess bool) {
	time.Sleep(time.Second * 5)
	retryCnt := 5
	for retryCnt > 0 {
		if allocator.GetStatus() == allocatorStatusFrozen {
			time.Sleep(time.Second*1)
			retryCnt--
			continue
		}
		break
	}
	if retryCnt == 0 {
		return
	}
	cancelFreezeSuccess = true
	return
}

func TestInoAllocatorV1_BitCursor(t *testing.T) {
	allocator := NewInoAllocatorV1(0, proto.DefaultMetaPartitionInodeIDStep)
	_ = allocator.SetStatus(allocatorStatusInit)
	_ = allocator.SetStatus(allocatorStatusAvailable)

	for index := uint64(0); index < 20033; index++ {
		allocator.SetId(index)
	}

	allocator.ResetBitCursorToEnd()

	id, needFreeze, _ := allocator.AllocateId()
	assert.Equal(t, true, needFreeze, fmt.Sprintf("need freeze, but allocate : %v", id))

	FreezeAllocator(allocator)
	_, _, err := allocator.AllocateId()
	if err == nil {
		t.Fatalf("allocator has been freezed, expect allocate failed")
		return
	}

	for index := uint64(1000); index < 1500; index++ {
		allocator.ClearId(index)
	}

	if !waitAllocatorCancelFreeze(allocator) {
		t.Fatalf("allocator cancel freeze failed")
		return
	}

	id, needFreeze, _ = allocator.AllocateId()
	assert.Equal(t, false, needFreeze, fmt.Sprintf("expect allocate success"))
	assert.Equal(t, uint64(20033), id)
}