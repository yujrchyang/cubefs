package flash

import (
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/btree"
	"strconv"
	"testing"
)

var (
	testCache = initTestRemoteCache()
	fgCount   = 5
	slotNum   = 5
)

func initTestRemoteCache() *RemoteCache {
	trc := &RemoteCache{
		flashGroups: btree.New(32),
	}
	newFGFunc := func(fgID, slotBegin, slotEnd int) *FlashGroup {
		fg := &FlashGroup{
			FlashGroupInfo: &proto.FlashGroupInfo{
				ID:    uint64(fgID),
				Slot:  make([]uint32, 0),
				Hosts: make([]string, 0),
			},
		}
		for i := slotBegin; i <= slotEnd; i++ {
			fg.Slot = append(fg.Slot, uint32(i))
		}
		fg.Hosts = append(fg.Hosts, strconv.FormatUint(fg.ID, 10))
		return fg
	}
	addItemFunc := func(fg *FlashGroup, tree *btree.BTree) {
		for _, slot := range fg.Slot {
			slotItem := &SlotItem{
				slot:       slot,
				FlashGroup: fg,
			}
			tree.ReplaceOrInsert(slotItem)
		}
	}
	for i := 1; i <= fgCount; i++ {
		fg := newFGFunc(i, (i-1)*slotNum+1, i*slotNum)
		addItemFunc(fg, trc.flashGroups)
	}
	return trc
}

func TestRemoteCache_GetFlashGroupBySlot(t *testing.T) {
	testCases := []struct {
		slot int
		want uint64
	}{
		{12, 3},
		{3, 1},
		{7, 2},
	}
	for i, tt := range testCases {
		fg := testCache.GetFlashGroupBySlot(uint32(tt.slot))
		if fg.ID != tt.want {
			t.Errorf("testCase(%d) failed: got(%v) want(%v)", i, fg.ID, tt.want)
		}
	}
}

func TestNewRemoteCache_rangeFlashGroups(t *testing.T) {
	got := testCache.getFlashHostsMap()
	for i := 1; i <= fgCount; i++ {
		if _, ok := got[strconv.Itoa(i)]; !ok {
			t.Errorf("can not fount fg(%v) host", i)
		}
	}
}
