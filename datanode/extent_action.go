package datanode

import "github.com/cubefs/cubefs/util/holder"

type extentAction struct {
	extentID uint64
	offset   int64
	size     int64
}

func (a *extentAction) Overlap(o holder.Action) bool {
	other, is := o.(*extentAction)
	return is &&
		a.extentID == other.extentID &&
		a.offset < other.offset+other.size &&
		other.offset < a.offset+a.size
}
