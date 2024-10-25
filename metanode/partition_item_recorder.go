package metanode

import "io"

// RecorderItemIterator construct a blank snapshot
type RecorderItemIterator struct {
	applyID uint64
}

func newRecorderItemIterator(applyID uint64) (*RecorderItemIterator, error) {
	si := new(RecorderItemIterator)
	si.applyID = applyID
	return si, nil
}

func (si *RecorderItemIterator) ApplyIndex() uint64 {
	return si.applyID
}

func (si *RecorderItemIterator) Close() {
	return
}

func (si *RecorderItemIterator) Version() uint32 {
	return 0
}

func (si *RecorderItemIterator) Next() (data []byte, err error) {
	return nil, io.EOF
}
