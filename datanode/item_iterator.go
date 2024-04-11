package datanode

import "io"

type ItemIterator struct {
	applyID uint64
}

// NewItemIterator creates a new item iterator.
func NewItemIterator(applyID uint64) *ItemIterator {

	si := new(ItemIterator)
	si.applyID = applyID
	return si
}

// ApplyIndex returns the appliedID
func (si *ItemIterator) ApplyIndex() uint64 {
	return si.applyID
}

// Close Closes the iterator.
func (si *ItemIterator) Close() {
	return
}

func (si *ItemIterator) Version() uint32 {
	return 0
}

// Next returns the next item in the iterator.
func (si *ItemIterator) Next() (data []byte, err error) {
	return nil, io.EOF
}
