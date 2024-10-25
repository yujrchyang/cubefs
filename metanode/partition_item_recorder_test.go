package metanode

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSnapshotForRecorder(t *testing.T)  {
	applyID := uint64(100)
	si, err := newRecorderItemIterator(applyID)
	assert.NoErrorf(t, err, "snapshot for recorder")
	assert.Equalf(t, applyID, si.ApplyIndex(), "apply index for snapshot")

	_, err = si.Next()
	assert.Equalf(t, io.EOF, err, "snapshot for recorder should be EOF")
}
