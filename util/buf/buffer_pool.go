package buf

import (
	"fmt"
	"sync"

	"github.com/cubefs/cubefs/util/unit"
)

var (
	bufferPoolSizes = []int{
		unit.PacketHeaderSizeForDbbak,
		unit.PacketHeaderSize,
		unit.BlockSize,
		unit.BlockSize + unit.RandomWriteRaftCommandHeaderSize,
		unit.MySQLInnoDBBlockSize, +unit.RandomWriteRaftCommandHeaderSize,
		unit.DefaultTinySizeLimit,
	}
)

// BufferPool defines the struct of a buffered pool with 4 objects.
type BufferPool struct {
	pools                  []*sync.Pool
	blockSizeGetNum        uint64
	avaliBlockSizePutNum   uint64
	unavaliBlockSizePutNum uint64
}

// NewBufferPool returns a new buffered pool.
func NewBufferPool() (bufferP *BufferPool) {
	bufferP = &BufferPool{
		pools: make([]*sync.Pool, len(bufferPoolSizes)),
	}
	for i, size := range bufferPoolSizes {
		var bufferSize = size
		bufferP.pools[i] = &sync.Pool{
			New: func() interface{} {
				return make([]byte, bufferSize)
			},
		}
	}
	return bufferP
}

// Get returns the data based on the given size. Different size corresponds to different object in the pool.
func (bufferP *BufferPool) Get(size int) (data []byte, err error) {
	for i := 0; i < len(bufferPoolSizes); i++ {
		if size <= bufferPoolSizes[i] {
			return bufferP.pools[i].Get().([]byte)[:size], nil
		}
	}
	return nil, fmt.Errorf("can only support 45 or 65536 bytes")
}

// Put puts the given data into the buffer pool.
func (bufferP *BufferPool) Put(data []byte) {
	if data == nil {
		return
	}
	capacity := cap(data)
	for i, bufferSize := range bufferPoolSizes {
		if bufferSize == capacity {
			bufferP.pools[i].Put(data[:bufferSize])
			return
		}
	}
}
