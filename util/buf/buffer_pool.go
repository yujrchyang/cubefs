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
		unit.MySQLInnoDBBlockSize + unit.RandomWriteRaftCommandHeaderSize,
		unit.BlockSize,
		unit.BlockSize + unit.RandomWriteRaftCommandHeaderSize,
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
	switch size {
	case unit.PacketHeaderSizeForDbbak:
		return bufferP.pools[0].Get().([]byte)[:size], nil
	case unit.PacketHeaderSize:
		return bufferP.pools[1].Get().([]byte)[:size], nil
	case unit.MySQLInnoDBBlockSize + unit.RandomWriteRaftCommandHeaderSize:
		return bufferP.pools[2].Get().([]byte)[:size], nil
	case unit.BlockSize:
		return bufferP.pools[3].Get().([]byte)[:size], nil
	case unit.BlockSize + unit.RandomWriteRaftCommandHeaderSize:
		return bufferP.pools[4].Get().([]byte)[:size], nil
	case unit.DefaultTinySizeLimit:
		return bufferP.pools[5].Get().([]byte)[:size], nil
	default:
		return nil, fmt.Errorf("can only support 45 or 65536 bytes")
	}
}

// Put puts the given data into the buffer pool.
func (bufferP *BufferPool) Put(data []byte) {
	if data == nil {
		return
	}
	size := len(data)
	switch size {
	case unit.PacketHeaderSizeForDbbak:
		bufferP.pools[0].Put(data[:size])
	case unit.PacketHeaderSize:
		bufferP.pools[1].Put(data[:size])
	case unit.MySQLInnoDBBlockSize + unit.RandomWriteRaftCommandHeaderSize:
		bufferP.pools[2].Put(data[:size])
	case unit.BlockSize:
		bufferP.pools[3].Put(data[:size])
	case unit.BlockSize + unit.RandomWriteRaftCommandHeaderSize:
		bufferP.pools[4].Put(data[:size])
	case unit.DefaultTinySizeLimit:
		bufferP.pools[5].Put(data[:size])
	default:
		return

	}
}
