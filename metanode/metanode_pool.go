package metanode

import (
	se "github.com/cubefs/cubefs/util/sortedextent"
	"github.com/cubefs/cubefs/util/unit"
	"sync"
)

const (
	defRespDataCap = 64*unit.KB
)

var (
	inodePool = NewInodePool()
	bytesPool = NewBytesPool()
)

type InodePool struct {
	pool *sync.Pool
}

func NewInodePool() *InodePool {
	return &InodePool{
		pool: &sync.Pool{
			New: func() interface{} {
				return &Inode{
					Extents: se.NewSortedExtents(),
				}
			},
		},
	}
}

func (p *InodePool) Get() *Inode {
	return p.pool.Get().(*Inode)
}

func (p *InodePool) Put(ino *Inode) {
	ino.Reset()
	p.pool.Put(ino)
}

func (p *InodePool) BatchGet(count int) (inodes []*Inode) {
	inodes = make([]*Inode, 0, count)
	for index := 0; index < count; index++ {
		ino := p.pool.Get().(*Inode)
		inodes = append(inodes, ino)
	}
	return
}

func (p *InodePool) BatchPut(inodes []*Inode) {
	for index := 0; index < len(inodes); index ++ {
		inodes[index].Reset()
		p.pool.Put(inodes[index])
	}
}

type BytesPool struct {
	pool *sync.Pool
}

func NewBytesPool() *BytesPool {
	return &BytesPool{
		pool: &sync.Pool{
			New: func() interface{} {
				return make([]byte, 0, defRespDataCap)
			},
		},
	}
}

func (p *BytesPool) Get() []byte {
	return p.pool.Get().([]byte)
}

func (p *BytesPool) Put(data []byte) {
	if cap(data) > defRespDataCap {
		return
	}
	data = data[:0]
	p.pool.Put(data)
}