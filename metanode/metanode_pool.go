package metanode

import (
	se "github.com/cubefs/cubefs/util/sortedextent"
	"sync"
)

var inodePool = NewInodePool()

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