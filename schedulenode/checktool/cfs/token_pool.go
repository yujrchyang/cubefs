package cfs

import "time"

// TokenPool
// 固定大小的token pool
// 随取随分配，不取不分配
// 取走一个token后，间隔interval时间再分配一个新token
type TokenPool struct {
	ch       chan struct{}
	interval time.Duration
	size     int
}

func newTokenPool(interval time.Duration, size int) *TokenPool {
	td := &TokenPool{
		interval: interval,
		ch:       make(chan struct{}, size),
		size:     size,
	}
	// init concurrency
	for i := 0; i < size; i++ {
		td.ch <- struct{}{}
	}
	return td
}

func (td *TokenPool) allow() bool {
	select {
	case <-td.ch:
		go func() {
			timer := time.NewTimer(td.interval)
			defer func() {
				timer.Stop()
			}()
			select {
			case <-timer.C:
				td.ch <- struct{}{}
			}
		}()
		return true
	default:
		return false
	}
}
