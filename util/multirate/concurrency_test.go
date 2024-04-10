package multirate

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBaseConcurrency(t *testing.T) {
	mc := NewMultiConcurrency()
	opread := 1
	ctx := context.Background()
	mc.addRule(opread, 1, 20*time.Millisecond)
	assert.Nil(t, mc.WaitUseDefaultTimeout(ctx, opread, "/disk1"))    // no delay
	assert.Nil(t, mc.WaitUseDefaultTimeout(ctx, opread, "/disk2"))    // no delay
	assert.NotNil(t, mc.WaitUseDefaultTimeout(ctx, opread, "/disk1")) // 20 ms delay
	go func() {
		time.Sleep(10 * time.Millisecond)
		mc.Done(opread, "/disk1")
	}()
	assert.Nil(t, mc.WaitUseDefaultTimeout(ctx, opread, "/disk1"))    // 10 ms delay, mc.done, get token
	assert.NotNil(t, mc.WaitUseDefaultTimeout(ctx, opread, "/disk1")) // 20 ms delay

	mc.addRule(opread, 2, 100*time.Millisecond)
	assert.Nil(t, mc.WaitUseDefaultTimeout(ctx, opread, "/disk1")) // no delay
	assert.Nil(t, mc.WaitUseDefaultTimeout(ctx, opread, "/disk1")) // no delay
	go func() {
		time.Sleep(200 * time.Millisecond)
		mc.Done(opread, "/disk1")
	}()
	assert.NotNil(t, mc.WaitUseDefaultTimeout(ctx, opread, "/disk1")) // 100 ms delay, timeout
}

func TestCancelConcurrency(t *testing.T) {
	mc := NewMultiConcurrency()
	opread := 1
	mc.addRule(opread, 1, 0)
	ctx, cancel := context.WithCancel(context.Background())

	assert.Nil(t, mc.WaitUseDefaultTimeout(ctx, opread, "/disk1"))
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		err := mc.WaitUseDefaultTimeout(ctx, opread, "/disk1")
		assert.Contains(t, err.Error(), "canceled")
		wg.Done()
	}()
	cancel()
	wg.Wait()
}

func TestResetConcurrency(t *testing.T) {
	count := uint64(100)
	c := newConcurrency(count, time.Millisecond)
	var wg sync.WaitGroup
	wg.Add(int(count) * 2)
	for i := uint64(0); i < count; i++ {
		go func() {
			c.done()
			wg.Done()
		}()
	}
	for ; count > 0; count-- {
		go func() {
			c.reset(count)
			wg.Done()
		}()
	}
	wg.Wait()
}
