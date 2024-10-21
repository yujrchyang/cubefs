package cfs

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestTokenPool(t *testing.T) {
	size := 2
	interval := 5 * time.Second
	tokenPool := newTokenPool(interval, size)
	for i := 0; i < size; i++ {
		assert.True(t, tokenPool.allow())
	}
	assert.False(t, tokenPool.allow())
	time.Sleep(interval + time.Second)
	assert.True(t, tokenPool.allow())
	assert.True(t, tokenPool.allow())
}
