package single_context

import (
	"github.com/stretchr/testify/assert"
	"math/rand"
	"testing"
	"time"
)

func TestSingleContext(t *testing.T) {
	var timeout = 64
	var minTimeout = 48
	s := NewSingleContextWithTimeout(timeout)
	assert.Equal(t, timeout, s.GetTimeoutMs())
	for i := 0; i < 32; i++ {
		go func() {
			for {
				r := rand.New(rand.NewSource(time.Now().UnixNano()))
				time.Sleep(time.Duration(r.Intn(timeout)) * time.Millisecond)
				tStart := time.Now()
				ctx := s.GetContextWithTimeout()
				assert.NotNil(t, ctx)
				deadline, ok := ctx.Deadline()
				assert.True(t, ok)
				assert.GreaterOrEqual(t, deadline.Sub(time.Now()), time.Duration(minTimeout)*time.Millisecond)
				select {
				case <-ctx.Done():
					t.Logf("timeout cost: %v", time.Since(tStart))
				case <-s.stopCh:
					return
				}
			}
		}()
	}
	time.Sleep(time.Second * 5)
	s.Stop()
}
