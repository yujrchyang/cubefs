package single_context

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestSingleContext(t *testing.T) {
	var timeout = 100
	var minTimeout = 60
	s := NewSingleContextWithTimeout(timeout)
	assert.Equal(t, timeout, s.GetTimeoutMs())
	for i := 0; i < 32; i++ {
		go func() {
			for {
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
