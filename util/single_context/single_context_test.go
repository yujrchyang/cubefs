package single_context

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestSingleContext(t *testing.T) {
	var timeout = 6
	s := NewSingleContextWithTimeout(timeout)
	assert.Equal(t, timeout, s.GetTimeoutMs())
	for i := 0; i < 128; i++ {
		go func() {
			for {
				tStart := time.Now()
				ctx := s.GetContextWithTimeout()
				assert.NotNil(t, ctx)
				select {
				case <-ctx.Done():
					assert.True(t, time.Since(tStart) >= time.Duration(timeout/2)*time.Millisecond)
				case <-s.stopCh:
					return
				}
			}
		}()
	}
	time.Sleep(time.Second * 5)
	s.Stop()
}
