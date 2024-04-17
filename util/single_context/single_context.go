package single_context

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

const (
	DefaultTimeoutMs    = 4
	DefaultMinTimeoutMs = 1
	DefaultMaxTimeoutMs = 1000
)

type SingleContext struct {
	ctx          context.Context
	timeoutMs    int
	minTimeoutMs int
	maxTimeoutMs int
	stopCh       chan struct{}
}

func NewSingleContextWithTimeout(timeoutMs int) (s *SingleContext) {
	s = &SingleContext{
		timeoutMs:    timeoutMs,
		minTimeoutMs: DefaultMinTimeoutMs,
		maxTimeoutMs: DefaultMaxTimeoutMs,
		stopCh:       make(chan struct{}),
	}
	s.ctx, _ = context.WithTimeout(context.Background(), time.Duration(s.timeoutMs)*time.Millisecond*2)
	go s.scheduleGenContext()
	http.HandleFunc("/singleContext/setTimeout", s.handleSetTimeout)
	return
}

func (s *SingleContext) scheduleGenContext() {
	t := time.NewTicker(time.Duration(s.timeoutMs) * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			s.ctx, _ = context.WithTimeout(context.Background(), time.Duration(s.timeoutMs)*time.Millisecond*2)
		case <-s.stopCh:
			return
		}
	}
}

func (s *SingleContext) GetContextWithTimeout() (ctx context.Context) {
	return s.ctx
}

func (s *SingleContext) Stop() {
	close(s.stopCh)
}

func (s *SingleContext) GetTimeoutMs() int {
	return s.timeoutMs
}

func (s *SingleContext) handleSetTimeout(w http.ResponseWriter, r *http.Request) {
	var err error
	defer func() {
		if err != nil {
			w.Write([]byte(err.Error()))
		}
	}()
	val := r.FormValue("ms")
	if val == "" {
		err = fmt.Errorf("parameter ms is empty")
		return
	}
	ms, err := strconv.Atoi(val)
	if err != nil {
		return
	}
	if ms > s.maxTimeoutMs {
		err = fmt.Errorf("ms can not be more than %v", s.maxTimeoutMs)
		return
	}
	if ms < s.minTimeoutMs {
		err = fmt.Errorf("ms can not be less than %v", s.minTimeoutMs)
		return
	}
	old := s.timeoutMs
	s.timeoutMs = ms
	w.Write([]byte(fmt.Sprintf("set cache read timeout from %v to %v success", old, ms)))
}
