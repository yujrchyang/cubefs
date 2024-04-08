package datanode

import "sync"

type WALApplyStatus struct {
	applied   uint64
	truncated uint64

	mu sync.RWMutex
}

func (s *WALApplyStatus) Init(applied, truncated uint64) (success bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if applied == 0 || (applied != 0 && applied >= truncated) {
		s.applied, s.truncated = applied, truncated
		success = true
	}
	return
}

func (s *WALApplyStatus) AdvanceApplied(id uint64) (snap WALApplyStatus, success bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.applied < id && s.truncated <= id {
		s.applied = id
		success = true
	}
	snap = WALApplyStatus{
		applied:   s.applied,
		truncated: s.truncated,
	}
	return
}

func (s *WALApplyStatus) Applied() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.applied
}

func (s *WALApplyStatus) AdvanceTruncated(id uint64) (snap WALApplyStatus, success bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.truncated < id && id <= s.applied {
		s.truncated = id
		success = true
	}
	snap = WALApplyStatus{
		applied:   s.applied,
		truncated: s.truncated,
	}
	return
}

func (s *WALApplyStatus) Truncated() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.truncated
}

func (s *WALApplyStatus) Snap() *WALApplyStatus {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return &WALApplyStatus{
		applied:   s.applied,
		truncated: s.truncated,
	}
}

func NewWALApplyStatus() *WALApplyStatus {
	return &WALApplyStatus{}
}