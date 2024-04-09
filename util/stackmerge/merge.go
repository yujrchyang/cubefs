package stackmerge

import (
	"container/list"
	"fmt"
	"strings"
	"sync"
)

// Stack stack[0]-start, stack[1]-end
type Stack [2]uint64

func (v *Stack) String() string {
	return fmt.Sprintf("[%v,%v]", v[0], v[1])
}

type StackList struct {
	sync.RWMutex
	list *list.List
}

func (s *StackList) String() string {
	sb := strings.Builder{}
	for r := s.list.Front(); r != nil; r = r.Next() {
		v := r.Value.(*Stack)
		sb.WriteString(fmt.Sprintf("%v,", v))
	}
	if sb.Len() == 0 {
		return ""
	}
	return strings.TrimSuffix(sb.String(), ",")
}

func NewStackList() *StackList {
	return &StackList{
		list: list.New(),
	}
}

func (s *StackList) Put(v *Stack) (*list.Element, error) {
	s.Lock()
	defer s.Unlock()
	if s.list.Len() == 0 {
		return s.list.PushBack(v), nil
	}
	for r := s.list.Front(); r != nil; r = r.Next() {
		cursor := r.Value.(*Stack)
		switch {
		case overLap(cursor, v):
			return nil, fmt.Errorf("stack overlap")
		case cursor[0] == v[1]:
			cursor[0] = v[0]
			return r, nil
		case cursor[1] == v[0]:
			cursor[1] = v[1]
			return r, nil
		case cursor[0] > v[1]:
			return s.list.InsertBefore(v, r), nil
		case cursor[1] < v[0]:
			if r.Next() == nil {
				return s.list.InsertAfter(v, r), nil
			}
		}
	}
	return nil, fmt.Errorf("unknown error")
}

func (s *StackList) Len() int {
	s.RLock()
	defer s.RUnlock()
	return s.list.Len()
}

func (s *StackList) Stacks() []*Stack {
	s.RLock()
	defer s.RUnlock()
	stacks := make([]*Stack, 0)
	for r := s.list.Front(); r != nil; r = r.Next() {
		v := r.Value.(*Stack)
		stacks = append(stacks, v)
	}
	return stacks
}

func (s *StackList) IsCover(start, end uint64) bool {
	s.RLock()
	defer s.RUnlock()
	for r := s.list.Front(); r != nil; r = r.Next() {
		cursor := r.Value.(*Stack)
		if start >= cursor[0] && end <= cursor[1] {
			return true
		}
	}
	return false
}

func overLap(a *Stack, b *Stack) bool {
	if a == nil || b == nil {
		return false
	}
	if a[0] >= b[1] || b[0] >= a[1] {
		return false
	}
	return true
}
