package stackmerge

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMerge(t *testing.T) {
	stack := NewStackList()
	stacks := []*Stack{
		{1, 2},
		{9, 21},
		{2, 4},
		{5, 8},
		{70, 80},
		{80, 90},
	}
	expectLen := 4
	for _, s := range stacks {
		_, err := stack.Put(s)
		if !assert.NoError(t, err) {
			return
		}
	}
	fmt.Printf("%v", stack)
	if !assert.Equal(t, stack.Len(), expectLen) {
		return
	}

	badStacks := []*Stack{
		{7, 20},
		{60, 100},
	}
	for _, s := range badStacks {
		_, err := stack.Put(s)
		if !assert.Error(t, err) {
			return
		}
	}
}

func TestCover(t *testing.T) {
	stack := NewStackList()
	stacks := []*Stack{
		{1, 2},
		{9, 21},
		{2, 4},
		{5, 8},
		{70, 80},
		{80, 90},
	}
	for _, s := range stacks {
		_, err := stack.Put(s)
		if !assert.NoError(t, err) {
			return
		}
	}
	coverTests := []*Stack{
		{3, 4},
		{14, 20},
		{70, 75},
	}
	unCoverTests := []*Stack{
		{3, 6},
		{100, 120},
	}
	for _, c := range coverTests {
		if !assert.True(t, stack.IsCover(c[0], c[1])) {
			return
		}
	}
	for _, c := range unCoverTests {
		if !assert.False(t, stack.IsCover(c[0], c[1])) {
			return
		}
	}
}
