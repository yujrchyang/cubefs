package tcp

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestLoadMetaPartition(t *testing.T) {
	t.Skipf("skip")
	t.Run("dbbak", func(t *testing.T) {
		mpr, err := loadMetaPartition(true, 1, "")
		if !assert.NoError(t, err) {
			return
		}
		fmt.Printf("meta partition info:%v", mpr)
	})

	t.Run("spark", func(t *testing.T) {
		mpr, err := loadMetaPartition(false, 1, "")
		if !assert.NoError(t, err) {
			return
		}
		fmt.Printf("meta partition info:%v", mpr)
	})
}
