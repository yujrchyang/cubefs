package cache

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDentry(t *testing.T) {
	dentryMap := make(map[string]uint64)
	dentryMap["file1"] = 10
	dentryMap["file2"] = 20000
	dentryMap["file3"] = 1677310

	dc := NewDentryCache(60, true)
	for name, ino := range dentryMap {
		dc.Put(name, ino, 0)
	}
	for name, ino := range dentryMap {
		actualIno, _, ok := dc.Get(name)
		assert.Equal(t, true, ok, "get existed dentry")
		assert.Equal(t, ino, actualIno, "get inode ID of dentry")
	}
	dc.Delete("file1")
	_, _, ok := dc.Get("file1")
	assert.Equal(t, false, ok, "get not existed dentry")
	assert.Equal(t, 2, dc.Count(), "get count of dentry cache")
	assert.Equal(t, false, dc.IsEmpty(), "get not empty dentry cache")
	assert.Equal(t, false, dc.IsExpired(), "get valid dentry cache")
	validSec := 1
	dc.ResetExpiration(uint32(validSec))
	assert.Equal(t, true, dc.Expiration() <= time.Now().Unix()+int64(validSec), "the expiration of dentry cache")

	time.Sleep(time.Duration(validSec+1) * time.Second)
	assert.Equal(t, true, dc.IsExpired(), "get invalid dentry cache")
	assert.Equal(t, 2, dc.Count(), "get count of dentry cache")
	_, _, ok = dc.Get("file2")
	assert.Equal(t, false, ok, "get not existed dentry")
	assert.Equal(t, true, dc.IsEmpty(), "get empty dentry cache")
}
