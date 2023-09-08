package cache_engine

import (
	"github.com/cubefs/cubefs/util/tmpfs"
	"github.com/cubefs/cubefs/util/unit"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

const (
	testTmpFS = "/cfs_test/tmpfs"
)

func TestFileStore_WriteAt_ReadAt(t *testing.T) {
	assert.Nil(t, initTestTmpfs(200*unit.MB))
	defer func() {
		assert.Nil(t, tmpfs.Umount(testTmpFS))
	}()
	fs, err := NewFileStore(testTmpFS + "/cache_data")
	if !assert.NoError(t, err) {
		return
	}
	defer fs.Close()
	data := make([]byte, 0)
	for i := 0; uint64(i) < 128*unit.KB; i++ {
		data = append(data, 'b')
	}
	_, err = fs.WriteAt(data, 0)
	if !assert.NoError(t, err) {
		return
	}
	readD := make([]byte, 128*unit.KB)
	_, err = fs.ReadAt(readD, 0)
	if !assert.NoError(t, err) {
		return
	}
}

func TestMemoryStore_WriteAt_ReadAt(t *testing.T) {
	ms := NewMemoryStore(128 * unit.KB)
	data := make([]byte, 0)
	for i := 0; uint64(i) < 128*unit.KB; i++ {
		data = append(data, 'b')
	}
	_, err := ms.WriteAt(data, 0)
	if !assert.NoError(t, err) {
		return
	}
	readD := make([]byte, 128*unit.KB)
	_, err = ms.ReadAt(readD, 0)
	if !assert.NoError(t, err) {
		return
	}
}

func BenchmarkFileStore(b *testing.B) {
	assert.Nil(b, initTestTmpfs(200*unit.MB))
	defer func() {
		assert.Nil(b, tmpfs.Umount(testTmpFS))
	}()
	fs, err := NewFileStore(testTmpFS + "/cache_data")
	if !assert.NoError(b, err) {
		return
	}
	defer fs.Close()
	data := make([]byte, 0)
	for i := 0; uint64(i) < 128*unit.KB; i++ {
		data = append(data, 'b')
	}
	b.Run("file-store-write", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			fs.WriteAt(data, 0)
			b.ReportMetric(float64(len(data)), "bytes/op")
		}
		b.ReportAllocs()
	})
	b.Run("file-store-read", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			fs.ReadAt(data, 0)
			b.ReportMetric(float64(len(data)), "bytes/op")
		}
		b.ReportAllocs()
	})
}

func BenchmarkMemoryStore(b *testing.B) {
	ms := NewMemoryStore(128 * unit.KB)
	data := make([]byte, 0)
	for i := 0; uint64(i) < 128*unit.KB; i++ {
		data = append(data, 'b')
	}
	b.Run("memory-store-write", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ms.WriteAt(data, 0)
			b.ReportMetric(float64(len(data)), "bytes/op")
		}
		b.ReportAllocs()
	})
	b.Run("memory-store-read", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ms.ReadAt(data, 0)
			b.ReportMetric(float64(len(data)), "bytes/op")
		}
		b.ReportAllocs()
	})
}

func initTestTmpfs(size int64) (err error) {
	_, err = os.Stat(testTmpFS)
	if err == nil {
		if tmpfs.IsTmpfs(testTmpFS) {
			if err = tmpfs.Umount(testTmpFS); err != nil {
				return err
			}
		}
	} else {
		if !os.IsNotExist(err) {
			return
		}
		_ = os.MkdirAll(testTmpFS, 0777)
		err = nil
	}
	err = tmpfs.MountTmpfs(testTmpFS, size)
	return
}
