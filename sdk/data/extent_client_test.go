package data

import (
	"math"
	"sync"
	"testing"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/unit"
	"github.com/stretchr/testify/assert"
	"golang.org/x/time/rate"
)

func TestSetExtentSize(t *testing.T) {
	caseSetExSize := []struct {
		name string
		size int
		want int
	}{
		{
			name: "<0",
			size: -1,
			want: 128 * 1024,
		},
		{
			name: "0",
			size: 0,
			want: 128 * 1024 * 1024,
		},
		{
			name: "<128K",
			size: 64 * 1024,
			want: 128 * 1024,
		},
		{
			name: "128K~128M, normal",
			size: 128 * 1024 * 64,
			want: 128 * 1024 * 64,
		},
		{
			name: "128K~128M, not power of 2",
			size: 128 * 1024 * 64 * 3,
			want: 128 * 1024 * 64 * 4,
		},
		{
			name: ">128M",
			size: 128 * 1024 * 1024 * 2,
			want: 128 * 1024 * 1024,
		},
		{
			name: "MaxInt64",
			size: math.MaxInt64,
			want: 128 * 1024 * 1024,
		},
	}
	for _, tt := range caseSetExSize {
		t.Run(tt.name, func(t *testing.T) {
			if ec.SetExtentSize(tt.size); ec.extentSize != tt.want {
				t.Fatalf("set[%v], want[%v], but got[%v]",
					tt.size, tt.want, ec.extentSize)
			}
		})
	}
}

func TestRateLimit(t *testing.T) {
	log.SetLogLevel(log.WarnLevel)
	defer log.SetLogLevel(log.DebugLevel)

	file := "TestRateLimit"
	info, _ := create(file)
	ec.OpenStream(info.Inode, false, false)
	data := []byte("a")
	offset := uint64(unit.DefaultTinySizeLimit)

	// limited by 100 op/s for writing
	limit := map[string]string{proto.VolumeKey: ltptestVolume, proto.ClientWriteVolRateKey: "100"}
	err := mc.AdminAPI().SetRateLimitWithMap(limit)
	assert.Nil(t, err)
	ec.updateConfig(true)
	assert.Equal(t, rate.Limit(100), ec.writeLimiter.Limit())
	// consume burst first
	for i := 0; i < 100; i++ {
		ec.Write(ctx, info.Inode, offset, data, false)
		offset++
	}
	begin := time.Now()
	for i := 0; i < 101; i++ {
		ec.Write(ctx, info.Inode, offset, data, false)
		offset++
	}
	cost := time.Since(begin)
	assert.True(t, cost > time.Second)

	// not limited for writing
	limit[proto.ClientWriteVolRateKey] = "0"
	err = mc.AdminAPI().SetRateLimitWithMap(limit)
	assert.Nil(t, err)
	ec.updateConfig(true)
	assert.Equal(t, rate.Inf, ec.writeLimiter.Limit())
	begin = time.Now()
	for i := 0; i < 200; i++ {
		ec.Write(ctx, info.Inode, offset, data, false)
		offset++
	}
	cost = time.Since(begin)
	assert.True(t, cost < 5*time.Millisecond)

	// not limited if op count is not more than burst
	limit[proto.ClientWriteVolRateKey] = "1000"
	err = mc.AdminAPI().SetRateLimitWithMap(limit)
	assert.Nil(t, err)
	ec.updateConfig(true)
	assert.Equal(t, rate.Limit(1000), ec.writeLimiter.Limit())
	// wait limiter to fill burst
	time.Sleep(time.Second)
	begin = time.Now()
	for i := 0; i < 500; i++ {
		ec.Write(ctx, info.Inode, offset, data, false)
		offset++
	}
	cost = time.Since(begin)
	assert.True(t, cost < 5*time.Millisecond)

	limit[proto.ClientWriteVolRateKey] = "0"
	err = mc.AdminAPI().SetRateLimitWithMap(limit)
	assert.Nil(t, err)
	ec.updateConfig(true)
	assert.Equal(t, rate.Inf, ec.writeLimiter.Limit())
}

// with OverWriteBuffer enabled, ek of prepared request may have been modified by ROW, resulting data loss
func TestOverWriteBuffer(t *testing.T) {
	info, err := create("TestOverWriteBuffer")
	ino := info.Inode
	ec.OpenStream(ino, true, false)
	streamer := ec.GetStreamer(ino)
	data0 := make([]byte, 6)
	data1 := []byte{1, 2, 3}
	_, _, err = ec.Write(ctx, ino, 0, data0, false)
	assert.Nil(t, err)
	err = ec.Flush(ctx, ino)
	assert.Nil(t, err)

	_, _, err = ec.Write(ctx, ino, 0, data1, false)
	assert.Nil(t, err)
	ec.dataWrapper.forceROW = true
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		err = ec.Flush(ctx, ino)
		assert.Nil(t, err)
		wg.Done()
	}()
	data2 := []byte{4, 5, 6}
	// following write should happen after overWriteReq has been taken, and before the new ek inserted
	for len(streamer.overWriteReq) > 0 {
		time.Sleep(time.Millisecond)
	}
	ec.Write(ctx, ino, 0, data2, false)
	wg.Wait()
	ec.dataWrapper.forceROW = false
	err = ec.Flush(ctx, ino)
	assert.Nil(t, err)
	_, _, err = ec.Read(ctx, ino, data1, 0, 3)
	assert.Nil(t, err)
	assert.Equal(t, data2, data1)
}

func BenchmarkExtentClient(b *testing.B) {
	info, _ := create("BenchmarkExtentClient")
	ino := info.Inode
	ec.OpenStream(ino, false, false)
	bs := 16 * 1024
	data := make([]byte, bs)
	b.Run("write", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ec.Write(ctx, ino, uint64(bs*i), data, false)
			ec.Flush(ctx, ino)
		}
		b.ReportAllocs()
	})
	b.Run("overwrite", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ec.Write(ctx, ino, uint64(bs*i), data, false)
		}
		b.ReportAllocs()
	})
	b.Run("read", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			ec.Read(ctx, ino, data, uint64(bs*i), bs)
		}
		b.ReportAllocs()
	})
}
