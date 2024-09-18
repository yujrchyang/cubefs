package data

import (
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
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

func TestGetRate(t *testing.T) {
	assert.NotEmpty(t, ec.GetRate())
}

func TestSetReadRate(t *testing.T) {
	assert.NotEmpty(t, ec.SetReadRate(10))
	assert.NotEmpty(t, ec.SetReadRate(0))
}

func TestSetWriteRate(t *testing.T) {
	assert.NotEmpty(t, ec.SetWriteRate(10))
	assert.NotEmpty(t, ec.SetWriteRate(0))
}

// with OverWriteBuffer enabled, ek of prepared request may have been modified by ROW, resulting data loss
func TestOverWriteBuffer(t *testing.T) {
	info, err := create("TestOverWriteBuffer")
	ino := info.Inode
	ec.OpenStream(ino, true)
	streamer := ec.GetStreamer(ino)
	data0 := make([]byte, 6)
	data1 := []byte{1, 2, 3}
	ctx := context.Background()
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
