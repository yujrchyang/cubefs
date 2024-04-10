package common

import (
	"context"
	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

func TestMarshalPb(t *testing.T) {
	req := &proto.CachePrepareRequest{
		CacheRequest: &proto.CacheRequest{
			Volume:          "test",
			Inode:           1,
			FixedFileOffset: 1000,
			Version:         1024,
			TTL:             90,
			Sources: []*proto.DataSource{
				{
					FileOffset:   1,
					PartitionID:  2,
					ExtentID:     3,
					ExtentOffset: 4,
					Size_:        -1,
					Hosts: []string{
						"1.1.1.1:80",
						"1.1.1.2:80",
					},
				},
			},
		},
		FlashNodes: make([]string, 0),
	}
	req2 := &proto.CachePrepareRequest{
		CacheRequest: nil,
		FlashNodes:   make([]string, 0),
	}
	for j := 0; j < 10; j++ {
		wg := sync.WaitGroup{}
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				p := NewCachePacket(context.Background(), 0, proto.OpCachePrepare)
				assert.NoError(t, p.MarshalDataPb(req))
				assert.NoError(t, p.MarshalDataPb(req2))
				time.Sleep(time.Second)
			}()
		}
		wg.Wait()
		t.Logf("round: %v", j)
	}
}
