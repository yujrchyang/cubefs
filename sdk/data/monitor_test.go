package data

import (
	"github.com/cubefs/cubefs/util/statistics"
	"testing"
)

var streamer = initTestStreamer()

func initTestStreamer() *Streamer {
	s := new(Streamer)
	client := &ExtentClient{
		dataWrapper: &Wrapper{
			HostsStatus: make(map[string]hostStatus),
		},
	}
	client.monitorStatistics.LoadOrStore("unknown", statistics.InitMonitorData(statistics.ModelClient))
	s.client = client
	return s
}

func BenchmarkStreamer_UpdateRead(b *testing.B) {
	for i := 0; i < b.N; i++ {
		streamer.UpdateRead("127.0.0.1", 4096)
	}
	b.ReportAllocs()
}
