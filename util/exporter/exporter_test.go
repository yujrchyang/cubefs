package exporter

import (
	"testing"
	"time"
)

func init() {
	Init(NewOption().WithCluster("spark").WithModule("client").WithUmpFilePrefix("spark_client"))
}

func BenchmarkExporter(b *testing.B) {
	for n := 0; n < b.N; n++ {
		metric := NewModuleTPUsWithStart("read", time.Now())
		metric.Set(nil)
	}
}
