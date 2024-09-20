package cfs

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestScheduler(t *testing.T) {
	monitor := NewChubaoFSMonitor(context.Background())
	monitor.NewSchedule(func() {
		time.Sleep(time.Second * 10)
		fmt.Printf("echo schedule\n")
	}, time.Second*1)
}
