package cfs

import "time"

// NewSchedule 侧重控制任务开始时间
func (s *ChubaoFSMonitor) NewSchedule(task func(), interval time.Duration) {
	task()
	ticker := time.NewTicker(interval)
	defer func() {
		ticker.Stop()
	}()
	for {
		select {
		case <-ticker.C:
			task()
		}
	}
}

// NewScheduleV2 侧重控制任务间隔时间
func (s *ChubaoFSMonitor) NewScheduleV2(task func(), interval time.Duration) {
	task()
	ticker := time.NewTimer(interval)
	defer func() {
		ticker.Stop()
	}()
	for {
		select {
		case <-ticker.C:
			task()
			ticker.Reset(interval)
		}
	}
}

// actually there is only one host
func (s *ChubaoFSMonitor) rangeAllHosts(iterator func(host *ClusterHost)) {
	for _, host := range s.hosts {
		iterator(host)
	}
}
