package cfs

import "time"

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

// actually there is only one host
func (s *ChubaoFSMonitor) rangeAllHosts(iterator func(host *ClusterHost)) {
	for _, host := range s.hosts {
		iterator(host)
	}
}
