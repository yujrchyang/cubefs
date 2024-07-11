// Copyright 2020 The CubeFS Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package data

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

const (
	DefaultRandomSelectorName = "default"
)

func init() {
	_ = RegisterDataPartitionSelector(DefaultRandomSelectorName, newDefaultRandomSelector)
}

func newDefaultRandomSelector(param *DpSelectorParam) (selector DataPartitionSelector, e error) {
	selector = &DefaultRandomSelector{
		partitions: make([]*DataPartition, 0),
		param:      param,
	}
	return
}

type DefaultRandomSelector struct {
	sync.RWMutex
	BaseSelector
	partitions []*DataPartition
	param      *DpSelectorParam
}

func (s *DefaultRandomSelector) Name() string {
	return DefaultRandomSelectorName
}

func (s *DefaultRandomSelector) Refresh(partitions []*DataPartition) (err error) {
	s.Lock()
	s.partitions = partitions
	s.Unlock()

	s.ClearRemoveInfo()
	return
}

func (s *DefaultRandomSelector) Select() (dp *DataPartition, err error) {
	s.RLock()
	partitions := s.partitions
	s.RUnlock()

	dp = s.getRandomDataPartition(partitions)
	if dp == nil {
		err = fmt.Errorf("no writable data partition")
	}
	return
}

func (s *DefaultRandomSelector) SummaryMetrics() []*proto.DataPartitionMetrics {
	return nil
}

func (s *DefaultRandomSelector) RefreshMetrics(enableRemote bool, dpMetrics map[uint64]*proto.DataPartitionMetrics) error {
	s.RLock()
	partitions := s.partitions
	s.RUnlock()

	if len(partitions) == 0 {
		return fmt.Errorf("no writable data partition")
	}
	for _, dp := range partitions {
		dp.LocalMetricsClear()
	}
	return nil
}

func (s *DefaultRandomSelector) getRandomDataPartition(partitions []*DataPartition) (dp *DataPartition) {
	length := len(partitions)
	if length == 0 {
		return nil
	}

	rand.Seed(time.Now().UnixNano())
	index := rand.Intn(length)
	dp = partitions[index]
	_, removed := s.removeDpForWrite.Load(dp.PartitionID)
	exclude := make(map[string]struct{})
	s.removeHost.Range(func(key, value interface{}) bool {
		exclude[key.(string)] = struct{}{}
		return true
	})
	if !isExcludedByHost(dp, exclude, s.param.quorum) && !removed {
		log.LogDebugf("DefaultRandomSelector: select dp[%v], index %v", dp, index)
		return dp
	}

	log.LogWarnf("DefaultRandomSelector: first random partition was excluded, get partition from others")

	var currIndex int
	for i := 0; i < length; i++ {
		currIndex = (index + i) % length
		_, removed := s.removeDpForWrite.Load(partitions[currIndex].PartitionID)
		if !isExcludedByHost(partitions[currIndex], exclude, s.param.quorum) && !removed {
			log.LogDebugf("DefaultRandomSelector: select dp[%v], index %v", partitions[currIndex], currIndex)
			return partitions[currIndex]
		}
	}
	return nil
}
