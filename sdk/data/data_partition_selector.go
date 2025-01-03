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
	"errors"
	"strings"
	"sync"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

// This type defines the constructor used to create and initialize the selector.
type DataPartitionSelectorConstructor = func(param *DpSelectorParam) (DataPartitionSelector, error)

// DataPartitionSelector is the interface defines the methods necessary to implement
// a selector for data partition selecting.
type DataPartitionSelector interface {
	// Name return name of current selector instance.
	Name() string

	// Refresh refreshes current selector instance by specified data partitions.
	// sameZoneIndex is the len of sameZoneDps which ranked at front of partitions
	Refresh(partitions []*DataPartition, sameZoneIndex int) error

	// Select returns a data partition picked by selector.
	Select(sameZone bool) (*DataPartition, error)

	// RemoveDpForWrite removes specified data partition.
	RemoveDpForWrite(partitionID uint64)

	RemoveDpForOverWrite(partitionID uint64)

	RangeRemoveDpForOverWrite(checkFunc func(partitionID uint64) bool) (leastCheckCount int)

	// RemoveHost removes specified host.
	RemoveHost(host string)

	// ClearRemoveInfo clear all the removed dp and host.
	ClearRemoveInfo()

	DpAvailableForOverWrite(dp *DataPartition) bool

	// SummaryMetrics summaries the metrics of dp write operate
	SummaryMetrics() []*proto.DataPartitionMetrics

	// RefreshMetrics refreshes the metrics of dp write operate
	RefreshMetrics(enableRemote bool, dpMetrics map[uint64]*proto.DataPartitionMetrics) error
}

type DpSelectorParam struct {
	kValue string
	quorum int
}

var (
	dataPartitionSelectorConstructors = make(map[string]DataPartitionSelectorConstructor)

	ErrDuplicatedDataPartitionSelectorConstructor = errors.New("duplicated data partition selector constructor")
	ErrDataPartitionSelectorConstructorNotExist   = errors.New("data partition selector constructor not exist")
)

// RegisterDataPartitionSelector registers a selector constructor.
// Users can register their own defined selector through this method.
func RegisterDataPartitionSelector(name string, constructor DataPartitionSelectorConstructor) error {
	var clearName = strings.TrimSpace(strings.ToLower(name))
	if _, exist := dataPartitionSelectorConstructors[clearName]; exist {
		return ErrDuplicatedDataPartitionSelectorConstructor
	}
	dataPartitionSelectorConstructors[clearName] = constructor
	return nil
}

func newDataPartitionSelector(name string, param *DpSelectorParam) (newDpSelector DataPartitionSelector, err error) {
	var clearName = strings.TrimSpace(strings.ToLower(name))
	constructor, exist := dataPartitionSelectorConstructors[clearName]
	if !exist {
		return nil, ErrDataPartitionSelectorConstructorNotExist
	}
	return constructor(param)
}

type BaseSelector struct {
	removeDpForWrite     sync.Map // map[dpid]struct{}
	removeDpForOverWrite sync.Map // map[dpid]checkCount
	removeHost           sync.Map // map[host]struct{}
}

func (s *BaseSelector) RemoveDpForWrite(partitionID uint64) {
	s.removeDpForWrite.Store(partitionID, struct{}{})
	log.LogWarnf("RemoveDpForWrite: dpId(%v)", partitionID)
	return
}

func (s *BaseSelector) RemoveDpForOverWrite(partitionID uint64) {
	if _, ok := s.removeDpForOverWrite.Load(partitionID); !ok {
		s.removeDpForOverWrite.Store(partitionID, 0)
	}
	log.LogWarnf("RemoveDpForOverWrite: dpId(%v)", partitionID)
	return
}

func (s *BaseSelector) RangeRemoveDpForOverWrite(checkFunc func(uint64) bool) (leastCheckCount int) {
	s.removeDpForOverWrite.Range(func(key, value interface{}) bool {
		dpId := key.(uint64)
		count := value.(int)
		if checkFunc(dpId) {
			s.removeDpForOverWrite.Delete(dpId)
			return true
		}
		if count+1 < leastCheckCount {
			leastCheckCount = count + 1
			s.removeDpForOverWrite.Store(dpId, count+1)
		}
		return true
	})
	return
}

func (s *BaseSelector) RemoveHost(host string) {
	s.removeHost.Store(host, struct{}{})
	log.LogWarnf("RemoveHost: host(%v)", host)
	return
}

func (s *BaseSelector) ClearRemoveInfo() {
	s.removeDpForWrite.Range(func(key, value interface{}) bool {
		s.removeDpForWrite.Delete(key)
		return true
	})
	s.removeDpForOverWrite.Range(func(key, value interface{}) bool {
		s.removeDpForOverWrite.Delete(key)
		return true
	})
	s.removeHost.Range(func(key, value interface{}) bool {
		s.removeHost.Delete(key)
		return true
	})
}

func (s *BaseSelector) DpAvailableForOverWrite(dp *DataPartition) bool {
	if _, ok := s.removeDpForOverWrite.Load(dp.PartitionID); ok {
		return false
	}
	for host := range dp.Hosts {
		if _, ok := s.removeHost.Load(host); ok {
			return false
		}
	}
	return true
}

func (w *Wrapper) initDpSelector() (err error) {
	w.dpSelectorChanged = false
	var selectorName = w.dpSelectorName
	if strings.TrimSpace(selectorName) == "" {
		log.LogDebugf("initDpSelector: can not find dp selector[%v], use default selector", w.dpSelectorName)
		selectorName = DefaultRandomSelectorName
	}
	dpSelectorParam := &DpSelectorParam{
		kValue: w.dpSelectorParm,
		quorum: w.quorum,
	}
	var selector DataPartitionSelector
	if selector, err = newDataPartitionSelector(selectorName, dpSelectorParam); err != nil {
		log.LogErrorf("initDpSelector: dpSelector[%v] init failed caused by [%v], use default selector", w.dpSelectorName, err)
		selectorName = DefaultRandomSelectorName
		if selector, err = newDataPartitionSelector(selectorName, dpSelectorParam); err != nil {
			return
		}
	}
	w.dpSelector = selector
	return
}

func (w *Wrapper) refreshDpSelector(partitions []*DataPartition, sameZoneIndex int) {
	w.RLock()
	dpSelector := w.dpSelector
	dpSelectorChanged := w.dpSelectorChanged
	w.RUnlock()

	if dpSelectorChanged {
		var selectorName = w.dpSelectorName
		if strings.TrimSpace(selectorName) == "" {
			log.LogWarnf("refreshDpSelector: can not find dp selector[%v], use default selector", w.dpSelectorName)
			selectorName = DefaultRandomSelectorName
		}
		dpSelectorParam := &DpSelectorParam{
			kValue: w.dpSelectorParm,
			quorum: w.quorum,
		}
		newDpSelector, err := newDataPartitionSelector(selectorName, dpSelectorParam)
		if err != nil {
			log.LogErrorf("refreshDpSelector: change dpSelector to [%v %v %v] failed caused by [%v],"+
				" use last valid selector. Please change dpSelector config through master.",
				w.dpSelectorName, w.dpSelectorParm, w.quorum, err)
		} else {
			_ = newDpSelector.Refresh(partitions, sameZoneIndex)
			w.Lock()
			log.LogInfof("refreshDpSelector: change dpSelector to [%v %v %v]", w.dpSelectorName, w.dpSelectorParm, w.quorum)
			w.dpSelector = newDpSelector
			w.dpSelectorChanged = false
			w.Unlock()
			return
		}
	}

	_ = dpSelector.Refresh(partitions, sameZoneIndex)
}

// getDpForWrite returns an available data partition for write.
func (w *Wrapper) getDpForWrite() (*DataPartition, error) {
	w.RLock()
	dpSelector := w.dpSelector
	w.RUnlock()

	return dpSelector.Select(w.IsSameZoneReadHAType())
}

func (w *Wrapper) removeDpForWrite(partitionID uint64) {
	w.RLock()
	dpSelector := w.dpSelector
	w.RUnlock()

	dpSelector.RemoveDpForWrite(partitionID)
}

func (w *Wrapper) removeDpForOverWrite(partitionID uint64) {
	w.RLock()
	dpSelector := w.dpSelector
	w.RUnlock()

	dpSelector.RemoveDpForOverWrite(partitionID)
}

func (w *Wrapper) removeHostForWrite(host string) {
	w.RLock()
	dpSelector := w.dpSelector
	w.RUnlock()

	dpSelector.RemoveHost(host)
}

func (w *Wrapper) clearRemoveInfo() {
	w.RLock()
	dpSelector := w.dpSelector
	w.RUnlock()

	dpSelector.ClearRemoveInfo()
}

func (w *Wrapper) dpAvailableForOverWrite(dp *DataPartition) bool {
	w.RLock()
	dpSelector := w.dpSelector
	w.RUnlock()

	return dpSelector.DpAvailableForOverWrite(dp)
}

func (w *Wrapper) checkDpForOverWrite() int {
	return w.dpSelector.RangeRemoveDpForOverWrite(func(partitionID uint64) bool {
		dp, err := w.GetDataPartition(partitionID)
		if err == nil {
			err = dp.OverWriteDetect()
		}
		return err == nil
	})
}

func (w *Wrapper) RefreshDataPartitionMetrics(enableRemote bool, dpMetricsMap map[uint64]*proto.DataPartitionMetrics) error {
	w.RLock()
	dpSelector := w.dpSelector
	w.RUnlock()

	return dpSelector.RefreshMetrics(enableRemote, dpMetricsMap)
}

func (w *Wrapper) SummaryDataPartitionMetrics() []*proto.DataPartitionMetrics {
	w.RLock()
	dpSelector := w.dpSelector
	w.RUnlock()

	return dpSelector.SummaryMetrics()
}
