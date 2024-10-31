// Copyright 2018 The CubeFS Authors.
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

package exporter

import (
	"fmt"
	"strings"
	"time"

	"github.com/cubefs/cubefs/util/exporter/backend/prom"
	"github.com/cubefs/cubefs/util/exporter/backend/ump"
)

var (
	unspecifiedTime = time.Time{}
	promKeyReplacer = strings.NewReplacer("-", "_", ".", "_", " ", "_", ",", "_", ":", "_")
	separater       = "_"
)

type TP interface {
	Set(err error)
	SetWithCount(value int64, err error)
	SetWithCost(cost int64, err error)
}

type promTP struct {
	tp        prom.Summary
	failure   prom.Counter
	start     time.Time
	precision ump.UMPTPPrecision
}

func (tp *promTP) Set(err error) {
	if tp == nil {
		return
	}
	if tp.tp != nil {
		tp.tp.Observe(float64(time.Since(tp.start).Nanoseconds()))
	}
	if tp.failure != nil {
		if err != nil {
			tp.failure.Add(1)
		} else {
			tp.failure.Add(0)
		}
	}
}

func (tp *promTP) SetWithCount(value int64, err error) {
	if tp == nil {
		return
	}
	if tp.tp != nil {
		tp.tp.Observe(float64(time.Since(tp.start).Nanoseconds()))
	}
	if tp.failure != nil {
		if err != nil {
			tp.failure.Add(1)
		} else {
			tp.failure.Add(0)
		}
	}
}

func (tp *promTP) SetWithCost(cost int64, err error) {
	if tp == nil {
		return
	}
	if tp.tp != nil {
		if tp.precision == ump.PrecisionMs {
			cost = cost * int64(time.Millisecond)
		} else if tp.precision == ump.PrecisionUs {
			cost = cost * int64(time.Microsecond)
		}
		tp.tp.Observe(float64(cost))
	}
	if tp.failure != nil {
		if err != nil {
			tp.failure.Add(1)
		} else {
			tp.failure.Add(0)
		}
	}
}

func newPromTP(name string, start time.Time, precision ump.UMPTPPrecision, lvs ...prom.LabelValue) TP {
	name = promKeyReplacer.Replace(name)
	var tp = &promTP{
		tp:        prom.GetSummary(name, lvs...),
		failure:   prom.GetCounter(fmt.Sprintf("%s_failure", name), lvs...),
		precision: precision,
	}
	if start == unspecifiedTime {
		tp.start = time.Now()
		return tp
	}
	tp.start = start
	return tp
}

func newUmpTP(key string, start time.Time, precision ump.UMPTPPrecision) TP {
	if start == unspecifiedTime {
		return ump.BeforeTP(key, precision)
	} else {
		return ump.BeforeTPWithStartTime(key, precision, start)
	}
}

type noonTP struct{}

func (tp *noonTP) Set(_ error) {
	return
}

func (tp *noonTP) SetWithCount(value int64, err error) {
	return
}

func (tp *noonTP) SetWithCost(value int64, err error) {
	return
}

func (tp *noonTP) SetWithCostUS(value int64, err error) {
	return
}

var singletonNoonTP = &noonTP{}

type multipleTP []TP

func (tp multipleTP) Set(err error) {
	for _, recorder := range tp {
		recorder.Set(err)
	}
}

func (tp multipleTP) SetWithCount(value int64, err error) {
	for _, recorder := range tp {
		recorder.SetWithCount(value, err)
	}
}

func (tp multipleTP) SetWithCost(value int64, err error) {
	for _, recorder := range tp {
		recorder.SetWithCost(value, err)
	}
}

func newTP(key string, start time.Time, precision ump.UMPTPPrecision) TP {
	if umpEnabled {
		if promEnabled {
			return multipleTP{newUmpTP(key, start, precision), newPromTP(key, start, precision)}
		} else {
			return newUmpTP(key, start, precision)
		}
	} else {
		if promEnabled {
			return newPromTP(key, start, precision)
		} else {
			return singletonNoonTP
		}
	}
}

func newModuleTP(op string, precision ump.UMPTPPrecision, start time.Time) (tp TP) {
	if len(zoneName) > 0 {
		return multipleTP{
			newTP(moduleKey(op), start, precision),
			newTP(zoneKey(op), start, precision),
		}
	}
	return newTP(moduleKey(op), start, precision)
}

func newVolumeTP(op string, volume string, precision ump.UMPTPPrecision) (tp TP) {
	return newTP(volKey(volume, op), unspecifiedTime, precision)
}

func newNodeAndVolumeModuleTP(op, volName string, precision ump.UMPTPPrecision) (tp TP) {
	if len(volName) > 0 {
		return multipleTP{
			newTP(moduleKey(op), unspecifiedTime, precision),
			newTP(volKey(volName, op), unspecifiedTime, precision),
		}
	}
	return newTP(moduleKey(op), unspecifiedTime, precision)
}

func NewModuleTP(op string) TP {
	return newModuleTP(op, ump.PrecisionMs, unspecifiedTime)
}

func NewModuleTPWithStart(op string, start time.Time) TP {
	return newModuleTP(op, ump.PrecisionMs, start)
}

func NewModuleTPUs(op string) TP {
	return newModuleTP(op, ump.PrecisionUs, unspecifiedTime)
}

func NewModuleTPUsWithStart(op string, start time.Time) TP {
	return newModuleTP(op, ump.PrecisionUs, start)
}

func NewVolumeTP(op string, volume string) TP {
	return newVolumeTP(op, volume, ump.PrecisionMs)
}

func NewVolumeTPUs(op string, volume string) TP {
	return newVolumeTP(op, volume, ump.PrecisionUs)
}

func NewNodeAndVolTP(op, volName string) TP {
	return newNodeAndVolumeModuleTP(op, volName, ump.PrecisionMs)
}

func NewCustomKeyTP(key string) TP {
	return newTP(key, unspecifiedTime, ump.PrecisionMs)
}

func NewCustomKeyTPUs(key string) TP {
	return newTP(key, unspecifiedTime, ump.PrecisionUs)
}

func NewCustomKeyTPWithStartTime(key string, start time.Time) TP {
	return newTP(key, start, ump.PrecisionMs)
}

func NewCustomKeyTPUsWithStartTime(key string, start time.Time) TP {
	return newTP(key, start, ump.PrecisionUs)
}

func moduleKey(op string) string {
	var sb strings.Builder
	sb.Grow(64)
	sb.WriteString(clusterName)
	sb.WriteString(separater)
	sb.WriteString(moduleName)
	sb.WriteString(separater)
	sb.WriteString(op)
	return sb.String()
}

func zoneKey(op string) string {
	var sb strings.Builder
	sb.Grow(64)
	sb.WriteString(clusterName)
	sb.WriteString(separater)
	sb.WriteString(zoneName)
	sb.WriteString(separater)
	sb.WriteString(moduleName)
	sb.WriteString(separater)
	sb.WriteString(op)
	return sb.String()
}

func volKey(volume string, op string) string {
	var sb strings.Builder
	sb.Grow(64)
	sb.WriteString(clusterName)
	sb.WriteString(separater)
	sb.WriteString(volume)
	sb.WriteString(separater)
	sb.WriteString(op)
	return sb.String()
}
