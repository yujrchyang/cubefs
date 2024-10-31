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

package ump

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/cubefs/cubefs/util/log"
)

type CollectMethod int

func (c CollectMethod) String() string {
	switch c {
	case CollectMethodFile:
		return "File"
	case CollectMethodJMTP:
		return "JMTP"
	default:
	}
	return "Unknown"
}

func (c CollectMethod) Valid() bool {
	switch c {
	case CollectMethodFile, CollectMethodJMTP:
		return true
	default:
	}
	return false
}

func (c CollectMethod) Int64() int64 {
	return int64(c)
}

func ParseCollectMethod(v int) CollectMethod {
	return CollectMethod(v)
}

const (
	CollectMethodUnknown CollectMethod = iota
	CollectMethodFile
	CollectMethodJMTP

	defaultCollectMethod = CollectMethodFile
)

type TpObject struct {
	Key       string
	StartTime int64
	precision UMPTPPrecision
}

type UMPTPPrecision int8

const (
	PrecisionMs UMPTPPrecision = iota
	PrecisionUs
)

const (
	TpMethod        = "TP"
	HeartbeatMethod = "Heartbeat"
	FunctionError   = "FunctionError"
)

var (
	HostName      string
	AppName       string
	LogTimeForMat = "20060102150405000"
	AlarmPool     = &sync.Pool{New: func() interface{} {
		return new(BusinessAlarm)
	}}
	TpObjectPool = &sync.Pool{New: func() interface{} {
		return new(TpObject)
	}}
	SystemAlivePool = &sync.Pool{New: func() interface{} {
		return new(SystemAlive)
	}}
	FunctionTpGroupByPool = &sync.Pool{New: func() interface{} {
		return new(FunctionTpGroupBy)
	}}
	enableUmp        = true
	FunctionTPKeyMap sync.Map
	umpCollectMethod CollectMethod
	jmtpWrite        *JmtpWrite
	jmtpWriteMutex   sync.Mutex

	checkUmpWaySleepTime = 10 * time.Second
	writeTpSleepTime     = time.Second
	aliveTickerTime      = 20 * time.Second
	alarmTickerTime      = time.Second
)

func init() {
	umpCollectMethod = CollectMethodFile
}

func InitUmp(module, appName string) (err error) {
	defer func() {
		if err != nil {
			log.LogErrorf("InitUmp err(%v)", err)
		}
	}()
	AppName = appName
	if err = initLogName(module); err != nil {
		return
	}
	backGroudWriteLog()
	return nil
}

func StopUmp() {
	stopLogWriter()
	if jmtpWrite != nil {
		jmtpWrite.stop()
	}
	jmtpWrite = nil
	AlarmPool = nil
	TpObjectPool = nil
	SystemAlivePool = nil
	FunctionTpGroupByPool = nil
}

func BeforeTP(key string, precision UMPTPPrecision) (o *TpObject) {
	if !enableUmp {
		return
	}

	o = TpObjectPool.Get().(*TpObject)
	o.Key = key
	o.StartTime = time.Now().UnixMicro()
	o.precision = precision
	return
}

func BeforeTPWithStartTime(key string, precision UMPTPPrecision, start time.Time) (o *TpObject) {
	if !enableUmp {
		return
	}

	o = TpObjectPool.Get().(*TpObject)
	o.Key = key
	o.StartTime = start.UnixMicro()
	o.precision = precision
	return
}

func (tp *TpObject) Set(err error) {
	tp.set(err, 1, -1)
}

func (tp *TpObject) SetWithCount(count int64, err error) {
	tp.set(err, count, -1)
}

func (tp *TpObject) SetWithCost(cost int64, err error) {
	tp.set(err, 1, cost)
}

func (tp *TpObject) set(err error, count int64, cost int64) {
	if tp == nil || !enableUmp {
		return
	}
	elapsedTime := int64(-1)
	if err == nil {
		if cost < 0 {
			elapsedTime = time.Now().UnixMicro() - tp.StartTime
		} else {
			elapsedTime = cost
		}
		if tp.precision == PrecisionMs {
			elapsedTime /= 1000
		}
	}
	mergeLogByUMPKey(tp.Key, elapsedTime, count)
	TpObjectPool.Put(tp)
}

func mergeLogByUMPKey(key string, elapsedTime int64, count int64) {
	var elapsedTimeCounter *sync.Map
	timeCountVal, ok := FunctionTPKeyMap.Load(key)
	if !ok {
		elapsedTimeCounter = &sync.Map{}
		elapsedTimeCounter.Store(elapsedTime, &count)
		FunctionTPKeyMap.Store(key, elapsedTimeCounter)
		return
	}
	elapsedTimeCounter = timeCountVal.(*sync.Map)
	countVal, ok := elapsedTimeCounter.Load(elapsedTime)
	if !ok {
		elapsedTimeCounter.Store(elapsedTime, &count)
		return
	}
	atomic.AddInt64(countVal.(*int64), count)
}

func Alive(key string) {
	if !enableUmp {
		return
	}
	alive := SystemAlivePool.Get().(*SystemAlive)
	alive.HostName = HostName
	alive.Key = key
	alive.Time = time.Now().Format(LogTimeForMat)
	ch := SystemAliveLogWrite.logCh
	if GetUmpCollectMethod() == CollectMethodJMTP && jmtpWrite != nil {
		ch = jmtpWrite.aliveCh
	}
	select {
	case ch <- alive:
	default:
	}
}

func Alarm(key, detail string) {
	if !enableUmp {
		return
	}
	alarm := AlarmPool.Get().(*BusinessAlarm)
	alarm.Time = time.Now().Format(LogTimeForMat)
	alarm.Key = key
	alarm.HostName = HostName
	alarm.BusinessType = "0"
	alarm.Value = "0"
	alarm.Detail = detail
	if len(alarm.Detail) > 512 {
		rs := []rune(detail)
		alarm.Detail = string(rs[0:510])
	}

	inflight := &BusinessAlarmLogWrite.inflight
	ch := BusinessAlarmLogWrite.logCh
	if GetUmpCollectMethod() == CollectMethodJMTP && jmtpWrite != nil {
		inflight = &jmtpWrite.inflight
		ch = jmtpWrite.alarmCh
	}
	select {
	case ch <- alarm:
		atomic.AddInt32(inflight, 1)
	default:
	}
}

func FlushAlarm() {
	flushAlarm(&BusinessAlarmLogWrite.inflight, BusinessAlarmLogWrite.empty)
	if jmtpWrite != nil {
		flushAlarm(&jmtpWrite.inflight, jmtpWrite.empty)
	}
}

func flushAlarm(inflight *int32, empty chan struct{}) {
	if atomic.LoadInt32(inflight) <= 0 {
		return
	}

	for {
		select {
		case <-empty:
			if atomic.LoadInt32(inflight) <= 0 {
				return
			}
		}
	}
}

func GetUmpCollectMethod() CollectMethod {
	return umpCollectMethod
}

// Delay jmtp client initialization to the first set of umpCollectMethod.
// Should call SetUmpJmtpAddr() before this function.
func SetUmpCollectMethod(method CollectMethod) {
	jmtpWriteMutex.Lock()
	defer jmtpWriteMutex.Unlock()
	if !method.Valid() {
		method = defaultCollectMethod
	}
	if method == CollectMethodJMTP && jmtpWrite == nil {
		if jmtp, err := NewJmtpWrite(); err == nil {
			jmtpWrite = jmtp
		}
	}
	umpCollectMethod = method
}

func SetUmpJmtpAddr(jmtpAddress string) {
	jmtpWriteMutex.Lock()
	defer jmtpWriteMutex.Unlock()
	if jmtpAddress == "" || jmtpAddress == umpJmtpAddress {
		return
	}
	umpJmtpAddress = jmtpAddress
	if jmtpWrite != nil {
		jmtpWrite.stop()
		if jmtp, err := NewJmtpWrite(); err == nil {
			jmtpWrite = jmtp
		}
	}
}

func SetUmpJmtpBatch(batch uint) {
	if batch > 0 && batch <= maxJmtpBatch {
		umpJmtpBatch = batch
	}
}
