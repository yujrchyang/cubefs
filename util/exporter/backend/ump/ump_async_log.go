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
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sync"
	"time"
)

const (
	FunctionTpSufixx        = "tp.log"
	FunctionTpGroupBySufixx = "groupby_tp.log"
	SystemAliveSufixx       = "alive.log"
	BusinessAlarmSufixx     = "business.log"
	LogFileOpt              = os.O_RDWR | os.O_CREATE | os.O_APPEND
	ChSize                  = 102400
	BusinessAlarmType       = "BusinessAlarm"
	SystemAliveType         = "SystemAlive"
	FunctionTpType          = "FunctionTp"
	HostNameFile            = "/proc/sys/kernel/hostname"
	MaxLogSize              = 1024 * 1024 * 10
	checkFileExistsTicket   = 1 * time.Minute
)

var (
	FunctionTpLogWrite = &LogWrite{logCh: make(chan interface{}, ChSize)}
	// FunctionTpGroupByLogWrite = &LogWrite{logCh: make(chan interface{}, ChSize)}
	SystemAliveLogWrite   = &LogWrite{logCh: make(chan interface{}, ChSize)}
	BusinessAlarmLogWrite = &LogWrite{logCh: make(chan interface{}, ChSize), empty: make(chan struct{}, 1)}
	UmpDataDir            = "/export/home/tomcat/UMP-Monitor/logs/"
	wg                    sync.WaitGroup
)

type LogWrite struct {
	logCh       chan interface{}
	logName     string
	logSize     int64
	seq         int
	logSufixx   string
	logFp       *os.File
	sigCh       chan bool
	bf          *bytes.Buffer
	jsonEncoder *json.Encoder
	// pending log
	inflight int32
	// Issue a signal to this channel when inflight hits zero.
	empty chan struct{}
	stopC chan struct{}
}

func (lw *LogWrite) initLogFp(sufixx string) (err error) {
	var fi os.FileInfo
	lw.seq = 0
	lw.sigCh = make(chan bool, 1)
	lw.stopC = make(chan struct{})
	lw.logSufixx = sufixx
	lw.logName = fmt.Sprintf("%s%s%s", UmpDataDir, "ump_", lw.logSufixx)
	lw.bf = bytes.NewBuffer([]byte{})
	lw.jsonEncoder = json.NewEncoder(lw.bf)
	lw.jsonEncoder.SetEscapeHTML(false)
	if lw.logFp, err = os.OpenFile(lw.logName, LogFileOpt, 0777); err != nil {
		return
	}
	os.Chmod(lw.logName, 0777)

	if fi, err = lw.logFp.Stat(); err != nil {
		return
	}
	lw.logSize = fi.Size()

	return
}

func (lw *LogWrite) backGroundCheckFile() (err error) {
	if lw.logSize <= MaxLogSize {
		return
	}
	lw.logFp.Close()
	lw.seq++
	if lw.seq > 3 {
		lw.seq = 1
	}

	name := fmt.Sprintf("%s%s%s.%d", UmpDataDir, "ump_", lw.logSufixx, lw.seq)
	if _, err = os.Stat(name); err == nil {
		os.Remove(name)
	}
	os.Rename(lw.logName, name)

	if lw.logFp, err = os.OpenFile(lw.logName, LogFileOpt, 0777); err != nil {
		lw.seq--
		return
	}
	os.Chmod(lw.logName, 0777)

	if err = os.Truncate(lw.logName, 0); err != nil {
		lw.seq--
		return
	}
	lw.logSize = 0

	return
}

func (lw *LogWrite) backGroundCheckFileExist() {
	defer wg.Done()
	var (
		err   error
		logFp *os.File
	)
	ticker := time.NewTicker(checkFileExistsTicket)
	defer ticker.Stop()
	for {
		select {
		case <-lw.stopC:
			return
		case <-ticker.C:
			if _, err = os.Stat(lw.logName); err == nil || !os.IsNotExist(err) {
				continue
			}
			if logFp, err = os.OpenFile(lw.logName, LogFileOpt, 0777); err != nil {
				continue
			}
			os.Chmod(lw.logName, 0777)
			lw.logFp.Close()
			lw.logFp = logFp
		}
	}
}

func initLogName(module string) (err error) {
	if err = os.MkdirAll(UmpDataDir, 0777); err != nil {
		return
	}
	os.Chmod(UmpDataDir, 0777)

	if HostName, err = GetLocalIpAddr(); err != nil {
		return
	}

	if err = FunctionTpLogWrite.initLogFp(module + "_" + FunctionTpSufixx); err != nil {
		return
	}
	// if err = FunctionTpGroupByLogWrite.initLogFp(module + "_" + FunctionTpGroupBySufixx); err != nil {
	// 	return
	// }
	if err = SystemAliveLogWrite.initLogFp(module + "_" + SystemAliveSufixx); err != nil {
		return
	}

	if err = BusinessAlarmLogWrite.initLogFp(module + "_" + BusinessAlarmSufixx); err != nil {
		return
	}

	return
}

func GetLocalIpAddr() (localAddr string, err error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return
	}
	for _, addr := range addrs {
		if ipNet, isIpNet := addr.(*net.IPNet); isIpNet && !ipNet.IP.IsLoopback() && !ipNet.IP.IsLinkLocalUnicast() {
			if ipv4 := ipNet.IP.To4(); ipv4 != nil {
				localAddr = ipv4.String()
				return
			}
		}
	}
	err = fmt.Errorf("cannot get local ip")
	return
}

func backGroudWriteLog() {
	wg.Add(6)
	go FunctionTpLogWrite.backGroupWriteForGroupByTPV629()
	go SystemAliveLogWrite.backGroupAliveWriteV629()
	go BusinessAlarmLogWrite.backGroupBusinessWriteV629()
	go FunctionTpLogWrite.backGroundCheckFileExist()
	go SystemAliveLogWrite.backGroundCheckFileExist()
	go BusinessAlarmLogWrite.backGroundCheckFileExist()
}

func stopLogWriter() {
	close(FunctionTpLogWrite.stopC)
	close(SystemAliveLogWrite.stopC)
	close(BusinessAlarmLogWrite.stopC)
	wg.Wait()
	FunctionTpLogWrite = nil
	SystemAliveLogWrite = nil
	BusinessAlarmLogWrite = nil
}
