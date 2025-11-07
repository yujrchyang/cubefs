// Copyright 2022 The CubeFS Authors.
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

package base

import (
	"io"
	"time"
)

type Writer struct {
	io.WriterAt       // 支持从指定位置写入
	Offset      int64 // 维护当前写入位置
}

type Reader struct {
	io.ReaderAt
	Offset int64
}

// 带耗时统计的 io.Reader 包装器
type TimeReader struct {
	r io.Reader //
	t int64     // 累计纳秒耗时
}

// 带耗时的 io.Writer 包装器
type TimeWriter struct {
	w io.Writer //
	t int64     // 累计纳秒耗时
}

func (p *Writer) Write(val []byte) (n int, err error) {
	n, err = p.WriteAt(val, p.Offset) // 调用底层 WriteAt 从当前 Offset 写入
	p.Offset += int64(n)              // Offset 递进
	return
}

func (p *Reader) Read(val []byte) (n int, err error) {
	n, err = p.ReadAt(val, p.Offset) // 调用底层 ReadAt 从当前 Offset 读取
	p.Offset += int64(n)             // Offset 递进
	return
}

func accumulateLatency(total *int64, begin int64) {
	// 计算从 begin 开始的纳秒时间差并累计到 total 中
	t := time.Now().UnixNano() - begin
	*total = *total + t
}

func (r *TimeReader) Read(p []byte) (n int, err error) {
	defer accumulateLatency(&r.t, time.Now().UnixNano()) // 计算读耗时
	n, err = r.r.Read(p)
	return
}

func (r *TimeReader) Duration() time.Duration {
	return time.Duration(r.t)
}

func (r *TimeWriter) Write(p []byte) (n int, err error) {
	defer accumulateLatency(&r.t, time.Now().UnixNano()) // 计算写耗时
	n, err = r.w.Write(p)
	return
}

func (r *TimeWriter) Duration() time.Duration {
	return time.Duration(r.t)
}

func NewTimeReader(reader io.Reader) *TimeReader {
	return &TimeReader{r: reader}
}

func NewTimeWriter(writer io.Writer) *TimeWriter {
	return &TimeWriter{w: writer}
}
