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

package qos

import (
	"context"
	"io"
	"time"

	"golang.org/x/time/rate"

	"github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/trace"
)

const _limited = "limited"

type qosLimiter interface {
	ReserveN(t time.Time, n int) *rate.Reservation
}

// 所有的读写操作被包装到 rateLimiter 中，每次读写前获取令牌
type rateLimiter struct {
	readerAt io.ReaderAt
	reader   io.Reader
	writer   io.Writer
	writerAt io.WriterAt
	ctx      context.Context

	ctrl qosLimiter // 实际指向 queueQos.limitBps
}

func (l *rateLimiter) Read(p []byte) (n int, err error) {
	err = l.doWithLimit(len(p))
	if err != nil {
		return
	}

	return l.reader.Read(p)
}

func (l *rateLimiter) readAt(p []byte, off int64) (n int, err error) {
	err = l.doWithLimit(len(p))
	if err != nil {
		return
	}

	return l.readerAt.ReadAt(p, off)
}

func (l *rateLimiter) ReadAt(p []byte, off int64) (readn int, err error) {
	select {
	case <-l.ctx.Done():
		return 0, l.ctx.Err()
	default:
	}

	var nn int
	for readn < len(p) && err == nil {
		nn, err = l.readAt(p[readn:], off)
		off += int64(nn)
		readn += nn
	}
	return
}

func (l *rateLimiter) Write(p []byte) (n int, err error) {
	err = l.doWithLimit(len(p))
	if err != nil {
		return
	}

	return l.writer.Write(p)
}

func (l *rateLimiter) WriteAt(p []byte, off int64) (n int, err error) {
	err = l.doWithLimit(len(p))
	if err != nil {
		return
	}

	return l.writerAt.WriteAt(p, off)
}

func (l *rateLimiter) doWithLimit(n int) (err error) {
	return l.doWithSingleLimit(n)
}

func (l *rateLimiter) doWithSingleLimit(n int) (err error) {
	now := time.Now()
	// 尝试为接下来的操作预留 n 个令牌，该接口不会阻塞，而是立即返回一个 Reservation 对象，
	// 表明是否可以立即执行操作，或需要等待多久
	reserve := l.ctrl.ReserveN(now, n)
	// 若请求大小超过 burst 容量（即令牌桶最大容量），直接拒绝
	if !reserve.OK() {
		return errors.ErrSizeOverBurst
	}

	// 返回需要等待的时间（如果令牌不足）
	// delay == 0 说明令牌充足，立即执行 IO
	delay := reserve.DelayFrom(now)
	if delay == 0 {
		return
	}

	// delay 大于 0，等待这么长的时间，ReserveN 提前预定了令牌，所以到时间后可以立即执行
	// 如果用户通过 ctx 取消，则立即返回，取消预定并返回错误
	t := time.NewTimer(delay)
	defer t.Stop()
	span := trace.SpanFromContextSafe(l.ctx)
	span.SetTag(_limited, delay.Milliseconds())
	select {
	case <-t.C:
		return
	case <-l.ctx.Done():
		reserve.Cancel()
		err = l.ctx.Err()
		return
	}
}
