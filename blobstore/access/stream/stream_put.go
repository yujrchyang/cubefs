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

package stream

import (
	"bytes"
	"context"
	"fmt"
	"hash/crc32"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/afex/hystrix-go/hystrix"

	"github.com/cubefs/cubefs/blobstore/api/access"
	"github.com/cubefs/cubefs/blobstore/api/blobnode"
	"github.com/cubefs/cubefs/blobstore/common/ec"
	errcode "github.com/cubefs/cubefs/blobstore/common/errors"
	"github.com/cubefs/cubefs/blobstore/common/proto"
	"github.com/cubefs/cubefs/blobstore/common/rpc"
	"github.com/cubefs/cubefs/blobstore/common/trace"
	"github.com/cubefs/cubefs/blobstore/util/errors"
	"github.com/cubefs/cubefs/blobstore/util/retry"
)

// TODO: To Be Continue
//  put empty shard to blobnode if file has been aligned.

// Put put one object
//
//	required: size, file size
//	optional: hasher map to calculate hash.Hash
func (h *Handler) Put(ctx context.Context,
	rc io.Reader, size int64, hasherMap access.HasherMap,
) (*proto.Location, error) {
	span := trace.SpanFromContextSafe(ctx)
	span.Debugf("put request size:%d hashes:b(%b)", size, hasherMap.ToHashAlgorithm())

	// 检查 size 大小
	if size <= 0 {
		return nil, errcode.ErrIllegalArguments
	}
	if size > h.maxObjectSize {
		span.Info("exceed max object size", h.maxObjectSize)
		return nil, errcode.ErrAccessExceedSize
	}

	// 1.make hasher
	if len(hasherMap) > 0 {
		rc = io.TeeReader(rc, hasherMap.ToWriter())
	}

	// 2.choose cluster and alloc volume from allocator
	// 根据策略选择 EC 模式
	selectedCodeMode := h.allCodeModes.SelectCodeMode(size)
	span.Debugf("select codemode %d", selectedCodeMode)

	blobSize := atomic.LoadUint32(&h.MaxBlobSize)
	// 分配 blob
	clusterID, blobs, err := h.allocFromAllocatorWithHystrix(ctx, selectedCodeMode, uint64(size), blobSize, 0)
	if err != nil {
		span.Error("alloc failed", errors.Detail(err))
		return nil, err
	}
	span.Debugf("allocated from %d %+v", clusterID, blobs)

	// 3.read body and split, alloc from mem pool;ec encode and put into data node
	limitReader := io.LimitReader(rc, int64(size))
	// 一个对象会被切分为多个 blob
	location := &proto.Location{
		ClusterID: clusterID,
		CodeMode:  selectedCodeMode,
		Size_:     uint64(size),
		SliceSize: blobSize,
		Slices:    blobs,
	}

	uploadSucc := false
	defer func() {
		if !uploadSucc {
			span.Infof("put failed clean location %+v", location)
			_, newCtx := trace.StartSpanFromContextWithTraceID(context.Background(), "", span.TraceID())
			if err := h.clearGarbage(newCtx, location); err != nil {
				span.Warn(errors.Detail(err))
			}
		}
	}()

	var buffer *ec.Buffer
	putTime := new(timeReadWrite)
	defer func() {
		// release ec buffer which have not takeover
		buffer.Release()
		span.AppendRPCTrackLog([]string{putTime.String()})
		putTime.Report(clusterID.ToString(), h.IDC, true)
	}()

	// concurrent buffer in per request
	const concurrence = 4
	ready := make(chan struct{}, concurrence)
	for range [concurrence]struct{}{} {
		ready <- struct{}{}
	}

	encoder := h.encoder[selectedCodeMode]
	tactic := selectedCodeMode.Tactic()
	// 这里遍历的单位还是 blob
	for _, blob := range location.Spread() {
		vid, bid, bsize := blob.Vid, blob.Bid, int(blob.Size)

		// new an empty ec buffer for per blob
		var err error
		st := time.Now()
		buffer, err = ec.NewBuffer(bsize, tactic, h.memPool)
		putTime.IncA(time.Since(st))
		if err != nil {
			return nil, err
		}

		readBuff := buffer.DataBuf[:bsize]
		// 将 blob 切分为 shard
		shards, err := encoder.Split(buffer.ECDataBuf)
		if err != nil {
			return nil, err
		}

		startRead := time.Now()
		n, err := io.ReadFull(limitReader, readBuff)
		putTime.IncR(time.Since(startRead))
		if err != nil && err != io.EOF {
			span.Infof("read blob data failed want:%d read:%d %s", bsize, n, err.Error())
			return nil, errcode.ErrAccessReadRequestBody
		}
		if n != bsize {
			span.Infof("read blob less data want:%d but:%d", bsize, n)
			return nil, errcode.ErrAccessReadRequestBody
		}

		// ec encode
		// ec 编码
		if err = encoder.Encode(shards); err != nil {
			return nil, err
		}

		blobident := blobIdent{clusterID, vid, bid}
		span.Debug("to write", blobident)

		// takeover the buffer, release to pool in function writeToBlobnodes
		takeoverBuffer := buffer
		buffer = nil
		<-ready
		startWrite := time.Now()
		// 写数据
		err = h.writeToBlobnodesWithHystrix(ctx, blobident, shards, func() {
			takeoverBuffer.Release()
			ready <- struct{}{}
		})
		putTime.IncW(time.Since(startWrite))
		if err != nil {
			return nil, errors.Info(err, "write to blobnode failed")
		}
	}

	uploadSucc = true
	return location, nil
}

func (h *Handler) writeToBlobnodesWithHystrix(ctx context.Context,
	blob blobIdent, shards [][]byte, callback func(),
) error {
	safe := make(chan struct{}, 1)
	err := hystrix.Do(rwCommand, func() error {
		safe <- struct{}{}
		return h.writeToBlobnodes(ctx, blob, shards, callback)
	}, nil)

	select {
	case <-safe:
	default:
		callback() // callback if fused by hystrix
	}
	return err
}

type shardPutStatus struct {
	index  int
	status bool
}

// writeToBlobnodes write shards to blobnodes.
// takeover ec buffer release by callback.
// return if had quorum successful shards, then wait all shards in background.
func (h *Handler) writeToBlobnodes(ctx context.Context,
	blob blobIdent, shards [][]byte, callback func(),
) (err error) {
	span := trace.SpanFromContextSafe(ctx)
	clusterID, vid, bid := blob.cid, blob.vid, blob.bid

	// 等待所有 shard 完成
	wg := &sync.WaitGroup{}
	defer func() {
		// waiting all shards done in background
		go func() {
			wg.Wait()
			callback()
		}()
	}()

	// 获取 volume
	volume, err := h.getVolume(ctx, clusterID, vid, true)
	if err != nil {
		return
	}
	// 获取服务控制器
	serviceController, err := h.clusterController.GetServiceController(clusterID)
	if err != nil {
		return
	}

	// 通过通道处理 put 返回的状态
	statusCh := make(chan shardPutStatus, len(volume.Units))
	tactic := volume.CodeMode.Tactic()
	putQuorum := uint32(tactic.PutQuorum)
	if num, ok := h.CodeModesPutQuorums[volume.CodeMode]; ok && num <= tactic.N+tactic.M {
		putQuorum = uint32(num)
	}

	// 计时
	writeStart := time.Now()
	writeTime := int32(0)

	// writtenNum ONLY apply on data and partiy shards
	// TODO: count N and M in each AZ,
	//    decision ec data is recoverable or not.
	// 计算分片数，用于后续的判断
	maxWrittenIndex := tactic.N + tactic.M
	writtenNum := uint32(0)

	// 并行处理
	wg.Add(len(volume.Units))
	for i, unitI := range volume.Units {
		index, unit := i, unitI

		go func() {
			// 声明本通道的 shard 状态
			status := shardPutStatus{index: index}
			// 结束时发送状态
			defer func() {
				statusCh <- status
				wg.Done()
			}()

			// 获取本 shard 对应的磁盘 ID
			diskID := unit.DiskID
			// 初始化给 blobnode io 的参数
			args := &blobnode.PutShardArgs{
				DiskID: diskID,
				Vuid:   unit.Vuid,
				Bid:    bid,
				Size:   int64(len(shards[index])),
				Type:   blobnode.NormalIO,
			}

			crcDisable := h.ShardCrcWriteDisable
			var crcOrigin uint32
			if !crcDisable {
				crcOrigin = crc32.ChecksumIEEE(shards[index])
			}

			// new child span to write to blobnode, we should finish it here.
			spanChild, ctxChild := trace.StartSpanFromContextWithTraceID(
				context.Background(), "WriteToBlobnode", span.TraceID())
			defer spanChild.Finish()

		RETRY:
			// 获取磁盘所在节点
			hostInfo, err := serviceController.GetDiskHost(ctxChild, diskID)
			if err != nil {
				span.Error("get disk host failed", errors.Detail(err))
				return
			}
			// punished disk, ignore and return
			// 如果节点异常并且被隔离，直接返回
			if hostInfo.Punished {
				span.Warnf("ignore punished disk(%d %s) uvid(%d) ecidx(%02d) in idc(%s)",
					diskID, hostInfo.Host, unit.Vuid, index, hostInfo.IDC)
				return
			}
			host := hostInfo.Host

			var (
				writeErr  error
				needRetry bool
				crc       uint32
			)
			// 带重试的写
			writeErr = retry.ExponentialBackoff(3, 200).RuptOn(func() (bool, error) {
				// 读数据到 body
				args.Body = bytes.NewReader(shards[index])
				// 写数据到 blobnode
				crc, err = h.blobnodeClient.PutShard(ctxChild, host, args)
				// 成功则返回
				if err == nil {
					if !crcDisable && crc != crcOrigin {
						return false, fmt.Errorf("crc mismatch 0x%x != 0x%x", crc, crcOrigin)
					}

					// slow disk if speed lower and time greater than most shards, also retry.
					if mostTime := atomic.LoadInt32(&writeTime); mostTime > 0 {
						shardTime := time.Since(writeStart)
						shardSpeed := float32(args.Size) / (float32(shardTime) / 1e9) / (1 << 10)
						if int(shardTime/1e6) > h.LogSlowBaseTimeMS &&
							shardSpeed < float32(h.LogSlowBaseSpeedKB) &&
							float32(shardTime) > h.LogSlowTimeFator*float32(mostTime) {
							span.Warnf("slow disk(host:%s diskid:%d) time(most:%dms shard:%dms) speed:%.2fKB/s",
								host, diskID, mostTime/1e6, shardTime/1e6, shardSpeed)
						}
					}

					needRetry = false
					return true, nil
				}

				// 异常处理
				code := rpc.DetectStatusCode(err)
				switch code {
				case errcode.CodeDiskBroken, errcode.CodeDiskNotFound,
					errcode.CodeChunkNoSpace, errcode.CodeVUIDReadonly:
					h.discardVidChan <- discardVid{
						cid:      clusterID,
						codeMode: volume.CodeMode,
						vid:      vid,
					}
				default:
				}

				switch code {
				// EIO and Readonly error, then we need to punish disk in local and no necessary to retry
				case errcode.CodeVUIDReadonly:
					h.punishVolume(ctx, clusterID, vid, host, "BrokenOrRO")
					h.punishDisk(ctx, clusterID, diskID, host, "BrokenOrRO")
					span.Warnf("punish disk:%d volume:%d cos:blobnode/%d", diskID, vid, code)
					return true, err

				// chunk no space, we should punish this volume
				case errcode.CodeChunkNoSpace:
					h.punishVolume(ctx, clusterID, vid, host, "NoSpace")
					span.Warnf("punish volume:%d cos:blobnode/%d", vid, code)
					return true, err

				// disk broken may be some chunks have repaired
				// vuid not found means the reflection between vuid and diskID has change, we should refresh the volume
				// disk not found means disk has been repaired or offline
				case errcode.CodeDiskBroken, errcode.CodeDiskNotFound, errcode.CodeVuidNotFound:
					latestVolume, e := h.getVolume(ctx, clusterID, vid, false)
					if e != nil {
						return true, errors.Base(err, "get volume with no cache failed").Detail(e)
					}

					newUnit := latestVolume.Units[index]
					if diskID != newUnit.DiskID {
						diskID = newUnit.DiskID
						unit = newUnit
						args.DiskID = newUnit.DiskID
						args.Vuid = newUnit.Vuid

						needRetry = true
						return true, err
					}

					reason := "NotFound"
					if code == errcode.CodeDiskBroken {
						reason = "Broken"
					}
					h.punishVolume(ctx, clusterID, vid, host, reason)
					h.punishDisk(ctx, clusterID, diskID, host, reason)
					span.Warnf("punish disk:%d volume:%d cos:blobnode/%d", diskID, vid, code)
					return true, err
				default:
				}

				// in timeout case and writtenNum is not satisfied with putQuorum, then should retry
				if errorTimeout(err) && atomic.LoadUint32(&writtenNum) < putQuorum {
					h.updateVolume(ctx, clusterID, vid)
					h.punishDiskWith(ctx, clusterID, diskID, host, "Timeout")
					span.Warn("timeout need to punish threshold disk", diskID, host)
					return false, err
				}

				// others, do not retry this round
				return true, err
			})

			if needRetry {
				goto RETRY
			}
			if writeErr != nil {
				span.Warnf("write %s on blobnode(vuid:%d disk:%d host:%s) ecidx(%02d): %s",
					blob.String(), args.Vuid, args.DiskID, hostInfo.Host, index, errors.Detail(writeErr))
				return
			}

			// 计数
			if index < maxWrittenIndex {
				atomic.AddUint32(&writtenNum, 1)
			}
			// 设置状态
			status.status = true
		}()
	}

	// 等待写完成
	received := make(map[int]shardPutStatus, len(volume.Units))
	for len(received) < len(volume.Units) && atomic.LoadUint32(&writtenNum) < putQuorum {
		st := <-statusCh
		received[st.index] = st

		// trace slow disk after written 3/4 shards
		if atomic.LoadInt32(&writeTime) == 0 && len(received) > len(volume.Units)*3/4 {
			atomic.StoreInt32(&writeTime, int32(time.Since(writeStart)))
		}
	}

	// 后台等待并分析结果
	// 如果总体成功则将失败的 shard 发送到修复队列
	writeDone := make(chan struct{}, 1)
	// write unaccomplished shard to repair queue
	go func(writeDone <-chan struct{}) {
		for len(received) < len(volume.Units) {
			st := <-statusCh
			received[st.index] = st
		}

		if _, ok := <-writeDone; !ok {
			return
		}

		badIdxes := make([]uint8, 0)
		for idx := range volume.Units {
			if st, ok := received[idx]; ok && st.status {
				continue
			}
			badIdxes = append(badIdxes, uint8(idx))
		}
		if len(badIdxes) > 0 {
			h.sendRepairMsgBg(ctx, blob, badIdxes)
		}
	}(writeDone)

	// return if had quorum successful shards
	if atomic.LoadUint32(&writtenNum) >= putQuorum {
		writeDone <- struct{}{}
		return
	}

	// It tolerate one az was down when we have 3 or more azs.
	// But MUST make sure others azs data is all completed,
	// And all data in the down az are failed.
	if tactic.AZCount >= 3 {
		allFine := 0
		allDown := 0

		for _, azIndexes := range tactic.GetECLayoutByAZ() {
			azFine := true
			azDown := true
			for _, idx := range azIndexes {
				if st, ok := received[idx]; !ok || !st.status {
					azFine = false
				} else {
					azDown = false
				}
			}
			if azFine {
				allFine++
			}
			if azDown {
				allDown++
			}
		}

		span.Debugf("tolerate-multi-az-write (az-fine:%d az-down:%d az-all:%d)", allFine, allDown, tactic.AZCount)
		if allFine == tactic.AZCount-1 && allDown == 1 {
			span.Warnf("tolerate-multi-az-write (az-fine:%d az-down:%d az-all:%d) of %s",
				allFine, allDown, tactic.AZCount, blob.String())
			writeDone <- struct{}{}
			return
		}
	}

	// 返回失败
	close(writeDone)
	err = fmt.Errorf("quorum write failed (%d < %d) of %s", writtenNum, putQuorum, blob.String())
	return
}
