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

package master

import (
	"time"

	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
)

// MetaRecorder represents the recorder replica of a data partition
type MetaRecorder struct {
	Addr              string
	nodeID            uint64
	ReportTime        int64
	Status            int8 // unavailable or readWrite
	IsRecover         bool
	ApplyId           uint64
	metaNode          *MetaNode
	createTime        int64
}

func newMetaRecorder(metaNode *MetaNode) (recorder *MetaRecorder) {
	recorder = &MetaRecorder{nodeID: metaNode.ID, Addr: metaNode.Addr}
	recorder.metaNode = metaNode
	recorder.ReportTime = time.Now().Unix()
	recorder.createTime = time.Now().Unix()
	return
}

func (mr *MetaRecorder) createTaskToDeleteRecorder(partitionID uint64) (t *proto.AdminTask) {
	req := &proto.DeleteMetaRecorderRequest{PartitionID: partitionID}
	t = proto.NewAdminTask(proto.OpDeleteMetaRecorder, mr.Addr, req)
	resetMetaPartitionTaskID(t, partitionID)
	return
}

func (mr *MetaRecorder) updateMetric(mgr *proto.MetaRecorderReport) {
	mr.Status = (int8)(mgr.Status)
	mr.IsRecover = mgr.IsRecover
	mr.ApplyId = mgr.ApplyId
	mr.ReportTime = time.Now().Unix()
}

func (mr *MetaRecorder) isActive() (active bool) {
	active = mr.metaNode.IsActive && time.Now().Unix()-mr.ReportTime < defaultMetaPartitionTimeOutSec
	if time.Now().Unix()-mr.createTime > defaultMetaReplicaCheckStatusSec {
		active = active && mr.Status != proto.Unavailable
	}

	if !active {
		log.LogInfof("metaNode.IsActive:%v mr.Status:%v diffReportTime:%v",
			mr.metaNode.IsActive, mr.Status, time.Now().Unix()-mr.ReportTime)
	}

	return
}