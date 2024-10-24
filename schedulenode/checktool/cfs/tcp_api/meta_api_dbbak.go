package tcp_api

import (
	"encoding/json"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/schedulenode/checktool/cfs/dbbak_tcp"
)

type LoadMetaPartitionMetricResponse struct {
	Start       uint64
	End         uint64
	Status      uint8
	Result      string
	PartitionID uint64
	DoCompare   bool
	ApplyID     uint64
	MaxInode    uint64
	DentryCount uint64
	Addr        string
	InodeCount  uint64
}

func LoadMetaPartitionDbbak(partition uint64, addr string) (mpr *MetaPartitionLoadResponse, err error) {
	var req interface{}
	req = &LoadMetaPartitionMetricResponse{PartitionID: partition}
	t := proto.NewAdminTask(dbbak_tcp.OpLoadMetaPartition, addr, req)
	packet := dbbak_tcp.NewDbbakPacket()
	packet.Opcode = t.OpCode
	packet.ReqID = proto.GenerateRequestID()
	packet.PartitionID = uint32(t.PartitionID)
	body, err := json.Marshal(t)
	if err != nil {
		return nil, err
	}
	packet.Size = uint32(len(body))
	packet.Data = body

	var reply *dbbak_tcp.DbbakPacket
	reply, err = dbbak_tcp.DbBackSendTcpPacket(addr, packet)
	if err != nil {
		return
	}
	mpr = new(MetaPartitionLoadResponse)
	mpr.Addr = addr
	mprDbbak := new(LoadMetaPartitionMetricResponse)
	if err = json.Unmarshal(reply.Data, mprDbbak); err != nil {
		return
	}
	mpr.PartitionID = mprDbbak.PartitionID
	mpr.ApplyID = mprDbbak.ApplyID
	mpr.DentryCount = mprDbbak.DentryCount
	mpr.InodeCount = mprDbbak.InodeCount
	mpr.MaxInode = mprDbbak.MaxInode
	return
}
