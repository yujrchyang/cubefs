package tcp_api

import (
	"context"
	"encoding/json"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/schedulenode/checktool/cfs/tcp"
)

type MetaPartitionLoadResponse struct {
	PartitionID uint64
	DoCompare   bool
	Addr        string
	MaxInode    uint64
	ApplyID     uint64
	DentryCount uint64
	InodeCount  uint64
}

func LoadMetaPartition(dbbak bool, partition uint64, addr string) (mpr *MetaPartitionLoadResponse, err error) {
	if dbbak {
		return LoadMetaPartitionDbbak(partition, addr)
	}
	var req interface{}
	req = &proto.MetaPartitionLoadRequest{PartitionID: partition}
	t := proto.NewAdminTask(proto.OpLoadMetaPartition, addr, req)
	packet := proto.NewPacket(context.Background())
	packet.Opcode = t.OpCode
	packet.ReqID = proto.GenerateRequestID()
	packet.PartitionID = t.PartitionID
	body, err := json.Marshal(t)
	if err != nil {
		return nil, err
	}
	packet.Size = uint32(len(body))
	packet.Data = body

	var reply *proto.Packet
	reply, err = tcp.SendTcpPacket(context.Background(), addr, packet)
	if err != nil {
		return
	}
	mpr = new(MetaPartitionLoadResponse)
	err = json.Unmarshal(reply.Data, mpr)
	mpr.Addr = addr
	return
}
