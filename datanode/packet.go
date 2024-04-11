package datanode

import (
	"encoding/binary"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/repl"
	"golang.org/x/net/context"
)

// NewPacketToBroadcastMinAppliedID returns a new packet to broadcast the min applied ID.
func NewPacketToBroadcastMinAppliedID(ctx context.Context, partitionID uint64, minAppliedID uint64) (p *repl.Packet) {
	p = new(repl.Packet)
	p.Opcode = proto.OpBroadcastMinAppliedID
	p.PartitionID = partitionID
	p.Magic = proto.ProtoMagic
	p.ReqID = proto.GenerateRequestID()
	p.Data = make([]byte, 8)
	binary.BigEndian.PutUint64(p.Data[0:8], minAppliedID)
	p.Size = uint32(len(p.Data))
	p.SetCtx(ctx)
	return
}

// NewPacketToGetAppliedID returns a new packet to get the applied ID.
func NewPacketToGetAppliedID(ctx context.Context, partitionID uint64) (p *repl.Packet) {
	p = new(repl.Packet)
	p.Opcode = proto.OpGetAppliedId
	p.PartitionID = partitionID
	p.Magic = proto.ProtoMagic
	p.ReqID = proto.GenerateRequestID()
	p.SetCtx(ctx)
	return
}

// NewPacketToGetPersistedAppliedID returns a new packet to get the applied ID.
func NewPacketToGetPersistedAppliedID(ctx context.Context, partitionID uint64) (p *repl.Packet) {
	p = new(repl.Packet)
	p.Opcode = proto.OpGetPersistedAppliedId
	p.PartitionID = partitionID
	p.Magic = proto.ProtoMagic
	p.ReqID = proto.GenerateRequestID()
	p.SetCtx(ctx)
	return
}

// NewPacketToGetPartitionSize returns a new packet to get the partition size.
func NewPacketToGetPartitionSize(ctx context.Context, partitionID uint64) (p *repl.Packet) {
	p = new(repl.Packet)
	p.Opcode = proto.OpGetPartitionSize
	p.PartitionID = partitionID
	p.Magic = proto.ProtoMagic
	p.ReqID = proto.GenerateRequestID()
	p.SetCtx(ctx)
	return
}

// NewPacketToGetMaxExtentIDAndPartitionSIze returns a new packet to get the partition size.
func NewPacketToGetMaxExtentIDAndPartitionSIze(ctx context.Context, partitionID uint64) (p *repl.Packet) {
	p = new(repl.Packet)
	p.Opcode = proto.OpGetMaxExtentIDAndPartitionSize
	p.PartitionID = partitionID
	p.Magic = proto.ProtoMagic
	p.ReqID = proto.GenerateRequestID()
	p.SetCtx(ctx)
	return
}