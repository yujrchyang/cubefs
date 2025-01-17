package migration

import (
	"encoding/json"
	"github.com/cubefs/cubefs/proto"
	"golang.org/x/net/context"
	"strings"
)

type DataPartition struct {
	PartitionID uint64
	Hosts       []string
}

func (dp *DataPartition) GetAllAddrs() (m string) {
	return strings.Join(dp.Hosts[1:], proto.AddrSplit) + proto.AddrSplit
}

func NewPacketToBatchDeleteExtent(ctx context.Context, dp *DataPartition, exts []proto.InodeExtentKey) *proto.Packet {
	p := new(proto.Packet)
	p.Magic = proto.ProtoMagic
	p.Opcode = proto.OpBatchDeleteExtent
	p.ExtentType = proto.NormalExtentType
	p.PartitionID = dp.PartitionID
	p.Data, _ = json.Marshal(exts)
	p.Size = uint32(len(p.Data))
	p.ReqID = proto.GenerateRequestID()
	p.RemainingFollowers = uint8(len(dp.Hosts) - 1)
	p.Arg = ([]byte)(dp.GetAllAddrs())
	p.ArgLen = uint32(len(p.Arg))
	p.SetCtx(ctx)
	return p
}
