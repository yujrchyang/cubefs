package dbbak_tcp

// Copyright 2018 The CFS Authors.
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

import (
	"encoding/binary"
	"github.com/cubefs/cubefs/util/buf"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/unit"
	"io"
	"net"
	"strconv"
	"sync/atomic"
	"time"
)

var (
	ReqIDGlobal = int64(1)
	Buffers     = buf.NewBufferPool()
)

func GetReqID() int64 {
	return atomic.AddInt64(&ReqIDGlobal, 1)
}

// operations
const (
	ProtoMagic               uint8 = 0xFF
	OpInitResultCode         uint8 = 0x00
	OpRead                   uint8 = 0x04
	OpStreamRead             uint8 = 0x05
	OpNormalExtentRepairRead uint8 = 0x10
	OpTinyExtentRepairRead   uint8 = 0x11
	OpLoadMetaPartition      uint8 = 0x44
)

const (
	WriteDeadlineTime  = 5
	NoReadDeadlineTime = -1
)

type DbbakPacket struct {
	Magic       uint8
	StoreMode   uint8
	Opcode      uint8
	ResultCode  uint8
	Nodes       uint8
	Crc         uint32
	Size        uint32
	Arglen      uint32
	PartitionID uint32
	FileID      uint64
	Offset      int64
	ReqID       int64
	Arg         []byte //if create or append ops, data contains addrs
	Data        []byte
	StartT      int64
}

func NewDbbakPacket() *DbbakPacket {
	p := new(DbbakPacket)
	p.ReqID = GetReqID()
	p.Magic = ProtoMagic
	p.StartT = time.Now().UnixNano()

	return p
}

func (p *DbbakPacket) MarshalHeader(out []byte) {
	out[0] = p.Magic
	out[1] = p.StoreMode
	out[2] = p.Opcode
	out[3] = p.ResultCode
	out[4] = p.Nodes
	binary.BigEndian.PutUint32(out[5:9], p.Crc)
	binary.BigEndian.PutUint32(out[9:13], p.Size)
	binary.BigEndian.PutUint32(out[13:17], p.Arglen)
	binary.BigEndian.PutUint32(out[17:21], p.PartitionID)
	binary.BigEndian.PutUint64(out[21:29], p.FileID)
	binary.BigEndian.PutUint64(out[29:37], uint64(p.Offset))
	binary.BigEndian.PutUint64(out[37:unit.PacketHeaderSizeForDbbak], uint64(p.ReqID))
	return
}

func (p *DbbakPacket) UnmarshalHeader(in []byte) error {
	p.Magic = in[0]
	if p.Magic != ProtoMagic {
		return errors.New("Bad Magic " + strconv.Itoa(int(p.Magic)))
	}

	p.StoreMode = in[1]
	p.Opcode = in[2]
	p.ResultCode = in[3]
	p.Nodes = in[4]
	p.Crc = binary.BigEndian.Uint32(in[5:9])
	p.Size = binary.BigEndian.Uint32(in[9:13])
	p.Arglen = binary.BigEndian.Uint32(in[13:17])
	p.PartitionID = binary.BigEndian.Uint32(in[17:21])
	p.FileID = binary.BigEndian.Uint64(in[21:29])
	p.Offset = int64(binary.BigEndian.Uint64(in[29:37]))
	p.ReqID = int64(binary.BigEndian.Uint64(in[37:unit.PacketHeaderSizeForDbbak]))

	return nil
}

func (p *DbbakPacket) WriteToConn(c net.Conn) (err error) {
	c.SetWriteDeadline(time.Now().Add(WriteDeadlineTime * time.Second))
	header, err := Buffers.Get(unit.PacketHeaderSizeForDbbak)
	if err != nil {
		header = make([]byte, unit.PacketHeaderSizeForDbbak)
	}
	defer Buffers.Put(header)

	p.MarshalHeader(header)
	if _, err = c.Write(header); err == nil {
		if _, err = c.Write(p.Arg[:int(p.Arglen)]); err == nil {
			if p.Data != nil && p.Size != 0 {
				_, err = c.Write(p.Data[:p.Size])
			}
		}
	}

	return
}

func ReadFull(c net.Conn, buf *[]byte, readSize int) (err error) {
	*buf = make([]byte, readSize)
	_, err = io.ReadFull(c, (*buf)[:readSize])
	return
}

func (p *DbbakPacket) ReadFromConn(c net.Conn, timeoutSec int) (err error) {
	if timeoutSec != NoReadDeadlineTime {
		c.SetReadDeadline(time.Now().Add(time.Second * time.Duration(timeoutSec)))
	} else {
		c.SetReadDeadline(time.Time{})
	}
	header, err := Buffers.Get(unit.PacketHeaderSizeForDbbak)
	if err != nil {
		header = make([]byte, unit.PacketHeaderSizeForDbbak)
	}
	defer Buffers.Put(header)
	if _, err = io.ReadFull(c, header); err != nil {
		return
	}
	if err = p.UnmarshalHeader(header); err != nil {
		return
	}

	if p.Arglen > 0 {
		if err = ReadFull(c, &p.Arg, int(p.Arglen)); err != nil {
			return
		}
	}

	if p.Size < 0 {
		return
	}
	size := p.Size
	if (p.Opcode == OpRead || p.Opcode == OpStreamRead || p.Opcode == OpNormalExtentRepairRead || p.Opcode == OpTinyExtentRepairRead) && p.ResultCode == OpInitResultCode {
		size = 0
	}
	return ReadFull(c, &p.Data, int(size))
}
