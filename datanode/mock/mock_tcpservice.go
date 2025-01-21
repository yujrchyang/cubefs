package mock

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/repl"
	"github.com/cubefs/cubefs/storage"
	"github.com/cubefs/cubefs/util/log"
	"github.com/cubefs/cubefs/util/unit"
	"hash/crc32"
	"io"
	"math/rand"
	"net"
)

const (
	RemoteNormalExtentCount        = 6
	LocalCreateExtentId     uint64 = 1000
)

var (
	SupportedGetAllWatermarksVersion            = 3
	ReplyGetRemoteExtentInfoForValidateCRCCount = 3
)
var letterRunes = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

const UpstreamReadSeed = 1993
const UpstreamReadSize = 10 * unit.MB

func RandTestData(size int, seed int64) (data []byte) {
	rand.Seed(seed)
	b := make([]byte, size)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return b
}

type MockTcp struct {
	ln     net.Listener
	listen int
	stopCh chan bool
}

func NewMockTcp(listen int) *MockTcp {
	return &MockTcp{
		stopCh: make(chan bool),
		listen: listen,
	}
}

func (m *MockTcp) Start() (err error) {
	if m.ln, err = net.Listen("tcp", fmt.Sprintf(":%d", m.listen)); err != nil {
		return
	}
	go m.serveAccept()
	return
}

func (m *MockTcp) Stop() {
	if m.ln != nil {
		_ = m.ln.Close()
	}
	close(m.stopCh)
}

func (m *MockTcp) serveAccept() {
	var conn net.Conn
	var err error
	for {
		if conn, err = m.ln.Accept(); err != nil {
			return
		}
		go func(c net.Conn) {
			for {
				select {
				case <-m.stopCh:
					return
				default:
					if err = m.serveConn(c); err != nil {
						_ = c.Close()
						return
					}
				}
			}
		}(conn)
	}
}

func (m *MockTcp) serveConn(conn net.Conn) (err error) {
	p := new(repl.Packet)
	_, err = p.ReadFromConnFromCli(conn, repl.ReplProtocalServerTimeOut)
	if err != nil {
		if err == io.EOF {
			return
		}
		log.LogErrorf("serveConnFromCli error: %v", err)
		return
	}
	switch p.Opcode {
	case proto.OpGetAllWatermarks:
		m.handleGetAllWatermarks(p, conn)
	case proto.OpGetAllWatermarksV2:
		m.handleGetAllWatermarksV2(p, conn)
	case proto.OpGetAllWatermarksV3:
		m.handleGetAllWatermarksV3(p, conn)
	case proto.OpExtentRepairRead:
		m.handleExtentRepairReadPacket(p, conn)
	case proto.OpTinyExtentRepairRead:
		m.handleTinyExtentRepairRead(p, conn)
	case proto.OpNotifyReplicasToRepair:
		m.handlePacketToNotifyExtentRepair(p, conn)
	case proto.OpGetAllExtentInfo:
		m.handlePacketToGetAllExtentInfo(p, conn)
	case proto.OpGetAppliedId:
		m.handlePacketToGetAppliedID(p, conn)
	case proto.OpStreamRead:
		m.handleStreamReadPacket(p, conn)
	case proto.OpStreamFollowerRead:
		m.handleStreamFollowerReadPacket(p, conn)
	}
	return
}

func (m *MockTcp) handleGetAllWatermarks(p *repl.Packet, c net.Conn) {
	var (
		err error
	)
	extents := make([]proto.ExtentInfoBlock, 0)
	var data []byte
	if p.ExtentType == proto.NormalExtentType {
		normalExtentCount := RemoteNormalExtentCount
		for i := 0; i < normalExtentCount-1; i++ {
			ei := proto.ExtentInfoBlock{
				uint64(i + 65),
				uint64(i+65) * 1024,
			}
			extents = append(extents, ei)
		}
		extents = append(extents, proto.ExtentInfoBlock{
			LocalCreateExtentId,
			LocalCreateExtentId * 1024,
		})
	} else {
		extentIDs := make([]uint64, 0)
		_ = json.Unmarshal(p.Data, &extentIDs)

		for _, eid := range extentIDs {
			ei := proto.ExtentInfoBlock{
				eid,
				eid * 1024,
			}
			extents = append(extents, ei)
		}
	}
	data, err = json.Marshal(extents)
	if err != nil {
		replyData(p, 0, data, proto.OpErr, c)
		p.PacketOkReply()
		return
	}
	if err = replyData(p, 0, data, proto.OpOk, c); err != nil {
		fmt.Printf("replyData err: %v", err)
	}
	p.PacketOkReply()
}

func (m *MockTcp) handleGetAllWatermarksV2(p *repl.Packet, c net.Conn) {
	var (
		err error
	)

	if SupportedGetAllWatermarksVersion < 2 {
		_ = replyData(p, 0, []byte(repl.ErrorUnknownOp.Error()), proto.OpErr, c)
		return
	}

	var data []byte
	if p.ExtentType == proto.NormalExtentType {
		normalExtentCount := RemoteNormalExtentCount
		data = make([]byte, 16*normalExtentCount)
		index := 0
		for i := 0; i < normalExtentCount-1; i++ {
			ei := &proto.ExtentInfoBlock{
				uint64(i + 65),
				uint64(i+65) * 1024,
			}
			binary.BigEndian.PutUint64(data[index:index+8], ei[storage.FileID])
			index += 8
			binary.BigEndian.PutUint64(data[index:index+8], ei[storage.Size])
			index += 8
		}
		ei := &proto.ExtentInfoBlock{
			LocalCreateExtentId,
			LocalCreateExtentId * 1024,
		}
		binary.BigEndian.PutUint64(data[index:index+8], ei[storage.FileID])
		index += 8
		binary.BigEndian.PutUint64(data[index:index+8], ei[storage.Size])
		index += 8
		data = data[:index]
	} else {
		var extentIDs = make([]uint64, 0, len(p.Data)/8)
		var extentID uint64
		var reader = bytes.NewReader(p.Data)
		for {
			err = binary.Read(reader, binary.BigEndian, &extentID)
			if err == io.EOF {
				err = nil
				break
			}
			if err != nil {
				return
			}
			extentIDs = append(extentIDs, extentID)
		}
		data = make([]byte, 16*len(extentIDs))
		index := 0
		for _, eid := range extentIDs {
			ei := &proto.ExtentInfoBlock{
				eid,
				eid * 1024,
			}
			binary.BigEndian.PutUint64(data[index:index+8], ei[storage.FileID])
			index += 8
			binary.BigEndian.PutUint64(data[index:index+8], ei[storage.Size])
			index += 8
		}
		data = data[:index]
	}
	p.PacketOkWithBody(data)
	if err = response(p, c); err != nil {
		fmt.Printf("replyData err: %v", err)
	}
}

func (m *MockTcp) handleGetAllWatermarksV3(p *repl.Packet, c net.Conn) {
	var (
		err error
	)

	if SupportedGetAllWatermarksVersion < 3 {
		p.PackErrorBody("ActionGetAllExtentWatermarksV3", repl.ErrorUnknownOp.Error())
		err = response(p, c)
		if err != nil {
			log.LogErrorf("ActionGetAllExtentWatermarksV3 response err: %v", err)
		}
		return
	}
	var data []byte
	if p.ExtentType == proto.NormalExtentType {
		normalExtentCount := RemoteNormalExtentCount
		data = make([]byte, 16*normalExtentCount)
		index := 0
		for i := 0; i < normalExtentCount-1; i++ {
			ei := &proto.ExtentInfoBlock{
				storage.FileID: uint64(i + 65),
				storage.Size:   uint64(i+65) * 1024,
			}
			binary.BigEndian.PutUint64(data[index:index+8], ei[storage.FileID])
			index += 8
			binary.BigEndian.PutUint64(data[index:index+8], ei[storage.Size])
			index += 8
		}
		ei := &proto.ExtentInfoBlock{
			storage.FileID: LocalCreateExtentId,
			storage.Size:   LocalCreateExtentId * 1024,
		}
		binary.BigEndian.PutUint64(data[index:index+8], ei[storage.FileID])
		index += 8
		binary.BigEndian.PutUint64(data[index:index+8], ei[storage.Size])
		index += 8
		data = data[:index]
		log.LogInfof("handleGetAllWatermarksV3 normal, data:%v, extents:%v", len(data), normalExtentCount)
	} else {
		var extentIDs = make([]uint64, 0, len(p.Data)/8)
		var extentID uint64
		var reader = bytes.NewReader(p.Data)
		for {
			err = binary.Read(reader, binary.BigEndian, &extentID)
			if err == io.EOF {
				err = nil
				break
			}
			if err != nil {
				return
			}
			extentIDs = append(extentIDs, extentID)
		}
		data = make([]byte, 16*len(extentIDs))
		index := 0
		for _, eid := range extentIDs {
			ei := &proto.ExtentInfoBlock{
				eid,
				eid * 1024,
			}
			binary.BigEndian.PutUint64(data[index:index+8], ei[storage.FileID])
			index += 8
			binary.BigEndian.PutUint64(data[index:index+8], ei[storage.Size])
			index += 8
		}
		data = data[:index]
		log.LogInfof("handleGetAllWatermarksV3 tiny, data:%v, extents:%v, extent type:%v", len(data), len(extentIDs), p.ExtentType)
	}
	var res = make([]byte, len(data)+8)
	binary.BigEndian.PutUint64(res[:8], LocalCreateExtentId)
	copy(res[8:], data)
	p.PacketOkWithBody(res)
	if err = response(p, c); err != nil {
		log.LogErrorf("replyData err: %v", err)
		return
	}
	log.LogInfof("handleGetAllWatermarksV3, response success, data:%v, extent type:%v, res:%v", len(data), p.ExtentType, len(res))
}

func response(reply *repl.Packet, c net.Conn) (err error) {
	if reply.IsErrPacket() {
		err = fmt.Errorf(reply.LogMessage("ActionWriteToClient", c.RemoteAddr().String(),
			reply.StartT, fmt.Errorf(string(reply.Data[:reply.Size]))))
		log.LogErrorf(err.Error())
	}

	if err = reply.WriteToConn(c, proto.WriteDeadlineTime); err != nil {
		err = fmt.Errorf(reply.LogMessage("ActionWriteToClient", fmt.Sprintf("local(%v)->remote(%v)", c.LocalAddr().String(),
			c.RemoteAddr().String()), reply.StartT, err))
		err = fmt.Errorf("remote(%v) will exit error(%v)",
			c.RemoteAddr(), err)
		log.LogErrorf(err.Error())
	}
	if log.IsDebugEnabled() {
		log.LogDebugf(reply.LogMessage("ActionWriteToClient",
			c.RemoteAddr().String(), reply.StartT, err))
	}
	return nil
}

func replyData(p *repl.Packet, ExtentOffset int64, data []byte, resultCode uint8, c net.Conn) (err error) {
	reply := repl.NewStreamReadResponsePacket(p.Ctx(), p.ReqID, p.PartitionID, p.ExtentID)
	reply.ArgLen = 17
	reply.Arg = make([]byte, reply.ArgLen)
	binary.BigEndian.PutUint64(reply.Arg[9:17], uint64(len(data)))
	reply.Data = data
	reply.Size = uint32(len(reply.Data))
	reply.ResultCode = resultCode
	reply.Opcode = p.Opcode
	reply.ExtentOffset = ExtentOffset
	reply.CRC = crc32.ChecksumIEEE(reply.Data[:reply.Size])
	header := make([]byte, unit.PacketHeaderSize)
	reply.MarshalHeader(header)
	if _, err = c.Write(header); err == nil {
		if _, err = c.Write(reply.Arg[:int(reply.ArgLen)]); err == nil {
			if reply.Data != nil && reply.Size != 0 {
				_, err = c.Write(reply.Data[:reply.Size])
			}
		}
	}
	return
}

func (m *MockTcp) handleExtentRepairReadPacket(p *repl.Packet, c net.Conn) {
	size := p.ExtentID * 1024
	data := make([]byte, size-uint64(p.ExtentOffset))
	if err := replyData(p, p.ExtentOffset, data, proto.OpOk, c); err != nil {
		fmt.Printf("replyData err: %v", err)
	}
	p.PacketOkReply()
	log.LogInfof("handleExtentRepairReadPacket, response extent:%v offset:%v data:%v remote:%v", p.ExtentID, p.ExtentOffset, len(data), c.RemoteAddr())
}

func (m *MockTcp) handleTinyExtentRepairRead(p *repl.Packet, c net.Conn) {
	tinyExtentFinfoSize := p.ExtentID * 1024
	needReplySize := int64(tinyExtentFinfoSize - uint64(p.ExtentOffset))
	avaliReplySize := uint64(needReplySize)
	data := make([]byte, avaliReplySize)
	if err := replyData(p, p.ExtentOffset, data, proto.OpOk, c); err != nil {
		fmt.Printf("replyData err: %v", err)
	}
	p.PacketOkReply()
	log.LogInfof("handleExtentRepairReadPacket, response extent:%v offset:%v data:%v remote:%v", p.ExtentID, p.ExtentOffset, len(data), c.RemoteAddr())
}

type DataPartitionRepairTask struct {
	TaskType                       uint8
	addr                           string
	extents                        map[uint64]storage.ExtentInfoBlock
	ExtentsToBeCreated             []storage.ExtentInfoBlock
	ExtentsToBeRepaired            []storage.ExtentInfoBlock
	ExtentsToBeRepairedSource      map[uint64]string
	LeaderTinyDeleteRecordFileSize int64
	LeaderAddr                     string
}

func (m *MockTcp) handlePacketToNotifyExtentRepair(p *repl.Packet, c net.Conn) {
	mf := new(DataPartitionRepairTask)
	err := json.Unmarshal(p.Data, mf)
	if err != nil {
		return
	}
	if err = replyData(p, p.ExtentOffset, []byte{}, proto.OpOk, c); err != nil {
		fmt.Printf("replyData err: %v", err)
	}
	p.PacketOkReply()
}

func (m *MockTcp) handlePacketToGetAllExtentInfo(p *repl.Packet, c net.Conn) {
	if ReplyGetRemoteExtentInfoForValidateCRCCount > 0 {
		_ = replyData(p, p.ExtentOffset, []byte{}, proto.OpErr, c)
		ReplyGetRemoteExtentInfoForValidateCRCCount--
		return
	}
	var extentCount = LocalCreateExtentId
	var extents []storage.ExtentInfoBlock
	var i uint64 = 1
	for ; i <= extentCount; i++ {
		extents = append(extents, storage.ExtentInfoBlock{
			i,
			i * 1024,
			1668494561,
		})
	}
	data := make([]byte, extentCount*20)
	index := 0
	for _, ext := range extents {
		binary.BigEndian.PutUint64(data[index:index+8], ext[storage.FileID])
		index += 8
		binary.BigEndian.PutUint64(data[index:index+8], ext[storage.Size])
		index += 8
		binary.BigEndian.PutUint32(data[index:index+4], uint32(ext[storage.Crc]))
		index += 4
	}
	data = data[:index]
	if err := replyData(p, p.ExtentOffset, data, proto.OpOk, c); err != nil {
		fmt.Printf("replyData err: %v", err)
	}
	p.PacketOkReply()
}

func (m *MockTcp) handlePacketToGetAppliedID(p *repl.Packet, c net.Conn) {
	var err error
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, 2)
	p.PacketOkWithBody(buf)
	p.AddMesgLog(fmt.Sprintf("_AppliedID(%v)", 2))
	if err = p.WriteToConn(c, proto.WriteDeadlineTime); err != nil {
		err = fmt.Errorf(p.LogMessage(repl.ActionWriteToClient, fmt.Sprintf("local(%v)->remote(%v)", c.LocalAddr().String(),
			c.RemoteAddr().String()), p.StartT, err))
		err = fmt.Errorf("ReplProtocol(%v) will exit error(%v)",
			c.RemoteAddr(), err)
		log.LogErrorf(err.Error())
	}
	return
}

func (m *MockTcp) handleStreamFollowerReadPacket(p *repl.Packet, connect net.Conn) {
	m.handleReadPacket(p, connect)
}

func (m *MockTcp) handleStreamReadPacket(p *repl.Packet, connect net.Conn) {
	m.handleReadPacket(p, connect)
}

func (m *MockTcp) handleReadPacket(p *repl.Packet, connect net.Conn) {
	var (
		err error
	)
	defer func() {
		if err != nil {
			logContent := fmt.Sprintf("action[operatePacket] %v.",
				p.LogMessage(p.GetOpMsg(), connect.RemoteAddr().String(), p.StartT, err))
			log.LogErrorf(logContent)
			p.PackErrorBody("ActionStreamRead", err.Error())
			p.WriteToConn(connect, proto.WriteDeadlineTime)
		}
	}()

	needReplySize := p.Size
	offset := p.ExtentOffset
	localData := RandTestData(UpstreamReadSize, UpstreamReadSeed)
	for {
		if needReplySize <= 0 {
			break
		}
		err = nil
		reply := repl.NewStreamReadResponsePacket(p.Ctx(), p.ReqID, p.PartitionID, p.ExtentID)
		reply.StartT = p.StartT
		currReadSize := uint32(unit.Min(int(needReplySize), unit.ReadBlockSize))
		if currReadSize == unit.ReadBlockSize {
			reply.Data, _ = proto.Buffers.Get(unit.ReadBlockSize)
		} else {
			reply.Data = make([]byte, currReadSize)
		}

		reply.ExtentOffset = offset
		p.Size = uint32(currReadSize)
		p.ExtentOffset = offset

		//fake read
		reply.CRC = crc32.ChecksumIEEE(localData[offset : offset+int64(currReadSize)])
		copy(reply.Data[0:currReadSize], localData[offset:offset+int64(currReadSize)])

		p.CRC = reply.CRC
		if err != nil {
			if currReadSize == unit.ReadBlockSize {
				proto.Buffers.Put(reply.Data)
			}
			return
		}
		reply.Size = uint32(currReadSize)
		reply.ResultCode = proto.OpOk
		reply.Opcode = p.Opcode
		p.ResultCode = proto.OpOk

		err = func() error {
			var netErr error
			netErr = reply.WriteToConn(connect, proto.WriteDeadlineTime)
			return netErr
		}()
		if err != nil {
			if currReadSize == unit.ReadBlockSize {
				proto.Buffers.Put(reply.Data)
			}
			logContent := fmt.Sprintf("action[operatePacket] %v.",
				reply.LogMessage(reply.GetOpMsg(), connect.RemoteAddr().String(), reply.StartT, err))
			log.LogErrorf(logContent)
			return
		}
		needReplySize -= currReadSize
		offset += int64(currReadSize)
		if currReadSize == unit.ReadBlockSize {
			proto.Buffers.Put(reply.Data)
		}
		logContent := fmt.Sprintf("action[operatePacket] %v.",
			reply.LogMessage(reply.GetOpMsg(), connect.RemoteAddr().String(), reply.StartT, err))
		log.LogReadf(logContent)
	}
	p.PacketOkReply()

	return
}
