package datanode

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/log"
	"math/rand"
	"sync"
	"time"
)

type rndWrtOpItem struct {
	opcode   uint8
	extentID uint64
	offset   int64
	size     int64
	data     []byte
	crc      uint32
	magic    int
}

// Marshal random write value to binary data.
// Binary frame structure:
//  +------+----+------+------+------+------+------+
//  | Item | extentID | offset | size | crc | data |
//  +------+----+------+------+------+------+------+
//  | byte |     8    |    8   |  8   |  4  | size |
//  +------+----+------+------+------+------+------+

const (
	BinaryMarshalMagicVersion        = 0xFF
	RandomWriteRaftLogMagicVersionV3 = 0xF3
	MaxRandomWriteOpItemPoolSize     = 32
)

func MarshalRandWriteRaftLogV3(opcode uint8, extentID uint64, offset, size int64, data []byte, crc uint32) (result []byte, err error) {
	if len(data) < proto.RandomWriteRaftLogV3HeaderSize {
		return nil, fmt.Errorf("data too low for MarshalRandWriteRaftLogV3(%v)", len(data))
	}
	var index int
	binary.BigEndian.PutUint32(data[index:index+4], uint32(RandomWriteRaftLogMagicVersionV3))
	index += 4
	data[index] = opcode
	index += 1
	binary.BigEndian.PutUint64(data[index:index+8], extentID)
	index += 8
	binary.BigEndian.PutUint64(data[index:index+8], uint64(offset))
	index += 8
	binary.BigEndian.PutUint64(data[index:index+8], uint64(size))
	index += 8
	binary.BigEndian.PutUint32(data[index:index+4], uint32(crc))
	index += 4
	result = data
	return
}

func MarshalRandWriteRaftLog(opcode uint8, extentID uint64, offset, size int64, data []byte, crc uint32) (result []byte, err error) {
	buff := bytes.NewBuffer(make([]byte, 0))
	buff.Grow(8 + 8*2 + 4 + int(size) + 4 + 4)
	if err = binary.Write(buff, binary.BigEndian, uint32(BinaryMarshalMagicVersion)); err != nil {
		return
	}
	if err = binary.Write(buff, binary.BigEndian, opcode); err != nil {
		return
	}
	if err = binary.Write(buff, binary.BigEndian, extentID); err != nil {
		return
	}
	if err = binary.Write(buff, binary.BigEndian, offset); err != nil {
		return
	}
	if err = binary.Write(buff, binary.BigEndian, size); err != nil {
		return
	}
	if err = binary.Write(buff, binary.BigEndian, crc); err != nil {
		return
	}
	if _, err = buff.Write(data); err != nil {
		return
	}
	result = buff.Bytes()
	return
}

var (
	RandomWriteOpItemPool [MaxRandomWriteOpItemPoolSize]*sync.Pool
)

func init() {
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < MaxRandomWriteOpItemPoolSize; i++ {
		RandomWriteOpItemPool[i] = &sync.Pool{
			New: func() interface{} {
				return new(rndWrtOpItem)
			},
		}
	}
}

func GetRandomWriteOpItem() (item *rndWrtOpItem) {
	magic := rand.Intn(MaxRandomWriteOpItemPoolSize)
	item = RandomWriteOpItemPool[magic].Get().(*rndWrtOpItem)
	item.magic = magic
	item.size = 0
	item.crc = 0
	item.offset = 0
	item.extentID = 0
	item.opcode = 0
	item.data = nil
	return
}

func PutRandomWriteOpItem(item *rndWrtOpItem) {
	if item == nil || item.magic == 0 {
		return
	}
	item.size = 0
	item.crc = 0
	item.offset = 0
	item.extentID = 0
	item.opcode = 0
	item.data = nil
	RandomWriteOpItemPool[item.magic].Put(item)
}

// RandomWriteSubmit submits the proposal to raft.
func UnmarshalRandWriteRaftLog(raw []byte, includeData bool) (opItem *rndWrtOpItem, err error) {
	var index int
	version := binary.BigEndian.Uint32(raw[index : index+4])
	//index+=4
	//if version==RandomWriteRaftLogMagicVersionV3{
	//	return BinaryUnmarshalRandWriteRaftLogV3(raw)
	//}
	buff := bytes.NewBuffer(raw)
	if err = binary.Read(buff, binary.BigEndian, &version); err != nil {
		return
	}

	if version != BinaryMarshalMagicVersion && version != RandomWriteRaftLogMagicVersionV3 {
		opItem, err = UnmarshalOldVersionRaftLog(raw)
		return
	}
	opItem = GetRandomWriteOpItem()
	if err = binary.Read(buff, binary.BigEndian, &opItem.opcode); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &opItem.extentID); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &opItem.offset); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &opItem.size); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &opItem.crc); err != nil {
		return
	}
	if !includeData {
		return
	}
	opItem.data = make([]byte, opItem.size)
	if _, err = buff.Read(opItem.data); err != nil {
		return
	}

	return
}

// RandomWriteSubmit submits the proposal to raft.
func BinaryUnmarshalRandWriteRaftLogV3(raw []byte) (opItem *rndWrtOpItem, err error) {
	opItem = GetRandomWriteOpItem()
	var index int
	if len(raw) < proto.RandomWriteRaftLogV3HeaderSize {
		err = fmt.Errorf("unavali RandomWriteRaftlog Header, raw len(%v)", len(raw))
	}
	version := binary.BigEndian.Uint32(raw[index : index+4])
	index += 4
	if version != RandomWriteRaftLogMagicVersionV3 {
		return nil, fmt.Errorf("unavali raftLogVersion %v", RandomWriteRaftLogMagicVersionV3)
	}
	opItem.opcode = raw[index]
	index += 1
	opItem.extentID = binary.BigEndian.Uint64(raw[index : index+8])
	index += 8
	opItem.offset = int64(binary.BigEndian.Uint64(raw[index : index+8]))
	index += 8
	opItem.size = int64(binary.BigEndian.Uint64(raw[index : index+8]))
	index += 8
	opItem.crc = binary.BigEndian.Uint32(raw[index : index+4])
	index += 4
	if opItem.size+int64(index) != int64(len(raw)) {
		err = fmt.Errorf("unavali RandomWriteRaftlog body, raw len(%v), has unmarshal(%v) opItemSize(%v)", len(raw), index, opItem.size)
		return nil, err
	}
	opItem.data = raw[index : int64(index)+opItem.size]

	return
}

func UnmarshalOldVersionRaftLog(raw []byte) (opItem *rndWrtOpItem, err error) {
	raftOpItem := new(RaftCmdItem)
	defer func() {
		log.LogDebugf("Unmarsh use oldVersion,result %v", err)
	}()
	if err = json.Unmarshal(raw, raftOpItem); err != nil {
		return
	}
	opItem, err = UnmarshalOldVersionRandWriteOpItem(raftOpItem.V)
	if err != nil {
		return
	}
	opItem.opcode = uint8(raftOpItem.Op)
	return
}

func UnmarshalOldVersionRandWriteOpItem(raw []byte) (result *rndWrtOpItem, err error) {
	var opItem rndWrtOpItem
	buff := bytes.NewBuffer(raw)
	if err = binary.Read(buff, binary.BigEndian, &opItem.extentID); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &opItem.offset); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &opItem.size); err != nil {
		return
	}
	if err = binary.Read(buff, binary.BigEndian, &opItem.crc); err != nil {
		return
	}
	opItem.data = make([]byte, opItem.size)
	if _, err = buff.Read(opItem.data); err != nil {
		return
	}
	result = &opItem
	return
}