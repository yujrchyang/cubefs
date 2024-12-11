package mqsender

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"hash/crc32"
	"math"
	"math/rand"
	"strconv"
	"testing"
	"time"
)

const (
	BinaryMarshalMagicVersion = 0xFF
)

type RaftCmdForTest struct {
	Op uint32 `json:"op"`
	K  string `json:"k"`
	V  []byte `json:"v"`
}

type MetaCommandForTest struct {
	Op          uint32              `json:"op"`
	K           []byte              `json:"k"`
	V           []byte              `json:"v"`
	From        string              `json:"frm"` // The address of the client that initiated the operation.
	Timestamp   int64               `json:"ts"`  // DeleteTime of operation
	TrashEnable bool                `json:"te"`  // enable trash
	ReqInfo     *RequestInfoForTest `json:"req"`
}

type RequestInfoForTest struct {
	ClientID           uint64 `json:"client_id"`
	ClientStartTime    int64  `json:"client_sTime"`
	ReqID              int64  `json:"id"`
	ClientIP           uint32 `json:"ip"`
	DataCrc            uint32 `json:"crc"`
	RequestTime        int64  `json:"req_time"`
	EnableRemoveDupReq bool   `json:"enable_rm_dupReq"`
	RespCode           uint8  `json:"-"`
}

type DataCommandForTest struct {
	opcode   uint8
	extentID uint64
	offset   int64
	size     int64
	data     []byte
	crc      uint32
	magic    int
}

func TestMasterCommand(t *testing.T) {
	clusterName := "testCluster"
	masterNodeIp := "127.0.0.1"
	messageSize := 200
	logIndexBase := uint64(10000)
	testOpCode := uint32(0x0C)
	testCmdKey := "#max_common_id"

	producer := &MetadataMQProducer{}
	producer.MsgChan = make(chan *RaftCmdWithIndex, 1000)

	var messageCounter int
	go func(msgChan chan *RaftCmdWithIndex) {
		for {
			select {
			case msg, ok := <-msgChan:
				if !ok {
					return
				}
				if msg == nil {
					continue
				}
				messageCounter++

				var cmdTest RaftCmdForTest
				err := json.Unmarshal(msg.Cmd, &cmdTest)
				if err != nil {
					t.Errorf(err.Error())
					return
				}
				assert.Equal(t, cmdTest.Op, testOpCode)
				assert.Equal(t, cmdTest.K, testCmdKey)
			default:
				time.Sleep(10 * time.Millisecond)
			}
		}
	}(producer.MsgChan)

	var cmds []*RaftCmdForTest
	for i := 1; i <= messageSize; i++ {
		testCmd := newMasterRaftTestCmd(uint64(i), testOpCode, testCmdKey)
		cmds = append(cmds, testCmd)
	}
	for i, cmd := range cmds {
		data, err := json.Marshal(cmd)
		if err != nil {
			t.Errorf(err.Error())
			return
		}
		producer.AddMasterNodeCommand(data, cmd.Op, cmd.K, logIndexBase+uint64(i), clusterName, masterNodeIp, true)
	}
	producer.Shutdown()
	assert.Equal(t, messageSize, messageCounter)
}

func TestMetaCommand(t *testing.T) {
	clusterName := "testCluster"
	metaNodeIp := "127.0.0.1"
	volumeName := "testVolume"
	mpId := uint64(900)
	messageSize := 200
	logIndexBase := uint64(10000)
	testOpCode := uint32(7)
	testCmdKey := "#max_common_id"

	producer := &MetadataMQProducer{}
	producer.MsgChan = make(chan *RaftCmdWithIndex, 1000)

	var messageCounter int
	go func(msgChan chan *RaftCmdWithIndex) {
		for {
			select {
			case msg, ok := <-msgChan:
				if !ok {
					return
				}
				if msg == nil {
					continue
				}
				messageCounter++

				var metaCmdTest MetaCommandForTest
				err := json.Unmarshal(msg.Cmd, &metaCmdTest)
				if err != nil {
					t.Errorf(err.Error())
					return
				}
				assert.Equal(t, metaCmdTest.Op, testOpCode)
				assert.Equal(t, string(metaCmdTest.K), testCmdKey)
			default:
				time.Sleep(10 * time.Millisecond)
			}
		}
	}(producer.MsgChan)

	var cmds []*MetaCommandForTest
	for i := 1; i <= messageSize; i++ {
		testCmd := newMetaCommandTestCmd(uint64(i), testOpCode, testCmdKey)
		cmds = append(cmds, testCmd)
	}
	for i, cmd := range cmds {
		data, err := json.Marshal(cmd)
		if err != nil {
			t.Errorf(err.Error())
			return
		}
		producer.AddMetaNodeCommand(data, cmd.Op, string(cmd.K), logIndexBase+uint64(i), clusterName, volumeName, metaNodeIp, true, mpId)
	}
	producer.Shutdown()
	assert.Equal(t, messageSize, messageCounter)
}

func TestDataCommand(t *testing.T) {
	clusterName := "testCluster"
	metaNodeIp := "127.0.0.1"
	volumeName := "testVolume"
	dpId := uint64(900)
	messageSize := 200
	logIndexBase := uint64(10000)
	testOpCode := uint8(10)
	extentID := uint64(30)
	offset := uint32(104857600)

	producer := &MetadataMQProducer{}
	producer.MsgChan = make(chan *RaftCmdWithIndex, 1000)

	var messageCounter int
	go func(msgChan chan *RaftCmdWithIndex) {
		for {
			select {
			case msg, ok := <-msgChan:
				if !ok {
					return
				}
				if msg == nil {
					continue
				}
				messageCounter++
				dataCmdTest, err := UnmarshalDataCommand(msg.Cmd, true)
				if err != nil {
					t.Errorf(err.Error())
					return
				}
				assert.Equal(t, dataCmdTest.opcode, testOpCode)
			default:
				time.Sleep(10 * time.Millisecond)
			}
		}
	}(producer.MsgChan)

	for i := 0; i < messageSize; i++ {
		data := newDataCommand(testOpCode, extentID, offset)
		producer.AddDataNodeCommand(data, testOpCode, logIndexBase+uint64(i), clusterName, volumeName, metaNodeIp, true, dpId)
	}
	producer.Shutdown()
	assert.Equal(t, messageSize, messageCounter)
}

func newMasterRaftTestCmd(commId uint64, op uint32, cmdKey string) (cmd *RaftCmdForTest) {
	metadata := new(RaftCmdForTest)
	// opSyncAllocCommonID        uint32 = 0x0C
	metadata.Op = op
	metadata.K = cmdKey
	metadata.V = []byte(strconv.FormatUint(commId, 10))
	return metadata
}

func newMetaCommandTestCmd(commId uint64, op uint32, cmdKey string) (cmd *MetaCommandForTest) {
	cmd = new(MetaCommandForTest)
	// opSyncAllocCommonID        uint32 = 0x0C
	cmd.Op = op
	cmd.K = []byte(cmdKey)
	cmd.V = []byte(strconv.FormatUint(commId, 10))
	cmd.From = ""
	cmd.Timestamp = time.Now().Unix()
	cmd.TrashEnable = false
	cmd.ReqInfo = newRequestInfoForTest()
	return cmd
}

func newRequestInfoForTest() (req *RequestInfoForTest) {
	return &RequestInfoForTest{
		ClientID:           uint64(rand.Intn(65535)),
		ReqID:              rand.Int63n(math.MaxInt64),
		ClientIP:           binary.BigEndian.Uint32([]byte{byte(rand.Intn(256)), byte(rand.Intn(256)), byte(rand.Intn(256)), byte(rand.Intn(256))}),
		DataCrc:            uint32(rand.Int31n(math.MaxInt32)),
		RequestTime:        time.Now().UnixMilli(),
		EnableRemoveDupReq: false,
	}
}

func newDataCommand(opcode uint8, extentID uint64, offset uint32) (cmd []byte) {
	dataStr := "data node message test data"
	data := []byte(dataStr)
	size := int64(len(data))
	crc32q := crc32.MakeTable(crc32.IEEE)
	crc := crc32.Checksum(data, crc32q)
	cmd = make([]byte, 4+1+8+8+8+4+size)
	var off int64
	binary.BigEndian.PutUint32(cmd[off:off+4], uint32(BinaryMarshalMagicVersion))
	off += 4
	cmd[off] = opcode
	off += 1
	binary.BigEndian.PutUint64(cmd[off:off+8], extentID)
	off += 8
	binary.BigEndian.PutUint64(cmd[off:off+8], uint64(offset))
	off += 8
	binary.BigEndian.PutUint64(cmd[off:off+8], uint64(size))
	off += 8
	binary.BigEndian.PutUint32(cmd[off:off+4], crc)
	off += 4
	copy(cmd[off:off+size], data[:size])
	return
}

func UnmarshalDataCommand(raw []byte, includeData bool) (opItem *DataCommandForTest, err error) {
	var index int
	version := binary.BigEndian.Uint32(raw[index : index+4])
	buff := bytes.NewBuffer(raw)
	if err = binary.Read(buff, binary.BigEndian, &version); err != nil {
		return
	}

	opItem = &DataCommandForTest{}
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

func TestDataCommandMarshallAndUnmarshall(t *testing.T) {
	opCode := uint8(10)
	extentID := uint64(30)
	offset := uint32(104857600)
	cmd := newDataCommand(opCode, extentID, offset)
	dataCmd, err := UnmarshalDataCommand(cmd, true)
	if err != nil {
		t.Fatalf(err.Error())
	}
	assert.Equal(t, opCode, dataCmd.opcode)
	assert.Equal(t, extentID, dataCmd.extentID)
	assert.Equal(t, int64(offset), dataCmd.offset)
}
