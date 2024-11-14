package master

import (
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
	"time"
)

func TestMessageChannel(t *testing.T) {
	clusterName := "testCluster"
	masterNodeIp := "127.0.0.1"
	messageSize := 200
	logIndexBase := uint64(10000)

	s := NewServer()
	s.clusterName = clusterName
	producer := &MetadataMQProducer{}
	producer.s = s
	producer.msgChan = make(chan *RaftCmdWithIndex, 1000)

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
				assert.Equal(t, msg.Cmd.Op, opSyncAllocCommonID)
				assert.Equal(t, msg.Cmd.K, maxCommonIDKey)
			default:
				time.Sleep(10 * time.Millisecond)
			}
		}
	}(producer.msgChan)

	var cmds []*RaftCmd
	for i := 1; i <= messageSize; i++ {
		testCmd := newRaftTestCmd(uint64(i))
		cmds = append(cmds, testCmd)
	}
	for i, cmd := range cmds {
		producer.addCommandMessage(cmd, logIndexBase+uint64(i), clusterName, masterNodeIp, true)
	}
	producer.shutdown()
	assert.Equal(t, messageSize, messageCounter)
}

func newRaftTestCmd(commId uint64) (cmd *RaftCmd) {
	metadata := new(RaftCmd)
	metadata.Op = opSyncAllocCommonID
	metadata.K = maxCommonIDKey
	metadata.V = []byte(strconv.FormatUint(commId, 10))
	return metadata
}
