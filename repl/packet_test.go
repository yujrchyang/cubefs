package repl

import (
	"context"
	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/assert"
	"strconv"
	"strings"
	"testing"
)

func Test_identificationErrorResultCode(t *testing.T) {
	p := &Packet{}
	errLog := ErrorUnknownOp.Error()
	errMsg := ErrorUnknownOp.Error() + strconv.Itoa(int(p.Opcode))
	p.identificationErrorResultCode(errLog, errMsg)
	p.Size = uint32(len([]byte(errLog + "_" + errMsg)))
	p.Data = make([]byte, p.Size)
	copy(p.Data[:int(p.Size)], []byte(errLog+"_"+errMsg))
	assert.Equal(t, p.ResultCode, proto.OpArgMismatchErr)
	assert.Equal(t, true, strings.Contains(string(p.Data), "unknown opcode"))
}

func TestDisableBlackListInCrossRegionHA(t *testing.T) {
	testCases := map[string]struct {
		quorum   int
		hosts    []string
		expected bool
	}{
		"Cross-Region-HA": {
			quorum: 3,
			hosts: []string{
				"192.168.0.11:17030",
				"192.168.0.12:17030",
				"192.168.0.13:17030",
				"192.168.0.14:17030",
				"192.168.0.15:17030",
			},
			expected: true,
		},
		"Normal-quorum-0-replica-5": {
			quorum: 0,
			hosts: []string{
				"192.168.0.11:17030",
				"192.168.0.12:17030",
				"192.168.0.13:17030",
				"192.168.0.14:17030",
				"192.168.0.15:17030",
			},
			expected: false,
		},
		"Normal-quorum-0-replica-3": {
			quorum: 0,
			hosts: []string{
				"192.168.0.11:17030",
				"192.168.0.12:17030",
				"192.168.0.13:17030",
			},
			expected: false,
		},
		"Normal-quorum-1-replica-1": {
			quorum: 1,
			hosts: []string{
				"192.168.0.11:17030",
			},
			expected: false,
		},
		"Normal-quorum-2-replica-2": {
			quorum: 2,
			hosts: []string{
				"192.168.0.11:17030",
				"192.168.0.12:17030",
			},
			expected: false,
		},
		"Normal-quorum-0-replica-2": {
			quorum: 0,
			hosts: []string{
				"192.168.0.11:17030",
				"192.168.0.12:17030",
			},
			expected: false,
		},
		"Normal-quorum-5": {
			quorum: 5,
			hosts: []string{
				"192.168.0.11:17030",
				"192.168.0.12:17030",
				"192.168.0.13:17030",
				"192.168.0.14:17030",
				"192.168.0.15:17030",
			},
			expected: false,
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			packet := NewPacket(context.Background())
			packet.Arg = EncodeReplPacketArg(tc.hosts[1:], tc.quorum)
			packet.ArgLen = uint32(len(packet.Arg))
			packet.followersAddrs, packet.quorum = DecodeReplPacketArg(packet.Arg[:packet.ArgLen])
			assert.Equal(t, tc.expected, packet.IsUseConnWithBlackList())
		})
	}
}
