package repl

import (
	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/assert"
	"strconv"
	"strings"
	"testing"
)

func Test_identificationErrorResultCode(t *testing.T) {
	p := &Packet{}
	errLog := ErrorUnknownOp.Error()
	errMsg := ErrorUnknownOp.Error()+strconv.Itoa(int(p.Opcode))
	p.identificationErrorResultCode(errLog, errMsg)
	p.Size = uint32(len([]byte(errLog + "_" + errMsg)))
	p.Data = make([]byte, p.Size)
	copy(p.Data[:int(p.Size)], []byte(errLog+"_"+errMsg))
	assert.Equal(t, p.ResultCode, proto.OpArgMismatchErr)
	assert.Equal(t, true, strings.Contains(string(p.Data), "unknown opcode"))
}