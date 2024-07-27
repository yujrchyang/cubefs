package util

import (
	"context"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/repl"
	"github.com/cubefs/cubefs/util/connpool"
	"github.com/cubefs/cubefs/util/errors"
	"net"
)

var (
	gConnectPool = connpool.NewConnectPool()
)

func SendTcpPacket(ctx context.Context, addr string, packet *repl.Packet) (reply *repl.Packet, err error) {
	var conn *net.TCPConn
	if conn, err = gConnectPool.GetConnect(addr); err != nil {
		err = errors.Trace(err, fmt.Sprintf("get connection failed: %v", err))
		return
	}
	defer func() {
		gConnectPool.PutConnectWithErr(conn, err)
	}()
	if err = packet.WriteToConn(conn, proto.WriteDeadlineTime); err != nil {
		return
	}
	reply = new(repl.Packet)
	reply.SetCtx(ctx)
	if err = reply.ReadFromConn(conn, proto.GetAllWatermarksDeadLineTime); err != nil {
		return
	}
	if reply.ResultCode != proto.OpOk {
		err = errors.New(string(reply.Data[:reply.Size]))
		return
	}
	return
}
