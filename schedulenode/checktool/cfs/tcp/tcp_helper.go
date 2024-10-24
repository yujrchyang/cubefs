package tcp

import (
	"context"
	"fmt"
	"github.com/cubefs/cubefs/proto"
	"github.com/cubefs/cubefs/util/connpool"
	"github.com/cubefs/cubefs/util/errors"
	"net"
)

var (
	gConnectPool = connpool.NewConnectPool()
)

// SendTcpPacket spark
func SendTcpPacket(ctx context.Context, addr string, packet *proto.Packet) (reply *proto.Packet, err error) {
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
	reply = new(proto.Packet)
	reply.SetCtx(ctx)
	if err = reply.ReadFromConn(conn, proto.SyncSendTaskDeadlineTime); err != nil {
		return
	}
	if reply.ResultCode != proto.OpOk {
		err = errors.New(string(reply.Data[:reply.Size]))
		return
	}
	return
}
