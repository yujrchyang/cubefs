package infra

import (
	"net"
	"time"
)

func CheckConn(conn net.Conn) (err error) {
	err = conn.SetWriteDeadline(time.Now().Add(time.Millisecond * 50))
	if err != nil {
		return
	}
	_, err = conn.Write(nil)
	if err != nil {
		return
	}
	return
}
