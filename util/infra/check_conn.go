package infra

import (
	"fmt"
	"net"
	"time"
)

func CheckConnIsAvali(conn net.Conn) (err error) {
	if conn == nil {
		return fmt.Errorf("conn is nil")
	}
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
