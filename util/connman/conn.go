package connman

import (
	"fmt"
	"net"
	"sync"
	"time"
)

// TCPConn 是受 ConnManager 管理TCP连接, 它实现了标准 net.Conn 接口中的所有方法
type TCPConn struct {
	conn       net.Conn // 底层网络连接
	lastActive int64    // 最后活跃时间的时间戳
	err        error    // 连接上的错误

	reuse func()

	eventLn EventListener

	closeOnce sync.Once
	closed    bool // 连接是否已关闭
}

// String 返回连接的字符串表示
// 包括本地地址、远程地址、最后活跃时间和错误信息
func (c *TCPConn) String() string {
	return fmt.Sprintf("TCPConn(conn %v->%v, lastActive %v, error %v)", c.LocalAddr(), c.RemoteAddr(), c.lastActive, c.err)
}

// HasError 检查连接是否有错误
// 返回 true 表示连接有错误，false 表示没有错误
func (c *TCPConn) HasError() bool {
	return c.err != nil
}

// LastActive 返回连接最后活跃的时间戳
// 返回值为 int64 类型的纳秒时间戳
func (c *TCPConn) LastActive() int64 {
	return c.lastActive
}

// checkError 检查并返回连接的错误
// 如果连接有错误，返回该错误；否则返回 nil
func (c *TCPConn) checkError() error {
	if c.err != nil {
		return c.err
	}
	return nil
}

// updateState 更新连接的状态
// 如果传入的错误不为 nil，则将该错误设置为连接的错误
// 否则更新连接的最后活跃时间为当前时间
func (c *TCPConn) updateState(err error) {
	if c.err == nil && err != nil {
		c.err = err
		return
	}
	c.lastActive = time.Now().UnixNano()
}

// Read 从连接中读取数据
// 参数 b 为读取数据的缓冲区
// 返回读取的字节数和可能的错误
func (c *TCPConn) Read(b []byte) (n int, err error) {
	if err = c.checkError(); err != nil {
		return
	}
	n, err = c.conn.Read(b)
	c.updateState(err)
	if n > 0 && c.eventLn != nil {
		c.eventLn.Read(c, n)
	}
	return
}

func (c *TCPConn) ReadWithDeadline(b []byte, deadline time.Time) (n int, err error) {
	if err = c.SetReadDeadline(deadline); err != nil {
		return
	}
	return c.Read(b)
}

func (c *TCPConn) ReadWithTimeout(b []byte, timeout time.Duration) (n int, err error) {
	return c.ReadWithDeadline(b, time.Now().Add(timeout))
}

// Write 向连接中写入数据
// 参数 b 为要写入的数据
// 返回写入的字节数和可能的错误
func (c *TCPConn) Write(b []byte) (n int, err error) {
	if err = c.checkError(); err != nil {
		return
	}
	n, err = c.conn.Write(b)
	c.updateState(err)
	if n > 0 && c.eventLn != nil {
		c.eventLn.Wrote(c, n)
	}
	return
}

func (c *TCPConn) WriteWithDeadline(b []byte, deadline time.Time) (n int, err error) {
	if err = c.SetWriteDeadline(deadline); err != nil {
		return
	}
	return c.Write(b)
}

func (c *TCPConn) WriteWithTimeout(b []byte, timeout time.Duration) (n int, err error) {
	return c.WriteWithDeadline(b, time.Now().Add(timeout))
}

// Close 关闭连接
func (c *TCPConn) Close() (err error) {
	c.closeOnce.Do(func() {
		err = c.conn.Close()
		c.closed = true
		if c.eventLn != nil {
			c.eventLn.Disconnected(c)
		}
	})
	return
}

func (c *TCPConn) Closed() bool {
	return c.closed
}

func (c *TCPConn) Reusable() bool {
	return c.reuse != nil
}

func (c *TCPConn) Reuse() {
	if c.reuse != nil {
		c.reuse()
	}
}

//func (c *TCPConn) Reuse() (err error) {
//	return c.connector.Put(c)
//}

// LocalAddr 返回连接的本地地址
// 返回值为 net.Addr 类型
func (c *TCPConn) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

// RemoteAddr 返回连接的远程地址
// 返回值为 net.Addr 类型
func (c *TCPConn) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *TCPConn) doSetTime(t time.Time, f func(time.Time) error) (err error) {
	if err = c.checkError(); err != nil {
		return
	}
	err = f(t)
	c.updateState(err)
	return
}

// SetDeadline 设置连接的读写截止时间
// 参数 t 为截止时间
// 返回可能的错误
func (c *TCPConn) SetDeadline(t time.Time) (err error) {
	return c.doSetTime(t, c.conn.SetDeadline)
}

// SetReadDeadline 设置连接的读取截止时间
// 参数 t 为截止时间
// 返回可能的错误
func (c *TCPConn) SetReadDeadline(t time.Time) (err error) {
	return c.doSetTime(t, c.conn.SetReadDeadline)
}

// SetWriteDeadline 设置连接的写入截止时间
// 参数 t 为截止时间
// 返回可能的错误
func (c *TCPConn) SetWriteDeadline(t time.Time) (err error) {
	return c.doSetTime(t, c.conn.SetWriteDeadline)
}
