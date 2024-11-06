package connman

import (
	"container/list"
	"context"
	"net"
	"sync"
	"time"
)

// GetTimeoutFunc 获取超时时间的函数类型
type GetTimeoutFunc func() time.Duration

// Get 获取超时时间
func (f GetTimeoutFunc) Get() time.Duration {
	if f == nil {
		return 0
	}
	return f()
}

// Connector 表示一个连接池
type Connector struct {
	mu      sync.RWMutex // 读写锁保护 connLst
	connLst *list.List   // 连接列表
	addr    string       // 连接地址

	eventLn EventListener

	getIdleTimeout GetTimeoutFunc // 获取空闲超时时间的函数
	getDialTimeout GetTimeoutFunc // 获取拨号超时时间的函数

	ctx       context.Context         // 连接池的上下文
	close     context.CancelCauseFunc // 取消函数
	closeOnce sync.Once               // 确保关闭操作只执行一次
}

// GC 运行垃圾回收
func (connector *Connector) GC() {
	idleTimeout := connector.getIdleTimeout.Get()
	if idleTimeout == 0 {
		return
	}
	nowUnixNano := time.Now().UnixNano()
	evicts := make([]*TCPConn, 0)
	connector.mu.Lock()
	for ele := connector.connLst.Back(); ele != nil; ele = ele.Prev() {
		o, is := ele.Value.(*TCPConn)
		if !is {
			continue
		}
		if o.HasError() || nowUnixNano-o.lastActive > int64(idleTimeout) {
			connector.connLst.Remove(ele)
			evicts = append(evicts, o)
		}
	}
	connector.mu.Unlock()
	for _, o := range evicts {
		_ = o.Close()
	}
}

// Close 关闭连接池
func (connector *Connector) Close() {
	connector.closeOnce.Do(connector.doClose)
}

func (connector *Connector) doClose() {
	connector.close(ConnectorClosedError)
	connector.mu.Lock()
	connLst := connector.connLst
	connector.connLst = list.New()
	connector.mu.Unlock()

	for ele := connLst.Front(); ele != nil; ele = ele.Next() {
		if o, is := ele.Value.(*TCPConn); is {
			_ = o.Close()
		}
	}
}

// allocConn 拨号并返回一个池化连接
func (connector *Connector) allocConn() (*TCPConn, error) {
	var err error
	var c net.Conn
	if dailTimeout := connector.getDialTimeout.Get(); dailTimeout > 0 {
		c, err = net.DialTimeout(tcpNetwork, connector.addr, dailTimeout)
	} else {
		c, err = net.Dial(tcpNetwork, connector.addr)
	}
	if err != nil {
		return nil, err
	}
	var conn = &TCPConn{
		conn:       c,
		lastActive: time.Now().UnixNano(),
		eventLn:    connector.eventLn,
	}
	conn.reuse = func() {
		_ = connector.Put(conn)
	}
	if connector.eventLn != nil {
		connector.eventLn.Connected(conn)
	}
	return conn, nil
}

// Get 获取一个池化连接
func (connector *Connector) Get() (c *TCPConn, err error) {
	if connector.Closed() {
		return nil, NewDialNetOpError(tcpNetwork, connector.addr, ConnectorClosedError)
	}
	if c = connector.innerGet(); c != nil {
		return c, nil
	}
	return connector.allocConn()
}

func (connector *Connector) innerGet() (c *TCPConn) {
	var idleTimeout = connector.getIdleTimeout.Get()
	connector.mu.Lock()
	defer connector.mu.Unlock()
	for ele := connector.connLst.Front(); ele != nil; ele = ele.Next() {
		o, is := ele.Value.(*TCPConn)
		connector.connLst.Remove(ele)
		if !is {
			continue
		}
		if o.HasError() || idleTimeout > 0 && time.Now().UnixNano()-o.LastActive() > int64(idleTimeout) {
			_ = o.Close()
			continue
		}
		c = o
		break
	}
	return
}

// Put 将连接放回池中
func (connector *Connector) Put(c *TCPConn) error {
	switch {
	case c == nil, c.Closed():
		return nil
	case connector.Closed(), connector.isConnIdleTimeout(c), c.HasError(), !c.Reusable():
		return c.Close()
	default:
	}

	connector.mu.Lock()
	connector.connLst.PushBack(c)
	connector.mu.Unlock()
	return nil
}

func (connector *Connector) Test() error {
	conn, err := connector.Get()
	if err != nil {
		return err
	}
	conn.Reuse()
	return nil
}

// Closed 检查连接池是否已关闭
func (connector *Connector) Closed() bool {
	return connector.ctx.Err() != nil
}

func (connector *Connector) isConnIdleTimeout(c *TCPConn) bool {
	return connector.getIdleTimeout.Get() > 0 && time.Now().UnixNano()-c.LastActive() > int64(connector.getIdleTimeout.Get())
}

// NewConnector 创建一个新的连接池
func NewConnector(ctx context.Context, addr string,
	getDialTimeout, getIdleTimeout GetTimeoutFunc,
	listener EventListener) (p *Connector) {
	p = &Connector{
		connLst:        list.New(),
		addr:           addr,
		getDialTimeout: getDialTimeout,
		getIdleTimeout: getIdleTimeout,
		eventLn:        listener,
	}
	p.ctx, p.close = context.WithCancelCause(ctx)
	return p
}
