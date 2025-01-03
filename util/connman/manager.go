package connman

import (
	"context"
	"net"
	"sync"
	"time"
)

const (
	DefaultIdleTimeout           = 30 * time.Second
	DefaultDialTimeout           = 1 * time.Second
	DefaultGCInterval            = 1 * time.Second
	DefaultBlacklistTestInterval = 1 * time.Second
	DefaultBlacklistTTL          = 1 * time.Minute

	tcpNetwork = "tcp"
)

type EventListener interface {
	Connected(conn net.Conn)
	Disconnected(conn net.Conn)

	Read(conn net.Conn, n int)
	Wrote(conn net.Conn, n int)

	AddrBlocked(addr string)
	AddrUnblocked(addr string)
}

// Config 表示连接管理器的配置
type Config struct {
	IdleTimeout           time.Duration // 空闲超时时间
	DailTimeout           time.Duration // 拨号超时时间
	GCInterval            time.Duration // 垃圾回收间隔
	BlacklistTestInterval time.Duration // 黑名单测试间隔
	BlacklistTTL          time.Duration // 黑名单超时时间
	EventListener         EventListener // 事件监听器
}

// NewDefaultConfig 返回默认配置
func NewDefaultConfig() *Config {
	return &Config{
		IdleTimeout:           DefaultIdleTimeout,
		DailTimeout:           DefaultDialTimeout,
		GCInterval:            DefaultGCInterval,
		BlacklistTestInterval: DefaultBlacklistTestInterval,
		BlacklistTTL:          DefaultBlacklistTTL,
	}
}

type Stats struct {
	IdleTimeout      time.Duration
	DialTimeout      time.Duration
	BlacklistTTL     time.Duration
	DisableBlackList bool
	Blacklist        map[string]int64
}

// ConnManager 管理连接池
type ConnManager struct {
	connectorMu sync.RWMutex          // 读写锁保护 connectors
	connectors  map[string]*Connector // 地址到连接池的映射

	eventLn EventListener

	listeners  map[string]net.Listener
	listenerMu sync.RWMutex

	idleTimeout time.Duration // 空闲超时时间
	dialTimeout time.Duration // 拨号超时时间

	ctx      context.Context         // 管理器的上下文
	cancel   context.CancelCauseFunc // 取消函数
	stopOnce sync.Once               // 确保关闭操作只执行一次
	wg       sync.WaitGroup          // 等待所有 goroutine 结束

	blacklist             sync.Map
	blacklistTTL          time.Duration
	blacklistTestInterval time.Duration
	disableBlackList      bool

	gcInterval time.Duration
}

// NewConnectionManager 创建一个新的连接管理器
func NewConnectionManager(conf *Config) (cp *ConnManager) {
	ctx, cancel := context.WithCancelCause(context.Background())
	cp = &ConnManager{
		connectors:            make(map[string]*Connector),
		eventLn:               conf.EventListener,
		idleTimeout:           conf.IdleTimeout,
		dialTimeout:           conf.DailTimeout,
		blacklistTTL:          conf.BlacklistTTL,
		blacklistTestInterval: conf.BlacklistTestInterval,
		gcInterval:            conf.GCInterval,
		ctx:                   ctx,
		cancel:                cancel,
	}

	cp.runAsync(cp.scheduleGC)
	cp.runAsync(cp.scheduleBlacklistTest)

	return cp
}

func (cm *ConnManager) Stats() *Stats {
	stats := new(Stats)
	if cm == nil {
		return stats
	}
	stats.IdleTimeout = cm.idleTimeout
	stats.DialTimeout = cm.dialTimeout
	stats.BlacklistTTL = cm.blacklistTTL
	stats.DisableBlackList = cm.disableBlackList
	stats.Blacklist = make(map[string]int64)
	cm.blacklist.Range(func(key, value interface{}) bool {
		stats.Blacklist[key.(string)] = value.(int64)
		return true
	})
	return stats
}

func (cm *ConnManager) runAsync(f func()) {
	cm.wg.Add(1)
	go cm.run(f)
}

func (cm *ConnManager) run(f func()) {
	defer func() {
		cm.wg.Done()
	}()
	for {
		select {
		case <-cm.ctx.Done():
			return
		default:
		}
		func() {
			defer func() {
				if r := recover(); r != nil {
				}
			}()
			f()
		}()
	}
}

// GetConnect 拨号并返回一个连接
//
// 该方法会根据提供的地址拨号并返回一个连接。如果连接管理器已经关闭，
// 则返回 ManagerClosedError 错误。如果地址在黑名单中，则返回 AddrBlockedError 错误。
//
// 参数:
// - addr: 目标地址，格式为 "host:port"。
//
// 返回值:
// - c: 返回的连接指针，类型为 *TCPConn。该指针实现了 net.Conn 接口的所有方法，包括 Read, Write, Close, LocalAddr, RemoteAddr, SetDeadline, SetReadDeadline 和 SetWriteDeadline。
// - err: 可能的错误。如果拨号失败，返回相应的错误。
func (cm *ConnManager) GetConnect(addr string) (c net.Conn, err error) {
	if cm.Closed() {
		return nil, NewDialNetOpError(tcpNetwork, addr, ManagerClosedError)
	}
	pool := cm.createAndGetPool(addr)
	c, err = pool.Get()
	return c, err
}

func (cm *ConnManager) GetConnectWithBlackList(addr string) (c net.Conn, err error) {
	if cm.Closed() {
		return nil, NewDialNetOpError(tcpNetwork, addr, ManagerClosedError)
	}
	if err = cm.checkAddr(addr); err != nil {
		return nil, err
	}

	pool := cm.createAndGetPool(addr)
	c, err = pool.Get()
	if !cm.disableBlackList && IsDialTimeoutError(err) {
		cm.blockAddr(addr)
	}
	return c, err
}

func (cm *ConnManager) PutConnect(c net.Conn, close bool) {
	if c == nil {
		return
	}
	if close {
		_ = c.Close()
		return
	}
	rc, is := c.(*TCPConn)
	if !is {
		_ = c.Close()
		return
	}
	rc.Reuse()
	return
}

func (cm *ConnManager) PutConnectWithErr(c net.Conn, err error) {
	if c == nil {
		return
	}
	if err != nil {
		_ = c.Close()
		return
	}
	cm.PutConnect(c, false)
}

func (cm *ConnManager) checkAddr(addr string) error {
	if cm.disableBlackList {
		return nil
	}
	if t, ok := cm.blacklist.Load(addr); ok && (cm.blacklistTTL == 0 || time.Now().UnixNano()-t.(int64) < int64(cm.blacklistTTL)) {
		return NewDialNetOpError(tcpNetwork, addr, AddrBlockedError)
	}
	return nil
}

func (cm *ConnManager) blockAddr(addr string) {
	cm.blacklist.Store(addr, time.Now().UnixNano())
	cm.connectorMu.Lock()
	pool, exist := cm.connectors[addr]
	if exist {
		delete(cm.connectors, addr)
	}
	cm.connectorMu.Unlock()
	if exist {
		pool.Close()
	}
	if cm.eventLn != nil {
		cm.eventLn.AddrBlocked(addr)
	}
}

// createAndGetPool 创建并获取连接池
func (cm *ConnManager) createAndGetPool(addr string) *Connector {
	cm.connectorMu.RLock()
	pool, ok := cm.connectors[addr]
	cm.connectorMu.RUnlock()
	if !ok {
		cm.connectorMu.Lock()
		pool, ok = cm.connectors[addr]
		if !ok {
			pool = cm.newConnector(addr)
			cm.connectors[addr] = pool
		}
		cm.connectorMu.Unlock()
	}
	return pool
}

// SetIdleTimeout 设置空闲超时时间
func (cm *ConnManager) SetIdleTimeout(timeout time.Duration) {
	cm.idleTimeout = timeout
}

// SetDialTimeout 设置拨号超时时间
func (cm *ConnManager) SetDialTimeout(timeout time.Duration) {
	cm.dialTimeout = timeout
}

func (cm *ConnManager) SetBlacklistTTL(ttl time.Duration) {
	cm.blacklistTTL = ttl
}

func (cm *ConnManager) DisableBlackList(disable bool) {
	cm.disableBlackList = disable
}

func (cm *ConnManager) IsDisableBlackList() bool {
	return cm.disableBlackList
}

// Cleanup 清除指定地址的连接池
func (cm *ConnManager) Cleanup(addr string) {
	cm.connectorMu.Lock()
	pool, ok := cm.connectors[addr]
	if ok {
		delete(cm.connectors, addr)
	}
	cm.connectorMu.Unlock()
	if ok {
		pool.Close()
	}
}

// scheduleGC 运行垃圾回收调度器
func (cm *ConnManager) scheduleGC() {
	var ticker = time.NewTicker(cm.gcInterval)
	defer func() {
		ticker.Stop()
	}()
	for {
		select {
		case <-cm.ctx.Done():
			return
		case <-ticker.C:
		}
		cm.connectorMu.RLock()
		pools := make([]*Connector, 0, len(cm.connectors))
		for _, pool := range cm.connectors {
			pools = append(pools, pool)
		}
		cm.connectorMu.RUnlock()
		for _, pool := range pools {
			pool.GC()
		}
	}
}

func (cm *ConnManager) scheduleBlacklistTest() {
	var ticker = time.NewTicker(cm.blacklistTestInterval)
	defer func() {
		ticker.Stop()
	}()
	var blacklistVisitor = func(key, value interface{}) bool {
		addr := key.(string)
		pool, err := cm.testAddress(addr)
		if err == nil {
			cm.connectorMu.Lock()
			_, exists := cm.connectors[addr]
			if !exists {
				cm.connectors[addr] = pool
			}
			cm.connectorMu.Unlock()
			if exists {
				pool.Close()
			}
			cm.blacklist.Delete(addr)
			if cm.eventLn != nil {
				cm.eventLn.AddrUnblocked(addr)
			}
		}
		return true
	}
	for {
		select {
		case <-cm.ctx.Done():
			return
		case <-ticker.C:
		}
		cm.blacklist.Range(blacklistVisitor)
	}
}

func (cm *ConnManager) testAddress(addr string) (*Connector, error) {
	pool := cm.newConnector(addr)
	if err := pool.Test(); err != nil {
		return nil, err
	}
	return pool, nil
}

// Close 关闭连接管理器
func (cm *ConnManager) Stop() {
	cm.stopOnce.Do(cm.doStop)
	cm.wg.Wait()
}

func (cm *ConnManager) doStop() {
	cm.cancel(ManagerClosedError)

	cm.connectorMu.Lock()
	pools := cm.connectors
	cm.connectors = make(map[string]*Connector)
	cm.connectorMu.Unlock()
	for _, pool := range pools {
		pool.Close()
	}
}

// Closed 检查连接管理器是否已关闭
func (cm *ConnManager) Closed() bool {
	return cm.ctx.Err() != nil
}

// GetDailTimeout 获取拨号超时时间
func (cm *ConnManager) GetDailTimeout() time.Duration {
	return cm.dialTimeout
}

// GetIdleTimeout 获取空闲超时时间
func (cm *ConnManager) GetIdleTimeout() time.Duration {
	return cm.idleTimeout
}

// newConnector 创建一个新的连接池
func (cm *ConnManager) newConnector(addr string) (p *Connector) {
	return NewConnector(
		cm.ctx,
		addr,
		cm.GetDailTimeout,
		cm.GetIdleTimeout,
		cm.eventLn)
}
