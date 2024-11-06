package connman_test

import (
	"context"
	"errors"
	"io"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cubefs/cubefs/util/connman"
)

type OnConnAcceptFunc func(conn net.Conn)
type OnConnCloseFunc func(conn net.Conn)
type OnConnReceiveFunc func(conn net.Conn, buf []byte) error
type OnConnError func(conn net.Conn, err error)

type tcpServer struct {
	port    int
	ln      net.Listener
	connMap sync.Map

	ctx      context.Context
	stop     context.CancelFunc
	stopOnce sync.Once

	onConnAccept  OnConnAcceptFunc
	onConnClose   OnConnCloseFunc
	onConnReceive OnConnReceiveFunc
	onConnError   OnConnError
}

func (s *tcpServer) Start() error {
	var err error
	if s.ln, err = net.Listen("tcp", ":"+strconv.Itoa(s.port)); err != nil {
		return err
	}
	go func() {
		var (
			err  error
			conn net.Conn
		)
		for {
			if s.stopped() {
				return
			}
			conn, err = s.ln.Accept()
			if err != nil {
				return
			}
			s.connMap.Store(conn.RemoteAddr().String(), conn)
			go s.serveConn(conn)
			if s.onConnAccept != nil {
				s.onConnAccept(conn)
			}
		}
	}()
	return nil
}

func (s *tcpServer) serveConn(conn net.Conn) {
	defer func() {
		_ = conn.Close()
		s.connMap.Delete(conn.RemoteAddr().String())
		if s.onConnClose != nil {
			s.onConnClose(conn)
		}
	}()
	var buf = make([]byte, 4096)
	var err error
	var n int
	for {
		n, err = conn.Read(buf)
		if errors.Is(err, io.EOF) {
			return
		}
		if err != nil {
			if s.onConnError != nil {
				s.onConnError(conn, err)
			}
			return
		}
		if s.onConnReceive != nil && n > 0 {
			if err = s.onConnReceive(conn, buf[:n]); err != nil {
				return
			}
		}
	}
}

func (s *tcpServer) stopped() bool {
	return s.ctx.Err() != nil
}

func (s *tcpServer) Stop() {
	s.stopOnce.Do(func() {
		s.stop()
		_ = s.ln.Close()
	})
}

func newTCPServer(port int,
	onConnAccept OnConnAcceptFunc,
	onConnClose OnConnCloseFunc,
	onConnReceive OnConnReceiveFunc,
	onConnError OnConnError) *tcpServer {
	var ctx, cancel = context.WithCancel(context.Background())
	return &tcpServer{
		port:          port,
		ctx:           ctx,
		stop:          cancel,
		onConnAccept:  onConnAccept,
		onConnClose:   onConnClose,
		onConnReceive: onConnReceive,
		onConnError:   onConnError,
	}
}

type eventListener struct {
	connects    uint64
	disconnects uint64
	reads       uint64
	writes      uint64

	blocked sync.Map
}

func (e *eventListener) Connected(conn net.Conn) {
	atomic.AddUint64(&e.connects, 1)
}

func (e *eventListener) Disconnected(conn net.Conn) {
	atomic.AddUint64(&e.disconnects, 1)
}

func (e *eventListener) Read(conn net.Conn, n int) {
	atomic.AddUint64(&e.reads, uint64(n))
}

func (e *eventListener) Wrote(conn net.Conn, n int) {
	atomic.AddUint64(&e.writes, uint64(n))
}

func (e *eventListener) AddrBlocked(addr string) {
	e.blocked.Store(addr, time.Now().UnixNano())
}

func (e *eventListener) AddrUnblocked(addr string) {
	e.blocked.Delete(addr)
}

func (e *eventListener) IsAddrBlocked(addr string) (blocked bool) {
	_, blocked = e.blocked.Load(addr)
	return
}

func newEventListener() *eventListener {
	return &eventListener{}
}

func TestConnectionManager(t *testing.T) {
	cases := map[string]func(*testing.T){
		"ConnectionReuse":            testConnectionReuse,
		"ConnectionEvict":            testConnectionEvict,
		"AddressBlacklist":           testAddressBlacklist,
		"ConnectionWithoutBlacklist": testConnectionWithoutBlacklist,
		"ConnectionBlackListSwitch":  testConnectionBlacklistSwitch,
	}
	for name, c := range cases {
		t.Run(name, c)
	}
}

func testConnectionReuse(t *testing.T) {
	var port = int(rand.Int31n(10000) + 1024)
	var (
		serverAddr          = "127.0.0.1:" + strconv.Itoa(port)
		serverConnAcceptCnt int64
		serverConnCloseCnt  int64
		connWG              sync.WaitGroup
	)
	var onConnAccept = func(conn net.Conn) {
		atomic.AddInt64(&serverConnAcceptCnt, 1)
		connWG.Add(1)
		return
	}
	var onConnClose = func(conn net.Conn) {
		atomic.AddInt64(&serverConnCloseCnt, 1)
		connWG.Done()
		return
	}
	var onConnReceive = func(conn net.Conn, buf []byte) error {
		return nil
	}
	var onConnError = func(conn net.Conn, err error) {
		return
	}
	var server = newTCPServer(port,
		onConnAccept,
		onConnClose,
		onConnReceive,
		onConnError)
	if err := server.Start(); err != nil {
		t.Fatalf("start tcp server failed: %v", err)
	}
	defer server.Stop()

	var conf = connman.NewDefaultConfig()
	var cm = connman.NewConnectionManager(conf)
	defer cm.Stop()

	// 验证链接复用性

	// 同一时刻只有一条连接使用
	for i := 0; i < 1024; i++ {
		conn, err := cm.GetConnectWithBlackList(serverAddr)
		if err != nil {
			t.Fatalf("dial failed: %v", err)
		}
		cm.PutConnect(conn, false)
	}

	// 验证服务端接受链接和关闭连接计数
	if atomic.LoadInt64(&serverConnAcceptCnt) != 1 {
		t.Fatalf("server-side conn accept count mismatch, expect 1, got %d", atomic.LoadInt64(&serverConnAcceptCnt))
	}
	if atomic.LoadInt64(&serverConnCloseCnt) != 0 {
		t.Fatalf("server-side conn close count mismatch, expect 0, got %d", atomic.LoadInt64(&serverConnCloseCnt))
	}

	// 同一时刻有两条链接使用
	for i := 0; i < 1024; i++ {
		conn1, err1 := cm.GetConnectWithBlackList(serverAddr)
		if err1 != nil {
			t.Fatalf("dial failed: %v", err1)
		}
		conn2, err2 := cm.GetConnectWithBlackList(serverAddr)
		if err2 != nil {
			t.Fatalf("dial failed: %v", err2)
		}
		cm.PutConnect(conn1, false)
		cm.PutConnect(conn2, false)
	}

	// 验证服务端接受链接和关闭连接计数
	if atomic.LoadInt64(&serverConnAcceptCnt) != 2 {
		t.Fatalf("server-side conn accept count mismatch, expect 1, got %d", atomic.LoadInt64(&serverConnAcceptCnt))
	}
	if atomic.LoadInt64(&serverConnCloseCnt) != 0 {
		t.Fatalf("server-side conn close count mismatch, expect 0, got %d", atomic.LoadInt64(&serverConnCloseCnt))
	}

	// 强行关闭链接
	for i := 0; i < 2; i++ {
		conn, err := cm.GetConnectWithBlackList(serverAddr)
		if err != nil {
			t.Fatalf("dial failed: %v", err)
		}
		_ = conn.Close()
	}

	connWG.Wait()

	// 验证服务端关闭连接计数
	if atomic.LoadInt64(&serverConnAcceptCnt) != 2 {
		t.Fatalf("server-side conn accept count mismatch, expect 1, got %d", atomic.LoadInt64(&serverConnAcceptCnt))
	}
	if atomic.LoadInt64(&serverConnCloseCnt) != 2 {
		t.Fatalf("server-side conn close mismatch, expect 1, got %d", atomic.LoadInt64(&serverConnCloseCnt))
	}
}

func testConnectionWithoutBlacklist(t *testing.T) {
	var port = int(rand.Int31n(10000) + 1024)
	var serverAddr = "2.2.2.2:" + strconv.Itoa(port)

	var eventLn = newEventListener()
	var conf = connman.NewDefaultConfig()
	conf.DailTimeout = time.Millisecond * 200
	conf.BlacklistTestInterval = time.Millisecond * 500
	conf.EventListener = eventLn
	var cm = connman.NewConnectionManager(conf)
	var err error
	if _, err = cm.GetConnect(serverAddr); err == nil {
		t.Errorf("dial should failed")
	}
	t.Logf("GetConnect from %v failed, err:%v", serverAddr, err)
	defer cm.Stop()
	// 检查目标地址是否不在黑名单中
	if eventLn.IsAddrBlocked(serverAddr) {
		t.Errorf("%v blocked", serverAddr)
	}

	// 使用普通方法重新连接该地址, 验证是否等待超时
	redialStart := time.Now()
	if _, err = cm.GetConnect(serverAddr); err == nil {
		t.Errorf("dial should failed")
	}
	t.Logf("GetConnect from %v failed, err:%v", serverAddr, err)

	timeout := time.Now().Sub(redialStart)
	if timeout < conf.DailTimeout {
		t.Errorf("GetConnect dial timeout:%v should be no less than conf.DailTimeout:%v ", timeout, conf.DailTimeout)
	}

	// 使用带黑名单的方法重新连接该地址, 验证是否等待超时
	redialStart = time.Now()
	if _, err = cm.GetConnectWithBlackList(serverAddr); err == nil {
		t.Errorf("dial should failed")
	}
	t.Logf("GetConnectWithBlackList from %v failed, err:%v", serverAddr, err)

	timeout = time.Now().Sub(redialStart)
	if timeout < conf.DailTimeout {
		t.Errorf("GetConnectWithBlackList dial timeout:%v should be no less than conf.DailTimeout:%v ", timeout, conf.DailTimeout)
	}
}

func testConnectionBlacklistSwitch(t *testing.T) {
	var port = int(rand.Int31n(10000) + 1024)
	var serverAddr = "2.2.2.2:" + strconv.Itoa(port)

	var eventLn = newEventListener()
	var conf = connman.NewDefaultConfig()
	conf.DailTimeout = time.Millisecond * 200
	conf.BlacklistTestInterval = time.Millisecond * 500
	conf.EventListener = eventLn
	var cm = connman.NewConnectionManager(conf)
	cm.DisableBlackList(true)

	var err error
	if _, err = cm.GetConnectWithBlackList(serverAddr); err == nil {
		t.Errorf("dial should failed")
	}
	t.Logf("GetConnect from %v failed, err:%v", serverAddr, err)
	defer cm.Stop()
	// 检查目标地址是否不在黑名单中
	if eventLn.IsAddrBlocked(serverAddr) {
		t.Errorf("%v blocked", serverAddr)
	}

	// 使用带黑名单的方法重新连接该地址, 验证是否等待超时
	redialStart := time.Now()
	if _, err = cm.GetConnectWithBlackList(serverAddr); err == nil {
		t.Errorf("dial should failed")
	}
	t.Logf("GetConnectWithBlackList from %v failed, err:%v", serverAddr, err)

	timeout := time.Now().Sub(redialStart)
	if timeout < conf.DailTimeout {
		t.Errorf("GetConnectWithBlackList dial timeout:%v should be no less than conf.DailTimeout:%v ", timeout, conf.DailTimeout)
	}
}

func testAddressBlacklist(t *testing.T) {
	var port = int(rand.Int31n(10000) + 1024)
	var serverAddr = "2.2.2.2:" + strconv.Itoa(port)

	var eventLn = newEventListener()
	var conf = connman.NewDefaultConfig()
	conf.DailTimeout = time.Millisecond * 200
	conf.BlacklistTestInterval = time.Millisecond * 500
	conf.EventListener = eventLn
	var cm = connman.NewConnectionManager(conf)
	var err error
	if _, err = cm.GetConnectWithBlackList(serverAddr); err == nil {
		t.Errorf("dial should failed")
	}
	defer cm.Stop()
	t.Logf("GetConnectWithBlackList from %v failed, err:%v", serverAddr, err)

	// 检查目标地址是否已在黑名单中
	if !eventLn.IsAddrBlocked(serverAddr) {
		t.Errorf("%v not blocked", serverAddr)
	}

	// 重新连接该地址, 验证是否立刻响应失败
	redialStart := time.Now()
	if _, err = cm.GetConnectWithBlackList(serverAddr); err == nil {
		t.Errorf("dial should failed")
	}
	t.Logf("GetConnectWithBlackList from %v failed, err:%v", serverAddr, err)

	if time.Now().Sub(redialStart) > time.Millisecond {
		t.Errorf("dial should failed immediately")
	}

}

func testConnectionEvict(t *testing.T) {
	var port = int(rand.Int31n(10000) + 1024)
	var (
		serverAddr          = "127.0.0.1:" + strconv.Itoa(port)
		serverConnAcceptCnt int64
		serverConnCloseCnt  int64
	)
	var onConnAccept = func(conn net.Conn) {
		atomic.AddInt64(&serverConnAcceptCnt, 1)
		return
	}
	var onConnClose = func(conn net.Conn) {
		atomic.AddInt64(&serverConnCloseCnt, 1)
		return
	}
	var onConnReceive = func(conn net.Conn, buf []byte) error {
		return nil
	}
	var onConnError = func(conn net.Conn, err error) {
		return
	}
	var server = newTCPServer(port,
		onConnAccept,
		onConnClose,
		onConnReceive,
		onConnError)
	if err := server.Start(); err != nil {
		t.Fatalf("start tcp server failed: %v", err)
	}
	defer server.Stop()

	var eventLn = newEventListener()
	var conf = connman.NewDefaultConfig()
	conf.EventListener = eventLn
	var cm = connman.NewConnectionManager(conf)
	defer cm.Stop()

	// 初始化一条连接
	var conn net.Conn
	var err error
	conn, err = cm.GetConnectWithBlackList(serverAddr)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	time.Sleep(time.Millisecond * 100)

	// 验证服务端接受链接和关闭连接计数
	if atomic.LoadInt64(&serverConnAcceptCnt) != 1 {
		t.Fatalf("server-side conn accept count mismatch, expect 1, got %d", atomic.LoadInt64(&serverConnAcceptCnt))
	}
	if atomic.LoadInt64(&serverConnCloseCnt) != 0 {
		t.Fatalf("server-side conn close count mismatch, expect 0, got %d", atomic.LoadInt64(&serverConnCloseCnt))
	}

	// 尝试从连接读数据，超时100毫秒，由于服务端没有任何响应逻辑，所以本次调用必定会收到超时错误
	if err = conn.SetReadDeadline(time.Now().Add(time.Millisecond * 100)); err != nil {
		t.Fatalf("setup read deadline failed: %v", err)
	}
	_, err = conn.Read(make([]byte, 1024))
	if err == nil {
		t.Fatalf("read should failed")
	}
	_ = conn.Close()

	time.Sleep(time.Millisecond * 100)

	// 对于出错的链接关闭时连接池应该自动清理
	if atomic.LoadInt64(&serverConnCloseCnt) != 1 {
		t.Fatalf("server-side conn close count mismatch, expect 1, got %d", atomic.LoadInt64(&serverConnCloseCnt))
	}

	if disconnects := atomic.LoadUint64(&eventLn.disconnects); disconnects != 1 {
		t.Fatalf("address evicts mismatch, expect 1, got %d", disconnects)
	}
}
