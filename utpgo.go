// Copyright (c) 2021 Storj Labs, Inc.
// Copyright (c) 2010 BitTorrent, Inc.
// See LICENSE for copying information.

package utp

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"runtime/pprof"
	"sync"
	"syscall"
	"time"

	"github.com/go-logr/logr"

	"storj.io/utp-go/buffers"
	"storj.io/utp-go/libutp"
)

// Buffer for data before it gets to µTP (there is another "send buffer" in
// the libutp code, but it is for managing flow control window sizes.
const (
	readBufferSize  = 200000
	writeBufferSize = 200000
)

var noopLogger = logr.DiscardLogger{}

type Addr net.UDPAddr

func (a *Addr) Network() string { return "utp" }
func (a *Addr) String() string  { return (*net.UDPAddr)(a).String() }

type Conn struct {
	utpSocket

	logger logr.Logger

	baseConn *libutp.Socket

	// set to true if the socket will close once the write buffer is empty
	willClose bool

	// closed when Close() is called
	closeChan chan struct{}

	// closed when baseConn has entered StateDestroying
	baseConnDestroyed chan struct{}

	// readBuffer tracks data that has been read on a particular Conn, but
	// not yet consumed by the application.
	readBuffer *buffers.SyncCircularBuffer

	// writeBuffer tracks data that needs to be sent on this Conn, which is
	// not yet ingested by µTP.
	writeBuffer *buffers.SyncCircularBuffer

	readDeadline  time.Time
	writeDeadline time.Time

	// Set to true while waiting for a connection to complete (got
	// state=StateConnect). The connectChan channel will be closed once this
	// is set.
	connecting  bool
	connectChan chan struct{}
}

type Listener struct {
	utpSocket

	acceptChan <-chan *Conn
}

// utpSocket is shared functionality between Conn and Listener.
type utpSocket struct {
	localAddr *net.UDPAddr

	// manager is shared by all sockets using the same local address
	// (for outgoing connections, only the one connection, but for incoming
	// connections, this includes all connections received by the associated
	// listening socket). It is reference-counted, and thus will only be
	// cleaned up entirely when the last related socket is closed.
	manager *socketManager

	// changes to encounteredError, manager, or other state variables in Conn
	// or Listener should all be protected with this lock. If it must be
	// acquired at the same time as manager.baseConnLock, the
	// manager.baseConnLock must be acquired first.
	stateLock sync.Mutex

	// Once set, all further Write/Read operations should fail with this error.
	encounteredError error
}

func Dial(network string, address string) (net.Conn, error) {
	switch network {
	case "utp", "utp4", "utp6":
		rAddr, err := ResolveUTPAddr(network, address)
		if err != nil {
			return nil, err
		}
		return DialUTP(network, nil, rAddr)
	}
	return net.Dial(network, address)
}

func DialUTP(network string, laddr, raddr *Addr) (*Conn, error) {
	return DialUTPOptions(network, laddr, raddr)
}

func DialUTPOptions(network string, laddr, raddr *Addr, options ...ListenOption) (*Conn, error) {
	var logger logr.Logger = &noopLogger
	for _, opt := range options {
		opt.apply(&logger)
	}
	logger = logger.WithValues("laddr", laddr, "raddr", raddr)
	manager, err := newSocketManager(logger, network, (*net.UDPAddr)(laddr), (*net.UDPAddr)(raddr))
	if err != nil {
		return nil, err
	}
	localAddr := manager.LocalAddr().(*net.UDPAddr)
	// in case local addr interface and/or port has been clarified
	logger = logger.WithValues("laddr", localAddr)

	utpConn := &Conn{
		utpSocket: utpSocket{
			localAddr: localAddr,
			manager:   manager,
		},
		logger:            logger.WithName("utp-conn").WithValues("dir", "out"),
		connecting:        true,
		connectChan:       make(chan struct{}),
		closeChan:         make(chan struct{}),
		baseConnDestroyed: make(chan struct{}),
		readBuffer:        buffers.NewSyncBuffer(readBufferSize),
		writeBuffer:       buffers.NewSyncBuffer(writeBufferSize),
	}
	logger.V(10).Info("creating outgoing socket", "raddr", raddr)
	// thread-safe here, because no other goroutines could have a handle to
	// this mx yet.
	utpConn.baseConn, err = manager.mx.Create(packetSendCallback, manager, (*net.UDPAddr)(raddr))
	if err != nil {
		return nil, err
	}
	utpConn.baseConn.SetCallbacks(&libutp.CallbackTable{
		OnRead:    onReadCallback,
		OnWrite:   onWriteCallback,
		GetRBSize: getRBSizeCallback,
		OnState:   onStateCallback,
		OnError:   onErrorCallback,
	}, utpConn)
	utpConn.baseConn.SetLogger(logger.WithName("utp-socket"))

	manager.start()

	func() {
		// now that the manager's goroutines have started, we do need
		// concurrency protection
		manager.baseConnLock.Lock()
		defer manager.baseConnLock.Unlock()
		logger.V(10).Info("initating libutp-level Connect()")
		utpConn.baseConn.Connect()
	}()

	// wait until connection is complete
	<-utpConn.connectChan

	utpConn.stateLock.Lock()
	err = utpConn.encounteredError
	utpConn.stateLock.Unlock()
	if err != nil {
		_ = utpConn.Close()
		return nil, err
	}
	return utpConn, nil
}

func Listen(network string, addr string) (net.Listener, error) {
	switch network {
	case "utp", "utp4", "utp6":
		udpAddr, err := ResolveUTPAddr(network, addr)
		if err != nil {
			return nil, err
		}
		return ListenUTP(network, udpAddr)
	}
	return net.Listen(network, addr)
}

func ListenUTP(network string, localAddr *Addr) (*Listener, error) {
	return ListenUTPOptions(network, localAddr)
}

func ListenUTPOptions(network string, localAddr *Addr, options ...ListenOption) (*Listener, error) {
	var logger logr.Logger = &noopLogger
	for _, opt := range options {
		opt.apply(&logger)
	}
	logger = logger.WithValues("laddr", localAddr)
	manager, err := newSocketManager(logger, network, (*net.UDPAddr)(localAddr), nil)
	if err != nil {
		return nil, err
	}
	udpLocalAddr := manager.LocalAddr().(*net.UDPAddr)
	utpListener := &Listener{
		utpSocket: utpSocket{
			localAddr: udpLocalAddr,
			manager:   manager,
		},
		acceptChan: manager.acceptChan,
	}
	manager.start()
	return utpListener, nil
}

type ListenOption interface {
	apply(l *logr.Logger)
}

type listenOptionLogger struct {
	logger logr.Logger
}

func (lo *listenOptionLogger) apply(l *logr.Logger) {
	*l = lo.logger
}

func WithLogger(logger logr.Logger) ListenOption {
	return &listenOptionLogger{logger: logger}
}

func (c *Conn) Close() error {
	// indicate our desire to close; once buffers are flushed, we can continue
	c.stateLock.Lock()
	if c.willClose {
		return errors.New("multiple calls to Close() not allowed")
	}
	c.willClose = true
	c.stateLock.Unlock()

	// wait for write buffer to be flushed
	c.writeBuffer.FlushAndClose()

	// if there are still any blocked reads, shut them down
	c.readBuffer.Close()

	// close baseConn
	err := func() error {
		// yes, even libutp.(*UTPSocket).Close() needs concurrency protection;
		// it may end up invoking callbacks
		c.manager.baseConnLock.Lock()
		defer c.manager.baseConnLock.Unlock()
		c.logger.V(10).Info("closing baseconn")
		return c.baseConn.Close()
	}()

	// wait for socket to enter StateDestroying
	<-c.baseConnDestroyed

	c.setEncounteredError(net.ErrClosed)
	socketCloseErr := c.utpSocket.Close()

	// even if err was already set, this one is likely to be more helpful/interesting.
	if socketCloseErr != nil {
		err = socketCloseErr
	}
	return err
}

func (c *Conn) SetLogger(logger logr.Logger) {
	c.baseConn.SetLogger(logger)
}

func (c *Conn) Read(buf []byte) (n int, err error) {
	return c.ReadContext(context.Background(), buf)
}

func (c *Conn) ReadContext(ctx context.Context, buf []byte) (n int, err error) {
	c.stateLock.Lock()
	encounteredErr := c.encounteredError
	deadline := c.readDeadline
	c.stateLock.Unlock()

	if !deadline.IsZero() {
		var cancel func()
		ctx, cancel = context.WithDeadline(ctx, deadline)
		defer cancel()
	}
	for {
		n, ok := c.readBuffer.TryConsume(buf)
		if ok {
			return n, nil
		}
		if encounteredErr != nil {
			return 0, encounteredErr
		}
		waitChan, cancelWait, err := c.readBuffer.WaitForBytesChan(1)
		if err != nil {
			return 0, err
		}
		select {
		case <-ctx.Done():
			cancelWait()
			err = ctx.Err()
			if errors.Is(err, context.DeadlineExceeded) {
				// transform deadline error to os.ErrDeadlineExceeded as per
				// net.Conn specification
				err = c.makeOpError("read", os.ErrDeadlineExceeded)
			}
			return 0, err
		case <-c.closeChan:
			cancelWait()
			return 0, c.makeOpError("read", net.ErrClosed)
		case <-waitChan:
		}
	}
}

func (c *Conn) Write(buf []byte) (n int, err error) {
	return c.WriteContext(context.Background(), buf)
}

func (c *Conn) WriteContext(ctx context.Context, buf []byte) (n int, err error) {
	c.stateLock.Lock()
	err = c.encounteredError
	willClose := c.willClose
	deadline := c.writeDeadline
	c.stateLock.Unlock()
	if err != nil {
		if err == io.EOF {
			// remote side closed connection cleanly, and µTP in/out streams
			// are not independently closeable. Doesn't make sense to return
			// an EOF from a Write method, so..
			err = c.makeOpError("write", syscall.ECONNRESET)
		}
		return 0, err
	}
	if willClose {
		// it may not be completely closed yet, but Close() has been called,
		// so we can't accept any more writes
		return 0, c.makeOpError("write", net.ErrClosed)
	}

	if !deadline.IsZero() {
		var cancel func()
		ctx, cancel = context.WithDeadline(ctx, deadline)
		defer cancel()
	}
	for {
		c.stateLock.Lock()
		willClose = c.willClose
		c.stateLock.Unlock()
		if willClose {
			return 0, c.makeOpError("write", net.ErrClosed)
		}

		if ok := c.writeBuffer.TryAppend(buf); ok {
			// make sure µTP knows about the new bytes. this might be a bit
			// confusing, but it doesn't matter if other writes occur between
			// the TryAppend() above and the acquisition of the baseConnLock
			// below. All that matters is that (a) there is at least one call
			// to baseConn.Write scheduled to be made after this point (without
			// undue blocking); (b) baseConnLock is held when that Write call
			// is made; and (c) the amount of data in the write buffer does not
			// decrease between the SpaceUsed() call and the start of the next
			// call to onWriteCallback.
			func() {
				c.manager.baseConnLock.Lock()
				defer c.manager.baseConnLock.Unlock()

				amount := c.writeBuffer.SpaceUsed()
				c.logger.V(10).Info("initiating write to libutp layer", "len", amount)
				c.baseConn.Write(amount)
			}()

			return len(buf), nil
		}

		waitChan, cancelWait, err := c.writeBuffer.WaitForSpaceChan(len(buf))
		if err != nil {
			return 0, err
		}

		// couldn't write the data yet; wait until we can, or until we hit the
		// timeout, or until the conn is closed.
		select {
		case <-ctx.Done():
			cancelWait()
			err = ctx.Err()
			if errors.Is(err, context.DeadlineExceeded) {
				// transform deadline error to os.ErrDeadlineExceeded as per
				// net.Conn specification
				err = c.makeOpError("write", os.ErrDeadlineExceeded)
			}
			return 0, err
		case <-c.closeChan:
			cancelWait()
			return 0, c.makeOpError("write", net.ErrClosed)
		case <-waitChan:
		}
	}
}

func (c *Conn) RemoteAddr() net.Addr {
	// GetPeerName is thread-safe
	return (*Addr)(c.baseConn.GetPeerName())
}

func (c *Conn) SetReadDeadline(t time.Time) error {
	c.stateLock.Lock()
	defer c.stateLock.Unlock()
	c.readDeadline = t
	return nil
}

func (c *Conn) SetWriteDeadline(t time.Time) error {
	c.stateLock.Lock()
	defer c.stateLock.Unlock()
	c.writeDeadline = t
	return nil
}

func (c *Conn) SetDeadline(t time.Time) error {
	c.stateLock.Lock()
	defer c.stateLock.Unlock()
	c.writeDeadline = t
	c.readDeadline = t
	return nil
}

func (c *Conn) makeOpError(op string, err error) error {
	opErr := c.utpSocket.makeOpError(op, err).(*net.OpError)
	opErr.Source = opErr.Addr
	opErr.Addr = c.RemoteAddr()
	return opErr
}

var _ net.Conn = &Conn{}

func (l *Listener) AcceptUTPContext(ctx context.Context) (*Conn, error) {
	select {
	case newConn, ok := <-l.acceptChan:
		if ok {
			return newConn, nil
		}
		err := l.encounteredError
		if err == nil {
			err = l.makeOpError("accept", net.ErrClosed)
		}
		return nil, err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (l *Listener) AcceptUTP() (*Conn, error) {
	return l.AcceptUTPContext(context.Background())
}

func (l *Listener) Accept() (net.Conn, error) {
	return l.AcceptUTP()
}

func (l *Listener) AcceptContext(ctx context.Context) (net.Conn, error) {
	return l.AcceptUTPContext(ctx)
}

func (l *Listener) Close() error {
	return l.utpSocket.Close()
}

func (l *Listener) Addr() net.Addr {
	return l.utpSocket.LocalAddr()
}

var _ net.Listener = &Listener{}

func (u *utpSocket) makeOpError(op string, err error) error {
	return &net.OpError{
		Op:     op,
		Net:    "utp",
		Source: nil,
		Addr:   u.LocalAddr(),
		Err:    err,
	}
}

func (u *utpSocket) Close() (err error) {
	u.stateLock.Lock()
	if u.manager != nil {
		err = u.manager.decrementReferences()
		u.manager = nil
	}
	u.stateLock.Unlock()
	return err
}

func (c *Conn) setEncounteredError(err error) {
	if err == nil {
		return
	}
	c.stateLock.Lock()
	defer c.stateLock.Unlock()

	// keep the first error if this is called multiple times
	if c.encounteredError == nil {
		c.encounteredError = err
	}
	if c.connecting {
		c.connecting = false
		close(c.connectChan)
	}
}

func (u *utpSocket) LocalAddr() net.Addr {
	return (*Addr)(u.localAddr)
}

type socketManager struct {
	mx        *libutp.SocketMultiplexer
	logger    logr.Logger
	udpSocket *net.UDPConn

	// this lock should be held when invoking any libutp functions or methods
	// that are not thread-safe or which themselves might invoke callbacks
	// (that is, nearly all libutp functions or methods). It can be assumed
	// that this lock is held in callbacks.
	baseConnLock sync.Mutex

	refCountLock sync.Mutex
	refCount     int

	// cancelManagement is a cancel function that should be called to close
	// down the socket management goroutines. The main managing goroutine
	// should clean up and return any close error on closeErr.
	cancelManagement func()
	// closeErr is a channel on which the managing goroutine will return any
	// errors from a close operation when all is complete.
	closeErr chan error

	// to be allocated with a buffer the size of the intended backlog. There
	// can be at most one utpSocket able to receive on this channel (one
	// Listener for any given UDP socket).
	acceptChan chan *Conn

	// just a way to accumulate errors in sending or receiving on the UDP
	// socket; this may cause future Write/Read method calls to return the
	// error in the future
	socketErrors     []error
	socketErrorsLock sync.Mutex

	pollInterval time.Duration
}

const (
	defaultUTPConnBacklogSize = 5
)

func newSocketManager(logger logr.Logger, network string, laddr, raddr *net.UDPAddr) (*socketManager, error) {
	switch network {
	case "utp", "utp4", "utp6":
	default:
		op := "dial"
		if raddr == nil {
			op = "listen"
		}
		return nil, &net.OpError{Op: op, Net: network, Source: laddr, Addr: raddr, Err: net.UnknownNetworkError(network)}
	}

	udpNetwork := "udp" + network[3:]
	// thread-safe here; don't need baseConnLock
	mx := libutp.NewSocketMultiplexer(logger.WithName("mx").WithValues("laddr", laddr.String()), nil)

	udpSocket, err := net.ListenUDP(udpNetwork, laddr)
	if err != nil {
		return nil, err
	}

	sm := &socketManager{
		mx:           mx,
		logger:       logger.WithName("manager").WithValues("laddr", udpSocket.LocalAddr()),
		udpSocket:    udpSocket,
		refCount:     1,
		closeErr:     make(chan error),
		acceptChan:   make(chan *Conn, defaultUTPConnBacklogSize),
		pollInterval: 5 * time.Millisecond,
	}
	return sm, nil
}

func (sm *socketManager) start() {
	ctx, cancel := context.WithCancel(context.Background())
	sm.cancelManagement = cancel

	managementLabels := pprof.Labels(
		"name", "socket-management", "udp-socket", sm.udpSocket.LocalAddr().String())
	receiverLabels := pprof.Labels(
		"name", "udp-receiver", "udp-socket", sm.udpSocket.LocalAddr().String())
	go func() {
		pprof.Do(ctx, managementLabels, sm.socketManagement)
	}()
	go func() {
		pprof.Do(ctx, receiverLabels, sm.udpMessageReceiver)
	}()
}

func (sm *socketManager) LocalAddr() net.Addr {
	return sm.udpSocket.LocalAddr()
}

func (sm *socketManager) socketManagement(ctx context.Context) {
	timer := time.NewTimer(sm.pollInterval)
	defer timer.Stop()
	for {
		timer.Reset(sm.pollInterval)
		select {
		case <-ctx.Done():
			// at this point, all attached Conn instances should be
			// closed already
			sm.internalClose()
			return
		case <-timer.C:
		}
		sm.checkTimeouts()
	}
}

func (sm *socketManager) processIncomingPacket(data []byte, destAddr *net.UDPAddr) {
	sm.baseConnLock.Lock()
	defer sm.baseConnLock.Unlock()
	sm.mx.IsIncomingUTP(gotIncomingConnectionCallback, packetSendCallback, sm, data, destAddr)
}

func (sm *socketManager) checkTimeouts() {
	sm.baseConnLock.Lock()
	defer sm.baseConnLock.Unlock()
	sm.mx.CheckTimeouts()
}

func (sm *socketManager) internalClose() {
	err := sm.udpSocket.Close()
	sm.mx = nil
	sm.closeErr <- err
	close(sm.closeErr)
	close(sm.acceptChan)
}

func (sm *socketManager) incrementReferences() {
	sm.refCountLock.Lock()
	sm.refCount++
	sm.refCountLock.Unlock()
}

func (sm *socketManager) decrementReferences() error {
	sm.refCountLock.Lock()
	defer sm.refCountLock.Unlock()
	sm.refCount--
	if sm.refCount == 0 {
		sm.logger.V(1).Info("closing socketManager")
		sm.cancelManagement()
		return <-sm.closeErr
	}
	if sm.refCount < 0 {
		return errors.New("socketManager closed too many times")
	}
	return nil
}

func (sm *socketManager) udpMessageReceiver(ctx context.Context) {
	// thread-safe; don't need baseConnLock
	maxSize := libutp.GetUDPMTU(sm.LocalAddr().(*net.UDPAddr))
	b := make([]byte, maxSize)
	for {
		n, addr, err := sm.udpSocket.ReadFromUDP(b)
		if err != nil {
			if ctx.Err() != nil {
				// we expect an error here; the socket has been closed; it's fine
				return
			}
			sm.registerSocketError(err)
			continue
		}
		sm.logger.V(10).Info("udp received bytes", "len", n, "raddr", addr)
		sm.processIncomingPacket(b[:n], addr)
	}
}

func (sm *socketManager) registerSocketError(err error) {
	sm.socketErrorsLock.Lock()
	defer sm.socketErrorsLock.Unlock()
	sm.logger.Error(err, "socket error")
	sm.socketErrors = append(sm.socketErrors, err)
}

func gotIncomingConnectionCallback(userdata interface{}, newBaseConn *libutp.Socket) {
	sm := userdata.(*socketManager)
	remoteAddr := sm.udpSocket.RemoteAddr()
	if remoteAddr != nil {
		// this is not a listening-mode socket! we'll reject this spurious packet
		_ = newBaseConn.Close()
		return
	}
	sm.incrementReferences()

	newUTPConn := &Conn{
		utpSocket: utpSocket{
			localAddr: sm.LocalAddr().(*net.UDPAddr),
			manager:   sm,
		},
		logger:            sm.logger.WithName("utp-socket").WithValues("dir", "in"),
		baseConn:          newBaseConn,
		closeChan:         make(chan struct{}),
		baseConnDestroyed: make(chan struct{}),
		readBuffer:        buffers.NewSyncBuffer(readBufferSize),
		writeBuffer:       buffers.NewSyncBuffer(writeBufferSize),
	}
	newBaseConn.SetCallbacks(&libutp.CallbackTable{
		OnRead:    onReadCallback,
		OnWrite:   onWriteCallback,
		GetRBSize: getRBSizeCallback,
		OnState:   onStateCallback,
		OnError:   onErrorCallback,
	}, newUTPConn)
	newUTPConn.baseConn.SetLogger(sm.logger.WithName("utp-socket").WithValues(
		"laddr", sm.LocalAddr().String(),
		"raddr", newUTPConn.RemoteAddr().String()))
	sm.logger.V(1).Info("accepted new connection", "raddr", newUTPConn.RemoteAddr())
	select {
	case sm.acceptChan <- newUTPConn:
		// it's the socketManager's problem now
	default:
		sm.logger.Info("dropping new connection because full backlog", "raddr", newUTPConn.RemoteAddr())
		// The accept backlog is full; drop this new connection. We can't call
		// (*Conn).Close() from here, because the baseConnLock is already held.
		// Fortunately, most of the steps done there aren't necessary here
		// because we have never exposed this instance to the user.
		_ = newUTPConn.baseConn.Close()
		// This step will decref the socketManager back to where it was before
		// this instance was created.
		_ = newUTPConn.manager.decrementReferences()
		newUTPConn.manager = nil
	}
}

func packetSendCallback(userdata interface{}, buf []byte, addr *net.UDPAddr) {
	sm := userdata.(*socketManager)
	sm.logger.V(10).Info("udp sending bytes", "len", len(buf), "raddr", addr.String())
	_, err := sm.udpSocket.WriteToUDP(buf, addr)
	if err != nil {
		sm.registerSocketError(err)
	}
}

func onReadCallback(userdata interface{}, buf []byte) {
	c := userdata.(*Conn)
	c.logger.V(10).Info("read callback from libutp layer providing bytes", "len", len(buf))
	if ok := c.readBuffer.TryAppend(buf); !ok {
		// I think this should not happen; the flow control mechanism should
		// keep us from getting more data than the (libutp-level) receive
		// buffer can hold.
		panic("receive buffer overflow")
	}
}

func onWriteCallback(userdata interface{}, buf []byte) {
	c := userdata.(*Conn)
	c.logger.V(10).Info("write callback from libutp layer consuming bytes", "len", len(buf))
	ok := c.writeBuffer.TryConsumeFull(buf)
	if !ok {
		// I think this should not happen; this callback should only be called
		// with data less than or equal to the number we pass in with
		// libutp.(*Socket).Write(). That gets passed in under the
		// baseConnLock, and this gets called under that same lock, so it also
		// shouldn't be possible for something to pull data from the write
		// buffer between that point and this point.
		panic("send buffer underflow")
	}
}

func getRBSizeCallback(userdata interface{}) int {
	c := userdata.(*Conn)
	return c.readBuffer.SpaceUsed()
}

// the baseConnLock should already be held when this callback is entered
func onStateCallback(userdata interface{}, state libutp.State) {
	c := userdata.(*Conn)
	c.logger.V(10).Info("onState callback from libutp layer", "state", state.String())
	switch state {
	case libutp.StateConnect, libutp.StateWritable:
		c.stateLock.Lock()
		if c.connecting {
			c.connecting = false
			close(c.connectChan)
		}
		if writeAmount := c.writeBuffer.SpaceUsed(); writeAmount > 0 {
			c.logger.V(10).Info("initiating write to libutp layer", "len", writeAmount)
			c.baseConn.Write(writeAmount)
		} else {
			c.logger.V(10).Info("nothing to write")
		}
		c.stateLock.Unlock()
	case libutp.StateEOF:
		c.setEncounteredError(io.EOF)
	case libutp.StateDestroying:
		close(c.baseConnDestroyed)
	}
}

// the baseConnLock should already be held when this callback is entered
func onErrorCallback(userdata interface{}, err error) {
	c := userdata.(*Conn)
	c.logger.Error(err, "onError callback from libutp layer")
	c.setEncounteredError(err)
}

func ResolveUTPAddr(network, address string) (*Addr, error) {
	switch network {
	case "utp", "utp4", "utp6":
		udpNetwork := "udp" + network[3:]
		udpAddr, err := net.ResolveUDPAddr(udpNetwork, address)
		if err != nil {
			return nil, err
		}
		return (*Addr)(udpAddr), nil
	}
	return nil, net.UnknownNetworkError(network)
}