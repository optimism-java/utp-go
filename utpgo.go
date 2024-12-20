// Copyright (c) 2021 Storj Labs, Inc.
// Copyright (c) 2010 BitTorrent, Inc.
// See LICENSE for copying information.

package utp

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"runtime/pprof"
	"sync"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/optimism-java/utp-go/buffers"
	"github.com/optimism-java/utp-go/libutp"

	"go.uber.org/zap"
)

// Buffer for data before it gets to µTP (there is another "send buffer" in
// the libutp code, but it is for managing flow control window sizes).
const (
	readBufferSize  = 4194304
	writeBufferSize = 4194304
	// Make the read buffer larger than advertised, so that surplus bytes can be
	// handled under certain conditions.
	receiveBufferMultiplier = 2
)

var noopLogger = zap.NewNop()

var ErrReceiveBufferOverflow = fmt.Errorf("receive buffer overflow")

// Addr represents a µTP address.
type Addr net.UDPAddr

// Network returns the network of the address ("utp").
func (a *Addr) Network() string { return "utp" }

// String returns the address formatted as a string.
func (a *Addr) String() string { return (*net.UDPAddr)(a).String() }

// Conn represents a µTP connection.
type Conn struct {
	utpSocket

	logger *zap.Logger

	baseConn *libutp.Socket

	// set to true if the socket will close once the write buffer is empty
	willClose bool

	// set to true once the libutp-layer Close has been called
	libutpClosed bool

	// set to true when the socket has been closed by the remote side (or the
	// conn has experienced a timeout or other fatal error)
	remoteIsDone bool

	// set to true if a read call is pending
	readPending bool

	// set to true if a write call is pending
	writePending bool

	// closed when Close() is called
	closeChan chan struct{}

	// closed when baseConn has entered StateDestroying
	baseConnDestroyed chan struct{}

	// readBuffer tracks data that has been read on a particular Conn, but
	// not yet consumed by the application.
	readBuffer *buffers.SyncCircularBuffer

	// writeBuffer tracks data that needs to be sent on this Conn, which
	// has not yet been collected by µTP.
	writeBuffer *buffers.SyncCircularBuffer

	readDeadline  time.Time
	writeDeadline time.Time

	// Set to true while waiting for a connection to complete (got
	// state=StateConnect). The connectChan channel will be closed once this
	// is set.
	connecting  bool
	connectChan chan struct{}
}

func NewConnIdGenerator() libutp.ConnIdGenerator {
	return libutp.NewConnIdGenerator()
}

// utpSocket is shared functionality between Conn and Listener.
type utpSocket struct {
	localAddr *net.UDPAddr

	// manager is shared by all sockets using the same local address
	// (for outgoing connections, only the one connection, but for incoming
	// connections, this includes all connections received by the associated
	// listening socket). It is reference-counted, and thus will only be
	// cleaned up entirely when the last related socket is closed.
	manager *SocketManager

	// changes to encounteredError, manager, or other state variables in Conn
	// or Listener should all be protected with this lock. If it must be
	// acquired at the same time as manager.baseConnLock, the
	// manager.baseConnLock must be acquired first.
	stateLock sync.Mutex

	// Once set, all further Write/Read operations should fail with this error.
	encounteredError error
}

// Dial attempts to make an outgoing µTP connection to the given address. It is
// analogous to net.Dial.
func Dial(network, address string) (net.Conn, error) {
	return DialOptions(network, address)
}

// DialContext attempts to make an outgoing µTP connection to the given address.
func DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	return DialOptions(network, address, WithContext(ctx))
}

// DialOptions attempts to make an outgoing µTP connection to the given address
// with the given options.
func DialOptions(network, address string, options ...ConnectOption) (net.Conn, error) {
	switch network {
	case "utp", "utp4", "utp6":
	default:
		return nil, fmt.Errorf("network %s not supported", network)
	}
	rAddr, err := ResolveUTPAddr(network, address)
	if err != nil {
		return nil, err
	}
	return DialUTPOptions(network, nil, rAddr, options...)
}

// DialUTP attempts to make an outgoing µTP connection with the given local
// and remote address endpoints. It is analogous to net.DialUDP.
func DialUTP(network string, localAddr, remoteAddr *Addr) (net.Conn, error) {
	return DialUTPOptions(network, localAddr, remoteAddr)
}

// DialUTPOptions attempts to make an outgoing µTP connection with the given
// local and remote address endpoints and the given options.
func DialUTPOptions(network string, localAddr, remoteAddr *Addr, options ...ConnectOption) (net.Conn, error) {
	s := utpDialState{
		logger:    noopLogger,
		ctx:       context.Background(),
		tlsConfig: nil,
	}
	for _, opt := range options {
		opt(&s)
	}
	conn, err := dial(s.ctx, &s, network, localAddr, remoteAddr)
	if err != nil {
		return nil, err
	}
	if s.tlsConfig != nil {
		return tls.Client(conn, s.tlsConfig), nil
	}
	return conn, nil
}

func dial(ctx context.Context, s *utpDialState, network string, localAddr, remoteAddr *Addr) (*Conn, error) {
	s.logger = s.logger.With(zap.Stringer("remote-addr", remoteAddr))
	manager, err := newSocketManager(s, network, (*net.UDPAddr)(localAddr), (*net.UDPAddr)(remoteAddr))
	if err != nil {
		return nil, err
	}
	localUDPAddr := manager.LocalAddr().(*net.UDPAddr)
	// different from managerLogger in case local addr interface and/or port
	// has been clarified
	var connLogger *zap.Logger
	if s.logger == nil {
		connLogger = s.logger.With(zap.Stringer("local-addr", localUDPAddr), zap.Stringer("remote-addr", remoteAddr), zap.String("dir", "out"))
	} else {
		connLogger = manager.logger.With(zap.Stringer("local-addr", localUDPAddr), zap.Stringer("remote-addr", remoteAddr), zap.String("dir", "out"))
	}

	utpConn := &Conn{
		utpSocket: utpSocket{
			localAddr: localUDPAddr,
			manager:   manager,
		},
		logger:            connLogger.Named("utp-conn"),
		connecting:        true,
		connectChan:       make(chan struct{}),
		closeChan:         make(chan struct{}),
		baseConnDestroyed: make(chan struct{}),
		readBuffer:        buffers.NewSyncBuffer(manager.readBufferSize * receiveBufferMultiplier),
		writeBuffer:       buffers.NewSyncBuffer(manager.writeBufferSize),
	}
	connLogger.Debug("creating outgoing socket")
	// thread-safe here, because no other goroutines could have a handle to
	// this mx yet.
	connId := s.connId
	if s.connId == 0 {
		connId = libutp.RandomUint16()
	}
	utpConn.baseConn, err = manager.mx.Create(packetSendCallback, manager, (*net.UDPAddr)(remoteAddr), libutp.SendCid(connId), enode.ID{})
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
	utpConn.baseConn.SetLogger(connLogger.Named("utp-socket"))
	utpConn.baseConn.SetSockOpt(syscall.SO_RCVBUF, manager.readBufferSize)

	manager.start()

	func() {
		// now that the manager's goroutines have started, we do need
		// concurrency protection
		manager.baseConnLock.Lock()
		defer manager.baseConnLock.Unlock()
		connLogger.Debug("initiating libutp-level Connect()")
		if manager.isShared {
			manager.incrementReferences()
		}
		utpConn.baseConn.Connect()
	}()

	select {
	case <-ctx.Done():
		_ = utpConn.Close()
		return nil, ctx.Err()
	case <-utpConn.connectChan:
	}

	// connection operation is complete, successful or not; record any error met
	utpConn.stateLock.Lock()
	err = utpConn.encounteredError
	utpConn.stateLock.Unlock()
	if err != nil {
		_ = utpConn.Close()
		return nil, utpConn.makeOpError("dial", err)
	}
	return utpConn, nil
}

type utpDialState struct {
	logger          *zap.Logger
	ctx             context.Context
	tlsConfig       *tls.Config
	blockQueueCount int
	connId          uint16
	sm              *SocketManager
	maxPacketSize   int
	pr              *PacketRouter
	readBufferSize  int
	writeBufferSize int
}

// ConnectOption is the interface which connection options should implement.
type ConnectOption func(state *utpDialState)

// WithLogger creates a connection option which specifies a logger to be
// attached to the connection. The logger will receive debugging messages
// about the socket.
func WithLogger(logger *zap.Logger) ConnectOption {
	return func(s *utpDialState) {
		s.logger = logger
	}
}

type optionContext struct {
	ctx context.Context
}

func (o *optionContext) apply(s *utpDialState) {
	s.ctx = o.ctx
}

// WithContext creates a connection option which specifies a context to be
// attached to the connection. If the context is closed, the dial operation
// will be canceled.
func WithContext(ctx context.Context) ConnectOption {
	return func(s *utpDialState) {
		s.ctx = ctx
	}
}

// WithTLS creates a connection option which specifies a TLS configuration
// structure to be attached to the connection. If specified, a TLS layer
// will be established on the connection before Dial returns.
func WithTLS(tlsConfig *tls.Config) ConnectOption {
	return func(s *utpDialState) {
		s.tlsConfig = tlsConfig
	}
}

// WithConnId Connect or Accept with certain connection id.If not specified, a random connection id will be generated.
func WithConnId(connId uint16) ConnectOption {
	return func(s *utpDialState) {
		s.connId = connId
	}
}

// WithSocketManager can share SocketManager with other conn
func WithSocketManager(sm *SocketManager) ConnectOption {
	return func(s *utpDialState) {
		s.sm = sm
	}
}

// WithMaxPacketSize Will set the maximum packet size for the connection which will send out size of packet.
func WithMaxPacketSize(size int) ConnectOption {
	return func(s *utpDialState) {
		s.maxPacketSize = size
	}
}

func WithPacketRouter(pr *PacketRouter) ConnectOption {
	return func(s *utpDialState) {
		s.pr = pr
	}
}

// WithBufferSize Will set the write buffer size and read buffer size for connection
func WithBufferSize(readSize int, writeSize int) ConnectOption {
	return func(s *utpDialState) {
		s.readBufferSize = readSize
		s.writeBufferSize = writeSize
	}
}

// Close closes a connection.
func (c *Conn) Close() error {
	// indicate our desire to close; once buffers are flushed, we can continue
	c.stateLock.Lock()
	if c.willClose {
		c.stateLock.Unlock()
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
		c.logger.Debug("closing baseConn")
		c.libutpClosed = true
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

// SetLogger sets the logger to be used by a connection. The logger will receive
// debugging information about the socket.
func (c *Conn) SetLogger(logger *zap.Logger) {
	c.baseConn.SetLogger(logger)
}

// Read reads from a Conn.
func (c *Conn) Read(buf []byte) (n int, err error) {
	return c.ReadContext(context.Background(), buf)
}

func (c *Conn) stateEnterRead() error {
	switch {
	case c.readPending:
		return buffers.ErrReaderAlreadyWaiting
	case c.willClose:
		return c.makeOpError("read", net.ErrClosed)
	case c.remoteIsDone && c.readBuffer.SpaceUsed() == 0:
		return c.makeOpError("read", c.encounteredError)
	}
	c.readPending = true
	return nil
}

// ReadContext reads from a Conn.
func (c *Conn) ReadContext(ctx context.Context, buf []byte) (n int, err error) {
	c.stateLock.Lock()
	encounteredErr := c.encounteredError
	deadline := c.readDeadline
	err = c.stateEnterRead()
	c.stateLock.Unlock()

	if err != nil {
		return 0, err
	}
	defer func() {
		c.stateLock.Lock()
		defer c.stateLock.Unlock()
		c.readPending = false
	}()
	if !deadline.IsZero() {
		var cancel func()
		ctx, cancel = context.WithDeadline(ctx, deadline)
		defer cancel()
	}

	for {
		var ok bool
		n, ok = c.readBuffer.TryConsume(buf)
		if ok {
			if n == 0 {
				return 0, io.EOF
			}
			c.manager.baseConnLock.Lock()
			c.baseConn.RBDrained()
			c.manager.baseConnLock.Unlock()
			return n, nil
		}
		if encounteredErr != nil {
			return 0, c.makeOpError("read", encounteredErr)
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

// Write writes to a Conn.
func (c *Conn) Write(buf []byte) (n int, err error) {
	return c.WriteContext(context.Background(), buf)
}

// WriteContext writes to a Conn.
func (c *Conn) WriteContext(ctx context.Context, buf []byte) (n int, err error) {
	const chunkSize = 64 * 1024 // 64KB chunks
	totalWritten := 0

	for i := 0; i < len(buf); i += chunkSize {
		end := i + chunkSize
		if end > len(buf) {
			end = len(buf)
		}

		chunk := buf[i:end]
		written, err := c.writeChunk(ctx, chunk)
		totalWritten += written
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = c.makeOpError("write", syscall.ECONNRESET)
			} else if errors.Is(err, net.ErrClosed) {
				err = c.makeOpError("write", net.ErrClosed)
			}
			return totalWritten, err
		}
	}
	return totalWritten, nil
}

func (c *Conn) writeChunk(ctx context.Context, chunk []byte) (n int, err error) {
	c.stateLock.Lock()
	if c.writePending {
		c.stateLock.Unlock()
		return 0, buffers.ErrWriterAlreadyWaiting
	}
	c.writePending = true
	deadline := c.writeDeadline
	c.stateLock.Unlock()

	defer func() {
		c.stateLock.Lock()
		defer c.stateLock.Unlock()
		c.writePending = false
	}()

	if !deadline.IsZero() {
		var cancel func()
		ctx, cancel = context.WithDeadline(ctx, deadline)
		defer cancel()
	}

	for {
		c.stateLock.Lock()
		willClose := c.willClose
		remoteIsDone := c.remoteIsDone
		encounteredError := c.encounteredError
		c.stateLock.Unlock()

		if willClose {
			return 0, c.makeOpError("write", net.ErrClosed)
		}
		if remoteIsDone {
			return 0, c.makeOpError("write", encounteredError)
		}

		if ok := c.writeBuffer.TryAppend(chunk); ok {
			func() {
				c.manager.baseConnLock.Lock()
				defer c.manager.baseConnLock.Unlock()

				amount := c.writeBuffer.SpaceUsed()
				c.logger.Debug("informing libutp layer of data for writing", zap.Int("len", amount))
				c.baseConn.Write(amount)
			}()

			return len(chunk), nil
		}

		waitChan, cancelWait, err := c.writeBuffer.WaitForSpaceChan(len(chunk))
		if err != nil {
			if errors.Is(err, buffers.ErrIsClosed) {
				err = c.makeOpError("write", c.encounteredError)
			}
			return 0, err
		}

		select {
		case <-ctx.Done():
			cancelWait()
			err = ctx.Err()
			if errors.Is(err, context.DeadlineExceeded) {
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

// RemoteAddr returns the address of the connection peer.
func (c *Conn) RemoteAddr() net.Addr {
	// GetPeerName is thread-safe
	return (*Addr)(c.baseConn.GetPeerName())
}

// SetReadDeadline sets a read deadline for future read operations.
func (c *Conn) SetReadDeadline(t time.Time) error {
	c.stateLock.Lock()
	defer c.stateLock.Unlock()
	c.readDeadline = t
	return nil
}

// SetWriteDeadline sets a write deadline for future write operations.
func (c *Conn) SetWriteDeadline(t time.Time) error {
	c.stateLock.Lock()
	defer c.stateLock.Unlock()
	c.writeDeadline = t
	return nil
}

// SetDeadline sets a deadline for future read and write operations.
func (c *Conn) SetDeadline(t time.Time) error {
	c.stateLock.Lock()
	defer c.stateLock.Unlock()
	c.writeDeadline = t
	c.readDeadline = t
	return nil
}

func (c *Conn) makeOpError(op string, err error) error {
	opErr := c.utpSocket.makeOpError(op, err).(*net.OpError) //nolint: errorlint
	opErr.Source = opErr.Addr
	opErr.Addr = c.RemoteAddr()
	return opErr
}

var _ net.Conn = &Conn{}

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
	defer u.stateLock.Unlock()
	if u.manager != nil {
		err = u.manager.decrementReferences()
		u.manager = nil
	}
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

type MessageWriter func(buf []byte, id enode.ID, addr *net.UDPAddr) (int, error)

type PacketRouter struct {
	udpSocket   *net.UDPConn
	sm          *SocketManager
	mw          MessageWriter
	once        sync.Once
	packetCache chan *packet
}

type NodeInfo struct {
	Id   enode.ID
	Addr *net.UDPAddr
}

type packet struct {
	buf  []byte
	node *NodeInfo
}

func NewPacketRouter(mw MessageWriter) *PacketRouter {
	return &PacketRouter{
		mw:          mw,
		packetCache: make(chan *packet, 100),
	}
}

func (pr *PacketRouter) WriteMsg(buf []byte, id enode.ID, addr *net.UDPAddr) (int, error) {
	return pr.mw(buf, id, addr)
}

func (pr *PacketRouter) ReceiveMessage(buf []byte, nodeInfo *NodeInfo) {
	pr.once.Do(func() {
		go func() {
			for {
				if packPt, ok := <-pr.packetCache; ok {
					pr.sm.processIncomingPacketWithNode(packPt.buf, packPt.node)
				} else {
					pr.sm.logger.Info("Packet router has been stopped")
					return
				}
			}
		}()
	})
	pr.packetCache <- &packet{buf: buf, node: nodeInfo}
}

type SocketManager struct {
	mx        *libutp.SocketMultiplexer
	logger    *zap.Logger
	pr        *PacketRouter
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

	readBufferSize  int
	writeBufferSize int

	pollInterval time.Duration

	startOnce  sync.Once
	localAddr  *net.UDPAddr
	remoteAddr *net.UDPAddr
	// isShared flag if this SocketManager is share with other. When using WithSocketManager option, it will be set true.
	isShared bool
}

const (
	defaultUTPConnBacklogSize = 100
)

func NewSocketManagerWithOptions(network string, localAddr *net.UDPAddr, options ...ConnectOption) (*SocketManager, error) {
	s := utpDialState{
		logger: noopLogger,
	}
	for _, opt := range options {
		opt(&s)
	}
	return newSocketManager(&s, network, localAddr, nil)
}

func newSocketManager(s *utpDialState, network string, localAddr, remoteAddr *net.UDPAddr) (*SocketManager, error) {
	if s.sm != nil {
		s.sm.isShared = true
		return s.sm, nil
	}
	if s.writeBufferSize < 0 || s.readBufferSize < 0 {
		return nil, errors.New("Buffer size is negative")
	}
	switch network {
	case "utp", "utp4", "utp6":
	default:
		op := "dial"
		if remoteAddr == nil {
			op = "listen"
		}
		return nil, &net.OpError{Op: op, Net: network, Source: localAddr, Addr: remoteAddr, Err: net.UnknownNetworkError(network)}
	}

	if s.pr == nil {
		udpNetwork := "udp" + network[3:]
		udpSocket, err := net.ListenUDP(udpNetwork, localAddr)
		if err != nil {
			return nil, err
		}
		s.pr = &PacketRouter{
			udpSocket: udpSocket,
			mw: func(buf []byte, id enode.ID, addr *net.UDPAddr) (int, error) {
				return udpSocket.WriteToUDP(buf, addr)
			},
		}
	}
	var localAddrStr fmt.Stringer
	mxLogger := s.logger.Named("mx")
	smLogger := s.logger.Named("manager")
	if s.pr.udpSocket != nil {
		localAddrStr = s.pr.udpSocket.LocalAddr()
		mxLogger.With(zap.Stringer("local-addr", localAddrStr))
		smLogger.With(zap.Stringer("local-addr", localAddrStr))
	}

	// thread-safe here; don't need baseConnLock
	mx := libutp.NewSocketMultiplexer(mxLogger, nil, s.maxPacketSize)

	sm := &SocketManager{
		mx:           mx,
		logger:       smLogger,
		pr:           s.pr,
		refCount:     1,
		closeErr:     make(chan error),
		acceptChan:   make(chan *Conn, defaultUTPConnBacklogSize),
		pollInterval: 5 * time.Millisecond,
		localAddr:    localAddr,
		remoteAddr:   remoteAddr,
		isShared:     false,
	}

	if s.readBufferSize == 0 {
		sm.readBufferSize = readBufferSize
	}
	if s.writeBufferSize == 0 {
		sm.writeBufferSize = writeBufferSize
	}

	sm.pr.sm = sm
	return sm, nil
}

func (sm *SocketManager) start() {
	sm.startOnce.Do(func() {
		ctx, cancel := context.WithCancel(context.Background())
		sm.cancelManagement = cancel

		managementLabels := pprof.Labels(
			"name", "socket-management", "udp-socket", sm.LocalAddr().String())
		receiverLabels := pprof.Labels(
			"name", "udp-receiver", "udp-socket", sm.LocalAddr().String())
		go func() {
			pprof.Do(ctx, managementLabels, sm.socketManagement)
		}()
		go func() {
			pprof.Do(ctx, receiverLabels, sm.udpMessageReceiver)
		}()
	})

}

func (sm *SocketManager) LocalAddr() net.Addr {
	if sm.pr.udpSocket != nil {
		return sm.pr.udpSocket.LocalAddr()
	} else {
		return sm.localAddr
	}
}

func (sm *SocketManager) RemoteAddr() net.Addr {
	if sm.pr.udpSocket != nil {
		return sm.pr.udpSocket.RemoteAddr()
	} else {
		return sm.remoteAddr
	}
}
func (sm *SocketManager) socketManagement(ctx context.Context) {
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

func (sm *SocketManager) processIncomingPacket(data []byte, destAddr *net.UDPAddr) {
	sm.baseConnLock.Lock()
	defer sm.baseConnLock.Unlock()
	sm.mx.IsIncomingUTP(gotIncomingConnectionCallback, packetSendCallback, sm, data, destAddr, enode.ID{})
}

func (sm *SocketManager) processIncomingPacketWithNode(data []byte, node *NodeInfo) {
	sm.baseConnLock.Lock()
	defer sm.baseConnLock.Unlock()
	sm.mx.IsIncomingUTP(gotIncomingConnectionCallback, packetSendCallback, sm, data, node.Addr, node.Id)
}

func (sm *SocketManager) checkTimeouts() {
	sm.baseConnLock.Lock()
	defer sm.baseConnLock.Unlock()
	sm.mx.CheckTimeouts()
}

func (sm *SocketManager) internalClose() {
	var err error
	if sm.pr.udpSocket != nil {
		err = sm.pr.udpSocket.Close()
	}
	sm.mx = nil
	sm.closeErr <- err
	close(sm.closeErr)
	close(sm.acceptChan)
	if sm.pr != nil && sm.pr.packetCache != nil {
		close(sm.pr.packetCache)
	}
}

func (sm *SocketManager) incrementReferences() {
	sm.refCountLock.Lock()
	sm.refCount++
	sm.refCountLock.Unlock()
}

func (sm *SocketManager) decrementReferences() error {
	sm.refCountLock.Lock()
	defer sm.refCountLock.Unlock()
	sm.refCount--
	if sm.refCount == 0 {
		sm.logger.Info("closing SocketManager")
		sm.cancelManagement()
		return <-sm.closeErr
	}
	if sm.refCount < 0 {
		return errors.New("SocketManager closed too many times")
	}
	return nil
}

func (sm *SocketManager) udpMessageReceiver(ctx context.Context) {
	if sm.pr.udpSocket == nil {
		return
	}
	// thread-safe; don't need baseConnLock for GetUDPMTU
	bufSize := libutp.GetUDPMTU(sm.LocalAddr().(*net.UDPAddr))
	// It turns out GetUDPMTU is frequently wrong, and when it gives us a lower
	// number than the real MTU, and the other side is sending bigger packets,
	// then we end up not being able to read the full packets. Start with a
	// receive buffer twice as big as we thought we might need, and increase it
	// further from there if needed.
	bufSize *= 2
	sm.logger.Info("udp message receiver started", zap.Uint16("receive-buf-size", bufSize))
	b := make([]byte, bufSize)
	for {
		n, _, flags, addr, err := sm.pr.udpSocket.ReadMsgUDP(b, nil)
		if err != nil {
			if ctx.Err() != nil {
				// we expect an error here; the socket has been closed; it's fine
				return
			}
			sm.registerSocketError(err)
			continue
		}
		if flags&msg_trunc != 0 {
			// we didn't get the whole packet. don't pass it on to µTP; it
			// won't recognize the truncation and will pretend like that's
			// all the data there is. let the packet loss detection stuff
			// do its part instead.
			continue
		}
		sm.logger.Debug("udp received bytes", zap.Int("len", n))
		sm.processIncomingPacket(b[:n], addr)
	}
}

func (sm *SocketManager) registerSocketError(err error) {
	sm.socketErrorsLock.Lock()
	defer sm.socketErrorsLock.Unlock()
	sm.logger.Error("socket error", zap.Error(err))
	sm.socketErrors = append(sm.socketErrors, err)
}

func gotIncomingConnectionCallback(userdata interface{}, newBaseConn *libutp.Socket) {
	sm := userdata.(*SocketManager)
	remoteAddr := sm.RemoteAddr()
	if remoteAddr != nil && !sm.isShared {
		// this is not a listening-mode socket! we'll reject this spurious packet
		_ = newBaseConn.Close()
		return
	}
	sm.incrementReferences()

	connLogger := sm.logger.Named("utp-socket").With(zap.String("dir", "in"), zap.Stringer("remote-addr", newBaseConn.GetPeerName()))
	newUTPConn := &Conn{
		utpSocket: utpSocket{
			localAddr: sm.LocalAddr().(*net.UDPAddr),
			manager:   sm,
		},
		logger:            connLogger,
		baseConn:          newBaseConn,
		closeChan:         make(chan struct{}),
		baseConnDestroyed: make(chan struct{}),
		readBuffer:        buffers.NewSyncBuffer(sm.readBufferSize * receiveBufferMultiplier),
		writeBuffer:       buffers.NewSyncBuffer(sm.writeBufferSize),
	}
	newBaseConn.SetCallbacks(&libutp.CallbackTable{
		OnRead:    onReadCallback,
		OnWrite:   onWriteCallback,
		GetRBSize: getRBSizeCallback,
		OnState:   onStateCallback,
		OnError:   onErrorCallback,
	}, newUTPConn)
	sm.logger.Info("accepted new connection", zap.Stringer("remote-addr", newUTPConn.RemoteAddr()), zap.Uint16("sendId", newUTPConn.baseConn.ConnIDSend), zap.Uint16("recvId", newUTPConn.baseConn.ConnIDRecv))
	newUTPConn.baseConn.SetSockOpt(syscall.SO_RCVBUF, sm.readBufferSize)
	select {
	case sm.acceptChan <- newUTPConn:
		// it's the SocketManager's problem now
	default:
		sm.logger.Info("dropping new connection because full backlog", zap.Stringer("remote-addr", newUTPConn.RemoteAddr()))
		// The accept backlog is full; drop this new connection. We can't call
		// (*Conn).Close() from here, because the baseConnLock is already held.
		// Fortunately, most of the steps done there aren't necessary here
		// because we have never exposed this instance to the user.
		_ = newUTPConn.baseConn.Close()
		// This step will decref the SocketManager back to where it was before
		// this instance was created.
		_ = newUTPConn.manager.decrementReferences()
		newUTPConn.manager = nil
	}
}

func packetSendCallback(userdata interface{}, buf []byte, id enode.ID, addr *net.UDPAddr) {
	sm := userdata.(*SocketManager)
	sm.logger.Debug("udp sending bytes", zap.Int("len", len(buf)))
	_, err := sm.pr.WriteMsg(buf, id, addr)
	if err != nil {
		sm.registerSocketError(err)
	}
}

func onReadCallback(userdata interface{}, buf []byte) {
	c := userdata.(*Conn)
	c.stateLock.Lock()
	c.stateDebugLogLocked("entering onReadCallback", "got-bytes", len(buf))
	isClosing := c.willClose
	c.stateLock.Unlock()

	if isClosing {
		// the local side has closed the connection; they don't want any additional data
		return
	}

	if ok := c.readBuffer.TryAppend(buf); !ok {
		// We've received more data than the receive buffer can hold, even with
		// receiveBufferMultiplier. We could keep on scaling up the receive
		// buffer forever, or we can give up on the connection. Since this is
		// expected to be uncommon, we'll go with dropping the connection for
		// now.
		used := c.readBuffer.SpaceUsed()
		avail := c.readBuffer.SpaceAvailable()
		err := ErrReceiveBufferOverflow
		c.logger.Error("receive buffer overflow", zap.Int("buffer-size", used+avail), zap.Int("buffer-holds", used), zap.Int("new-data", len(buf)))
		c.setEncounteredError(err)

		// clear out write buffer; we won't be able to send it now. If a call
		// to Close() is already waiting, we don't need to make it wait any
		// longer
		c.writeBuffer.Close()
		// this will allow any pending reads to complete (as short reads)
		c.readBuffer.CloseForWrites()
	}
	c.stateDebugLog("finishing onReadCallback")
}

func onWriteCallback(userdata interface{}, buf []byte) {
	c := userdata.(*Conn)
	c.stateLock.Lock()
	defer c.stateLock.Unlock()

	c.stateDebugLogLocked("entering onWriteCallback", "accepting-bytes", len(buf))
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
	c.stateDebugLogLocked("finishing onWriteCallback")
}

func getRBSizeCallback(userdata interface{}) int {
	c := userdata.(*Conn)
	return c.readBuffer.SpaceUsed()
}

func (c *Conn) onConnectOrWritable(state libutp.State) {
	c.stateLock.Lock()
	c.stateDebugLogLocked("entering onConnectOrWritable", "libutp-state", state)
	if c.connecting {
		c.connecting = false
		close(c.connectChan)
	}
	c.stateLock.Unlock()

	if writeAmount := c.writeBuffer.SpaceUsed(); writeAmount > 0 {
		c.logger.Debug("initiating write to libutp layer", zap.Int("len", writeAmount))
		c.baseConn.Write(writeAmount)
	} else {
		c.logger.Debug("nothing to write")
	}

	c.stateDebugLog("finishing onConnectOrWritable")
}

func (c *Conn) onConnectionFailure(err error) {
	c.stateDebugLog("entering onConnectionFailure", "err-text", err.Error())

	// mark EOF as encountered error, so that it gets returned from
	// subsequent Read calls
	c.setEncounteredError(err)
	// clear out write buffer; we won't be able to send it now. If a call
	// to Close() is already waiting, we don't need to make it wait any
	// longer
	c.writeBuffer.Close()

	// this will allow any pending reads to complete (as short reads)
	c.readBuffer.CloseForWrites()

	c.stateDebugLog("finishing onConnectionFailure")
}

// the baseConnLock should already be held when this callback is entered.
func onStateCallback(userdata interface{}, state libutp.State) {
	c := userdata.(*Conn)
	switch state {
	case libutp.StateConnect, libutp.StateWritable:
		c.onConnectOrWritable(state)
	case libutp.StateEOF:
		c.onConnectionFailure(io.EOF)
	case libutp.StateDestroying:
		close(c.baseConnDestroyed)
	}
}

// This could be ECONNRESET, ECONNREFUSED, or ETIMEDOUT.
//
// the baseConnLock should already be held when this callback is entered.
func onErrorCallback(userdata interface{}, err error) {
	c := userdata.(*Conn)
	c.logger.Error("onError callback from libutp layer", zap.Error(err))

	// we have to treat this like a total connection failure
	c.onConnectionFailure(err)

	// and we have to cover a corner case where this error was encountered
	// _during_ the libutp Close() call- in this case, libutp would sit
	// forever and never get to StateDestroying, so we have to prod it again.
	if c.libutpClosed {
		if err := c.baseConn.Close(); err != nil {
			c.logger.Error("error from libutp layer Close()", zap.Error(err))
		}
	}
}

// ResolveUTPAddr resolves a µTP address. It is analogous to net.ResolveUDPAddr.
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
