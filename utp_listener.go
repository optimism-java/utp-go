package utp

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/optimism-java/utp-go/libutp"
)

type AcceptReq struct {
	connCh    chan *Conn
	cid       *libutp.ConnId
	nodeId    enode.ID
	waitingId string
}

type PendingAcceptConn struct {
	key   string
	conn  *Conn
	timer *time.Timer
}

// Listener represents a listening µTP socket.
type Listener struct {
	utpSocket

	lock                 sync.Mutex
	ctx                  context.Context
	acceptChan           <-chan *Conn
	acceptReq            chan *AcceptReq
	acceptWithCidReq     chan *AcceptReq
	stopWaitingWithCidCh chan *AcceptReq
	stopWaitingCh        chan string
	pendingTimeout       chan string
	closed               chan struct{}
	closeOnce            sync.Once
}

// Listen creates a listening µTP socket on the local network address. It is
// analogous to net.Listen.
func Listen(network string, addr string) (net.Listener, error) {
	return ListenOptions(network, addr)
}

// ListenOptions creates a listening µTP socket on the local network address with
// the given options.
func ListenOptions(network, addr string, options ...ConnectOption) (net.Listener, error) {
	s := utpDialState{
		logger: noopLogger,
	}
	for _, opt := range options {
		opt(&s)
	}
	switch network {
	case "utp", "utp4", "utp6":
	default:
		return nil, fmt.Errorf("network %s not supported", network)
	}
	udpAddr, err := ResolveUTPAddr(network, addr)
	if err != nil {
		return nil, err
	}
	listener, err := listen(&s, network, udpAddr)
	if err != nil {
		return nil, err
	}
	if s.tlsConfig != nil {
		return tls.NewListener(listener, s.tlsConfig), nil
	}
	return listener, nil
}

// ListenUTP creates a listening µTP socket on the local network address. It is
// analogous to net.ListenUDP.
func ListenUTP(network string, localAddr *Addr) (*Listener, error) {
	return listen(&utpDialState{}, network, localAddr)
}

// ListenUTPOptions creates a listening µTP socket on the given local network
// address and with the given options.
func ListenUTPOptions(network string, localAddr *Addr, options ...ConnectOption) (*Listener, error) {
	s := utpDialState{
		logger: noopLogger,
	}
	for _, opt := range options {
		opt(&s)
	}
	return listen(&s, network, localAddr)
}

func listen(s *utpDialState, network string, localAddr *Addr) (*Listener, error) {
	manager, err := newSocketManager(s, network, (*net.UDPAddr)(localAddr), nil)
	if err != nil {
		return nil, err
	}
	udpLocalAddr := manager.LocalAddr().(*net.UDPAddr)
	utpListener := &Listener{
		utpSocket: utpSocket{
			localAddr: udpLocalAddr,
			manager:   manager,
		},
		ctx:                  context.Background(),
		acceptChan:           manager.acceptChan,
		acceptReq:            make(chan *AcceptReq, 10),
		acceptWithCidReq:     make(chan *AcceptReq, 10),
		stopWaitingCh:        make(chan string, 10),
		stopWaitingWithCidCh: make(chan *AcceptReq, 10),
		pendingTimeout:       make(chan string, 10),
		closed:               make(chan struct{}),
	}
	manager.start()
	go utpListener.listenerLoop()
	return utpListener, nil
}

// AcceptUTPContext accepts a new µTP connection on a listening socket.
func (l *Listener) AcceptUTPContext(ctx context.Context, id enode.ID, connId *libutp.ConnId) (c *Conn, err error) {
	req := &AcceptReq{
		connCh:    make(chan *Conn),
		cid:       connId,
		nodeId:    id,
		waitingId: strconv.Itoa(int(libutp.RandomUint32())),
	}
	defer func(acceptReq *AcceptReq) {
		close(req.connCh)
		if err == nil {
			return
		}
		if connId != nil {
			l.stopWaitingWithCidCh <- req
		} else {
			l.stopWaitingCh <- req.waitingId
		}
	}(req)
	if connId == nil {
		l.acceptReq <- req
	} else {
		l.acceptWithCidReq <- req
	}
	for {
		select {
		case c = <-req.connCh:
			return
		case <-ctx.Done():
			err = fmt.Errorf("accept timeout: id = %s, connId = %d", id.String(), connId.SendId())
			return
		case <-l.closed:
			err = net.ErrClosed
			return
		}
	}
}

// AcceptUTPWithConnId accepts a new µTP connection with special connection id on a listening socket.
func (l *Listener) AcceptUTPWithConnId(id enode.ID, connId *libutp.ConnId) (*Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()
	return l.AcceptUTPContext(ctx, id, connId)
}

// AcceptUTP accepts a new µTP connection on a listening socket.
func (l *Listener) AcceptUTP() (*Conn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()
	return l.AcceptUTPContext(ctx, enode.ID{}, nil)
}

// Accept accepts a new µTP connection on a listening socket.
func (l *Listener) Accept() (net.Conn, error) {
	return l.AcceptUTP()
}

// AcceptContext accepts a new µTP connection on a listening socket.
func (l *Listener) AcceptContext(ctx context.Context) (net.Conn, error) {
	return l.AcceptUTPContext(ctx, enode.ID{}, nil)
}

// Close closes a Listener.
func (l *Listener) Close() error {
	l.closeOnce.Do(func() {
		close(l.closed)
	})
	return l.utpSocket.Close()
}

// Addr returns the local address of a Listener.
func (l *Listener) Addr() net.Addr {
	return l.utpSocket.LocalAddr()
}

func (l *Listener) listenerLoop() {
	//incommingExpirations DelayedQueue[string]
	//awaitingExpirations  DelayedQueue[string]
	incomingConns := make(map[string]*PendingAcceptConn)
	awaiting := make(map[string]*AcceptReq)
	awaitingWithCid := make(map[string]*AcceptReq)

	incomingKey := func(incomingConn *Conn) string {
		return fmt.Sprintf("%s_%d_%d", incomingConn.baseConn.NodeId.String(), incomingConn.baseConn.ConnIDSend, incomingConn.baseConn.ConnIDRecv)
	}
	awaitingKey := func(req *AcceptReq) string {
		return fmt.Sprintf("%s_%d_%d", req.nodeId.String(), req.cid.SendId(), req.cid.RecvId())
	}

forLoop:
	for {
		select {
		case c, ok := <-l.acceptChan:
			if !ok {
				return
			}
			key := incomingKey(c)
			if req, ok := awaitingWithCid[key]; ok {
				req.connCh <- c
				delete(awaitingWithCid, key)
				continue
			}
			for waitId, req := range awaiting {
				req.connCh <- c
				delete(awaiting, waitId)
				continue forLoop
			}

			pendingConn := &PendingAcceptConn{
				key:  key,
				conn: c,
			}

			incomingConns[key] = pendingConn
			l.connAcceptTimeout(pendingConn)

		case req := <-l.acceptReq:
			if len(incomingConns) == 0 {
				awaiting[req.waitingId] = req
				continue
			}
			var key string
			for key = range incomingConns {
				break
			}
			pending := incomingConns[key]
			delete(incomingConns, key)
			req.connCh <- pending.conn
			pending.timer.Stop()

		case withCidReq := <-l.acceptWithCidReq:
			reqKey := awaitingKey(withCidReq)
			if pending, ok := incomingConns[reqKey]; ok {
				withCidReq.connCh <- pending.conn
				delete(incomingConns, reqKey)
				pending.timer.Stop()
				continue
			}
			awaitingWithCid[reqKey] = withCidReq
		case stopWait := <-l.stopWaitingWithCidCh:
			reqKey := awaitingKey(stopWait)
			delete(awaiting, reqKey)
		case waitId := <-l.stopWaitingCh:
			delete(awaiting, waitId)
		case key := <-l.pendingTimeout:
			pending, ok := incomingConns[key]
			delete(incomingConns, key)
			if ok {
				pending.timer.Stop()
				_ = pending.conn.Close()
			}

		case <-l.ctx.Done():
			return
		case <-l.closed:
			return
		}
	}
}

func (l *Listener) connAcceptTimeout(pending *PendingAcceptConn) {
	var (
		timer *time.Timer
		done  = make(chan struct{})
	)
	timer = time.AfterFunc(20*time.Second, func() {
		<-done
		select {
		case l.pendingTimeout <- pending.key:
		case <-l.ctx.Done():
		}
	})
	pending.timer = timer
	close(done)
}

var _ net.Listener = &Listener{}
