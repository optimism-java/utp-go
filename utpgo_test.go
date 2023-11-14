// Copyright (c) 2021 Storj Labs, Inc.
// See LICENSE for copying information.

package utp_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha512"
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"io"
	"net"
	"runtime/pprof"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/optimism-java/utp-go"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
	"golang.org/x/sync/errgroup"
)

const (
	// use -10 for the most detail.
	logLevel = 0
	repeats  = 2
)

func TestUTPConnsInSerial(t *testing.T) {
	logger := zaptest.NewLogger(t, zaptest.Level(zapcore.Level(-10)))
	l := newTestServer(t, logger.Named("server"))

	group := newLabeledErrgroup(context.Background())
	var acceptCount atomic.Int32
	group.Go(func(ctx context.Context) error {
		for {
			newConn, err := l.AcceptUTPContext(ctx, 0)
			if err != nil {
				if errors.Is(err, net.ErrClosed) {
					return nil
				}
				return err
			}

			logger.Info("Accept succeeded count", zap.Any("count", acceptCount.Add(1)))
			//logger.Info("Accept succeeded", zap.Any("remote", newConn.RemoteAddr()))
			group.Go(func(ctx context.Context) error {
				return handleConn(ctx, newConn)
			}, "task", "handle", "remote", newConn.RemoteAddr().String())
		}
	}, "task", "accept")
	group.Go(func(ctx context.Context) error {
		for i := 0; i < repeats; i++ {
			index := i
			if err := makeConn(ctx, logger.With(zap.Any("i", index)), l.Addr()); err != nil {
				return err
			}
			logger.Info("connect succeeded count", zap.Any("count", index+1))
		}
		return l.Close()
	}, "task", "connect")
	err := group.Wait()
	require.NoError(t, err)
}

func TestUTPConnsInParallel(t *testing.T) {
	logger := zaptest.NewLogger(t, zaptest.Level(zapcore.Level(-10)))
	l := newTestServer(t, logger.Named("server"))

	group := newLabeledErrgroup(context.Background())
	group.Go(func(ctx context.Context) error {
		subgroup := newLabeledErrgroup(ctx)
		for i := 0; i < repeats; i++ {
			subgroup.Go(func(ctx context.Context) error {
				newConn, err := l.AcceptContext(ctx)
				if err != nil {
					if errors.Is(err, net.ErrClosed) {
						return nil
					}
					return err
				}
				logger.Info("Accept succeeded", zap.Any("remote", newConn.RemoteAddr()))
				return handleConn(ctx, newConn.(*utp.Conn))
			}, "task", "handle")
		}
		err := subgroup.Wait()
		closeErr := l.Close()
		if err == nil {
			err = closeErr
		}
		return err
	}, "task", "accept")
	group.Go(func(ctx context.Context) error {
		subgroup := newLabeledErrgroup(ctx)
		for i := 0; i < repeats; i++ {
			index := strconv.Itoa(i)
			subgroup.Go(func(ctx context.Context) error {
				return makeConn(ctx, logger.With(zap.Any("i", index)), l.Addr())
			}, "task", "connect", "i", index)
		}
		err := subgroup.Wait()
		closeErr := l.Close()
		if err == nil {
			err = closeErr
		}
		return err
	}, "task", "connect-spawner")
	err := group.Wait()
	require.NoError(t, err)
}

func TestDefaultConnIdGenerator_GenCid(t *testing.T) {
	gen := utp.NewConnIdGenerator()
	s := struct {
		id  uint32
		url string
	}{
		id:  1,
		url: "test_url/test_path",
	}
	cid_not_initiator := gen.GenCid(s, false)
	assert.Equal(t, cid_not_initiator.RecvId(), cid_not_initiator.SendId()+1)

	cid_initiator := gen.GenCid(s, true)
	assert.Equal(t, cid_initiator.RecvId(), cid_initiator.SendId()-1)
}

func newTestServer(tb testing.TB, logger *zap.Logger) *utp.Listener {
	lAddr, err := utp.ResolveUTPAddr("utp", "127.0.0.1:0")
	require.NoError(tb, err)
	server, err := utp.ListenUTPOptions("utp", lAddr, utp.WithLogger(logger))
	require.NoError(tb, err)
	logger.Info("now listening", zap.Any("laddr", server.Addr()))
	return server
}

type contextReader interface {
	ReadContext(ctx context.Context, buf []byte) (n int, err error)
}

func readContextFull(ctx context.Context, r contextReader, buf []byte) (n int, err error) {
	gotBytes := 0
	for gotBytes < len(buf) {
		n, err = r.ReadContext(ctx, buf[gotBytes:])
		gotBytes += n
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = nil
			}
			return gotBytes, err
		}
	}
	return len(buf), nil
}

const dataBlobSize = 2048

func handleConn(ctx context.Context, conn *utp.Conn) (err error) {
	defer func() {
		closeErr := conn.Close()
		if err == nil {
			err = closeErr
		}
	}()

	buf := make([]byte, dataBlobSize)
	_, err = readContextFull(ctx, conn, buf)
	if err != nil {
		_, _ = conn.WriteContext(ctx, []byte{0x1})
		return err
	}
	sig := make([]byte, sha512.Size)
	_, err = readContextFull(ctx, conn, sig)
	if err != nil {
		_, _ = conn.WriteContext(ctx, []byte{0x2})
		return err
	}
	fmt.Println("--------------------------start")
	fmt.Println(fmt.Sprintf("conn(%p) received \ndata: %x;\n hash: %x", conn, buf, sig))
	fmt.Println("--------------------------end")
	hashOfData := sha512.Sum512(buf)
	if bytes.Compare(hashOfData[:], sig) != 0 {
		_, _ = conn.WriteContext(ctx, []byte{0x3})
		return fmt.Errorf("hashes do not match: %x != %x", hashOfData, sig)
	}
	n, err := conn.WriteContext(ctx, []byte{0xcc})
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("bad response write n=%d", n)
	}
	return nil
}

func makeConn(ctx context.Context, logger *zap.Logger, addr net.Addr) (err error) {
	netConn, err := utp.DialUTPOptions("utp", nil, addr.(*utp.Addr), utp.WithContext(ctx), utp.WithLogger(logger.Named("cli")))
	logger.Info("dial to server")
	if err != nil {
		logger.Info("dialing error", zap.Any("err", err))
		return err
	}

	conn := netConn.(*utp.Conn)
	logger.Named("makeConn").With(
		zap.Any("local-addr", conn.LocalAddr()), zap.Any("remote-addr", addr))

	logger.Info("connection succeeded")
	defer func() {
		logger.Info("closing connection", zap.Any("err", err))
		if closeErr := conn.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}()

	// do the things
	data := make([]byte, dataBlobSize+sha512.Size)
	_, err = io.ReadFull(rand.Reader, data[:dataBlobSize])
	if err != nil {
		return err
	}
	hashOfData := sha512.Sum512(data[:dataBlobSize])
	copy(data[dataBlobSize:], hashOfData[:])
	fmt.Println("--------------------------start")
	fmt.Println(fmt.Sprintf("will send \ndata: %x;\nhash: %x", data[:dataBlobSize], hashOfData[:]))
	fmt.Println("--------------------------end")
	n, err := conn.WriteContext(ctx, data)
	if err != nil {
		return err
	}
	if n < len(data) {
		return fmt.Errorf("short write: %d < %d", n, len(data))
	}
	n, err = conn.ReadContext(ctx, data[:1])
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("short read: %d < %d", n, 1)
	}
	if int(data[0]) != 0xcc {
		return fmt.Errorf("got %x response from remote instead of cc", int(data[0]))
	}
	return nil
}

type labeledErrgroup struct {
	*errgroup.Group
	ctx context.Context
}

func newLabeledErrgroup(ctx context.Context) *labeledErrgroup {
	group, innerCtx := errgroup.WithContext(ctx)
	return &labeledErrgroup{Group: group, ctx: innerCtx}
}

func (e *labeledErrgroup) Go(f func(context.Context) error, labels ...string) {
	e.Group.Go(func() error {
		var err error
		pprof.Do(e.ctx, pprof.Labels(labels...), func(ctx context.Context) {
			err = f(ctx)
		})
		return err
	})
}
