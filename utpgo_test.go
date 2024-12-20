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
	"io"
	"net"
	"runtime/pprof"
	"strconv"
	"testing"

	"github.com/ethereum/go-ethereum/p2p/enode"
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
	repeats  = 20
)

func TestUTPConnsInSerial(t *testing.T) {
	logger := zaptest.NewLogger(t, zaptest.Level(zapcore.Level(logLevel)))
	l := newTestServer(t, logger.Named("server"))

	group := newLabeledErrgroup(context.Background())

	group.Go(func(ctx context.Context) error {
		for {
			newConn, err := l.AcceptUTPContext(ctx, enode.ID{}, nil)
			if err != nil {
				if errors.Is(err, net.ErrClosed) {
					return nil
				}
				return err
			}
			logger.Info("Accept succeeded", zap.Any("remote", newConn.RemoteAddr()))
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
		}
		return l.Close()
	}, "task", "connect")

	err := group.Wait()
	require.NoError(t, err)
}

func TestUTPConnsInParallel(t *testing.T) {
	logger := zaptest.NewLogger(t, zaptest.Level(zapcore.Level(0)))
	l := newTestServer(t, logger.Named("server"))

	group := newLabeledErrgroup(context.Background())
	group.Go(func(ctx context.Context) error {
		subgroup := newLabeledErrgroup(ctx)
		for i := 0; i < repeats; i++ {
			subgroup.Go(func(ctx context.Context) error {
				newConn, err := l.AcceptUTPContext(ctx, enode.ID{}, nil)
				if err != nil {
					if errors.Is(err, net.ErrClosed) {
						return nil
					}
					return err
				}
				logger.Info("Accept succeeded", zap.Any("remote", newConn.RemoteAddr()))
				return handleConn(ctx, newConn)
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
			index := i
			subgroup.Go(func(ctx context.Context) error {
				return makeConn(ctx, logger.Named(strconv.Itoa(index)), l.Addr())
			}, "task", "connect", "i", strconv.Itoa(index))
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

func newTestServer(tb testing.TB, logger *zap.Logger) *utp.Listener {
	lAddr, err := utp.ResolveUTPAddr("utp", "127.0.0.1:0")
	require.NoError(tb, err)
	server, err := utp.ListenUTPOptions("utp", lAddr, utp.WithLogger(logger), utp.WithMaxPacketSize(980))
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

const dataBlobSize = 10000000

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
	hashOfData := sha512.Sum512(buf)
	if bytes.Compare(hashOfData[:], sig) != 0 {
		_, _ = conn.WriteContext(ctx, []byte{0x3})
		return fmt.Errorf("hashes do not match: %x != %x", hashOfData, sig)
	}
	n, err := conn.WriteContext(ctx, hashOfData[:])
	if err != nil {
		return err
	}
	if n != 64 {
		return fmt.Errorf("bad response write n=%d", n)
	}
	return nil
}

func makeConn(ctx context.Context, logger *zap.Logger, addr net.Addr) (err error) {
	netConn, err := utp.DialUTPOptions("utp", nil, addr.(*utp.Addr), utp.WithLogger(logger), utp.WithMaxPacketSize(980))
	if err != nil {
		return err
	}

	conn := netConn.(*utp.Conn)
	logger = logger.Named("makeConn").With(zap.Any("local-addr", conn.LocalAddr()), zap.Any("remote-addr", addr))
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
	logger.Info("writing bytes", zap.Any("len", len(data)))
	n, err := conn.WriteContext(ctx, data)
	if err != nil {
		return err
	}
	if n < len(data) {
		return fmt.Errorf("short write: %d < %d", n, len(data))
	}
	returnData := make([]byte, 68)
	n, err = conn.ReadContext(ctx, returnData)
	if err != nil && err != io.EOF {
		return err
	}
	if n != 64 {
		return fmt.Errorf("short read: %d != %d", n, 64)
	}
	if bytes.Compare(hashOfData[:], returnData[:64]) != 0 {
		return fmt.Errorf("hashes do not match: %x != %x", hashOfData, returnData)
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
