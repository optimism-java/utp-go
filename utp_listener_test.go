package utp_test

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/p2p/enode"
	"github.com/optimism-java/utp-go"
	"github.com/optimism-java/utp-go/libutp"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
)

func TestAcceptUtpWithConnId(t *testing.T) {
	logger := zaptest.NewLogger(t, zaptest.Level(zapcore.DebugLevel))
	l := newTestServer(t, logger.Named("server"))
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		conn, err := l.AcceptUTPWithConnId(enode.ID{}, libutp.ReceConnId(12))
		if err != nil {
			panic(err)
		}
		logger.Info("accept a conn with connectionId:", zap.Any("connId", 12))
		buf := make([]byte, 100)
		n, err := conn.Read(buf)
		if err != nil {
			panic(err)
		}
		assert.Equal(t, "hello! connId is 12", string(buf[:n]))
		wg.Done()
	}()

	connSetConnId, err := utp.DialOptions("utp", l.Addr().String(),
		utp.WithContext(context.Background()), utp.WithConnId(12))

	if err != nil {
		panic(err)
	}
	_, err = connSetConnId.Write([]byte("hello! connId is 12"))
	if err != nil {
		panic(err)
	}
	wg.Wait()
	err = l.Close()
	if err != nil {
		panic(err)
	}
}

func TestAcceptWithoutConnId(t *testing.T) {
	logger := zaptest.NewLogger(t, zaptest.Level(zapcore.DebugLevel))
	l := newTestServer(t, logger.Named("server"))
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		conn, err := l.AcceptUTP()
		if err != nil {
			panic(err)
		}
		logger.Info("accept a conn without connectionId")
		buf := make([]byte, 100)
		n, err := conn.Read(buf)
		if err != nil {
			panic(err)
		}
		assert.Equal(t, "hello! connId is not defined", string(buf[:n]))

		n, err = conn.Write([]byte("hello! connId is not defined"))
		assert.NoError(t, err, "server write to conn has error")
		wg.Done()
	}()

	connNoSetConnId, err := utp.DialOptions("utp", l.Addr().String(), utp.WithLogger(logger.Named("client")))
	if err != nil {
		panic(err)
	}
	buf := []byte("hello! connId is not defined")
	fmt.Println("write buf:", len(buf))
	_, err = connNoSetConnId.Write(buf)
	if err != nil {
		panic(err)
	}
	readBuf := make([]byte, 100)
	n, err := connNoSetConnId.Read(readBuf)
	assert.NoError(t, err, "cli conn read buf has error")
	assert.Equal(t, "hello! connId is not defined", string(readBuf[:n]))

}

func TestAcceptTimeout(t *testing.T) {
	logger := zaptest.NewLogger(t, zaptest.Level(zapcore.DebugLevel))
	l := newTestServer(t, logger.Named("server"))
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(1)*time.Second)
		_, err := l.AcceptUTPContext(ctx, enode.ID{}, libutp.SendCid(13))
		assert.Equal(t, true, err != nil, "except timeout but not")
		cancel()
	}()
	time.Sleep(time.Duration(2) * time.Second)
}

func TestListenerClosed(t *testing.T) {
	logger := zaptest.NewLogger(t, zaptest.Level(zapcore.DebugLevel))
	l := newTestServer(t, logger.Named("server"))
	go func() {
		now := time.Now()
		timeout := time.Duration(10) * time.Second
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		_, _ = l.AcceptUTPContext(ctx, enode.ID{}, libutp.ReceConnId(13))
		timeComsumed := time.Since(now).Seconds()
		assert.Equal(t, true, timeComsumed < timeout.Seconds(), "except stoped immediately but not")
		cancel()
	}()
	_ = l.Close()
	now := time.Now()
	timeout := time.Duration(10) * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	_, _ = l.AcceptUTPContext(ctx, enode.ID{}, libutp.ReceConnId(13))
	timeComsumed := time.Since(now).Seconds()
	assert.Equal(t, true, timeComsumed < timeout.Seconds(), "except stoped immediately but not")
	cancel()
}

func randomUint32() uint32 {
	var buf [4]byte
	_, err := io.ReadFull(rand.Reader, buf[:])
	if err != nil {
		panic("can't read from random source: " + err.Error())
	}
	return binary.LittleEndian.Uint32(buf[:])
}

func randomPayload(l int) []byte {
	buf := make([]byte, l)
	_, err := io.ReadFull(rand.Reader, buf[:])
	if err != nil {
		panic("can't read from random source: " + err.Error())
	}
	return buf
}
