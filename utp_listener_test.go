package utp_test

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
	"io"
	"sync"
	"testing"
	"time"
)

func TestAcceptUtpWithConnId(t *testing.T) {
	logger := zaptest.NewLogger(t, zaptest.Level(zapcore.DebugLevel))
	l := newTestServer(t, logger.Named("server"))
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		conn, err := l.AcceptUTPWithConnId(12)
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
		fmt.Println(string(buf))
		wg.Done()
	}()

	connNoSetConnId, err := utp.Dial("utp", "127.0.0.1:8080")
	if err != nil {
		panic(err)
	}
	_, err = connNoSetConnId.Write([]byte("hello! connId is not defined"))
	if err != nil {
		panic(err)
	}

	connSetConnId, err := utp.DialOptions("utp", "127.0.0.1:8080",
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

func TestAcceptTimeout(t *testing.T) {
	logger := zaptest.NewLogger(t, zaptest.Level(zapcore.DebugLevel))
	l := newTestServer(t, logger.Named("server"))
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(1)*time.Second)
		_, err := l.AcceptUTPContext(ctx, 13)
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
		_, _ = l.AcceptUTPContext(ctx, 13)
		timeComsumed := time.Since(now).Seconds()
		assert.Equal(t, true, timeComsumed < timeout.Seconds(), "except stoped immediately but not")
		cancel()
	}()
	_ = l.Close()
	now := time.Now()
	timeout := time.Duration(10) * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	_, _ = l.AcceptUTPContext(ctx, 13)
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
