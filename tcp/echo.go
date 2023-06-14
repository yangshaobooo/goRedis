package tcp

/**
 * A echo server to test whether the server is functioning normally
 */
import (
	"bufio"
	"context"
	"fmt"
	"goRedis/lib/logger"
	"goRedis/lib/sync/atomic"
	"goRedis/lib/sync/wait"
	"io"
	"net"
	"sync"
	"time"
)

// EchoHandler echos received line to client,using for test
type EchoHandler struct {
	activeConn sync.Map
	closing    atomic.Boolean
}

// MakeEchoHandler creates EchoHandler
func MakeEchoHandler() *EchoHandler {
	return &EchoHandler{}
}

// EchoClient is client for EchoHandler,using for test
type EchoClient struct {
	Conn    net.Conn
	Waiting wait.Wait
}

// Close closing connection
func (c *EchoClient) Close() error {
	c.Waiting.WaitWithTimeout(10 * time.Second) // 超时就关闭
	c.Conn.Close()
	return nil
}

// Close stops echo handler
func (h *EchoHandler) Close() error {
	logger.Info("handler shutting down...")
	h.closing.Set(true)
	h.activeConn.Range(func(key, value interface{}) bool {
		Client := key.(*EchoClient)
		_ = Client.Close()
		return true
	})
	return nil
}

// Handle echos received line to client
func (h *EchoHandler) Handle(ctx context.Context, conn net.Conn) {
	if h.closing.Get() {
		// closing handler refuse new connection
		_ = conn.Close()
		return
	}
	client := &EchoClient{
		Conn: conn,
	}
	h.activeConn.Store(client, struct{}{})
	reader := bufio.NewReader(conn)
	for {
		// may occur client eof,client timeout,server early close
		msg, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				logger.Info("connection close")
				h.activeConn.Delete(client)
			} else {
				logger.Warn(err)
			}
			return
		}
		client.Waiting.Add(1)
		b := []byte(msg)
		_, _ = conn.Write(b)
		fmt.Printf("客户端发送消息为%v\n", string(b))
		client.Waiting.Done()
	}
}
