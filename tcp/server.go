package tcp

import (
	"context"
	"fmt"
	"goRedis/interface/tcp"
	"goRedis/lib/logger"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// Config stores tcp server properties
type Config struct {
	Address    string        `yaml:"address"`
	MaxConnect uint32        `yaml:"max-connect"`
	Timeout    time.Duration `yaml:"timeout"`
}

// ClientCounter Record the number of clients in the current goRedis server
var ClientCounter int

// ListenAndServeWithSignal binds port and handle requests,blocking until receive stop signal
func ListenAndServeWithSignal(cfg *Config, handler tcp.Handler) error {
	closeChan := make(chan struct{}) // empty struct as signal
	sigCh := make(chan os.Signal)
	signal.Notify(sigCh, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT) // get system signal
	go func() {
		sig := <-sigCh
		switch sig {
		case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			closeChan <- struct{}{}
		}
	}()
	// start listen
	listener, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		return err
	}
	logger.Info(fmt.Sprintf("bind:%s,start listening...", cfg.Address))
	ListenAndServe(listener, handler, closeChan) // 端口信息  tcp.handler 函数   关闭信号
	return nil
}

// ListenAndServe binds port and handle requests,blocking until close
func ListenAndServe(listener net.Listener, handler tcp.Handler, closeChan <-chan struct{}) {
	// listen signal
	errCh := make(chan error, 1) // 接收错误信号
	defer close(errCh)
	go func() {
		select { // 阻塞在这里，等待关闭或者err信号
		case <-closeChan: // 系统关闭
			logger.Info("get exit signal")
		case er := <-errCh: // 出现error
			logger.Info(fmt.Sprintf("accept error:%s", er.Error()))
		}
		logger.Info("shutting down...")
		_ = listener.Close() // listener.Accept() will return err immediately  立刻不接受连接了
		_ = handler.Close()  // close connections  关闭连接
	}()
	ctx := context.Background()
	var waitDone sync.WaitGroup // waitGroup保证在协程结束之前，主函数不会结束
	for {                       // 死循环等着accept
		conn, err := listener.Accept() // 建立连接
		if err != nil {
			errCh <- err
			break
		}
		// handle
		logger.Info("accept link")
		ClientCounter++
		waitDone.Add(1)
		go func() {
			defer func() {
				waitDone.Done()
				ClientCounter--
			}()
			handler.Handle(ctx, conn) // 用协程来处理连接，一个协程对应一个tcp连接
		}()
	}
	waitDone.Wait()
}
