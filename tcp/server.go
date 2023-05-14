package tcp

import (
	"context"
	"fmt"
	"memgo/interface/tcp"
	"memgo/logger"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// Config Tcp服务器配置
type Config struct {
	Address string        `yaml:"address"`
	MaxConn uint32        `yaml:"max-connect"` // 暂未使用
	Timeout time.Duration `yaml:"timeout"`     // 暂未使用
}

var ClientCounter int

// ListenAndServeWithSignal 实现优雅的退出 监听内核推送的信号
func ListenAndServeWithSignal(cfg *Config, handler tcp.HandlerIntf) error {
	// 注册需要监听的系统信号
	sigCh := make(chan os.Signal)
	signal.Notify(sigCh, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	// 监听系统信号，若事件发生，则通知server退出
	closeChan := make(chan struct{})
	go func() {
		sig := <-sigCh
		switch sig {
		case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			closeChan <- struct{}{}
		}
	}()
	// 绑定端口并开始监听
	listener, err := net.Listen("tcp", cfg.Address)
	if err != nil {
		return err
	}
	logger.Info(fmt.Sprintf("bind: %s, start listening...", cfg.Address))
	ListenAndServe(listener, handler, closeChan)
	return nil
}

func ListenAndServe(listener net.Listener, handler tcp.HandlerIntf, closeChan <-chan struct{}) {
	errChan := make(chan error, 1)
	defer close(errChan)
	// 启一个协程监控 closeChan/ errChan, 若收到退出/错误信号则关闭连接
	go func() {
		select {
		case <-closeChan:
			logger.Info("get exit signal")
		case err := <-errChan:
			logger.Info(fmt.Sprintf("accept error: %s", err.Error()))
		}
		// 执行关闭逻辑
		logger.Info("shutting down...")
		_ = listener.Close() // 关闭监听socket
		_ = handler.Close()  // 关闭实际业务的处理handler
	}()

	var wg sync.WaitGroup
	for {
		// Accept 会一直阻塞直到有新的连接建立或者listen中断才会返回
		conn, err := listener.Accept()
		if err != nil {
			// 通常是由于listener被关闭无法继续监听导致的错误
			errChan <- err
			break
		}
		// 开启新的 goroutine 处理该连接
		logger.Info("accept link")
		ctx := context.Background()
		wg.Add(1)
		ClientCounter++
		go func() {
			defer func() {
				wg.Done()
				ClientCounter--
			}()
			handler.Handle(ctx, conn)
		}()
	}
	// 保证那些正在处理的handler执行完成后再退出
	wg.Wait()
}
