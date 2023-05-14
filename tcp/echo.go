// echo.go 利用已有的 handler接口、tcp服务器函数
// 测试 tcp包的功能是否完善
// 本文件需要实现 EchoHandler接口，并且实现一个 EchoClient用来测试

package tcp

import (
	"bufio"
	"context"
	"io"
	"memgo/logger"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type Wait struct {
	sigChan chan struct{}
}

func (w *Wait) Get() {
	w.sigChan <- struct{}{}
}

func (w *Wait) Release() {
	<-w.sigChan
}

func (w *Wait) WaitWithTimeout(timeout time.Duration) bool {
	ch := make(chan struct{}, 1)
	go func() {
		defer close(ch)
		w.Get()
		ch <- struct{}{}
		w.Release()
	}()
	select {
	case <-ch:
		return false
	case <-time.After(timeout):
		return true
	}
}

func MakeWait() *Wait {
	return &Wait{
		sigChan: make(chan struct{}, 1),
	}
}

type EchoClient struct {
	conn net.Conn
	wait *Wait
}

func (c *EchoClient) close() error {

	c.conn.Close()
	return nil
}

type EchoHandler struct {
	activeConn sync.Map // 用作保存活跃连接的集合
	closing    atomic.Bool
}

func MakeEchoClient(conn net.Conn) *EchoClient {
	return &EchoClient{
		conn: conn,
		wait: MakeWait(),
	}
}

func (c *EchoClient) Close() error {
	c.wait.WaitWithTimeout(10 * time.Second)
	_ = c.conn.Close()
	return nil
}

func MakeEchoHandler() *EchoHandler {
	return &EchoHandler{}
}

func (h *EchoHandler) Handle(ctx context.Context, conn net.Conn) {
	// 若hanlder已关闭，则关闭连接
	if h.closing.Load() {
		_ = conn.Close()
		return
	}
	client := MakeEchoClient(conn)
	h.activeConn.Store(client, nil)

	reader := bufio.NewReader(conn)
	for {
		msg, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				logger.Info("connect close")
				h.activeConn.Delete(client)
			} else {
				logger.Warn(err)
			}
			return
		}
		client.wait.Get()
		b := []byte(msg)
		_, _ = conn.Write(b)
		client.wait.Release()
	}
}

// 需要等待设置 handler的退出状态, 遍历handler的每一个 activeConn 调用它们的 close方法
func (h *EchoHandler) Close() error {
	h.closing.Store(true)
	logger.Info("handler shutting down...")
	h.activeConn.Range(func(key interface{}, val interface{}) bool {
		client := key.(*EchoClient)
		_ = client.Close()
		return true
	})
	return nil
}
