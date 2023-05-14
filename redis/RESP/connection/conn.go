// 该包主要是为了实现 业务handler的组成部分
// activeConn sync.map （key: ConnectionIntf, val: struct{}{})
// 在协议层管理连接

package connection

import (
	"memgo/utils/wait"
	"net"
	"sync"
	"time"
)

type Connection struct {
	Conn         net.Conn
	waitingReply wait.Wait  // 等待直到发送完数据，用于优雅地关闭连接
	mu           sync.Mutex // 保留
	selectedDB   int
}

func NewConn(conn net.Conn) *Connection {
	return &Connection{
		Conn:         conn,
		waitingReply: wait.Wait{},
		mu:           sync.Mutex{},
		selectedDB:   0,
	}
}

func (c *Connection) RemoteAddr() net.Addr {
	return c.Conn.RemoteAddr()
}

func (c *Connection) Write(bytes []byte) (int, error) {
	// 传进来的bytes为空，不进行传输
	if len(bytes) == 0 {
		return 0, nil
	}
	c.waitingReply.Add(1)
	defer c.waitingReply.Done()
	return c.Conn.Write(bytes)

	//c.mu.Lock()
	//c.waitingReply.Add(1)
	//defer func() {
	//	c.mu.Unlock()
	//	c.waitingReply.Done()
	//}()
	//return c.Conn.Write(bytes)
}

func (c *Connection) Close() error {
	// 设置10秒超时
	c.waitingReply.WaitWithTimeout(10 * time.Second)
	_ = c.Conn.Close()
	return nil
}

func (c *Connection) GetDBIndex() int {
	return c.selectedDB
}

func (c *Connection) SelectDB(i int) {
	c.selectedDB = i
}
