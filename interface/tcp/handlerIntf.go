package tcp

import (
	"context"
	"net"
)

// HandlerFunc 代表应用程序处理函数
type HandlerFunc func(ctx context.Context, conn net.Conn)

// HandlerIntf 业务处理handler的抽象
type HandlerIntf interface {
	Handle(ctx context.Context, conn net.Conn)
	Close() error
}
