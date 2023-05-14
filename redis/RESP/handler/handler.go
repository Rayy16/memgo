package handler

import (
	"context"
	"io"
	"memgo/database"
	databaseIntf "memgo/interface/database"
	"memgo/logger"
	"memgo/redis/RESP/connection"
	"memgo/redis/RESP/parser"
	"memgo/redis/RESP/protocol"
	"memgo/utils/atomic"
	"net"
	"strings"
	"sync"
)

var unKnownErrReplyBytes = []byte("-ERR unknown\r\n")

type RespHandler struct {
	activeConn sync.Map                  // 存储的就是 ConnectionIntf
	dbIntf     databaseIntf.DBServerIntf // database层的抽象
	closing    atomic.Boolean
}

// 从activeConn中关闭其中一个连接 Conn
func (r *RespHandler) closeClient(client *connection.Connection) {
	_ = client.Close()
	r.dbIntf.AfterClientClose(client)
	r.activeConn.Delete(client)
}

// MakeHandler NODE 目前使用 SimpleMemgoDBServer 作为存储引擎
func MakeHandler() *RespHandler {
	var dbIntf databaseIntf.DBServerIntf
	dbIntf = database.NewMemgoServer()
	return &RespHandler{
		activeConn: sync.Map{},
		dbIntf:     dbIntf,
		closing:    0,
	}
}

func (r *RespHandler) Handle(ctx context.Context, conn net.Conn) {
	// 若handler处于关闭中 关闭新连接conn
	if r.closing.Get() {
		_ = conn.Close()
	}
	client := connection.NewConn(conn)
	r.activeConn.Store(client, struct{}{})

	ch := parser.ParseStream(conn)

	// 接收payload的两种情况
	// 1. payload中err不为nil =》 错误处理
	// 2. payload中err为nil =》 db.Exec()
	for payload := range ch {
		// 1. payload中err不为nil =》 错误处理
		if payload.Err != nil {
			// 客户端断开连接
			if payload.Err == io.EOF ||
				payload.Err == io.ErrUnexpectedEOF ||
				strings.Contains(payload.Err.Error(), "use of closed network connection") {
				// 关闭客户端连接, 打印log
				r.closeClient(client)
				logger.Info("connection closed: " + client.RemoteAddr().String())
				return
			}
			// protocol error 协议解析错误 =》 返回给客户端
			errReply := protocol.MakeErrReply(payload.Err.Error())
			// 如果 回写客户端错误信息时 出错了，关闭连接
			_, err := client.Write(errReply.ToBytes())
			if err != nil {
				r.closeClient(client)
				logger.Info("write back <" + string(errReply.ToBytes()) + "> to client error: " + err.Error())
				logger.Info("connection closed: " + client.RemoteAddr().String())
				return
			}
			// 回写客户端 成功，继续
			continue
		}
		// 2. payload中err为nil =》 db.Exec()
		if payload.Data == nil {
			// 解析出来的数据是空的
			logger.Error("empty payload")
			continue
		}
		// 客户端传输的命令必须实现了 MultiBulkReply类型
		mbReply, ok := payload.Data.(*protocol.MultiBulkReply)
		if !ok {
			logger.Error("require multi bulk protocol: " + string(payload.Data.ToBytes()))
			continue
		}
		execResultReply := r.dbIntf.Exec(client, mbReply.Args)
		// 执行结果Reply 为 nil =》 未知错误
		if execResultReply == nil {
			// TODO 使用 error报文
			_, _ = client.Write(unKnownErrReplyBytes)
			continue
		}
		_, err := client.Write(execResultReply.ToBytes())
		// 将 执行结果Reply 写回给 客户端时 出错
		if err != nil {
			logger.Error("write back <" + string(execResultReply.ToBytes()) + "> to client error: " + err.Error())
			_, _ = client.Write(unKnownErrReplyBytes)
		}
	}
}

func (r *RespHandler) Close() error {
	logger.Info("handler shutting down...")
	r.closing.Set(true)
	// 关闭所有连接
	r.activeConn.Range(
		func(key, value interface{}) bool {
			client := key.(*connection.Connection)
			_ = client.Close()
			return true
		},
	)
	// 关闭db
	r.dbIntf.Close()
	return nil
}
