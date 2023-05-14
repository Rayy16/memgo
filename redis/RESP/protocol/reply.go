// 本文件主要依托于 reply接口 实现三种reply类型
// 以及相应的工厂方法

package protocol

import (
	"bytes"
	"memgo/interface/resp"
	"strconv"
)

var (
	// CRLF RESP协议中的分隔符
	CRLF = "\r\n"
)

type BulkReply struct {
	Arg []byte
}

func (r *BulkReply) ToBytes() []byte {
	return []byte("$" + strconv.Itoa(len(r.Arg)) + CRLF + string(r.Arg) + CRLF)
}

func MakeBulkReply(arg []byte) *BulkReply {
	return &BulkReply{
		Arg: arg,
	}
}

type MultiBulkReply struct {
	Args [][]byte
}

func (r *MultiBulkReply) ToBytes() []byte {
	lines := len(r.Args)
	var buf bytes.Buffer
	buf.WriteString("*" + strconv.Itoa(lines) + CRLF)
	for _, arg := range r.Args {
		// $-1\r\n 表示 nil
		if arg == nil {
			buf.WriteString("$-1" + CRLF)
		} else {
			buf.WriteString("$" + strconv.Itoa(len(arg)) + CRLF + string(arg) + CRLF)
		}
	}
	return buf.Bytes()
}

func MakeMultiBulkReply(args [][]byte) *MultiBulkReply {
	return &MultiBulkReply{
		Args: args,
	}
}

type StatusReply struct {
	Status string
}

func (r *StatusReply) ToBytes() []byte {
	return []byte("+" + r.Status + CRLF)
}

func MakeStatusReply(status string) *StatusReply {
	return &StatusReply{
		Status: status,
	}
}

func IsOKReply(reply resp.ReplyIntf) bool {
	return string(reply.ToBytes()) == string(MakeOkReply().ToBytes())
}

type IntReply struct {
	Code int64
}

func (r *IntReply) ToBytes() []byte {
	return []byte(":" + strconv.FormatInt(r.Code, 10) + CRLF)
}

func MakeIntReply(code int64) *IntReply {
	return &IntReply{
		Code: code,
	}
}

// ErrorReply 它即实现了 error接口 又实现了 redis.Reply接口
type ErrorReply interface {
	Error() string
	ToBytes() []byte
}

// StandardErrorReply Standard 模板模式 提供标准的错误回复
type StandardErrorReply struct {
	Status string
}

func (r *StandardErrorReply) Error() string {
	return r.Status
}

func (r *StandardErrorReply) ToBytes() []byte {
	return []byte("-" + r.Status + CRLF)
}

func MakeErrReply(err string) *StandardErrorReply {
	return &StandardErrorReply{
		Status: err,
	}
}
func IsErrorReply(reply resp.ReplyIntf) bool {
	return reply.ToBytes()[0] == '-'
}
