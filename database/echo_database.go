package database

import (
	"memgo/interface/resp"
	"memgo/redis/RESP/protocol"
)

type EchoDatabase struct {
}

func NewEchoDatabase() *EchoDatabase {
	return &EchoDatabase{}
}

func (e EchoDatabase) Exec(client resp.ConnectionIntf, args [][]byte) resp.ReplyIntf {
	return protocol.MakeMultiBulkReply(args)
}

func (e EchoDatabase) Close() {

}

func (e EchoDatabase) AfterClientClose(c resp.ConnectionIntf) {

}
