package database

import (
	"memgo/interface/database"
	"memgo/interface/resp"
	"memgo/redis/RESP/protocol"
)

func Ping(db database.DbObjectIntf, args CmdLine) resp.ReplyIntf {
	return protocol.MakePongReply()
}

func init() {
	RegisterCommand("ping", Ping, noPrepare, 1)
}
