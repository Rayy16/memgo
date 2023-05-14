package database

import (
	"fmt"
	"memgo/aof"
	"memgo/config"
	"memgo/interface/database"
	"memgo/interface/resp"
	"memgo/logger"
	"memgo/redis/RESP/protocol"
	"runtime/debug"
	"strconv"
	"strings"
	"time"
)

type MemgoServer struct {
	dbSet     []*DbObject
	persister *aof.Persister
}

func TmpDbSvrMaker() database.DBEngine {
	TmpServer := &MemgoServer{}
	TmpServer.dbSet = make([]*DbObject, config.Properties.Databases)
	for i := range TmpServer.dbSet {
		dbObj := MakeDbObject()
		dbObj.index = i
		TmpServer.dbSet[i] = dbObj
	}
	return TmpServer
}

func NewMemgoServer() *MemgoServer {
	server := &MemgoServer{}

	// 先初始化 MemgoServer的每一个 DbObject
	if config.Properties.Databases == 0 {
		config.Properties.Databases = 16
	}
	server.dbSet = make([]*DbObject, config.Properties.Databases)

	for i := range server.dbSet {
		dbObject := MakeDbObject()
		dbObject.index = i
		server.dbSet[i] = dbObject
	}

	if config.Properties.AppendOnly {
		// 创建 aof persister
		aofHandler, err := aof.NewPersister(server, true, config.Properties.AppendFilename, config.Properties.AppendFsync,
			TmpDbSvrMaker)
		if err != nil {
			panic("new aof persister failer: " + err.Error())
		}
		// 绑定 aof persister
		for i := range server.dbSet {
			dbIdx := i
			addAof := func(cmdline CmdLine) {
				aofHandler.SaveCmdLine(dbIdx, cmdline)
			}
			server.dbSet[i].addAof = addAof
		}
		server.persister = aofHandler
	}

	return server
}

func BGRewriteAof(server *MemgoServer, args CmdLine) resp.ReplyIntf {
	if server.persister == nil {
		return protocol.MakeErrReply("Aof persistence is not enabled")
	}
	err := server.persister.ReWrite()
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	return protocol.MakeOkReply()
}

func (server *MemgoServer) Exec(client resp.ConnectionIntf, cmdLine database.CmdLine) (result resp.ReplyIntf) {
	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			result = protocol.MakeUnknownErrReply()
		}
	}()

	cmdName := strings.ToLower(string(cmdLine[0]))
	// 特殊命令
	if cmdName == "rewriteaof" {
		return BGRewriteAof(server, cmdLine)
	}

	// 正常命令
	if cmdName == "select" {
		if len(cmdLine) != 2 {
			return protocol.MakeArgNumErrReply("select")
		}
		return server.ExecSelect(client, cmdLine[1:])
	}
	selectedDB := client.GetDBIndex()
	return server.dbSet[selectedDB].Exec(client, cmdLine)
}

func (server *MemgoServer) Close() {
	if server.persister != nil {
		server.persister.Close()
	}
}

func (server *MemgoServer) AfterClientClose(conn resp.ConnectionIntf) {
}

func (server *MemgoServer) ForEach(idx int, entity2reply func(key string, entity *database.DataEntity, expireAt *time.Time) bool) {
	if idx > len(server.dbSet) || idx < 0 {
		panic("error db idx in <ForEach>")
	}
	server.dbSet[idx].ForEach(entity2reply)
}

func (server *MemgoServer) ExecSelect(client resp.ConnectionIntf, cmdLine CmdLine) resp.ReplyIntf {
	dbIndex, err := strconv.Atoi(string(cmdLine[0]))
	if err != nil {
		return protocol.MakeErrReply("ERR invalid DB index")
	}
	if dbIndex >= len(server.dbSet) || dbIndex < 0 {
		return protocol.MakeErrReply("ERR DB index is out of range")
	}
	client.SelectDB(dbIndex)

	return protocol.MakeOkReply()
}
