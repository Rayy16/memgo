package database

import (
	"memgo/aof"
	"memgo/config"
	"memgo/interface/database"
	"memgo/interface/resp"
	"memgo/logger"
	"memgo/redis/RESP/protocol"
	"strconv"
	"strings"
	"time"
)

const defaultDatabases = 16

// SimpleMemgoDBServer 是多个DB的集合, 它被resp.handler持有, 作为存储引擎
// TODO 需要优化
type SimpleMemgoDBServer struct {
	dbSet      []*DbObjectWithoutTTL // 存储每一个DB实例
	aofHandler *aof.AofHandlerWithoutReWrite
}

func (server *SimpleMemgoDBServer) ForEach(idx int, entity2reply func(key string, entity *database.DataEntity, expireAt *time.Time) bool) {

}

func NewSimpleMemgoServer() (DbEngine *SimpleMemgoDBServer) {
	DbEngine = &SimpleMemgoDBServer{}
	if config.Properties.Databases == 0 {
		config.Properties.Databases = 16
	}
	// 按配置实例化出对应个数的 databse实例, 可以看作是 dbObject
	DbEngine.dbSet = make([]*DbObjectWithoutTTL, config.Properties.Databases)
	for i := range DbEngine.dbSet {
		dbObject := MakeDbObjectWithoutTTL()
		dbObject.index = i
		// 将每一个 dbObject(实例化对象) 赋值给 dbSet
		DbEngine.dbSet[i] = dbObject
	}

	// 初始化 aofHandler
	DbEngine.aofHandler = nil
	if config.Properties.AppendOnly {
		aofHandler, err := aof.NewAofHandlerWithoutReWrite(DbEngine)
		if err != nil {
			panic(err)
		}
		DbEngine.aofHandler = aofHandler
		for i := range DbEngine.dbSet {
			// 函数闭包
			// NODE i在 for range时 是同一份变量，若不拷贝直接塞进闭包 变量会发生逃逸 所以需要拷贝
			// NODE 否则所有的 dbObject的 addAof方法的 dbIndex都被写死为最后一个
			index := i
			addAof := func(cmdLine CmdLine) {
				aofHandler.SaveCmdLine(index, cmdLine)
			}
			DbEngine.dbSet[index].addAof = addAof
		}
	}

	return DbEngine
}

func (server *SimpleMemgoDBServer) Exec(client resp.ConnectionIntf, cmdLine database.CmdLine) resp.ReplyIntf {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(err)
		}
	}()
	// logger.Info("receive cmd: " + string(cmdLine[0]))
	cmdName := strings.ToLower(string(cmdLine[0]))
	if cmdName == "select" {
		if len(cmdLine) != 2 {
			return protocol.MakeArgNumErrReply("select")
		}
		return server.ExecSelect(client, cmdLine[1:])
	}
	// TODO 需要优化
	dbIndex := client.GetDBIndex()
	return server.dbSet[dbIndex].Exec(client, cmdLine)
}

func (server *SimpleMemgoDBServer) Close() {

}

func (server *SimpleMemgoDBServer) AfterClientClose(conn resp.ConnectionIntf) {

}

// ExecSelect 根据用户发送的 select 指令，更改 Conn中的当前数据库
// select 2
func (server *SimpleMemgoDBServer) ExecSelect(conn resp.ConnectionIntf, args CmdLine) resp.ReplyIntf {
	dbIndex, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return protocol.MakeErrReply("ERR invalid DbObjectWithoutTTL index :" + err.Error())
	}
	if dbIndex > config.Properties.Databases {
		return protocol.MakeErrReply("ERR DbObjectWithoutTTL index is out of range")
	}
	conn.SelectDB(dbIndex)
	return protocol.MakeOkReply()
}
