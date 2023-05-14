package database

import (
	"memgo/datastruct/dict"
	"memgo/interface/database"
	"memgo/interface/resp"
	"memgo/redis/RESP/protocol"
	"strings"
	"time"
)

// DbObjectWithoutTTL SimpleMemgoDBServer 持有的 database 实例,可以看作是 dbObject
type DbObjectWithoutTTL struct {
	index  int
	data   dict.DictIntf // DB下的存储引擎 由 DS : Dict 接口实现
	addAof func(CmdLine)
}

func (db *DbObjectWithoutTTL) ForEach(entity2reply func(key string, entity *database.DataEntity, expireAt *time.Time) bool) {
	//TODO implement me
	panic("implement me")
}

func MakeDbObjectWithoutTTL() *DbObjectWithoutTTL {
	return &DbObjectWithoutTTL{
		index:  0,
		data:   dict.MakeSyncDict(),
		addAof: func(cmdLine CmdLine) {},
	}
}

func (db *DbObjectWithoutTTL) Exec(conn resp.ConnectionIntf, cmdLine CmdLine) resp.ReplyIntf {
	// PING SET SETNX GET
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	if !ok {
		return protocol.MakeErrReply("ERR unknown command")
	}
	// 校验参数个数是否合法
	if !validateArity(cmd.arity, cmdLine) {
		// 参数个数错误报文 ERR wrong number of arguments for 'set' command
		return protocol.MakeArgNumErrReply(string(cmdLine[0]))
	}
	fun := cmd.executor
	// SET K 100 =》 只需要 k 100 即可, 因为已经找到了 SET命令 对应的 executor
	return fun(db, cmdLine[1:])
}

// GetEntity 在go中，函数形参是空接口类型，那么传入参数时，自动发生类型转换; 而返回类型为空接口时，想要拿到所指向的具体类型，需要使用断言进行转换
func (db *DbObjectWithoutTTL) GetEntity(key string) (*database.DataEntity, bool) {
	rawVal, ok := db.data.Get(key)
	if !ok {
		return nil, false
	}

	entity, _ := rawVal.(*database.DataEntity)
	return entity, true

	//entity := &database.DataEntity{Data: rawVal}
	//return entity, true
}

// PutEntity 在go中，函数形参是空接口类型，那么传入参数时，自动发生类型转换; 而返回类型为空接口时，想要拿到所指向的具体类型，需要使用断言进行转换
func (db *DbObjectWithoutTTL) PutEntity(key string, entity *database.DataEntity) int {
	return db.data.Put(key, entity)
}

func (db *DbObjectWithoutTTL) PutIfExists(key string, entity *database.DataEntity) int {
	return db.data.PutIfExists(key, entity)
}

func (db *DbObjectWithoutTTL) PutIfAbsent(key string, entity *database.DataEntity) int {
	return db.data.PutIfAbsent(key, entity)
}

func (db *DbObjectWithoutTTL) Remove(key string) {
	db.data.Remove(key)
}

func (db *DbObjectWithoutTTL) Removes(keys ...string) int {
	res := 0
	for _, key := range keys {
		_, ok := db.data.Get(key)
		if ok {
			db.Remove(key)
			res++
		}
	}
	return res
}

func (db *DbObjectWithoutTTL) Flush() {
	db.data.Clear()
}
