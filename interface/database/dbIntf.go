package database

import (
	"memgo/interface/resp"
	"time"
)

type CmdLine = [][]byte

type ExecFunc func(db *DbObjectIntf, args CmdLine) resp.ReplyIntf

// DataEntity 我们使用DataEntity类型来表示存储引擎中存储的所有val
type DataEntity struct {
	Data interface{}
}

// DBServerIntf 指 DataBase层 抽象
// DB memgo server层抽象
type DBServerIntf interface {
	Exec(client resp.ConnectionIntf, cmdLine CmdLine) resp.ReplyIntf
	Close()
	AfterClientClose(c resp.ConnectionIntf) // 保留方法
}

type DBEngine interface {
	DBServerIntf
	ForEach(idx int, entity2reply func(key string, entity *DataEntity, expireAt *time.Time) bool)
}

type DbObjectIntf interface {
	Exec(conn resp.ConnectionIntf, cmdline CmdLine) resp.ReplyIntf
	GetEntity(key string) (*DataEntity, bool)
	PutEntity(key string, entity *DataEntity) int
	PutIfExists(key string, entity *DataEntity) int
	PutIfAbsent(key string, entity *DataEntity) int
	Remove(key string)
	Removes(keys ...string) int
	Flush()
	ForEach(entity2reply func(key string, entity *DataEntity, expireAt *time.Time) bool)
}
