package database

import (
	"memgo/interface/database"
	"memgo/interface/resp"
	"memgo/redis/RESP/protocol"
	"memgo/utils"
)

func (db *DbObject) getAsString(key string) ([]byte, protocol.ErrorReply) {
	entity, exists := db.GetEntity(key)
	if !exists {
		return nil, nil
	}
	bytes, ok := entity.Data.([]byte)
	if !ok {
		return nil, &protocol.WrongTypeErrReply{}
	}
	return bytes, nil
}

func execGet_DbObj(db database.DbObjectIntf, args CmdLine) resp.ReplyIntf {
	var dbObject = db.(*DbObject)
	key := string(args[0])
	bytes, err := dbObject.getAsString(key)
	if err != nil {
		return err
	}
	// nullBulkReply 代表 nil
	if bytes == nil {
		return protocol.MakeNullBulkReply()
	}
	return protocol.MakeBulkReply(bytes)
}

// execSet only implement SET k v
func execSet_DbObj(db database.DbObjectIntf, args CmdLine) resp.ReplyIntf {
	var dbObject = db.(*DbObject)
	key := string(args[0])
	val := args[1]
	entity := &database.DataEntity{Data: val}
	dbObject.addAof(utils.ToCmdLine3("SET", args...))
	return onlySetKV_DbObj(db, key, entity)
}

func onlySetKV_DbObj(db database.DbObjectIntf, key string, entity *database.DataEntity) resp.ReplyIntf {
	var dbObject = db.(*DbObject)
	dbObject.PutEntity(key, entity)
	return protocol.MakeOkReply()
}

func execSetNx_DbObj(db database.DbObjectIntf, args CmdLine) resp.ReplyIntf {
	var dbObject = db.(*DbObject)
	key := string(args[0])
	val := args[1]
	entity := &database.DataEntity{Data: val}
	code := db.PutIfAbsent(key, entity)
	if code == 1 {
		dbObject.addAof(utils.ToCmdLine3("SETNX", args...))
	}
	return protocol.MakeIntReply(int64(code))
}

// GETSET k v
func execGetSet_DbObj(db database.DbObjectIntf, args CmdLine) resp.ReplyIntf {
	var dbObject = db.(*DbObject)
	key := string(args[0])
	val := args[1]
	// 返回旧的 kv对
	bytes, err := dbObject.getAsString(key)
	if err != nil {
		return err
	}

	// 设置新的 kv对
	newEntity := &database.DataEntity{Data: val}
	dbObject.PutEntity(key, newEntity)

	dbObject.addAof(utils.ToCmdLine3("GETSET", args...))
	// nullBulkReply 代表 nil
	if bytes == nil {
		return protocol.MakeNullBulkReply()
	}
	return protocol.MakeBulkReply(bytes)
}

func execStrlen_DbObj(db database.DbObjectIntf, args CmdLine) resp.ReplyIntf {
	var dbObject = db.(*DbObject)
	key := string(args[0])
	bytes, err := dbObject.getAsString(key)
	if err != nil {
		return err
	}
	// nullBulkReply 代表 nil
	if bytes == nil {
		return protocol.MakeNullBulkReply()
	}
	return protocol.MakeIntReply(int64(len(bytes)))
}

func init() {
	RegisterCommand("GET", execGet_DbObj, readFirstKey, 2) // GET K
	// TODO 目前实现的SET方法为 only SET K V ; 需要迭代更新
	RegisterCommand("SET", execSet_DbObj, writeFirstKey, 3)       // SET K V
	RegisterCommand("SETNX", execSetNx_DbObj, writeFirstKey, 3)   // SETNX K V
	RegisterCommand("GETSET", execGetSet_DbObj, writeFirstKey, 3) // GETSET K V
	RegisterCommand("STRLEN", execStrlen_DbObj, readFirstKey, 2)  // STRLEN K
}
