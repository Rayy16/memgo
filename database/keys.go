package database

import (
	"memgo/interface/database"
	"memgo/interface/resp"
	"memgo/redis/RESP/protocol"
	"memgo/utils"
	"memgo/utils/wildcard"
	"strconv"
	"time"
)

func execDel_DbObj(db database.DbObjectIntf, args CmdLine) resp.ReplyIntf {
	var dbObject = db.(*DbObject)
	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = string(arg)
	}
	res := dbObject.Removes(keys...)
	if res > 0 {
		dbObject.addAof(utils.ToCmdLine3("DEL", args...))
	}
	return protocol.MakeIntReply(int64(res))
}

func execExists_DbObj(db database.DbObjectIntf, args CmdLine) resp.ReplyIntf {
	var dbObject = db.(*DbObject)
	var res int64
	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = string(arg)
	}
	for _, key := range keys {
		_, ok := dbObject.GetEntity(key)
		if ok {
			res++
		}
	}
	return protocol.MakeIntReply(res)
}

func execFlushDB_DbObj(db database.DbObjectIntf, args CmdLine) resp.ReplyIntf {
	var dbObject = db.(*DbObject)
	dbObject.Flush()
	dbObject.addAof(utils.ToCmdLine3("FLUSHDB", args...))
	return protocol.MakeOkReply()
}

func execType_DbObj(db database.DbObjectIntf, args CmdLine) resp.ReplyIntf {
	var dbObject = db.(*DbObject)
	key := string(args[0])
	entity, ok := dbObject.GetEntity(key)
	if ok {
		switch entity.Data.(type) {
		case []byte:
			return protocol.MakeStatusReply("string")
		// TODO 其他类型进行匹配
		default:
			return protocol.MakeUnknownErrReply()
		}
	} else {
		return protocol.MakeStatusReply("none")
	}
}

func execRename_DbObj(db database.DbObjectIntf, args CmdLine) resp.ReplyIntf {
	var dbObject = db.(*DbObject)
	oldKey := string(args[0])
	newKey := string(args[1])
	entity, ok := dbObject.GetEntity(oldKey)
	if ok {
		dbObject.Remove(oldKey)
		dbObject.PutEntity(newKey, entity)
		dbObject.addAof(utils.ToCmdLine3("RENAME", args...))
		return protocol.MakeOkReply()
	}
	return protocol.MakeErrReply("no such key")
}

func execRenameNx_DbObj(db database.DbObjectIntf, args CmdLine) resp.ReplyIntf {
	var dbObject = db.(*DbObject)
	oldKey := string(args[0])
	newKey := string(args[1])
	_, ok1 := dbObject.GetEntity(newKey)
	if ok1 {
		return protocol.MakeIntReply(0)
	}
	entity, ok2 := dbObject.GetEntity(oldKey)
	if ok2 {
		dbObject.Remove(oldKey)
		dbObject.PutEntity(newKey, entity)
		dbObject.addAof(utils.ToCmdLine3("RENAMENX", args...))
		return protocol.MakeIntReply(1)
	}
	return protocol.MakeErrReply("no such key")
}

func execKeys_DbObj(db database.DbObjectIntf, args CmdLine) resp.ReplyIntf {
	var dbObject = db.(*DbObject)
	pattern, err := wildcard.CompilePattern(string(args[0]))
	if err != nil {
		return protocol.MakeErrReply("illegal pattern")
	}
	keys := make([][]byte, 0)
	dbObject.data.ForEach(func(key string, val interface{}) bool {
		if pattern.IsMatch(key) {
			keys = append(keys, []byte(key))
		}
		return true
	})
	return protocol.MakeMultiBulkReply(keys)
}

func execExpire(db database.DbObjectIntf, args CmdLine) resp.ReplyIntf {
	var dbObject = db.(*DbObject)
	key := string(args[0])
	ttlArg, err := strconv.ParseInt(string(args[1]), 10, 32)
	if err != nil {
		return protocol.MakeIntReply(0)
	}
	_, exists := dbObject.GetEntity(key)
	if !exists {
		return protocol.MakeIntReply(0)
	}
	ttl := time.Duration(ttlArg) * time.Second
	expireAt := time.Now().Add(ttl)
	dbObject.addAof(utils.MakeExpireCmd(key, expireAt).Args)
	dbObject.Expire(key, expireAt)
	return protocol.MakeIntReply(1)
}

// TODO addAof
func execExpireAt(db database.DbObjectIntf, args CmdLine) resp.ReplyIntf {
	var dbObject = db.(*DbObject)
	key := string(args[0])
	rawTTL, err := strconv.ParseInt(string(args[1]), 10, 32)
	if err != nil {
		return protocol.MakeIntReply(0)
	}
	_, exists := dbObject.GetEntity(key)
	if !exists {
		return protocol.MakeIntReply(0)
	}

	expireAt := time.Unix(rawTTL, 0)
	dbObject.addAof(utils.MakeExpireCmd(key, expireAt).Args)
	dbObject.Expire(key, expireAt)
	return protocol.MakeIntReply(1)
}

func execTTL(db database.DbObjectIntf, args CmdLine) resp.ReplyIntf {
	var dbObject = db.(*DbObject)
	key := string(args[0])
	_, exists := dbObject.GetEntity(key)
	// key不存在 返回-2
	if !exists {
		return protocol.MakeIntReply(-2)
	}

	raw, isExpire := dbObject.ttlMap.Get(key)
	// 没有设置过期时间 返回-1
	if !isExpire {
		return protocol.MakeIntReply(-1)
	}
	expireTime := raw.(time.Time)
	ttl := expireTime.Sub(time.Now())
	return protocol.MakeIntReply(int64(ttl / time.Second))
}

func init() {
	RegisterCommand("TTL", execTTL, readFirstKey, 2)                 // TTL k1
	RegisterCommand("EXPIREAT", execExpireAt, writeFirstKey, 3)      // EXPIREAT k 1324687
	RegisterCommand("EXPIRE", execExpire, writeFirstKey, 3)          // EXPIRE k 3
	RegisterCommand("DEL", execDel_DbObj, writeAllKeys, -2)          // DEL k1 k2 k3 ...
	RegisterCommand("EXISTS", execExists_DbObj, readAllKeys, -2)     // EXISTS k1 k2 k3 ...
	RegisterCommand("FLUSHDB", execFlushDB_DbObj, noPrepare, 1)      // FLUSHDB
	RegisterCommand("TYPE", execType_DbObj, readFirstKey, 2)         // TYPE key
	RegisterCommand("RENAME", execRename_DbObj, writeAllKeys, 3)     // RENAME src dest
	RegisterCommand("RENAMENX", execRenameNx_DbObj, writeAllKeys, 3) // RENAMENX src dest
	RegisterCommand("KEYS", execKeys_DbObj, noPrepare, 2)            // KEYS pattern
}
