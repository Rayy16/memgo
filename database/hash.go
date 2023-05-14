package database

import (
	"memgo/datastruct/dict"
	"memgo/interface/database"
	"memgo/interface/resp"
	"memgo/redis/RESP/protocol"
	"memgo/utils"
)

func newDictObject() dict.DictIntf {
	return dict.MakeSimpleDict()
}

func (db *DbObject) getAsDict(key string) (dict.DictIntf, resp.ReplyIntf) {
	entity, exists := db.GetEntity(key)
	if !exists {
		return nil, nil
	}
	data, ok := entity.Data.(dict.DictIntf)
	if !ok {
		return nil, &protocol.WrongTypeErrReply{}
	}
	return data, nil
}

func (db *DbObject) getOrInitDict(key string) (dict.DictIntf, bool, resp.ReplyIntf) {
	dictObj, errReply := db.getAsDict(key)
	if errReply != nil {
		return nil, false, errReply
	}
	inited := false
	if dictObj == nil {
		dictObj = newDictObject()
		db.PutEntity(key, &database.DataEntity{Data: dictObj})
		inited = true
	}
	return dictObj, inited, nil
}

func execHSet(db database.DbObjectIntf, args CmdLine) resp.ReplyIntf {
	key := string(args[0])
	field := string(args[1])
	value := args[2]
	dbObj := db.(*DbObject)
	dictObj, _, errReply := dbObj.getOrInitDict(key)
	if errReply != nil {
		return errReply
	}
	result := dictObj.Put(field, value)
	dbObj.addAof(utils.ToCmdLine3("HSet", args...))
	return protocol.MakeIntReply(int64(result))
}

func execHSetNx(db database.DbObjectIntf, args CmdLine) resp.ReplyIntf {
	key := string(args[0])
	field := string(args[1])
	value := args[2]
	dbObj := db.(*DbObject)
	dictObj, _, errReply := dbObj.getOrInitDict(key)
	if errReply != nil {
		return errReply
	}
	result := dictObj.PutIfAbsent(field, value)
	dbObj.addAof(utils.ToCmdLine3("HSetNX", args...))
	return protocol.MakeIntReply(int64(result))
}

func execHGet(db database.DbObjectIntf, args CmdLine) resp.ReplyIntf {
	key := string(args[0])
	field := string(args[1])
	dbObj := db.(*DbObject)
	dictObj, errReply := dbObj.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dictObj == nil {
		return protocol.MakeNullBulkReply()
	}
	raw, exists := dictObj.Get(field)
	if !exists {
		return protocol.MakeNullBulkReply()
	}
	value := raw.([]byte)
	return protocol.MakeBulkReply(value)
}

func execHExists(db database.DbObjectIntf, args CmdLine) resp.ReplyIntf {
	key := string(args[0])
	field := string(args[1])
	dbObj := db.(*DbObject)
	dictObj, errReply := dbObj.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dictObj == nil {
		return protocol.MakeNullBulkReply()
	}
	_, exists := dictObj.Get(field)
	if exists {
		return protocol.MakeIntReply(int64(1))
	} else {
		return protocol.MakeIntReply(int64(0))
	}
}

func execHDel(db database.DbObjectIntf, args CmdLine) resp.ReplyIntf {
	key := string(args[0])
	fieldArgs := args[1:]
	fields := make([]string, len(args)-1)
	dbObj := db.(*DbObject)
	dictObj, errReply := dbObj.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dictObj == nil {
		return protocol.MakeNullBulkReply()
	}
	for i := range fields {
		fields[i] = string(fieldArgs[i])
	}
	delCount := 0
	for _, field := range fields {
		result := dictObj.Remove(field)
		delCount += result
	}
	if dictObj.Len() == 0 {
		dbObj.Remove(key)
	}
	if delCount > 0 {
		dbObj.addAof(utils.ToCmdLine3("HDel", args...))
	}
	return protocol.MakeIntReply(int64(delCount))
}

func execHLen(db database.DbObjectIntf, args CmdLine) resp.ReplyIntf {
	key := string(args[0])
	dbObj := db.(*DbObject)
	dictObj, errReply := dbObj.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dictObj == nil {
		return protocol.MakeIntReply(0)
	}
	return protocol.MakeIntReply(int64(dictObj.Len()))
}

func execHStrlen(db database.DbObjectIntf, args CmdLine) resp.ReplyIntf {
	key := string(args[0])
	field := string(args[1])
	dbObj := db.(*DbObject)
	dictObj, errReply := dbObj.getAsDict(key)
	if errReply != nil {
		return errReply
	}
	if dictObj == nil {
		return protocol.MakeIntReply(0)
	}
	raw, exists := dictObj.Get(field)
	if !exists {
		return protocol.MakeIntReply(0)
	}
	value := raw.([]byte)
	return protocol.MakeIntReply(int64(len(value)))
}

func init() {
	RegisterCommand("HSet", execHSet, writeFirstKey, 4)      // HSet key field value
	RegisterCommand("HSetNX", execHSetNx, writeFirstKey, 4)  // HSetNx key field value
	RegisterCommand("HExists", execHExists, readFirstKey, 3) // HExists key field
	RegisterCommand("HGet", execHGet, readFirstKey, 3)       // HGet key field
	RegisterCommand("HDel", execHDel, writeFirstKey, -3)     // HDel key field1 ...
	RegisterCommand("HLen", execHLen, readFirstKey, 2)       // HLen key
	RegisterCommand("HStrlen", execHStrlen, readFirstKey, 3) // HStrlen key field
}
