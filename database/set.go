package database

import (
	"memgo/datastruct/set"
	"memgo/interface/database"
	"memgo/interface/resp"
	"memgo/redis/RESP/protocol"
	"strconv"
)

func (db *DbObject) getAsSet(key string) (*set.Set, resp.ReplyIntf) {
	entity, exists := db.GetEntity(key)
	if !exists {
		return nil, nil
	}
	setObj, ok := entity.Data.(*set.Set)
	if !ok {
		return nil, &protocol.WrongTypeErrReply{}
	}
	return setObj, nil
}

func (db *DbObject) getOrInitSet(key string) (*set.Set, bool, resp.ReplyIntf) {
	setObj, err := db.getAsSet(key)
	if err != nil {
		return nil, false, err
	}
	inited := false
	if setObj == nil {
		setObj = set.MakeSet()
		db.PutEntity(key, &database.DataEntity{Data: setObj})
		inited = true
	}
	return setObj, inited, nil
}

func execSAdd(db database.DbObjectIntf, args CmdLine) resp.ReplyIntf {
	key := string(args[0])
	member := string(args[1])
	dbObj, _ := db.(*DbObject)
	setObj, _, err := dbObj.getOrInitSet(key)
	if err != nil {
		return err
	}
	return protocol.MakeIntReply(int64(setObj.Add(member)))
}

func execSIsMember(db database.DbObjectIntf, args CmdLine) resp.ReplyIntf {
	key := string(args[0])
	member := string(args[1])
	DbObj, _ := db.(*DbObject)
	setObj, err := DbObj.getAsSet(key)
	if err != nil {
		return err
	}
	if setObj == nil || !setObj.IsMember(member) {
		return protocol.MakeIntReply(0)
	}
	return protocol.MakeIntReply(1)
}

func execSPop(db database.DbObjectIntf, args CmdLine) resp.ReplyIntf {
	key := string(args[0])
	DbObj, _ := db.(*DbObject)
	setObj, err := DbObj.getAsSet(key)
	if err != nil {
		return err
	}
	if setObj == nil || setObj.Len() == 0 {
		return protocol.MakeNullBulkReply()
	}
	res := setObj.RandomMembers(1)
	setObj.Remove(res[0])
	return protocol.MakeBulkReply([]byte(res[0]))
}

func execSRandMember(db database.DbObjectIntf, args CmdLine) resp.ReplyIntf {
	key := string(args[0])
	DbObj, _ := db.(*DbObject)
	setObj, errReply := DbObj.getAsSet(key)
	if errReply != nil {
		return errReply
	}
	if setObj == nil || setObj.Len() == 0 {
		return protocol.MakeNullBulkReply()
	}

	if len(args) == 1 {
		res := setObj.RandomMembers(1)
		return protocol.MakeBulkReply([]byte(res[0]))
	}

	countStr := string(args[1])
	count, err := strconv.ParseInt(countStr, 10, 64)
	if err != nil {
		return protocol.MakeErrReply(err.Error())
	}
	
	if count > 0 {
		res := make([][]byte, int(count))
		for i, mem := range setObj.RandomDistinctMembers(int(count)) {
			res[i] = []byte(mem)
		}
		return protocol.MakeMultiBulkReply(res)
	} else if count < 0 {
		res := make([][]byte, int(-count))
		for i, mem := range setObj.RandomMembers(int(-count)) {
			res[i] = []byte(mem)
		}
		return protocol.MakeMultiBulkReply(res)
	} else {
		return protocol.MakeEmptyMultiBulkReply()
	}
}

func execSRem(db database.DbObjectIntf, args CmdLine) resp.ReplyIntf {
	key := string(args[0])
	mems := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		mems[i-1] = string(args[i])
	}
	DbObj, _ := db.(*DbObject)
	setObj, err := DbObj.getAsSet(key)
	if err != nil {
		return err
	}
	if setObj == nil {
		return protocol.MakeIntReply(0)
	}
	counter := 0
	for _, mem := range mems {
		counter += setObj.Remove(mem)
	}
	if setObj.Len() == 0 {
		DbObj.Remove(key)
	}
	return protocol.MakeIntReply(int64(counter))
}

func execSInter(db database.DbObjectIntf, args CmdLine) resp.ReplyIntf {
	DbObj, _ := db.(*DbObject)
	keys := make([]string, len(args))
	for i := range args {
		keys[i] = string(args[i])
	}

	var res *set.Set
	for _, key := range keys {
		setObj, errReply := DbObj.getAsSet(key)
		if errReply != nil {
			return errReply
		}
		if setObj == nil {
			return protocol.MakeNullBulkReply()
		}
		if res == nil {
			res = set.MakeSet(setObj.ToSlice()...)
		} else {
			res = res.Intersect(setObj)
		}
	}
	resStrs := res.ToSlice()
	bytes := make([][]byte, len(resStrs))
	for i, str := range resStrs {
		bytes[i] = []byte(str)
	}
	if len(bytes) == 0 {
		return protocol.MakeEmptyMultiBulkReply()
	}
	return protocol.MakeMultiBulkReply(bytes)
}

func execSUnion(db database.DbObjectIntf, args CmdLine) resp.ReplyIntf {
	DbObj, _ := db.(*DbObject)
	keys := make([]string, len(args))
	for i := range args {
		keys[i] = string(args[i])
	}

	var res *set.Set
	for _, key := range keys {
		setObj, errReply := DbObj.getAsSet(key)
		if errReply != nil {
			return errReply
		}
		if setObj == nil {
			continue
		}
		if res == nil {
			res = set.MakeSet(setObj.ToSlice()...)
		}
		res = res.Union(setObj)
	}

	resStrs := res.ToSlice()
	bytes := make([][]byte, len(resStrs))
	for i, str := range resStrs {
		bytes[i] = []byte(str)
	}
	if len(bytes) == 0 {
		return protocol.MakeEmptyMultiBulkReply()
	}
	return protocol.MakeMultiBulkReply(bytes)
}

func execSDiff(db database.DbObjectIntf, args CmdLine) resp.ReplyIntf {
	DbObj, _ := db.(*DbObject)
	keys := make([]string, len(args))
	for i := range args {
		keys[i] = string(args[i])
	}

	var res *set.Set
	for i, key := range keys {
		setObj, errReply := DbObj.getAsSet(key)
		if errReply != nil {
			return errReply
		}
		if setObj == nil {
			if i == 0 {
				return protocol.MakeNullBulkReply()
			}
			continue
		}
		if res == nil {
			res = set.MakeSet(setObj.ToSlice()...)
			continue
		}
		res = res.Diff(setObj)
		if res.Len() == 0 {
			return protocol.MakeNullBulkReply()
		}
	}

	resStrs := res.ToSlice()
	bytes := make([][]byte, len(resStrs))
	for i, str := range resStrs {
		bytes[i] = []byte(str)
	}
	if len(bytes) == 0 {
		return protocol.MakeEmptyMultiBulkReply()
	}
	return protocol.MakeMultiBulkReply(bytes)
}

func init() {
	RegisterCommand("SAdd", execSAdd, writeFirstKey, 3)               // SAdd s1 k1
	RegisterCommand("SIsMember", execSIsMember, readFirstKey, 3)      // SIsMember s1 k1
	RegisterCommand("SPop", execSPop, writeFirstKey, 2)               // SPop s1
	RegisterCommand("SRandMember", execSRandMember, readFirstKey, -2) // SRandMember s1 [count]
	RegisterCommand("SRem", execSRem, writeFirstKey, -3)              // SRem s1 mem1 mem2 mem3
	RegisterCommand("SInter", execSInter, readAllKeys, -3)            // SInter s1 s2 s3...
	RegisterCommand("SUnion", execSUnion, readAllKeys, -3)
	RegisterCommand("SDiff", execSDiff, readAllKeys, -3)
}
