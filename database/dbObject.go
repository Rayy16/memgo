// NODE 该DbObject实现了 ttl、以及涉及多个Keys的操作并发安全 eg: lrange
// 与 redis的单线程引擎不同; memgo的存储引擎是并发的
// 优势: 实现ttl直接启协成进行监听，很容易实现定期删除
// 劣势: 对多个keys进行并发操作时, 为了保证并发安全 需要加锁
// NODE
// 目前实现的 存储核心 dictIntf 是由 sync.Map 实现的,
// 而对多个keys的并发安全操作需要对整个dict上锁, 锁粒度太大, 性能太低 =》
// 该方法的问题是 SingleKey操作 与 MultiKeys操作 类似于 read、write操作
// 改为 locker_map 将 key 哈希到不同的槽中，每个槽中配了一把锁, 来降低粒度

package database

import (
	"memgo/database/locker"
	"memgo/datastruct/dict"
	"memgo/interface/database"
	lockerIntf "memgo/interface/locker"
	"memgo/interface/resp"
	"memgo/logger"
	"memgo/redis/RESP/protocol"
	"memgo/utils/timewheel"
	"strings"
	"time"
)

const (
	dataDictSize = 1 << 16
	ttlDictSize  = 1 << 10
	lockerSize   = 1024
)

// DbObject TODO 已经保证了对单个key操作的并发安全; 需要保证对多个key操作的并发安全
type DbObject struct {
	index int
	// dict.DictIntf 是保证对它操作的并发安全的
	data   dict.DictIntf
	ttlMap dict.DictIntf
	// 使用locker来保证对多个keys操作是并发安全的
	locker lockerIntf.LockerIntf
	// 将增删改操作追加到aof文件中
	// NODE 初始化时必须不为nil, 否则loadAof时会error
	addAof func(CmdLine)
}

// MakeDbObject 使用ConcurrentDict
//func MakeDbObject() *DbObject {
//	return &DbObject{
//		index:  0,
//		data:   dict.MakeConcurrentDict(dataDictSize),
//		ttlMap: dict.MakeConcurrentDict(ttlDictSize),
//		locker: locker.MakeSegMentedLocker(lockerSize),
//		addAof: func(CmdLine) {},
//	}
//}

// MakeDbObject 使用SyncDict
func MakeDbObject() *DbObject {
	return &DbObject{
		index:  0,
		data:   dict.MakeSyncDict(),
		ttlMap: dict.MakeSyncDict(),
		locker: locker.MakeSegMentedLocker(lockerSize),
		addAof: func(CmdLine) {},
	}
}

// MakeDbObject 使用SimpleDict
//func MakeDbObject() *DbObject {
//	return &DbObject{
//		index:  0,
//		data:   dict.MakeSimpleDict(),
//		ttlMap: dict.MakeSimpleDict(),
//		locker: locker.MakeSegMentedLocker(lockerSize),
//		addAof: func(CmdLine) {},
//	}
//}

func (dbObj *DbObject) Exec(conn resp.ConnectionIntf, cmdLine CmdLine) resp.ReplyIntf {
	// TODO 事务命令

	// 普通命令, 执行不需要连接Conn
	return dbObj.execNormalCommand(cmdLine)
}

func (dbObj *DbObject) execNormalCommand(cmdLine CmdLine) resp.ReplyIntf {
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmd, ok := cmdTable[cmdName]
	// 合法性检验
	if !ok {
		return protocol.MakeErrReply("ERR unknown command")
	}
	if !validateArity(cmd.arity, cmdLine) {
		return protocol.MakeArgNumErrReply(cmdName)
	}
	// 需要区分是否为 MultiKeys Operation or SingleKey Operation
	fun := cmd.executor
	pre := cmd.prepare
	// SingleKey Operation
	writeKeys, readKeys := pre(cmdLine[1:])
	dbObj.Locks(writeKeys, readKeys)
	defer dbObj.UnLocks(writeKeys, readKeys)

	return fun(dbObj, cmdLine[1:])
}

// ======= TTL Function ======= //

func (dbObj *DbObject) Expire(key string, expireTime time.Time) {
	// 过期时间设置错误 直接过期
	if time.Now().After(expireTime) {
		dbObj.Remove(key)
		return
	}
	// 过期时间设置正常, 加入 ttlMap中 记录过期时间点, 加入时间轮中
	dbObj.ttlMap.Put(key, expireTime)
	taskKey := genExpireTask(key)
	timewheel.At(expireTime, taskKey, func() {
		dbObj.Lock(key)
		defer dbObj.UnLock(key)

		// check-lock-check, ttl may be updated during waiting lock
		logger.Info("expire " + key)
		rawExpireTime, exists := dbObj.ttlMap.Get(key)
		if !exists {
			return
		}
		ExpireTime, _ := rawExpireTime.(time.Time)
		IfExpired := time.Now().After(ExpireTime)
		// 惰性删除
		if IfExpired {
			dbObj.Remove(key)
		}
	})
}

func (dbObj *DbObject) IsExpire(key string) bool {
	rawExpireTime, exists := dbObj.ttlMap.Get(key)
	if !exists {
		return false
	}
	ExpireTime, _ := rawExpireTime.(time.Time)
	IfExpired := time.Now().After(ExpireTime)
	// 惰性删除
	if IfExpired {
		dbObj.Remove(key)
	}
	return IfExpired
}

func genExpireTask(key string) string {
	return "expire: " + key
}

// ======= locker Function ======= //

func (dbObj *DbObject) Locks(writeKeys []string, readKeys []string) {
	dbObj.locker.RWLocks(writeKeys, readKeys)
}

func (dbObj *DbObject) UnLocks(writeKeys []string, readKeys []string) {
	dbObj.locker.RWUnLocks(writeKeys, readKeys)
}

func (dbObj *DbObject) Lock(key string) {
	keys := []string{key}
	dbObj.Locks(keys, nil)
}

func (dbObj *DbObject) UnLock(key string) {
	keys := []string{key}
	dbObj.UnLocks(keys, nil)
}

// ======= Accept Data ======= //

// GetEntity 在go中，函数形参是空接口类型，那么传入参数时，自动发生类型转换; 而返回类型为空接口时，想要拿到所指向的具体类型，需要使用断言进行转换
func (dbObj *DbObject) GetEntity(key string) (*database.DataEntity, bool) {
	rawVal, ok := dbObj.data.Get(key)
	if !ok {
		return nil, false
	}
	// 惰性删除
	if dbObj.IsExpire(key) {
		return nil, false
	}
	entity, _ := rawVal.(*database.DataEntity)
	return entity, true
}

// PutEntity 在go中，函数形参是空接口类型，那么传入参数时，自动发生类型转换; 而返回类型为空接口时，想要拿到所指向的具体类型，需要使用断言进行转换
func (dbObj *DbObject) PutEntity(key string, entity *database.DataEntity) int {
	return dbObj.data.Put(key, entity)
}

func (dbObj *DbObject) PutIfExists(key string, entity *database.DataEntity) int {
	return dbObj.data.PutIfExists(key, entity)
}

func (dbObj *DbObject) PutIfAbsent(key string, entity *database.DataEntity) int {
	return dbObj.data.PutIfAbsent(key, entity)
}

func (dbObj *DbObject) Remove(key string) {
	// TODO 看是否需要将以下两个操作原子
	dbObj.data.Remove(key)
	dbObj.ttlMap.Remove(key)
	taskKey := genExpireTask(key)
	timewheel.Cancel(taskKey)
}

func (dbObj *DbObject) Removes(keys ...string) int {
	res := 0
	for _, key := range keys {
		_, ok := dbObj.data.Get(key)
		if ok {
			dbObj.Remove(key)
			res++
		}
	}
	return res
}

func (dbObj *DbObject) Flush() {
	dbObj.data.Clear()
}

// ForEach DbObject层面的 ForEach实际上是根据 key value去ttlMap中 取出过期时间, 然后调用回调函数entity2reply
func (dbObj *DbObject) ForEach(entity2reply func(key string, entity *database.DataEntity, expireAt *time.Time) bool) {
	dbObj.data.ForEach(func(key string, raw interface{}) bool {
		entity, _ := raw.(*database.DataEntity)
		// 创建空指针
		var expireAt *time.Time
		rawExpireTime, ok := dbObj.ttlMap.Get(key)
		if ok {
			expireTime, _ := rawExpireTime.(time.Time)
			expireAt = &expireTime
		}
		return entity2reply(key, entity, expireAt)
	})
}
