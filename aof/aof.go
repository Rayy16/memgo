package aof

import (
	"context"
	"io"
	"memgo/interface/database"
	"memgo/logger"
	"memgo/redis/RESP/connection"
	"memgo/redis/RESP/parser"
	"memgo/redis/RESP/protocol"
	"memgo/utils"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type CmdLine = [][]byte

const (
	aofQueueSize = 1 << 16

	FsyncAlways = "always"

	FsyncEverySec = "everysec"

	FsyncNo = "no"
)

type payload struct {
	cmdLine CmdLine
	dbIndex int
}

// Persister
type Persister struct {
	// 回收协程
	ctx    context.Context
	cancel context.CancelFunc
	// 用于aof重写时生成 临时DB svr
	tmpDBsvrMaker func() database.DBEngine

	dbServer    database.DBEngine
	aofChan     chan *payload
	aofFile     *os.File
	aofFinished chan struct{}
	aofFilename string
	currentDB   int

	bufSize    int64
	aofFsync   string     // 刷盘策略
	pausingAof sync.Mutex // 暂停 Aof
	// 刷盘策略
	// =》 fsyncEverySecond 启协程 内含 ticker
	// =》 fsyncAlways 每次将 从 aofChan读出时刷盘
}

func NewPersister(dbEngine database.DBEngine, load bool, filename, fsync string, tmpDBsvrMaker func() database.DBEngine) (*Persister, error) {
	handler := &Persister{
		ctx:           nil,
		cancel:        nil,
		tmpDBsvrMaker: tmpDBsvrMaker,
		dbServer:      dbEngine,
		aofChan:       make(chan *payload, aofQueueSize),
		aofFile:       nil,
		aofFinished:   make(chan struct{}),
		aofFilename:   filename,
		currentDB:     0,
		bufSize:       0,
		aofFsync:      strings.ToLower(fsync),
		pausingAof:    sync.Mutex{},
	}
	if load {
		handler.loadAof(-1)
	}
	aofFile, err := os.OpenFile(handler.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	handler.aofFile = aofFile
	ctx, cancel := context.WithCancel(context.Background())
	handler.ctx, handler.cancel = ctx, cancel
	// 启一个协程 监听命令
	go func() {
		handler.listenCmd()
	}()

	if handler.aofFsync == FsyncEverySec {
		handler.fsyncEverySec()
	}

	return handler, nil
}

func (persister *Persister) listenCmd() {
	for p := range persister.aofChan {
		persister.writeAof(p)
	}
	persister.aofFinished <- struct{}{}
}

// SaveCmdLine 提供给 dbObject 的 addAof 回调方法
func (persister *Persister) SaveCmdLine(dbIndex int, cmdLine CmdLine) {
	if persister.aofChan == nil {
		return
	}
	p := &payload{
		cmdLine: cmdLine,
		dbIndex: dbIndex,
	}
	if persister.aofFsync == FsyncAlways {
		persister.writeAof(p)
		return
	}
	persister.aofChan <- p
}

func (persister *Persister) writeAof(p *payload) {
	persister.pausingAof.Lock()
	defer persister.pausingAof.Unlock()

	if p.dbIndex != persister.currentDB {
		selectCmd := utils.ToCmdLine("select", strconv.Itoa(p.dbIndex))
		data := protocol.MakeMultiBulkReply(selectCmd).ToBytes()
		_, err := persister.aofFile.Write(data)
		if err != nil {
			logger.Warn("write aofFile fail: ", err)
			return
		}

		persister.currentDB = p.dbIndex
	}
	data := protocol.MakeMultiBulkReply(p.cmdLine).ToBytes()
	_, err := persister.aofFile.Write(data)
	if err != nil {
		logger.Warn("write aofFile fail: ", err)
	}
	if persister.aofFsync == FsyncAlways {
		err := persister.aofFile.Sync()
		if err != nil {
			logger.Warn("aofFile Sync fail: ", err)
		}
	}
}

func (persister *Persister) fsyncEverySec() {
	ticker := time.NewTicker(time.Second)
	go func() {
		for {
			select {
			case <-ticker.C:
				persister.pausingAof.Lock()
				if err := persister.aofFile.Sync(); err != nil {
					logger.Error("fsync failed: ", err)
				}
				persister.pausingAof.Unlock()
			case <-persister.ctx.Done():
				return
			}
		}
	}()
}

func (persister *Persister) Close() {
	if persister.aofFile != nil {
		close(persister.aofChan)
		<-persister.aofFinished

		err := persister.aofFile.Close()
		if err != nil {
			logger.Warn("aof close err: ", err)
		}
	}
	persister.cancel()
}

func (persister *Persister) loadAof(limit int64) {
	// NODE 由于dbServer 运行时可能调用 loadAof 此时 dbServer里的每个 dbObj已经 和 addAof 绑定
	// NODE 防止  loadAof 时执行的命令 又写入 aofFile中
	aofChan := persister.aofChan
	persister.aofChan = nil
	defer func(aofChan chan *payload) {
		persister.aofChan = aofChan
	}(aofChan)

	file, err := os.Open(persister.aofFilename)
	if err != nil {
		if _, ok := err.(*os.PathError); ok {
			return
		}
		logger.Error(err)
		return
	}
	defer file.Close()

	var reader io.Reader
	if limit > 0 {
		reader = io.LimitReader(file, limit)
	} else {
		reader = file
	}
	// 解析 aof文件
	ch := parser.ParseStream(reader)

	// NODE 一个伪连接
	fakeConn := &connection.Connection{}
	for p := range ch {
		// 1. payload中err不为nil =》 错误处理
		if p.Err != nil {
			// 客户端断开连接
			if p.Err == io.EOF {
				return
			}
			logger.Error("parse error: " + p.Err.Error())
			continue
		}

		// 2. payload中err为nil =》 db.Exec()
		if p.Data == nil {
			// 解析出来的数据是空的
			logger.Error("empty payload")
			continue
		}
		// 客户端传输的命令必须实现了 MultiBulkReply类型
		mbReply, ok := p.Data.(*protocol.MultiBulkReply)
		if !ok {
			logger.Error("require multi bulk <load aof>: " + string(p.Data.ToBytes()))
			continue
		}

		// NODE 执行命令
		execResultReply := persister.dbServer.Exec(fakeConn, mbReply.Args)

		if protocol.IsErrorReply(execResultReply) {
			logger.Error("exec error in <load aof>: " + string(execResultReply.ToBytes()))
		}

		// NODE select命令还需要更改 handler中的 currentDB
		if strings.ToLower(string(mbReply.Args[0])) == "select" {
			dbIndex, err := strconv.Atoi(string(mbReply.Args[1]))
			if err == nil {
				persister.currentDB = dbIndex
				fakeConn.SelectDB(dbIndex)
			}
		}

	}
}
