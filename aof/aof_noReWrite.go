package aof

import (
	"io"
	"memgo/config"
	"memgo/interface/database"
	"memgo/logger"
	"memgo/redis/RESP/connection"
	"memgo/redis/RESP/parser"
	"memgo/redis/RESP/protocol"
	"memgo/utils"
	"os"
	"strconv"
	"strings"
)

// AofHandlerWithoutReWrite NODE without rewrite
type AofHandlerWithoutReWrite struct {
	dbServer    database.DBEngine
	aofChan     chan *payload
	aofFile     *os.File
	aofFilename string
	currentDB   int
}

func NewAofHandlerWithoutReWrite(dbEngine database.DBEngine) (*AofHandlerWithoutReWrite, error) {
	handler := &AofHandlerWithoutReWrite{
		dbServer:    dbEngine,
		aofChan:     nil,
		aofFile:     nil,
		aofFilename: "",
		currentDB:   0,
	}
	handler.aofFilename = config.Properties.AppendFilename
	// XXX
	logger.Info(config.Properties.AppendFilename)

	handler.loadAof()

	aofFile, err := os.OpenFile(handler.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	handler.aofFile = aofFile
	handler.aofChan = make(chan *payload, aofQueueSize)
	// 启一个协程 将aof缓冲区中的cmd 落盘
	go func() {
		handler.handleAof()
	}()
	return handler, nil
}

func (handler *AofHandlerWithoutReWrite) SaveCmdLine(dbIndex int, cmdLine CmdLine) {
	if handler.aofChan == nil {
		return
	}
	handler.aofChan <- &payload{
		cmdLine: cmdLine,
		dbIndex: dbIndex,
	}
}

func (handler *AofHandlerWithoutReWrite) handleAof() {
	for p := range handler.aofChan {
		if p.dbIndex != handler.currentDB {
			selectCmd := utils.ToCmdLine("SELECT", strconv.Itoa(p.dbIndex))
			data := protocol.MakeMultiBulkReply(selectCmd).ToBytes()
			_, err := handler.aofFile.Write(data)
			if err != nil {
				logger.Warn(err)
				// 略过此处命令cmd
				continue
			}
			handler.currentDB = p.dbIndex
		}
		data := protocol.MakeMultiBulkReply(p.cmdLine).ToBytes()
		_, err := handler.aofFile.Write(data)
		if err != nil {
			logger.Warn(err)
		}
	}
}

func (handler *AofHandlerWithoutReWrite) loadAof() {
	// TODO memgo启动的时候，加载之前的aof文件
	// aof文件是按照resp协议编码的，所以我们直接解析即可
	file, err := os.Open(handler.aofFilename)
	if err != nil {
		if _, ok := err.(*os.PathError); ok {
			return
		}
		logger.Error(err)
		return
	}
	defer file.Close()

	var reader io.Reader
	reader = file
	ch := parser.ParseStream(reader)

	// NODE 一个伪连接
	fakeConn := &connection.Connection{}
	for p := range ch {
		// 1. payload中err不为nil =》 错误处理
		if p.Err != nil {
			// 客户端断开连接
			if p.Err == io.EOF ||
				p.Err == io.ErrUnexpectedEOF ||
				strings.Contains(p.Err.Error(), "use of closed network connection") {
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
		execResultReply := handler.dbServer.Exec(fakeConn, mbReply.Args)
		// XXX
		logger.Info(string(execResultReply.ToBytes()))
		if protocol.IsErrorReply(execResultReply) {
			logger.Error("exec error in <load aof>: " + string(execResultReply.ToBytes()))
		}

		// NODE select命令还需要更改 handler中的 currentDB
		if strings.ToLower(string(mbReply.Args[0])) == "select" {
			dbIndex, err := strconv.Atoi(string(mbReply.Args[1]))
			if err == nil {
				handler.currentDB = dbIndex
				fakeConn.SelectDB(dbIndex)
			}
		}

	}
}
