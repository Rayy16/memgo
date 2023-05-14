package aof

import (
	"io"
	"memgo/config"
	"memgo/interface/database"
	"memgo/logger"
	"memgo/redis/RESP/protocol"
	"memgo/utils"
	"os"
	"strconv"
	"time"
)

// rewrite逻辑
// 1.暂停aof持久化, 设置进行重写准备工作(生成 ReWriteCtx), 恢复aof持久化
// 2.通过 tmpDBsvrMaker 生成 tmpDBsvr, 重放 aofFile 生成 DB副本 NODE 不直接拷贝是因为防止阻塞
// 3.根据 tmpDBsvr中的数据快照, 生成 set命令 的 resp报文（重写aof文件）
// 4.暂停aof持久化 将开始重写后的 新追加的aof文件 追加到 aof重写文件后; 替换aof文件 恢复aof持久化

type RewriteCtx struct {
	tmpFile     *os.File // aof重写文件
	filePointer int64    // 记录开始aof重写时 aof文件写到哪里了
	dbIdx       int      // 记录开始aof重写时的 dbidx
}

func (persister *Persister) newReWriteHandler() *Persister {
	handler := &Persister{}
	handler.aofFilename = persister.aofFilename
	handler.dbServer = persister.tmpDBsvrMaker()
	return handler
}

func (persister *Persister) ReWrite() error {
	ctx, err := persister.prepReWrite()
	if err != nil {
		return err
	}
	err = persister.genTmpDBsvrAndReplayAof(ctx)
	if err != nil {
		return err
	}
	persister.appendTmpAof(ctx)
	return nil
}

func (persister *Persister) prepReWrite() (*RewriteCtx, error) {
	persister.pausingAof.Lock()
	defer persister.pausingAof.Unlock()

	// 先将没刷盘的aof文件刷盘
	err := persister.aofFile.Sync()
	if err != nil {
		logger.Error("fsync failed: ", err)
		return nil, err
	}

	fileInfo, _ := os.Stat(persister.aofFilename)
	filePointer := fileInfo.Size()

	file, err := os.CreateTemp("./", "*.aof")
	if err != nil {
		logger.Error("tmp file create failed")
		return nil, err
	}
	return &RewriteCtx{
		tmpFile:     file,
		filePointer: filePointer,
		dbIdx:       persister.currentDB,
	}, nil
}

func (persister *Persister) genTmpDBsvrAndReplayAof(ctx *RewriteCtx) error {
	tmpAofHandler := persister.newReWriteHandler()
	tmpAofHandler.loadAof(ctx.filePointer)

	// NODE replay AOF
	for i := 0; i < config.Properties.Databases; i++ {
		data := protocol.MakeMultiBulkReply(utils.ToCmdLine("select", strconv.Itoa(i))).ToBytes()
		_, err := ctx.tmpFile.Write(data)
		if err != nil {
			return err
		}
		// dump db
		// aof重写的逻辑并不是扫描原Aof文件中的key，并将其合并
		// 而是通过 aof重写前的 aof文件，进行重放，随后对重放之后的 db里的数据，挨个生成set命令即可
		tmpAofHandler.dbServer.ForEach(i, func(key string, entity *database.DataEntity, expireAt *time.Time) bool {
			cmd := utils.EntityToCmd(key, entity)
			if cmd != nil {
				_, _ = ctx.tmpFile.Write(cmd.ToBytes())
			}
			if expireAt != nil {
				cmd := utils.MakeExpireCmd(key, *expireAt)
				if cmd != nil {
					_, _ = ctx.tmpFile.Write(cmd.ToBytes())
				}
			}
			return true
		})
	}
	return nil
}

func (persister *Persister) appendTmpAof(ctx *RewriteCtx) {
	persister.pausingAof.Lock()
	defer persister.pausingAof.Unlock()

	srcAof, err := os.Open(persister.aofFilename)
	if err != nil {
		logger.Error("open aofFile fail: ", err)
		return
	}
	defer srcAof.Close()

	_, err = srcAof.Seek(ctx.filePointer, 0)
	if err != nil {
		logger.Error("seek aofFile fail: ", err)
	}
	// 同步 aof重写时的 dbidx
	data := protocol.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(ctx.dbIdx))).ToBytes()
	_, err = ctx.tmpFile.Write(data)
	if err != nil {
		logger.Error("tmp file rewrite failed: " + err.Error())
		return
	}

	_, err = io.Copy(ctx.tmpFile, srcAof)
	if err != nil {
		logger.Error("copy aof file to Rewrite aof file error: ", err)
	}
	tmpFileName := ctx.tmpFile.Name()
	_ = ctx.tmpFile.Close()
	_ = persister.aofFile.Close()
	if err := os.Rename(tmpFileName, persister.aofFilename); err != nil {
		logger.Info("<appendTmpAof> rename:", err.Error())
	}

	// 重新打开文件
	aofFile, err := os.OpenFile(persister.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		panic("aofFile rewrite over, try open new aof file failed: " + err.Error())
	}
	persister.aofFile = aofFile

	// 同步 aof重写完成时的 dbidx
	data = protocol.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(persister.currentDB))).ToBytes()
	_, err = persister.aofFile.Write(data)
	if err != nil {
		panic("aofFile rewrite over, write new aof file failed: " + err.Error())
	}
}
