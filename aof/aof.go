package aof

import (
	"context"
	"goRedis/config"
	databaseface "goRedis/interface/database"
	"goRedis/lib/logger"
	"goRedis/lib/utils"
	"goRedis/resp/connection"
	"goRedis/resp/parser"
	"goRedis/resp/reply"
	"io"
	"os"
	"strconv"
	"sync"
	"time"
)

// CmdLine is alias for [][]byte, represents a command line
type CmdLine = [][]byte

const (
	aofQueueSize = 1 << 16
)

type payload struct {
	cmdLine CmdLine
	dbIndex int
}

// Listener will be called-back after receiving a aof payload
// with a listener we can forward the updates to slave nodes etc.
type Listener interface {
	// Callback will be called-back after receiving a aof payload
	Callback([]CmdLine)
}

// AofHandler receive msgs from channel and write to AOF file
type AofHandler struct {
	ctx         context.Context
	cancel      context.CancelFunc
	db          databaseface.Database
	aofChan     chan *payload
	aofFile     *os.File
	aofFilename string
	currentDB   int
	// aof goroutine will send msg to main goroutine through this channel when aof tasks finished and ready to shut down
	aofFinished chan struct{}
	// pause aof for start/finish aof rewrite progress
	pausingAof sync.Mutex
	listeners  map[Listener]struct{}
	// reuse cmdLine buffer
	buffer []CmdLine
}

// NewAOFHandler creates a new aof.AofHandler
func NewAOFHandler(db databaseface.Database) (*AofHandler, error) {
	handler := &AofHandler{}
	handler.aofFilename = config.Properties.AppendFilename
	handler.db = db
	handler.LoadAof(0)
	aofFile, err := os.OpenFile(handler.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	handler.aofFile = aofFile
	handler.aofChan = make(chan *payload, aofQueueSize)
	handler.aofFinished = make(chan struct{})
	handler.listeners = make(map[Listener]struct{})
	// start aof goroutine to write aof file in background and fsync periodically if needed (see fsyncEverySecond)
	go func() {
		handler.listenCmd()
	}()
	ctx, cancel := context.WithCancel(context.Background())
	handler.ctx = ctx
	handler.cancel = cancel
	// 现在我们配置的就是是everysec，和redis的默认配置相同
	if config.Properties.AppendFsync == "everysec" {
		handler.fsyncEverySecond()
	}
	return handler, nil
}

// AddAof send command to aof goroutine through channel
func (handlerAof *AofHandler) AddAof(dbIndex int, cmdLine CmdLine) {
	if config.Properties.AppendOnly && handlerAof.aofChan != nil {
		handlerAof.aofChan <- &payload{
			cmdLine: cmdLine,
			dbIndex: dbIndex,
		}
	}
}

// listenCmd listen aof channel and write into file
func (handlerAof *AofHandler) listenCmd() {
	for p := range handlerAof.aofChan {
		handlerAof.writeAof(p)
	}
	handlerAof.aofFinished <- struct{}{}
}

// writeAof write payload to file from aofChan
func (handlerAof *AofHandler) writeAof(p *payload) {
	// 清空数组内容，但是复用数组空间，防止频繁的创建数组
	handlerAof.buffer = handlerAof.buffer[:0] // reuse underlying array
	handlerAof.pausingAof.Lock()              // prevent other goroutines from pausing aof
	defer handlerAof.pausingAof.Unlock()
	// ensure aof is in the right database
	if p.dbIndex != handlerAof.currentDB {
		// select db
		selectCmd := utils.ToCmdLine("SELECT", strconv.Itoa(p.dbIndex))
		handlerAof.buffer = append(handlerAof.buffer, selectCmd)
		data := reply.MakeMultiBulkReply(selectCmd).ToBytes()
		_, err := handlerAof.aofFile.Write(data)
		if err != nil {
			logger.Warn(err)
			return
		}
		handlerAof.currentDB = p.dbIndex
	}
	// 没有切换数据库的情况
	data := reply.MakeMultiBulkReply(p.cmdLine).ToBytes()
	handlerAof.buffer = append(handlerAof.buffer, p.cmdLine)
	_, err := handlerAof.aofFile.Write(data)
	if err != nil {
		logger.Warn(err)
	}
	// 回调函数还没写
	//for listener := range handlerAof.listeners {
	//	listener.Callback(handlerAof.buffer)
	//}
	if config.Properties.AppendFsync == "always" {
		_ = handlerAof.aofFile.Sync() // 如果是always，那就一直刷新
	}
}

// LoadAof read aof file
func (handlerAof *AofHandler) LoadAof(maxBytes int) {

	file, err := os.Open(handlerAof.aofFilename)
	if err != nil {
		logger.Warn(err)
		return
	}
	defer file.Close()
	ch := parser.ParseStream(file)
	fakeConn := &connection.Connection{} // only used for save dbIndex
	for p := range ch {
		if p.Err != nil {
			if p.Err == io.EOF {
				break
			}
			logger.Error("parse error: " + p.Err.Error())
			continue
		}
		if p.Data == nil {
			logger.Error("empty payload")
			continue
		}
		r, ok := p.Data.(*reply.MultiBulkReply)
		if !ok {
			logger.Error("require multi bulk reply")
			continue
		}
		ret := handlerAof.db.Exec(fakeConn, r.Args)
		if reply.IsErrorReply(ret) {
			logger.Error("exec err", err)
		}
	}
}

// Fsync flusher aof file to disk
func (handlerAof *AofHandler) Fsync() {
	handlerAof.pausingAof.Lock()
	if err := handlerAof.aofFile.Sync(); err != nil {
		logger.Errorf("fsync failed: %v", err)
	}
	handlerAof.pausingAof.Unlock()
}

// fsyncEverySecond fsync aof file every second
func (handlerAof *AofHandler) fsyncEverySecond() {
	// 定时器，每秒刷一次盘
	ticker := time.NewTicker(time.Second * 1)
	go func() {
		for {
			select {
			case <-ticker.C:
				handlerAof.Fsync()
			case <-handlerAof.ctx.Done():
				return
			}
		}
	}()
}

// generateAof 重新生成一个aof文件
