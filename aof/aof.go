package aof

import (
	"context"
	"goRedis/interface/database"
	"goRedis/lib/logger"
	"goRedis/lib/utils"
	"goRedis/resp/connection"
	"goRedis/resp/parser"
	"goRedis/resp/reply"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
)

// CmdLine is alias for [][]byte, represents a command line
type CmdLine = [][]byte

const (
	aofQueueSize = 1 << 16
	// FsyncAlways do fsync for every command
	FsyncAlways = "always"
	// FsyncEverySec do fsync every second
	FsyncEverySec = "everysec"
	// FsyncNo lets operating system decides when to do fsync
	FsyncNo = "no"
)

type payload struct {
	cmdLine CmdLine
	dbIndex int
	wg      *sync.WaitGroup
}

// Listener will be called-back after receiving a aof payload
// with a listener we can forward the updates to slave nodes etc.
type Listener interface {
	// Callback will be called-back after receiving a aof payload
	Callback([]CmdLine)
}

// Persister receive msgs from channel and write to AOF file
type Persister struct {
	ctx        context.Context
	cancel     context.CancelFunc
	db         database.Database
	tmpDBMaker func() database.Database
	// aofChan is the channel to receive aof payload(listenCmd will send payload to this channel)
	aofChan chan *payload
	// aofFile is the file handler of aof file
	aofFile *os.File
	// aofFilename is the path of aof file
	aofFilename string
	// aofFsync is the strategy of fsync
	aofFsync string
	// aof goroutine will send msg to main goroutine through this channel when aof tasks finished and ready to shut down
	aofFinished chan struct{}
	// pause aof for start/finish aof rewrite progress
	pausingAof sync.Mutex
	currentDB  int
	listeners  map[Listener]struct{}
	// reuse cmdLine buffer
	buffer []CmdLine
}

// NewPersister creates a new aof.Persister
func NewPersister(db database.Database, filename string, load bool, fsync string, tmpDBMaker func() database.Database) (*Persister, error) {
	persister := &Persister{}
	persister.aofFilename = filename
	persister.aofFsync = strings.ToLower(fsync)
	persister.db = db
	persister.tmpDBMaker = tmpDBMaker
	persister.currentDB = 0 // 默认初始都是0
	// load aof file if needed
	if load {
		persister.LoadAof(0)
	}
	// 打开文件，打开文件需要的权限：追加、创建、读写
	aofFile, err := os.OpenFile(persister.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	persister.aofFile = aofFile
	// 创建接收消息的缓冲区
	persister.aofChan = make(chan *payload, aofQueueSize)
	persister.aofFinished = make(chan struct{}) // 空结构体作为通知信号，通知主协程aof 任务结束
	persister.listeners = make(map[Listener]struct{})
	// start aof goroutine to write aof file in background and fsync periodically if needed (see fsyncEverySecond)
	go func() {
		persister.listenCmd()
	}()
	ctx, cancel := context.WithCancel(context.Background())
	persister.ctx = ctx
	persister.cancel = cancel
	// fsync every second if needed
	if persister.aofFsync == FsyncEverySec {
		persister.fsyncEverySecond()
	}
	return persister, nil
}

// RemoveListener removes a listener from aof handler, so wen can close the listener
func (persister *Persister) RemoveListener(listener Listener) {
	persister.pausingAof.Lock()
	defer persister.pausingAof.Unlock()
	delete(persister.listeners, listener)
}

// SaveCmdLine send command to aof goroutine through channel
func (persister *Persister) SaveCmdLine(dbIndex int, cmdLine CmdLine) {
	// aofChan will be set as nil temporarily during load aof see Persister.LoadAof
	if persister.aofChan == nil {
		return
	}
	if persister.aofFsync == FsyncAlways {
		// aof 落盘时机选择为always
		p := &payload{
			cmdLine: cmdLine,
			dbIndex: dbIndex,
		}
		persister.writeAof(p)
		return
	}
	// 落盘时机不是always,先写入缓冲区
	persister.aofChan <- &payload{
		cmdLine: cmdLine,
		dbIndex: dbIndex,
	}
}

// listenCmd listen aof channel and write into file
func (persister *Persister) listenCmd() {
	for p := range persister.aofChan {
		persister.writeAof(p)
	}
	persister.aofFinished <- struct{}{}
}

func (persister *Persister) writeAof(p *payload) {
	//清空切片并重用底层数组，以减少内存分配次数，提高性能。但在重用底层数组之前，需要确保底层数组中的数据已经不再需要。
	persister.buffer = persister.buffer[:0] // reuse underlying array
	// 主要是为了防止这个时候进行aof重写
	persister.pausingAof.Lock() // prevent other goroutines from pausing aof
	defer persister.pausingAof.Unlock()
	// ensure aof is in the right database
	if p.dbIndex != persister.currentDB {
		// 写的时候如果当前数据库和要写的数据不一样，需要添加一条切换数据库的语句 select x
		selectCmd := utils.ToCmdLine("SELECT", strconv.Itoa(p.dbIndex))
		persister.buffer = append(persister.buffer, selectCmd)
		data := reply.MakeMultiBulkReply(selectCmd).ToBytes() // 把select x 这条命令封装成resp协议的格式
		_, err := persister.aofFile.Write(data)               // 写入文件
		if err != nil {                                       // 写入失败不会终止程序，跳过这条命令
			logger.Warn(err)
			return // skip this command
		}
		// 当前数据库记录一下
		persister.currentDB = p.dbIndex
	}
	// 数据库没有切换的情况下
	data := reply.MakeMultiBulkReply(p.cmdLine).ToBytes()
	persister.buffer = append(persister.buffer, p.cmdLine)
	_, err := persister.aofFile.Write(data)
	if err != nil {
		logger.Warn(err)
	}
	for listener := range persister.listeners {
		listener.Callback(persister.buffer)
	}
	if persister.aofFsync == FsyncAlways {
		// 将数据刷入磁盘
		_ = persister.aofFile.Sync()
	}
}

// LoadAof read aof file, cna only be used before Persister.listenCmd started
func (persister *Persister) LoadAof(maxBytes int) {
	// persister.db.Exec may call persister.AddAof
	// delete aofChan to prevent loaded commands back into aofChan
	aofChan := persister.aofChan
	persister.aofChan = nil             // 设置为nil，防止继续写入
	defer func(aofChan chan *payload) { // 结束load之后，恢复aofChan
		persister.aofChan = aofChan
	}(aofChan)

	file, err := os.Open(persister.aofFilename)
	if err != nil {
		if _, ok := err.(*os.PathError); ok {
			return
		}
		logger.Warn(err)
		return
	}
	defer file.Close()

	// load rdb preamble if needed
	decoder := rdb.newDecoder(file)
	err = persister.db.LoadRDB(decoder)
	if err != nil {
		// no rdb preamble,所以将文件的读写文职定位到文件的开头
		file.Seek(0, io.SeekStart)
	} else {
		// has rdb preamble
		_, _ = file.Seek(int64(decoder.GetReadCount())+1, io.SeekStart)
		maxBytes = maxBytes - decoder.GetReadCount() // 减去已经读取的个数
	}
	var reader io.Reader
	if maxBytes > 0 {
		reader = io.LimitReader(file, int64(maxBytes)) // 读取限定的字节数
	} else {
		reader = file // 其余情况就全都读
	}
	ch := parser.ParseStream(reader) //读取数据
	fakeConn := connection.NewFakeConn() // 创建一个虚拟连接
	for p:=range ch{
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
		r,ok:=p.Data.(*reply.MultiBulkReply)
		if !ok {
			logger.Error("require multi bulk protocol")
			continue
		}
		ret:=persister.db.Exec(,r.Args) // 执行命令
	}
}
