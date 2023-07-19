package aof

import (
	"goRedis/config"
	"goRedis/lib/logger"
	"os"
)

func (handlerAof *AofHandler) newRewriteHandler() *AofHandler {
	h := &AofHandler{}
	h.aofFilename = handlerAof.aofFilename
	return h
}

// RewriteCtx holds context of an AOD rewriting procedure
type RewriteCtx struct {
	tmpFile  *os.File // tmpFile is the file handler of aof tmpFile
	fileSize int64
	dbIdx    int // selected db index when startRewrite
}

// Rewrite carries out AOF rewrite
func (handlerAof *AofHandler) Rewrite() error {
	ctx, err := handlerAof.StartRewrite()
	if err != nil {
		return err
	}
	err = handlerAof.DoRewrite(ctx)
	if err != nil {
		return err
	}
	handlerAof.FinishRewrite(ctx)
	return nil
}

// StartRewrite prepares rewrite procedure
func (handlerAof *AofHandler) StartRewrite() (*RewriteCtx, error) {
	handlerAof.pausingAof.Lock()
	defer handlerAof.pausingAof.Unlock()
	err := handlerAof.aofFile.Sync()
	if err != nil {
		logger.Warn("fsync failed")
		return nil, err
	}

	// get current aof file size
	fileInfo, _ := os.Stat(handlerAof.aofFilename)
	filesize := fileInfo.Size()

	// create tmp file
	file, err := os.CreateTemp(config.GetTmpDir(), "*.aof")
	if err != nil {
		logger.Warn("tmp file create failed")
		return nil, err
	}
	return &RewriteCtx{
		tmpFile:  file,
		fileSize: filesize,
		dbIdx:    handlerAof.currentDB,
	}, nil
}

// DoRewrite actually rewrite aof file
// makes DoRewrite public for testing only,please use Rewrite instead
func (handlerAof *AofHandler) DoRewrite(ctx *RewriteCtx) (err error) {
	// start rewrite
	if !config.Properties.AofUseRdbPreamble {
		logger.Info("generate aof preamble")
		//err = handlerAof.generateAof(ctx)
	} else {
		// 使用rdb
		logger.Info("generate rdb preamble")
		//err = handlerAof.generateRDB(ctx)
	}
	return err
}
