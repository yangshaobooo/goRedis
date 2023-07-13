package database

import (
	"goRedis/interface/resp"
)

type CmdLine [][]byte

// Database redis的业务核心，需要三个功能：执行、关闭、关闭后的操作
type Database interface {
	Exec(client resp.Connection, args [][]byte) resp.Reply
	Close()
	AfterClientClose(c resp.Connection)
}

type DataEntity struct {
	Data interface{}
}
