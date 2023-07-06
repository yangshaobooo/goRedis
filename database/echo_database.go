package database

import (
	"goRedis/interface/redis"
	"goRedis/redis/protocol"
)

type EchoDatabase struct {
}

func NewEchoDatabase() *EchoDatabase {
	return &EchoDatabase{}
}

func (e EchoDatabase) Exec(client redis.Connection, args [][]byte) redis.Reply {
	return protocol.MakeMultiBulkReply(args)
}

func (e EchoDatabase) Close() {
}

func (e EchoDatabase) AfterClientClose(c redis.Connection) {
}
