package server

import (
	"context"
	databaaseface "goRedis/interface/database"
	"goRedis/lib/logger"
	"goRedis/lib/sync/atomic"
	"goRedis/redis/connection"
	"net"
	"sync"
)

type RespHandler struct {
	activeConn sync.Map
	db         databaaseface.Database
	closing    atomic.Boolean
}

func (r RespHandler) Handle(ctx context.Context, conn net.Conn) {
	//TODO implement me
	panic("implement me")
}

func (r *RespHandler) Close() error {
	logger.Info("handler shutting down")
	r.closing.Set(true)
	r.activeConn.Range(
		func(key interface{}, value interface{}) bool {
			client := key.(*connection.Connection)
			_ = client.Close()
			return true
		})
	r.db.Close()
	return nil
}
