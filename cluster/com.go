package cluster

import (
	"context"
	"errors"
	"goRedis/interface/resp"
	"goRedis/lib/utils"
	"goRedis/resp/client"
	"goRedis/resp/reply"
	"strconv"
)

// getPeerClient 从连接池中拿一个连接
func (cluster *ClusterDatabase) getPeerClient(peer string) (*client.Client, error) {
	// 获取自己和兄弟节点建立的连接池
	factory, ok := cluster.peerConnection[peer]
	if !ok {
		return nil, errors.New("connection factory not found")
	}
	// 从连接池中借一个连接
	raw, err := factory.BorrowObject(context.Background())
	if err != nil {
		return nil, err
	}
	conn, ok := raw.(*client.Client) // 类型断言
	if !ok {
		return nil, errors.New("connection factory make wrong type")
	}
	return conn, nil
}

// returnPeerClient 把连接还回去
func (cluster *ClusterDatabase) returnPeerClient(peer string, peerClient *client.Client) error {
	connectionFactory, ok := cluster.peerConnection[peer]
	if !ok {
		return errors.New("connection factory not found")
	}
	return connectionFactory.ReturnObject(context.Background(), peerClient)
}

// relay 转发 command to peer
// select db by c.GetDBIndex()
// cannot call Prepare, Commit, execRollback of self node
func (cluster *ClusterDatabase) relay(peer string, c resp.Connection, args [][]byte) resp.Reply {
	// 如果是自己，本地数据库直接执行
	if peer == cluster.self {
		// to self db
		return cluster.db.Exec(c, args)
	}
	peerClient, err := cluster.getPeerClient(peer) //获取一个连接
	if err != nil {
		return reply.MakeErrReply(err.Error())
	}
	defer func() {
		_ = cluster.returnPeerClient(peer, peerClient) // 归还连接
	}()
	// 先切换数据库
	peerClient.Send(utils.ToCmdLine("SELECT", strconv.Itoa(c.GetDBIndex())))
	return peerClient.Send(args)
}

// broadcast 广播 command to all node in cluster
func (cluster *ClusterDatabase) broadcast(c resp.Connection, args [][]byte) map[string]resp.Reply {
	result := make(map[string]resp.Reply)
	for _, node := range cluster.nodes {
		reply := cluster.relay(node, c, args)
		result[node] = reply
	}
	return result
}
