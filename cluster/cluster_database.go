package cluster

import (
	"context"
	"fmt"
	pool "github.com/jolestar/go-commons-pool/v2"
	"goRedis/config"
	"goRedis/database"
	databaseface "goRedis/interface/database"
	"goRedis/interface/resp"
	"goRedis/lib/consistenthash"
	"goRedis/lib/logger"
	"goRedis/resp/reply"
	"runtime/debug"
	"strings"
)

// ClusterDatabase represents a node of godis cluster
// it holds part of data and coordinates other nodes to finish transactions
type ClusterDatabase struct {
	self string // 记录自己的地址

	nodes          []string                    // 整个集群的节点
	peerPicker     *consistenthash.NodeMap     //
	peerConnection map[string]*pool.ObjectPool // 多个连接池
	db             databaseface.Database
}

// MakeClusterDatabase creates and starts a node of cluster
func MakeClusterDatabase() *ClusterDatabase {
	cluster := &ClusterDatabase{
		self: config.Properties.Self,

		db:             database.NewStandaloneDatabase(), // 该节点单机的redis数据库
		peerPicker:     consistenthash.NewNodeMap(nil),
		peerConnection: make(map[string]*pool.ObjectPool), // 该节点和其他节点的连接池
	}
	nodes := make([]string, 0, len(config.Properties.Peers)+1) // 所有的节点
	for _, peer := range config.Properties.Peers {             // 添加兄弟节点地址
		nodes = append(nodes, peer)
	}
	nodes = append(nodes, config.Properties.Self) // 添加自己的地址
	cluster.peerPicker.AddNode(nodes...)          // 向集群中添加节点
	ctx := context.Background()
	for _, peer := range config.Properties.Peers { // 自己和兄弟节点之间建立连接池
		cluster.peerConnection[peer] = pool.NewObjectPoolWithDefaultConfig(ctx, &connectionFactory{
			Peer: peer,
		})
	}
	cluster.nodes = nodes
	return cluster
}

// CmdFunc represents the handler of a redis command
type CmdFunc func(cluster *ClusterDatabase, c resp.Connection, cmdAndArgs [][]byte) resp.Reply

// Close stops current node of cluster
func (cluster *ClusterDatabase) Close() {
	cluster.db.Close()
}

var router = makeRouter()

// Exec executes command on cluster
func (cluster *ClusterDatabase) Exec(c resp.Connection, cmdLine [][]byte) (result resp.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			result = &reply.UnknownErrReply{}
		}
	}()
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmdFunc, ok := router[cmdName]
	if !ok {
		return reply.MakeErrReply("ERR unknown command '" + cmdName + "', or not supported in cluster mode")
	}
	result = cmdFunc(cluster, c, cmdLine)
	return
}

// AfterClientClose does some clean after client close connection
func (cluster *ClusterDatabase) AfterClientClose(c resp.Connection) {
	cluster.db.AfterClientClose(c)
}
