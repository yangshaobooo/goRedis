package dict

import (
	"math"
	"sync"
)

// ConcurrentDict is thread safe map using sharding lock
type ConcurrentDict struct {
	table      []*shard
	count      int32
	shardCount int
}

type shard struct {
	m     map[string]interface{}
	mutex sync.RWMutex // 读写锁
}

// computeCapacity 计算大于param的最小的 2 的幂次方
func computeCapacity(param int) (size int) {
	if param <= 16 {
		return 16
	}
	n := param - 1
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	if n < 0 {
		return math.MaxInt32
	}
	return n + 1
}

// MakeConcurrent creates ConcurrentDict with the given shard count
//func MakeConcurrent(shardCount int) *ConcurrentDict {
//	shardCount = computeCapacity(shardCount)
//	table := make([]*shard, shardCount)
//}
