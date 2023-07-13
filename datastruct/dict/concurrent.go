package dict

import (
	"math"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"
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
func MakeConcurrent(shardCount int) *ConcurrentDict {
	shardCount = computeCapacity(shardCount) // 计算需要多少个map
	table := make([]*shard, shardCount)
	for i := 0; i < shardCount; i++ {
		table[i] = &shard{
			m: make(map[string]interface{}), // 初始化每一个map
		}
	}
	d := &ConcurrentDict{
		count:      0,
		table:      table,
		shardCount: shardCount,
	}
	return d
}

const prime32 = uint32(16777619) // 设定好的一个哈希质数，可以减少哈希冲突

// 哈希算法: FNV-1a哈希算法
func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}

// 计算哈希值对应在哪个位置
func (dict *ConcurrentDict) spread(hashCode uint32) uint32 {
	if dict == nil {
		panic("dict is nil")
	}
	tableSize := uint32(len(dict.table))
	// 减1的目的是为了不超出索引范围
	return (tableSize - 1) & hashCode // 长度和哈希值按位与，和go的map用的一样。
}

// getShard 得到指定的map片
func (dict *ConcurrentDict) getShard(index uint32) *shard {
	if dict == nil {
		panic("dict is nil")
	}
	return dict.table[index]
}

// Get returns the binding value and whether the key is exist
func (dict *ConcurrentDict) Get(key string) (val interface{}, exists bool) {
	if dict == nil {
		panic("dict is nil")
	}
	hashCode := fnv32(key)         // 计算哈希值
	index := dict.spread(hashCode) // 计算哈希值对应的map
	s := dict.getShard(index)      // 得到对应的map
	s.mutex.RLock()                // 上读锁  源码里用的写锁，博客里用的读锁
	defer s.mutex.RUnlock()
	val, exists = s.m[key]
	return
}

// GetWithLock get without lock
func (dict *ConcurrentDict) GetWithLock(key string) (val interface{}, exists bool) {
	if dict == nil {
		panic("dict is nil")
	}
	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	s := dict.getShard(index)
	val, exists = s.m[key]
	return
}

// Len returns the number of dict
func (dict *ConcurrentDict) Len() int {
	if dict == nil {
		panic("dict is nil")
	}
	// atomic.LoadInt32() 使用原子操作，保证在并发环境中取得正确的count
	return int(atomic.LoadInt32(&dict.count))
}

// Put puts key value into dict and returns the number of new inserted key-value
func (dict *ConcurrentDict) Put(key string, val interface{}) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	hashCode := fnv32(key)         // 计算哈希值
	index := dict.spread(hashCode) // 计算对应的map
	s := dict.getShard(index)      // 得到对应的map
	s.mutex.Lock()                 // 写入要用写锁
	defer s.mutex.Unlock()

	if _, ok := s.m[key]; ok {
		s.m[key] = val // 如果已经存在
		return 0
	}
	dict.addCount() // 字典中key-value 数量+1
	s.m[key] = val  //
	return 1
}

func (dict *ConcurrentDict) PutWithLock(key string, val interface{}) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	s := dict.getShard(index)
	if _, ok := s.m[key]; ok {
		s.m[key] = val
		return 0
	}
	dict.addCount()
	s.m[key] = val
	return 1
}

// PutIfAbsent puts value if the key is not exists and returns the number of updated key-value
func (dict *ConcurrentDict) PutIfAbsent(key string, val interface{}) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	s := dict.getShard(index)
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if _, ok := s.m[key]; ok {
		return 0
	}
	s.m[key] = val
	dict.addCount()
	return 1
}

func (dict *ConcurrentDict) PutIfAbsentWithLock(key string, val interface{}) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	s := dict.getShard(index)

	if _, ok := s.m[key]; ok {
		return 0
	}
	s.m[key] = val
	dict.addCount()
	return 1
}

// PutIfExists puts value if the key is exist and returns the number of inserted key-value
func (dict *ConcurrentDict) PutIfExists(key string, val interface{}) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	s := dict.getShard(index)
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if _, ok := s.m[key]; ok {
		s.m[key] = val
		return 1
	}
	return 0
}

func (dict *ConcurrentDict) PutIfExistsWithLock(key string, val interface{}) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	s := dict.getShard(index)

	if _, ok := s.m[key]; ok {
		s.m[key] = val
		return 1
	}
	return 0
}

// Remove removes the key and return the number of deleted key-value
func (dict *ConcurrentDict) Remove(key string) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	s := dict.getShard(index)
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if _, ok := s.m[key]; ok {
		delete(s.m, key)
		dict.decreaseCount()
		return 1
	}
	return 0
}

func (dict *ConcurrentDict) RemoveWithLock(key string) (val interface{}, result int) {
	if dict == nil {
		panic("dict is nil")
	}
	hashCode := fnv32(key)
	index := dict.spread(hashCode)
	s := dict.getShard(index)

	if val, ok := s.m[key]; ok {
		delete(s.m, key)
		dict.decreaseCount()
		return val, 1
	}
	return val, 0
}

func (dict *ConcurrentDict) addCount() int32 {
	return atomic.AddInt32(&dict.count, 1)
}

func (dict *ConcurrentDict) decreaseCount() int32 {
	return atomic.AddInt32(&dict.count, -1)
}

// ForEach traversal the dict
// it may not visits new entry inserted during traversal
func (dict *ConcurrentDict) ForEach(consumer Consumer) {
	if dict == nil {
		panic("dict is nil")
	}

	for _, s := range dict.table {
		s.mutex.RLock()
		f := func() bool {
			defer s.mutex.RUnlock()
			for key, value := range s.m {
				continues := consumer(key, value)
				if !continues {
					return false
				}
			}
			return false
		}
		if !f() {
			break
		}
	}
}

func (dict *ConcurrentDict) keys() []string {
	keys := make([]string, dict.Len())
	i := 0
	dict.ForEach(func(key string, val interface{}) bool {
		if i < len(keys) {
			keys[i] = key
			i++
		} else {
			keys = append(keys, key)
		}
		return true
	})
	return keys
}

// RandomKey returns a key randomly
func (shard *shard) RandomKey() string {
	if shard == nil {
		panic("shard is nil")
	}
	shard.mutex.RLock()
	defer shard.mutex.RUnlock()

	for key := range shard.m {
		return key
	}
	return ""
}

// RandomKeys randomly returns keys of the given number, may contain duplicated key
func (dict *ConcurrentDict) RandomKeys(limit int) []string {
	size := dict.Len()
	if limit >= size {
		return dict.keys() // 超过dict的大小，直接返回所有的key
	}
	shardCount := len(dict.table)
	result := make([]string, limit)
	nR := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < limit; {
		s := dict.getShard(uint32(nR.Intn(shardCount))) // 随机映射一个map
		if s == nil {
			continue
		}
		key := s.RandomKey() // 从当前map中获取一个key
		if key != "" {
			result[i] = key
			i++
		}
	}
	return result
}

// RandomDistinctKeys randomly returns keys of the given number, won't contain duplicated key
func (dict *ConcurrentDict) RandomDistinctKeys(limit int) []string {
	size := dict.Len()
	if limit >= size {
		return dict.keys()
	}
	shardCount := len(dict.table)
	result := make(map[string]struct{})
	nR := rand.New(rand.NewSource(time.Now().UnixNano()))
	for len(result) < limit {
		shardIndex := uint32(nR.Intn(shardCount))
		s := dict.getShard(shardIndex)
		if s == nil {
			continue
		}
		key := s.RandomKey()
		for key != "" {
			if _, exists := result[key]; !exists {
				result[key] = struct{}{}
			}
		}
	}
	arr := make([]string, limit)
	i := 0
	for k := range result {
		arr[i] = k
		i++
	}
	return arr
}

// Clear removes all keys in dict
func (dict *ConcurrentDict) Clear() {
	*dict = *MakeConcurrent(dict.shardCount)
}

func (dict *ConcurrentDict) toLockIndices(keys []string, reverse bool) []uint32 {
	indexMap := make(map[uint32]struct{}) //代码创建了一个空的映射 indexMap，用于存储分片索引index
	for _, key := range keys {
		index := dict.spread(fnv32(key))
		indexMap[index] = struct{}{}
	}
	indices := make([]uint32, 0, len(indexMap))
	for index := range indexMap {
		indices = append(indices, index)
	}
	sort.Slice(indices, func(i, j int) bool {
		if !reverse {
			return indices[i] < indices[j]
		}
		return indices[i] > indices[j]
	})
	return indices
}

// RWLocks locks write keys and read keys together. allow duplicate keys
func (dict *ConcurrentDict) RWLocks(writeKeys []string, readKeys []string) {
	keys := append(writeKeys, readKeys...)
	indices := dict.toLockIndices(keys, false)
	writeIndexSet := make(map[uint32]struct{})
	for _, wKey := range writeKeys {
		idx := dict.spread(fnv32(wKey))
		writeIndexSet[idx] = struct{}{}
	}
	for _, index := range indices {
		_, w := writeIndexSet[index]
		mu := &dict.table[index].mutex
		if w {
			mu.Lock() // 写的加写锁
		} else {
			mu.RLock() // 读的加读锁
		}
	}
}

// RWUnlocks unlocks write keys and read keys together. allow duplicate keys
func (dict *ConcurrentDict) RWUnLocks(writeKeys []string, readKeys []string) {
	keys := append(writeKeys, readKeys...)
	indices := dict.toLockIndices(keys, true)
	writeIndexSet := make(map[uint32]struct{})
	for _, wkey := range writeKeys {
		idx := dict.spread(fnv32(wkey))
		writeIndexSet[idx] = struct{}{}
	}
	for _, index := range indices {
		_, w := writeIndexSet[index]
		mu := &dict.table[index].mutex
		if w {
			mu.Unlock()
		} else {
			mu.RUnlock()
		}
	}
}
