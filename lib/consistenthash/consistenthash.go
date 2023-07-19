package consistenthash

import (
	"hash/crc32"
	"sort"
)

type HashFunc func(data []byte) uint32

type NodeMap struct {
	hashFunc    HashFunc
	nodeHashVal []int
	nodeHashMap map[int]string
}

func NewNodeMap(fn HashFunc) *NodeMap {
	m := &NodeMap{
		hashFunc:    fn,
		nodeHashMap: make(map[int]string),
	}
	if m.hashFunc == nil {
		m.hashFunc = crc32.ChecksumIEEE
	}
	return m
}

func (m *NodeMap) IsEmpty() bool {
	return len(m.nodeHashVal) == 0
}

// AddNode 增加集群节点
func (m *NodeMap) AddNode(keys ...string) {
	for _, key := range keys {
		if key == "" {
			continue
		}
		hash := int(m.hashFunc([]byte(key))) // get hash value
		m.nodeHashVal = append(m.nodeHashVal, hash)
		m.nodeHashMap[hash] = key
	}
	sort.Ints(m.nodeHashVal) // 把哈希值排序
}

// PickNode 判断key在哪个节点
func (m *NodeMap) PickNode(key string) string {
	if m.IsEmpty() {
		return ""
	}
	hash := int(m.hashFunc([]byte(key))) //get hash value
	// 得到对应哈希值在的区间，找大于等于它的节点的哈希值
	idx := sort.Search(len(m.nodeHashVal), func(i int) bool {
		return m.nodeHashVal[i] >= hash
	})
	if idx == len(m.nodeHashVal) {
		idx = 0
	}
	return m.nodeHashMap[m.nodeHashVal[idx]] // 得到节点的名称
}
