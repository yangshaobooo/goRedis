package dict

import "sync"

type SyncDict struct {
	m sync.Map // 并发安全的map
}

// MakeSyncDict makes a new map
func MakeSyncDict() *SyncDict {
	return &SyncDict{}
}

// Get return the binding value and whether the key is exist
func (dict *SyncDict) Get(key string) (val interface{}, exists bool) {
	val, ok := dict.m.Load(key)
	return val, ok
}

// Len returns the number of dict
func (dict *SyncDict) Len() int {
	length := 0
	dict.m.Range(func(k, v interface{}) bool {
		length++
		return true // 必须返回true，这样可以一直遍历下去
	})
	return length
}

// Put puts key value into dict and returns the number of new inserted key-value
func (dict *SyncDict) Put(key string, val interface{}) (result int) {
	_, existed := dict.m.Load(key)
	dict.m.Store(key, val)
	if existed {
		return 0
	}
	return 1
}

// PutIfAbsent puts value if the key is not exists and returns the number of updated key-value
func (dict *SyncDict) PutIfAbsent(key string, val interface{}) (result int) {
	_, existed := dict.m.Load(key)
	if existed {
		return 0
	}
	dict.m.Store(key, val)
	return 1
}

// PutIfExists puts value of the key is exists and returns the number of inserted key-value
func (dict *SyncDict) PutIfExists(key string, val interface{}) (result int) {
	_, existed := dict.m.Load(key)
	if existed {
		dict.m.Store(key, val)
		return 1
	}
	return 0
}

// Remove removes the key and return the number of deleted key-value
func (dict *SyncDict) Remove(key string) (result int) {
	_, existed := dict.m.Load(key)
	dict.m.Delete(key)
	if existed {
		return 1
	}
	return 0
}

// ForEach traversal the dict
func (dict *SyncDict) ForEach(consumer Consumer) {
	dict.m.Range(func(key, value interface{}) bool {
		consumer(key.(string), value)
		return true
	})
}

// Keys returns all keys in dict
func (dict *SyncDict) keys() []string {
	result := make([]string, dict.Len())
	i := 0
	dict.m.Range(func(key, value interface{}) bool {
		result[i] = key.(string)
		i++
		return true
	})
	return result
}

// RandomKeys randomly returns keys of the given number,may contain duplicated key
func (dict *SyncDict) RandomKeys(limit int) []string {
	result := make([]string, limit)
	for i := 0; i < limit; i++ {
		dict.m.Range(func(key, value interface{}) bool {
			result[i] = key.(string)
			return false // 每次只遍历第一个
		})
	}
	return result
}

// RandomDistinctKeys randomly returns keys of the given number,won't contain duplicated key
func (dict *SyncDict) RandomDistinctKeys(limit int) []string {
	result := make([]string, limit)
	i := 0
	dict.m.Range(func(key, value interface{}) bool {
		result[i] = key.(string)
		i++
		if i == limit {
			return false
		}
		return true
	})
	return result
}

// Clear removes all keys in dict
func (dict *SyncDict) Clear() {
	*dict = *MakeSyncDict()
}
