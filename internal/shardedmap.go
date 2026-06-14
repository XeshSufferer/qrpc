package internal

import "sync"

const shardCount = 256

type shard struct {
	mu    sync.RWMutex
	items map[uint64]any
}

// ShardedMap is a concurrent map sharded into 256 buckets for
// O(1) request-ID-to-channel dispatch with minimal lock contention.
type ShardedMap struct {
	shards [shardCount]shard
}

// NewShardedMap creates a new empty ShardedMap.
func NewShardedMap() *ShardedMap {
	sm := &ShardedMap{}
	for i := range sm.shards {
		sm.shards[i].items = make(map[uint64]any)
	}
	return sm
}

// Store sets the value for the given key.
func (sm *ShardedMap) Store(key uint64, value any) {
	shard := &sm.shards[key%shardCount]
	shard.mu.Lock()
	shard.items[key] = value
	shard.mu.Unlock()
}

// LoadAndDelete atomically loads and deletes the value for the given key.
func (sm *ShardedMap) LoadAndDelete(key uint64) (any, bool) {
	shard := &sm.shards[key%shardCount]
	shard.mu.Lock()
	v, ok := shard.items[key]
	if ok {
		delete(shard.items, key)
	}
	shard.mu.Unlock()
	return v, ok
}

// Delete removes the value for the given key.
func (sm *ShardedMap) Delete(key uint64) {
	shard := &sm.shards[key%shardCount]
	shard.mu.Lock()
	delete(shard.items, key)
	shard.mu.Unlock()
}
