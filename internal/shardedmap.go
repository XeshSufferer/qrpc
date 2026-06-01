package internal

import "sync"

const shardCount = 256

type shard struct {
	mu    sync.RWMutex
	items map[uint64]any
}

type ShardedMap struct {
	shards [shardCount]shard
}

func NewShardedMap() *ShardedMap {
	sm := &ShardedMap{}
	for i := range sm.shards {
		sm.shards[i].items = make(map[uint64]any)
	}
	return sm
}

func (sm *ShardedMap) Store(key uint64, value any) {
	shard := &sm.shards[key%shardCount]
	shard.mu.Lock()
	shard.items[key] = value
	shard.mu.Unlock()
}

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

func (sm *ShardedMap) Delete(key uint64) {
	shard := &sm.shards[key%shardCount]
	shard.mu.Lock()
	delete(shard.items, key)
	shard.mu.Unlock()
}
