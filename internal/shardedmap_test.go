package internal

import (
	"sync"
	"testing"
)

func TestShardedMapStoreAndLoadAndDelete(t *testing.T) {
	sm := NewShardedMap()
	sm.Store(1, "val1")
	sm.Store(2, "val2")

	v, ok := sm.LoadAndDelete(1)
	if !ok {
		t.Fatal("expected key 1 to exist")
	}
	if v.(string) != "val1" {
		t.Fatalf("expected val1, got %v", v)
	}

	_, ok = sm.LoadAndDelete(1)
	if ok {
		t.Fatal("expected key 1 to be deleted")
	}

	v, ok = sm.LoadAndDelete(2)
	if !ok {
		t.Fatal("expected key 2 to exist")
	}
	if v.(string) != "val2" {
		t.Fatalf("expected val2, got %v", v)
	}
}

func TestShardedMapDelete(t *testing.T) {
	sm := NewShardedMap()
	sm.Store(42, "value")
	sm.Delete(42)

	_, ok := sm.LoadAndDelete(42)
	if ok {
		t.Fatal("expected key to be deleted")
	}
}

func TestShardedMapLoadAndDeleteMissing(t *testing.T) {
	sm := NewShardedMap()
	v, ok := sm.LoadAndDelete(999)
	if ok {
		t.Fatal("expected missing key")
	}
	if v != nil {
		t.Fatalf("expected nil, got %v", v)
	}
}

func TestShardedMapOverwrite(t *testing.T) {
	sm := NewShardedMap()
	sm.Store(1, "old")
	sm.Store(1, "new")

	v, ok := sm.LoadAndDelete(1)
	if !ok {
		t.Fatal("expected key 1 to exist")
	}
	if v.(string) != "new" {
		t.Fatalf("expected new, got %v", v)
	}
}

func TestShardedMapManyKeys(t *testing.T) {
	sm := NewShardedMap()
	n := 10000
	for i := 0; i < n; i++ {
		sm.Store(uint64(i), i)
	}
	for i := 0; i < n; i++ {
		v, ok := sm.LoadAndDelete(uint64(i))
		if !ok {
			t.Fatalf("expected key %d to exist", i)
		}
		if v.(int) != i {
			t.Fatalf("expected %d, got %v", i, v)
		}
	}
}

func TestShardedMapConcurrentStoreAndDelete(t *testing.T) {
	sm := NewShardedMap()
	var wg sync.WaitGroup
	n := 1000

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sm.Store(uint64(i), i)
		}(i)
	}
	wg.Wait()

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sm.LoadAndDelete(uint64(i))
		}(i)
	}
	wg.Wait()
}

func TestShardedMapConcurrentSameShard(t *testing.T) {
	sm := NewShardedMap()
	var wg sync.WaitGroup
	base := uint64(shardCount)

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := base + uint64(i)
			sm.Store(key, i)
			sm.LoadAndDelete(key)
		}(i)
	}
	wg.Wait()
}

func TestShardedMapHashDistribution(t *testing.T) {
	sm := NewShardedMap()
	n := 10000
	used := make(map[uint64]bool)

	for i := 0; i < n; i++ {
		sm.Store(uint64(i), i)
	}
	for i := 0; i < n; i++ {
		_, ok := sm.LoadAndDelete(uint64(i))
		if !ok {
			t.Fatalf("key %d missing", i)
		}
		used[uint64(i)%shardCount] = true
	}

	if len(used) < 200 {
		t.Fatalf("poor hash distribution: only %d shards used out of %d", len(used), shardCount)
	}
}
