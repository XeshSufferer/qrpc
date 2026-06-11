package internal

import (
	"sync"
	"testing"
)

func TestLocalsSetStringGetString(t *testing.T) {
	l := NewLocals().(*LocalsImpl)
	l.SetString("key1", "val1")
	l.SetString("key2", "val2")

	if v := l.GetString("key1"); v != "val1" {
		t.Fatalf("expected val1, got %v", v)
	}
	if v := l.GetString("key2"); v != "val2" {
		t.Fatalf("expected val2, got %v", v)
	}
	if v := l.GetString("nonexistent"); v != "" {
		t.Fatalf("expected empty, got %v", v)
	}
}

func TestLocalsSetGet(t *testing.T) {
	l := NewLocals().(*LocalsImpl)
	l.Set("int", 42)
	l.Set("str", "hello")
	l.Set("struct", struct{ X int }{X: 1})

	if v := l.Get("int").(int); v != 42 {
		t.Fatalf("expected 42, got %v", v)
	}
	if v := l.Get("str").(string); v != "hello" {
		t.Fatalf("expected hello, got %v", v)
	}
	if v := l.Get("nonexistent"); v != nil {
		t.Fatalf("expected nil, got %v", v)
	}
}

func TestLocalsReset(t *testing.T) {
	l := NewLocals().(*LocalsImpl)
	l.SetString("k1", "v1")
	l.Set("k2", 42)
	l.Reset()

	if v := l.GetString("k1"); v != "" {
		t.Fatalf("expected empty after reset, got %v", v)
	}
	if v := l.Get("k2"); v != nil {
		t.Fatalf("expected nil after reset, got %v", v)
	}
}

func TestLocalsOverwrite(t *testing.T) {
	l := NewLocals().(*LocalsImpl)
	l.SetString("key", "old")
	l.SetString("key", "new")
	if v := l.GetString("key"); v != "new" {
		t.Fatalf("expected new, got %v", v)
	}

	l.Set("key", 1)
	l.Set("key", 2)
	if v := l.Get("key").(int); v != 2 {
		t.Fatalf("expected 2, got %v", v)
	}
}

func TestLocalsConcurrent(t *testing.T) {
	l := NewLocals().(*LocalsImpl)
	var wg sync.WaitGroup
	n := 100

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := string(rune('a' + i))
			l.SetString(key, key)
			l.GetString(key)
			l.Set(key, i)
			l.Get(key)
		}(i)
	}
	wg.Wait()
}

func TestLocalsResetConcurrent(t *testing.T) {
	l := NewLocals().(*LocalsImpl)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				l.SetString("k", "v")
				l.Set("k", j)
				l.Reset()
			}
		}()
	}
	wg.Wait()
}
