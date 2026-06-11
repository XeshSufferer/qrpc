package internal

import "sync"

type Locals interface {
	SetString(key, value string)
	GetString(key string) string
	Set(key string, value any)
	Get(key string) any
	Reset()
}

type LocalsImpl struct {
	anyes   map[string]any
	strings map[string]string
	mu      sync.RWMutex
}

func NewLocals() Locals {
	return &LocalsImpl{
		anyes:   map[string]any{},
		strings: map[string]string{},
		mu:      sync.RWMutex{},
	}
}

func (l *LocalsImpl) SetString(key, value string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.strings[key] = value
}

func (l *LocalsImpl) GetString(key string) string {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.strings[key]
}

func (l *LocalsImpl) Get(key string) any {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.anyes[key]
}

func (l *LocalsImpl) Set(key string, value any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.anyes[key] = value
}

func (l *LocalsImpl) Reset() {
	l.mu.Lock()
	defer l.mu.Unlock()
	clear(l.anyes)
	clear(l.strings)
}
