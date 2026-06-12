package client

import (
	"sync"
	"time"

	quic "github.com/XeshSufferer/aquic-go"
)

const (
	DefaultBatchWindow = 150 * time.Microsecond
	BatchingEnabled    = true
)

type Batcher struct {
	stream    *quic.Stream
	mu        sync.Mutex
	buf       []byte
	lastFlush time.Time
	window    time.Duration
}

func NewBatcher(s *quic.Stream) *Batcher {
	return &Batcher{
		stream:    s,
		lastFlush: time.Now(),
		window:    DefaultBatchWindow,
	}
}

func (b *Batcher) Write(data []byte) error {
	if !BatchingEnabled {
		_, err := (*b.stream).Write(data)
		return err
	}

	b.mu.Lock()

	if len(b.buf) == 0 {
		b.lastFlush = time.Now()
		b.mu.Unlock()
		_, err := (*b.stream).Write(data)
		return err
	}

	b.buf = append(b.buf, data...)
	b.mu.Unlock()
	return nil
}

func (b *Batcher) Flush() error {
	if !BatchingEnabled {
		return nil
	}

	b.mu.Lock()
	if len(b.buf) == 0 {
		b.mu.Unlock()
		return nil
	}
	data := b.buf
	b.buf = b.buf[:0]
	b.lastFlush = time.Now()
	b.mu.Unlock()

	_, err := (*b.stream).Write(data)
	return err
}

func (b *Batcher) Flushed() bool {
	b.mu.Lock()
	flushed := len(b.buf) == 0
	b.mu.Unlock()
	return flushed
}
