package quic

import (
	"sync"
	"sync/atomic"

	quic "github.com/XeshSufferer/aquic-go"
	"github.com/XeshSufferer/qrpc/transport/types"
)

type Balancer interface {
	GetStream() (*quic.Stream, error)
	AddStream(stream *quic.Stream)
	Reset()
}

type streamsSnapshot struct {
	streams []*quic.Stream
}

type BalancerImpl struct {
	snapshot atomic.Pointer[streamsSnapshot]
	m        sync.Mutex
	counter  atomic.Uint32
}

func NewBalancer() Balancer {
	b := &BalancerImpl{}

	b.snapshot.Store(&streamsSnapshot{
		streams: make([]*quic.Stream, 0, 32),
	})

	return b
}

func (b *BalancerImpl) GetStream() (*quic.Stream, error) {
	snapshot := b.snapshot.Load()
	streams := snapshot.streams

	if len(streams) == 0 {
		return nil, types.StreamsListIsEmpty
	}

	idx := b.counter.Add(1) - 1
	return streams[idx%uint32(len(streams))], nil
}

func (b *BalancerImpl) AddStream(stream *quic.Stream) {
	b.m.Lock()
	defer b.m.Unlock()

	current := b.snapshot.Load()
	n := len(current.streams)

	newStreams := make([]*quic.Stream, n+1, n+1)
	copy(newStreams, current.streams)
	newStreams[n] = stream

	b.snapshot.Store(&streamsSnapshot{
		streams: newStreams,
	})
}

func (b *BalancerImpl) Reset() {
	b.m.Lock()
	defer b.m.Unlock()

	b.counter.Store(0)

	old := b.snapshot.Swap(&streamsSnapshot{
		streams: make([]*quic.Stream, 0, 32),
	})

	for _, s := range old.streams {
		func() {
			defer func() { recover() }()
			s.Close()
		}()
	}
}
