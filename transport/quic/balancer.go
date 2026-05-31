package quic

import (
	"sync"
	"sync/atomic"

	"github.com/XeshSufferer/qrpc/transport/types"
	"github.com/quic-go/quic-go"
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

	newStreams := make([]*quic.Stream, len(current.streams)+1)
	copy(newStreams, current.streams)
	newStreams[len(current.streams)] = stream

	b.snapshot.Store(&streamsSnapshot{
		streams: newStreams,
	})
}

func (b *BalancerImpl) Reset() {
	b.m.Lock()
	defer b.m.Unlock()

	b.counter.Store(0)

	b.snapshot.Store(&streamsSnapshot{
		streams: make([]*quic.Stream, 0, 32),
	})
}
