package quic

import (
	"strconv"
	"sync"
	"testing"

	quicgo "github.com/XeshSufferer/aquic-go"
	"github.com/XeshSufferer/qrpc/transport/types"
)

func TestBalancerNewIsEmpty(t *testing.T) {
	b := NewBalancer()
	_, err := b.GetStream()
	if err != types.StreamsListIsEmpty {
		t.Fatalf("expected StreamsListIsEmpty, got %v", err)
	}
}

func TestBalancerAddAndGet(t *testing.T) {
	b := NewBalancer()
	var s1, s2 quicgo.Stream
	b.AddStream(&s1)
	b.AddStream(&s2)

	got1, err := b.GetStream()
	if err != nil {
		t.Fatal(err)
	}
	if got1 != &s1 && got1 != &s2 {
		t.Fatal("got unexpected stream")
	}

	got2, err := b.GetStream()
	if err != nil {
		t.Fatal(err)
	}
	if got2 != &s1 && got2 != &s2 {
		t.Fatal("got unexpected stream")
	}

	if got1 == got2 {
		t.Fatal("round-robin should return different streams")
	}
}

func TestBalancerRoundRobin(t *testing.T) {
	b := NewBalancer()
	var streams [3]quicgo.Stream
	for i := range streams {
		b.AddStream(&streams[i])
	}

	seq := []*quicgo.Stream{&streams[0], &streams[1], &streams[2], &streams[0], &streams[1]}
	for i, want := range seq {
		got, err := b.GetStream()
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("round %d: expected stream %p, got %p", i, want, got)
		}
	}
}

func TestBalancerReset(t *testing.T) {
	b := NewBalancer()
	var s quicgo.Stream
	b.AddStream(&s)
	b.Reset()

	_, err := b.GetStream()
	if err != types.StreamsListIsEmpty {
		t.Fatal("expected empty after reset")
	}
}

func TestBalancerAddAfterReset(t *testing.T) {
	b := NewBalancer()
	var s1, s2 quicgo.Stream
	b.AddStream(&s1)
	b.Reset()
	b.AddStream(&s2)

	got, err := b.GetStream()
	if err != nil {
		t.Fatal(err)
	}
	if got != &s2 {
		t.Fatal("should return newly added stream")
	}
}

func TestBalancerConcurrentAddAndGet(t *testing.T) {
	b := NewBalancer()
	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var s quicgo.Stream
			b.AddStream(&s)
		}()
	}

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b.GetStream()
		}()
	}

	wg.Wait()
}

func TestBalancerConcurrentResetAndGet(t *testing.T) {
	b := NewBalancer()
	var s quicgo.Stream
	b.AddStream(&s)

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b.Reset()
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			b.GetStream()
		}()
	}
	wg.Wait()
}

func TestBalancerGetStreamAfterEmpty(t *testing.T) {
	b := NewBalancer()
	_, err := b.GetStream()
	if err != types.StreamsListIsEmpty {
		t.Fatal("expected empty")
	}

	var s quicgo.Stream
	b.AddStream(&s)

	got, err := b.GetStream()
	if err != nil {
		t.Fatal(err)
	}
	if got != &s {
		t.Fatal("expected the added stream")
	}
}

func TestBalancerManyStreamsRoundRobin(t *testing.T) {
	b := NewBalancer()
	n := 100
	var streams [100]quicgo.Stream
	for i := 0; i < n; i++ {
		b.AddStream(&streams[i])
	}

	seen := make(map[*quicgo.Stream]int)
	for i := 0; i < n; i++ {
		s, err := b.GetStream()
		if err != nil {
			t.Fatal(err)
		}
		seen[s]++
	}

	if len(seen) != n {
		t.Fatalf("expected %d unique streams, got %d", n, len(seen))
	}

	for _, count := range seen {
		if count != 1 {
			t.Fatalf("round-robin: expected each stream once, got count %d", count)
		}
	}
}

func BenchmarkBalancerGetStream(b *testing.B) {
	streamCounts := []int{1, 8, 16, 32}
	for _, n := range streamCounts {
		b.Run("streams="+strconv.Itoa(n), func(b *testing.B) {
			balancer := NewBalancer()
			for i := 0; i < n; i++ {
				var s quicgo.Stream
				balancer.AddStream(&s)
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := balancer.GetStream()
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkBalancerAddStream(b *testing.B) {
	baseCounts := []int{0, 8, 16}
	for _, base := range baseCounts {
		b.Run("base="+strconv.Itoa(base), func(b *testing.B) {
			balancer := NewBalancer()
			streams := make([]quicgo.Stream, base)
			for i := 0; i < base; i++ {
				balancer.AddStream(&streams[i])
			}
			var s quicgo.Stream
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				balancer.Reset()
				for j := 0; j < base; j++ {
					balancer.AddStream(&streams[j])
				}
				b.StartTimer()
				balancer.AddStream(&s)
			}
		})
	}
}

func BenchmarkBalancerGetStream_Parallel(b *testing.B) {
	balancer := NewBalancer()
	for i := 0; i < 16; i++ {
		var s quicgo.Stream
		balancer.AddStream(&s)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := balancer.GetStream()
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
