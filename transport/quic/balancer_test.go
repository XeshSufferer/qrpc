package quic

import (
	"strconv"
	"testing"

	quicgo "github.com/XeshSufferer/aquic-go"
)

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
