package internal

import (
	"testing"

	"github.com/XeshSufferer/qrpc/protos/pb/gen"
)

func benchmarkEncodeRequest(b *testing.B, bodySize int) {
	encoder := NewEncoder()
	req := &gen.Request{
		RequestId: 1,
		Method:    []byte("bench.method"),
		Body:      make([]byte, bodySize),
		Headers:   []byte("trace-id:abc"),
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf, err := encoder.EncodeRequest(req)
		if err != nil {
			b.Fatal(err)
		}
		buf.Release()
	}
}

func BenchmarkEncodeRequest_16B(b *testing.B)  { benchmarkEncodeRequest(b, 16) }
func BenchmarkEncodeRequest_1KB(b *testing.B)  { benchmarkEncodeRequest(b, 1024) }
func BenchmarkEncodeRequest_64KB(b *testing.B) { benchmarkEncodeRequest(b, 64<<10) }

func benchmarkEncodeResponse(b *testing.B, bodySize int) {
	encoder := NewEncoder()
	resp := &gen.Response{
		RequestId: 1,
		Code:      200,
		Body:      make([]byte, bodySize),
		Headers:   []byte("trace-id:abc"),
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf, err := encoder.EncodeResponse(resp)
		if err != nil {
			b.Fatal(err)
		}
		buf.Release()
	}
}

func BenchmarkEncodeResponse_16B(b *testing.B)  { benchmarkEncodeResponse(b, 16) }
func BenchmarkEncodeResponse_1KB(b *testing.B)  { benchmarkEncodeResponse(b, 1024) }
func BenchmarkEncodeResponse_64KB(b *testing.B) { benchmarkEncodeResponse(b, 64<<10) }

func BenchmarkEncodeRequest_Parallel(b *testing.B) {
	encoder := NewEncoder()
	req := &gen.Request{
		RequestId: 1,
		Method:    []byte("bench.method"),
		Body:      make([]byte, 256),
		Headers:   []byte("trace-id:abc"),
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			buf, err := encoder.EncodeRequest(req)
			if err != nil {
				b.Fatal(err)
			}
			buf.Release()
		}
	})
}

func BenchmarkEncodeResponse_Parallel(b *testing.B) {
	encoder := NewEncoder()
	resp := &gen.Response{
		RequestId: 1,
		Code:      200,
		Body:      make([]byte, 256),
		Headers:   []byte("trace-id:abc"),
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			buf, err := encoder.EncodeResponse(resp)
			if err != nil {
				b.Fatal(err)
			}
			buf.Release()
		}
	})
}

func BenchmarkBufferPool(b *testing.B) {
	sizes := []int{64, 256, 1024, 4096, 16384}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		size := sizes[i%len(sizes)]
		buf := getBuffer(size)
		buf.Release()
	}
}
