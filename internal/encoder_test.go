package internal

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/XeshSufferer/qrpc/protos/pb/gen"
	"github.com/XeshSufferer/qrpc/transport/types"
)

func TestEncoderEncodeRequestFrameFormat(t *testing.T) {
	encoder := NewEncoder()
	req := &gen.Request{
		RequestId: 1,
		Method:    []byte("test"),
		Body:      []byte("payload"),
		Headers:   [][]byte{[]byte("h"), []byte("v")},
	}

	buf, err := encoder.EncodeRequest(req)
	if err != nil {
		t.Fatal(err)
	}
	defer buf.Release()

	data := buf.Bytes()
	if len(data) < 5 {
		t.Fatal("frame too short")
	}

	length := binary.BigEndian.Uint32(data[:4])
	flag := data[4]

	if flag != types.REQUEST_FLAG && flag != types.REQUEST_ZSTD_FLAG {
		t.Fatalf("unexpected flag: %d", flag)
	}
	if int(length) != len(data)-4 {
		t.Fatalf("length mismatch: header says %d, actual %d", length, len(data)-4)
	}
}

func TestEncoderEncodeResponseFrameFormat(t *testing.T) {
	encoder := NewEncoder()
	resp := &gen.Response{
		RequestId: 1,
		Code:      200,
		Body:      []byte("ok"),
	}

	buf, err := encoder.EncodeResponse(resp)
	if err != nil {
		t.Fatal(err)
	}
	defer buf.Release()

	data := buf.Bytes()
	flag := data[4]
	if flag != types.RESPONSE_FLAG && flag != types.RESPONSE_ZSTD_FLAG {
		t.Fatalf("unexpected flag: %d", flag)
	}
}

func TestEncoderEncodeEventFrameFormat(t *testing.T) {
	encoder := NewEncoder()
	req := &gen.Request{
		Method: []byte("event.test"),
		Body:   []byte("data"),
	}

	buf, err := encoder.EncodeEvent(req)
	if err != nil {
		t.Fatal(err)
	}
	defer buf.Release()

	data := buf.Bytes()
	flag := data[4]
	if flag != types.EVENT_FLAG && flag != types.EVENT_ZSTD_FLAG {
		t.Fatalf("unexpected flag: %d", flag)
	}
}

func TestEncoderRequestResponseRoundtrip(t *testing.T) {
	encoder := NewEncoder()
	original := &gen.Request{
		RequestId: 42,
		Method:    []byte("ping"),
		Body:      []byte("hello"),
		Headers:   [][]byte{[]byte("k"), []byte("v")},
	}

	buf, err := encoder.EncodeRequest(original)
	if err != nil {
		t.Fatal(err)
	}
	defer buf.Release()

	data := buf.Bytes()
	flag := data[4]
	payload := data[5:]

	if flag == types.REQUEST_ZSTD_FLAG {
		payload, err = DecompressZstd(payload)
		if err != nil {
			t.Fatal(err)
		}
	}

	decoded := &gen.Request{}
	if err := decoded.UnmarshalVT(payload); err != nil {
		t.Fatal(err)
	}

	if decoded.RequestId != 42 {
		t.Fatalf("RequestId mismatch: %d", decoded.RequestId)
	}
	if string(decoded.Method) != "ping" {
		t.Fatalf("Method mismatch: %s", decoded.Method)
	}
	if string(decoded.Body) != "hello" {
		t.Fatalf("Body mismatch: %s", decoded.Body)
	}
}

func TestEncoderResponseRoundtrip(t *testing.T) {
	encoder := NewEncoder()
	original := &gen.Response{
		RequestId: 7,
		Code:      200,
		Body:      []byte("response-data"),
		Headers:   [][]byte{[]byte("k"), []byte("v")},
	}

	buf, err := encoder.EncodeResponse(original)
	if err != nil {
		t.Fatal(err)
	}
	defer buf.Release()

	data := buf.Bytes()
	flag := data[4]
	payload := data[5:]

	if flag == types.RESPONSE_ZSTD_FLAG {
		payload, err = DecompressZstd(payload)
		if err != nil {
			t.Fatal(err)
		}
	}

	decoded := &gen.Response{}
	if err := decoded.UnmarshalVT(payload); err != nil {
		t.Fatal(err)
	}

	if decoded.RequestId != 7 {
		t.Fatalf("RequestId mismatch: %d", decoded.RequestId)
	}
	if decoded.Code != 200 {
		t.Fatalf("Code mismatch: %d", decoded.Code)
	}
	if string(decoded.Body) != "response-data" {
		t.Fatalf("Body mismatch: %s", decoded.Body)
	}
}

func TestEncoderEventRoundtrip(t *testing.T) {
	encoder := NewEncoder()
	original := &gen.Request{
		Method: []byte("event.test"),
		Body:   []byte("event-data"),
	}

	buf, err := encoder.EncodeEvent(original)
	if err != nil {
		t.Fatal(err)
	}
	defer buf.Release()

	data := buf.Bytes()
	flag := data[4]
	payload := data[5:]

	if flag == types.EVENT_ZSTD_FLAG {
		payload, err = DecompressZstd(payload)
		if err != nil {
			t.Fatal(err)
		}
	}

	decoded := &gen.Request{}
	if err := decoded.UnmarshalVT(payload); err != nil {
		t.Fatal(err)
	}

	if string(decoded.Method) != "event.test" {
		t.Fatalf("Method mismatch: %s", decoded.Method)
	}
	if string(decoded.Body) != "event-data" {
		t.Fatalf("Body mismatch: %s", decoded.Body)
	}
}

func TestEncoderCompressionThreshold(t *testing.T) {
	encoder := NewEncoder()

	small := &gen.Request{
		RequestId: 1,
		Method:    []byte("test"),
		Body:      make([]byte, 1024),
	}
	smallBuf, err := encoder.EncodeRequest(small)
	if err != nil {
		t.Fatal(err)
	}
	if smallBuf.Bytes()[4] != types.REQUEST_FLAG {
		t.Fatal("small request should not be compressed")
	}
	smallBuf.Release()

	large := &gen.Request{
		RequestId: 1,
		Method:    []byte("test"),
		Body:      make([]byte, compressionThreshold+1),
	}
	largeBuf, err := encoder.EncodeRequest(large)
	if err != nil {
		t.Fatal(err)
	}
	if largeBuf.Bytes()[4] == types.REQUEST_FLAG {
		t.Fatal("large request should be compressed")
	}
	largeBuf.Release()
}

func TestEncoderBufferPoolReuse(t *testing.T) {
	buf := getBuffer(64)
	buf.Release()

	buf2 := getBuffer(64)
	if cap(buf2.data) < 64 {
		t.Fatal("buffer should have sufficient capacity")
	}
	buf2.Release()
}

func TestEncoderBufferGrow(t *testing.T) {
	buf := getBuffer(10000)
	if cap(buf.data) < 10000 {
		t.Fatal("buffer should have capacity >= requested size")
	}
	buf.Release()
}

func TestEncoderEmptyBody(t *testing.T) {
	encoder := NewEncoder()

	req := &gen.Request{RequestId: 1, Method: []byte("empty")}
	buf, err := encoder.EncodeRequest(req)
	if err != nil {
		t.Fatal(err)
	}
	defer buf.Release()

	data := buf.Bytes()
	payload := data[5:]
	if flag := data[4]; flag == types.REQUEST_ZSTD_FLAG {
		payload, err = DecompressZstd(payload)
		if err != nil {
			t.Fatal(err)
		}
	}

	decoded := &gen.Request{}
	if err := decoded.UnmarshalVT(payload); err != nil {
		t.Fatal(err)
	}
	if decoded.RequestId != 1 || string(decoded.Method) != "empty" {
		t.Fatal("empty body request roundtrip failed")
	}
}

func TestEncoderNilBody(t *testing.T) {
	encoder := NewEncoder()
	req := &gen.Request{RequestId: 1, Method: []byte("nil"), Body: nil}
	buf, err := encoder.EncodeRequest(req)
	if err != nil {
		t.Fatal(err)
	}
	buf.Release()
}

func TestEncoderCompressDecompressZstd(t *testing.T) {
	input := bytes.Repeat([]byte("hello world! "), 1000)
	compressed, err := CompressZstd(input)
	if err != nil {
		t.Fatal(err)
	}

	if len(compressed) >= len(input) {
		t.Fatal("compression should reduce size for repetitive data")
	}

	decompressed, err := DecompressZstd(compressed)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(decompressed, input) {
		t.Fatal("decompressed data does not match original")
	}
}

func TestEncoderCompressDecompressSmall(t *testing.T) {
	input := []byte("small")
	compressed, err := CompressZstd(input)
	if err != nil {
		t.Fatal(err)
	}

	decompressed, err := DecompressZstd(compressed)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(decompressed, input) {
		t.Fatal("decompressed data does not match original")
	}
}

func TestEncoderBufferReleaseNilSafe(t *testing.T) {
	var buf *Buffer
	buf.Release()
}

func benchmarkEncodeRequest(b *testing.B, bodySize int) {
	encoder := NewEncoder()
	req := &gen.Request{
		RequestId: 1,
		Method:    []byte("bench.method"),
		Body:      make([]byte, bodySize),
		Headers:   [][]byte{[]byte("trace-id"), []byte("abc")},
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
		Headers:   [][]byte{[]byte("trace-id"), []byte("abc")},
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
		Headers:   [][]byte{[]byte("trace-id"), []byte("abc")},
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
		Headers:   [][]byte{[]byte("trace-id"), []byte("abc")},
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
