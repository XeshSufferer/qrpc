package internal

import (
	"encoding/binary"
	"sync"

	"github.com/XeshSufferer/qrpc/protos/pb/gen"
	"github.com/XeshSufferer/qrpc/transport/types"
	"github.com/klauspost/compress/zstd"
)

const (
	headerSize           = 5 // 4 bytes length + 1 byte flag
	defaultBufSize       = 1024
	compressionThreshold = 16384
)

// Buffer is a reusable byte buffer obtained from a pool.
type Buffer struct {
	data []byte
	used int
}

// Bytes returns the written portion of the buffer.
func (b *Buffer) Bytes() []byte { return b.data[:b.used] }

// Release returns the buffer to the pool for reuse.
func (b *Buffer) Release() {
	if b == nil {
		return
	}
	b.data = b.data[:cap(b.data)]
	b.used = 0
	bufferPool.Put(b)
}

var bufferPool = sync.Pool{
	New: func() any {
		return &Buffer{data: make([]byte, defaultBufSize)}
	},
}

// Encoder serializes protobuf Request and Response messages
// into the qrpc wire format with optional zstd compression.
type Encoder interface {
	EncodeRequest(req *gen.Request) (*Buffer, error)
	EncodeResponse(resp *gen.Response) (*Buffer, error)
	EncodeEvent(req *gen.Request) (*Buffer, error)
}

// EncoderImpl is a stateless encoder that pools internal buffers.
type EncoderImpl struct{}

// NewEncoder creates a new Encoder.
func NewEncoder() Encoder {
	return &EncoderImpl{}
}

// Frame format:
// [4 bytes payload length][1 byte flag][protobuf payload]
//
// payload length includes:
//   - 1 byte flag
//   - protobuf payload
//
// Example:
// [00 00 00 15][01][protobuf...]
// EncodeRequest serializes a protobuf Request into a Buffer.
func (e *EncoderImpl) EncodeRequest(req *gen.Request) (*Buffer, error) {
	return e.encodeMessage(
		req.SizeVT(),
		types.REQUEST_FLAG,
		types.REQUEST_ZSTD_FLAG,
		req.MarshalToSizedBufferVT,
	)
}

// EncodeResponse serializes a protobuf Response into a Buffer.
func (e *EncoderImpl) EncodeResponse(resp *gen.Response) (*Buffer, error) {
	return e.encodeMessage(
		resp.SizeVT(),
		types.RESPONSE_FLAG,
		types.RESPONSE_ZSTD_FLAG,
		resp.MarshalToSizedBufferVT,
	)
}

// EncodeEvent serializes a protobuf Request as a one-way event frame.
func (e *EncoderImpl) EncodeEvent(req *gen.Request) (*Buffer, error) {
	return e.encodeMessage(
		req.SizeVT(),
		types.EVENT_FLAG,
		types.EVENT_ZSTD_FLAG,
		req.MarshalToSizedBufferVT,
	)
}

func (e *EncoderImpl) encodeMessage(
	payloadSize int,
	flag byte,
	zstdFlag byte,
	marshal func([]byte) (int, error),
) (*Buffer, error) {
	totalSize := headerSize + payloadSize
	b := getBuffer(totalSize)

	payload := b.data[headerSize:]
	n, err := marshal(payload)
	if err != nil {
		b.Release()
		return nil, err
	}

	useFlag := flag
	payloadLen := n

	if n > compressionThreshold {
		compressed, cErr := CompressZstd(b.data[headerSize : headerSize+n])
		if cErr != nil {
			b.Release()
			return nil, cErr
		}
		if len(compressed) < n {
			payloadLen = len(compressed)
			useFlag = zstdFlag
			copy(b.data[headerSize:], compressed)
		}
	}

	b.data[4] = useFlag
	binary.BigEndian.PutUint32(b.data[:4], uint32(1+payloadLen))
	b.used = headerSize + payloadLen

	return b, nil
}

func getBuffer(size int) *Buffer {
	b := bufferPool.Get().(*Buffer)

	if cap(b.data) < size {
		return &Buffer{data: make([]byte, size)}
	}

	b.data = b.data[:size]
	return b
}

var zstdEncPool = sync.Pool{
	New: func() any {
		enc, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
		if err != nil {
			panic(err)
		}
		return enc
	},
}

var zstdDecPool = sync.Pool{
	New: func() any {
		dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
		if err != nil {
			panic(err)
		}
		return dec
	},
}

// CompressZstd compresses data using zstd with a pooled encoder.
func CompressZstd(src []byte) ([]byte, error) {
	enc := zstdEncPool.Get().(*zstd.Encoder)
	defer zstdEncPool.Put(enc)
	return enc.EncodeAll(src, nil), nil
}

// DecompressZstd decompresses data using zstd with a pooled decoder.
func DecompressZstd(src []byte) ([]byte, error) {
	dec := zstdDecPool.Get().(*zstd.Decoder)
	defer zstdDecPool.Put(dec)
	return dec.DecodeAll(src, nil)
}
