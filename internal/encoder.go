package internal

import (
	"encoding/binary"
	"sync"

	"github.com/XeshSufferer/qrpc/protos/pb/gen"
	"github.com/XeshSufferer/qrpc/transport/types"
)

const (
	headerSize      = 5 // 4 bytes length + 1 byte flag
	defaultBufSize = 1024
)

type Buffer struct {
	data []byte
	used int
}

func (b *Buffer) Bytes() []byte { return b.data[:b.used] }

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

type Encoder interface {
	EncodeRequest(req *gen.Request) (*Buffer, error)
	EncodeResponse(resp *gen.Response) (*Buffer, error)
}

type EncoderImpl struct{}

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
func (e *EncoderImpl) EncodeRequest(req *gen.Request) (*Buffer, error) {
	return e.encodeMessage(
		req.SizeVT(),
		types.REQUEST_FLAG,
		req.MarshalToSizedBufferVT,
	)
}

func (e *EncoderImpl) EncodeResponse(resp *gen.Response) (*Buffer, error) {
	return e.encodeMessage(
		resp.SizeVT(),
		types.RESPONSE_FLAG,
		resp.MarshalToSizedBufferVT,
	)
}

func (e *EncoderImpl) encodeMessage(
	payloadSize int,
	flag byte,
	marshal func([]byte) (int, error),
) (*Buffer, error) {

	totalSize := headerSize + payloadSize

	b := getBuffer(totalSize)

	// protobuf payload region
	payload := b.data[headerSize:]

	// vtprotobuf writes backwards into the provided buffer
	n, err := marshal(payload)
	if err != nil {
		b.Release()
		return nil, err
	}

	// write flag
	b.data[4] = flag

	// write frame length:
	// 1 byte flag + protobuf payload
	binary.BigEndian.PutUint32(
		b.data[:4],
		uint32(1+n),
	)

	b.used = headerSize + n

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
