package internal

import (
	"encoding/binary"
	"sync"

	"github.com/XeshSufferer/qrpc/protos/pb/gen"
	"github.com/XeshSufferer/qrpc/transport/types"
)

const (
	headerSize      = 5 // 4 bytes length + 1 byte flag
	defaultBufSize  = 1024
	maxPooledBuffer = 64 << 10 // 64KB
)

type Encoder interface {
	EncodeRequest(req *gen.Request) ([]byte, error)
	EncodeResponse(resp *gen.Response) ([]byte, error)
	ReleaseBuffer(buf []byte)
}

type EncoderImpl struct {
	buffers *sync.Pool
}

func NewEncoder() Encoder {
	pool := &sync.Pool{
		New: func() any {
			return make([]byte, defaultBufSize)
		},
	}

	return &EncoderImpl{
		buffers: pool,
	}
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
func (e *EncoderImpl) EncodeRequest(req *gen.Request) ([]byte, error) {
	return e.encodeMessage(
		req.SizeVT(),
		types.REQUEST_FLAG,
		req.MarshalToSizedBufferVT,
	)
}

func (e *EncoderImpl) EncodeResponse(resp *gen.Response) ([]byte, error) {
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
) ([]byte, error) {

	totalSize := headerSize + payloadSize

	buf := e.getBuffer(totalSize)

	// protobuf payload region
	payload := buf[headerSize:]

	// vtprotobuf writes backwards into the provided buffer
	n, err := marshal(payload)
	if err != nil {
		e.ReleaseBuffer(buf)
		return nil, err
	}

	// actual payload start after reverse write
	payloadOffset := len(payload) - n

	// move payload if marshal did not fill from beginning
	if payloadOffset != 0 {
		copy(payload[:n], payload[payloadOffset:])
	}

	// write flag
	buf[4] = flag

	// write frame length:
	// 1 byte flag + protobuf payload
	binary.BigEndian.PutUint32(
		buf[:4],
		uint32(1+n),
	)

	return buf[:headerSize+n], nil
}

func (e *EncoderImpl) ReleaseBuffer(buf []byte) {
	if buf == nil {
		return
	}

	// avoid keeping giant buffers in pool
	if cap(buf) > maxPooledBuffer {
		return
	}

	e.buffers.Put(buf[:cap(buf)])
}

func (e *EncoderImpl) getBuffer(size int) []byte {
	buf := e.buffers.Get().([]byte)

	if cap(buf) < size {
		return make([]byte, size)
	}

	return buf[:size]
}
