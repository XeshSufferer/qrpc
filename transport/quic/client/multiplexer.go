package client

import (
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"sync"
	"time"

	quic "github.com/XeshSufferer/aquic-go"
	"github.com/XeshSufferer/qrpc/internal"
	"github.com/XeshSufferer/qrpc/protos/pb/gen"
	qrpc_quic "github.com/XeshSufferer/qrpc/transport/quic"
	"github.com/XeshSufferer/qrpc/transport/types"
)

type Multiplexer interface {
	GetStream() (*quic.Stream, error)
	Close()
}

type MultiplexerImpl struct {
	balancer qrpc_quic.Balancer
	conn     *quic.Conn
	chansMap *internal.ShardedMap
	batchers sync.Map
}

const StreamOpenDelay = 10 // ms

var requests = &sync.Pool{
	New: func() any {
		return &gen.Request{}
	},
}

var responses = &sync.Pool{
	New: func() any {
		return &gen.Response{}
	},
}

func NewMultiplexer(
	conn *quic.Conn,
	quicbalancer qrpc_quic.Balancer,
	streamsCount uint16,
	chansMap *internal.ShardedMap,
) Multiplexer {
	m := &MultiplexerImpl{
		balancer: quicbalancer,
		conn:     conn,
		chansMap: chansMap,
	}

	go m.start(streamsCount)

	return m
}

func (m *MultiplexerImpl) start(c uint16) {
	for i := uint16(0); i < c; i++ {
		s, err := m.conn.OpenStream()

		if err != nil {
			slog.Error("error by open multiplex streams", "err", err)
			time.Sleep(time.Millisecond * StreamOpenDelay)
			i--
			continue
		}

		m.balancer.AddStream(s)
		m.batchers.Store(s, NewBatcher(s))

		go m.readCycle(s)

		time.Sleep(time.Millisecond * StreamOpenDelay)
	}
}

func (m *MultiplexerImpl) GetBatcher(s *quic.Stream) *Batcher {
	v, ok := m.batchers.Load(s)
	if !ok {
		b := NewBatcher(s)
		m.batchers.Store(s, b)
		return b
	}
	return v.(*Batcher)
}

func (m *MultiplexerImpl) GetStream() (*quic.Stream, error) {
	s, err := m.balancer.GetStream()

	if err != nil {
		return nil, err
	}

	return s, nil
}

func (m *MultiplexerImpl) Close() {
	m.balancer.Reset()
}

func (m *MultiplexerImpl) readCycle(s *quic.Stream) {
	defer m.batchers.Delete(s)

	headerLengthBuff := make([]byte, 4)
	flagBuff := make([]byte, 1)

	var reqBuff []byte

	for {
		if err := m.GetBatcher(s).Flush(); err != nil {
			slog.Error("error by flush batch buffer", "err", err)
			return
		}
		_, err := io.ReadFull(s, headerLengthBuff)
		if err != nil {
			if IsTimeoutErr(err) {
				slog.Debug("peer disconnected")
				return
			}
			if isApplicationErr(err) {
				slog.Debug("client closed connection")
				return
			}
			slog.Error("error by read header length in read cycle", "err", err)
			return
		}

		length := binary.BigEndian.Uint32(headerLengthBuff)

		if length < 1 {
			slog.Error("invalid frame length", "length", length)
			return
		}

		_, err = io.ReadFull(s, flagBuff)
		if err != nil {
			if IsTimeoutErr(err) {
				slog.Debug("peer disconnected")
				return
			}
			if isApplicationErr(err) {
				slog.Debug("client closed connection")
				return
			}

			slog.Error("error by read flag in read cycle", "err", err)
			return
		}

		payloadLen := int(length) - 1

		if cap(reqBuff) < payloadLen {
			reqBuff = make([]byte, payloadLen)
		}

		reqBuff = reqBuff[:payloadLen]

		_, err = io.ReadFull(s, reqBuff)
		if err != nil {
			if IsTimeoutErr(err) {
				slog.Debug("peer disconnected")
				return
			}
			if isApplicationErr(err) {
				slog.Debug("client closed connection")
				return
			}

			slog.Error("error by read request payload in read cycle", "err", err)
			return
		}

		switch flagBuff[0] {
		case types.RESPONSE_FLAG, types.RESPONSE_ZSTD_FLAG:
			data, err := maybeDecompress(reqBuff, flagBuff[0])
			if err != nil {
				slog.Error("error by decompress response", "err", err)
				continue
			}

			resp := GetResponse()

			err = resp.UnmarshalVT(data)
			if err != nil {
				slog.Error("error by unmarshal response buffer", "err", err)
				ReleaseResponse(resp)
				continue
			}

			if v, ok := m.chansMap.LoadAndDelete(resp.RequestId); ok {
				ch := v.(chan *gen.Response)
				select {
				case ch <- resp:
				default:
					ReleaseResponse(resp)
				}
			} else {
				ReleaseResponse(resp)
			}

		case types.REQUEST_FLAG, types.REQUEST_ZSTD_FLAG:
			data, err := maybeDecompress(reqBuff, flagBuff[0])
			if err != nil {
				slog.Error("error by decompress request", "err", err)
				continue
			}

			req := GetRequest()

			err = req.UnmarshalVT(data)
			if err != nil {
				slog.Error("error by unmarshal request buffer", "err", err)
				ReleaseRequest(req)
				continue
			}

			if v, ok := m.chansMap.LoadAndDelete(req.RequestId); ok {
				ch := v.(chan *gen.Request)
				select {
				case ch <- req:
				default:
					ReleaseRequest(req)
				}
			} else {
				ReleaseRequest(req)
			}

		case types.EVENT_FLAG, types.EVENT_ZSTD_FLAG:
			data, err := maybeDecompress(reqBuff, flagBuff[0])
			if err != nil {
				slog.Error("error by decompress event", "err", err)
				continue
			}

			req := GetRequest()
			err = req.UnmarshalVT(data)
			if err != nil {
				slog.Error("error by unmarshal event buffer", "err", err)
				ReleaseRequest(req)
				continue
			}

			slog.Debug("received event", "method", string(req.Method), "request_id", req.RequestId)
			ReleaseRequest(req)
		}
	}
}

func maybeDecompress(data []byte, flag byte) ([]byte, error) {
	switch flag {
	case types.REQUEST_ZSTD_FLAG, types.RESPONSE_ZSTD_FLAG, types.EVENT_ZSTD_FLAG:
		return internal.DecompressZstd(data)
	default:
		return data, nil
	}
}

func GetRequest() *gen.Request {
	return requests.Get().(*gen.Request)
}

func GetResponse() *gen.Response {
	return responses.Get().(*gen.Response)
}

func ReleaseRequest(req *gen.Request) {
	headers := req.Headers[:0]
	method := req.Method[:0]
	body := req.Body[:0]
	req.Reset()
	req.Headers = headers
	req.Method = method
	req.Body = body
	requests.Put(req)
}

func ReleaseResponse(resp *gen.Response) {
	headers := resp.Headers[:0]
	method := resp.Method[:0]
	body := resp.Body[:0]
	resp.Reset()
	resp.Headers = headers
	resp.Method = method
	resp.Body = body
	responses.Put(resp)
}

func IsTimeoutErr(err error) bool {
	var idleTimeoutErr *quic.IdleTimeoutError

	if errors.As(err, &idleTimeoutErr) {
		return true
	}

	return false
}

func isApplicationErr(err error) bool {
	var appErr *quic.ApplicationError
	return errors.As(err, &appErr)
}
