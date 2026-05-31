package client

import (
	"encoding/binary"
	"errors"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/XeshSufferer/qrpc/protos/pb/gen"
	qrpc_quic "github.com/XeshSufferer/qrpc/transport/quic"
	"github.com/XeshSufferer/qrpc/transport/types"
	"github.com/quic-go/quic-go"
)

type Multiplexer interface {
	GetStream() (*quic.Stream, error)
}

type MultiplexerImpl struct {
	balancer qrpc_quic.Balancer
	conn     *quic.Conn
	chansMap *sync.Map
}

const StreamOpenDelay = 50 // ms

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
	chansMap *sync.Map,
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

		go m.readCycle(s)

		time.Sleep(time.Millisecond * StreamOpenDelay)
	}
}

func (m *MultiplexerImpl) GetStream() (*quic.Stream, error) {
	s, err := m.balancer.GetStream()

	if err != nil {
		return nil, err
	}

	return s, nil
}

func (m *MultiplexerImpl) readCycle(s *quic.Stream) {
	headerLengthBuff := make([]byte, 4)
	flagBuff := make([]byte, 1)

	for {
		// read frame length
		_, err := io.ReadFull(s, headerLengthBuff)
		if err != nil {

			if IsTimeoutErr(err) {
				slog.Debug("peer disconnected")
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

		// read flag
		_, err = io.ReadFull(s, flagBuff)
		if err != nil {

			if IsTimeoutErr(err) {
				slog.Debug("peer disconnected")
				return
			}

			slog.Error("error by read flag in read cycle", "err", err)
			return
		}

		// payload size excludes flag byte
		payloadLen := int(length) - 1

		reqBuff := make([]byte, payloadLen)

		// read protobuf payload
		_, err = io.ReadFull(s, reqBuff)
		if err != nil {
			if IsTimeoutErr(err) {
				slog.Debug("peer disconnected")
				return
			}

			slog.Error("error by read request payload in read cycle", "err", err)
			return
		}

		switch flagBuff[0] {

		case types.RESPONSE_FLAG:
			resp := GetResponse()

			err := resp.UnmarshalVT(reqBuff)

			if err != nil {
				slog.Error("error by unmarshal response buffer", "err", err)
				ReleaseResponse(resp)
				continue
			}

			if v, ok := m.chansMap.Load(resp.RequestId); ok {
				ch := v.(chan *gen.Response)
				ch <- resp
				m.chansMap.Delete(resp.RequestId)
			} else {
				ReleaseResponse(resp)
			}

		case types.REQUEST_FLAG:
			req := GetRequest()

			err := req.UnmarshalVT(reqBuff)

			if err != nil {
				slog.Error("error by unmarshal request buffer", "err", err)
				ReleaseRequest(req)
				continue
			}

			if v, ok := m.chansMap.Load(req.RequestId); ok {
				ch := v.(chan *gen.Request)
				ch <- req
				m.chansMap.Delete(req.RequestId)
			} else {
				ReleaseRequest(req)
			}
		}
	}
}

func GetRequest() *gen.Request {
	return requests.Get().(*gen.Request)
}

func GetResponse() *gen.Response {
	return responses.Get().(*gen.Response)
}

func ReleaseRequest(req *gen.Request) {
	req.Reset()
	requests.Put(req)
}

func ReleaseResponse(resp *gen.Response) {
	resp.Reset()
	responses.Put(resp)
}

func IsTimeoutErr(err error) bool {
	var idleTimeoutErr *quic.IdleTimeoutError

	if errors.As(err, &idleTimeoutErr) {
		return true
	}

	return false
}
