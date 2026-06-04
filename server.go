package qrpc

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"io"
	"log"
	"log/slog"
	"time"

	"github.com/XeshSufferer/qrpc/internal"
	"github.com/XeshSufferer/qrpc/protos/pb/gen"
	"github.com/XeshSufferer/qrpc/transport/quic/client"
	"github.com/XeshSufferer/qrpc/transport/types"
	"github.com/quic-go/quic-go"
)

type QRpcServer interface {
	startListen()
	AddHandler(method string, handler func(*gen.Request, *gen.Response))
}

type QRPCServerImpl struct {
	listener *quic.Listener
	conns    map[uint32]*quic.Conn
	handlers map[string]func(*gen.Request, *gen.Response)
	encoder  internal.Encoder
}

func NewServer(addr string, tls *tls.Config) (QRpcServer, error) {

	config := &quic.Config{
		KeepAlivePeriod: 15 * time.Second,
		MaxIdleTimeout:  0,

		InitialStreamReceiveWindow: 8 << 20,  // 8 MB
		MaxStreamReceiveWindow:     32 << 20, // 32 MB

		InitialConnectionReceiveWindow: 16 << 20, // 16 MB
		MaxConnectionReceiveWindow:     64 << 20, // 64 MB

		MaxIncomingStreams:   10000,
		HandshakeIdleTimeout: 30 * time.Second,
	}

	listener, err := quic.ListenAddr(addr, tls, config)

	if err != nil {
		return nil, err
	}

	server := newServer(listener)
	go server.startListen()

	return server, nil
}

func newServer(listener *quic.Listener) QRpcServer {
	return &QRPCServerImpl{
		listener: listener,
		conns:    make(map[uint32]*quic.Conn, 4),
		handlers: make(map[string]func(*gen.Request, *gen.Response), 4),
		encoder:  internal.NewEncoder(),
	}
}

func (s *QRPCServerImpl) AddHandler(method string, handler func(*gen.Request, *gen.Response)) {
	s.handlers[method] = handler
}

func (s *QRPCServerImpl) startListen() {

	for {
		conn, err := s.listener.Accept(context.Background())

		if err != nil {

			slog.Error("Error by accepting QUIC connection", "Error", err)
			return
		}

		go s.connReadCycle(conn)
	}
}

func (s *QRPCServerImpl) connReadCycle(conn *quic.Conn) {
	for {
		stream, err := conn.AcceptStream(conn.Context())

		if err != nil {
			if IsTimeoutErr(err) {
				slog.Debug("peer disconnected")
				return
			}

			slog.Error("Error by accepting QUIC stream", "Error", err)
			return
		}

		go s.streamReadCycle(stream)
	}
}

func (s *QRPCServerImpl) streamReadCycle(stream *quic.Stream) {
	headerLengthBuff := make([]byte, 4)
	flagBuff := make([]byte, 1)

	var reqBuff []byte

	for {
		if stream == nil {
			log.Println("Stream dead. It's ok")
			return
		}

		_, err := io.ReadFull(stream, headerLengthBuff)
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

		_, err = io.ReadFull(stream, flagBuff)
		if err != nil {
			slog.Error("error by read flag in read cycle", "err", err)
			return
		}

		payloadLen := int(length) - 1

		if cap(reqBuff) < payloadLen {
			reqBuff = make([]byte, payloadLen)
		}

		reqBuff = reqBuff[:payloadLen]

		_, err = io.ReadFull(stream, reqBuff)
		if err != nil {
			slog.Error("error by read request payload in read cycle", "err", err)
			return
		}

		switch flagBuff[0] {
		case types.REQUEST_FLAG:
			req := client.GetRequest()
			resp := client.GetResponse()

			err := req.UnmarshalVT(reqBuff)
			if err != nil {
				slog.Error("error by unmarshal request buffer", "err", err)
				client.ReleaseRequest(req)
				client.ReleaseResponse(resp)
				continue
			}

			handler, exists := s.handlers[string(req.Method)]

			if !exists {
				slog.Error("handler not found", "method", string(req.Method))
				client.ReleaseRequest(req)
				client.ReleaseResponse(resp)
				return
			}

			resp.RequestId = req.RequestId

			handler(req, resp)

			client.ReleaseRequest(req)

			buf, err := s.encoder.EncodeResponse(resp)
			client.ReleaseResponse(resp)

			if err != nil {
				slog.Error("error by encoding response", "err", err)
				s.encoder.ReleaseBuffer(buf)
				continue
			}

			_, err = stream.Write(buf)

			s.encoder.ReleaseBuffer(buf)

			if err != nil {
				slog.Error("error by writing response buffer", "err", err)
				return
			}
		}
	}
}

func IsTimeoutErr(err error) bool {
	var idleTimeoutErr *quic.IdleTimeoutError

	if errors.As(err, &idleTimeoutErr) {
		return true
	}

	return false
}
