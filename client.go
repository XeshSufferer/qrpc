package qrpc

import (
	"context"
	"crypto/tls"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	quic "github.com/XeshSufferer/aquic-go"
	"github.com/XeshSufferer/qrpc/internal"
	"github.com/XeshSufferer/qrpc/protos/pb/gen"
	qrpc_quic "github.com/XeshSufferer/qrpc/transport/quic"
	"github.com/XeshSufferer/qrpc/transport/quic/client"
)

var _ Request = (*RequestImpl)(nil)
var _ Response = (*ResponseImpl)(nil)

type Client interface {
	NewRequest() Request
	SendRequest(ctx context.Context, reqCtx Request) (Response, error)
	ReleaseResponse(respCtx Response)
	SendEvent(ctx context.Context, reqCtx Request) error
}

type ClientImpl struct {
	conns        []*quic.Conn
	multiplexors []client.Multiplexer
	connCounter  atomic.Uint32
	encoder      internal.Encoder
	chansMap     *internal.ShardedMap
	chansPool    *sync.Pool
}

func NewClient(ctx context.Context, addr string, tls *tls.Config, connsCount int) (Client, error) {

	config := &quic.Config{
		KeepAlivePeriod: 15 * time.Second,
		MaxIdleTimeout:  60 * time.Second,

		InitialStreamReceiveWindow: 8 << 20,  // 8 MB
		MaxStreamReceiveWindow:     32 << 20, // 32 MB

		InitialConnectionReceiveWindow: 16 << 20, // 16 MB
		MaxConnectionReceiveWindow:     64 << 20, // 64 MB

		MaxIncomingStreams:      10000,
		HandshakeIdleTimeout:    30 * time.Second,
		DisablePathMTUDiscovery: true,
		InitialPacketSize:       1452,
	}

	if connsCount < 1 {
		connsCount = 1
	}

	conns := make([]*quic.Conn, 0, connsCount)
	for i := 0; i < connsCount; i++ {
		conn, err := quic.DialAddr(ctx, addr, tls, config)
		if err != nil {
			for _, c := range conns {
				c.CloseWithError(0, "")
			}
			return nil, err
		}
		conns = append(conns, conn)
	}

	return newClient(conns), nil
}

func newClient(conns []*quic.Conn) Client {
	chans := internal.NewShardedMap()
	chansPool := &sync.Pool{
		New: func() any {
			return make(chan *gen.Response, 1)
		},
	}

	multiplexors := make([]client.Multiplexer, len(conns))
	for i, conn := range conns {
		multiplexors[i] = client.NewMultiplexer(conn, qrpc_quic.NewBalancer(), 32, chans)
	}

	return &ClientImpl{
		conns:        conns,
		multiplexors: multiplexors,
		chansMap:     chans,
		chansPool:    chansPool,
		encoder:      internal.NewEncoder(),
	}
}

var TimeoutDuration = time.Second * 30

func (clientimpl *ClientImpl) getMultiplexor() client.Multiplexer {
	idx := clientimpl.connCounter.Add(1) - 1
	return clientimpl.multiplexors[idx%uint32(len(clientimpl.multiplexors))]
}

func (clientimpl *ClientImpl) NewRequest() Request {
	return NewRequest(client.GetRequest())
}

func (clientimpl *ClientImpl) sendRequestInternal(req *gen.Request) (chan *gen.Response, error) {
	if req.RequestId == 0 {
		req.RequestId = rand.Uint64()
	}

	ch := clientimpl.getChan()

	clientimpl.chansMap.Store(req.RequestId, ch)

	buf, err := clientimpl.encoder.EncodeRequest(req)
	client.ReleaseRequest(req)
	if err != nil {
		return nil, err
	}

	mux := clientimpl.getMultiplexor()
	stream, err := mux.GetStream()
	if err != nil {
		return nil, err
	}

	if err := stream.SetWriteDeadline(time.Now().Add(TimeoutDuration)); err != nil {
		buf.Release()
		return nil, err
	}

	_, err = stream.Write(buf.Bytes())
	buf.Release()

	if err != nil {
		return nil, err
	}

	return ch, nil
}

func (clientimpl *ClientImpl) waitResponse(
	ctx context.Context,
	ch chan *gen.Response,
	id uint64,
) (*gen.Response, error) {

	select {
	case <-ctx.Done():
		clientimpl.putChan(ch)
		clientimpl.chansMap.Delete(id)
		return nil, ctx.Err()

	case v := <-ch:
		clientimpl.putChan(ch)
		return v, nil
	}
}

func (clientimpl *ClientImpl) SendRequest(
	ctx context.Context,
	reqCtx Request,
) (Response, error) {

	impl := reqCtx.(*RequestImpl)
	r := impl.Req()
	if r.RequestId == 0 {
		r.RequestId = rand.Uint64()
	}

	id := r.RequestId

	ch, err := clientimpl.sendRequestInternal(r)
	ReleaseRequest(impl)
	if err != nil {
		return nil, err
	}

	resp, err := clientimpl.waitResponse(ctx, ch, id)
	if err != nil {
		return nil, err
	}

	return NewResponse(resp), nil
}

func (clientimpl *ClientImpl) sendEventInternal(req *gen.Request) error {
	req.RequestId = 0

	buf, err := clientimpl.encoder.EncodeEvent(req)
	client.ReleaseRequest(req)
	if err != nil {
		return err
	}

	mux := clientimpl.getMultiplexor()
	stream, err := mux.GetStream()
	if err != nil {
		buf.Release()
		return err
	}

	if err := stream.SetWriteDeadline(time.Now().Add(TimeoutDuration)); err != nil {
		buf.Release()
		return err
	}

	_, err = stream.Write(buf.Bytes())
	buf.Release()

	return err
}

func (clientimpl *ClientImpl) SendEvent(
	ctx context.Context,
	reqCtx Request,
) error {

	impl := reqCtx.(*RequestImpl)
	err := clientimpl.sendEventInternal(impl.Req())
	ReleaseRequest(impl)
	return err
}

func (c *ClientImpl) ReleaseResponse(respCtx Response) {
	impl := respCtx.(*ResponseImpl)
	client.ReleaseResponse(impl.Resp())
	ReleaseResponse(impl)
}

func (c *ClientImpl) getChan() chan *gen.Response {
	return c.chansPool.Get().(chan *gen.Response)
}

func (c *ClientImpl) putChan(ch chan *gen.Response) {
	c.chansPool.Put(ch)
}
