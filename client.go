package qrpc

import (
	"context"
	"crypto/tls"
	"errors"
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

var (
	_ Request  = (*RequestImpl)(nil)
	_ Response = (*ResponseImpl)(nil)

	ErrClientClosed = errors.New("client is closed")
)

type Client interface {
	NewRequest() Request
	SendRequest(ctx context.Context, req Request) (Response, error)
	ReleaseResponse(resp Response)
	SendEvent(ctx context.Context, req Request) error
	ReleaseRequest(req Request)
	Close()
}

type ClientImpl struct {
	conns        []*quic.Conn
	multiplexors []client.Multiplexer
	connCounter  atomic.Uint32
	encoder      internal.Encoder
	chansMap     *internal.ShardedMap
	chansPool    *sync.Pool

	closeOnce sync.Once
	closeCh   chan struct{}
	pendingWg sync.WaitGroup
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
		closeCh:      make(chan struct{}),
	}
}

var TimeoutDuration = time.Second * 30

func (c *ClientImpl) getMultiplexor() client.Multiplexer {
	idx := c.connCounter.Add(1) - 1
	return c.multiplexors[idx%uint32(len(c.multiplexors))]
}

func (c *ClientImpl) NewRequest() Request {
	return NewRequest(client.GetRequest())
}

func (c *ClientImpl) ReleaseRequest(req Request) {
	ReleaseRequest(req.(*RequestImpl))
}

func (c *ClientImpl) sendRequestInternal(req *gen.Request) (chan *gen.Response, error) {
	select {
	case <-c.closeCh:
		return nil, ErrClientClosed
	default:
	}

	if req.RequestId == 0 {
		req.RequestId = rand.Uint64()
	}

	ch := c.getChan()
	c.chansMap.Store(req.RequestId, ch)

	buf, err := c.encoder.EncodeRequest(req)
	client.ReleaseRequest(req)
	if err != nil {
		c.chansMap.Delete(req.RequestId)
		c.putChan(ch)
		return nil, err
	}

	mux := c.getMultiplexor()
	stream, err := mux.GetStream()
	if err != nil {
		c.chansMap.Delete(req.RequestId)
		c.putChan(ch)
		return nil, err
	}

	if err := stream.SetWriteDeadline(time.Now().Add(TimeoutDuration)); err != nil {
		c.chansMap.Delete(req.RequestId)
		c.putChan(ch)
		buf.Release()
		return nil, err
	}

	batcher := mux.(*client.MultiplexerImpl).GetBatcher(stream)
	err = batcher.Write(buf.Bytes())
	buf.Release()

	if err != nil {
		c.chansMap.Delete(req.RequestId)
		c.putChan(ch)
		return nil, err
	}

	return ch, nil
}

func (c *ClientImpl) waitResponse(
	ctx context.Context,
	ch chan *gen.Response,
	id uint64,
) (*gen.Response, error) {
	c.pendingWg.Add(1)
	defer c.pendingWg.Done()

	select {
	case <-ctx.Done():
		c.putChan(ch)
		c.chansMap.Delete(id)
		return nil, ctx.Err()

	case <-c.closeCh:
		c.putChan(ch)
		c.chansMap.Delete(id)
		return nil, ErrClientClosed

	case v := <-ch:
		c.putChan(ch)
		return v, nil
	}
}

func (c *ClientImpl) SendRequest(
	ctx context.Context,
	req Request,
) (Response, error) {
	impl := req.(*RequestImpl)
	r := impl.Req()
	if r.RequestId == 0 {
		r.RequestId = rand.Uint64()
	}

	id := r.RequestId

	ch, err := c.sendRequestInternal(r)
	ReleaseRequest(impl)
	if err != nil {
		return nil, err
	}

	resp, err := c.waitResponse(ctx, ch, id)
	if err != nil {
		return nil, err
	}

	return NewResponse(resp), nil
}

func (c *ClientImpl) sendEventInternal(req *gen.Request) error {
	select {
	case <-c.closeCh:
		return ErrClientClosed
	default:
	}

	req.RequestId = 0
	buf, err := c.encoder.EncodeEvent(req)
	client.ReleaseRequest(req)
	if err != nil {
		return err
	}

	mux := c.getMultiplexor()
	stream, err := mux.GetStream()
	if err != nil {
		buf.Release()
		return err
	}

	if err := stream.SetWriteDeadline(time.Now().Add(TimeoutDuration)); err != nil {
		buf.Release()
		return err
	}

	batcher := mux.(*client.MultiplexerImpl).GetBatcher(stream)
	err = batcher.Write(buf.Bytes())
	buf.Release()

	return err
}

func (c *ClientImpl) SendEvent(
	ctx context.Context,
	req Request,
) error {
	impl := req.(*RequestImpl)
	err := c.sendEventInternal(impl.Req())
	ReleaseRequest(impl)
	return err
}

func (c *ClientImpl) ReleaseResponse(resp Response) {
	impl := resp.(*ResponseImpl)
	client.ReleaseResponse(impl.Resp())
	ReleaseResponse(impl)
}

func (c *ClientImpl) getChan() chan *gen.Response {
	return c.chansPool.Get().(chan *gen.Response)
}

func (c *ClientImpl) putChan(ch chan *gen.Response) {
	c.chansPool.Put(ch)
}

func (c *ClientImpl) Close() {
	c.closeOnce.Do(func() {
		close(c.closeCh)

		c.pendingWg.Wait()

		for _, m := range c.multiplexors {
			m.Close()
		}
	})
}
