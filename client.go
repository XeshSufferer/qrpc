package qrpc

import (
	"context"
	"crypto/tls"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/XeshSufferer/qrpc/internal"
	"github.com/XeshSufferer/qrpc/protos/pb/gen"
	qrpc_quic "github.com/XeshSufferer/qrpc/transport/quic"
	"github.com/XeshSufferer/qrpc/transport/quic/client"
	"github.com/quic-go/quic-go"
)

type Client interface {
	SendRequest(c context.Context, method, body, headers []byte) (*gen.Response, error)
	SendRawRequest(c context.Context, req *gen.Request) (*gen.Response, error)
}

type ClientImpl struct {
	Conn        *quic.Conn
	multiplexor client.Multiplexer
	encoder     internal.Encoder
	chansMap    *internal.ShardedMap
	chansPool   *sync.Pool
}

func NewClient(ctx context.Context, addr string, tls *tls.Config) (Client, error) {

	config := &quic.Config{
		KeepAlivePeriod: 15 * time.Second,
		MaxIdleTimeout:  0,
	}

	conn, err := quic.DialAddr(ctx, addr, tls, config)

	if err != nil {
		return nil, err
	}

	return newClient(conn), nil
}

func newClient(conn *quic.Conn) Client {
	chans := internal.NewShardedMap()
	chansPool := &sync.Pool{
		New: func() any {
			return make(chan *gen.Response, 1)
		},
	}
	qrpc := ClientImpl{
		Conn:      conn,
		chansMap:  chans,
		chansPool: chansPool,
	}

	qrpc.multiplexor = client.NewMultiplexer(conn, qrpc_quic.NewBalancer(), 32, chans)
	qrpc.encoder = internal.NewEncoder()
	return &qrpc
}

var TimeoutDuration = time.Second * 8

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

	stream, err := clientimpl.multiplexor.GetStream()
	if err != nil {
		return nil, err
	}

	_, err = stream.Write(buf)
	if err != nil {
		return nil, err
	}

	clientimpl.encoder.ReleaseBuffer(buf)

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
	method,
	body,
	headers []byte,
) (*gen.Response, error) {

	r := client.GetRequest()

	r.Method = method
	r.Body = body
	r.Headers = headers
	r.RequestId = rand.Uint64()

	ch, err := clientimpl.sendRequestInternal(r)
	if err != nil {
		return nil, err
	}

	return clientimpl.waitResponse(ctx, ch, r.RequestId)
}

func (clientimpl *ClientImpl) SendRawRequest(
	c context.Context,
	req *gen.Request,
) (*gen.Response, error) {

	ctx, cancel := context.WithTimeout(c, TimeoutDuration)
	defer cancel()

	ch, err := clientimpl.sendRequestInternal(req)
	if err != nil {
		return nil, err
	}

	return clientimpl.waitResponse(ctx, ch, req.RequestId)
}

func (c *ClientImpl) getChan() chan *gen.Response {
	return c.chansPool.Get().(chan *gen.Response)
}

func (c *ClientImpl) putChan(ch chan *gen.Response) {
	c.chansPool.Put(ch)
}
