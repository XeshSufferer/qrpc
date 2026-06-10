package grpc

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/XeshSufferer/qrpc/protos/pb/gen"
	"github.com/XeshSufferer/qrpc/stress_tester/internal/tls"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type connPair struct {
	conn   *grpc.ClientConn
	client BenchServiceClient
}

type Client struct {
	pairs  []connPair
	cursor atomic.Uint64
}

func NewClient(addr string, connsCount int) (*Client, error) {
	if connsCount < 1 {
		connsCount = 1
	}

	tlsCfg, err := tls.GetGRPCTLSConfig()
	if err != nil {
		return nil, fmt.Errorf("tls config: %w", err)
	}

	pairs := make([]connPair, connsCount)
	for i := 0; i < connsCount; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

		conn, err := grpc.DialContext(ctx, addr,
			grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)),
			grpc.WithBlock(),
		)
		cancel()

		if err != nil {
			for j := 0; j < i; j++ {
				pairs[j].conn.Close()
			}
			return nil, fmt.Errorf("grpc dial %d/%d: %w", i+1, connsCount, err)
		}

		pairs[i] = connPair{
			conn:   conn,
			client: NewBenchServiceClient(conn),
		}
	}

	return &Client{pairs: pairs}, nil
}

func (c *Client) SendRequest(ctx context.Context, method []byte, body []byte, headers []byte) (*gen.Response, error) {
	idx := c.cursor.Add(1) - 1
	p := &c.pairs[idx%uint64(len(c.pairs))]

	req := &gen.Request{
		Method:  method,
		Body:    body,
		Headers: headers,
	}
	return p.client.Call(ctx, req)
}

func (c *Client) Close() error {
	var firstErr error
	for _, p := range c.pairs {
		if err := p.conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
