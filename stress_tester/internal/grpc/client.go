package grpc

import (
	"context"
	"fmt"
	"time"

	"github.com/XeshSufferer/qrpc/protos/pb/gen"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	conn   *grpc.ClientConn
	client BenchServiceClient
}

func NewClient(addr string) (*Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, fmt.Errorf("grpc dial: %w", err)
	}
	return &Client{
		conn:   conn,
		client: NewBenchServiceClient(conn),
	}, nil
}

func (c *Client) SendRequest(ctx context.Context, method []byte, body []byte, headers []byte) (*gen.Response, error) {
	req := &gen.Request{
		Method:  method,
		Body:    body,
		Headers: headers,
	}
	return c.client.Call(ctx, req)
}

func (c *Client) Close() error {
	return c.conn.Close()
}
