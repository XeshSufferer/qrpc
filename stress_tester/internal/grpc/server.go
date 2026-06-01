package grpc

import (
	"context"
	"fmt"
	"log"
	"net"

	"github.com/XeshSufferer/qrpc/protos/pb/gen"
	"google.golang.org/grpc"
)

type BenchServer struct {
	UnimplementedBenchServiceServer
	addr    string
	srv     *grpc.Server
	cpuLoad bool
}

func NewBenchServer(addr string, cpuLoad bool) *BenchServer {
	return &BenchServer{
		addr:    addr,
		cpuLoad: cpuLoad,
	}
}

func (s *BenchServer) Call(ctx context.Context, req *gen.Request) (*gen.Response, error) {
	if s.cpuLoad {
		simulateCPU(100)
	}
	return &gen.Response{
		Code:      200,
		RequestId: req.RequestId,
		Method:    req.Method,
	}, nil
}

func (s *BenchServer) Start() error {
	lis, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("grpc listen: %w", err)
	}
	s.srv = grpc.NewServer()
	RegisterBenchServiceServer(s.srv, s)
	log.Printf("[server] gRPC server listening on %s (cpu_load=%v)", s.addr, s.cpuLoad)
	return s.srv.Serve(lis)
}

func (s *BenchServer) Stop() {
	if s.srv != nil {
		s.srv.GracefulStop()
	}
}

func simulateCPU(n int) {
	var acc float64
	for i := 0; i < n*100000; i++ {
		acc += float64(i) * 0.000001
	}
	_ = acc
}

func RunServer(addr string, cpuLoad bool) error {
	s := NewBenchServer(addr, cpuLoad)
	if err := s.Start(); err != nil {
		return fmt.Errorf("grpc server: %w", err)
	}
	return nil
}
