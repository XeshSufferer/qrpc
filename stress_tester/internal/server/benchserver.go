package server

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/XeshSufferer/qrpc"
	"github.com/XeshSufferer/qrpc/internal"
	"github.com/XeshSufferer/qrpc/stress_tester/internal/tls"
)

type BenchServer struct {
	addr    string
	qrpc    qrpc.QRpcServer
	cpuLoad bool
	wg      sync.WaitGroup
}

func NewBenchServer(addr string, cpuLoad bool) *BenchServer {
	return &BenchServer{
		addr:    addr,
		cpuLoad: cpuLoad,
	}
}

func (s *BenchServer) Start() error {
	tlsCfg, err := tls.GetQuicTLSConfig()
	if err != nil {
		return fmt.Errorf("tls config: %w", err)
	}

	server, err := qrpc.NewServer(s.addr, tlsCfg)
	if err != nil {
		return fmt.Errorf("new server: %w", err)
	}

	s.qrpc = server

	s.qrpc.AddHandler("echo", func(ctx internal.Ctx) {
		if s.cpuLoad {
			simulateCPU(100)
		}
		ctx.SetCode(200)
		ctx.SetBody(nil)
		ctx.SetHeaders(nil)
	})

	s.qrpc.AddHandler("upload", func(ctx internal.Ctx) {
		if s.cpuLoad {
			simulateCPU(50)
		}
		ctx.SetCode(200)
		ctx.SetBody(nil)
		ctx.SetHeaders(nil)
	})

	s.qrpc.AddHandler("ping", func(ctx internal.Ctx) {
		ctx.SetCode(200)
		ctx.SetBody(nil)
		ctx.SetHeaders(nil)
	})

	log.Printf("[server] qRPC server listening on %s (cpu_load=%v)", s.addr, s.cpuLoad)
	return nil
}

func (s *BenchServer) Stop() error {
	log.Printf("[server] stopping ...")
	return nil
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
		return fmt.Errorf("start server: %w", err)
	}
	<-context.Background().Done()
	return nil
}
