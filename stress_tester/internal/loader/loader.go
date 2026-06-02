package loader

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"fmt"
	"log"
	"math"
	mrand "math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/XeshSufferer/qrpc"
	"github.com/XeshSufferer/qrpc/stress_tester/internal/config"
	"github.com/XeshSufferer/qrpc/stress_tester/internal/grpc"
	"github.com/XeshSufferer/qrpc/stress_tester/internal/metrics"
	itls "github.com/XeshSufferer/qrpc/stress_tester/internal/tls"
	"github.com/XeshSufferer/qrpc/protos/pb/gen"
)

type RPCClient interface {
	SendRequest(ctx context.Context, method []byte, body []byte, headers []byte) (*gen.Response, error)
	Close() error
}

type qrpcClientWrapper struct {
	client qrpc.Client
}

func (w *qrpcClientWrapper) SendRequest(ctx context.Context, method []byte, body []byte, headers []byte) (*gen.Response, error) {
	return w.client.SendRequest(ctx, method, body, headers)
}

func (w *qrpcClientWrapper) Close() error {
	return nil
}

type LoadGenerator struct {
	cfg        config.LoadConfig
	addr       string
	collector  *metrics.Collector
	wg         sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc
	collecting atomic.Bool
}

func New(cfg config.LoadConfig, addr string, collector *metrics.Collector) *LoadGenerator {
	return &LoadGenerator{
		cfg:       cfg,
		addr:      addr,
		collector: collector,
	}
}

func (lg *LoadGenerator) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	lg.ctx = ctx
	lg.cancel = cancel

	var client RPCClient
	var err error

	switch lg.cfg.System {
	case config.SystemGRPC:
		client, err = grpc.NewClient(lg.addr)
	default:
		var tlsCfg *tls.Config
		tlsCfg, err = itls.GetQuicTLSConfig()
		if err != nil {
			return fmt.Errorf("tls config: %w", err)
		}
		var qc qrpc.Client
		qc, err = qrpc.NewClient(ctx, lg.addr, tlsCfg, lg.cfg.Connections)
		if err == nil {
			client = &qrpcClientWrapper{qc}
		}
	}
	if err != nil {
		return fmt.Errorf("new client: %w", err)
	}

	lg.collector.StartTimer()

	for i := 0; i < lg.cfg.Workers; i++ {
		lg.wg.Add(1)
		go lg.worker(ctx, i, client)
	}

	return nil
}

func (lg *LoadGenerator) Warmup(duration time.Duration) {
	log.Printf("[loader] warmup %s ...", duration)
	select {
	case <-lg.ctx.Done():
	case <-time.After(duration):
	}
	lg.collecting.Store(true)
	log.Printf("[loader] collecting started")
}

func (lg *LoadGenerator) Run(duration time.Duration) {
	log.Printf("[loader] running for %s ...", duration)
	select {
	case <-lg.ctx.Done():
	case <-time.After(duration):
	}
	lg.collecting.Store(false)
	lg.cancel()

	done := make(chan struct{})
	go func() {
		lg.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-lg.ctx.Done():
		log.Printf("[loader] interrupted while waiting for workers")
	}

	lg.collector.StopTimer()
	log.Printf("[loader] done")
}

func (lg *LoadGenerator) worker(ctx context.Context, id int, client RPCClient) {
	defer lg.wg.Done()

	rng := mrand.New(mrand.NewSource(time.Now().UnixNano() + int64(id)))

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		payload := lg.nextPayload(rng)
		method := []byte(lg.cfg.Method)

		start := time.Now()

		resp, err := client.SendRequest(ctx, method, payload, nil)

		latency := time.Since(start)

		if !lg.collecting.Load() {
			continue
		}

		if err != nil {
			lg.collector.Record(metrics.Sample{
				Start:       start,
				Duration:    latency,
				Success:     false,
				Error:       err.Error(),
				PayloadSize: len(payload),
				WorkerID:    id,
			})
			continue
		}

		if resp == nil {
			lg.collector.Record(metrics.Sample{
				Start:       start,
				Duration:    latency,
				Success:     false,
				Error:       "nil response without error",
				PayloadSize: len(payload),
				WorkerID:    id,
			})
			continue
		}

		if resp.Code != 200 {
			lg.collector.Record(metrics.Sample{
				Start:       start,
				Duration:    latency,
				Success:     false,
				Error:       fmt.Sprintf("unexpected code %d", resp.Code),
				PayloadSize: len(payload),
				WorkerID:    id,
			})
			continue
		}

		lg.collector.Record(metrics.Sample{
			Start:       start,
			Duration:    latency,
			Success:     true,
			PayloadSize: len(payload),
			WorkerID:    id,
		})
	}
}

func (lg *LoadGenerator) nextPayload(rng *mrand.Rand) []byte {
	switch lg.cfg.Workload {
	case config.WorkloadFixed:
		return makePayload(lg.cfg.PayloadSize)

	case config.WorkloadRandom:
		size := lg.cfg.MinPayload + rng.Intn(lg.cfg.MaxPayload-lg.cfg.MinPayload+1)
		return makePayload(size)

	case config.WorkloadMixed:
		if rng.Float64() < 0.8 {
			return makePayload(64 + rng.Intn(1024-64))
		}
		mb := 1 + rng.Intn(10)
		return makePayload(mb * 1024 * 1024)

	default:
		return makePayload(1024)
	}
}

func makePayload(size int) []byte {
	buf := make([]byte, size)
	if size <= 1024 {
		for i := range buf {
			buf[i] = byte(i % 256)
		}
		return buf
	}
	_, err := rand.Read(buf)
	if err != nil {
		for i := range buf {
			buf[i] = byte(i % 256)
		}
	}
	return buf
}

type StreamPool struct {
	mu       sync.Mutex
	streams  []int
	acquired int
}

func NewStreamPool(count int) *StreamPool {
	return &StreamPool{
		streams: make([]int, count),
	}
}

func (sp *StreamPool) Acquire(timeout time.Duration) (int, error) {
	deadline := time.Now().Add(timeout)
	for {
		sp.mu.Lock()
		for i, v := range sp.streams {
			if v == 0 {
				sp.streams[i] = 1
				sp.acquired++
				sp.mu.Unlock()
				return i, nil
			}
		}
		sp.mu.Unlock()

		if time.Now().After(deadline) {
			return -1, fmt.Errorf("stream acquire timeout")
		}
		time.Sleep(100 * time.Microsecond)
	}
}

func (sp *StreamPool) Release(idx int) {
	sp.mu.Lock()
	sp.streams[idx] = 0
	sp.acquired--
	sp.mu.Unlock()
}

type AggregatedResult struct {
	Scenario   string
	Profile    string
	System     config.RPCSystem
	Config     config.LoadConfig
	Report     *metrics.Report
	KneePoints map[string]time.Duration
}

func CalculateKneePoints(results []AggregatedResult, metric string) map[float64]float64 {
	if len(results) < 3 {
		return nil
	}

	points := make(map[float64]float64)

	for i := 1; i < len(results)-1; i++ {
		prev := latencyForMetric(results[i-1].Report, metric)
		curr := latencyForMetric(results[i].Report, metric)
		next := latencyForMetric(results[i+1].Report, metric)

		prevSlope := curr - prev
		nextSlope := next - curr

		if nextSlope > prevSlope*2 {
			profileName := results[i].Profile
			var profileVal float64
			switch metric {
			case "loss":
				profile := config.DefaultProfiles[profileName]
				profileVal = profile.Loss
			case "delay":
				profile := config.DefaultProfiles[profileName]
				d, _ := time.ParseDuration(profile.Delay)
				profileVal = d.Seconds() * 1000
			}
			if profileVal > 0 {
				points[profileVal] = curr.Seconds() * 1000
			}
		}
	}

	return points
}

func latencyForMetric(r *metrics.Report, metric string) time.Duration {
	if r == nil {
		return 0
	}
	switch metric {
	case "avg":
		return r.AvgLatency
	case "p50":
		return r.P50
	case "p95":
		return r.P95
	case "p99":
		return r.P99
	default:
		return r.P95
	}
}

func FormatDuration(d time.Duration) string {
	switch {
	case d >= time.Second:
		return fmt.Sprintf("%.3fs", d.Seconds())
	case d >= time.Millisecond:
		return fmt.Sprintf("%.2fms", float64(d.Microseconds())/1000)
	case d >= time.Microsecond:
		return fmt.Sprintf("%.2fµs", float64(d.Nanoseconds())/1000)
	default:
		return fmt.Sprintf("%dns", d.Nanoseconds())
	}
}

func FormatRPS(rps float64) string {
	switch {
	case rps >= 1000000:
		return fmt.Sprintf("%.2fM", rps/1000000)
	case rps >= 1000:
		return fmt.Sprintf("%.2fK", rps/1000)
	default:
		return fmt.Sprintf("%.2f", rps)
	}
}

func FormatFloat(v float64) string {
	if v == math.Trunc(v) {
		return fmt.Sprintf("%.0f", v)
	}
	return fmt.Sprintf("%.2f", v)
}
