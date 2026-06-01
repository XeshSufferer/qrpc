package metrics

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/XeshSufferer/qrpc/stress_tester/internal/config"
)

type Sample struct {
	Start       time.Time     `json:"start"`
	Duration    time.Duration `json:"duration"`
	QueueTime   time.Duration `json:"queue_time"`
	NetworkTime time.Duration `json:"network_time"`
	Success     bool          `json:"success"`
	Error       string        `json:"error,omitempty"`
	PayloadSize int           `json:"payload_size"`
	WorkerID    int           `json:"worker_id"`
}

type Report struct {
	Timestamp     time.Time             `json:"timestamp"`
	Scenario      string                `json:"scenario"`
	Profile       string                `json:"profile"`
	System        config.RPCSystem      `json:"system"`
	LoadConfig    config.LoadConfig     `json:"load_config"`
	TotalRequests int64                 `json:"total_requests"`
	SuccessCount  int64                 `json:"success_count"`
	ErrorCount    int64                 `json:"error_count"`
	SuccessRate   float64               `json:"success_rate"`
	RPS           float64               `json:"rps"`
	AvgLatency    time.Duration         `json:"avg_latency"`
	MinLatency    time.Duration         `json:"min_latency"`
	MaxLatency    time.Duration         `json:"max_latency"`
	P50           time.Duration         `json:"p50"`
	P90           time.Duration         `json:"p90"`
	P95           time.Duration         `json:"p95"`
	P99           time.Duration         `json:"p99"`
	P999          time.Duration         `json:"p99_9"`
	LatencyDist   map[string]int        `json:"latency_distribution,omitempty"`
	Elapsed       time.Duration         `json:"elapsed"`
	RawSamples    []Sample              `json:"raw_samples,omitempty"`
}

type Collector struct {
	mu       sync.Mutex
	samples  []Sample
	started  time.Time
	elapsed  time.Duration
}

func NewCollector() *Collector {
	return &Collector{
		samples: make([]Sample, 0, 100000),
	}
}

func (c *Collector) Record(s Sample) {
	c.mu.Lock()
	c.samples = append(c.samples, s)
	c.mu.Unlock()
}

func (c *Collector) StartTimer() {
	c.mu.Lock()
	c.started = time.Now()
	c.mu.Unlock()
}

func (c *Collector) StopTimer() {
	c.mu.Lock()
	c.elapsed = time.Since(c.started)
	c.mu.Unlock()
}

func (c *Collector) Elapsed() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.elapsed
}

func (c *Collector) Report(scenario string, profile string, system config.RPCSystem, lc config.LoadConfig, saveRaw bool) *Report {
	c.mu.Lock()
	samples := make([]Sample, len(c.samples))
	copy(samples, c.samples)
	elapsed := c.elapsed
	c.mu.Unlock()

	if len(samples) == 0 {
		return &Report{
			Timestamp: time.Now(),
			Scenario:  scenario,
			Profile:   profile,
			System:    system,
			Elapsed:   elapsed,
		}
	}

	var successCount, errorCount int64
	totalLatency := time.Duration(0)
	minLatency := time.Duration(math.MaxInt64)
	maxLatency := time.Duration(0)
	latencies := make([]float64, 0, len(samples))

	for _, s := range samples {
		if s.Success {
			successCount++
			totalLatency += s.Duration
			if s.Duration < minLatency {
				minLatency = s.Duration
			}
			if s.Duration > maxLatency {
				maxLatency = s.Duration
			}
			latencies = append(latencies, float64(s.Duration))
		} else {
			errorCount++
		}
	}

	sort.Float64s(latencies)

	r := &Report{
		Timestamp:     time.Now(),
		Scenario:      scenario,
		Profile:       profile,
		System:        system,
		LoadConfig:    lc,
		TotalRequests: int64(len(samples)),
		SuccessCount:  successCount,
		ErrorCount:    errorCount,
		Elapsed:       elapsed,
	}

	if len(samples) > 0 {
		r.SuccessRate = float64(successCount) / float64(len(samples)) * 100
	}

	if elapsed > 0 {
		r.RPS = float64(successCount) / elapsed.Seconds()
	}

	if len(latencies) > 0 {
		r.AvgLatency = time.Duration(totalLatency / time.Duration(len(latencies)))
		r.MinLatency = minLatency
		r.MaxLatency = maxLatency
		r.P50 = time.Duration(percentile(latencies, 50))
		r.P90 = time.Duration(percentile(latencies, 90))
		r.P95 = time.Duration(percentile(latencies, 95))
		r.P99 = time.Duration(percentile(latencies, 99))
		r.P999 = time.Duration(percentile(latencies, 99.9))
	}

	r.LatencyDist = buildDistribution(latencies)

	if saveRaw {
		r.RawSamples = samples
	}

	return r
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if p <= 0 {
		return sorted[0]
	}
	if p >= 100 {
		return sorted[len(sorted)-1]
	}
	idx := (p / 100) * float64(len(sorted)-1)
	i := int(idx)
	frac := idx - float64(i)
	if i+1 < len(sorted) {
		return sorted[i] + frac*(sorted[i+1]-sorted[i])
	}
	return sorted[i]
}

func buildDistribution(latencies []float64) map[string]int {
	dist := make(map[string]int)
	buckets := []struct {
		name  string
		limit float64
	}{
		{"<1ms", float64(time.Millisecond)},
		{"1-5ms", float64(5 * time.Millisecond)},
		{"5-10ms", float64(10 * time.Millisecond)},
		{"10-25ms", float64(25 * time.Millisecond)},
		{"25-50ms", float64(50 * time.Millisecond)},
		{"50-100ms", float64(100 * time.Millisecond)},
		{"100-250ms", float64(250 * time.Millisecond)},
		{"250-500ms", float64(500 * time.Millisecond)},
		{"500ms-1s", float64(time.Second)},
		{"1-2s", float64(2 * time.Second)},
		{"2-5s", float64(5 * time.Second)},
		{">5s", math.MaxFloat64},
	}

	for _, l := range latencies {
		for _, b := range buckets {
			if l < b.limit {
				dist[b.name]++
				break
			}
		}
	}
	return dist
}

func SaveJSON(report *Report, outputDir, prefix string) (string, error) {
	if outputDir == "" {
		outputDir = "."
	}
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return "", fmt.Errorf("create output dir: %w", err)
	}

	filename := filepath.Join(outputDir, fmt.Sprintf("%s_%s_%s.json",
		prefix, report.Scenario, report.Profile))
	if report.System != "" {
		filename = filepath.Join(outputDir, fmt.Sprintf("%s_%s_%s_%s.json",
			prefix, report.Scenario, report.Profile, report.System))
	}

	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return "", fmt.Errorf("marshal report: %w", err)
	}

	if err := os.WriteFile(filename, data, 0644); err != nil {
		return "", fmt.Errorf("write report: %w", err)
	}

	return filename, nil
}

func SaveCSV(report *Report, outputDir, prefix string) (string, error) {
	if outputDir == "" {
		outputDir = "."
	}
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return "", fmt.Errorf("create output dir: %w", err)
	}

	filename := filepath.Join(outputDir, fmt.Sprintf("%s_%s_%s_%s.csv",
		prefix, report.Scenario, report.Profile, report.System))

	f, err := os.Create(filename)
	if err != nil {
		return "", fmt.Errorf("create csv: %w", err)
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	w.Write([]string{"timestamp", "scenario", "profile", "system",
		"workers", "streams", "workload", "total", "success", "error",
		"success_rate", "rps", "avg_latency_ns", "min_latency_ns",
		"max_latency_ns", "p50_ns", "p90_ns", "p95_ns", "p99_ns", "p99_9_ns"})

	w.Write([]string{
		report.Timestamp.Format(time.RFC3339),
		report.Scenario,
		report.Profile,
		string(report.System),
		fmt.Sprintf("%d", report.LoadConfig.Workers),
		fmt.Sprintf("%d", report.LoadConfig.Streams),
		string(report.LoadConfig.Workload),
		fmt.Sprintf("%d", report.TotalRequests),
		fmt.Sprintf("%d", report.SuccessCount),
		fmt.Sprintf("%d", report.ErrorCount),
		fmt.Sprintf("%.4f", report.SuccessRate),
		fmt.Sprintf("%.2f", report.RPS),
		fmt.Sprintf("%d", report.AvgLatency.Nanoseconds()),
		fmt.Sprintf("%d", report.MinLatency.Nanoseconds()),
		fmt.Sprintf("%d", report.MaxLatency.Nanoseconds()),
		fmt.Sprintf("%d", report.P50.Nanoseconds()),
		fmt.Sprintf("%d", report.P90.Nanoseconds()),
		fmt.Sprintf("%d", report.P95.Nanoseconds()),
		fmt.Sprintf("%d", report.P99.Nanoseconds()),
		fmt.Sprintf("%d", report.P999.Nanoseconds()),
	})

	if report.RawSamples != nil {
		for _, s := range report.RawSamples {
			errStr := ""
			if !s.Success {
				errStr = s.Error
			}
			w.Write([]string{
				s.Start.Format(time.RFC3339Nano),
				fmt.Sprintf("%d", s.Duration.Nanoseconds()),
				fmt.Sprintf("%d", s.QueueTime.Nanoseconds()),
				fmt.Sprintf("%d", s.NetworkTime.Nanoseconds()),
				fmt.Sprintf("%t", s.Success),
				errStr,
				fmt.Sprintf("%d", s.PayloadSize),
				fmt.Sprintf("%d", s.WorkerID),
			})
		}
	}

	return filename, nil
}
