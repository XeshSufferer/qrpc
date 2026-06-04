package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/XeshSufferer/qrpc/stress_tester/internal/config"
	"github.com/XeshSufferer/qrpc/stress_tester/internal/loader"
	"github.com/XeshSufferer/qrpc/stress_tester/internal/metrics"
	"github.com/XeshSufferer/qrpc/stress_tester/internal/netem"
	"github.com/XeshSufferer/qrpc/stress_tester/internal/scenarios"
	"github.com/XeshSufferer/qrpc/stress_tester/internal/grpc"
	"github.com/XeshSufferer/qrpc/stress_tester/internal/server"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	cmd := os.Args[1]

	switch cmd {
	case "run":
		runBenchmark(os.Args[2:])
	case "server":
		runServer(os.Args[2:])
	case "profiles":
		listProfiles()
	case "scenarios":
		listScenarios()
	case "cleanup":
		runCleanup(os.Args[2:])
	case "help", "--help", "-h":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n\n", cmd)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Print(`qRPC Benchmark Framework — stress testing and network degradation analysis.

Usage:
  stress_test <command> [options]

Commands:
  run         Run a benchmark scenario
  server      Start the benchmark server
  profiles    List available network profiles
  scenarios   List available test scenarios
  cleanup     Clean network emulation rules

Run 'stress_test <command> -h' for command-specific help.
`)
}

func listProfiles() {
	fmt.Println("Available Network Profiles:")
	fmt.Println(strings.Repeat("-", 60))
	fmt.Printf("%-12s %-10s %-10s %-8s %s\n", "Name", "Delay", "Jitter", "Loss%", "Description")
	fmt.Println(strings.Repeat("-", 60))
	for _, name := range []string{"clean", "wifi", "lte", "bad_lte", "extreme"} {
		p := config.DefaultProfiles[name]
		fmt.Printf("%-12s %-10s %-10s %-8.1f %s\n", p.Name, p.Delay, p.Jitter, p.Loss, p.Description)
	}
	fmt.Println(strings.Repeat("-", 60))
}

func listScenarios() {
	fmt.Println("Available Test Scenarios:")
	fmt.Println(strings.Repeat("-", 70))
	for name, s := range config.DefaultScenarios {
		fmt.Printf("\n  %s:\n", name)
		fmt.Printf("    %s\n", s.Description)
		fmt.Printf("    Workers: %d, Pipelining: %d, Streams: %d, Connections: %d, Workload: %s, Duration: %s\n",
			s.LoadConfig.Workers, s.LoadConfig.Pipelining, s.LoadConfig.Streams, s.LoadConfig.Connections,
			s.LoadConfig.Workload, s.LoadConfig.Duration.Duration())
		fmt.Printf("    Profiles: %s\n", strings.Join(s.Profiles, ", "))
	}
	fmt.Println(strings.Repeat("-", 70))
}

func runCleanup(args []string) {
	fs := flag.NewFlagSet("cleanup", flag.ExitOnError)
	iface := fs.String("iface", "lo", "Network interface to clean")
	fs.Parse(args)

	emu := netem.New(*iface)
	if err := emu.Clean(); err != nil {
		if errors.Is(err, netem.ErrNotPermitted) {
			log.Printf("cleanup skipped (not root): %v", err)
		} else {
			log.Fatalf("cleanup failed: %v", err)
		}
	} else {
		log.Printf("network rules cleaned on %s", *iface)
	}
}

func runServer(args []string) {
	fs := flag.NewFlagSet("server", flag.ExitOnError)
	addr := fs.String("addr", "0.0.0.0:8081", "Server listen address")
	cpu := fs.Bool("cpu", false, "Simulate CPU load on each request")
	system := fs.String("system", "qrpc", "RPC system: qrpc, grpc")
	fs.Parse(args)

	log.Printf("starting benchmark server on %s (cpu_load=%v, system=%s)", *addr, *cpu, *system)

	switch config.RPCSystem(*system) {
	case config.SystemGRPC:
		if err := grpc.RunServer(*addr, *cpu); err != nil {
			log.Fatalf("grpc server error: %v", err)
		}
	default:
		if err := server.RunServer(*addr, *cpu); err != nil {
			log.Fatalf("server error: %v", err)
		}
	}
}

func runBenchmark(args []string) {
	fs := flag.NewFlagSet("run", flag.ExitOnError)

	scenario := fs.String("scenario", "concurrency_stress", "Scenario: baseline_latency, concurrency_stress, multiplex_stress, loss_sensitivity, rtt_scaling")
	profile := fs.String("profile", "clean", "Network profile: clean, wifi, lte, bad_lte, extreme (use comma-separated for multiple)")
	system := fs.String("system", "qrpc", "RPC system: qrpc, grpc")
	addr := fs.String("addr", "127.0.0.1:8081", "Server address")
	iface := fs.String("iface", "lo", "Network interface for emulation")
	output := fs.String("output", "results", "Output directory for results")
	raw := fs.Bool("raw", false, "Save raw latency samples")

	workers := fs.Int("workers", 0, "Override worker count (0 = use scenario default)")
	pipelining := fs.Int("pipelining", 0, "Override pipelining per worker (0 = use scenario default)")
	streams := fs.Int("streams", 0, "Override stream count (0 = use scenario default)")
	connections := fs.Int("connections", 0, "Override QUIC connection count (0 = use scenario default)")
	duration := fs.Duration("duration", 0, "Override test duration (0 = use scenario default)")
	warmup := fs.Duration("warmup", 0, "Override warmup duration (0 = use scenario default)")
	payloadSize := fs.Int("payload-size", 0, "Override fixed payload size in bytes (0 = use scenario default)")
	retryAttempts := fs.Int("retry-attempts", 3, "Number of connection retry attempts on init failure")
	retryDelay := fs.Duration("retry-delay", 500*time.Millisecond, "Delay between connection retry attempts")

	fs.Parse(args)

	profiles := strings.Split(*profile, ",")
	for i := range profiles {
		profiles[i] = strings.TrimSpace(profiles[i])
	}

	if _, ok := config.DefaultScenarios[*scenario]; !ok {
		fmt.Fprintf(os.Stderr, "unknown scenario: %s\n\n", *scenario)
		fmt.Println("Available scenarios:")
		for name := range config.DefaultScenarios {
			fmt.Printf("  - %s\n", name)
		}
		os.Exit(1)
	}

	for _, p := range profiles {
		if _, ok := config.DefaultProfiles[p]; !ok {
			fmt.Fprintf(os.Stderr, "unknown profile: %s\n\n", p)
			listProfiles()
			os.Exit(1)
		}
	}

	lc := config.DefaultScenarios[*scenario].LoadConfig
	if *workers > 0 {
		lc.Workers = *workers
	}
	if *pipelining > 0 {
		lc.Pipelining = *pipelining
	}
	if *streams > 0 {
		lc.Streams = *streams
	}
	if *connections > 0 {
		lc.Connections = *connections
	}
	if *duration > 0 {
		lc.Duration = config.Duration(*duration)
	}
	if *warmup > 0 {
		lc.Warmup = config.Duration(*warmup)
	}
	if *payloadSize > 0 {
		lc.PayloadSize = *payloadSize
		lc.Workload = config.WorkloadFixed
	}
	if *retryAttempts > 0 {
		lc.RetryAttempts = *retryAttempts
	}
	if *retryDelay > 0 {
		lc.RetryDelay = config.Duration(*retryDelay)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Printf("received %v, shutting down gracefully (press again to force)...", sig)
		cancel()

		select {
		case <-sigCh:
			log.Println("forced exit")
			os.Exit(1)
		case <-time.After(3 * time.Second):
		}
	}()

	r := scenarios.NewRunner(*iface, *output, *raw, config.RPCSystem(*system), *addr)

	log.Printf("starting benchmark: scenario=%s profiles=%v system=%s",
		*scenario, profiles, *system)

	reports, err := r.RunScenario(ctx, *scenario, profiles, lc)
	if err != nil {
		log.Fatalf("benchmark failed: %v", err)
	}

	printSummary(*scenario, profiles, reports)
}

func printSummary(scenario string, profiles []string, reports []metrics.Report) {
	fmt.Println("\n" + strings.Repeat("=", 70))
	fmt.Printf("BENCHMARK SUMMARY: %s\n", scenario)
	fmt.Println(strings.Repeat("=", 70))

	fmt.Printf("\n%-12s %-12s %-12s %-12s %-12s %-12s %-12s %-12s\n",
		"Profile", "Total", "Success%", "RPS", "Avg", "P50", "P95", "P99")
	fmt.Println(strings.Repeat("-", 100))

	for _, r := range reports {
		fmt.Printf("%-12s %-12d %-11.2f %-12s %-12s %-12s %-12s %-12s\n",
			r.Profile,
			r.TotalRequests,
			r.SuccessRate,
			loader.FormatRPS(r.RPS),
			loader.FormatDuration(r.AvgLatency),
			loader.FormatDuration(r.P50),
			loader.FormatDuration(r.P95),
			loader.FormatDuration(r.P99),
		)
	}
	fmt.Println(strings.Repeat("=", 100))

	fmt.Println("\nTail Latency Analysis:")
	fmt.Printf("%-12s %-12s %-12s %-12s %-12s\n",
		"Profile", "P95", "P99", "P99.9", "Max")
	fmt.Println(strings.Repeat("-", 64))
	for _, r := range reports {
		fmt.Printf("%-12s %-12s %-12s %-12s %-12s\n",
			r.Profile,
			loader.FormatDuration(r.P95),
			loader.FormatDuration(r.P99),
			loader.FormatDuration(r.P999),
			loader.FormatDuration(r.MaxLatency),
		)
	}

	fmt.Println("\nLatency Distribution:")
	for _, r := range reports {
		fmt.Printf("\n  %s:\n", r.Profile)
		for _, bucket := range []string{"<1ms", "1-5ms", "5-10ms", "10-25ms", "25-50ms",
			"50-100ms", "100-250ms", "250-500ms", "500ms-1s", "1-2s", "2-5s", ">5s"} {
			if count, ok := r.LatencyDist[bucket]; ok && count > 0 {
				pct := float64(count) / float64(r.SuccessCount) * 100
				bar := strings.Repeat("█", int(pct/2))
				fmt.Printf("    %-10s %8d (%5.1f%%) %s\n", bucket, count, pct, bar)
			}
		}
	}
	fmt.Println()
}
