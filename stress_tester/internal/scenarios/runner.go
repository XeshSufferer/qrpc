package scenarios

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/XeshSufferer/qrpc/stress_tester/internal/config"
	"github.com/XeshSufferer/qrpc/stress_tester/internal/loader"
	"github.com/XeshSufferer/qrpc/stress_tester/internal/metrics"
	"github.com/XeshSufferer/qrpc/stress_tester/internal/netem"
)

type Runner struct {
	emu       *netem.Emulator
	outputDir string
	saveRaw   bool
	system    config.RPCSystem
	serverAddr string
	mu        sync.Mutex
}

func NewRunner(iface, outputDir string, saveRaw bool, system config.RPCSystem, serverAddr string) *Runner {
	return &Runner{
		emu:        netem.New(iface),
		outputDir:  outputDir,
		saveRaw:    saveRaw,
		system:     system,
		serverAddr: serverAddr,
	}
}

func (r *Runner) RunScenario(ctx context.Context, scenarioName string, profiles []string, lc config.LoadConfig) ([]metrics.Report, error) {
	scenarioCfg, ok := config.DefaultScenarios[scenarioName]
	if !ok {
		return nil, fmt.Errorf("unknown scenario: %s", scenarioName)
	}

	if len(profiles) == 0 {
		profiles = scenarioCfg.Profiles
	}

	reports := make([]metrics.Report, 0, len(profiles))

	for _, profileName := range profiles {
		select {
		case <-ctx.Done():
			return reports, ctx.Err()
		default:
		}

		report, err := r.runSingle(ctx, scenarioName, profileName, lc)
		if err != nil {
			log.Printf("[runner] scenario=%s profile=%s error: %v", scenarioName, profileName, err)
			continue
		}
		reports = append(reports, *report)
	}

	if err := r.emu.Clean(); err != nil {
		log.Printf("[runner] final netem cleanup: %v", err)
	}

	return reports, nil
}

func (r *Runner) runSingle(ctx context.Context, scenario, profileName string, lc config.LoadConfig) (*metrics.Report, error) {
	profile, ok := config.DefaultProfiles[profileName]
	if !ok {
		return nil, fmt.Errorf("unknown profile: %s", profileName)
	}

	log.Printf("============================================")
	log.Printf("Scenario : %s", scenario)
	log.Printf("Profile  : %s (%s)", profileName, profile.Description)
	log.Printf("Workers  : %d", lc.Workers)
	log.Printf("Streams  : %d", lc.Streams)
	log.Printf("Workload : %s", lc.Workload)
	log.Printf("Duration : %s", lc.Duration.Duration())
	log.Printf("Warmup   : %s", lc.Warmup.Duration())
	log.Printf("============================================")

	if err := r.emu.Apply(profile); err != nil {
		log.Printf("[netem] WARNING: could not apply profile '%s': %v", profileName, err)
		log.Printf("[netem] continuing without network emulation (run as root for tc netem)")
	} else {
		log.Printf("[netem] applied profile '%s': delay=%s jitter=%s loss=%.2f%%",
			profileName, profile.Delay, profile.Jitter, profile.Loss)
	}

	netemActive := true
	defer func() {
		if netemActive {
			if err := r.emu.Clean(); err != nil {
				log.Printf("[netem] cleanup skipped: %v (non-root?)", err)
			} else {
				log.Printf("[netem] cleaned up")
			}
		}
	}()

	collector := metrics.NewCollector()

	lc.System = r.system
	lg := loader.New(lc, r.serverAddr, collector)

	if err := lg.Start(ctx); err != nil {
		return nil, fmt.Errorf("start loader: %w", err)
	}

	lg.Warmup(lc.Warmup.Duration())

	lg.Run(lc.Duration.Duration())

	report := collector.Report(scenario, profileName, r.system, lc, r.saveRaw)

	log.Printf("--------------------------------------------")
	log.Printf("Results for %s / %s:", scenario, profileName)
	log.Printf("  Success : %d / %d (%.2f%%)", report.SuccessCount, report.TotalRequests, report.SuccessRate)
	log.Printf("  RPS     : %.2f", report.RPS)
	log.Printf("  Avg     : %s", report.AvgLatency)
	log.Printf("  Min     : %s", report.MinLatency)
	log.Printf("  Max     : %s", report.MaxLatency)
	log.Printf("  P50     : %s", report.P50)
	log.Printf("  P90     : %s", report.P90)
	log.Printf("  P95     : %s", report.P95)
	log.Printf("  P99     : %s", report.P99)
	log.Printf("  P99.9   : %s", report.P999)
	log.Printf("--------------------------------------------")

	jsonFile, err := metrics.SaveJSON(report, r.outputDir, "bench")
	if err != nil {
		log.Printf("[runner] save json: %v", err)
	} else {
		log.Printf("[output] %s", jsonFile)
	}

	csvFile, err := metrics.SaveCSV(report, r.outputDir, "bench")
	if err != nil {
		log.Printf("[runner] save csv: %v", err)
	} else {
		log.Printf("[output] %s", csvFile)
	}

	netemActive = false

	return report, nil
}

func RunBaselineLatency(ctx context.Context, r *Runner, lc *config.LoadConfig, system config.RPCSystem) ([]metrics.Report, error) {
	cfg := config.DefaultScenarios["baseline_latency"]
	if lc != nil {
		cfg.LoadConfig = *lc
	}
	return r.RunScenario(ctx, "baseline_latency", cfg.Profiles, cfg.LoadConfig)
}

func RunConcurrencyStress(ctx context.Context, r *Runner, lc *config.LoadConfig, system config.RPCSystem) ([]metrics.Report, error) {
	cfg := config.DefaultScenarios["concurrency_stress"]
	if lc != nil {
		cfg.LoadConfig = *lc
	}
	return r.RunScenario(ctx, "concurrency_stress", cfg.Profiles, cfg.LoadConfig)
}

func RunMultiplexStress(ctx context.Context, r *Runner, lc *config.LoadConfig, system config.RPCSystem) ([]metrics.Report, error) {
	cfg := config.DefaultScenarios["multiplex_stress"]
	if lc != nil {
		cfg.LoadConfig = *lc
	}
	return r.RunScenario(ctx, "multiplex_stress", cfg.Profiles, cfg.LoadConfig)
}

func RunLossSensitivity(ctx context.Context, r *Runner, lc *config.LoadConfig, system config.RPCSystem) ([]metrics.Report, error) {
	cfg := config.DefaultScenarios["loss_sensitivity"]
	if lc != nil {
		cfg.LoadConfig = *lc
	}
	return r.RunScenario(ctx, "loss_sensitivity", cfg.Profiles, cfg.LoadConfig)
}

func RunRTTScaling(ctx context.Context, r *Runner, lc *config.LoadConfig, system config.RPCSystem) ([]metrics.Report, error) {
	cfg := config.DefaultScenarios["rtt_scaling"]
	if lc != nil {
		cfg.LoadConfig = *lc
	}
	return r.RunScenario(ctx, "rtt_scaling", cfg.Profiles, cfg.LoadConfig)
}
