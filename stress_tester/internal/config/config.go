package config

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

type Duration time.Duration

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(v)
	return nil
}

func (d Duration) Duration() time.Duration {
	return time.Duration(d)
}

type WorkloadType string

const (
	WorkloadFixed  WorkloadType = "fixed"
	WorkloadRandom WorkloadType = "random"
	WorkloadMixed  WorkloadType = "mixed"
)

type RPCSystem string

const (
	SystemQRPC RPCSystem = "qrpc"
	SystemGRPC RPCSystem = "grpc"
)

type LoadConfig struct {
	Workers       int          `json:"workers"`
	Pipelining    int          `json:"pipelining"`
	Streams       int          `json:"streams"`
	Connections   int          `json:"connections"`
	Workload      WorkloadType `json:"workload"`
	PayloadSize   int          `json:"payload_size"`
	MinPayload    int          `json:"min_payload"`
	MaxPayload    int          `json:"max_payload"`
	Duration      Duration     `json:"duration"`
	Warmup        Duration     `json:"warmup"`
	Method        string       `json:"method"`
	System        RPCSystem    `json:"system,omitempty"`
	RetryAttempts int          `json:"retry_attempts,omitempty"`
	RetryDelay    Duration     `json:"retry_delay,omitempty"`
}

type NetworkProfile struct {
	Name        string  `json:"name"`
	Description string  `json:"description"`
	Delay       string  `json:"delay"`
	Jitter      string  `json:"jitter"`
	Loss        float64 `json:"loss"`
	Rate        string  `json:"rate"`
	Correlation float64 `json:"correlation"`
}

var DefaultProfiles = map[string]NetworkProfile{
	"clean": {
		Name:        "clean",
		Description: "Clean network with minimal latency, zero loss",
		Delay:       "1ms",
		Jitter:      "0ms",
		Loss:        0,
		Rate:        "1000mbit",
	},
	"wifi": {
		Name:        "wifi",
		Description: "Typical WiFi: low latency, near-zero loss",
		Delay:       "5ms",
		Jitter:      "2ms",
		Loss:        0.1,
		Rate:        "100mbit",
	},
	"lte": {
		Name:        "lte",
		Description: "4G LTE: moderate latency, low loss",
		Delay:       "30ms",
		Jitter:      "10ms",
		Loss:        1.0,
		Rate:        "50mbit",
	},
	"bad_lte": {
		Name:        "bad_lte",
		Description: "Bad 4G LTE: high latency, moderate loss",
		Delay:       "60ms",
		Jitter:      "20ms",
		Loss:        5.0,
		Rate:        "10mbit",
	},
	"extreme": {
		Name:        "extreme",
		Description: "Extreme: very high latency, heavy loss",
		Delay:       "150ms",
		Jitter:      "50ms",
		Loss:        10.0,
		Rate:        "5mbit",
	},
	"super_extreme": {
		Name:        "super_extreme",
		Description: "Super Extreme: Super very high latency, heavy loss",
		Delay:       "200ms",
		Jitter:      "75ms",
		Loss:        15.0,
		Rate:        "3mbit",
	},
	"hell_network": {
		Name:        "hell_network",
		Description: "Hell network: Extremly high latency, ultra heavy loss",
		Delay:       "300ms",
		Jitter:      "100ms",
		Loss:        30.0,
		Rate:        "2mbit",
	},
	"no_network": {
		Name:        "no_network",
		Description: "No network: Ultra Extremly high latency, ultra extremly heavy loss",
		Delay:       "500ms",
		Jitter:      "200ms",
		Loss:        50.0,
		Rate:        "1mbit",
	},
	"high_loss": {
		Name:        "high_loss",
		Description: "High loss: Moderate latency, 70% loss",
		Delay:       "20ms",
		Jitter:      "2ms",
		Loss:        70.0,
		Rate:        "5mbit",
	},
	"heavy_high_loss": {
		Name:        "heavy_high_loss",
		Description: "Heavy high loss: Moderate latency, 85% loss",
		Delay:       "20ms",
		Jitter:      "2ms",
		Loss:        85.0,
		Rate:        "5mbit",
	},
}

var DefaultScenarios = map[string]struct {
	Description string
	LoadConfig  LoadConfig
	Profiles    []string
}{
	"baseline_latency": {
		Description: "Single worker, single stream, fixed 1KB payload — measures pure RPC latency",
		LoadConfig: LoadConfig{
			Workers:     1,
			Pipelining:  1,
			Streams:     1,
			Connections: 1,
			Workload:    WorkloadFixed,
			PayloadSize: 1024,
			Duration:    Duration(10 * time.Second),
			Warmup:      Duration(2 * time.Second),
			Method:      "echo",
		},
		Profiles: []string{"clean", "wifi", "lte"},
	},
	"concurrency_stress": {
		Description: "50-200 workers, limited streams, variable payload — concurrency behavior",
		LoadConfig: LoadConfig{
			Workers:     100,
			Pipelining:  1,
			Streams:     32,
			Connections: 1,
			Workload:    WorkloadRandom,
			MinPayload:  1024,
			MaxPayload:  32 * 1024,
			Duration:    Duration(30 * time.Second),
			Warmup:      Duration(3 * time.Second),
			Method:      "echo",
		},
		Profiles: []string{"clean", "wifi", "lte", "bad_lte"},
	},
	"multiplex_stress": {
		Description: "Large upload + small RPCs concurrently — checks HOL blocking",
		LoadConfig: LoadConfig{
			Workers:     50,
			Pipelining:  1,
			Streams:     64,
			Connections: 1,
			Workload:    WorkloadMixed,
			MinPayload:  1024,
			MaxPayload:  1024,
			Duration:    Duration(30 * time.Second),
			Warmup:      Duration(3 * time.Second),
			Method:      "echo",
		},
		Profiles: []string{"clean", "lte", "bad_lte"},
	},
	"loss_sensitivity": {
		Description: "Concurrency stress repeated at different packet loss levels (0%-10%)",
		LoadConfig: LoadConfig{
			Workers:     100,
			Pipelining:  1,
			Streams:     32,
			Connections: 1,
			Workload:    WorkloadRandom,
			MinPayload:  1024,
			MaxPayload:  16 * 1024,
			Duration:    Duration(20 * time.Second),
			Warmup:      Duration(3 * time.Second),
			Method:      "echo",
		},
		Profiles: []string{"clean", "wifi", "lte", "bad_lte", "extreme"},
	},
	"rtt_scaling": {
		Description: "Fixed load at increasing RTT (10ms-200ms) to measure latency degradation slope",
		LoadConfig: LoadConfig{
			Workers:     50,
			Pipelining:  1,
			Streams:     16,
			Connections: 1,
			Workload:    WorkloadFixed,
			PayloadSize: 1024,
			Duration:    Duration(15 * time.Second),
			Warmup:      Duration(2 * time.Second),
			Method:      "echo",
		},
		Profiles: []string{"clean", "wifi", "lte", "bad_lte", "extreme"},
	},
}

type BenchConfig struct {
	ServerAddr  string          `json:"server_addr"`
	System      RPCSystem       `json:"system"`
	Profile     string          `json:"profile"`
	Scenario    string          `json:"scenario"`
	Interface   string          `json:"interface"`
	OutputDir   string          `json:"output_dir"`
	SaveRaw     bool            `json:"save_raw"`
	LoadConfig  *LoadConfig     `json:"load_config,omitempty"`
	ProfileConf *NetworkProfile `json:"profile_config,omitempty"`
}

func LoadProfiles(path string) (map[string]NetworkProfile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return DefaultProfiles, nil
		}
		return nil, err
	}
	var profiles map[string]NetworkProfile
	if err := json.Unmarshal(data, &profiles); err != nil {
		return nil, fmt.Errorf("parse profiles: %w", err)
	}
	for k, v := range DefaultProfiles {
		if _, ok := profiles[k]; !ok {
			profiles[k] = v
		}
	}
	return profiles, nil
}

func ProfileNames(profiles map[string]NetworkProfile) []string {
	names := make([]string, 0, len(profiles))
	for name := range profiles {
		names = append(names, name)
	}
	return names
}
