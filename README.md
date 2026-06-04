[**Русская версия**](README.ru.md)

# qrpc

[![Go Version](https://img.shields.io/badge/Go-1.26.2-blue.svg)](go.mod)
[![License](https://img.shields.io/badge/License-Apache%202.0-green.svg)](LICENSE)

**qrpc** — a high-performance RPC framework built on QUIC (HTTP/3 transport). It uses protocol buffers with vtprotobuf acceleration, lock-free stream multiplexing, and aggressive object pooling to deliver low-latency, high-throughput communication.

---

## Features

- **QUIC Transport** — UDP + TLS 1.3, no head-of-line blocking, built-in encryption.
- **Protobuf + vtprotobuf** — compact binary serialization with zero-copy marshal.
- **Stream Multiplexing** — N pre-opened QUIC streams per connection, lock-free atomic round-robin balancer.
- **Object Pooling** — `sync.Pool` for encoder buffers, request/response objects, and response channels — minimizes GC pressure.
- **Sharded Concurrent Map** — 256-shard map for O(1) request-ID-to-channel dispatch.
- **Simple API** — `NewServer` → `AddHandler`, `NewClient` → `SendRequest`.

---

## Quick Start

### Server

```go
package main

import (
  "crypto/tls"
  "log"
  "github.com/XeshSufferer/qrpc"
  "github.com/XeshSufferer/qrpc/protos/pb/gen"
)

func main() {
  tlsConfig := /* *tls.Config */
  server, err := qrpc.NewServer("0.0.0.0:8081", tlsConfig)
  if err != nil {
    log.Fatal(err)
  }
  server.AddHandler("echo", func(req *gen.Request, resp *gen.Response) {
    resp.Body = req.Body
    resp.Code = 0
  })
  select {}
}
```

### Client

```go
package main

import (
  "context"
  "crypto/tls"
  "log"
  "github.com/XeshSufferer/qrpc"
)

func main() {
  tlsConfig := /* *tls.Config */
  client, err := qrpc.NewClient(context.Background(), "127.0.0.1:8081", tlsConfig, 1)
  if err != nil {
    log.Fatal(err)
  }
  resp, err := client.SendRequest(
    context.Background(),
    []byte("echo"), []byte("hello qrpc"), nil,
  )
  if err != nil {
    log.Fatal(err)
  }
  log.Printf("Response: %s", resp.Body)
}
```

---

## Wire Protocol

### Frame Format

```
┌─────────────────────────────────────────────┐
│  4 bytes: payload length (big-endian uint32) │
├─────────────────────────────────────────────┤
│  1 byte:  flag                               │
│           REQUEST  = 1                       │
│           RESPONSE = 2                       │
│           EVENT    = 3                       │
├─────────────────────────────────────────────┤
│  N bytes: protobuf Request / Response        │
└─────────────────────────────────────────────┘
```

- `payload length` = 1 (flag) + protobuf bytes
- vtprotobuf uses `MarshalToSizedBufferVT` (reverse write into pre-allocated buffer)

### Messages (protobuf)

```protobuf
message Request {
  uint64 request_id = 3;
  bytes  headers    = 1;
  bytes  method     = 2;
  bytes  body       = 4;
}

message Response {
  uint64 request_id = 3;
  uint32 code       = 5;
  bytes  headers    = 1;
  bytes  method     = 2;
  bytes  body       = 4;
}
```

Requests are matched to responses via a random `uint64` `request_id`.

---

## Architecture

```
┌───────────────┐     QUIC (UDP)      ┌───────────────┐
│   Client      │ ◄─────────────────► │   Server      │
│               │  TLS 1.3, ALPN     │               │
│  ┌─────────┐  │  "qrpc"            │  ┌─────────┐  │
│  │ Sharded │  │                     │  │Handlers │  │
│  │  Map    │  │                     │  │  Map    │  │
│  └────┬────┘  │                     │  └─────────┘  │
│  ┌────▼────┐  │  N pre-opened      │  ┌─────────┐  │
│  │Multiplex│  │  streams            │  │  Stream │  │
│  │   -er   │──┤───────────────────►│  │  Read   │  │
│  └────┬────┘  │                     │  │  Cycle  │  │
│  ┌────▼────┐  │                     │  └─────────┘  │
│  │Balancer │  │  atomic round-robin │               │
│  │(lockfree)│  │                     │               │
│  └─────────┘  │                     │               │
└───────────────┘                     └───────────────┘
```

**Client flow.** On `NewClient`, the multiplexer opens N QUIC streams (default 32) and starts a read-cycle goroutine per stream. `SendRequest` assigns a random `request_id`, stores a `chan *Response` in the sharded map, encodes the request, and writes it to a stream obtained from the round-robin balancer. The read-cycle goroutine decodes incoming frames, looks up `request_id`, and dispatches the response to the waiting channel.

**Server flow.** `NewServer` starts a QUIC listener. Each accepted connection gets a goroutine that accepts streams. Each stream runs a read cycle that decodes frames, dispatches to the registered handler, encodes the response, and writes it back.

---

## Performance

Benchmarks run via the stress-test framework in `stress_tester/` on localhost with Linux `tc` netem for network emulation. All runs use ~100B fixed payload and 10s test duration (3s warmup) unless noted. Results reflect the `concurrency_stress` scenario at varying network profiles.

### qrpc — Degradation Across Network Profiles

| Profile   | RTT    | Loss  | Workers | RPS      | Avg       | P50       | P95       | P99       | Success |
|-----------|--------|-------|---------|----------|-----------|-----------|-----------|-----------|---------|
| clean     | 1ms    | 0%    | 12      | **4,407**| 2.1 ms    | 2.1 ms    | 2.2 ms    | 2.2 ms    | 100%    |
| wifi      | 5ms    | 0.1%  | 64      | 119      | 411 ms    | 200 ms    | 239 ms    | 7.0 s     | 100%    |
| lte       | 30ms   | 1%    | 12      | 128      | 72 ms     | 70 ms     | 78 ms     | 226 ms    | 100%    |
| high_loss | 20ms   | 70%   | 50      | 25       | 3.8 s     | 4.2 s     | 9.6 s     | 9.6 s     | 100%    |
| hell_net  | 300ms  | 30%   | 50      | 587      | 3.9 s     | 3.6 s     | 7.8 s     | 8.8 s     | 100%    |
| extreme   | 150ms  | 10%   | 50      | 1,802    | 1.8 s     | 1.0 s     | 5.8 s     | 10.8 s    | 100%    |
| no_net    | 500ms  | 50%   | 50      | 165      | 8.7 s     | 10.2 s    | 12.1 s    | 12.7 s    | 100%    |

> qrpc achieves **100% success rate across all profiles**, including 50% packet loss and 500ms RTT. Latency degrades predictably with network conditions.

### qrpc vs gRPC — Extreme & No-Network

| Metric   | Extreme qrpc | Extreme gRPC | No-Net qrpc | No-Net gRPC |
|----------|-------------|--------------|-------------|--------------|
| RPS      | **1,802**   | 716          | **165**     | 39           |
| Avg      | **1.77 s**  | 3.77 s       | 8.70 s      | **4.33 s**   |
| P50      | **1.04 s**  | 3.68 s       | 10.2 s      | **5.01 s**   |
| P95      | 5.76 s      | **4.99 s**   | 12.1 s      | **5.15 s**   |
| P99      | 10.8 s      | **5.23 s**   | 12.7 s      | **5.38 s**   |
| Success  | 100%        | 100%         | 100%        | 100%         |

> Under extreme conditions (150ms RTT, 10% loss) qrpc achieves **2.5× higher throughput** than gRPC with **52% lower avg latency**. At 50% loss (no_network) throughput advantage grows to **4.2×**.

### Multiplex Stress — Mixed Payloads (50 workers, 64 streams, mixed 1 KB + 1–11 MB, clean)

| Metric   | qrpc        |
|----------|-------------|
| RPS      | 76          |
| Avg      | 516 ms      |
| P50      | 163 ms      |
| P95      | 2.02 s      |
| P99      | 3.18 s      |
| Success  | 100%        |

---

## Summary

| Profile        | Key finding                                |
|----------------|--------------------------------------------|
| clean          | 4,400 RPS, ~2 ms avg latency              |
| extreme (150ms, 10% loss) | **2.5× throughput vs gRPC**, 100% success |
| no_network (500ms, 50% loss) | **4.2× throughput vs gRPC**, 100% success |
| high loss (70–85%) | 100% delivery, degraded throughput      |

qrpc's QUIC-based transport excels under real-world network conditions — packet loss, high latency, and bandwidth constraints — where it significantly outperforms gRPC in throughput while maintaining 100% delivery.

---

## Installation

```bash
go get github.com/XeshSufferer/qrpc
```

Requires Go 1.26.2+.

---

## Benchmarking

### Unit Benchmarks

```bash
go test -bench=. -benchmem ./...
```

### Stress Test Framework

```bash
cd stress_tester
go build -o qrpc-stress .

# Start a benchmark server
sudo ./qrpc-stress server -addr 127.0.0.1:8081

# Run a specific scenario
sudo ./qrpc-stress run -scenario baseline_latency -profile clean,wifi,lte -system qrpc -addr 127.0.0.1:8081

# Run the full automated suite
sudo ./run_all.sh --duration 10s --warmup 3s
```

### HTML Report

```bash
python3 analyze.py results --html report.html
```

Network profiles are applied via Linux `tc` netem (requires `sudo`).

---

## Network Profiles

| Profile         | RTT    | Jitter | Loss | Bandwidth | Description              |
|-----------------|--------|--------|------|-----------|--------------------------|
| clean           | 1ms    | 0ms    | 0%   | 1000 Mbps | Local / DC               |
| wifi            | 5ms    | 2ms    | 0.1% | 100 Mbps  | Typical WiFi             |
| lte             | 30ms   | 10ms   | 1%   | 50 Mbps   | 4G LTE                   |
| bad_lte         | 60ms   | 20ms   | 5%   | 10 Mbps   | Poor LTE                 |
| high_loss       | 20ms   | 2ms    | 70%  | 5 Mbps    | High packet loss         |
| heavy_high_loss | 20ms   | 2ms    | 85%  | 5 Mbps    | Extreme packet loss      |
| extreme         | 150ms  | 50ms   | 10%  | 5 Mbps    | Extreme stress           |
| hell_network    | 300ms  | 100ms  | 30%  | 2 Mbps    | Hell network conditions  |
| no_network      | 500ms  | 200ms  | 50%  | 1 Mbps    | Near-total network loss  |

---

## Test Scenarios

| Scenario           | Workers | Streams | Payload                 | Profiles Used                                              |
|--------------------|---------|---------|-------------------------|------------------------------------------------------------|
| baseline_latency   | 1       | 1       | 1 KB fixed              | clean, wifi, lte                                           |
| concurrency_stress | 12–100  | 16–32   | 100B fixed (—32768 rand)| clean, wifi, lte, high_loss, heavy_high_loss, hell_network, extreme, no_network |
| multiplex_stress   | 50      | 64      | mixed 1 KB + 1–11 MB    | clean                                                      |
| loss_sensitivity   | 100     | 32      | 1–16 KB random          | clean, wifi, lte, bad_lte, extreme                         |
| rtt_scaling        | 50      | 16      | 1 KB fixed                 | clean, wifi, lte, bad_lte, extreme |

---

## Dependencies

| Library     | Version   | Purpose                     |
|-------------|-----------|-----------------------------|
| quic-go     | v0.59.1   | QUIC transport              |
| vtprotobuf  | v0.6.0    | Fast protobuf marshal       |
| google.golang.org/protobuf | v1.36.11 | Protobuf runtime |

---

## License

Apache License 2.0 — see [LICENSE](LICENSE).
