[**Русская версия**](README.ru.md)

# qrpc

[![Go Version](https://img.shields.io/badge/Go-1.26.2-blue.svg)](go.mod)
[![License](https://img.shields.io/badge/License-Apache%202.0-green.svg)](LICENSE)

**qrpc** — a high-performance RPC framework built on QUIC (HTTP/3 transport). It uses protocol buffers with vtprotobuf acceleration, lock-free stream multiplexing, and aggressive object pooling to deliver low-latency, high-throughput communication. Supports both request-response RPC and one-way event messaging.

---

## Features

- **QUIC Transport** — UDP + TLS 1.3, no head-of-line blocking, built-in encryption.
- **Protobuf + vtprotobuf** — compact binary serialization with zero-copy marshal.
- **Stream Multiplexing** — N pre-opened QUIC streams per connection, lock-free atomic round-robin balancer.
- **Object Pooling** — `sync.Pool` for encoder buffers, request/response objects, and response channels — minimizes GC pressure.
- **Sharded Concurrent Map** — 256-shard map for O(1) request-ID-to-channel dispatch.
- **Event Messaging** — one-way event delivery alongside request-response RPC.
- **Simple API** — `NewServer` → `AddHandler` / `AddEventHandler`, `NewClient` → `NewRequest` → `SendRequest` / `SendEvent`.

---

## Quick Start

### Server

```go
package main

import (
  "crypto/tls"
  "log"
  "github.com/XeshSufferer/qrpc"
  "github.com/XeshSufferer/qrpc/internal"
)

func main() {
  tlsConfig := /* *tls.Config */
  server, err := qrpc.NewServer("0.0.0.0:8081", tlsConfig)
  if err != nil {
    log.Fatal(err)
  }
  server.AddHandler("echo", func(c internal.Ctx) {
    c.SetBody(c.Body())
    c.SetCode(0)
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

  req := client.NewRequest()
  req.SetMethod([]byte("echo"))
  req.SetBody([]byte("hello qrpc"))

  resp, err := client.SendRequest(context.Background(), req)
  if err != nil {
    log.Fatal(err)
  }
  log.Printf("Response: %s", resp.Body)
  client.ReleaseResponse(resp)
}
```

### Events (one-way messaging)

```go
// Server
server.AddEventHandler("notify", func(c internal.EventCtx) {
  log.Printf("event %s: %s", c.Method(), c.Body())
})

// Client
req := client.NewRequest()
req.SetMethod([]byte("notify"))
req.SetBody([]byte("hello"))
err := client.SendEvent(context.Background(), req)
```

---

## Wire Protocol

### Frame Format

```
┌─────────────────────────────────────────────┐
│  4 bytes: payload length (big-endian uint32) │
├─────────────────────────────────────────────┤
│  1 byte:  flag                               │
│           REQUEST       = 1                  │
│           RESPONSE      = 2                  │
│           EVENT         = 3                  │
│           REQUEST_ZSTD  = 4                  │
│           RESPONSE_ZSTD = 5                  │
│           EVENT_ZSTD    = 6                  │
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

Requests are matched to responses via a random `uint64` `request_id`. Events use `request_id = 0` and do not generate a response.

Payloads larger than 16 KB are automatically compressed with **zstd** (flags 4–6).

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

**Client flow.** On `NewClient`, the multiplexer opens N QUIC streams (default 32) per connection and starts a read-cycle goroutine per stream. `NewRequest` obtains a pooled `ReqCtx` wrapping a protobuf `Request`. `SendRequest` assigns a random `request_id`, stores a `chan *Response` in the sharded map, encodes the request via the encoder, and writes it to a stream obtained from the round-robin balancer. The read-cycle goroutine decodes incoming frames, looks up `request_id` in the sharded map, and dispatches the response to the waiting channel. `SendEvent` writes a one-way frame with `request_id = 0` and no response is expected.

**Server flow.** `NewServer` starts a QUIC listener. Each accepted connection gets a goroutine that accepts streams. Each stream runs a read cycle that decodes frames. Request frames (flag 1/4) dispatch to the registered handler via `internal.Ctx`; the handler sets the response and the server encodes and writes it back. Event frames (flag 3/6) dispatch to the event handler via `internal.EventCtx`; no response is sent.

---

## API Reference

### Server

```go
func NewServer(addr string, tls *tls.Config) (QRpcServer, error)
```

| Method | Signature | Description |
|--------|-----------|-------------|
| `AddHandler` | `(method string, handler func(internal.Ctx))` | Register an RPC handler |
| `AddEventHandler` | `(method string, handler func(internal.EventCtx))` | Register an event handler |

**`internal.Ctx`** (RPC handler):

| Method | Returns | Description |
|--------|---------|-------------|
| `Body()` | `[]byte` | Request body |
| `Headers()` | `[][]byte` | Request headers (key-value pairs) |
| `Method()` | `[]byte` | Request method name |
| `GetHeader(key, default)` | `string` | Get a header value by key |
| `SetBody([]byte)` | — | Set response body |
| `SetCode(uint32)` | — | Set response status code |
| `SetHeader(key, value)` | — | Set a response header |
| `SetHeaders([][]byte)` | — | Set all response headers |
| `Locals()` | `Locals` | Per-request local storage |

**`internal.EventCtx`** (event handler): `Body()`, `Headers()`, `Method()`, `GetHeader()`, `Locals()` — read-only, no response.

### Client

```go
func NewClient(ctx context.Context, addr string, tls *tls.Config, connsCount int) (Client, error)
```

| Method | Returns | Description |
|--------|---------|-------------|
| `NewRequest()` | `ReqCtx` | Get a pooled request context |
| `SendRequest(ctx, reqCtx)` | `(RespCtx, error)` | Send RPC and wait for response |
| `SendEvent(ctx, reqCtx)` | `error` | Fire-and-forget event |
| `ReleaseResponse(RespCtx)` | — | Return response to pool |

**`ReqCtx`:** `Body()`, `SetBody()`, `Headers()`, `SetHeaders()`, `Method()`, `SetMethod()`, `RequestId()`, `Locals()`.

**`RespCtx`:** `Body()`, `Headers()`, `Code()`, `RequestId()`.

---

## Performance

Benchmarks run on localhost with Linux `tc` netem for network emulation. All runs test **5 network profiles** × **5 scenarios** × **2 payload sizes (100 B / 4 KB)** × **2 connection counts (1 / 16)**, using **64 pipelining**, **16 QUIC streams**, and **10s duration + 3s warmup**.

### Throughput (RPS) — Best Across Payload Sizes & Connection Counts

| Scenario | Workers | clean | wifi | lte | bad_lte | extreme |
|---|---|---|---|---|---|---|
| baseline_latency | 1 | **23,607** | 3,690 | 625 | 278 | 102 |
| concurrency_stress | 100 | **506,590** | 60,334 | 5,355 | 1,576 | 827 |
| multiplex_stress | 50 | **478,703** | 60,234 | 4,864 | 1,229 | 720 |
| loss_sensitivity | 100 | **484,582** | 59,982 | 4,568 | 1,207 | 619 |
| rtt_scaling | 50 | **462,074** | 60,467 | 5,316 | 1,339 | 831 |

All scenarios maintain **100% success rate** across all network profiles.

### Tail Latency (P95) — Worst Across Payload Sizes & Connection Counts

| Scenario | clean | wifi | lte | bad_lte | extreme |
|---|---|---|---|---|---|
| baseline_latency | 3.4 ms | 23.5 ms | 405 ms | 1.74 s | 2.85 s |
| concurrency_stress | 1.74 s | 7.69 s | 8.74 s | 8.46 s | 8.46 s |
| multiplex_stress | 23.8 ms | 5.04 s | 5.90 s | 4.84 s | 7.07 s |
| loss_sensitivity | 1.60 s | 4.89 s | 4.99 s | 7.24 s | 6.49 s |
| rtt_scaling | 175 ms | 5.08 s | 5.39 s | 6.13 s | 6.68 s |

> On clean networks qrpc delivers **23K–506K RPS** with low millisecond latency. Under extreme conditions (150ms RTT, 10% loss) throughput degrades predictably while maintaining 100% delivery — **102–831 RPS** with P95 latency of 2.85–8.46 s depending on concurrency.

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
go build -o stress_tester .

# Start a benchmark server
sudo ./stress_tester server -addr 127.0.0.1:8081

# Run a specific scenario
sudo ./stress_tester run -scenario baseline_latency -profile clean,wifi,lte -system qrpc -addr 127.0.0.1:8081

# Run the full automated suite
sudo ./run_all.sh --duration 10s --warmup 3s
```

### Automated Suite (100 runs)

```bash
go run ./runall_stress/main.go
```

Runs all 5 scenarios × 5 profiles × 2 payload sizes × 2 connection counts and generates `results/heatmap.html`.

### HTML Report

```bash
cd stress_tester
python3 analyze.py results --html report.html
```

Network profiles are applied via Linux `tc` netem (requires `sudo`).

---

## Network Profiles

| Profile   | RTT  | Jitter | Loss | Bandwidth | Description        |
|-----------|------|--------|------|-----------|--------------------|
| clean     | 1ms  | 0ms    | 0%   | 1000 Mbps | Local / DC         |
| wifi      | 5ms  | 2ms    | 0.1% | 100 Mbps  | Typical WiFi       |
| lte       | 30ms | 10ms   | 1%   | 50 Mbps   | 4G LTE             |
| bad_lte   | 60ms | 20ms   | 5%   | 10 Mbps   | Poor LTE           |
| extreme   | 150ms| 50ms   | 10%  | 5 Mbps    | Extreme conditions |

---

## Test Scenarios

| Scenario           | Workers | Streams | Pipelining | Payload            | Profiles Used                       |
|--------------------|---------|---------|------------|--------------------|-------------------------------------|
| baseline_latency   | 1       | 16      | 64         | 100 B / 4 KB fixed | clean, wifi, lte, bad_lte, extreme |
| concurrency_stress | 100     | 16      | 64         | 100 B / 4 KB fixed | clean, wifi, lte, bad_lte, extreme |
| multiplex_stress   | 50      | 16      | 64         | 100 B / 4 KB fixed | clean, wifi, lte, bad_lte, extreme |
| loss_sensitivity   | 100     | 16      | 64         | 100 B / 4 KB fixed | clean, wifi, lte, bad_lte, extreme |
| rtt_scaling        | 50      | 16      | 64         | 100 B / 4 KB fixed | clean, wifi, lte, bad_lte, extreme |

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
