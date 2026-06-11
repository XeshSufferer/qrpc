[**English version**](README.md)

# qrpc

[![Go Version](https://img.shields.io/badge/Go-1.26.2-blue.svg)](go.mod)
[![License](https://img.shields.io/badge/License-Apache%202.0-green.svg)](LICENSE)

**qrpc** — высокопроизводительный RPC-фреймворк поверх QUIC (транспорт HTTP/3). Использует protocol buffers с ускорением vtprotobuf, lock-free мультиплексирование стримов и агрессивный object pooling для минимальных задержек и максимальной пропускной способности. Поддерживает как запрос-ответ RPC, так и однонаправленные события.

---

## Возможности

- **Транспорт QUIC** — UDP + TLS 1.3, отсутствие head-of-line блокировок, встроенное шифрование.
- **Protobuf + vtprotobuf** — компактная бинарная сериализация с zero-copy marshal.
- **Мультиплексирование стримов** — N предварительно открытых QUIC-стримов на соединение, lock-free atomic round-robin балансировщик.
- **Object Pooling** — `sync.Pool` для буферов кодировщика, объектов запросов/ответов и каналов ответов — минимизация нагрузки на GC.
- **Sharded Concurrent Map** — 256-секционная конкурентная карта для O(1) диспетчеризации по `request_id`.
- **События** — однонаправленная доставка событий наряду с RPC.
- **Простой API** — `NewServer` → `AddHandler` / `AddEventHandler`, `NewClient` → `NewRequest` → `SendRequest` / `SendEvent`.

---

## Быстрый старт

### Сервер

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

### Клиент

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

### События (однонаправленная отправка)

```go
// Сервер
server.AddEventHandler("notify", func(c internal.EventCtx) {
  log.Printf("событие %s: %s", c.Method(), c.Body())
})

// Клиент
req := client.NewRequest()
req.SetMethod([]byte("notify"))
req.SetBody([]byte("hello"))
err := client.SendEvent(context.Background(), req)
```

---

## Проводной протокол

### Формат фрейма

```
┌─────────────────────────────────────────────┐
│  4 байта: длина payload (big-endian uint32)  │
├─────────────────────────────────────────────┤
│  1 байт:  флаг                               │
│           REQUEST       = 1                  │
│           RESPONSE      = 2                  │
│           EVENT         = 3                  │
│           REQUEST_ZSTD  = 4                  │
│           RESPONSE_ZSTD = 5                  │
│           EVENT_ZSTD    = 6                  │
├─────────────────────────────────────────────┤
│  N байт: protobuf Request / Response         │
└─────────────────────────────────────────────┘
```

- `payload length` = 1 (флаг) + protobuf байты
- vtprotobuf использует `MarshalToSizedBufferVT` (обратная запись в предварительно выделенный буфер)

### Сообщения (protobuf)

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

Запросы сопоставляются с ответами через случайный `uint64` `request_id`. События используют `request_id = 0` и не генерируют ответ.

Payload более 16 КБ автоматически сжимается **zstd** (флаги 4–6).

---

## Архитектура

```
┌───────────────┐     QUIC (UDP)      ┌───────────────┐
│   Client      │ ◄─────────────────► │   Server      │
│               │  TLS 1.3, ALPN     │               │
│  ┌─────────┐  │  "qrpc"            │  ┌─────────┐  │
│  │ Sharded │  │                     │  │Handlers │  │
│  │  Map    │  │                     │  │  Map    │  │
│  └────┬────┘  │                     │  └─────────┘  │
│  ┌────▼────┐  │  N предв.          │  ┌─────────┐  │
│  │Multiplex│  │  открытых стримов  │  │  Stream │  │
│  │   -er   │──┤───────────────────►│  │  Read   │  │
│  └────┬────┘  │                     │  │  Cycle  │  │
│  ┌────▼────┐  │                     │  └─────────┘  │
│  │Balancer │  │  atomic round-robin │               │
│  │(lockfree)│  │                     │               │
│  └─────────┘  │                     │               │
└───────────────┘                     └───────────────┘
```

**Клиент.** При `NewClient` мультиплексор открывает N QUIC-стримов (по умолчанию 32) на соединение и запускает по горутине чтения на стрим. `NewRequest` получает из пула `ReqCtx`, обёртку над protobuf `Request`. `SendRequest` генерирует случайный `request_id`, сохраняет `chan *Response` в sharded map, кодирует запрос через encoder и отправляет на стрим, полученный от round-robin балансировщика. Горутина чтения декодирует входящие фреймы, находит `request_id` в карте и отправляет ответ в канал. `SendEvent` отправляет однонаправленный фрейм с `request_id = 0` без ожидания ответа.

**Сервер.** `NewServer` запускает QUIC-слушатель. Каждое принятое соединение получает горутину, принимающую стримы. Каждый стрим выполняет цикл чтения: декодирует фрейм. RPC-запросы (флаг 1/4) диспетчеризуются к хендлеру через `internal.Ctx`; хендлер устанавливает ответ, сервер кодирует и отправляет его обратно. События (флаг 3/6) диспетчеризуются к обработчику событий через `internal.EventCtx`; ответ не отправляется.

---

## Справочник API

### Сервер

```go
func NewServer(addr string, tls *tls.Config) (QRpcServer, error)
```

| Метод | Сигнатура | Описание |
|--------|-----------|----------|
| `AddHandler` | `(method string, handler func(internal.Ctx))` | Регистрация RPC-обработчика |
| `AddEventHandler` | `(method string, handler func(internal.EventCtx))` | Регистрация обработчика событий |

**`internal.Ctx`** (RPC-обработчик):

| Метод | Возвращает | Описание |
|--------|-----------|----------|
| `Body()` | `[]byte` | Тело запроса |
| `Headers()` | `[][]byte` | Заголовки запроса (пары ключ-значение) |
| `Method()` | `[]byte` | Имя метода |
| `GetHeader(key, default)` | `string` | Получить заголовок по ключу |
| `SetBody([]byte)` | — | Установить тело ответа |
| `SetCode(uint32)` | — | Установить код ответа |
| `SetHeader(key, value)` | — | Установить заголовок ответа |
| `SetHeaders([][]byte)` | — | Установить все заголовки ответа |
| `Locals()` | `Locals` | Локальное хранилище запроса |

**`internal.EventCtx`** (обработчик событий): `Body()`, `Headers()`, `Method()`, `GetHeader()`, `Locals()` — только чтение, ответ не отправляется.

### Клиент

```go
func NewClient(ctx context.Context, addr string, tls *tls.Config, connsCount int) (Client, error)
```

| Метод | Возвращает | Описание |
|--------|-----------|----------|
| `NewRequest()` | `ReqCtx` | Получить контекст запроса из пула |
| `SendRequest(ctx, reqCtx)` | `(RespCtx, error)` | Отправить RPC и ждать ответ |
| `SendEvent(ctx, reqCtx)` | `error` | Отправить событие (fire-and-forget) |
| `ReleaseResponse(RespCtx)` | — | Вернуть ответ в пул |

**`ReqCtx`:** `Body()`, `SetBody()`, `Headers()`, `SetHeaders()`, `Method()`, `SetMethod()`, `RequestId()`, `Locals()`.

**`RespCtx`:** `Body()`, `Headers()`, `Code()`, `RequestId()`.

---

## Производительность

Все бенчмарки запущены на localhost с эмуляцией сети через Linux `tc` netem. Тестируются **5 сетевых профилей** × **5 сценариев** × **2 размера payload (100 B / 4 KB)** × **2 количества соединений (1 / 16)**, с **64 пиплайнингом**, **16 QUIC-стримами** и **длительностью 10s + 3s прогрева**.

### Пропускная способность (RPS) — лучшие значения по размерам payload и соединениям

| Сценарий | Workers | clean | wifi | lte | bad_lte | extreme |
|---|---|---|---|---|---|---|
| baseline_latency | 1 | **23,607** | 3,690 | 625 | 278 | 102 |
| concurrency_stress | 100 | **506,590** | 60,334 | 5,355 | 1,576 | 827 |
| multiplex_stress | 50 | **478,703** | 60,234 | 4,864 | 1,229 | 720 |
| loss_sensitivity | 100 | **484,582** | 59,982 | 4,568 | 1,207 | 619 |
| rtt_scaling | 50 | **462,074** | 60,467 | 5,316 | 1,339 | 831 |

Все сценарии показывают **100% успешных запросов** на всех сетевых профилях.

### Хвостовая задержка (P95) — худшие значения по размерам payload и соединениям

| Сценарий | clean | wifi | lte | bad_lte | extreme |
|---|---|---|---|---|---|
| baseline_latency | 3.4 ms | 23.5 ms | 405 ms | 1.74 s | 2.85 s |
| concurrency_stress | 1.74 s | 7.69 s | 8.74 s | 8.46 s | 8.46 s |
| multiplex_stress | 23.8 ms | 5.04 s | 5.90 s | 4.84 s | 7.07 s |
| loss_sensitivity | 1.60 s | 4.89 s | 4.99 s | 7.24 s | 6.49 s |
| rtt_scaling | 175 ms | 5.08 s | 5.39 s | 6.13 s | 6.68 s |

> На чистых сетях qrpc достигает **23K–506K RPS** с задержками в единицах миллисекунд. В экстремальных условиях (150ms RTT, 10% потерь) пропускная способность предсказуемо снижается при 100% успешной доставке — **102–831 RPS** с P95 от 2.85 до 8.46 s в зависимости от конкурентности.

---

## Итоги

| Сценарий            | Ключевой вывод                                              |
|---------------------|-------------------------------------------------------------|
| baseline_latency    | 23.6K RPS, ~2.1 ms avg latency на чистой сети               |
| concurrency_stress  | **506K RPS** на чистой сети, 100% успеха на всех профилях   |
| multiplex_stress    | 479K RPS на чистой сети, стабильная работа при задержках    |
| loss_sensitivity    | 485K RPS на чистой сети, предсказуемое снижение при потерях |
| rtt_scaling         | 462K RPS на чистой сети, плавная деградация с RTT           |

qrpc показывает 100% успешную доставку на всех сетевых профилях от чистого localhost до экстремальных условий (150ms RTT, 10% потерь). Благодаря QUIC-транспорту и агрессивному object pooling, qrpc обеспечивает высокую пропускную способность и предсказуемую деградацию в реальных сетевых условиях.

---

## Установка

```bash
go get github.com/XeshSufferer/qrpc
```

Требуется Go 1.26.2+.

---

## Тестирование

### Модульные бенчмарки

```bash
go test -bench=. -benchmem ./...
```

### Фреймворк нагрузочного тестирования

```bash
cd stress_tester
go build -o stress_tester .

# Запуск сервера для бенчмарков
sudo ./stress_tester server -addr 127.0.0.1:8081

# Запуск сценария
sudo ./stress_tester run -scenario baseline_latency -profile clean,wifi,lte -system qrpc -addr 127.0.0.1:8081

# Полный набор тестов
sudo ./run_all.sh --duration 10s --warmup 3s
```

### Автоматизированный прогон (100 запусков)

```bash
go run ./runall_stress/main.go
```

Запускает все 5 сценариев × 5 профилей × 2 размера payload × 2 типа соединений и генерирует `results/heatmap.html`.

### HTML-отчёт

```bash
cd stress_tester
python3 analyze.py results --html report.html
```

Сетевые профили применяются через Linux `tc` netem (требуется `sudo`).

---

## Сетевые профили

| Профиль  | RTT  | Джиттер | Потери | Пропускная | Описание             |
|----------|------|---------|--------|------------|----------------------|
| clean    | 1ms  | 0ms     | 0%     | 1000 Mbps  | Локальная / ЦОД      |
| wifi     | 5ms  | 2ms     | 0.1%   | 100 Mbps   | Типичный WiFi        |
| lte      | 30ms | 10ms    | 1%     | 50 Mbps    | 4G LTE               |
| bad_lte  | 60ms | 20ms    | 5%     | 10 Mbps    | Плохой LTE           |
| extreme  | 150ms| 50ms    | 10%    | 5 Mbps     | Экстремальные условия|

---

## Сценарии тестов

| Сценарий            | Workers | Streams | Pipelining | Payload              | Профили                        |
|---------------------|---------|---------|------------|----------------------|--------------------------------|
| baseline_latency    | 1       | 16      | 64         | 100 B / 4 KB fixed   | clean, wifi, lte, bad_lte, extreme |
| concurrency_stress  | 100     | 16      | 64         | 100 B / 4 KB fixed   | clean, wifi, lte, bad_lte, extreme |
| multiplex_stress    | 50      | 16      | 64         | 100 B / 4 KB fixed   | clean, wifi, lte, bad_lte, extreme |
| loss_sensitivity    | 100     | 16      | 64         | 100 B / 4 KB fixed   | clean, wifi, lte, bad_lte, extreme |
| rtt_scaling         | 50      | 16      | 64         | 100 B / 4 KB fixed   | clean, wifi, lte, bad_lte, extreme |

---

## Зависимости

| Библиотека | Версия    | Назначение                   |
|------------|-----------|------------------------------|
| quic-go    | v0.59.1   | Транспорт QUIC               |
| vtprotobuf | v0.6.0    | Быстрый marshal protobuf     |
| google.golang.org/protobuf | v1.36.11 | Среда protobuf |

---

## Лицензия

Apache License 2.0 — см. [LICENSE](LICENSE).
