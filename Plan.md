# Riffy — Rust Diffy Implementation Plan

> **Historical document.** This captures the original diffy-based design and
> uses the old terminology (`primary`/`secondary`/`candidate`) and module layout
> (`handler/`, `proxy/`). The terminology and packaging were later changed for
> clarity (see `Progress.md` R30): upstreams are now **baseline** / **candidate**
> / **control**, and HTTP code lives in `http/` with the upstream client in
> `upstream/`. For what the code does *today*, read `docs/architecture.md`.

## Overview

Riffy is a Rust reverse proxy that compares responses from three upstream services (primary, secondary, candidate) to detect regressions. It uses diffy's statistical noise detection: primary vs secondary disagreement = noise baseline, primary vs candidate disagreement = raw diff. Fields where raw significantly exceeds noise are real regressions.

## Architecture

```
                    Client Request
                         │
                         ▼
               ┌──────────────────┐
               │  axum HTTP server │  (proxy port, e.g. 8880)
               │  router + handler │
               └────────┬─────────┘
                        │
          ┌─────────────┼─────────────┐
          │             │             │
          ▼             ▼             ▼
     ┌─────────┐  ┌──────────┐  ┌─────────┐
     │ primary │  │ candidate│  │secondary│   (reqwest, parallel via join!)
     │ :9100   │  │ :9000    │  │ :9200   │
     └────┬────┘  └────┬─────┘  └────┬────┘
          │             │             │
          └─────────────┼─────────────┘
                        │
              responsePicker (config: primary/candidate/secondary/none)
                        │
                        ▼
                   Client Response (immediate, no wait for analysis)
                        │
                        ▼
               ┌──────────────────┐
               │ Diff + Analysis   │  (mpsc channel → consumer task)
               │ Pipeline          │
               │                   │
               │ 1. Parse JSON     │
               │ 2. Diff P vs C    │──▶ Raw counters (DashMap)
               │ 3. Diff P vs S    │──▶ Noise counters (DashMap)
               │ 4. Per-request    │──▶ Redis stream (XADD)
               │    diff entry     │
               │ 5. Periodic       │──▶ Redis hash (aggregation snapshot)
               │    aggregation    │
               └──────────────────┘
```

## Project Structure

```
riffy/
├── Cargo.toml
├── config.example.yaml
├── Dockerfile
├── Makefile
├── src/
│   ├── main.rs                  # Entry point, wiring, graceful shutdown
│   ├── config/
│   │   └── mod.rs               # figment config structs
│   ├── proxy/
│   │   ├── mod.rs               # Proxy orchestration (parallel upstream calls)
│   │   ├── handler.rs           # axum handler: receives request, calls proxy, returns response
│   │   ├── router.rs            # axum router setup + middleware
│   │   └── upstream.rs          # reqwest client wrapper, response model
│   ├── compare/
│   │   ├── mod.rs               # Difference ADT + apply() dispatcher
│   │   └── flatten.rs           # Recursive flattening to dot-path field map
│   ├── analysis/
│   │   ├── mod.rs               # DifferenceAnalyzer (raw + noise diff, counter updates)
│   │   ├── collector.rs         # InMemoryDifferenceCollector (DashMap + AtomicU64)
│   │   ├── filter.rs            # DifferencesFilterFactory (threshold predicate)
│   │   └── joined.rs            # JoinedField, JoinedEndpoint (raw + noise join)
│   ├── endpoint/
│   │   └── mod.rs               # Path template matching (:param extraction)
│   ├── pipeline/
│   │   ├── mod.rs               # Pipeline wiring (channel producer)
│   │   └── consumer.rs          # Consumer task: Redis stream writes + periodic aggregation
│   ├── redis/
│   │   └── mod.rs               # Redis connection + XADD + HSET operations
│   ├── telemetry/
│   │   ├── mod.rs               # tracing subscriber init (JSON)
│   │   └── metrics.rs           # Prometheus metrics (proxy throughput, diff pipeline lag)
│   └── error.rs                 # AppError enum (thiserror), IntoResponse impl
```

## Dependencies (Cargo.toml)

```toml
[package]
name = "riffy"
version = "0.1.0"
edition = "2021"

[dependencies]
# Async runtime
tokio = { version = "1", features = ["full"] }

# HTTP framework
axum = "0.7"
tower = "0.5"
tower-http = { version = "0.5", features = ["trace", "timeout"] }

# HTTP client (upstream proxy)
reqwest = { version = "0.12", features = ["json"] }

# Serialization
serde = { version = "1", features = ["derive"] }
serde_json = "1"

# Configuration
figment = { version = "0.10", features = ["env", "yaml"] }

# Redis
redis = { version = "0.27", features = ["tokio-comp", "aio"] }

# Concurrency
dashmap = "6"

# Error handling
anyhow = "1"
thiserror = "2"

# Observability
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["chrono", "json", "env-filter"] }
metrics = "0.24"
metrics-exporter-prometheus = "0.16"

# Allocator (optional)
[target.'cfg(not(target_env = "msvc"))'.dependencies]
tikv-jemallocator = { version = "0.6", optional = true }

[features]
default = ["jemalloc"]
jemalloc = ["tikv-jemallocator"]
```

## Configuration Format (config.example.yaml)

```yaml
riffy:
  service-name: "my-service"

proxy:
  port: 8880
  response-mode: primary        # primary | candidate | secondary | none
  allow-http-side-effects: false

upstream:
  primary: "localhost:9100"
  secondary: "localhost:9200"
  candidate: "localhost:9000"
  protocol: http
  timeout: 30s

endpoints:
  - pattern: "/api/v1/users/:id"
  - pattern: "/api/v1/orders/:order_id/items/:item_id"
  - pattern: "/api/v1/health"

threshold:
  relative: 20.0               # percent
  absolute: 0.03               # percent

redis:
  uri: "redis://localhost:6379"
  stream-key: "riffy:diffs"
  aggregation-interval: 10s
  aggregation-key-prefix: "riffy:agg"

server:
  address: "0.0.0.0"
  port: 8888                    # admin/API port (health, metrics)

logging:
  level: info
  format: json

metrics:
  enabled: true
  port: 9090
```

## Key Algorithms

### 1. Reverse Proxy Flow

```
handler(request):
  1. Check method: if mutating && !allow_side_effects → return 405
  2. Identify endpoint via path template matching
  3. Build upstream requests (clone headers + body)
  4. tokio::join!(primary_req, candidate_req, secondary_req)
  5. Send (endpoint, primary_resp, candidate_resp, secondary_resp) to mpsc channel
  6. Return response per response-mode config
```

### 2. JSON Difference Algorithm

Recursive type dispatch on `serde_json::Value`:

```
diff(left: Value, right: Value) -> Difference:
  match (left, right):
    (null, null)         → NoDifference
    (bool, bool)         → if eq → NoDifference else PrimitiveDiff
    (Number, Number)     → if eq → NoDifference else PrimitiveDiff
    (String, String)     → if eq → NoDifference else PrimitiveDiff
    (Object, Object)     → MapDiff:
                            - key set diff (leftNotRight, rightNotLeft)
                            - per-key recursive diff on shared keys
    (Array, Array):
      if sizes differ   → SeqSizeDiff (leftNotRight, rightNotLeft)
      if same size       → IndexedDiff (per-element recursive diff)
    (_, _)               → TypeDiff (type mismatch)
```

### 3. Flattening

Recursive descent producing `HashMap<String, Difference>`:
- `Object` fields: `"parent.child"` dot-path
- `Array` elements: `"parent.0"`, `"parent.1"` indexed path
- Leaf differences: terminal value at full path

### 4. Noise Detection (Statistical)

Per endpoint, per field path, maintain:
- `raw_count: AtomicU64` — times primary vs candidate differ on this field
- `noise_count: AtomicU64` — times primary vs secondary differ on this field
- `total_count: AtomicU64` — total requests for this endpoint

Classification predicate (same as diffy):
```
is_real_regression(field):
  raw  = field.raw_count
  noise = field.noise_count
  total = endpoint.total_count

  raw > noise
  AND |raw - noise| / (raw + noise) * 100 > threshold.relative (20%)
  AND |raw - noise| / total * 100 > threshold.absolute (0.03%)
```

### 5. Pipeline: Channel → Redis

Producer (proxy handler, per request):
```
AnalysisMessage {
    endpoint: String,
    timestamp: Instant,
    raw_diffs: HashMap<String, Difference>,     // primary vs candidate
    noise_diffs: HashMap<String, Difference>,    // primary vs secondary
    primary_status: u16,
    candidate_status: u16,
    secondary_status: u16,
}
```

Consumer task (single task, serial processing):
1. Receive from mpsc channel
2. Update DashMap counters (raw, noise, total)
3. XADD per-request diff entry to Redis stream
4. Periodic (every N seconds): snapshot aggregation to Redis hash

Redis stream entry format:
```
XADD riffy:diffs * endpoint <string> timestamp <iso8601>
  raw_fields <json: {field_path: {left, right}}>
  noise_fields <json: {field_path: {left, right}}>
  primary_status <int> candidate_status <int> secondary_status <int>
```

Redis aggregation hash (per endpoint):
```
HSET riffy:agg:<endpoint>
  total <int>
  fields <json: {field_path: {raw_count, noise_count, is_regression}}>
  last_updated <iso8601>
```

## Endpoint Path Matching

Config defines path templates with `:param` placeholders:
```yaml
endpoints:
  - pattern: "/api/v1/users/:id"
```

Matching algorithm:
1. Split request path and template into segments
2. Template segment starting with `:` = wildcard (match any value, capture param name)
3. Exact match on non-`:` segments
4. Segment count must match
5. Unmatched paths use raw path as endpoint key (with query string stripped)

## Error Handling

```rust
#[derive(Debug, thiserror::Error)]
enum AppError {
    #[error("upstream timeout")]
    UpstreamTimeout(#[source] reqwest::Error),    // 504

    #[error("upstream error")]
    UpstreamError(#[source] reqwest::Error),       // 502

    #[error("bad config")]
    BadConfig(String),                              // 500

    #[error("redis error")]
    RedisError(#[source] redis::RedisError),        // 500 (non-fatal, log + continue)
}
```

Internal propagation: `anyhow::Result`. HTTP boundary: `AppError` → `IntoResponse`.

## Observability

- **tracing**: JSON structured logs. RUST_LOG env for filtering.
- **Prometheus metrics**:
  - `riffy_proxy_request_total` (labels: method, endpoint, status)
  - `riffy_proxy_request_duration_seconds` (labels: method, endpoint)
  - `riffy_upstream_request_duration_seconds` (labels: upstream, endpoint)
  - `riffy_diff_pipeline_lag_seconds` (time between request received and diff published)
  - `riffy_diff_fields_total` (labels: endpoint, diff_type=raw|noise)
- **Health check**: `GET /healthz` → 204 on admin port
- **Graceful shutdown**: SIGTERM / Ctrl+C → stop accepting → drain in-flight → exit

## Implementation Order

### Phase 1: Skeleton + Proxy
1. `cargo init riffy` with Cargo.toml dependencies
2. `src/config/mod.rs` — figment config structs
3. `src/main.rs` — basic tokio spawn, config load, placeholder axum server
4. `src/proxy/upstream.rs` — reqwest client, send request to upstream, return response
5. `src/proxy/handler.rs` — axum handler: receive request, clone, fan out to 3 upstreams
6. `src/proxy/router.rs` — axum router with catch-all route
7. `src/error.rs` — AppError enum

**Deliverable**: Proxy that forwards requests to primary and returns response. No analysis yet.

### Phase 2: Diff Engine
8. `src/compare/mod.rs` — Difference ADT + `diff()` recursive function on serde_json::Value
9. `src/compare/flatten.rs` — Flatten Difference tree into `HashMap<String, Difference>`
10. Unit tests for diff engine (objects, arrays, nested, type mismatches)

**Deliverable**: Diff engine works on serde_json::Value pairs. Tested.

### Phase 3: Analysis Pipeline
11. `src/endpoint/mod.rs` — Path template matching
12. `src/analysis/collector.rs` — DashMap-based InMemoryDifferenceCollector
13. `src/analysis/joined.rs` — JoinedField with threshold calculations
14. `src/analysis/filter.rs` — DifferencesFilterFactory predicate
15. `src/analysis/mod.rs` — DifferenceAnalyzer: takes 3 responses, produces raw + noise diffs, updates counters

**Deliverable**: Analysis pipeline updates in-memory counters correctly.

### Phase 4: Redis Output
16. `src/redis/mod.rs` — Redis connection pool, XADD, HSET
17. `src/pipeline/mod.rs` — mpsc channel + AnalysisMessage type
18. `src/pipeline/consumer.rs` — Consumer task: receive diffs → update counters → XADD → periodic aggregation

**Deliverable**: Diffs written to Redis stream. Aggregation snapshots in Redis hash.

### Phase 5: Observability + Hardening
19. `src/telemetry/mod.rs` — tracing subscriber (JSON, env filter)
20. `src/telemetry/metrics.rs` — Prometheus exporter + middleware
21. Graceful shutdown in main.rs
22. Health check endpoint
23. Side-effect safety (block mutating methods)
24. Config validation (endpoint patterns, upstream reachability)
25. Integration test: spin up 3 mock HTTP servers, send requests through riffy, verify Redis output

**Deliverable**: Production-ready binary.

## Best Practices

1. **Zero-copy where possible**: Clone body bytes only when needed for fan-out. Use `Bytes` for body buffers.
2. **Connection pooling**: Single `reqwest::Client` shared across all handlers (Arc-wrapped).
3. **Backpressure**: mpsc channel with bounded capacity. If consumer falls behind, drop oldest diff entries (log warning).
4. **No unwrap in proxy path**: All fallible operations return Result. Unwrap only in tests.
5. **jemalloc**: Default allocator for production (lower memory fragmentation under load).
6. **Header forwarding**: Forward all headers except hop-by-hop (Connection, Keep-Alive, Transfer-Encoding, TE, Upgrade).
7. **Timeout budget**: Upstream timeout < overall request timeout. Fail fast on slow upstreams.
8. **No body buffering for large payloads**: Stream bodies when possible. Set max body size limit (configurable).
