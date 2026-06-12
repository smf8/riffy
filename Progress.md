# Riffy — Progress Tracker

## Status: ALL PHASES (1–5) COMPLETE

## Decisions Log

| # | Decision | Choice | Rationale |
|---|----------|--------|-----------|
| Q1 | Endpoint scope | Static config, one triplet | MVP scope. Multi-endpoint later. |
| Q2 | Proxy mode | Primary first (blocking), candidate+secondary background | Zero added latency for client. Analysis doesn't affect proxy. |
| Q3 | Noise detection | Full statistical (diffy-style) | Core value prop. Per-field counters. |
| Q4 | Redis output | Per-request diffs + periodic aggregation | Stream + hash. |
| Q5 | Content types | JSON only | serde_json::Value. Add HTML/text later. |
| Q6 | HTTP client | reqwest | Battle-tested, connection pooling. |
| Q7 | Config format | figment (YAML + ENV) | Consistent with abrust. |
| Q8 | Endpoint ID | Explicit path templates (:param) | User defines patterns. No auto-detect. |
| Q9 | Aggregation DS | DashMap + AtomicU64 | Lock-free counters. High throughput. |
| Q10 | Flush strategy | mpsc channel → consumer task | Clean separation. Backpressure. |
| Q11 | Side-effect safety | Block mutating methods by default | Safe by default. Config override. |
| Q12 | Project name | riffy | Rust + diffy. |
| Q13 | Response mode | Always primary | Client always sees primary. No choice. |
| Q14 | Observability | Full stack | tracing + Prometheus + health + graceful shutdown. |
| Q15 | Error handling | anyhow + thiserror dual | Consistent with abrust. Typed HTTP errors. |

## Revisions (Phase 1)

| Rev | What changed | Why |
|-----|-------------|-----|
| R1 | Tracing init → Registry-based (abrust pattern) | ChronoLocal timer, crate-specific filter directives, JSON fmt layer |
| R2 | Proxy flow → primary first, candidate+secondary in background tokio::spawn | Zero latency impact on client. Analysis is fire-and-forget. |
| R3 | Removed ResponseMode enum/config | Always return primary response. Simplifies handler. |
| R4 | Compression: strip transfer-encoding + content-length from upstream response, set correct content-length in handler | We buffer full body. Must not send stale headers. |
| R5 | Config: made redis.uri, server.address, server.port mandatory | No silent defaults for production-critical values. |
| R6 | Added #[allow(dead_code)] on structs/variants for later phases | make lint passes with -D warnings. |

## Implementation Phases

### Phase 1: Skeleton + Proxy ✅ (Revised)
- [x] 1.1 Cargo.toml with all dependencies
- [x] 1.2 src/config/mod.rs — figment config structs (mandatory fields for production-critical config)
- [x] 1.3 src/error.rs — AppError enum
- [x] 1.4 src/main.rs — tokio entry, Registry-based tracing, dual servers, graceful shutdown
- [x] 1.5 src/proxy/upstream.rs — reqwest client wrapper, hop-by-hop + content-length header stripping
- [x] 1.6 src/proxy/handler.rs — primary first, background candidate+secondary via tokio::spawn
- [x] 1.7 src/proxy/router.rs — catch-all route + AppState + AnalysisMessage
- [x] 1.8 config.example.yaml

**make format + make lint pass clean.**

### Phase 2: Diff Engine ✅
- [x] 2.1 src/compare/mod.rs — Difference ADT + diff()
- [x] 2.2 src/compare/flatten.rs — recursive flatten to dot-path map
- [x] 2.3 Unit tests for diff engine (src/compare/tests/)

### Phase 3: Analysis Pipeline ✅
- [x] 3.1 src/endpoint/mod.rs — path template matching (EndpointMatcher; resolution happens in consumer, off hot path)
- [x] 3.2 src/analysis/collector.rs — DashMap InMemoryDifferenceCollector
- [x] 3.3 src/analysis/joined.rs — JoinedField + threshold calculations
- [x] 3.4 src/analysis/filter.rs — DifferencesFilter predicate (factory pattern simplified to a struct)
- [x] 3.5 src/analysis/mod.rs — DifferenceCollector trait + DifferenceAnalyzer

### Phase 4: Redis Output ✅
- [x] 4.1 src/redis/mod.rs — DiffStore trait; store.rs (RedisDiffStore: ConnectionManager, XADD, pipelined HSET); memory.rs (InMemoryDiffStore for tests)
- [x] 4.2 src/pipeline/mod.rs — mpsc channel, AnalysisMessage (moved here from proxy/router.rs)
- [x] 4.3 src/pipeline/consumer.rs — consumer task (endpoint resolve → analyze → XADD → periodic + final aggregation flush)

### Phase 5: Observability + Hardening ✅
- [x] 5.1 src/telemetry/mod.rs — init_tracing moved here from main.rs
- [x] 5.2 src/telemetry/metrics.rs — Prometheus exporter (admin /metrics), proxy middleware (request total + duration), upstream durations, pipeline lag, diff fields counters
- [x] 5.3 Graceful shutdown — consumer now drained with 5s timeout (was abort())
- [x] 5.4 Health check endpoint (GET /healthz on admin port)
- [x] 5.5 Side-effect safety (handler.rs)
- [x] 5.6 Config validation — Riffy::validate() called from config::load()
- [x] 5.7 Integration test — tests/proxy_integration.rs (3 mock upstreams → proxy → InMemoryDiffStore)

## Files Created (Phase 1)

| File | Purpose |
|------|---------|
| Cargo.toml | Dependencies: tokio, axum, reqwest, serde, figment, redis, dashmap, thiserror, tracing, metrics, bytes, humantime-serde |
| config.example.yaml | Full config spec. Mandatory: service-name, proxy.port, upstream.*, endpoints, redis.uri, server.* |
| src/main.rs | Entry point, dual servers (proxy+admin), Registry-based tracing, graceful shutdown |
| src/config/mod.rs | figment config structs. Mandatory vs default fields documented. |
| src/error.rs | AppError enum: UpstreamTimeout(504), UpstreamError(502), AllUpstreamsFailed(502), BadConfig(500), RedisError(500), BodyReadError(400) |
| src/proxy/mod.rs | Module re-exports |
| src/proxy/upstream.rs | UpstreamClient: reqwest wrapper, skip hop-by-hop + content-length, response model |
| src/proxy/handler.rs | proxy_handler: side-effect check, primary first, tokio::spawn for candidate+secondary, correct content-length |
| src/proxy/router.rs | AppState, AnalysisMessage, catch-all router |

## Revisions (Phases 2–5)

| Rev | What changed | Why |
|-----|-------------|-----|
| R7 | OrderingDifference detection moved to the same-size array branch (multiset-equal → Ordering) | The old check (different sizes, both set-diffs empty) was mathematically unreachable. Matches diffy semantics. |
| R8 | Flatten paths: no leading dot for root fields, arrays use `parent.0` (was `parent[0]`) | Conform to Plan.md §Flattening dot-path spec. |
| R9 | TypeDifference dropped `left_type`/`right_type` fields | Never read; types are evident from the stored left/right values. |
| R10 | All unit tests moved to `tests/` subdirectories per module (src/*/tests/*.rs) | Project convention: tests never co-located with implementation. |
| R11 | Endpoint resolution happens in the consumer + metrics middleware, not the proxy handler | Keeps analysis work off the hot path; middleware shares the resolved key via request extensions. |
| R12 | Handler uses `try_send` (drops newest on full channel, warns) | Plan said "drop oldest"; try_send is the simple non-blocking equivalent without consumer cooperation. |
| R13 | Diff entries written to the stream only when raw/noise diffs are non-empty or statuses mismatch | Avoids flooding Redis with no-op entries; totals still counted in the collector. |
| R14 | DiffStore trait (redis/mod.rs) with RedisDiffStore + InMemoryDiffStore impls | Redis conventions: storage behind a trait, swappable for tests/local dev. |
| R15 | redis crate `connection-manager` feature; ConnectionManager for auto-reconnect; pipelined HSET aggregation | Minimize RTT, survive Redis restarts. |
| R16 | ~~Compressed (content-encoding) bodies are skipped in analysis~~ Superseded by R20. | — |
| R17 | Crate split into lib (src/lib.rs) + thin bin (src/main.rs) | Required so tests/ at crate root can drive the proxy end-to-end. |
| R18 | UpstreamClient sets reqwest `.no_proxy()` | Upstreams are direct in-cluster targets; env HTTP_PROXY (set on this dev machine) silently broke upstream calls. |
| R19 | Consumer drained on shutdown (5s timeout) instead of abort() | Final aggregation snapshot flush before exit. |
| R20 | async-compression (user-approved): pipeline/decode.rs decompresses gzip/x-gzip, deflate (zlib per RFC 9110), br, zstd before JSON parsing | Analysis-side only, off the hot path. Unsupported encodings and corrupt bodies are skipped with a warning. |
| R21 | Cancellation-aware metrics via drop guards (ProxyRequestGuard, UpstreamTimer in telemetry/metrics.rs): `riffy_proxy_request_total` status gains `cancelled`; `riffy_upstream_request_duration_seconds` gains `outcome` label (ok/error/cancelled) — a label deviation from Plan.md | Drop-based cancellation skipped all post-await recording, so abandoned (slowest) requests were invisible and histograms had survivorship bias; upstream errors/timeouts were also indistinguishable from successes. |

## Notes for Next Session

- **All 5 phases complete.** `make format`, `make lint` (zero warnings) and `make test` (101 tests: unit + integration) pass.
- Metrics are cancellation-aware (R21): `ProxyRequestGuard`/`UpstreamTimer` drop guards record `cancelled` samples when futures are dropped mid-flight. Guard tests install a real Prometheus recorder (process-global, shared via OnceLock in src/telemetry/tests/).
- Decompression decision resolved: user chose **async-compression** (R20). Supported: gzip, x-gzip, deflate (zlib), br, zstd. Multi-token `content-encoding` values (e.g. "gzip, br") are not handled — treated as unsupported and skipped.
- **Open question:** the Redis stream (`riffy:diffs`) is uncapped — consider a configurable `XADD MAXLEN ~ N` to bound memory.
- `metrics.port` config field is currently unused — metrics are served on the admin port (`server.port`) at `/metrics`. Either honor it with a separate listener or remove the field.
- Real-Redis behavior (RedisDiffStore) is exercised only at the type level; integration tests use InMemoryDiffStore. Manual verification against `docker compose up -d` Redis still pending.
- Per-request param values from `:param` templates are not captured (only the template is used as the endpoint key) — not needed by the current data model.
- **docs/architecture.md** holds the runtime DAG (Mermaid) + metrics/Redis data tables; it documents what the code does and cites R# revisions on deviations from Plan.md. Maintain it via the `update-architecture-doc` skill (`.claude/skills/update-architecture-doc/SKILL.md`), which also encodes the standing doc rules from user feedback.
- See Plan.md for full architecture, algorithms, and implementation details.
