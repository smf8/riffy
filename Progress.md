# Riffy — Progress Tracker

## Status: ALL PHASES (1–5) COMPLETE + review.md fixes (R22–R28) + query API & opt-in Redis (R29) + terminology/packaging sweep (R30) + raw-counts/classify-on-read (R31)

> **Terminology (R30):** upstreams are **baseline** (served + trusted), **candidate** (new code), **control** (baseline replica → noise floor). Diffs: **raw** = baseline vs candidate, **noise** = baseline vs control; a field is a **regression** when raw exceeds noise past the thresholds. Module layout: `http/` (forward + query + routers), `upstream/` (the client). This supersedes the diffy `primary`/`secondary` names used in `Plan.md`.

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

## Revisions (review.md fixes)

| Rev | What changed | Why |
|-----|-------------|-----|
| R22 | Generic type parameters removed: `Consumer<C, S>` → plain `Consumer` holding `Arc<dyn DifferenceCollector>` + `Arc<dyn DiffStore>`; `EndpointMatcher::new<I, S>` → `new(&[String])`; `DiffStore` converted to `#[async_trait]` (new `async-trait` dep) so it works as a trait object — boxing is analysis-side only, never on the hot path | review.md: code must be easy to understand for non-Rust experts; no unnecessary generics. |
| R23 | Consumer checks HTTP status before comparing bodies: a body is decoded/parsed/diffed only when that upstream returned the same status as primary; a different status is reported directly as the signal. `DifferenceAnalyzer` dissolved into the consumer (it reduced to a passthrough); `AnalysisError` deleted; analyzer tests merged into consumer tests | review.md: skip body processing on status mismatch; no Option params in analysis. |
| R24 | New `src/handler/` module owns all HTTP input: `handler/proxy.rs` (proxy_handler), `handler/router.rs` (AppState, proxy router, admin router with /healthz + /metrics — moved out of main.rs). `src/proxy/` keeps only actual proxying (upstream.rs, error.rs) | review.md: separate HTTP input interactions from proxying. |
| R25 | `src/redis/` → `src/storage/`: `DiffStore` trait + models (DiffEntry, EndpointAggregation, FieldAggregation) in mod.rs per the traits-in-mod.rs convention; `redis.rs` (RedisDiffStore), `memory.rs` (InMemoryDiffStore). Redis key conventions unchanged | review.md: model + trait don't belong in a backend-named module. |
| R26 | Dockerfile (multi-stage: rust:1.94-slim + cargo-chef layer caching → distroless/cc-debian12:nonroot, non-root UID 65532, rustls so no OpenSSL) + .dockerignore + `[profile.release]` lto=thin, strip=true (panic stays unwind so a panicking analysis task can't abort the proxy) | review.md: production-ready image with security/performance best practices. |
| R27 | Diff-crate feasibility researched (serde_json_diff, jsondiffpatch, json_diff_ng, treediff, json-patch): none implement the diffy multiset/ordering array semantics (serde_json_diff is positional-only, jsondiffpatch is LCS, json-patch is a patch format). Custom `compare` module stays | review.md: evaluate replacing compare with an existing crate. |
| R28 | All per-item `#[allow(dead_code)]` attributes removed (supersedes R6); `make lint` now passes `-A dead_code` to clippy instead. `#[allow(unused_imports)]` re-export attrs (storage/mod.rs, compare/mod.rs) are untouched — different lint | User request: one Makefile-level policy beats 12 scattered attributes. Note: plain `cargo build` now prints dead_code warnings; only `make lint` suppresses them. |
| R31 | **Store raw counts only; classify at read time (resolves the live-counters vs stored-snapshot duality).** `FieldAggregation` dropped `is_regression` — the store now persists only `raw_count`/`noise_count` (+ endpoint `total`). The `LiveCounters` DashMap is explicitly a write buffer drained on a short flush (default `aggregation-interval` 10s→**1s**; in-memory fallback const also 1s). The `RegressionClassifier` moved off the consumer (no longer a `Consumer` field/param) onto `AdminState`; `diff_detail` derives `is_regression` + relative/absolute % at read time via `FromRef<AdminState> for RegressionClassifier`. All reads go through the store; per-instance in-memory views and the Redis published view are both accepted (user decision). Benefit: changing a threshold reclassifies instantly with no re-flush; one canonical stored representation. Old Redis hashes with a stale `is_regression` field still parse (serde ignores it) | User decision: counters are a buffer only; store raw data; compute stats on every read. |
| R30 | **Terminology + packaging clarity sweep (drop diffy jargon).** Upstreams renamed `primary`→`baseline`, `secondary`→`control` (`candidate` kept) across config (`upstream.*`), `UpstreamClient` fields, `DiffEntry`/`AnalysisMessage` fields (`*_status`/`*_response`), Redis stream field names, and the `upstream=` metric label values. `raw`/`noise` kept (user choice). Modules: `handler/`→`http/` (`proxy.rs`→`forward.rs`, `proxy_handler`→`forward`), `proxy/`→`upstream/` (`upstream.rs`→`client.rs`), resolving the handler/proxy name collision. Types: `DifferenceCollector`→`DiffCounters`, `InMemoryDifferenceCollector`→`LiveCounters` (`collector.rs`→`counters.rs`), `JoinedField`/`JoinedEndpoint`→`FieldSnapshot`/`EndpointSnapshot` (`joined.rs`→`snapshot.rs`), `DifferencesFilter`→`RegressionClassifier` (`filter.rs`→`classify.rs`), `FlatDiff`→`FieldDiff`, `ProxyError`→`UpstreamError`, `AppError::Proxy`→`AppError::Upstream`. `compare/` & `analysis/` kept separate (targeted scope). Docs (architecture.md, CLAUDE.md, this skill) updated; Plan.md left as historical with a forward-pointer. **Breaking** for existing config files, Redis data, and Prometheus dashboards | User request: make the project easier to understand by dropping diffy terminology and the module-name collision. |
| R29 | **Redis is opt-in + read-side query API.** Config `redis` is now `Option<RedisConfig>`; absent → `InMemoryDiffStore` with a 10s default aggregation interval (the snapshot cadence the read API depends on). The store `Arc<dyn DiffStore>` is shared between the consumer (writer) and the admin server (reader). `DiffStore` gained three read methods: `get_aggregation` (HGETALL `riffy:agg:{endpoint}`), `list_aggregations` (cursor SCAN `riffy:agg:*` + pipelined HGETALL, dedup), `recent_samples` (paged XREVRANGE on `riffy:diffs`, exclusive `(id` cursor, newest-first, `offset`/`limit`). New admin routes `GET /diffs/paths` (endpoints → diffing field paths; optional `?endpoint=`) and `GET /diffs/detail?endpoint=&path=` (field stats + paginated samples). Supporting changes: `FlatDiff`/`DiffType`/`FieldAggregation` gained `Deserialize`; chrono `serde` feature enabled; `AppError` gained `Storage`(500)/`NotFound`(404); `admin_router` now takes `AdminState { metrics, store }` with `FromRef` substate extraction (metrics handler signature unchanged) | User request: expose recorded diffs over HTTP and make persistence optional. |

## Notes for Next Session

- **Store now holds raw counts; classification is read-time (R31).** `LiveCounters` is a write buffer flushed every `aggregation-interval` (now 1s). `is_regression`/percentages are computed in `diff_detail` from the stored counts via the `RegressionClassifier` in `AdminState`. The consumer no longer takes a classifier. `make format`/`make lint` (zero warnings)/`make test` (102) pass.
- **Open item this raises:** the per-request **sample stream** (`riffy:diffs`) is still uncapped (pre-existing). With reads now the only consumer of samples and an O(1) count path, capping the stream (`XADD MAXLEN ~ N`) is the remaining scalability lever — see the long-standing open question below.
- **Terminology/packaging sweep done (R30).** All renames are mechanical; `make format`, `make lint` (zero warnings), and `make test` (102 tests) pass. The earlier revision rows (R1–R29) and `Plan.md` still use the old `primary`/`secondary` names and old `handler/`/`proxy/` paths — they are **historical**; the code, `config.example.yaml`, `docs/architecture.md`, and `CLAUDE.md` are the current source of truth (baseline/control, `http/`/`upstream/`).
- **R30 is breaking:** existing `config.yaml` files (`upstream.primary/secondary` → `baseline/control`), persisted Redis data (`*_status` stream fields), and Prometheus dashboards (`upstream=` label values) must be migrated. Project is `0.1.0` with no external consumers, so this was the moment to do it.
- **Read-side query API + opt-in Redis added (R29).** `make format`, `make lint` (zero warnings) and `make test` (**102 tests**) pass. New tests: `src/storage/tests/memory_test.rs` (get/list aggregation, sample pagination) and `tests/query_api.rs` (both query endpoints over the admin router with an in-memory store), plus `config::tests::absent_redis_section_is_valid`.
- **Query API lives on the admin server (`server.port`)** — `GET /diffs/paths` and `GET /diffs/detail` — because the proxy port is a catch-all proxy. Endpoint keys (and `:param` templates) contain `/` and `:`, so both are passed as **query params**, not path segments.
- **Redis read paths (`get_aggregation`/`list_aggregations`/`recent_samples`) are only type-checked, not exercised** — like the write paths, integration tests use `InMemoryDiffStore`. `list_aggregations` uses cursor SCAN (not a maintained endpoint-index Set); fine for low endpoint cardinality, revisit if it grows. `recent_samples` uses XREVRANGE with an exclusive `(id` cursor (Redis ≥ 6.2). Manual verification against `docker compose up -d` Redis still pending.
- The detail endpoint reads field stats from the aggregation **snapshot** (so `is_regression`/counts lag by ≤ the aggregation interval) and the actual left/right `samples` from the per-request stream. With the in-memory store, the read API only sees data after the consumer's periodic flush has run.
- **All 5 phases complete; review.md fixes (R22–R28) applied.** Test count dropped from 101: six analyzer tests merged/removed with the analyzer, two new consumer tests added (status-mismatch skips body compare; invalid candidate JSON skipped).
- **Docker image build not yet verified** — the Docker daemon was not running on this machine. Run `docker build -t riffy .` to confirm the multi-stage build (cargo-chef install + release build inside the container takes several minutes on first run).- Metrics are cancellation-aware (R21): `ProxyRequestGuard`/`UpstreamTimer` drop guards record `cancelled` samples when futures are dropped mid-flight. Guard tests install a real Prometheus recorder (process-global, shared via OnceLock in src/telemetry/tests/).
- Decompression decision resolved: user chose **async-compression** (R20). Supported: gzip, x-gzip, deflate (zlib), br, zstd. Multi-token `content-encoding` values (e.g. "gzip, br") are not handled — treated as unsupported and skipped.
- **Open question:** the Redis stream (`riffy:diffs`) is uncapped — consider a configurable `XADD MAXLEN ~ N` to bound memory.
- `metrics.port` config field is currently unused — metrics are served on the admin port (`server.port`) at `/metrics`. Either honor it with a separate listener or remove the field.
- Real-Redis behavior (RedisDiffStore) is exercised only at the type level; integration tests use InMemoryDiffStore. Manual verification against `docker compose up -d` Redis still pending.
- Per-request param values from `:param` templates are not captured (only the template is used as the endpoint key) — not needed by the current data model.
- **docs/architecture.md** holds the runtime DAG (Mermaid) + metrics/Redis data tables; it documents what the code does and cites R# revisions on deviations from Plan.md. Maintain it via the `update-architecture-doc` skill (`.claude/skills/update-architecture-doc/SKILL.md`), which also encodes the standing doc rules from user feedback.
- See Plan.md for full architecture, algorithms, and implementation details.
