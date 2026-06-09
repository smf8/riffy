# Riffy — Progress Tracker

## Status: PHASE 1 COMPLETE (REVISED) — Ready for Phase 2 (Diff Engine)

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

### Phase 2: Diff Engine ← NEXT
- [ ] 2.1 src/compare/mod.rs — Difference ADT + diff()
- [ ] 2.2 src/compare/flatten.rs — recursive flatten to dot-path map
- [ ] 2.3 Unit tests for diff engine

### Phase 3: Analysis Pipeline
- [ ] 3.1 src/endpoint/mod.rs — path template matching
- [ ] 3.2 src/analysis/collector.rs — DashMap InMemoryDifferenceCollector
- [ ] 3.3 src/analysis/joined.rs — JoinedField + threshold calculations
- [ ] 3.4 src/analysis/filter.rs — DifferencesFilterFactory predicate
- [ ] 3.5 src/analysis/mod.rs — DifferenceAnalyzer wiring

### Phase 4: Redis Output
- [ ] 4.1 src/redis/mod.rs — connection pool, XADD, HSET
- [ ] 4.2 src/pipeline/mod.rs — mpsc channel, AnalysisMessage
- [ ] 4.3 src/pipeline/consumer.rs — consumer task

### Phase 5: Observability + Hardening
- [ ] 5.1 src/telemetry/mod.rs — tracing subscriber (already done in main.rs)
- [ ] 5.2 src/telemetry/metrics.rs — Prometheus exporter + middleware
- [ ] 5.3 Graceful shutdown in main.rs (already done)
- [ ] 5.4 Health check endpoint (already done — GET /healthz)
- [ ] 5.5 Side-effect safety (already done in handler.rs)
- [ ] 5.6 Config validation
- [ ] 5.7 Integration test

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

## Notes for Next Session

- Phase 1 revised per user feedback. All 7 review items addressed.
- Proxy flow: primary = blocking hot path, candidate+secondary = background analysis only.
- Compression: raw bytes passthrough. No reqwest decompression. Analysis pipeline must handle decompression for JSON parsing (Phase 3/4).
- JSON validation lives in analysis pipeline, not proxy handler.
- Placeholder consumer task in main.rs logs analysis messages. Phase 4 replaces it.
- `AnalysisMessage` carries all 3 upstream responses. Uses raw path as endpoint (TODO: path template matching in Phase 3).
- Module dirs exist: `src/{compare,analysis,endpoint,pipeline,redis,telemetry}` — all empty, ready for Phase 2+.
- See Plan.md for full architecture, algorithms, and implementation details.
