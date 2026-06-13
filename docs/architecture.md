# Riffy — Runtime Architecture (DAG)

Riffy is a reverse proxy that detects statistical regressions between three
upstream deployments of the same service. Every request is answered from the
**primary** upstream with zero analysis overhead; the **candidate** (new code)
and **secondary** (primary replica) are called in the background, and their
responses are diffed against the primary asynchronously. Fields where
primary-vs-candidate disagreement (**raw**) significantly exceeds
primary-vs-secondary disagreement (**noise**) are flagged as real regressions.

This document describes what the code *does today*. Where it deviates from
`Plan.md`, the deviation is recorded as a numbered revision (R#) in
`Progress.md`. To update this doc, use the `update-architecture-doc` skill in
`.claude/skills/`.

## Request & Analysis DAG

```mermaid
flowchart TD
    client(["Client"]) --> mw

    subgraph proxyserver ["Proxy server — axum on proxy.port (src/handler/router.rs, R24)"]
        mw["track_proxy middleware<br/>resolve endpoint template once;<br/>drop-guard records count + duration<br/>exactly once — real status, or<br/>status=cancelled if dropped (R21)<br/>(src/telemetry/metrics.rs)"]
        guard{"mutating method and<br/>allow-http-side-effects=false?<br/>(src/handler/proxy.rs)"}
        mw --> guard
    end

    guard -- "yes" --> blocked(["405 Method Not Allowed<br/>(no upstream is contacted)"])

    subgraph hotpath ["HOT PATH — client-blocking; must never carry analysis work (R2)"]
        primary["UpstreamClient.send → PRIMARY<br/>reqwest, .no_proxy() (R18),<br/>hop-by-hop headers stripped<br/>(src/proxy/upstream.rs)"]
        resp(["Client response<br/>= primary response, always (Q13/R3)"])
        guard -- "no" --> primary --> resp
    end

    primary -. "tokio::spawn — fire-and-forget" .-> fanout

    subgraph background ["Background task, one per request (src/handler/proxy.rs)"]
        fanout["tokio::join!<br/>CANDIDATE + SECONDARY in parallel<br/>(src/proxy/upstream.rs)"]
        send["try_send AnalysisMessage<br/>bounded mpsc, capacity 1024;<br/>drop newest + warn when full (R12)<br/>(src/pipeline/mod.rs)"]
        fanout --> send
    end

    send --> recv

    subgraph consumer ["Analysis consumer — single task (src/pipeline/consumer.rs)"]
        recv["recv AnalysisMessage"]
        resolve["EndpointMatcher.resolve<br/>:param template match,<br/>else raw path, query stripped<br/>(src/endpoint/mod.rs)"]
        primaryparse["parse primary body:<br/>decode_body — gzip / x-gzip / deflate(zlib) / br / zstd<br/>via async-compression (R20) — then JSON parse;<br/>unparseable → skip request<br/>(src/pipeline/decode.rs, consumer.rs)"]
        statuscheck{"per upstream:<br/>responded with same<br/>status as primary? (R23)"}
        diff["parse body, then flatten_value:<br/>diff → flatten to dot-paths<br/>raw = primary vs candidate<br/>noise = primary vs secondary<br/>(src/compare/)"]
        nodiff["body not compared —<br/>empty diff map; a status mismatch<br/>is itself the signal (R23)"]
        record["DifferenceCollector.record<br/>DashMap + AtomicU64 counters:<br/>endpoint total, per-field raw / noise<br/>(src/analysis/collector.rs)"]
        decide{"any raw/noise diffs,<br/>or upstream status mismatch?"}
        entry["DiffEntry (R13)"]
        skip(["no stream entry —<br/>only counters moved"])
        recv --> resolve --> primaryparse --> statuscheck
        statuscheck -- "yes" --> diff --> record
        statuscheck -- "no / upstream failed" --> nodiff --> record
        record --> decide
        decide -- "yes" --> entry
        decide -- "no" --> skip

        ticker["interval ticker<br/>redis.aggregation-interval (10s default)<br/>+ one final flush on shutdown (R19)"]
        snapshot["collector.snapshot →<br/>JoinedEndpoint / JoinedField<br/>(src/analysis/joined.rs)"]
        classify["DifferencesFilter.is_regression:<br/>raw > noise<br/>AND relative diff > threshold.relative (20%)<br/>AND absolute diff > threshold.absolute (0.03%)<br/>(src/analysis/filter.rs)"]
        ticker --> snapshot --> classify
    end

    entry --> xadd
    classify --> hset

    subgraph store ["DiffStore trait (src/storage/mod.rs, R25) — RedisDiffStore (redis.rs) / InMemoryDiffStore for tests (memory.rs)"]
        xadd[("XADD riffy:diffs<br/>per-request diff entry")]
        hset[("HSET riffy:agg:{endpoint}<br/>pipelined snapshot, one RTT (R15)")]
    end
```

Solid arrows are data flow within one request's lifecycle; the dotted arrow is
the only hand-off from the client-blocking path to async work. The graph is
acyclic: the ticker is an independent root, not a back-edge.

## Observability sidecar

```mermaid
flowchart LR
    operator(["Operator / Prometheus"]) --> admin

    subgraph admin ["Admin server — axum on server.port, admin_router (src/handler/router.rs, R24)"]
        hz["GET /healthz → 204<br/>(src/handler/router.rs)"]
        mx["GET /metrics → PrometheusHandle.render<br/>empty body when metrics.enabled=false<br/>(src/telemetry/metrics.rs)"]
    end
```

| Metric | Labels | Emitted from |
|--------|--------|--------------|
| `riffy_proxy_request_total` | method, endpoint, status (HTTP code or `cancelled`) | `ProxyRequestGuard` in `track_proxy` |
| `riffy_proxy_request_duration_seconds` | method, endpoint | `ProxyRequestGuard` in `track_proxy` |
| `riffy_upstream_request_duration_seconds` | upstream (primary/candidate/secondary), endpoint, outcome (`ok`/`error`/`cancelled`) | `UpstreamTimer` in `proxy_handler` + its background task |
| `riffy_diff_pipeline_lag_seconds` | — | consumer, after a diff entry is stored |
| `riffy_diff_fields_total` | endpoint, diff_type (raw/noise) | consumer, after a diff entry is stored |

Request and upstream timings are recorded by **drop guards** (R21): when a
future is dropped at an `.await` (client disconnect, shutdown, panic unwind),
the guard's `Drop` impl records the sample with `status="cancelled"` /
`outcome="cancelled"` instead of losing it. Duration histograms therefore
include abandoned requests (time until abandonment) and carry no survivorship
bias. Consumer-side metrics need no guard — they run in a detached task that
client cancellation cannot drop.

## Data written to Redis

**Stream entry** (`XADD riffy:diffs`), one per request that produced diffs or a
status mismatch:

| Field | Content |
|-------|---------|
| `endpoint` | resolved template (e.g. `/api/v1/users/:id`) or raw path |
| `timestamp` | RFC 3339 |
| `raw_fields` / `noise_fields` | JSON: `{ "<dot.path>": { "left"?, "right"?, "diff_type" } }` |
| `primary_status` | always present |
| `candidate_status` / `secondary_status` | omitted when that upstream failed |

`diff_type` is one of `primitive`, `missing_field`, `extra_field`, `seq_size`,
`ordering`, `type_mismatch` (`src/compare/flatten.rs`).

**Aggregation hash** (`HSET riffy:agg:{endpoint}`), rewritten every
`redis.aggregation-interval`:

| Field | Content |
|-------|---------|
| `total` | requests analyzed for this endpoint |
| `fields` | JSON: `{ "<dot.path>": { "raw_count", "noise_count", "is_regression" } }` |
| `last_updated` | RFC 3339 |

## Invariants (do not regress)

1. **Hot path is sacred (R2):** nothing between "request received" and "primary
   response returned" may block on, wait for, or compute analysis. Candidate
   and secondary calls, decoding, diffing, and Redis I/O all live behind
   `tokio::spawn` + the mpsc channel.
2. **The client always receives the primary response (Q13/R3).** There is no
   response-mode configuration.
3. **Mutating methods (POST/PUT/PATCH/DELETE) are blocked before any upstream
   is contacted** unless `proxy.allow-http-side-effects` is set (Q11).
4. **A failed candidate/secondary must not poison counters:** absent or
   unparseable bodies contribute empty diff maps; an unparseable primary skips
   the request entirely (not counted in totals).
   Statuses are checked before bodies (R23): a candidate/secondary that
   answered with a different status than primary is reported as a status
   mismatch directly — its body is never decoded or compared.
5. **Backpressure sheds load, it never queues unbounded:** a full analysis
   channel drops the newest message with a warning (R12).
6. **Every tracked request/upstream call is recorded exactly once (R21):**
   completion records the real status/outcome; cancellation records
   `cancelled` via the guard's `Drop`. No code path may silently skip a
   metric sample.
