# Riffy — Runtime Architecture (DAG)

Riffy is a reverse proxy that detects statistical regressions between three
upstream deployments of the same service. Every request is answered from the
**baseline** upstream with zero analysis overhead; the **candidate** (new code)
and **control** (baseline replica) are called in the background, and their
responses are diffed against the baseline asynchronously. Fields where
baseline-vs-candidate disagreement (**raw**) significantly exceeds
baseline-vs-control disagreement (**noise**) are flagged as real regressions.

This document describes what the code *does today* and is the single source of
truth for the runtime architecture. The inline `(R#)` tags mark deliberate
design decisions made along the way — historical markers, not entries in a
separate changelog. To update this doc, use the `update-architecture-doc` skill
in `.claude/skills/`.

## Request & Analysis DAG

```mermaid
flowchart TD
    client(["Client"]) --> mw

    subgraph proxyserver ["Proxy server — axum on server.proxy-port (src/http/router.rs, R24/R33)"]
        mw["track_proxy middleware<br/>resolve endpoint template once;<br/>drop-guard records count + duration<br/>exactly once — real status, or<br/>status=cancelled if dropped (R21)<br/>(src/telemetry/metrics.rs)"]
        guard{"mutating method and<br/>allow-http-side-effects=false?<br/>(src/http/forward.rs)"}
        mw --> guard
    end

    guard -- "yes" --> blocked(["405 Method Not Allowed<br/>(no upstream is contacted)"])

    subgraph hotpath ["HOT PATH — client-blocking; must never carry analysis work (R2)"]
        baseline["UpstreamClient.send → BASELINE<br/>reqwest, .no_proxy() (R18), scheme derived<br/>from address (R33), hop-by-hop headers stripped<br/>(src/upstream/client.rs)"]
        resp(["Client response<br/>= baseline response, always (Q13/R3)"])
        guard -- "no" --> baseline --> resp
    end

    baseline -. "tokio::spawn — fire-and-forget" .-> fanout

    subgraph background ["Background task, one per request (src/http/forward.rs)"]
        fanout["tokio::join!<br/>CANDIDATE + CONTROL in parallel<br/>(src/upstream/client.rs)"]
        send["try_send AnalysisMessage<br/>bounded mpsc, capacity = pipeline.channel-capacity<br/>(default 1024, R32); drop newest + warn when full (R12)<br/>(src/pipeline/mod.rs)"]
        fanout --> send
    end

    send --> recv

    subgraph consumer ["Analysis consumer — single task (src/pipeline/consumer.rs)"]
        recv["recv AnalysisMessage"]
        resolve["EndpointMatcher.resolve<br/>:param template match,<br/>else raw path, query stripped<br/>(src/endpoint/mod.rs)"]
        baselineparse["parse baseline body:<br/>decode_body — gzip / x-gzip / deflate(zlib) / br / zstd<br/>via async-compression (R20) — then JSON parse;<br/>unparseable → skip request<br/>(src/pipeline/decode.rs, consumer.rs)"]
        statuscheck{"per upstream:<br/>responded with same<br/>status as baseline? (R23)"}
        diff["parse body, then flatten_value:<br/>diff → flatten to dot-paths<br/>raw = baseline vs candidate<br/>noise = baseline vs control<br/>(src/compare/)"]
        nodiff["body not compared —<br/>empty diff map; a status mismatch<br/>is itself the signal (R23)"]
        record["LiveCounters.record<br/>DashMap + AtomicU64 write buffer:<br/>endpoint total, per-field raw / noise<br/>(src/analysis/counters.rs)"]
        decide{"any raw/noise diffs,<br/>or upstream status mismatch?"}
        entry["DiffEntry (R13)"]
        skip(["no stream entry —<br/>only counters moved"])
        recv --> resolve --> baselineparse --> statuscheck
        statuscheck -- "yes" --> diff --> record
        statuscheck -- "no / upstream failed" --> nodiff --> record
        record --> decide
        decide -- "yes" --> entry
        decide -- "no" --> skip

        ticker["interval ticker — buffer drain<br/>aggregation-interval (1s default) (R29/R31)<br/>+ one final drain on shutdown (R19)"]
        snapshot["LiveCounters.drain →<br/>raw count deltas per field (counters reset);<br/>restored to buffer on store-write failure (R32)<br/>(src/analysis/counters.rs)"]
        ticker --> snapshot
    end

    entry --> xadd
    snapshot --> hset

    subgraph store ["DiffStore trait (src/storage/mod.rs, R25) — RedisDiffStore (redis.rs) / InMemoryDiffStore, selected by storage.backend (default in-memory) (memory.rs, R29/R33)"]
        xadd[("XADD MAXLEN ~ riffy:diffs<br/>per-request diff entry;<br/>capped at storage.stream-cap (R33)")]
        hset[("HINCRBY riffy:agg:{endpoint}<br/>add deltas: total + raw:{path}/noise:{path};<br/>atomic pipeline, sums across instances, one RTT (R32)")]
    end
```

Solid arrows are data flow within one request's lifecycle; the dotted arrow is
the only hand-off from the client-blocking path to async work. The graph is
acyclic: the ticker is an independent root, not a back-edge.

## Admin server (observability + query API)

The admin server carries `AdminState { metrics, store, classifiers, counters }`
(R29/R31/R33); `FromRef` hands each route only the substate it needs. The query
API reads the same `DiffStore` the consumer writes to, so it reflects the
periodic aggregation snapshots (staleness ≤ the aggregation interval) and the
per-request stream. A minimal Alpine.js dashboard is served at `GET /` (HTML +
vendored Alpine embedded via `include_str!`, no build step) and drives that same
read API from a browser (R34).

```mermaid
flowchart LR
    operator(["Operator / Prometheus"]) --> admin

    subgraph admin ["Admin server — axum on server.admin-port, admin_router (src/http/router.rs, R24/R29/R33)"]
        ui["GET / + /alpine.js<br/>embedded Alpine.js dashboard, no build step;<br/>consumes the JSON query API (R34)<br/>(src/http/ui.rs, ui/index.html)"]
        hz["GET /healthz → 204"]
        mx["GET /metrics → PrometheusHandle.render<br/>empty body when metrics.enabled=false<br/>(src/telemetry/metrics.rs)"]
        paths["GET /diffs/paths[?endpoint=]<br/>endpoints → diffing field paths<br/>(src/http/query.rs)"]
        detail["GET /diffs/detail?endpoint=&path=<br/>raw counts + paginated samples;<br/>per-endpoint classifier applied at read time (R31/R33)<br/>(src/http/query.rs)"]
        reset["DELETE /diffs?endpoint=<br/>clear an endpoint's aggregation + live buffer (R33)<br/>(src/http/query.rs)"]
    end

    paths -. "get/list_aggregations" .-> readstore
    detail -. "get_aggregation + recent_samples" .-> readstore
    reset -. "reset_aggregation + counters.reset_endpoint" .-> readstore
    readstore[("DiffStore read side (R29)<br/>HGETALL / SCAN / XREVRANGE / DEL")]
```

The store persists **raw counts only** (R31); `is_regression` and the
relative/absolute percentages are derived in `diff_detail` at read time. Each
configured endpoint may carry its own thresholds, so the classifier is looked
up per endpoint via `EndpointClassifiers` (held in `AdminState`), falling back
to the diffy defaults for unmatched endpoints (R33); changing a threshold
reclassifies everything instantly with no re-flush. Read methods on `DiffStore`
(analysis-side only, never the hot path): `get_aggregation` (HGETALL one
`riffy:agg:{endpoint}` hash, regrouping the flat `raw:{path}`/`noise:{path}`
entries by path), `list_aggregations` (cursor SCAN `riffy:agg:*` + pipelined
HGETALL), `recent_samples` (paged, newest-first XREVRANGE over `riffy:diffs`,
filtered to one endpoint + field path, `offset`/`limit` paginated),
`reset_aggregation` (DEL one `riffy:agg:{endpoint}` hash).

| Query route | Response |
|-------------|----------|
| `GET /diffs/paths` | `{ "endpoints": [ { endpoint, total, paths[], last_updated } ] }`, sorted by endpoint |
| `GET /diffs/paths?endpoint=<ep>` | one `{ endpoint, total, paths[], last_updated }`; 404 if unknown |
| `GET /diffs/detail?endpoint=&path=` | `{ endpoint, path, total, raw_count, noise_count, is_regression, relative_difference, absolute_difference, last_updated, samples }` (`is_regression`/percentages computed at read time from the stored counts); `samples = { items[], limit, offset, has_more }`, newest-first; 404 if nothing recorded for that endpoint+path. `limit` default 20 / max 100 |
| `DELETE /diffs?endpoint=<ep>` | clears the endpoint's stored aggregation counts and its live counter buffer; `204` on success, `404` if the endpoint has no recorded statistics. Samples age out via the stream cap, not purged here (R33) |

| Metric | Labels | Emitted from |
|--------|--------|--------------|
| `riffy_proxy_request_total` | method, endpoint, status (HTTP code or `cancelled`) | `ProxyRequestGuard` in `track_proxy` |
| `riffy_proxy_request_duration_seconds` | method, endpoint | `ProxyRequestGuard` in `track_proxy` |
| `riffy_upstream_request_duration_seconds` | upstream (baseline/candidate/control), endpoint, outcome (`ok`/`error`/`cancelled`) | `UpstreamTimer` in `forward` + its background task |
| `riffy_diff_pipeline_lag_seconds` | — | consumer, after a diff entry is stored |
| `riffy_diff_fields_total` | endpoint, diff_type (raw/noise) | consumer, after a diff entry is stored |

Request and upstream timings are recorded by **drop guards** (R21): when a
future is dropped at an `.await` (client disconnect, shutdown, panic unwind),
the guard's `Drop` impl records the sample with `status="cancelled"` /
`outcome="cancelled"` instead of losing it. Duration histograms therefore
include abandoned requests (time until abandonment) and carry no survivorship
bias. Consumer-side metrics need no guard — they run in a detached task that
client cancellation cannot drop.

**Trace export (R33):** when `logging.otlp.enabled` (off by default), spans are
exported to a Jaeger collector over OTLP/HTTP (`logging.otlp.endpoint`, default
the local Jaeger OTLP receiver) via a `tracing-opentelemetry` layer on the same
subscriber. The batch exporter reuses reqwest/rustls and is flushed on shutdown.

## Data written to Redis

The stream and aggregation **keys are fixed constants** — `storage::DIFF_STREAM_KEY`
(`riffy:diffs`) and `storage::AGGREGATION_KEY_PREFIX` (`riffy:agg`), not config
(R33). The backend (Redis vs in-memory), the `aggregation-interval`, and the
`stream-cap` come from the `storage` config section.

**Stream entry** (`XADD MAXLEN ~ riffy:diffs`, trimmed to `storage.stream-cap`),
one per request that produced diffs or a status mismatch:

| Field | Content |
|-------|---------|
| `endpoint` | resolved template (e.g. `/api/v1/users/:id`) or raw path |
| `timestamp` | RFC 3339 |
| `raw_fields` / `noise_fields` | JSON: `{ "<dot.path>": { "left"?, "right"?, "diff_type" } }` |
| `baseline_status` | always present |
| `candidate_status` / `control_status` | omitted when that upstream failed |

`diff_type` is one of `primitive`, `missing_field`, `extra_field`, `seq_size`,
`ordering`, `type_mismatch` (`src/compare/flatten.rs`).

**Aggregation hash** (`riffy:agg:{endpoint}`), counts incremented with `HINCRBY`
every `aggregation-interval` so concurrent instances sum into the same hash
instead of overwriting (R32):

| Field | Content |
|-------|---------|
| `total` | requests analyzed for this endpoint (cumulative) |
| `raw:{dot.path}` | baseline-vs-candidate diff count at that path |
| `noise:{dot.path}` | baseline-vs-control diff count at that path |
| `last_updated` | RFC 3339 (last buffer drain) |

Per-field counts are flat hash entries (not a JSON blob) so `HINCRBY` can target
them atomically; `is_regression` and the relative/absolute percentages are
derived at read time (R31), never stored.

## Invariants (do not regress)

1. **Hot path is sacred (R2):** nothing between "request received" and "baseline
   response returned" may block on, wait for, or compute analysis. Candidate
   and control calls, decoding, diffing, and Redis I/O all live behind
   `tokio::spawn` + the mpsc channel.
2. **The client always receives the baseline response (Q13/R3).** There is no
   response-mode configuration.
3. **Mutating methods (POST/PUT/PATCH/DELETE) are blocked before any upstream
   is contacted** unless `proxy.allow-http-side-effects` is set (Q11).
4. **A failed candidate/control must not poison counters:** absent or
   unparseable bodies contribute empty diff maps; an unparseable baseline skips
   the request entirely (not counted in totals).
   Statuses are checked before bodies (R23): a candidate/control that
   answered with a different status than baseline is reported as a status
   mismatch directly — its body is never decoded or compared.
5. **Backpressure sheds load, it never queues unbounded:** a full analysis
   channel drops the newest message with a warning (R12).
6. **Every tracked request/upstream call is recorded exactly once (R21):**
   completion records the real status/outcome; cancellation records
   `cancelled` via the guard's `Drop`. No code path may silently skip a
   metric sample.
