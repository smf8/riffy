# Riffy — Metrics & Grafana Reference

This document lists the Prometheus metrics riffy emits and the PromQL/Grafana
queries you can build panels from. Metrics are scraped from the admin server's
`GET /metrics` route (`server.admin_port`); the body is empty when
`metrics.enabled = false`. Each metric is defined in the module that emits it
(see `docs/architecture.md`).

## Histogram buckets

`telemetry::install_prometheus()` configures explicit seconds-scale buckets, so
every `histogram!` exports as a true Prometheus histogram with
`_bucket`/`_sum`/`_count` series. Compute percentiles with `histogram_quantile()`
over the bucket rate — any quantile can be derived after the fact and percentiles
aggregate correctly across instances (unlike client-side summary quantiles).

Configured buckets (seconds):
`0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0`

To change the resolution or range, edit `LATENCY_BUCKETS` in
`src/telemetry/mod.rs`.

## Emitted metrics

| Metric | Type (exported as) | Labels |
|---|---|---|
| `riffy_proxy_request_total` | counter | `method`, `endpoint`, `status` (HTTP code or `cancelled`) |
| `riffy_proxy_request_duration_seconds` | histogram | `method`, `endpoint` |
| `riffy_upstream_request_duration_seconds` | histogram | `upstream` (`baseline`/`candidate`/`control`), `endpoint`, `outcome` (`ok`/`error`/`cancelled`) |
| `riffy_sample_store_lag_seconds` | histogram | *(none)* |
| `riffy_samples_stored_total` | counter | `endpoint` |

`endpoint` is the resolved template (e.g. `/api/v1/users/:id`) or `undefined` for
unmatched paths.

## Panels & queries

Examples use Grafana's `$__rate_interval`. Add a dashboard variable
`$endpoint = label_values(riffy_proxy_request_total, endpoint)` to make panels
per-endpoint.

### Traffic

```promql
# Total request rate (RPS)
sum(rate(riffy_proxy_request_total[$__rate_interval]))

# RPS by endpoint
sum by (endpoint) (rate(riffy_proxy_request_total[$__rate_interval]))

# RPS by status code
sum by (status) (rate(riffy_proxy_request_total[$__rate_interval]))
```

### Errors & client cancellations

```promql
# 5xx error ratio (overall)
sum(rate(riffy_proxy_request_total{status=~"5.."}[$__rate_interval]))
  / sum(rate(riffy_proxy_request_total[$__rate_interval]))

# 4xx ratio by endpoint
sum by (endpoint) (rate(riffy_proxy_request_total{status=~"4.."}[$__rate_interval]))
  / sum by (endpoint) (rate(riffy_proxy_request_total[$__rate_interval]))

# Client-cancelled / disconnected requests (the GuardedTimer "cancelled" path)
sum by (endpoint) (rate(riffy_proxy_request_total{status="cancelled"}[$__rate_interval]))
```

### Client-facing latency (the baseline hot path)

```promql
# p99 latency by endpoint (swap 0.99 for any quantile)
histogram_quantile(0.99,
  sum by (le, endpoint) (rate(riffy_proxy_request_duration_seconds_bucket[$__rate_interval]))
)

# Average latency by endpoint
sum by (endpoint) (rate(riffy_proxy_request_duration_seconds_sum[$__rate_interval]))
  / sum by (endpoint) (rate(riffy_proxy_request_duration_seconds_count[$__rate_interval]))
```

### Upstream latency — baseline vs candidate vs control (riffy's core comparison)

```promql
# p99 latency per upstream (one series each: baseline/candidate/control)
histogram_quantile(0.99,
  sum by (le, upstream) (rate(riffy_upstream_request_duration_seconds_bucket{endpoint="$endpoint"}[$__rate_interval]))
)

# Average latency per upstream
sum by (upstream) (rate(riffy_upstream_request_duration_seconds_sum{endpoint="$endpoint"}[$__rate_interval]))
  / sum by (upstream) (rate(riffy_upstream_request_duration_seconds_count{endpoint="$endpoint"}[$__rate_interval]))

# Candidate latency regression vs baseline (avg delta, seconds) — alert when > 0
(
  sum(rate(riffy_upstream_request_duration_seconds_sum{upstream="candidate"}[$__rate_interval]))
    / sum(rate(riffy_upstream_request_duration_seconds_count{upstream="candidate"}[$__rate_interval]))
)
-
(
  sum(rate(riffy_upstream_request_duration_seconds_sum{upstream="baseline"}[$__rate_interval]))
    / sum(rate(riffy_upstream_request_duration_seconds_count{upstream="baseline"}[$__rate_interval]))
)
```

### Upstream reliability (uses the `outcome` label on the summary's `_count`)

```promql
# Upstream call rate by outcome, per upstream
sum by (upstream, outcome) (rate(riffy_upstream_request_duration_seconds_count[$__rate_interval]))

# Candidate failure ratio (most important: a failing candidate skews/poisons analysis)
sum(rate(riffy_upstream_request_duration_seconds_count{upstream="candidate", outcome="error"}[$__rate_interval]))
  / sum(rate(riffy_upstream_request_duration_seconds_count{upstream="candidate"}[$__rate_interval]))

# Same for control (control errors inflate "noise=0" → false regressions)
sum(rate(riffy_upstream_request_duration_seconds_count{upstream="control", outcome="error"}[$__rate_interval]))
  / sum(rate(riffy_upstream_request_duration_seconds_count{upstream="control"}[$__rate_interval]))
```

### Sampling & pipeline health

```promql
# Samples actually stored per second, by endpoint
sum by (endpoint) (rate(riffy_samples_stored_total[$__rate_interval]))

# Effective sample ratio (stored / proxied) — sanity-check sample_rate config
sum by (endpoint) (rate(riffy_samples_stored_total[$__rate_interval]))
  / sum by (endpoint) (rate(riffy_proxy_request_total[$__rate_interval]))

# Store lag p99 — time from request receipt to sample persisted (producer→consumer→store)
histogram_quantile(0.99,
  sum by (le) (rate(riffy_sample_store_lag_seconds_bucket[$__rate_interval]))
)

# Store lag average
rate(riffy_sample_store_lag_seconds_sum[$__rate_interval])
  / rate(riffy_sample_store_lag_seconds_count[$__rate_interval])
```

A rising **store lag** or a **stored/proxied ratio** far below the configured
`sample_rate` both indicate the consumer is falling behind and the channel is
shedding load.

## Gaps — not observable via metrics today

These have **no metric** and are only visible in logs or the read-time query API:

1. **Dropped analysis messages** (channel full, `try_send` fails in
   `forward.rs`) — only `tracing::warn!`. Backpressure is invisible to Prometheus
   except indirectly via the stored/proxied ratio. A `riffy_analysis_dropped_total`
   counter would make it directly alertable.
2. **Discarded samples** (the same-status-but-unstorable-body discard, and the
   non-JSON / over-cap baseline skip) — only `tracing::error!`/`warn!`. A
   `riffy_samples_discarded_total{reason=...}` counter would surface candidates
   returning garbage.
3. **Regression verdicts** — computed at read time in the `DiffEngine`, never
   exported. "Number of regressing endpoints/fields" lives only in the admin UI
   and `/diffs/*` API, not Prometheus.
