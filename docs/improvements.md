# Riffy — improvement backlog (sub-agent task briefs)

Each section below is a self-contained task for an autonomous agent. Items are
independent unless a dependency is noted. Pick one, implement it end-to-end,
and open a focused commit/PR.

## Ground rules (apply to every task)

- Read `docs/architecture.md` first (runtime source of truth) and `CLAUDE.md`
  (project rules). The config schema is `config.example.yaml` + `src/config/`.
- Gate of done: `make format && make lint` (zero warnings) **and** `make test`.
- No `unwrap()`/`expect()` in non-test code without a safety comment; typed
  errors via `thiserror`, `anyhow` only at app startup. All I/O async.
- Tests live in separate files (module `tests/` subdir or crate `tests/`).
- The proxy hot path must stay zero-overhead: never add analysis/IO between
  "request received" and "baseline response returned".
- **Adding a dependency requires presenting options to the user and getting
  confirmation first** (crate policy) — do not silently add crates.
- If the runtime architecture changes, update `docs/architecture.md` via the
  `update-architecture-doc` skill. If the admin read API changes, update the UI
  per the `frontend-ui` skill.

---

## 1. Per-field ignore / noise rules  (highest value; diffy parity)

**Why:** Fields like timestamps, UUIDs, request IDs, and `Date` headers differ
on every request and generate permanent false regressions. Diffy supports
ignoring such fields; riffy has no equivalent.

**Do:** Add an ignore-rule list, configurable per endpoint (and/or global), of
dot-path patterns (support a trailing wildcard, e.g. `meta.*`, `items.*.id`).
Apply it in the consumer **after** flatten (`src/compare/flatten.rs` output) so
ignored paths never reach the counters or the sample stream. Extend
`EndpointConfig` in `src/config/mod.rs` (e.g. `ignore: [String]`) and thread it
through `EndpointMatcher`/consumer.

**Acceptance:** A configured ignored path produces no field counter movement
and no sample; unit tests cover exact and wildcard matches; `config.example.yaml`
documents it.

**Gotchas:** Decide match semantics (glob vs prefix) and keep it cheap (compiled
once per endpoint, not per request). Ignored fields should still not block a
real regression on a sibling field.

---

## 2. Response header diffing  (diffy parity)

**Why:** Riffy only diffs JSON bodies. Header regressions (content-type,
cache-control, set-cookie, custom headers) are invisible.

**Do:** Optionally diff a configurable allowlist of response headers
(baseline vs candidate = raw, vs control = noise), surfaced as pseudo-paths
under a reserved prefix (e.g. `header:content-type`) so they reuse the existing
field machinery and the UI. Capture headers in `UpstreamResponse` (already
present) and add a header-diff step in the consumer.

**Acceptance:** Configured headers that differ show as fields in
`/diffs/detail`; hop-by-hop and volatile headers (Date) excluded by default;
tests cover present/absent/changed headers.

**Gotchas:** Header names are case-insensitive; multi-value headers; don't diff
every header by default (cardinality/noise) — allowlist only.

---

## 3. Explicit request sampling rate  (diffy parity)

**Why:** Riffy analyzes every request it receives; the only sampling knob is the
k8s replica ratio. Each analyzed request costs two extra upstream calls
(candidate + control). Diffy supports a sample rate.

**Do:** Add `pipeline.sample-rate` (0.0–1.0, default 1.0). In the handler,
before firing the candidate/control fan-out, sample (e.g. fast RNG) — sampled-out
requests are proxied (baseline only) with no analysis. Keep it off the hot path
cost-wise (one cheap comparison).

**Acceptance:** With `sample-rate: 0`, no analysis messages/upstream fan-out
occur; with `1.0`, behavior is unchanged; validate 0.0–1.0. Tests assert the
fan-out is skipped when sampled out.

**Gotchas:** Sampling must not change what the client receives. Decide
per-request RNG vs deterministic (hashing) — per-request RNG is fine.

---

## 4. Non-JSON body diffing fallback  (diffy parity)

**Why:** Non-JSON bodies are status-only today. Text/`application/x-www-form-
urlencoded`/plain responses can't be compared.

**Do:** When the body isn't JSON, fall back to a normalized comparison
(e.g. exact-bytes-equal → single synthetic field, or line-based diff for text).
Keep JSON as the rich path. Decide the representation in `src/compare/`.

**Acceptance:** A text endpoint with differing bodies records a diff; identical
bodies record none; JSON behavior unchanged. Tests cover text + form bodies.

**Gotchas:** Don't try to structurally diff arbitrary text — a coarse
equal/not-equal (or unified-diff snippet in the sample) is enough. Respect the
body-size concern (don't diff huge bodies; see code comment in consumer).

---

## 5. CI pipeline (GitHub Actions)

**Why:** No CI in-repo; regressions in fmt/lint/tests/build aren't caught on PRs.

**Do:** Add `.github/workflows/ci.yml`: jobs for `make lint` (fmt check + clippy
`-D warnings`), `make test`, and a Docker build (`docker build .`). Cache cargo.
Consider a separate job (or matrix) for the real-Redis tests (see task 6).

**Acceptance:** Workflow runs on PR + push to master and is green on the current
tree. No `dangerously`-style network assumptions in the sandbox.

**Gotchas:** Pin the Rust toolchain to match the Dockerfile (`1.94`). Keep the
Docker build job using the existing multi-stage Dockerfile.

---

## 6. Real-Redis integration tests

**Why:** The Redis `DiffStore` (`src/storage/redis.rs`) is only type-checked;
`XADD MAXLEN`, `HINCRBY` aggregation, `XREVRANGE` paging, and `DEL` reset are
never exercised against a real server.

**Do:** Add integration tests that run against a Redis instance (testcontainers
crate, or assume the `docker-compose.yaml` Redis and gate behind an env flag).
Exercise: append+page samples, atomic add accumulation across two writers,
reset, and the per-field `raw:`/`noise:` regrouping on read.

**Acceptance:** Tests pass against a live Redis and are skipped/ignored when none
is available (so `make test` stays green offline).

**Gotchas:** testcontainers is a new dev-dependency → confirm with the user
first. Use `.no_proxy()`-style direct connections; the dev machine has an
`http_proxy` that breaks localhost.

---

## 7. In-repo Helm chart + published image

**Why:** The `riffy-onboarding` skill describes integrating riffy into a *user's*
chart, but there's no installable riffy chart and no documented published image.

**Do:** Add `charts/riffy/` (Chart.yaml, values.yaml, templates for the riffy
Deployment, ConfigMap rendering `config.yaml`, admin Service, optional Redis/
Jaeger deps). Mirror the topology and values schema in the onboarding skill.
Document image publication (tag = chart appVersion).

**Acceptance:** `helm lint charts/riffy` and `helm template` render cleanly with
`riffy.enabled` true/false; the rendered config matches `config.example.yaml`.

**Gotchas:** Reuse the loop-avoidance label strategy from the skill; keep all
knobs values-driven. Don't bake config into the image.

---

## 8. Sample redaction / PII scrubbing + truncation

**Why:** Stored samples (`/diffs/detail`) contain real `left`/`right` response
values — potential PII — and large values bloat the stream.

**Do:** Add config for (a) redacting/hashing configured field paths before they
enter the sample stream, and (b) truncating large values to a max length. Apply
in the consumer before `append_diff`. Counters/regression detection still use
the real diff; only the *stored sample value* is scrubbed.

**Acceptance:** Configured paths are redacted in stored samples but still count
toward raw/noise; values over the limit are truncated with a marker; tests cover
both.

**Gotchas:** Don't scrub before classification (that would change counts). Keep
the redaction list per-endpoint and global.

---

## 9. Hot-path & diff-engine benchmarks (Criterion)

**Why:** `make bench` exists but there are no benchmarks. The zero-overhead
hot-path invariant and diff throughput are unguarded against regressions.

**Do:** Add Criterion benches: (a) the `compare`/`flatten` engine over
representative JSON payloads (small/large/nested), (b) the proxy forward path
latency overhead vs a passthrough. Wire into `make bench`.

**Acceptance:** `make bench` runs; benches are documented; numbers recorded in a
short note. No bench in the normal `make test` path.

**Gotchas:** Criterion is a new dev-dependency → confirm with the user first.
Bench the diff engine in isolation (no network) for stable numbers.

---

## 10. Regressions overview view + UI (incl. status mismatches)

**Why:** You currently must drill per endpoint+path. There's no top-level "what's
regressing now, worst-first" view, and status mismatches (once surfaced as a
pseudo-field) deserve prominence.

**Do:** Add a read endpoint (e.g. `GET /diffs/regressions`) that scans
aggregations, classifies each field, and returns the regressing ones ranked by
severity (relative/absolute). Add a UI panel listing them with links into the
existing detail view. Reuse `EndpointClassifiers`.

**Acceptance:** The endpoint returns ranked regressions across all endpoints;
the UI shows them; integration test covers the ranking + empty case.

**Gotchas:** Depends on the per-endpoint classifier already in `AdminState`.
If the status-mismatch pseudo-field work (bug fix) lands, include it here. Keep
the scan bounded (it reads all aggregations — fine at low endpoint cardinality,
revisit if large).

---

## 11. TLS for listeners + optional admin auth  (lower priority — auth deemed acceptable)

**Why:** Listeners are plaintext and the admin surface is unauthenticated. The
admin-auth risk was accepted *on the assumption the admin port stays internal*,
so this is optional hardening, not a blocker.

**Do:** Add optional TLS (rustls) for the proxy and/or admin listeners, config-
driven (cert/key paths). Optionally add a simple bearer/token guard on the admin
router (esp. `DELETE /diffs`). Consider mTLS to upstreams.

**Acceptance:** TLS can be enabled via config without changing default plaintext
behavior; admin token (if added) protects mutation; tests cover the guard.

**Gotchas:** Keep it opt-in and off by default. Document the assumption that the
admin port is otherwise internal-only.
