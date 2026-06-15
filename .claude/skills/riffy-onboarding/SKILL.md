---
name: riffy-onboarding
description: Use to onboard an endpoint to riffy or integrate riffy into an existing Helm chart. Interactively interviews the user about the endpoint (honoring riffy's rules), proposes the riffy configuration to run it effectively, and wires riffy into the chart — a deployment that shares the app's Service-selector labels plus a loop-free upstream Service — all driven by values files.
---

# Onboarding an endpoint / integrating riffy via Helm

Two linked workflows, run **interactively** (ask, wait, confirm — never dump the
whole plan first):

- **A. Interview → propose config.** Question the user about an endpoint and
  riffy's rules, then propose the `riffy` config that runs it *effectively*
  (meaningful diffs, no unsafe replays).
- **B. Helm integration.** Wire riffy into an existing chart so a slice of live
  traffic flows through it, with **no routing loop**, and everything is managed
  by editing values files.

Use `AskUserQuestion` for the consequential decisions (side-effects, upstream
mapping, storage backend, sampling). Drive the interview one or a few questions
at a time. The riffy config schema is `config.example.yaml` / `src/config` —
keep proposals in sync with it.

## Riffy rules to weave into every proposal (non-negotiable)

1. **The client always gets the baseline response**, and the hot path is
   zero-overhead — candidate/control are called in the background. So inserting
   riffy must not change what clients see (when riffy is healthy).
2. **Mutating methods (POST/PUT/PATCH/DELETE) are blocked** from reaching
   candidate/control unless `proxy.allow-http-side-effects=true`. riffy replays
   each sampled request to candidate **and** control, so enabling it on a
   side-effecting endpoint causes **duplicate writes**. Default to **off**;
   only enable for endpoints that are safe to replay (idempotent, read-only).
3. **Only JSON response bodies are diffed** (gzip/deflate/br/zstd are decoded
   first). Non-JSON endpoints record status diffs only.
4. **Endpoints are `:param` templates** (e.g. `/api/v1/users/:id`); query
   strings are stripped. Each endpoint has its own thresholds (diffy defaults:
   relative 20 %, absolute 0.03 %).
5. **baseline / candidate / control:** baseline = the served, trusted version;
   candidate = the new code under test; control = a second replica of baseline
   (the noise floor). raw = baseline-vs-candidate, noise = baseline-vs-control;
   a field regresses when raw exceeds noise past the thresholds. **Diffs are
   only meaningful when candidate is a genuinely different deployment from
   baseline.**
6. **Storage:** Redis aggregates stats across riffy replicas; in-memory is
   per-pod (each replica reports only its own slice). Multi-replica riffy in
   k8s ⇒ Redis for a combined view.

## Phase A — interview

Ask these, mapping each answer to config. Lead with the safety-critical ones.

1. **Endpoint pattern(s)** — exact path templates with `:params`. → `endpoints[].pattern`.
2. **Methods + side-effects** — which HTTP methods, and are they safe to replay
   to two extra backends (idempotent, no writes/emails/charges)? → if any are
   mutating and the user still wants them analyzed, this forces
   `allow-http-side-effects=true`; otherwise keep it off and tell them mutating
   requests will be blocked from analysis. **Recommend read-only paths first.**
3. **Upstream mapping** — what in the cluster is baseline, candidate, control?
   (Service/Deployment names, versions.) If only one version exists, explain
   that control = a baseline replica and candidate must be a *new* version for
   diffs to mean anything; offer to point all three at the same upstream just to
   validate wiring (expect zero raw diff).
4. **Response shape** — JSON? content-encoding? (sets expectations; non-JSON →
   status-only diffs).
5. **Thresholds** — keep diffy defaults unless a field is known-noisy; per
   endpoint. → `endpoints[].threshold`.
6. **Sampling** — what fraction of live traffic should flow through riffy? In
   the shared-selector topology this is `riffyReplicas / (riffyReplicas +
   appReplicas)`. → riffy `replicas`.
7. **Persistence** — combined cross-replica stats (Redis) or per-pod
   (in-memory)? → `storage.backend`.
8. **Ops** — timeout, `stream-cap`, `channel-capacity`, OTLP/Jaeger endpoint
   (sane defaults: 30 s / 10000 / 1024 / off).

Then **propose** the `riffy` config and confirm with `AskUserQuestion` before
touching the chart — especially side-effects and the upstream mapping.

## Phase B — Helm topology (read the chart first)

Before writing anything, **inspect the target chart**: `_helpers.tpl` (the
`selectorLabels`/`labels` helpers), the app Deployment (its
`spec.selector.matchLabels`, pod labels, containerPort) and the app Service
(its selector + `port`/`targetPort`). Reuse the chart's existing selector labels
**verbatim** — do not invent new ones for the main Service.

Let `L` = the app Service's selector labels and `P` = the app's container/target
port. The injection works like this:

```
            ┌──────────── Service: <app> (existing, selector L) ──────────────┐
 client ───▶│   load-balances over ALL pods with labels L (app + riffy)        │
            └─────────────┬─────────────────────────────────┬──────────────────┘
                          │ (1 − f)                          │ f = sampled fraction
                          ▼                                  ▼
                  ┌────────────────┐                ┌────────────────────┐
                  │   app pods     │                │   riffy pods       │
                  │ L + role=app   │                │ L + role=proxy     │
                  │ listen :P      │                │ proxy listens :P   │
                  └──────▲─────────┘                └─────────┬──────────┘
                         │                                    │ baseline/control/candidate
                         │  ┌── Service: <app>-riffy-upstream ─┘
                         └──┤  selector  L + role=app  → ONLY app pods, never riffy
                            └──────────────────────────────  ⇒ no loop
```

**Why no loop:** riffy pods carry `L`, so the *main* Service routes a fraction
of client traffic to them. Riffy forwards baseline/control/candidate to the new
**upstream Service**, whose selector adds `role=app`, so it resolves to app pods
only — riffy (`role=proxy`) is excluded. Traffic that bypasses riffy still hits
the app directly, and riffy returns the baseline response, so clients see the
same thing either way.

### Templates to add / modify (gate all on `.Values.riffy.enabled`)

- **(modify) app Deployment** — add one pod-template label `role: app` (use a
  namespaced key like `riffy.io/role: app`). This is the *only* change to the
  existing app; it lets the upstream Service select app-only. Do **not** change
  the app Service or its selector.
- **(new) `templates/riffy/deployment.yaml`** — riffy Deployment. Pod labels =
  `L` **plus** `riffy.io/role: proxy` (it must carry all of `L` so the main
  Service includes it). `server.proxy-port = P` (so the main Service's
  `targetPort` reaches it); admin port separate. Mount the ConfigMap and run
  `riffy --config /etc/riffy/config.yaml`. Readiness/liveness probe →
  `GET /healthz` on the admin port (an unready riffy is pulled from the main
  Service, so traffic falls back to the app — safe rollout).
- **(new) `templates/riffy/upstream-service.yaml`** — the loop-free Service
  `<app>-riffy-upstream`, selector `L + riffy.io/role: app`, port `P`. This is
  what riffy's `upstream.baseline`/`control`/`candidate` point at (in-cluster
  DNS, e.g. `http://<app>-riffy-upstream.<ns>.svc.cluster.local:P`). When a
  separate candidate version exists, `candidate` points at *its* Service.
- **(new) `templates/riffy/configmap.yaml`** — renders riffy's `config.yaml`
  from `.Values.riffy` (the whole config surface; see schema below).
- **(new, optional) `templates/riffy/admin-service.yaml`** — exposes the admin
  port for the UI (`/`) and metrics (`/metrics`); add Prometheus scrape
  annotations or a ServiceMonitor if the chart uses one.

### values schema (everything is managed here)

```yaml
riffy:
  enabled: false
  image: { repository: ghcr.io/snapp/riffy, tag: "" }   # tag defaults to .Chart.AppVersion
  replicas: 1                 # sampling fraction = replicas / (replicas + <app>.replicas)
  proxyPort: 8080             # MUST equal the app Service targetPort (P)
  adminPort: 7678
  allowHttpSideEffects: false # keep false unless endpoints are safe to replay
  upstream:                   # in-cluster DNS; default baseline/control → the upstream Service
    baseline: ""              # default: http://<app>-riffy-upstream:P
    control: ""               # default: same as baseline (noise floor)
    candidate: ""             # point at the NEW version's Service for real diffs
    timeout: 30s
  endpoints:
    - pattern: "/api/v1/users/:id"
      threshold: { relative: 20.0, absolute: 0.03 }
  storage:
    backend: redis            # redis | in-memory ; redis for cross-replica stats
    redisUri: "redis://riffy-redis:6379"
    aggregationInterval: 1s
    streamCap: 10000
  pipeline: { channelCapacity: 1024 }
  logging:
    level: info
    otlp: { enabled: false, endpoint: "http://jaeger-collector:4318" }
  metrics: { enabled: true }
  resources: {}
```

Operators change riffy's behavior **only** by editing values + `helm upgrade`:
enable/disable, scale `replicas` to dial sampling, add/edit `endpoints` and
thresholds, repoint `upstream.candidate` at a new version, flip
`allowHttpSideEffects`, switch storage. No template edits for day-to-day use.

## Caveats to surface to the user

- **Selector overlap.** riffy pods share `L` with the app Deployment's selector,
  so the selectors overlap — Kubernetes discourages this. Pods are owned by
  their own ReplicaSets (ownerRefs), so neither controller adopts the other's
  pods; the `riffy.io/role` label keeps the upstream Service unambiguous. Flag
  it; if the chart's Service selector is broader than its Deployment selector,
  prefer sharing only the Service-selector subset.
- **Side-effects are the main footgun.** A sampled fraction of requests is
  replayed to candidate + control. Never enable `allowHttpSideEffects` for
  mutating endpoints in a shared environment.
- **Port match.** The main Service `targetPort` must equal `riffy.proxyPort`,
  and riffy must forward on the upstream Service's port.
- **Meaningful diffs need a real candidate.** Pointing all three upstreams at
  the same Service validates wiring but produces no raw diff.

## Verify before finishing

- `helm lint` and `helm template . --set riffy.enabled=true` (and `=false`) —
  render cleanly both ways; confirm the upstream Service selector includes
  `role: app` and riffy pod labels include all of `L` plus `role: proxy`.
- Re-read the rendered riffy `config.yaml` against `config.example.yaml` so the
  keys/shape match the binary's expectations.
