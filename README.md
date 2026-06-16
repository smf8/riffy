# Riffy

A Rust reverse proxy for diffy-style statistical regression detection. It forwards each request to three upstreams (baseline, control, candidate) in parallel, **always returns the baseline response to the client with zero hot-path overhead**, and asynchronously compares the responses to detect regressions using noise-baseline analysis. Detected diffs are queryable via a JSON API and a minimal embedded admin dashboard.

## Run

```bash
docker compose up -d                       # start Redis (+ optional Jaeger)
cp config.example.yaml config.yaml         # edit upstreams/endpoints as needed
make run                                    # or: cargo run -- --config config.yaml
```

- Proxy: `:7677` · Admin UI + diff API: `:7678` · Metrics: `:9090`
- Config is layered: embedded defaults → `config.yaml` (cwd) → `RIFFY__*` env vars → CLI flags.

## Develop

```bash
make build   # compile
make test    # run tests
make lint    # fmt check + clippy (zero warnings required)
make format  # format + clippy --fix
```
