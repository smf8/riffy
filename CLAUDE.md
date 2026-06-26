# CLAUDE.md — Riffy Project Guide

## Project Overview

**Riffy** is a Rust reverse proxy that implements diffy-style statistical regression detection. It forwards requests to three upstream services (baseline, control, candidate) in parallel, always returns the baseline response to the client with zero overhead, and asynchronously compares the responses to detect regressions using noise-baseline analysis.

---

## Session Start Checklist

**Before doing any work**, read `docs/architecture.md` — it is the source of truth for the runtime architecture: the request/analysis DAG, the storage model, the query API, and the **invariants that must not regress**. Whenever you change the runtime architecture, keep that doc current via the `update-architecture-doc` skill in `.claude/skills/`.

---

## Make Commands

| Command | Purpose |
|---------|---------|
| `make build` | Compile the project |
| `make run` | Run the application locally |
| `make test` | Run all unit and integration tests |
| `make format` | Format code and auto-fix clippy lints |
| `make lint` | Check formatting and run clippy (must pass before commit) |
| `make bench` | Run benchmarks |

> **Rule:** Every set of changes **must** pass `make format && make lint` before being considered complete. There must be zero warnings.

---

## Infrastructure

- `docker-compose.yaml` spins up all external dependencies (Redis, etc.) for local development. Use `docker compose up -d` to start them.
- `Dockerfile` is for CI/CD and production image builds only — do not use it for local development.
- Use `make` commands for all local interactions with the app.

---

## Architecture Constraints

### The #1 Rule: Proxy Hot Path Must Be Zero-Overhead
The reverse proxy path (receive request → call baseline upstream → return response) **must never block, wait, or incur overhead from analysis work**. Specifically:
- The client response is sent immediately after the baseline upstream responds.
- Candidate and control upstream calls are fired as background `tokio::spawn` tasks.
- Analysis, diffing, and Redis writes happen asynchronously via an mpsc channel — never on the proxy hot path.

### Response Rule
The client **always** receives the baseline upstream response. There is no configurable response mode.

### Side-Effect Safety
By default, mutating HTTP methods (POST, PUT, PATCH, DELETE) are **blocked** from being forwarded to candidate/control. This is controlled by `proxy.allow-http-side-effects` in config.

---

## Code Style & Quality

### No `unwrap()` in Production Code
Every use of `.unwrap()` or `.expect()` in non-test code requires an explicit comment proving it is 100% safe and unavoidable. Default to `?`, proper error propagation, or returning a typed error.

### Error Handling
- Use **`thiserror`** for typed, domain-specific errors (per-module `error.rs` files).
- Use **`anyhow`** for application-level context chaining (e.g., in `main.rs` startup code).
- `AppError` in `src/error.rs` is the axum `IntoResponse` error type for the proxy handler.

### Async & I/O
- All I/O-bound work **must** be async. Use the `tokio` async versions of all crates and methods.
- Never call blocking I/O from an async context without `tokio::task::spawn_blocking`.
- Evaluate every `.clone()`: if the type is on a hot path, assess whether it is costly and consider `Arc<T>` instead.

### Lints
- All code is compiled with `-D warnings` and `-D clippy::all`. Zero warnings are allowed — there are **no exceptions**, including `dead_code` (R28).
- Dead code must be **deleted, not suppressed**: never silence it with `#[allow(dead_code)]`. Note the compiler's `dead_code` lint cannot see unused `pub` items in the lib crate (it treats them as public API), so removing a feature means hunting down its now-unused `pub` functions, enum variants, and dependencies by hand.
- Run `make format` before `make lint`.

---

## Module & File Conventions

- **Trait definitions always live in `mod.rs`** of their module.
- Implementations live in separate files within the module directory.
- Unit tests **must be in a separate file** — never inline in the same file as the implementation. Place them in a `tests/` subdirectory of the module or in `tests/` at the crate root.
- **Comment like a senior Rust engineer.** Write self-explanatory code and add a comment **only where a human would actually put one** — sparing, terse, and explaining the **WHY**, never the what. Legitimate reasons to comment: a hidden constraint, a subtle invariant, an RFC reference, a bug workaround, an `unwrap()`/`expect()` safety proof, or a design decision that would surprise a reader. If removing the comment wouldn't confuse a future reader, don't write it.
  - **Never** use `//`, `///`, or `//!` to describe what a function, struct, module, field, enum variant, or test *does*. Delete such docs outright — do not just trim them. Keep the rare surviving WHY comment to a single line.
  - This applies to **test code too**: the test name plus its assertions are the spec — do not narrate what a test sets up or checks.
  - **The one exception:** `///` on a clap `#[derive(Parser)]` struct and its fields *is* the `--help` text (functional, not descriptive) — keep it. serde/`garde` config structs get no such pass; their `///` are descriptive and must go.

---

## Front-end (Admin UI)

The admin server serves a minimal dashboard at `GET /` on `server.admin-port`: a single embedded page (`src/http/ui/index.html`) built with **Alpine.js** (vendored at `src/http/ui/alpine.min.js`, served at `/alpine.js`), baked into the binary via `include_str!`. **No build step, no node/npm** — it consumes the JSON query API (`/diffs/paths`, `/diffs/detail`, `DELETE /diffs`) directly via `fetch`.

> **Rule:** Evaluate **every** change for front-end impact and apply the required UI modifications automatically, in the same change — do not ask first. Whenever you alter the read API the dashboard depends on (the response shape/fields of `/diffs/*`, route paths/methods, or the data model behind them), update `src/http/ui/index.html` so the UI stays correct. Keep the UI build-free and embedded. See the `frontend-ui` skill in `.claude/skills/` for conventions.

---

## Redis Conventions

- **Key format:** `{app_name}:{resource}:{type}` — e.g., `riffy:diffs:stream`, `riffy:agg:hash`.
- **Storage abstraction:** Design Redis-backed types behind a trait so the implementation can be swapped for an in-memory version (useful for tests and local dev without Redis).
- **Minimize RTT:** Evaluate all possible Redis data types before choosing one. Use pipelining or batch operations (`pipeline()`, `MULTI/EXEC`) wherever multiple commands can be grouped.
- Before implementing any new Redis storage mechanism, evaluate all relevant Redis types (String, Hash, List, Set, Sorted Set, Stream, etc.) and choose the best fit.

---

## Crate & Dependency Policy

For any **medium-to-large** new piece of logic:
1. First, search for relevant open-source crates.
2. Present the options to the user with a brief trade-off summary.
3. **Wait for confirmation** before proceeding to implement it yourself or pull in the crate.

---

## Git Conventions

All commit messages must follow this format (don't include the "this commit will" in the actual commit message):
```
This commit will <commit_message>
```
Examples:
- `Add the diff engine for JSON response comparison`
- `Wire the Redis pipeline consumer task`

---

## Testing Strategy

- **Test application logic and functionality only.** Cover the things that
  encode behavior the product depends on: the diff engine, flatten, endpoint
  matching, analysis calculations, config parsing/validation, storage round-trips,
  the sample consumer, and the curl renderer.
- **Do NOT write tests for observability plumbing.** Metrics (Prometheus
  counters/histograms, label shapes, the guarded timer) and traces (span/sampler
  wiring) are not application logic — do not add unit tests for them. This extends
  to any similar cross-cutting instrumentation whose only job is to report on the
  system rather than implement its behavior.
- Write **unit tests** for pure logic and **integration tests** for the proxy
  handler and sample consumer.
- Tests live in separate files — never co-located with implementation code.
- Use `make test` to run the full suite.
