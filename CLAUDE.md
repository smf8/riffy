# CLAUDE.md — Riffy Project Guide

## Project Overview

**Riffy** is a Rust reverse proxy that implements diffy-style statistical regression detection. It forwards requests to three upstream services (primary, secondary, candidate) in parallel, always returns the primary response to the client with zero overhead, and asynchronously compares the responses to detect regressions using noise-baseline analysis.

---

## Session Start Checklist

**Before doing any work**, always:
1. Read `Plan.md` — understand the full architecture, algorithms, and data model.
2. Read `Progress.md` — identify the current phase and which tasks remain. Only work within the current phase unless explicitly told otherwise.
3. After completing any work, update `Progress.md` checkboxes and add a note under "Notes for Next Session".

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
The reverse proxy path (receive request → call primary upstream → return response) **must never block, wait, or incur overhead from analysis work**. Specifically:
- The client response is sent immediately after the primary upstream responds.
- Candidate and secondary upstream calls are fired as background `tokio::spawn` tasks.
- Analysis, diffing, and Redis writes happen asynchronously via an mpsc channel — never on the proxy hot path.

### Response Rule
The client **always** receives the primary upstream response. There is no configurable response mode.

### Side-Effect Safety
By default, mutating HTTP methods (POST, PUT, PATCH, DELETE) are **blocked** from being forwarded to candidate/secondary. This is controlled by `proxy.allow-http-side-effects` in config.

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
- All code is compiled with `-D warnings` and `-D clippy::all`. Zero warnings are allowed.
- Run `make format` before `make lint`.

---

## Module & File Conventions

- **Trait definitions always live in `mod.rs`** of their module.
- Implementations live in separate files within the module directory.
- Unit tests **must be in a separate file** — never inline in the same file as the implementation. Place them in a `tests/` subdirectory of the module or in `tests/` at the crate root.
- Only add inline comments where logic is non-obvious. Do not add doc comments to everything — only where they add real value.

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

All commit messages must follow this format:
```
This commit will <description>
```
Examples:
- `This commit will add the diff engine for JSON response comparison`
- `This commit will wire the Redis pipeline consumer task`

---

## Testing Strategy

- Write **unit tests** for all pure logic (diff engine, flatten, endpoint matching, analysis calculations).
- Write **integration tests** for the proxy handler and pipeline consumer.
- Tests live in separate files — never co-located with implementation code.
- Use `make test` to run the full suite.

---

## Implementation Phases Reference

Work is organized into phases defined in `Progress.md`. Always check the current phase before starting:

| Phase | Scope |
|-------|-------|
| 1 | Skeleton + Proxy ✅ |
| 2 | Diff Engine |
| 3 | Analysis Pipeline |
| 4 | Redis Output |
| 5 | Observability + Hardening |

**Do not implement work from a future phase** unless explicitly instructed.
