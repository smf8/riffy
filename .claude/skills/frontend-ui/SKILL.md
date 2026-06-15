---
name: frontend-ui
description: Use when changing the admin dashboard UI, or when a backend change affects the diff query API the dashboard consumes (response shapes/fields of /diffs/*, routes, or the underlying data model). Keeps src/http/ui/index.html correct and build-free.
---

# Updating the admin dashboard UI

Riffy's admin dashboard is a **single embedded page**, deliberately build-free:

- `src/http/ui/index.html` — the whole UI (HTML + CSS + an Alpine.js component),
  embedded via `include_str!` in `src/http/ui.rs` and served at `GET /` on the
  admin server.
- `src/http/ui/alpine.min.js` — vendored Alpine.js runtime, served at
  `/alpine.js` (referenced as `<script defer src="/alpine.js">`).
- Routes are wired in `src/http/router.rs` (`admin_router`).

There is **no node/npm, no bundler, no framework build**. Do not introduce one
without an explicit decision from the user (it would add a toolchain to the
Makefile/Dockerfile/CI). The page talks to the existing JSON API with `fetch`.

## When an update is required

Apply UI changes automatically, in the same change as the backend edit, whenever:

- The response shape or fields of `/diffs/paths` or `/diffs/detail` change
  (e.g. a renamed/added field the table or stats panel reads).
- A query route changes path or method (`/diffs/paths`, `/diffs/detail`,
  `DELETE /diffs`), or a new admin route the UI should surface is added.
- The data model behind those responses changes in a way the dashboard shows
  (raw/noise counts, thresholds/regression verdict, samples).

## Conventions

- The Alpine component is registered on `alpine:init` via
  `Alpine.data('diffs', () => ({ ... }))`; state + methods live there. Markup
  uses `x-data="diffs"`, `x-for`/`x-if`/`x-text`/`@click`, and `x-cloak`
  (hidden until Alpine loads).
- Fetch the JSON API with `fetch`; build query strings with `URL` +
  `searchParams` (never string concatenation). Surface failures in the `error`
  banner rather than throwing.
- Keep the dark, dependency-free CSS inline in the page. No external fonts/CDNs
  — the UI must work offline (Alpine is vendored, not loaded from a CDN).
- Content types are set in `src/http/ui.rs` (`text/html`, `application/javascript`).

## Verify

- `make format && make lint` (zero warnings) and `make test` — the admin router
  serving the assets is covered by `admin_serves_ui_assets` in
  `tests/query_api.rs`; extend it if you add routes.
- The UI consumes the same store the query-API integration tests exercise, so
  keep field names in `index.html` in sync with `DiffDetail`/`EndpointPaths`
  in `src/http/query.rs`.
