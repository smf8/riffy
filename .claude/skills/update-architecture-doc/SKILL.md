---
name: update-architecture-doc
description: Use after any change to riffy's request flow, module wiring, pipeline stages, Redis keys/formats, metrics, or config surface — keeps docs/architecture.md (the runtime DAG) truthful. Also use when asked to update or review the architecture doc.
---

# Updating docs/architecture.md

`docs/architecture.md` is the single runtime-truth document: a Mermaid DAG of
the request/analysis flow plus tables for metrics and Redis data. It describes
what the code **does today** — not what `Plan.md` planned. When they disagree,
the doc follows the code and cites the revision number (R#) from
`Progress.md`.

## When an update is required

- A node changes: handler/consumer/analyzer behavior, new or removed pipeline
  stage, module moved or renamed.
- An edge changes: what calls what, what is sync vs spawned, channel semantics.
- Any rename or addition of: metric names/labels, Redis keys or entry fields,
  config keys, threshold semantics, supported content-encodings.
- A new invariant is established or an existing one is strengthened.

## Procedure

1. Read the current `docs/architecture.md` and the diff you just made.
2. Trace the affected path in code — do not write from memory. The flow lives
   in: `src/proxy/router.rs` → `src/telemetry/metrics.rs` (middleware) →
   `src/proxy/handler.rs` → `src/proxy/upstream.rs` → `src/pipeline/mod.rs` →
   `src/pipeline/consumer.rs` (→ `decode.rs`, `src/endpoint/`,
   `src/analysis/`, `src/compare/`) → `src/redis/`.
3. Update the DAG nodes/edges and the tables together — a table that
   contradicts the diagram is worse than no update.
4. Verify every literal name against code before saving (names must be
   copy-pasteable):
   - metrics: `grep -rn 'metrics::\(counter\|histogram\)' src/`
   - Redis keys/fields: `grep -rn 'xadd\|hset\|stream_key\|aggregation_key_prefix' src/redis/`
   - config keys: check `src/config/mod.rs` (kebab-case serde renames) and `config.example.yaml`
   - file paths named in node labels: confirm each file still exists.
5. If the architecture deviated from `Plan.md`, add a numbered revision row to
   `Progress.md` first, then cite that R# in the doc.
6. Update `Progress.md` "Notes for Next Session" (session checklist applies to
   doc work too).

## Diagram conventions

- Mermaid `flowchart TD`; the graph must stay **acyclic**. Periodic work (the
  aggregation ticker) is a separate root node, never a back-edge.
- Every node label names its source file in parentheses.
- Subgraphs separate execution contexts: proxy server / hot path / background
  task / consumer task / store. Do not merge them for compactness.
- The hot path → background hand-off is the only dotted edge; keep it that way
  so the single fire-and-forget point stays visible.
- Use stadium nodes `([...])` for terminal outcomes, diamonds for decisions,
  cylinders for Redis.

## Standing rules from user feedback (do not relearn these)

1. **Hot-path purity is the #1 rule (R2).** Never draw analysis, decoding, or
   Redis I/O on the client-blocking path — if a change would require that, the
   change is wrong, not the diagram.
2. **The client always gets the primary response (Q13/R3).** Response-mode
   options were explicitly removed in Phase 1 review; do not reintroduce them
   in docs or code.
3. **Raw vs noise terminology is fixed** (diffy parity): raw = primary vs
   candidate, noise = primary vs secondary. Don't invent synonyms.
4. **Exact names only.** Metric names, Redis key formats
   (`{app_name}:{resource}:{type}`), config keys, and threshold defaults must
   match code character-for-character; verify with grep (step 4), never from
   memory.
5. **Dependencies appear in the doc only after the user approved them** (crate
   policy). Example: body decompression was documented only after the user
   chose async-compression (R20). Never document a speculative crate choice.
6. **Deviations are revisions.** The user tracks every architecture deviation
   as a numbered R# row in `Progress.md`; the doc references those numbers
   instead of re-explaining history.
7. **Concise over exhaustive.** The doc is a map, not a mirror: one DAG,
   short tables, a hard-invariants list. Don't paste code into it, and don't
   duplicate `Plan.md` content.
8. If the same change also touched code, the usual gate applies before the
   work is complete: `make format && make lint` (zero warnings) and
   `make test`.
