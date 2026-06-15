---
name: update-architecture-doc
description: Use after any change to riffy's request flow, module wiring, pipeline stages, Redis keys/formats, metrics, or config surface — keeps docs/architecture.md (the runtime DAG) truthful. Also use when asked to update or review the architecture doc.
---

# Updating docs/architecture.md

`docs/architecture.md` is the single runtime-truth document: a Mermaid DAG of
the request/analysis flow plus tables for metrics and Redis data. It describes
what the code **does today**; when the doc and the code disagree, the doc is
wrong — fix it to follow the code.

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
   in: `src/http/router.rs` → `src/telemetry/metrics.rs` (middleware) →
   `src/http/forward.rs` → `src/upstream/client.rs` → `src/pipeline/mod.rs` →
   `src/pipeline/consumer.rs` (→ `decode.rs`, `src/endpoint/`,
   `src/analysis/`, `src/compare/`) → `src/storage/`.
3. Update the DAG nodes/edges and the tables together — a table that
   contradicts the diagram is worse than no update.
4. Verify every literal name against code before saving (names must be
   copy-pasteable):
   - metrics: `grep -rn 'metrics::\(counter\|histogram\)' src/`
   - Redis keys/fields: `grep -rn 'xadd\|hset\|stream_key\|aggregation_key_prefix' src/storage/`
   - config keys: check `src/config/mod.rs` (kebab-case serde renames) and `config.example.yaml`
   - file paths named in node labels: confirm each file still exists.
5. If a change reflects a deliberate, non-obvious design decision, you may tag
   the affected node/row inline with the next `(R#)` marker for traceability.
   These are lightweight historical markers — there is no separate changelog to
   maintain.

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
2. **The client always gets the baseline response (Q13/R3).** Response-mode
   options were explicitly removed in Phase 1 review; do not reintroduce them
   in docs or code.
3. **Upstream + diff terminology is fixed (R30):** the three upstreams are
   **baseline** (served + trusted), **candidate** (new code), **control**
   (baseline replica for the noise floor). The two diffs keep the **raw** /
   **noise** names: raw = baseline vs candidate, noise = baseline vs control.
   Don't invent synonyms or reintroduce diffy's primary/secondary.
4. **Exact names only.** Metric names, Redis key formats
   (`{app_name}:{resource}:{type}`), config keys, and threshold defaults must
   match code character-for-character; verify with grep (step 4), never from
   memory.
5. **Dependencies appear in the doc only after the user approved them** (crate
   policy). Example: body decompression was documented only after the user
   chose async-compression (R20). Never document a speculative crate choice.
6. **`(R#)` tags are historical markers.** They flag deliberate past decisions
   inline; keep existing ones stable (don't renumber or delete them), and there
   is no separate log to update.
7. **Concise over exhaustive.** The doc is a map, not a mirror: one DAG,
   short tables, a hard-invariants list. Don't paste code into it.
8. If the same change also touched code, the usual gate applies before the
   work is complete: `make format && make lint` (zero warnings) and
   `make test`.
