## Context

The `dashboard` capability (see `proposal.md` and `specs/dashboard/spec.md`) is the operator-facing Feature Workbench for the demo. Today `components/Dashboard/` is an empty placeholder (`.gitkeep` only) and the repo has **no build, lint, or test tooling** — per `CLAUDE.md`, tooling must be established explicitly, not assumed.

The dashboard is a *consumer*, not a producer, of two upstream contracts:

- **Data contract** — `schemas/realtime_attributed_wide_table_schema.md` (~150 columns, each carrying `type`, `source`, `semantic_type`, `null`, `feat`, `enum_ref`).
- **UX + workflow source** — `proj_trd/end_to_end_mlops_wide_table_demo.md` §7.10 (seven screens), §7.4 (fixed dimension families), §7.5 (aggregation strategy library), §7.6 (formula DSL + validation rules).

The capability scanner (TRD §7.7 Step 1), the generic aggregation runner (§7.7 Step 4), and the simulation runner (§7.9) are **separate sub-projects**. This dashboard must not re-implement them; it consumes their outputs.

```
   schema.md ──scan (sub-project A)──▶ feature_capabilities catalog ─┐
                                                                     ├─▶  DASHBOARD (this change)
   agg runner (sub-project B) ──▶ ml_shadow_feature.* primitives ────┤     reads catalog + tables,
   sim runner  (sub-project C) ──▶ ml_shadow.simulation_* results ───┘     writes only config objects
```

## Goals / Non-Goals

**Goals:**

- Realize the five surfaces in the spec (Capability Map, Aggregation Builder, Formula Studio, Simulation Lab, Lineage) as a demo-quality UI.
- Read all capability metadata from a machine-readable catalog so the UI contract tracks the data contract with no manual re-entry.
- Enforce the reviewed-workflow guardrails (fixed dimension families, no arbitrary dimensions, leakage/point-in-time validation) in the UI layer.
- Keep the dashboard runnable in isolation (fixtures) so the demo does not require the live pipeline sub-projects to exist yet.

**Non-Goals:**

- Not implementing the capability scanner, aggregation runner, or simulation runner (separate sub-projects).
- Not writing to any production system or GMinor serving.
- Not building a general BI tool or allowing arbitrary SQL / arbitrary dimensions.
- Not committing to production-grade auth, scale, or persistence.

## Decisions

### D1. Read from a data-access adapter, fixtures first

The UI talks to a single `WorkbenchDataSource` interface (list capabilities, preview aggregation cost, validate formula, list/launch/read simulation runs). Two implementations: a **fixture adapter** (static JSON snapshots checked into the repo) and a later **live adapter** (Trino/Iceberg + the `ml_shadow.*` catalog tables from §7.11).

- *Why:* the demo must run before sub-projects A/B/C are live; the interface documents exactly what the dashboard needs from them.
- *Alternatives:* wire directly to Trino (rejected — hard dependency on unbuilt sub-projects, no offline demo); parse `schema.md` markdown in the UI (rejected — the scanner already produces the catalog; parsing prose duplicates it and drifts).

### D2. Capability metadata comes from the `feature_capabilities` catalog, not the schema markdown

The dashboard consumes the catalog table/JSON that the scanner emits (TRD §7.3.1 shape), whose fields are derived 1:1 from the schema's `semantic_type` / `feat` / `null` / `enum_ref`. The `feat` value drives selectability directly: `exclude` → never shown; un-profiled → not `available`; `leak_risk` → allowed as label/target only.

- *Why:* the schema explicitly states its metadata is ingested "without a second pass"; the UI should honor that boundary.

### D3. Formula DSL validated client-side against a small AST

Implement the §7.6 DSL (`safe_div`, `cv`, `rate`, `dd_percentile`, …) as a parser → AST → validator that checks: type correctness, primitive availability for the same (dimension family, window), division safety, and the point-in-time / no-future-label rule. Label and `leak_risk` fields are rejected as direct inputs.

- *Why:* immediate feedback is the whole point of Formula Studio; the DSL is deliberately small and closed, so a client validator is tractable.
- *Alternatives:* free-text SQL with server validation (rejected by TRD §7.6 — "Do not allow arbitrary SQL in the UI").

### D4. Fixed dimension families as static config

The four families (`device_level_v1`, `non_device_context_v1`, `inventory_context_lite_v1`, `global_baseline_v1`) ship as a checked-in config object mirroring TRD §7.4. The Aggregation Builder only lets operators pick from these; anything else is blocked with a pointer to the closest family.

### D5. Aggregation Builder previews cost; it does not materialize

The Builder writes a config object (the §7.7 Step 2 YAML/JSON) and shows the compiler's estimate (rows/day, bytes/day, table names, warnings). Actual materialization is the aggregation runner's job (sub-project B).

### D6. Frontend stack — React + TypeScript + Vite (recommended, pending confirmation)

A single-page app: React + TypeScript, Vite for dev/build, a charting lib for SHAP/lift/cohort visuals. This is a *recommendation* to unblock tasks, not a locked decision — see Open Questions.

- *Why:* the visual surfaces (lineage graph, SHAP beeswarm, cohort heatmap) are interactive and data-dense; a component SPA fits, and TS matches the metadata-heavy contract.

## Risks / Trade-offs

- **Fixture drift** — fixtures diverge from the real catalog/tables. → Generate fixtures from the schema/TRD shapes and validate the fixture against the `WorkbenchDataSource` types in CI once tooling exists.
- **Client-side leakage validation is advisory** — a determined operator could bypass the UI. → The authoritative point-in-time enforcement stays in the offline dataset builder (§7.9.3); the UI check is a fast guard, not the gate.
- **Catalog shape not yet frozen** — the scanner sub-project may change `feature_capability` fields. → Isolate the mapping in the adapter so a catalog change touches one module.
- **Scope creep into BI** — self-serve UI tempts arbitrary dimensions. → Guardrails (D4) are spec requirements, not options.

## Migration Plan

Greenfield — no existing dashboard to migrate. Rollout is additive within `components/Dashboard/`:

1. Establish tooling (D6) and the `WorkbenchDataSource` interface (D1).
2. Ship fixture adapter + fixtures; build screens against it.
3. Swap in the live adapter when sub-projects A/B/C exist; no UI changes expected beyond the adapter.

Rollback: the change is isolated to `components/Dashboard/`; reverting the directory removes it with no cross-repo impact.

## Open Questions

- **Frontend stack (D6):** ~~confirm React/TS/Vite?~~ **RESOLVED — React + TypeScript + Vite.**
- **Simulation launch in-demo:** ~~trigger runs or display-only?~~ **RESOLVED — display-only from fixtures; no sim-runner contract yet, launch control stubbed/disabled.**
- **One screen or two for simulation:** the README folds Result into "Simulation Lab"; TRD splits Lab (Screen 5) from Result (Screen 6). Kept as one surface for the demo; may split later.
- **Auth / multi-user:** out of scope for the demo. No auth; registry views (§7.10.4) render owner as a display field only.
