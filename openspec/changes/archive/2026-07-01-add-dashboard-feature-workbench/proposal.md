## Why

`components/Dashboard/README.md` describes the Feature Workbench UI in prose, but the demo has no testable, requirement-level contract for it. Promoting that README into an OpenSpec capability gives the operator-facing surface a spec that scenarios can be verified against and that stays consistent with the MLOps TRD (`proj_trd/end_to_end_mlops_wide_table_demo.md` §7.10) and the wide-table data contract (`schemas/realtime_attributed_wide_table_schema.md`).

## What Changes

- Introduce a `dashboard` capability that formalizes the five Feature Workbench surfaces the README names: Capability Map, Aggregation Builder, Formula Studio, Simulation Lab, and end-to-end lineage.
- Encode the README's guardrail — no arbitrary dimension combinations — as a normative requirement backed by the TRD's fixed dimension families (§7.4).
- Bind each surface to the metadata it consumes from the wide-table schema (`semantic_type`, `feat`, `null`, `enum_ref`, label/`leak_risk` flags) so the UI contract matches the data contract.
- No implementation, build tooling, or framework choice is decided here; that is deferred to `design.md`.

## Capabilities

### New Capabilities

- `dashboard`: the operator-facing Feature Workbench UI — discovering wide-table capabilities, configuring aggregations, composing derived-feature formulas, launching/reviewing offline simulations, and tracing lineage, while enforcing the reviewed (non-arbitrary) demo workflow.

### Modified Capabilities

<!-- None. No existing specs in openspec/specs/. -->

## Impact

- Docs: `components/Dashboard/README.md` becomes the informal companion to `openspec/specs/dashboard/spec.md` (the authoritative spec once archived).
- Depends on the wide-table schema as its data contract and the MLOps TRD §7.10 screen designs as its UX source.
- `components/Dashboard/` remains an empty placeholder — no code is added by this change.
