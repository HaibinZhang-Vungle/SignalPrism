## Context

The shipped dashboard (change `add-dashboard-feature-workbench`, archived) implemented steps 5–7 of the project's "fast screen" flow but omitted steps 1–4 — the cheap distribution screen that is the project's reason to exist. This change adds that front half on top of the existing React + TS + Vite app and the `WorkbenchDataSource` contract, without a rewrite.

## Goals / Non-Goals

**Goals:**
- Make the distribution screen the centerpiece and gate aggregation/training behind it.
- Keep the existing data-access seam: extend `WorkbenchDataSource`, don't replace it.
- Keep everything fixture-driven and deterministic (no `Math.random`, no live pipeline).

**Non-Goals:**
- Real residual computation or real KL/PSI over live data — the stats are demo fixtures the scanner would produce.
- Changing the wide-table schema or the TRDs.

## Decisions

### D1. Distribution stats live on the capability; separation is pocket-relative

Each capability carries static profiling stats (`coverage`, `bucketConcentration`, `psi`, `klDivergence`, `baseSeparation`, `family`). Subgroup separation is computed per pocket: `separation = clamp01(baseSeparation × (family ∈ pocket.proposedFamilies ? 1.25 : 0.75))`. This makes different pockets promote different fields, deterministically, with no RNG.

- *Alternative:* a full per-(field,pocket) fixture matrix — rejected as verbose and redundant for a demo.

### D2. Screen verdict is a pure function

`screenField(cap, pocket) → { stats, verdict, reasons }` with thresholds: `blocked` if coverage < 0.35; `weak` if separation < 0.35 or bucketConcentration > 0.9; else `strong`. PSI above 0.2 adds a non-blocking drift flag. Pure and unit-tested, mirroring the DSL validator pattern.

### D3. Promotion state lifts to `App`

A `Set<capabilityId>` of promoted fields lives in `App` and is threaded to the Distribution Screen (writes) and the Aggregation Builder (reads → filters). This is the mechanism that enforces "nothing reaches aggregation until it passes the screen." Only `strong` fields can be added to the set.

### D4. Nav reordered around the fast screen

`Residual Diagnostics → Distribution Screen → Capability Map → Aggregation Builder → Formula Studio → Simulation Lab → Lineage`. Capability Map is kept (it is the literal "scan wide-table raw fields" step and an existing requirement) but the two new fast-screen surfaces lead.

### D5. `WorkbenchDataSource` gains three methods

`listResidualPockets()`, `listFieldFamilies()`, and `screenFields(pocketId)` (returns ranked `ScreenedField[]`). The fixture adapter implements them from a new `residualPockets.json` plus the extended capability fixtures.

## Risks / Trade-offs

- **Demo stats can look authoritative** → label the screen as demo profiling in the UI; verdict thresholds are visible.
- **Gating could dead-end the demo** (nothing promoted → empty Aggregation Builder) → the empty state explicitly tells the operator to screen and promote first; a couple of fields are strong for the default pocket so the happy path flows.
- **Existing tests/screens must keep passing** → additive changes; the Aggregation Builder gains a promoted-set filter but keeps its existing behavior when fields are promoted.

## Migration Plan

Additive within `components/Dashboard/`. Existing surfaces keep working; the Aggregation Builder gains an upstream dependency (promoted set). No data migration.

## Open Questions

- Should Capability Map eventually fold into the Distribution Screen entirely? Deferred — kept separate for now to preserve the existing requirement.
