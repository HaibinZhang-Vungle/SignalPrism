## Why

The project's purpose is a **cheap, distribution-based screen that runs *before* expensive model training** — weak feature candidates are filtered on visible evidence instead of burning full retrain cycles ("slow loop" → "fast screen"). The shipped dashboard built the *downstream* half well (aggregate → retrain → validate lift) but let users jump straight there, with no cheap screen in between — i.e. it enabled the exact slow loop the project exists to avoid. This change adds the fast-screen front half and makes the model pipeline reachable only after candidates already look plausible.

## What Changes

- Add a **Residual Diagnostics** surface: when the model stalls, surface high-error pockets and the wide-table field families worth searching for each pocket (fast-screen steps 1–2).
- Add a **Distribution Screen** surface that ranks raw fields on distribution evidence — coverage, missingness, bucket concentration, KL/PSI-style drift, and subgroup separation against the selected residual pocket (step 3).
- Add a **promotion gate**: only fields with strong distribution evidence can be promoted; weak/low-coverage fields are blocked from aggregation (step 4).
- **BREAKING (workflow):** the Aggregation Builder now only offers *promoted* capabilities; nothing reaches aggregation/simulation until it passes the screen. Reframe nav so the fast screen leads and Aggregation/Simulation are explicitly downstream (steps 5–7).
- Extend the data contract with per-field distribution stats, residual pockets, and field families; extend fixtures accordingly.

## Capabilities

### New Capabilities

<!-- None — all requirements extend the existing `dashboard` capability. -->

### Modified Capabilities

- `dashboard`: add residual-diagnostics, distribution-screen, and promotion-gate requirements; modify the Aggregation Builder requirement so only screened/promoted fields are selectable.

## Impact

- `components/Dashboard/` — new screens (`ResidualDiagnostics`, `DistributionScreen`), extended `WorkbenchDataSource` + types + fixtures, reordered nav, promotion state lifted to `App`, Aggregation Builder gated.
- `openspec/specs/dashboard/spec.md` — updated on archive.
- No change to the wide-table schema or the two TRDs; the distribution stats are demo-level profiling outputs the scanner would produce.
