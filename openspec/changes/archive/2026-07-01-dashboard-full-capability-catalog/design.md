## Context

The Capability Map should reflect the whole wide table (~200 schema field rows), not a 14-field sample. But distribution stats (`bucketConcentration`, `psi`, `baseSeparation`) can't be honestly hand-authored for 200 fields, and dimensions/keys aren't feature candidates. So "full catalog" and "screen candidates" are two different sets.

## Goals / Non-Goals

**Goals:** full, schema-faithful catalog in the Capability Map; keep the fast-screen working on a curated, distribution-profiled subset; keep the dashboard consuming a catalog (not parsing markdown at runtime).

**Non-Goals:** real distribution profiling of every field; changing the schema or the screen behaviour.

## Decisions

### D1. Offline generator, not runtime parsing

`scripts/genCapabilities.mjs` parses the schema's field tables and writes `capabilities.json`. It plays the role of the scanner offline (consistent with the prior design's D2: the app consumes a catalog, it does not parse prose). Re-runnable via `npm run gen:capabilities`.

- Robust row filter: only rows whose 6th cell is a known `feat` value (`key|dim|feature|feature_after_encode|leak_risk|exclude`) are fields — this skips the dedup table, enum appendix, and dimension-family key lists automatically.
- Domain inferred from the nearest `##`/`###` section header; `source_event_type` from column prefix (`hbn_`→hbn, `jgr_tpat`→tpat, else delivery); `family` inferred from domain.
- `allowedAggregationStrategies` derived from `semantic_type`; feature-ish fields default to `non_device_context_v1`/`inventory_context_lite_v1`.

### D2. Curated profiling overlay, merged by wide-table column

`curatedProfiling.json` maps a wide-table column → the enriched entry (stable `capabilityId`, distribution stats, `screenProfiled: true`, allowed strategies/families) for the ~12 fields that have primitives/tests. The generator overlays these onto the parsed rows keyed by column, so existing `capabilityId`s (e.g. `hbn_settlement_price`) and primitive references stay valid.

### D3. `screenProfiled` separates catalog from candidates

Add `screenProfiled?: boolean`. Screen candidates = feature-ish AND `available` AND `screenProfiled`. Generated (non-curated) fields are catalogued (`available`) but `screenProfiled: false`, so they never flood the Distribution Screen.

### D4. Capability Map renders all fields with role badges

Show every field; badge `feat` (key/dim/feature/feature_after_encode/leak_risk/exclude) and profiling status. `exclude`/`dim`/`key` render as non-selectable catalog entries (no promote/trace-as-feature). This satisfies "PII not offered" while still cataloguing it.

## Risks / Trade-offs

- **Generated coverage/stats are nominal** → mark them as not distribution-profiled; the Distribution Screen only uses curated stats. UI shows profiling status honestly.
- **Parser brittleness** → the feat-cell filter is resilient to non-field tables; the generator is re-runnable and its output is validated by the fixture-drift test.
- **Catalog size (~200)** → group by domain and keep cards compact; source-event tabs already scope the view.

## Open Questions

- Long term the scanner replaces the generator; the overlay then becomes real profiling output. Out of scope here.
