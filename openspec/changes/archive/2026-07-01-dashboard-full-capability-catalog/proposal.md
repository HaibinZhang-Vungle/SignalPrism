## Why

The Capability Map is meant to be the *scanned catalog of the wide table* — the schema (`schemas/realtime_attributed_wide_table_schema.md`) is its declared input (schema doc header; TRD §7.10.1 "scan wide-table raw fields"). It currently shows a 14-field hand-picked sample, which undersells that step. Presenting the full catalog makes the "scan raw fields" step truthful without faking distribution stats for every field.

## What Changes

- Add a one-time fixture **generator** that parses all field rows (~200) from the wide-table schema into `capabilities.json`, inferring domain from section headers and source-event type from column prefix.
- The Capability Map shows the **full catalog** grouped by domain, with each field's schema metadata and its role: `key`/`dim` (not a feature candidate), `exclude` (PII, shown but not selectable), `feature*`/`leak_risk` (candidate).
- Distinguish **catalog entries from screen candidates**: add `screenProfiled` so the Distribution Screen only screens the curated, distribution-profiled subset — the full catalog does not flood the screen.
- Preserve the existing curated capabilities (with distribution stats + primitives + tests) via a profiling overlay merged by wide-table column.

## Capabilities

### New Capabilities

<!-- None. -->

### Modified Capabilities

- `dashboard`: modify the Capability Map requirement to present the full schema catalog (all raw fields, role-badged), and clarify that only distribution-profiled feature candidates flow into the Distribution Screen.

## Impact

- `components/Dashboard/scripts/genCapabilities.mjs` (new), `src/fixtures/capabilities.json` (regenerated ~200), `src/fixtures/curatedProfiling.json` (new overlay), `src/data/types.ts` (add `screenProfiled`, domains `identity`/`rtb`), `src/data/fixtureAdapter.ts` (screen-candidate filter), `src/screens/CapabilityMap.tsx` (render all + role badges), tests updated.
- No change to the schema, TRDs, or the fast-screen behaviour.
