## Why

The dashboard lets operators *create* aggregations (Aggregation Builder) and *use* aggregate features in formulas (Formula Studio), but there is no page to *browse the existing aggregate features* defined in `schemas/realtime_attributed_aggregation_table_schema.md` — the §5 metric catalog and the §3/§4 dimension families. The MLOps TRD already specs this as §7.10.4 "Feature Registry View"; it was never built as a dedicated surface. This adds it.

## What Changes

- Add a **Feature Registry** nav page that browses the aggregate-feature catalog: the two aggregation tables (`device_level_v1`, `non_device_context_v1`), the shared metric catalog (distribution families expanded to their `_sum/_count/_min/_max/_squaresum` columns, plus count metrics), and each table's dimension columns.
- Add a **generator** that parses the aggregation table schema into a fixture, so the registry stays faithful to the contract (same offline-scanner pattern as the Capability Map).
- Flag label-like / point-in-time-sensitive metrics (e.g. `settlement_price`, `auction_winner_price`) in the registry.

## Capabilities

### New Capabilities

<!-- None. -->

### Modified Capabilities

- `dashboard`: add a Feature Registry requirement (browse existing aggregate features from the aggregation table schema).

## Impact

- `components/Dashboard/scripts/genAggregateFeatures.mjs` (new), `src/fixtures/aggregateFeatures.json` (new), `src/data/types.ts` (aggregate-feature types), `src/data/WorkbenchDataSource.ts` + `fixtureAdapter.ts` (new `listAggregateFeatures`), `src/screens/FeatureRegistry.tsx` (new), `src/App.tsx` (nav + route), tests.
- No change to the schema, the fast-screen behaviour, or other surfaces.
