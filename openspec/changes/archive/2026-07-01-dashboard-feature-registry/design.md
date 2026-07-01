## Context

The aggregation table schema (`realtime_attributed_aggregation_table_schema.md`) documents the *existing* aggregate features: two hourly tables, a shared metric catalog (§5 — ~20 distribution families each expanding to 5 columns, plus ~12 count metrics), and per-table dimension columns (§3/§4). The dashboard has no surface to browse them. This mirrors the Capability Map's approach one level up (materialized aggregates instead of raw fields).

## Goals / Non-Goals

**Goals:** a schema-faithful, read-only Feature Registry; reuse the offline-generator pattern; no runtime markdown parsing.

**Non-Goals:** editing aggregates; changing the Aggregation Builder or Formula Studio (Formula Studio keeps its own primitive list for now); real materialization state.

## Decisions

### D1. Offline generator parses the aggregation schema

`scripts/genAggregateFeatures.mjs` parses the schema into `aggregateFeatures.json` (`npm run gen:aggregate-features`). It tracks the current section and only collects rows within the metric/dimension tables:
- §5.2 Distribution Metric Families → `{ metricId, kind: 'distribution', generatedColumns: [id+_sum/_count/_min/_max/_squaresum], source, notes, labelLike }`.
- §5.3 Count Metric Columns → `{ metricId, kind: 'count', dataType, generatedColumns: [id], source, notes }`.
- §3 device dims / §4 non-device dims → dimension column lists attached to the two tables.
- `labelLike` = notes mention "label" or "point-in-time".

The two tables' identity (tableId, dimensionFamily, purpose, primaryKey) is stable and taken from §1.

### D2. Catalog shape

`listAggregateFeatures(): { tables: AggregateTable[], metrics: AggregateMetric[] }`. Metrics are shared across tables (schema §5 generalizes them); dimensions differ per table.

### D3. Nav placement

Insert **Feature Registry** after Aggregation Builder — it shows the aggregates that the Builder produces and the Formula Studio consumes: `… Aggregation Builder → Feature Registry → Formula Studio …`.

## Risks / Trade-offs

- **Parser must skip non-catalog tables (§1, §6)** → section-scoped collection + header/separator-row filtering, same resilience as the capability generator.
- **Formula Studio primitives stay separate** → acceptable; the registry is a browse view. A later change can source Formula Studio's inputs from this catalog.

## Open Questions

- Whether to later unify Formula Studio's primitive list with this registry. Deferred.
