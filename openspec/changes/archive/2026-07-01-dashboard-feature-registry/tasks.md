## 1. Types & contract

- [x] 1.1 Add `AggregateMetric`, `AggregateTable`, `AggregateFeatureCatalog` types
- [x] 1.2 Add `listAggregateFeatures()` to `WorkbenchDataSource`

## 2. Generator & fixture

- [x] 2.1 Write `scripts/genAggregateFeatures.mjs`: parse §3/§4 dims + §5.2/§5.3 metrics → `aggregateFeatures.json`
- [x] 2.2 Add `npm run gen:aggregate-features`; generate the fixture
- [x] 2.3 Implement `listAggregateFeatures` in the fixture adapter

## 3. Feature Registry screen

- [x] 3.1 Table tabs (`device_level_v1` / `non_device_context_v1`) with purpose + primary key
- [x] 3.2 Metric catalog: cards grouped by kind; distribution shows the 5 generated columns; label-like flagged; source shown
- [x] 3.3 Dimension columns list for the selected table
- [x] 3.4 Add the Feature Registry nav item + route (after Aggregation Builder)

## 4. Verification

- [x] 4.1 Tests: catalog shape (distribution expands to 5 cols, counts, label-like flag), screen renders metrics + dims
- [x] 4.2 Run gen + typecheck + lint + tests + build green; screenshot the registry
