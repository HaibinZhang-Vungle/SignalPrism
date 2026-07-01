# Design: Realtime Attributed Hourly Aggregation Pipeline

- **Date:** 2026-07-01
- **Status:** Approved (brainstorming) — pending spec review
- **Authoritative contract:** `schemas/realtime_attributed_aggregation_table_schema.md`
- **Reference codebase:** `/Users/twang/Projects/lena` (Scala 2.12 / Spark 3.5)
- **Target location:** `components/Data/`
- **Builds on:** `docs/superpowers/specs/2026-06-30-realtime-attributed-wide-table-pipeline-design.md`

---

## 1. Goal

Build the hourly aggregation layer defined by
`schemas/realtime_attributed_aggregation_table_schema.md`: two reviewed-dimension-family
tables materialized from `ml_shadow.realtime_attributed_event_wide` (now populated with
real, unsampled data).

| output table | dimension_family | grain key |
|---|---|---|
| `ml_shadow_feature.realtime_attributed_device_level_hly` | `device_level_v1` | `device_id` (`jgr_lo_id`) |
| `ml_shadow_feature.realtime_attributed_non_device_context_hly` | `non_device_context_v1` | `context_dim_id` |

Both are **hourly** grain, sharing one metric catalog (§5 of the contract). This realizes
MLOps TRD §7.5/§7.7 and demo-plan **Week 2** ("config compiler + first aggregations;
materialize one non-device and one device-level aggregation"). It is the input to the
Formula Studio / feature-set / simulation stages (later rounds), and the GMinor join
contract in `gminor_log_schema.md`.

## 2. Key decisions (from brainstorming)

| Decision | Choice | Rationale |
|---|---|---|
| Build altitude | **Concrete jobs, contract-driven** — generated from the schema-md via the same codegen framework the wide table uses | Fastest path to real aggregate data; mirrors what already works. Not a runtime config engine (TRD §7.7 generic runner is deferred). |
| Round scope | **Both hourly tables only** | Matches the contract 1:1. `1d`/`7d` rollups the Formula-DSL features read are a follow-up round. |
| Job count | **One parametrized Spark job**, selected by `dimension_family` | Both tables share the entire metric SQL; only the GROUP BY dim list + surrogate key + output table differ. DRY, and closest to the TRD's "no feature-specific code path" spirit. |
| Metric coverage | **Full contract; null-fill the gaps** (same precedent as the wide table) | DDL carries every metric-catalog column; the job computes what is sourceable; `appendToIcebergTable` null-fills absent columns. See §6 classification. |
| Predicate-dependent metrics | **Emit `NULL` now, flagged in `aggregation_version`** — do not guess the modulo predicate | The base column is present but the exact modulo filter (VX/non-ACC/Moloco/mediation) is unreviewed. Guessing would produce wrong-but-plausible values. |
| Top-N bucketing | **Deferred** — normalize + null→`__unknown__` now; `_bucket` columns pass through normalized values | True global top-N needs a frequency/reference table (its own mini-build). On sampled data cardinality stays bounded; `_bucket` cols exist in DDL so adding real top-N later is non-breaking. |
| Sampling | Reuse deterministic event-id-hash `in_user_sample` UDF | Same cohort unit as the wide table; consistent with the whole pipeline. |
| Artifacts | Full set **including data-cd backfill YAMLs** | The wide table is now run via `lena/cd/lena-test/` backfill YAMLs; aggregation follows suit. |

## 3. Architecture

```
[input, populated] ml_shadow.realtime_attributed_event_wide
        |
        |  codegen (new: agg_schema_catalog.py + agg_generate.py)
        v
  resources/  sql/*.template (DDL, full contract)
              agg_specs/{device_level_v1,non_device_context_v1}.json  (dims + surrogate recipe)
              agg_specs/metric_catalog.json                            (per-metric kind + expr + computable)
        |
        |  one Spark job, dimension_family arg
        v
  com.vungle.signalprism.data.realtime_attributed_aggregation.SparkMain
        |
        +--> ml_shadow_feature.realtime_attributed_device_level_hly
        +--> ml_shadow_feature.realtime_attributed_non_device_context_hly
        |
        |  data-cd backfill YAMLs (2 invocations)
        v
  lena/cd/lena-test/stage-signal-prism-agg-{device-level,non-device-context}-backfill.yaml
```

## 4. Component detail

### 4.1 `codegen/agg_schema_catalog.py` (new parser)

The existing `schema_catalog.parse_catalog` keys off a fixed 7-column header
(`column|type|source|semantic_type|null|feat|description`) that the aggregation md does
**not** use, so aggregation gets its own parser. It parses the five table shapes:

- **§1 shared columns** — `column|type|role|description` → time (`event_time`), partition
  (`ingest_time`, `hashid`), audit (`source_event_count`, `first/last_source_event_time`,
  `aggregation_version`).
- **§3 `device_level_v1` dims** — `column|type|source / derivation|role|notes`.
- **§4 `non_device_context_v1` dims** — `column|type|source / derivation|notes`.
- **§5.2 distribution families** — `family|generated columns|source / derivation|notes` →
  each family expands to five columns `_{sum,count,min,max,squaresum}`.
- **§5.3 count metrics** — `column|type|source / derivation|notes` (materialized as named).

For each metric it derives:
- `base_expr` — the wide-table column/expression from the "source / derivation" cell.
- `kind` ∈ {`computed`, `null_absent_source`, `null_predicate_dependent`} (see §6).

For each dimension it derives a normalization directive from the "source / derivation" cell:
`normalize` (lowercase/trim), `parse_major` (version), `bucket` (deferred → pass-through
normalized), or `passthrough`, plus the null→`__unknown__` rule.

### 4.2 `codegen/agg_generate.py` (new generators)

Emits into `components/Data/src/main/resources`:

- **DDL templates** `sql/realtime_attributed_device_level_hly.template` and
  `..._non_device_context_hly.template` — full contract: shared cols + family dims + every
  expanded metric column. Iceberg, `PARTITIONED BY (hours(event_time), ingest_time, hashid)`
  (modulo precedent, contract §1). Placeholders `?table?` / `?location?` like the wide table.
- **`agg_specs/device_level_v1.json`, `agg_specs/non_device_context_v1.json`** — ordered
  dimension list, per-dim normalization directive, surrogate-key recipe (`device_dim_id` =
  `sha256(device_id)`; `context_dim_id` = `sha256(concat_ws('|', <dims in table order>))`),
  `hashid` recipe, and the family's drop rule (device_level drops null `jgr_lo_id`).
- **`agg_specs/metric_catalog.json`** — per-metric `{name, kind, base_expr, columns[]}` so the
  Scala job builds the metric SELECT without hardcoding the catalog.

Wire into `generate.py`'s `__main__` (or a sibling entrypoint) so one run regenerates
wide-table + aggregation artifacts.

### 4.3 `data/realtime_attributed_aggregation/SparkMain.scala` (new, one job)

`object SparkMain extends BoilerplateSparkMain`, parametrized by `dimension_family`.

- **`requiredArgs`:** `spark.app.env`, `spark.app.batch_jobs.db.url`, `<NS>.dimension_family`,
  `<NS>.input.tableName`, `<NS>.output.tableName`.
- **`defaultArgs`:** `<NS>.sample_rate` (default `1.0`), `<NS>.aggregation_version`,
  `<NS>.test.create.table=false`, `<NS>.output.s3Dir`, standard Spark/serializer defaults.
- **`run`:** register UDFs (`in_user_sample`, `format_country`, `normalize_device_id`, …);
  `assertTestTableName`; optional test-create from the family's DDL template; resolve `till`
  (`getTillTime` in backfill; watermark progress in incremental); iterate
  `hourlyPeriodsForCurrentBatchMinute`; `process(start, till)` per hour.
- **`process`:** read the family spec + metric catalog from resources, then build one SQL:
  - **Dimension projection:** null→`__unknown__`, normalization/`parse_major`, then the
    surrogate key + `hashid` (`sha256(...)`); `device_level` filters `jgr_lo_id IS NOT NULL`.
  - **Metric aggregation:**
    - distribution family `X` → `SUM(X) X_sum, COUNT(X) X_count, MIN(X) X_min, MAX(X) X_max,
      SUM(X*X) X_squaresum`.
    - count metric with a known predicate (e.g. `delivery_count` = `SUM(CASE WHEN
      jgr_no_serv_reason = 0 THEN 1 ELSE 0 END)`).
    - `kind != computed` metrics → **not emitted** (omitted from the SELECT); the shared
      `appendToIcebergTable` null-fills them against the full-contract DDL.
  - **Audit:** `COUNT(*) source_event_count`, `MIN/MAX(source_event_time)`,
    `'<aggregation_version>' aggregation_version` (version string encodes which predicate/
    absent metrics are null this round).
  - `WHERE source_event_time >= start AND source_event_time < till`;
    `GROUP BY <dims>, hours(source_event_time)` → `event_time`.
  - Write via `appendToIcebergTable(outputTable, agg)`.

### 4.4 data-cd YAMLs (`lena/cd/lena-test/`)

Two `ScheduledSparkApplication` backfills cloned from the wide-table backfills, both with
`mainClass = com.vungle.signalprism.data.realtime_attributed_aggregation.SparkMain`,
differing only in `dimension_family`, `output.tableName`, and pod-name prefix:

- `stage-signal-prism-agg-device-level-backfill.yaml`
- `stage-signal-prism-agg-non-device-context-backfill.yaml`

`input.tableName = hive_stg.ml_shadow.realtime_attributed_event_wide`; `till` set to the
already-attributed window; `sample_rate` `1.0` for the full run.

### 4.5 Tests

- **Codegen unit tests** (pytest, matching `components/Data/codegen/test_*`): parse the agg
  md; assert dimension lists per family; assert each distribution family expands to exactly
  five `_{sum,count,min,max,squaresum}` columns; assert metric classification (§6); assert DDL
  contains the full contract and the correct `PARTITIONED BY`.
- **Scala lint test** `test_lint_agg_job.py` in the existing style (structural checks on the
  generated/handwritten SparkMain: required args, no raw PII columns, uses `appendToIcebergTable`).

## 5. Artifact layout (`components/Data/`)

```
codegen/
  agg_schema_catalog.py            # new parser for the aggregation md
  agg_generate.py                  # new DDL + agg_specs generators
  test_agg_schema_catalog.py       # new
  test_generate_agg_ddl.py         # new
  test_lint_agg_job.py             # new
src/main/resources/
  sql/realtime_attributed_device_level_hly.template          # new
  sql/realtime_attributed_non_device_context_hly.template    # new
  agg_specs/device_level_v1.json                             # new
  agg_specs/non_device_context_v1.json                       # new
  agg_specs/metric_catalog.json                              # new
src/main/scala/com/vungle/signalprism/data/
  realtime_attributed_aggregation/SparkMain.scala            # new (one parametrized job)
ddl/
  realtime_attributed_device_level_hly.sql                   # new (rendered contract, for reference)
  realtime_attributed_non_device_context_hly.sql             # new
```
Plus, in `/Users/twang/Projects/lena/cd/lena-test/`: the two backfill YAMLs (§4.4).

## 6. Metric classification (this round)

| kind | metrics | handling |
|---|---|---|
| **computed** | dist families `min_bid_to_win`, `second_place_price`, `edsp_highest_price`, `mediation_floor`, `bid_price`, `settlement_price`(+`_won`/`_loss` via won/loss filter), `auction_winner_price`, `bid_price_acc`, `unshaded_bid_price`; count `delivery_count` | computed from wide-table columns |
| **null — absent source** | `mediation_{loss,win,bill}_count`, `event_start_count` (TPAT), `bid_price_all`, `net_revenue`, `adv_spend`, `pub_revenue` | not emitted → null-filled by writer |
| **null — predicate-dependent (flagged)** | `vx_min_bid_to_win`, `edsp_highest_price_non_acc`, `bid_price_moloco`, `bid_count_moloco/acc`, `min_bid_to_win_med`, `mediation_floor_txn`, `sp_at_mediation_floor_count`, `no_bid_count`, `bid_count` | not emitted → null-filled; recorded in `aggregation_version` |

`settlement_price_won`/`_loss` are computed **only if** a won/loss flag is derivable from
wide-table columns (`jgr_no_serv_reason` / settlement status); otherwise they drop to
predicate-dependent-null. Confirmed during implementation against the wide-table columns.

## 7. Out of scope (this round)

- `1d` / `7d` rollup tables and the derived-feature Formula DSL (later round).
- The runtime generic aggregation config engine (`GenericAggregationRunner`, TRD §7.7).
- True global top-N bucketing (frequency/reference table).
- Metrics needing an external source (mediation counts, TPAT, all-bids) or a reviewed formula
  (net_revenue, adv_spend, pub_revenue), and unreviewed modulo predicates.
- GMinor self-served log ingestion and the point-in-time simulation dataset builder.

## 8. Verified facts / open items

- **Verified:** `appendToIcebergTable` → `matchDFSchemaWithTargetTable` (lena
  `Boilerplate.scala:1902-1911`) null-fills target-only columns — the wide table relies on this
  exact behavior, and this design reuses it for absent/predicate-dependent metrics.
- **Verified:** the existing codegen parser is header-specific and will not parse the
  aggregation md tables → a new parser is required (not a reuse).
- **Open (resolve in implementation):** exact wide-table column names/types backing each
  distribution family (confirm against `schemas/realtime_attributed_wide_table_schema.md`);
  whether a won/loss flag exists for `settlement_price_won/_loss`; the `net_revenue` inputs
  present in the wide table (kept null this round regardless).
