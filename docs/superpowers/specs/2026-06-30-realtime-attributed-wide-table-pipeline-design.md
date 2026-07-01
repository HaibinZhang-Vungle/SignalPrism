# Design: `realtime_attributed_event_wide` Pipeline

- **Date:** 2026-06-30
- **Status:** Approved (brainstorming) — pending spec review
- **Authoritative contract:** `schemas/realtime_attributed_wide_table_schema.md`
- **Reference codebase:** `/Users/twang/Projects/lena` (Scala 2.12 / Spark 3.5)
- **Target location:** `components/Data/`

---

## 1. Goal

Build a lena-style Spark pipeline that materializes `ml_shadow.realtime_attributed_event_wide`
— the impression-grain (`(event_id, imp_id)`) wide table defined by
`schemas/realtime_attributed_wide_table_schema.md`, formed by exploding the
`ex-jaeger-transaction` Kafka topic on `placement_serve_results[]` and `LEFT JOIN`-ing the
served/winning `hb-transactions` bid.

The table has **two consumers**:

1. **MLOps feature/training source** — the Chapter 7 end-to-end demo's capability scanner
   (`proj_trd/end_to_end_mlops_wide_table_demo.md` §7.7 Step 1) and Feature Workbench read
   column-level metadata and feature values from this table.
2. **Phase-0 golden dataset** — the offline join output that the realtime attribution POC
   (`proj_trd/realtime_attribution_wide_table_demo.md` §7.9.4, §7.11) is graded against via
   the `offline_join_match_rate` gate.

This pipeline is the **offline batch join producer** of that table. The realtime KV
writer/lookup path (Phases 1–2) is a serving-layer project and is **out of scope** here.

## 2. Key decisions (from brainstorming)

| Decision | Choice | Rationale |
|---|---|---|
| Implementation form | Scala `object SparkMain extends BoilerplateSparkMain` | Same Catalyst plan as a pure-SQL job (identical performance), but inherits coba2 read, watermark progress, Iceberg merge, metrics, and `UDFUtil` UDFs for free. |
| Source | Consume the **coba2 raw landing tables** `coba2.ex_jaeger_transaction` / `coba2.hb_transactions` via `withCoba2TempViewInRange(S3base, topic, …)` | The existing `coba/ingestion2` job already lands the two Kafka topics to S3/coba2 as **full-payload** parquet (all nested fields). We begin from that coba2 output — not a new Kafka consumer, and not the slim domain tables (`edsp_deliveries` = 6 cols). This yields all ~150 fields for model training. |
| Pipeline count | **3 jobs** (2 ingestion + 1 join) | Maps onto lena's `ingestion → attribution` taxonomy; independent watermark/retry per topic; focused col_maps; cheap final join on pre-sampled, pre-projected inputs. |
| Sampling | **Deterministic global event-id hash sample** on both ingestion jobs | Cost control (raw = tens of billions rows/day, §7.6); cohort-match with the realtime shadow output for the §7.8/§7.11 comparison; reuses lena's `in_user_sample`/`sample_score` UDFs. |
| Column scope | **Content-only** — schema md exactly (~150 `jgr_`/`hbn_` cols + keys + `source_event_time`) | The §7.5 realtime operational columns (`lookup_status`, `attribution_store_layer`, cluster ids) are degenerate in an offline join and belong to the realtime producer only. |
| Artifacts | Full lena artifact set **minus CI fixtures and CD YAML** | User opted out of CI/CD for this demo. |

## 3. Architecture

```
[upstream, existing] coba/ingestion2  ── lands Kafka ex-jaeger-transaction / hb-transactions ──▶
   coba2.ex_jaeger_transaction (full payload)   coba2.hb_transactions (full payload)

coba2.ex_jaeger_transaction ──▶ [Job 1: jaeger_transaction_ingestion]
   (withCoba2TempViewInRange + sample)  explode placement_serve_results[] + placements[]
                                        + serve_result.rtbconnections[] → pick winner
                                        (winner_id = rtb_conn.id); project all jgr_* fields
                                        └──▶ ml_shadow.jaeger_transaction_wide_staging
                                             (grain: one row per (event_id, imp_id))

coba2.hb_transactions ─────────▶ [Job 2: hb_transactions_ingestion]
   (withCoba2TempViewInRange + same sample)  keep served/winning bid (row_number dedup)
                                        project all hbn_* fields
                                        └──▶ ml_shadow.hb_transactions_wide_staging
                                             (grain: served bid per (event_id, imp_id))

[Job 3: realtime_attributed_wide]
   jaeger_staging  LEFT JOIN  hb_staging
     ON event_id = h.event_id AND imp_id = h.bidrequest_imp_id
   apply §3.1 dedup decisions (jaeger wins; HB copies dropped)
   emit go/no-go metrics (attribution_hit_rate, one-to-many cardinality)
   └──▶ ml_shadow.realtime_attributed_event_wide
        (grain: one row per (event_id, imp_id) — the schema-md contract)
```

Staging table names: `ml_shadow.jaeger_transaction_wide_staging` and
`ml_shadow.hb_transactions_wide_staging` (default; final output keeps the schema-md name
`ml_shadow.realtime_attributed_event_wide`).

## 4. Component detail

### Job 1 — `jaeger_transaction_ingestion`

Modeled directly on lena's `edsp/deliveries_ingestion` (which reads the same coba2 table).

- **Reads:** `coba2.ex_jaeger_transaction` via `withCoba2TempViewInRange(S3base, "ex-jaeger-transaction",
  start, till, "_jaeger", storeS3IngestTime = true)` (full nested struct: `device`, `app`,
  `placements[]`, `placement_serve_results[]`, geo, tcf, floors…).
- **Multi-stage CTE (edsp pattern):**
  1. `served` — `explode(placement_serve_results) AS serve_result`; carry transaction-level fields.
  2. `served_placement` — `explode(placements) AS placement_` with
     `WHERE serve_result.placement_reference_id = placement_.reference_id`.
  3. `served_rtb` — `explode(serve_result.rtbconnections) AS rtb_conn`.
  4. `served_winner` — `WHERE serve_result.winner_id = rtb_conn.id` (the winning RTB; **no**
     `is_internal=FALSE` filter — we keep `jgr_rtb_is_internal` as a column).
- **Sampling gate:** `in_user_sample(sha1(serve_result.ad_event_id), sample_rate)` — arg-driven,
  default 0.0001.
- **Keys:** `event_id = format_id(serve_result.ad_event_id)`, `imp_id = serve_result.imp_id`,
  `source_event_time = timestamp`.
- **Projection:** every `jgr_*` column via `getColsMapInJson("col_maps/jaeger_transaction_wide.json")`
  mapped as `"$expr AS $target"`. `jgr_rtb_*`/`jgr_winner_account_id` map from `rtb_conn.*`.
  Transforms go through `UDFUtil` UDFs (`normalize_device_id`, `format_country`, `format_id`,
  `extract_adomain`, `mongo_id_to_timestamp`, …) — no ad-hoc logic.
- **Writes:** `mergeToIcebergTable(ml_shadow.jaeger_transaction_wide_staging, …,
  mergeKeysAllowNull = Array("imp_id"))`.

### Job 2 — `hb_transactions_ingestion`

Modeled on lena's `hbp/auctions_served_ingestion` (which reads the same coba2 table).

- **Reads:** `coba2.hb_transactions` via `withCoba2TempViewInRange(S3base, "hb-transactions", …)`.
- **Sampling gate:** identical `in_user_sample(sha1(event_id), sample_rate)` (same rate as Job 1
  so both sides cover the same cohort).
- **Served/winning bid dedup:** `row_number() OVER (PARTITION BY event_id, bidrequest_imp_id
  ORDER BY timestamp)` keep first (§2.2 step 2); losing bidders dropped (§7).
- **Projection:** every `hbn_*` column from schema md via `col_maps/hb_transactions_wide.json`.
- **Writes:** `mergeToIcebergTable(ml_shadow.hb_transactions_wide_staging, ...)`.

### Job 3 — `realtime_attributed_wide`

- **Reads:** the two staging tables.
- **Join (§2.2 step 3):**
  ```sql
  FROM jaeger_staging j
  LEFT JOIN hb_staging h
    ON j.event_id = h.event_id AND j.imp_id = h.bidrequest_imp_id
  ```
  `LEFT JOIN` from jaeger: HB cols are NULL for SDK-direct traffic — a meaningful signal, not
  missing data (§2.2).
- **Dedup decisions (§3.1):** HB copies of floors / device / geo / shading / `supply_traffic_source`
  are simply *not selected* — jaeger wins; the dropped copies are never columns.
- **Output columns:** exactly the schema-md catalog (content-only). DDL from
  `sql/realtime_attributed_event_wide.template`, `PARTITIONED BY (hours(source_event_time))`,
  sort-order on `(source_event_time, event_id)`.
- **Metrics (`reportStatsMetric`):** `attribution_hit_rate = count(hbn_bidrequest_id IS NOT NULL)
  / count(*)` (§2.3), plus go/no-go signals from §7.11: multiple-HB-per-event_id rate, row count
  by `jgr_supply_traffic_source`, null-rate sampling for payload-size proxy.
- **Watermark:** gate the join `till` on the slower of the two upstream ingestion progresses
  (pattern from `auction/notifications_attribution`).

## 5. Artifact layout (`components/Data/`)

Mirrors lena's package + resources structure, minus CI/CD:

```
components/Data/
  README.md                         # purpose, run args, dual consumer framing, golden-dataset note
  src/main/scala/com/vungle/signalprism/data/
    jaeger_transaction_ingestion/SparkMain.scala
    hb_transactions_ingestion/SparkMain.scala
    realtime_attributed_wide/SparkMain.scala
  src/main/resources/
    columns/jaeger_transaction_wide.json
    columns/hb_transactions_wide.json
    columns/realtime_attributed_event_wide.json
    col_maps/jaeger_transaction_wide.json      # source-expr -> jgr_* target
    col_maps/hb_transactions_wide.json         # source-expr -> hbn_* target
    sql/jaeger_transaction_wide_staging.template
    sql/hb_transactions_wide_staging.template
    sql/realtime_attributed_event_wide.template
```

(Package base `com.vungle.signalprism.data` is a placeholder; if these are intended to drop
straight into lena later, switch to `com.vungle.lena.<domain>`.)

## 6. Field source of truth

`schemas/realtime_attributed_wide_table_schema.md` is the single source for: column name,
physical `type` (→ DDL), `source` expression (→ col_map key), and the `jgr_`/`hbn_` prefix.
The col_maps and DDL are generated mechanically from that table. Enum columns (§8) are kept as
their raw `enum_code` values; decoding is a downstream/feature-encoding concern.

## 7. Out of scope

- Realtime KV `AttributionContext` writer + HBN/TPAT lookup path (Phases 1–2).
- §7.5 realtime operational columns (`lookup_status`, `attribution_store_layer`, cluster ids).
- The `feature_capabilities` catalog table (this table is its *input*) and all aggregation /
  derived-feature / simulation jobs (end_to_end §7.7 Steps 2–5).
- Losing-bidder grain; TPAT/HBN-as-rows grain (this table is served/winning-bid only).
- CI fixtures + `expected_output`, and CD prod/test YAML.
- Kafka ingestion of any topic beyond the two named.

## 8. Verified facts / open items

Verified live against Trino + the lena coba schema YAMLs (2026-06-30):

- **Tables confirmed:** `raw.coba2.ex_jaeger_transaction` (served) and `raw.coba2.hb_transactions`.
  Catalog `raw`, schema `coba2`, partitions `dt, hr, mn` (no `az` on the served tables). Both
  fresh (`max(dt)=2026-06-30`). HB volume ≈ 487M rows/minute (~700B/day) → event-id sampling is
  mandatory, not optional.
- **⚠️ The landed coba2 parquet is truth; both Trino and the coba YAMLs are INCOMPLETE declarations.**
  - Trino omits (but parquet/YAML have): jaeger `edsp_floor`/`direct_floor`/`acc_floor`/`bid_dsp_size`/`vxac_exp_id`, HB `bidrequest_imp_id`.
  - The coba YAMLs omit (but parquet/Trino have): jaeger `incoming_bid_request_id`, `dup_key`, `double_verify_fraud_reason`, `device.geo.ipservice`, `serve_result.pd_cl`/`pd_cpx`/`ad_podding_multiplier`; HB `bidrequest_time`.
  - `withCoba2TempViewInRange` reads the parquet via schema-on-read, so the Spark job sees the union of both — every schema-md source field resolves. The join key `j.imp_id = h.bidrequest_imp_id` (§2.2) is valid (HB YAML line 63).
  - The codegen's `check_against_coba_yaml()` is **advisory** (the YAML's incompleteness yields false positives); the hard gate is `validate()` (artifacts vs schema md). Cross-check any flagged leaf against live coba2 before treating it as a real gap.
- **Source mechanism confirmed:** `withCoba2TempViewInRange(S3base, topic, …)` reads the full
  nested coba2 payload (confirmed in `edsp/deliveries_ingestion`, same `coba2.ex_jaeger_transaction`).
  `coba/ingestion2` is the upstream lander, out of scope (already running in lena).
- **Winner predicate confirmed:** `serve_result.winner_id = rtb_conn.id` after
  `explode(serve_result.rtbconnections)`; `jgr_winner_account_id` = `rtb_conn.account_id`.
- **Helper confirmed:** `getColsMapInJson` (ordered source-expr→target map), used as `"$k AS $v"`.
- Sample rate default and `seed` to be set per demo phase (0.001% → 0.01% → 0.1%, §7.8).
