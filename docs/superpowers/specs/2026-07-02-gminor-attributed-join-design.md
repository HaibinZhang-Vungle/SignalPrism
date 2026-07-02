# GMinor Attributed Join — Design Spec

**Date:** 2026-07-02
**Status:** approved (brainstorming) → ready for implementation plan
**Component:** `components/Data` (new `gminor_attributed_join` SparkMain job)
**Contracts:** `schemas/gminor_log_schema.md` (§4 join contract), `schemas/realtime_attributed_aggregation_table_schema.md`, MLOps TRD `proj_trd/end_to_end_mlops_wide_table_demo.md` §7.9

---

## 1. Goal & scope

Build the offline-simulation dataset builder: attach point-in-time historical features from the
two aggregation feature tables (`device_level_v1`, `non_device_context_v1`) to event-grain GMinor
prediction samples, plus the wide-table labels/outcomes, producing one training/simulation row per
GMinor sample.

**In scope:** a runnable `gminor_attributed_join` SparkMain job (mirrors the existing 4 jobs) +
the join-contract corrections in the schema docs (as *added notes*, not rewrites).

**Out of scope:** feature/prediction blob decoding (`features`/`predictions` are carried opaque),
simulation model training (Modes A–D), and the config-driven feature-set UI.

## 2. Sources & target

| role | table | grain |
|---|---|---|
| GMinor samples | `raw.coba2.ex_gminor_logs` (coba2 landing of `ex-gminor-logs`) | event |
| bridge (keys + labels) | `hive_stg.ml_shadow.realtime_attributed_event_wide` | impression |
| device features | `hive_stg.ml_shadow.realtime_attributed_device_level_hly` | device-attr × hour |
| context features | `hive_stg.ml_shadow.realtime_attributed_non_device_context_hly` | context × hour |
| **output** | `hive_stg.ml_shadow.gminor_attributed_training` | one row / GMinor sample |

GMinor schema confirmed to match `gminor_log_schema.md` exactly; for `dt=2026-06-28 hr=00`,
`event_id`/`device_id`/`lo_id` are 100% populated (1.9B rows, 4 projects).

## 3. Architecture

```
raw.coba2.ex_gminor_logs ──(sample by event_id; parse timestamp→source_event_time)──▶ gminor_enriched
gminor_enriched ──LEFT JOIN ON event_id──▶ realtime_attributed_event_wide   (wide = BRIDGE ONLY)
   └─ derive, with the SAME agg_specs/*.json key recipe the aggregation job uses:
        device_dim_id  = sha256(concat_ws('|', <device_level_v1 dim exprs over wide cols>))
        context_dim_id = sha256(concat_ws('|', <non_device_context_v1 dim exprs over wide cols>))
   └─ carry wide labels/outcomes
gminor_keyed ──PIT LEFT JOIN ON device_dim_id──▶ device_level_hly     (agg.event_time < event_hour, latest)
gminor_keyed ──PIT LEFT JOIN ON context_dim_id─▶ non_device_context_hly (agg.event_time < event_hour, latest)
             ──▶ ml_shadow.gminor_attributed_training
```

The wide table is a **bridge**: it supplies the columns needed to compute the two surrogate keys and
the labels. Features come from the aggregate tables.

## 4. Join-key derivation (spec-driven, shared, no drift)

Both surrogate keys are built from the wide row using the **identical** expressions the aggregation
job derives from `agg_specs/device_level_v1.json` and `agg_specs/non_device_context_v1.json`
(per-dim `dimExpr` normalization + the `sha256(concat_ws('|', coalesce(CAST(dim AS STRING),
'__unknown__'), …))` surrogate recipe). Because keys are built from the same source columns with the
same code, they equal the agg tables' `device_dim_id` / `context_dim_id` **by construction** — no
alignment risk, no cross-producer mismatch.

**Implication:** the device join uses `device_dim_id` (the full device-attribute surrogate), **not**
raw `device_id`, and **not** GMinor's own `device_id`/`lo_id` (which are carried as columns only).
This realizes `gminor_log_schema.md` §4.2's note: "derive those dimensions from the wide-table
enrichment and include them in the join."

**Refactor to guarantee no drift:** extract the spec→expression logic (`dimExpr`, `keyConcatArg`,
surrogate concat) currently inline in `realtime_attributed_aggregation/SparkMain.scala` into a shared
`com.vungle.signalprism.data.agg` helper (object/trait). Both the aggregation job and the
`gminor_attributed_join` job call it, so a spec change updates both keys together.

## 5. Point-in-time semantics

- **Strict prior-hour only:** join predicate is `agg.event_time < g.event_hour` where
  `event_hour = date_trunc('hour', source_event_time)`. This is stricter than the
  `a.event_time < g.source_event_time` shown in `gminor_log_schema.md` §4.2, which would admit the
  **same** hour's aggregate (that hour can contain data at/after the event). Correcting this prevents
  label/feature leakage (§7.9.3, §5 "aggregate rows historical only").
- **Latest snapshot:** `row_number() OVER (PARTITION BY g.event_id ORDER BY agg.event_time DESC) = 1`
  per agg table (independent device / context picks).
- **Bounded scan:** a `agg.lookback.hours` param filters `agg.event_time >= event_hour - lookback`
  (default 168h) so the agg side is pruned instead of full-history scanned.
- **LEFT JOINs:** unmatched samples keep their row with null features + a boolean hit flag.

## 6. Output schema `ml_shadow.gminor_attributed_training`

One row per (sampled) GMinor event. Column groups:

1. **GMinor sample** — `event_id`, `project_name`, `experiment_id`, `project_experiment_key`,
   `source_event_time`, `event_hour`, `traffic_allocation`, `downsampling_rate`, `sample_weight`,
   `feature_schema_version`, `version`, `cloud_provider`, `device_id`, `lo_id`, `features`,
   `predictions` (blobs carried opaque).
2. **Derived keys** — `device_dim_id`, `context_dim_id`.
3. **Wide labels/outcomes** — `wide_join_hit` flag + the label/outcome columns from the wide table
   (e.g. `jgr_settlement_price`, `jgr_winning_bid_price`, `jgr_no_serv_reason`, net-revenue fields),
   prefixed `lbl_`. Exact set enumerated in the plan from the wide DDL's `leak_risk`/label fields.
4. **Device features** — all `device_level_hly` metric columns, prefixed `dl_`, plus
   `dl_agg_hit` and `dl_agg_event_time` (the matched snapshot hour).
5. **Context features** — all `non_device_context_hly` metric columns, prefixed `ndc_`, plus
   `ndc_agg_hit` and `ndc_agg_event_time`.

Iceberg, `PARTITIONED BY (hours(source_event_time))`. DDL rendered under `components/Data/ddl/`
and a matching `sql/*.template` resource (generated where a generator exists; hand-authored + linted
otherwise, following the wide-table DDL precedent).

## 7. Sampling & cohort alignment

Read GMinor with `in_user_sample(sha1(event_id), <sample_rate>)` — the **same** deterministic
event-id sampling as the wide/ingestion jobs (default `0.0001`). This makes the sampled GMinor cohort
overlap the sampled wide cohort, so `wide_join_hit` is high rather than near-zero. Raise per demo
phase in lockstep with the ingestion jobs.

## 8. Job config (mirror `realtime_attributed_aggregation`)

`NS = spark.app.signalprism.data.gminor_attributed_join`

Required: `spark.app.env`, `spark.app.batch_jobs.db.url`, `$NS.input.gminor.s3Dir`,
`$NS.input.gminor.topic`, `$NS.input.wide.tableName`, `$NS.input.device_agg.tableName`,
`$NS.input.context_agg.tableName`, `$NS.output.tableName`.
Defaults: `$NS.sample_rate=0.0001`, `$NS.agg.lookback.hours=168`, `$NS.test.create.table=false`,
plus the standard Kryo/S3/backfill knobs. Reads GMinor via `withCoba2TempViewInRange` like the
jaeger/hb ingestion jobs. Registers `UDFUtil.registerCommonUDF` (provides `normalize_device_id`,
`in_user_sample`). Writes via `appendToIcebergTable` (null-fills against the full DDL).

## 9. Contract updates (schema `.md` — **added notes only**, do not rewrite original lines)

Per the standing rule, annotate `schemas/gminor_log_schema.md` without changing existing lines:
- Add a note at §3 `effective_device_id` / §4.2 that the aggregate is now keyed on
  `normalize_device_id(jgr_dev_normalized_id)` (not `jgr_lo_id`), so the device join uses
  `device_dim_id` derived from the wide bridge — `lo_id`/`device_id` are carried, not joined on.
- Add a note at §4.2/§4.3 and §5 that the point-in-time predicate is `agg.event_time < event_hour`
  (strict prior hour), superseding the `< source_event_time` shown in the inline SQL example.

## 10. Operational requirement (point-in-time needs prior agg hours)

Because features are strictly prior-hour, a GMinor sample in hour `H` matches only agg rows from
`< H`. The current backfill only produced agg for `2026-06-28 00:00`. To see non-null features the
demo must: backfill the two agg tables for a **range** of hours (e.g. `2026-06-28 00:00–06:00`) and
run `gminor_attributed_join` for a **later** hour (e.g. `2026-06-28 07:00`), or within `lookback`.
The plan will include the multi-hour agg backfill as a prerequisite step.

## 11. Testing / validation

- **Codegen/lint** (if a generator is added): artifact-consistency check like `validate.py`; else a
  structural lint that the output DDL columns = GMinor group + keys + `lbl_`/`dl_`/`ndc_` prefixes.
- **Key-parity test:** unit-assert that the shared key helper produces, for a fixed synthetic wide
  row, the same `device_dim_id`/`context_dim_id` string as the aggregation job's spec path.
- **Point-in-time test:** on a tiny fixture, assert no matched `agg.event_time >= event_hour`.
- **Smoke:** run for a later hour against multi-hour agg; assert `dl_agg_hit`/`ndc_agg_hit` > 0 and
  row count = sampled GMinor rows.

## 12. Risks / open items

- **Wide coverage bounds features.** Only GMinor events with a wide row get keys → features. Shared
  event-id sampling keeps overlap high; `wide_join_hit` is reported as a go/no-go metric
  (`gminor_log_join_rate`, TRD §7.9).
- **Device-cohort multiplicity.** Joining on `device_dim_id` matches the exact device-attribute
  cohort; if a device's attributes vary hour to hour, prior-hour snapshots reflect that cohort. This
  is the intended grain (documented, not a bug).
- **GMinor `features`/`predictions` decoding** is deferred; carried opaque for a later mode.
