# components/Data — Realtime Attributed Wide Table Pipeline

Materializes `ml_shadow.realtime_attributed_event_wide` per
`schemas/realtime_attributed_wide_table_schema.md`. This is the **offline batch join**
producer of the table — both (a) the MLOps feature/training source for the Chapter 7 demo
and (b) the Phase-0 golden dataset the realtime attribution POC is graded against
(`proj_trd/realtime_attribution_wide_table_demo.md` §7.11 `offline_join_match_rate`).

## Source: coba2 raw landing tables

The two ingestion jobs **consume the existing coba2 output** — `coba2.ex_jaeger_transaction` and
`coba2.hb_transactions`, the full-payload S3 landings produced upstream by lena's `coba/ingestion2`
Kafka→S3 lander. We read them via `withCoba2TempViewInRange(S3base, topic, …)`. We do **not** run a
Kafka consumer and we do **not** read the slim domain tables (e.g. `edsp_deliveries`).

## Jobs (lena-style, Scala/Spark)

1. `jaeger_transaction_ingestion` — `coba2.ex_jaeger_transaction` → explode
   `placement_serve_results[]`/`placements[]`/winning `rtbconnections[]`, event-id sample,
   project all `jgr_*` → `ml_shadow.jaeger_transaction_wide_staging`. (Models `edsp/deliveries_ingestion`.)
2. `hb_transactions_ingestion` — `coba2.hb_transactions` → event-id sample, keep served/winning
   bid (`row_number`), project all `hbn_*` → `ml_shadow.hb_transactions_wide_staging`.
   (Models `hbp/auctions_served_ingestion`.)
3. `realtime_attributed_wide` — `LEFT JOIN` the two staging tables on `(event_id, imp_id)` →
   `ml_shadow.realtime_attributed_event_wide`. (Models `auction/notifications_attribution`.)

All three extend lena's `BoilerplateSparkMain` and use only `UDFUtil` UDFs.

## Resource artifacts are generated

`columns/*.json`, `col_maps/*.json`, and `sql/*.template` are generated from the schema md —
the single source of truth. Regenerate and validate:

```bash
cd codegen
python3 generate.py        # regenerate the 8 resource files
python3 validate.py        # assert artifacts match the schema md (exit 0 == OK)
python3 -m pytest -v       # run all parser/generator/lint tests
```

## Sampling

Both ingestion jobs gate on `in_user_sample(sha1(event_id), <sample_rate>)` with the **same**
rate (default `0.0001`) so both sides cover the same event-id cohort (schema §2.3, TRD §7.8).
Raise per demo phase: 0.001% → 0.01% → 0.1%.

## Out of scope

Realtime KV writer/lookup path (Phases 1–2), §7.5 operational columns, feature/aggregation/
simulation jobs, losing-bidder grain, CI fixtures, CD YAML. See the design spec:
`docs/superpowers/specs/2026-06-30-realtime-attributed-wide-table-pipeline-design.md`.

## Promotion into lena

These files are structured to drop into `/Users/twang/Projects/lena` (move `src/main/scala`
+ `src/main/resources` into lena's tree, switch package base to `com.vungle.lena.<domain>`,
confirm boilerplate helper names, then `sbt compile`).
