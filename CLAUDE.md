# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repo is

Signal Prism is currently a **docs/prompts repository**, not a codebase. It holds the design
(TRDs) and the machine-readable data contract for a two-part demo:

1. **Realtime attribution wide table** — move expensive long-range `event_id` joins out of the
   batch data pipeline and into the serving/logging layer, producing a directly consumable
   impression-grain wide table (`ml_shadow.realtime_attributed_event_wide`).
2. **End-to-end MLOps feature workbench** — on top of that wide table, let ML create features by
   *configuration* (capability → aggregation spec → derived-feature formula → feature set →
   offline simulation) instead of writing a new pipeline per feature.

There is no build, lint, or test tooling yet. The `components/{WideTable,MachineLearning,Dashboard,Data}/`
directories are empty placeholders (`.gitkeep` only) for future implementation. Do not invent build/run
commands — if asked to implement, scaffold within these folders and establish tooling explicitly.

## Layout

- `proj_trd/realtime_attribution_wide_table_demo.md` — TRD for the wide table: realtime flow, attribution
  key strategy, multi-layer KV storage (L0–L3 by measured delay CDF), cross-cluster POC via global
  event-id hash sampling, POC phases 0–4, go/no-go metrics.
- `proj_trd/end_to_end_mlops_wide_table_demo.md` — TRD for the MLOps demo: metadata objects, fixed
  dimension families, aggregation strategy library, formula DSL, config-driven flow, offline simulation
  modes (A–D), 7-screen UI design, week-by-week demo plan.
- `schemas/realtime_attributed_wide_table_schema.md` — the authoritative column-level contract for the
  wide table. This is the **input** to the capability scanner and Capability Map UI; it is the file to
  edit when the table's columns or metadata change.

## How the docs interlock (read before changing any one of them)

The schema is the data contract that both TRDs depend on; keep them consistent:

- The wide table is built by exploding `ex-jaeger-transaction.placement_serve_results[]` to
  **one row per `(ad_event_id, imp_id)`** (served/attempted impression), then `LEFT JOIN`ing the
  served/winning bid from `hb-transactions`. `hbn_*` null is a meaningful signal ("not HB/S2S"), not
  missing data.
- Each schema field row carries the exact metadata the MLOps TRD's `feature_capability` object
  (§7.3.1) ingests: `type`, `source`, `semantic_type`, `null`, `feat`, `enum_ref`. When adding a
  field, fill all of these so the capability scanner needs no second pass.
- The schema's **dimension table contracts** (`device_level_v1`, `non_device_context_v1`) realize the
  TRD's "fixed dimension families" (§7.4). The TRD also names `inventory_context_lite_v1` and
  `global_baseline_v1`. Dimension families are intentionally *closed* — arbitrary dimensions are
  disallowed to keep cost predictable; route changes through these contracts.
- Label / leakage flags in schema §7 enforce the formula DSL validation rules in MLOps TRD §7.6
  ("formula cannot reference future labels"). `leak_risk` / label-adjacent fields
  (`jgr_settlement_price`, `jgr_winning_bid_price`, `jgr_no_serv_reason`, ...) are targets, not
  inputs; trailing-window aggregates of them are valid features only with point-in-time correctness.

## Conventions to preserve

- **Source-prefixed column names**: `jgr_*` (from Jaeger), `hbn_*` (from hb-transactions), unprefixed
  for composite/derived keys. Overlapping concepts are deduped to a single authoritative source
  (rationale lives in schema §3.1) — the dropped copy is *not* carried as a column.
- **No raw PII as features / dimensions**: `jgr_dev_ifa`, `jgr_dev_ip`, `jgr_dev_ua` are `exclude`;
  use the hashed `jgr_lo_id`.
- **Bucket high-cardinality fields before use**: device model, app/city name, carrier, adomain
  (top-N MAP), etc. Raw `jgr_dev_model` is explicitly too high-cardinality.
- **Global event-id hash sampling** (`hash64(event_id, seed) % 1_000_000 < threshold`) is the sampling
  unit everywhere — never sample by Kubernetes cluster (delivery and HBN/TPAT can land in different
  clusters). The same function must run in writer, reader, and repair paths.
- Enum codes referenced by `enum_ref` are defined in schema §9; their source-of-truth is the upstream
  `EnumeratedList.md` files (jaeger / hb-notifications), cited per table.

## Note on language

The TRDs mix English and Chinese prose (especially the MLOps doc's intro). Match the surrounding
language and tone of the file you are editing.
