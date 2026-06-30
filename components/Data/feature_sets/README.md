# feature_sets — shared folder contract

This folder is the **file-exchange point for the feature-selection round** of the
MLOps config-driven flow (capability → aggregation → formula → **feature set** →
simulation). It is owned by the Data component (which owns "feature sets").

- **Writer**: `.claude/skills/feature-dashboard/serve_round.py` (the Feature
  Workbench in round mode) writes here on each Save.
- **Readers**: the next round (as its baseline) and downstream stages
  (offline simulation / training dataset builders) that consume the selected
  feature list.

The contract here aligns with the MLOps TRD **§7.3.4 `feature_set`** object.

## Files

| file | role |
|---|---|
| `round_000.json` | **Empty genesis baseline** for round 0 (0 features). Written once by the seed step; a lineage record of "before any features were chosen". |
| `current_feature_set.json` | **Rolling pointer** — the latest committed feature_set. The input for the next round and what downstream stages read. First created by the seed step (the "normal features" pre-load); atomically replaced on every save. |
| `round_<NNN>_feature_set.json` | **Immutable snapshot** of the feature_set committed in round N (zero-padded, e.g. `round_000_feature_set.json`, `round_001_feature_set.json`). Never overwritten unless the writer is run with `--force`. |
| `CHANGELOG.md` | Append-only human log: one section per round with timestamp, round number, base id, totals, and the added/removed column lists. |

> These files are produced at runtime; only this `README.md` is checked in. The
> `*.json` files and `CHANGELOG.md` are working artifacts of a run.

## Round semantics

- **Seed (first run).** `seed_feature_set.py` writes `round_000.json` (empty) and
  `current_feature_set.json` populated with the schemas' "normal" features
  (suitability == `feature`). The seed is marked `round: -1` so the first
  interactive Save commits as **round 0** → `round_000_feature_set.json`.
- **Each round.** The writer reads `current_feature_set.json`, sets
  `this_round = base.round + 1`, sets `base_feature_set` to the baseline's
  `feature_set_id`, writes `round_<this>_feature_set.json`, and rolls
  `current_feature_set.json` forward to the just-committed set.
- With **no** `current_feature_set.json` at all, the writer bootstraps **Round 0**
  from an empty baseline (`base_feature_set: null`).
- `added_features` / `removed_features` are the delta of this round's selection
  versus the baseline it loaded, computed by the writer (not the client).

## Output shape

```jsonc
{
  "feature_set_id": "feature_set_candidate_round_000",
  "base_feature_set": "feature_set_candidate_seed",   // the seed on round 0; prior round id thereafter
  "added_features":   ["...columns added this round..."],   // column-name lists
  "removed_features": ["...columns removed this round..."],
  "owner": "mle",
  "purpose": "offline_floor_simulation",
  "round": 2,
  "generated_from": "Signal Prism schemas",
  "schema_snapshot": "schemas: gminor_log_schema.md, realtime_attributed_aggregation_table_schema.md, realtime_attributed_wide_table_schema.md",
  "saved_at": "2026-06-30T05:57:20Z",
  "count": 37,
  "features": [
    {
      "column": "jgr_bid_floor",
      "schema": "realtime_attributed_wide_table_schema",
      "group": "Floor Lifecycle",
      "type": "DOUBLE",
      "semantic_type": "money_cpm",
      "suitability": "feature",
      "null_semantics": "not_observed",
      "source": "jaeger.placement_serve_results[].bid_floor"
    }
    // ...
  ]
}
```

- `features[]` is the **fully-resolved** list (same fields as the dashboard's
  Export JSON) so a downstream stage can consume it without re-resolving metadata.
- Every `column` is validated against the live schema catalog at save time, and
  its metadata is re-filled from the schema — the writer is the source of truth,
  not the browser payload.

## Producing / consuming

```bash
# First time only — seed round_000.json (empty) + current_feature_set.json (normal features):
python3 .claude/skills/feature-dashboard/seed_feature_set.py

# Run a round (reads the latest feature_set here, writes the new one back):
python3 .claude/skills/feature-dashboard/serve_round.py

# Point at this folder explicitly (default is exactly this path):
python3 .claude/skills/feature-dashboard/serve_round.py \
  --shared-dir components/Data/feature_sets

# Consume the current selection downstream:
#   read components/Data/feature_sets/current_feature_set.json -> .features[].column
```

See `.claude/skills/feature-dashboard/SKILL.md` ("Round mode") for the full CLI,
the loopback/same-origin security note, and verification steps.
