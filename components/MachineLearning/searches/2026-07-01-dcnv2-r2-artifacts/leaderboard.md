# Artifact Run Leaderboard — 2026-07-01-dcnv2-r2-artifacts

**Goal**: Produce `model.pt` + `val_predictions.parquet` for Phase 1 feature gap analysis  
**HP**: T004 config from Round 1 (best trial, val_loss=1.1565e-05, -5.9% vs control)  
**Change vs Round 1**: temporal val split (by timestamp) instead of random shuffle; `val_split=0.0`  
**Data**: pipeline 102, 7-day window ending 2026-06-29, no MAX_ROWS cap  
**Cluster**: g4dn.4xlarge (T4 GPU, 16 vCPU, 64 GB RAM)

| Rank | Trial | Description | val_loss | Status | Artifacts |
|------|-------|-------------|----------|--------|-----------|
| — | 000 | T004 HP, 5M-row cap (artifact generation) | — | ✅ train done, val done | `model.pt`, `label_encoders.pkl`, `val_predictions.parquet` |
| — | 001 | T004 HP, full 3-day data, no MAX_ROWS | — | 🔄 training (run_id=66034724886) | pending |

**Trial 000 artifacts**: `s3://vungle2-ssp-dev/gminor/hp_sweep/floor_optimization/dcnv2/2026-07-01-dcnv2-r2-artifacts/trial_000/`  
**Trial 001 artifacts**: same prefix, `trial_001/` — full-data model for production use

---

## Phase 1 Outputs

### 1A — Segment Analysis ✅ COMPLETE

**Notebook**: `notebooks_databricks/phase1a_segment_analysis.py`  
**Run**: Databricks run_id=476587322071797, CPU cluster 0113-063823-4l9ug55c  
**S3 outputs**: `s3://vungle2-ssp-dev/chenliu/floor_opt_analysis/phase1a/trial_000/`

| Output | S3 Key | Status |
|--------|--------|--------|
| GBT feature importance plot | `phase1a_importance.png` | ✅ |
| CART segment summary | `segment_summary.json` | ✅ |
| High-error leaf event_id lists | `segments/leaf_{id}_event_ids.txt` | ✅ |
| Multi-dim drill-down (hierarchical) | `multidim_drill_down_summary.txt` | ✅ |
| Multi-dim drill-down (JSON) | `multidim_drill_down.json` | ✅ |

**Key findings (global MAE = 0.000360):**

Primary driver: `avg_hbedsp_highest_price_7d` — root CART split at 20.36; error scales with CPM tier.

Error contribution by segment (n × mae / total — the correct priority metric):

| Segment | mae_vs_global | err_contrib% | Action |
|---------|--------------|-------------|--------|
| `ad_type=video` | 2.4x | **88.6%** | Primary target |
| `placement_type=interstitial` | 2.9x | 48.5% | Overlaps with video |
| `country=US` | 2.0x | 40.4% | High volume driver |
| `placement_type=appopen` | 4.4x | 7.0% | Distinct format, secondary target |
| `supply_name=rovio` | 9.7x | 0.2% | High ratio but tiny n, not actionable |

CART high-error leaves (mae_vs_global ≥ 2.0): leaves 24, 23, 22, 19, 15, 14, 20, 11, 7, 12  
Worst CART leaf: `avg_hbedsp_highest_price_7d > 110.93` — 85x global, n=639

Multi-dim drill-down key finding: `appopen → US` = 28.2x global (n=683); `appopen → US → admob → android` = 33.8x (n=203, err_contrib=0.66% — small absolute impact).

**Feature gap conclusion**: model lacks price *volatility* signal (`std_hbedsp_highest_price_7d`, `cv = std/avg`). avg smooths over volatile CPM slots → systematic underfit.

---

### 1B — Feature Gap Analysis ⏳ PENDING (waiting for val dt=2026-06-28)

**🔑 Key finding: candidate features already exist in pipeline 102 — no Trino needed**

Pipeline 102 outputs 61 columns; training only uses 23. The 44 unused columns include exactly the price volatility and format-specific features we need:

| Unused column family | Count | What it gives us |
|---------------------|-------|-----------------|
| `var_hbedsp_highest_price_{placement_type}7d` | 7 | Price **variance** by placement type — the volatility signal |
| `avg_hbedsp_highest_price_{placement_type}7d` | 7 | Placement-type-specific price avg (vs current global avg) |
| `max_hbedsp_highest_price_{placement_type}7d` | 7 | Placement-type-specific price ceiling |
| `hbedsp_bid_density_{placement_type}7d` | 7 | Placement-type-specific bid density |
| `hbedsp_net_rev_{placement_type}7d` | 7 | Placement-type-specific net revenue |
| `avg_hb_adx_bid_price_video7d` | 1 | Video-specific ADX bid price |

Placement types: `app_open`, `banner`, `in_line`, `interstitial`, `mrec`, `native`, `rewarded`

**Why Phase 1C `ad_type` ≈ 0 is explained:** we feed the model a *global* `avg_hbedsp_highest_price_7d`; the format-specific version already exists in the data — just never added to training.

**Feature gap hypothesis (unchanged):**

`avg_hbedsp_highest_price_7d` alone masks price distribution shape. `var_hbedsp_highest_price_{type}7d` is the variance proxy we need. Quantiles preferred over std/cv (right-skewed bid prices; floor setting is a percentile problem). `(p75 - p25) / p50` (IQR ratio) = normalized volatility.

Since `var` is already in pipeline 102, quantile features (`p25`, `p75`, `p90`) would require new pipeline engineering — validate `var` first, then decide if quantiles add further.

**Phase 1B plan (no Trino required):**
1. Load pipeline 102 parquet (dt=2026-06-28) with all 61 cols + join to val_predictions on event_id
2. Within each segment: correlate unused cols with `abs_error` (Spearman / MI)
3. Probe LightGBM on unused cols → predict abs_error → rank feature families by importance
4. Confirm top-3 families → add to Round 2 training features

**Target segments:**

| Segment | Filter | Failure mode |
|---------|--------|-------------|
| A — High-CPM video | `ad_type='video' AND avg_hbedsp_highest_price_7d > 20` | Price volatility, avg too smooth |
| B — appopen | `placement_type='appopen'` | Different auction dynamics per format |
| C — Sparse video | `ad_type='video' AND hbedsp_bid_density_7d < 1.0` | Insufficient bid history |

---

### 1C — Cross Weight Heatmap ⏳ PENDING

**Input**: `trial_000/model.pt`  
**Goal**: 23×23 feature interaction matrix; find near-zero rows (missing interaction partners)

---

## Phase 2 — Feature Validation Trials

**Status: TBD** — depends on Phase 1B/1C findings.

Leading candidates based on Phase 1A:
1. `std_hbedsp_highest_price_7d` + `cv_hbedsp_highest_price_7d` (price volatility)
2. Supply × format-level price percentile rank
3. appopen-specific floor reference features (Phase 1B to confirm)
