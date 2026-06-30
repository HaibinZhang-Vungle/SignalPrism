# DCN v2 Floor Optimization — Round 2 Plan

**Branch**: `incandescent-mimosa`  
**Status**: Phase 1A complete ✅ — `val_predictions.parquet` done, segment analysis done; trial_001 full-data training RUNNING (run_id=66034724886); Phase 1B pending Trino re-auth  
**Root cause of Round 1 plateau**: Feature bottleneck, not architecture. The model has exhausted
signal from 23 features; gradient norms collapse by ep11–13 across all trials.

---

## Core Principle: Existing Features Are the Base

Round 2 Phase 0 retrains T004 **with the same 23 features unchanged**. New feature additions
are deferred until Phase 1 analysis identifies which candidates are worth adding.

**Numerical (16):**
`prediction_floor, mediation_floor, avg_device_market_price1d_raw, avg_hbedsp_market_price1d,
count_hb_of_requests1d, req_order, hbedsp_bid_density_7d, avg_hbedsp_highest_price_7d,
hbedsp_net_rev_7d, count_of_device_requests1d_raw, floor_to_hb_avg_ratio, floor_minus_hb_avg,
floor_to_max_hbedsp_ratio, floor_minus_max_hbedsp, mediation_minus_hb_avg, floor_diff`

**Categorical (7):**
`country, placement_id (hashed→512), placement_type, device_os, supply_name, ad_type,
sdk_supply_name`

---

## Data Source

**Pipeline 102 only** (same parquet input as Round 1). No offline_decorate
(`modulo_device_data_lookup`) needed — feature gap analysis is done via Trino in Phase 1B.

---

## Phase 0 — Instrument Training Notebook

**Goal**: retrain T004 (same HP, same features) to produce `model.pt` + `val_predictions.parquet`
for Phase 1 analysis.

**New search directory**: `autoresearch/projects/floor_optimization/searches/2026-07-01-dcnv2-r2-artifacts/`

### Notebook split (implemented 2026-06-30)

Training and val inference are **two separate notebooks** to avoid re-training when debugging val:

**`autoresearch_train.py`** — train only:
- Loads data, trains DCNv2, saves `model.pt` + `label_encoders.pkl` to S3
- Writes `result.json` with train metrics only (`final_val_loss: null`)
- Does NOT load val data or run inference
- ~11 min on GPU cluster

**`autoresearch_val.py`** — val inference only:
- Loads `model.pt` + `label_encoders.pkl` from S3 (no training)
- Reads full val partition `dt=DT` (~129M rows) via Spark
- Writes to `s3://vungle2-ssp-dev/chenliu/dcnv2_r2_val/{SEARCH_ID}/{TRIAL_ID}` with Spark executors (no JVM buffer)
- Reads back with `spark.read.parquet()` (no s3fs needed), samples ~10M rows, `toPandas()`
- Runs inference, saves `val_predictions.parquet`, updates `result.json` with val_loss

### Key failure history and root causes

| Run | Failure | Root cause |
|-----|---------|------------|
| Runs 1–4 | `IOException: No space left on device` | `val_spark.count()` triggered temporal sort → shuffle spilled to local disk; fixed by clearCache + fresh post-training val read |
| Run 5 (run_id=245304641600382) | Driver OOM exit 137 at ~40 min | `toPandas()` on 129M val rows allocated JVM Arrow buffers over 29 min, exhausted driver RAM |
| Run 6 (run_id=895998113528526) | Same OOM | Comprehensive tensor cleanup insufficient; crash was in val, not training |
| Run 7 (run_id=365298168187074) | `ImportError: Install s3fs` | `pd.read_parquet(s3_path)` needs s3fs; Spark uses Hadoop S3A (different); **fix: use spark.read.parquet() instead** |

### Config
- `train_end_date: 2026-06-27`, `train_n_days: 3`
- `model_output.bucket: vungle2-ssp-dev` (NEVER vungle2-ssp)
- Val sample: `val_sample_rows: 10_000_000` (default in autoresearch_val.py)

### Trial 000

T004 exact HP config — no changes:

| HP | Value |
|----|-------|
| `num_cross_layers` | 3 |
| `hidden_dim1/2/3` | 512 / 256 / 128 |
| `embedding_dim` | 16 |
| `learning_rate` | 0.0003 |
| `epochs` | 20 |
| `val_split` | 0.0 (disabled; temporal split used instead) |

---

## Phase 1A — Residual Analysis

**Input:** `val_predictions.parquet` (23 features + event_id + prediction/label/residual/abs_error)

### Permutation importance

```python
def permutation_importance(model_pt_path, val_df, feature_cols, n_repeats=3):
    """Shuffle each feature, measure val_loss increase — higher = more important."""
    model = load_dcnv2(model_pt_path)
    base_loss = compute_mse(model, val_df)
    importances = {}
    for col in feature_cols:
        scores = []
        for _ in range(n_repeats):
            shuffled = val_df.copy()
            shuffled[col] = np.random.permutation(shuffled[col].values)
            scores.append(compute_mse(model, shuffled) - base_loss)
        importances[col] = np.mean(scores)
    return pd.Series(importances).sort_values(ascending=False)
```

### Segment discovery — CART tree on residuals

Fit a shallow decision tree on `abs_error` to find naturally-occurring high-error segments:

```python
from sklearn.tree import DecisionTreeRegressor, export_text

def find_error_segments(val_df, feature_cols, max_depth=4, min_samples_leaf=500):
    X = val_df[feature_cols].fillna(-1)
    y = val_df["abs_error"]
    tree = DecisionTreeRegressor(max_depth=max_depth,
                                  min_samples_leaf=min_samples_leaf,
                                  random_state=42)
    tree.fit(X, y)
    val_df = val_df.copy()
    val_df["leaf_id"] = tree.apply(X)
    leaf_stats = (val_df.groupby("leaf_id")["abs_error"]
                  .agg(["mean", "median", "count", "std"])
                  .rename(columns={"mean": "mae", "median": "mdae",
                                   "count": "n", "std": "std_err"})
                  .sort_values("mae", ascending=False))
    leaf_stats["mae_vs_global"] = leaf_stats["mae"] / val_df["abs_error"].mean()
    print(export_text(tree, feature_names=feature_cols, max_depth=4))
    return tree, leaf_stats
```

**Threshold for "high-error segment":** `mae_vs_global > 2.0`

**Output:** segment definition (e.g. `country=US AND placement_type=rewarded AND supply_name=max`)
+ `event_id` list for each high-error leaf → passed to Phase 1B.

---

## Phase 1B — Feature Gap via Trino

**Input:** 3 target segments from Phase 1A + event_ids in `val_predictions.parquet`  
**Goal:** within each segment, find candidate new features from `ex_jaeger_transaction` that correlate with abs_error.

**Trino table:** `raw.coba2.ex_jaeger_transaction`  
**Fields of interest:** `placement_serve_results` / `placements` (nested struct — schema TBD, pending Trino re-auth)  
**Join key:** `event_id`

### Target segments (from Phase 1A, ranked by error_contribution%)

| Segment | Filter | err_contrib% | Failure mode | Features to find |
|---------|--------|-------------|-------------|-----------------|
| A — High-CPM video | `ad_type='video' AND avg_hbedsp_highest_price_7d > 20` | ~40%+ | Price volatility, avg too smooth | bid price std/p25/p75, win rate at floor tier, DSP count |
| B — appopen | `placement_type='appopen'` | 7% | Different auction dynamics | mediation floor history, fill rate, auction timeout |
| C — Sparse video | `ad_type='video' AND hbedsp_bid_density_7d < 1.0` | TBD | Insufficient bid history | days since last valid bid, placement-type fallback |

**Note on segment prioritization**: use `n × mae / total_error` (error_contribution%) not just `mae_vs_global`. rovio has 9.7x ratio but only 0.2% contribution — not actionable.

### Workflow

```
1. Trino re-auth → confirm schema of placement_serve_results / placements
2. For each segment A/B/C:
   - Filter ex_jaeger_transaction by segment conditions + date range
   - Unnest placement_serve_results / placements struct
   - Pull candidate new columns
3. Join on event_id with val_predictions → correlate candidates with abs_error
4. Rank by |Pearson r| or mutual information
```

### SQL sketch

```sql
SELECT
    t.event_id,
    t.placements[1].candidate_field_a,
    t.placements[1].candidate_field_b,
    -- add more candidate columns once schema confirmed
    v.abs_error,
    v.residual
FROM raw.coba2.ex_jaeger_transaction t
JOIN (
    -- val event_ids for this segment, uploaded or filtered inline
    SELECT event_id, abs_error, residual
    FROM val_segment_events
) v ON t.event_id = v.event_id
WHERE t.dt BETWEEN '2026-06-20' AND '2026-06-27'
  AND <segment_conditions>    -- from Phase 1A leaf definition
LIMIT 50000
```

**Open item:** confirm exact column names inside `placement_serve_results`/`placements`
by re-authenticating Trino (`/mcp` → "claude.ai MX Trino Beta" → re-auth).

### Analysis

```python
# After pulling Trino result into pandas df:
candidate_cols = [c for c in trino_df.columns
                  if c not in ("event_id", "abs_error", "residual")]
correlations = {col: trino_df[col].corr(trino_df["abs_error"])
                for col in candidate_cols}
print(pd.Series(correlations).abs().sort_values(ascending=False))
```

---

## Phase 1C — Cross Weight Interpretability

**Input:** `model.pt`  
**Goal:** see which feature pairs the cross network learned; identify near-zero rows
(features with no cross signal = missing interaction partner).

### Formula

```
I(i,j) = (1/L) × Σ_{l=1}^{L} ||W_l[block_i, block_j]||_F / sqrt(dim_i × dim_j)
```

Where `block_i` = input-vector indices for feature i (scalar for numerical; 16-dim slice for categorical).

### Implementation

```python
import torch, numpy as np, pandas as pd, seaborn as sns, matplotlib.pyplot as plt

def compute_feature_interaction_matrix(model_pt_path, numerical_features, categorical_features,
                                        embedding_dim=16):
    state = torch.load(model_pt_path, map_location="cpu")
    cross_weights = [v.numpy() for k, v in state.items()
                     if "cross_layers" in k and "weight" in k]
    if not cross_weights:
        raise ValueError("No cross layer weights found in checkpoint")

    features = numerical_features + categorical_features
    n_num = len(numerical_features)
    blocks = {}
    for i, f in enumerate(numerical_features):
        blocks[f] = [i]
    for j, f in enumerate(categorical_features):
        start = n_num + j * embedding_dim
        blocks[f] = list(range(start, start + embedding_dim))

    n = len(features)
    I = np.zeros((n, n))
    for W in cross_weights:
        for ai, fa in enumerate(features):
            for bi, fb in enumerate(features):
                sub = W[np.ix_(blocks[fa], blocks[fb])]
                I[ai, bi] += np.linalg.norm(sub, "fro") / np.sqrt(len(blocks[fa]) * len(blocks[fb]))
    I /= len(cross_weights)

    df_I = pd.DataFrame(I, index=features, columns=features)
    fig, ax = plt.subplots(figsize=(14, 12))
    sns.heatmap(df_I, ax=ax, cmap="viridis", square=True,
                xticklabels=True, yticklabels=True)
    ax.set_title("DCNv2 Cross Layer Feature Interaction Strength")
    plt.tight_layout()
    plt.savefig("/tmp/cross_interaction_heatmap.png", dpi=150)

    df_flat = (df_I.stack().reset_index()
               .rename(columns={"level_0": "feature_a", "level_1": "feature_b", 0: "score"}))
    df_flat = df_flat[df_flat.feature_a != df_flat.feature_b]
    df_flat["pair"] = df_flat.apply(lambda r: tuple(sorted([r.feature_a, r.feature_b])), axis=1)
    df_flat = df_flat.drop_duplicates("pair").sort_values("score", ascending=False)
    print(df_flat.head(10).to_string(index=False))
    return df_I, df_flat
```

### Interpretation guide

| Pattern | Meaning | Action |
|---------|---------|--------|
| Strong diagonal, weak off-diagonal | Cross ≈ DNN; no feature combinations learned | Add categorical features with more diversity |
| Near-zero row for feature X | Feature X has no cross signal | Either redundant or missing interaction partner |
| Strong I(a, b) for specific pair | Model captures that interaction | Keep; look for similar pairs to add |

---

## Phase 2 — Feature Validation Trials

**Status: TBD — depends on Phase 1 findings.**

Will be designed after Phase 1A/1B/1C conclusions. Candidate additions (from current 23-feature
analysis) will be finalized then. No feature changes before Phase 1 completes.

---

## Execution Checklist

### Phase 0

- [x] Create search directory: `searches/2026-07-01-dcnv2-r2-artifacts/`
- [x] Write `config.yaml` (`train_end_date: 2026-06-27`, `train_n_days: 3`, `model_output.bucket: vungle2-ssp-dev`)
- [x] Write `trials/000.json` (T004 HP: `num_cross_layers=3, hidden_dim1=512, hidden_dim2=256, hidden_dim3=128, embedding_dim=16, lr=0.0003, epochs=20`)
- [x] `autoresearch_train.py` (train only): `spark.catalog.clearCache()`, `val_split=0.0`, DBFS write to bypass JVM OOM, saves `model.pt` + `label_encoders.pkl`
- [x] `autoresearch_val_cpu.py` (val inference, CPU cluster): fraction=0.08 sample first, direct `toPandas()`, pydantic v2 patch, saves `val_predictions.parquet`
- [x] **`model.pt` saved** ✅ `s3://vungle2-ssp-dev/gminor/hp_sweep/floor_optimization/dcnv2/2026-07-01-dcnv2-r2-artifacts/trial_000/model.pt`
- [x] **`label_encoders.pkl` saved** ✅ same prefix
- [x] **`val_predictions.parquet` saved** ✅ same prefix (~1.03M rows, 8% sample of 3-day val)
- [x] **trial_001 full-data training** 🔄 RUNNING (run_id=66034724886, GPU cluster 0628-085936-cs87thcg, `trials/001.json`, no MAX_ROWS cap)

### Phase 0 done when

- [x] `model.pt` exists ✅
- [x] `label_encoders.pkl` exists ✅
- [x] `val_predictions.parquet` exists ✅ (23 features + event_id + prediction/label/residual/abs_error, ~1.03M rows)
- [ ] trial_001 full-data model done 🔄 RUNNING

### Phase 1 done when

- [x] **1A**: GBT importance + CART segments done ✅; multi-dim hierarchical drill-down done ✅; error_contribution% computed; 3 target segments for 1B identified
- [ ] **1B**: Trino re-auth → schema confirmed → SQL per segment → candidates ranked by error correlation
- [ ] **1C**: 23×23 cross interaction heatmap saved; top-5 off-diagonal pairs documented

---

## Reference Paths

| Item | Path |
|------|------|
| Round 2 train notebook | `searches/2026-07-01-dcnv2-r2-artifacts/notebooks_databricks/autoresearch_train.py` |
| Round 2 val notebook | `searches/2026-07-01-dcnv2-r2-artifacts/notebooks_databricks/autoresearch_val.py` |
| Round 1 training notebook (reference) | `searches/2026-06-27-dcnv2-hp-initial/notebooks_databricks/autoresearch_train.py` |
| Round 1 leaderboard | `searches/2026-06-27-dcnv2-hp-initial/leaderboard.md` |
| GPU cluster | `0628-085936-cs87thcg` (floor-opt-nn-gpu, g4dn.4xlarge); `$CLUSTER_ID` in env is WRONG — always pass explicitly |
| Trino table (Phase 1B) | `raw.coba2.ex_jaeger_transaction` (placement_serve_results / placements) |
| offline_sim reference | `notebooks/floor_optimization/offline_simulation_event_level.py` |
| Best Round 1 result S3 | `s3://vungle2-ssp-dev/gminor/hp_sweep/floor_optimization/dcnv2/2026-06-27-dcnv2-hp-initial/trial_004/result.json` |
| Phase 0 artifact output S3 | `s3://vungle2-ssp-dev/gminor/hp_sweep/floor_optimization/dcnv2/2026-07-01-dcnv2-r2-artifacts/trial_000/` |
| Val intermediate S3 | `s3://vungle2-ssp-dev/chenliu/dcnv2_r2_val/{SEARCH_ID}/{TRIAL_ID}` |
