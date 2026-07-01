# DCN v2 Floor Optimization HP Search — Round 1 Summary

**Date**: 2026-06-27 ~ 2026-06-29  
**Search ID**: `2026-06-27-dcnv2-hp-initial`  
**Branch**: `incandescent-mimosa`  
**Status**: Round 1 COMPLETE — all 10 trials finished; best = trial 004 (val_loss=1.1565e-05, -5.9%)

---

## 1. What We Were Trying To Do

We ran an adaptive hyperparameter search for a DCN v2 (Deep & Cross Network v2) model for floor optimization. The goal was to find a configuration that beats the catboost/lgb baseline as measured by **offline simulation** (not val_loss).

### Model Architecture (DCN v2)

```
Input (128-dim): 16 numerical + 7 categorical × 16 embedding_dim
      ↓
Cross Network: num_cross_layers × Linear(128, 128)
  x_{l+1} = x0 ⊙ (W_l · x_l + b_l) + x_l    ← matrix cross
      ↓
DNN Tower: Linear(hidden1) → BN → ReLU → Dropout → ... × 3 layers
      ↓
Concat(cross_out, dnn_out) → Linear → scalar prediction
```

Code: `src/gminor/core/models/torch/dcn_v2.py`

### Search Infrastructure

- **Pattern**: file-based autoresearch — `trials/NNN.json` + `leaderboard.md` committed to git
- **Submission**: `run_notebook.py` via `git_source` (branch loaded directly, no Repos sync)
- **Training notebook**: `autoresearch/projects/floor_optimization/searches/2026-06-27-dcnv2-hp-initial/notebooks_databricks/autoresearch_train.py`
- **Cluster**: `0628-085936-cs87thcg` (floor-opt-nn-gpu), g4dn.4xlarge, T4 GPU 16GB, Single User (cliu@liftoff.io)
- **Wall time per trial**: ~65–80 min (57 min Spark data load + 7–20 min GPU training)

---

## 2. Data Setup

| Item | Value |
|------|-------|
| Source | Pipeline 102, event-level floor opt features |
| Date window | 7 days ending 2026-06-27 (~5M rows) |
| Val split | Random shuffle 10% holdout (**has temporal leakage**) |
| Val loss use | HP ranking only — not comparable across models or to catboost/lgb |
| True eval metric | Offline sim at `sim_dt = 2026-06-28` (t+1 from train_end_date) |

**Important**: val_loss has temporal leakage because val rows are randomly sampled from the full dataset, not held out by time. Val_loss rankings are internally consistent but absolute values are overly optimistic. Always use offline sim for cross-model comparison.

---

## 3. Round 1 Trial Results

| Trial | val_loss | vs ctrl | LR | Arch (DNN) | Cross | emb | Clip | Ep | Outcome |
|-------|----------|---------|-----|------------|-------|-----|------|----|---------|
| 000 (ctrl) | 1.2296e-05 | — | 0.001 | 256-128-64 | 3 | 16 | — | 20 | baseline |
| 001 | 1.1611e-05 | -5.6% | 0.0003 | 256-128-64 | 3 | 16 | — | 20 | LR fixed |
| 002 | 1.1602e-05 | -5.7% | 0.003 | 256-128-64 | 3 | 16 | — | 20 | LR=0.003 oscillates |
| 003 | 1.1588e-05 | -5.8% | 0.0003 | 128-64-32 | 3 | 16 | — | 20 | small arch ok |
| 004 | **1.1565e-05** | **-5.9%** | 0.0003 | 512-256-128 | 3 | 16 | — | 20 | **best** |
| 005 | 1.1583e-05 | -5.8% | 0.0003 | 512-256-128 | 5 | 16 | — | 20 | cross=5 ≈ cross=3 |
| 006 | 3.2305e-05 | +163% | 0.0003 | 256-128-64 | 3 | 16 | 1.0 | 12 | ❌ UNDERFIT: clip too aggressive |
| 007 | 1.1967e-05 | -2.7% | 0.0003 | 256-128-64 | 3 | 16 | — | 12 | ❌ 12ep insufficient |
| 008 | 1.1573e-05 | -5.9% | 0.0003 | 512-256-128 | 3 | 32 | — | 20 | emb=32 ≈ emb=16 |
| 009 | 1.1568e-05 | -5.9% | 0.0003 | 512-256-128 | 1 | 16 | — | 20 | cross=1 ≈ cross=3 (0.03% diff = noise) |

**Best trial**: 004 (val_loss=1.1565e-05, -5.9% vs control)  
**S3 model**: `s3://vungle2-ssp-dev/gminor/hp_sweep/floor_optimization/dcnv2/2026-06-27-dcnv2-hp-initial/trial_004/result.json`

---

## 4. Key Findings from Round 1

### What Does NOT Matter (all give ~1.156–1.161e-05)
- **LR**: 0.0003 and 0.003 are equivalent; 0.0003 preferred for cleaner convergence
- **DNN arch width**: 128-64-32, 256-128-64, 512-256-128 — all the same
- **Cross layers depth**: 3 ≈ 5 (1 layer being tested in trial 009)
- **Embedding dim**: 16 ≈ 32 (0.07% difference = noise)

### What DOES Matter
- **Epochs**: 20 is necessary; 12 epochs gives only -2.7% vs control (model still learns past ep12)
- **Gradient clipping**: Do NOT use ≤ 1.0. Grad norms at ep0 are 1316–2808; clip=1.0 reduces effective step by 1000×, causing catastrophic underfitting
- **LR=0.001 (control)**: clearly suboptimal; 0.0003 or 0.003 both give ~5.7% gain

### The Plateau Problem
**All well-configured trials land at the same val_loss (~1.157e-05).** This is the central unresolved question. Possible causes:

1. **Val split leakage**: Random shuffle val split inflates all val_losses uniformly — all models look equally good, masking real differences
2. **Data/label noise ceiling**: The task has inherent label noise that limits model discriminability on val set
3. **Cross + DNN converge to the same solution**: Both components may be learning the same function, so architectural variation doesn't help
4. **Regularization dominating**: dropout=0.2 + weight_decay=0.0001 may be washing out HP signal

### What Trial 009 Will Tell Us
`cross_layers=1` vs control `cross_layers=3`:
- If val_loss ≈ 1.157e-05 → cross network contributes nothing; model is effectively a DNN
- If val_loss significantly worse → cross depth matters; best is cross=3

---

## 5. Known Infrastructure Issues (fixed in Round 1, must remember for Round 2)

### Issue 1: Wrong Databricks Host
- `DATABRICKS_HOST` in `.envrc` is broken (token doesn't start with `dapi`)
- `CLUSTER_ID` in `~/.zshrc` = `0113-063823-4l9ug55c` (Iceberg cluster, **no GPU**)
- **Fix**: Always use `--cluster-id 0628-085936-cs87thcg` explicitly; use host `https://vungle-datasci.cloud.databricks.com`

### Issue 2: Correct Widget Names for Submission
The training notebook expects **full JSON blobs**, not bare IDs:
```python
# WRONG — causes KeyError: 'trial_id' in <30s
--param trial_id=009 --param search_id=2026-06-27-dcnv2-hp-initial

# CORRECT — pass full JSON of trial file and config.yaml
--param "trial_json=$(cat trials/009.json)" \
--param "search_config_json=$(python3 -c 'import json,yaml; ...')"
```

**Submission template** (copy-paste for Round 2):
```bash
python3 -c "
import json, yaml, datetime
from pathlib import Path
base = Path('autoresearch/projects/floor_optimization/searches/<SEARCH_ID>')
trial = json.loads((base / 'trials/NNN.json').read_text())
def sd(o):
    import datetime
    if isinstance(o, dict): return {k: sd(v) for k,v in o.items()}
    if isinstance(o, list): return [sd(v) for v in o]
    if isinstance(o, (datetime.date, datetime.datetime)): return o.isoformat()
    return o
with open(base / 'config.yaml') as f:
    cfg = sd(yaml.safe_load(f))
Path('/tmp/tNNN_trial.json').write_text(json.dumps(trial))
Path('/tmp/tNNN_config.json').write_text(json.dumps(cfg))
"

TRIAL_JSON=$(cat /tmp/tNNN_trial.json)
CONFIG_JSON=$(cat /tmp/tNNN_config.json)

python3 /Users/chenliu/.claude/skills/databricks-run-notebook/run_notebook.py \
  --notebook-path "autoresearch/projects/floor_optimization/searches/<SEARCH_ID>/notebooks_databricks/autoresearch_train" \
  --branch "<BRANCH>" \
  --cluster-id "0628-085936-cs87thcg" \
  --param "trial_json=${TRIAL_JSON}" \
  --param "search_config_json=${CONFIG_JSON}" \
  --timeout 7200 \
  --no-wait
```

### Issue 3: S3 Write Bucket
- **NEVER** write to `vungle2-ssp` (prod)
- All outputs go to `vungle2-ssp-dev`
- Reading training data from `vungle2-ssp` is fine

### Issue 4: AWS Token Expiry
When `aws s3 cp` fails with `ExpiredToken`, use the Databricks `get-output` API instead:
```bash
curl -s -H "Authorization: Bearer $TOKEN" \
  "https://vungle-datasci.cloud.databricks.com/api/2.1/jobs/runs/get-output?run_id=NNN" \
  | python3 -c "import json,sys; print(json.load(sys.stdin)['notebook_output']['result'])"
```

---

## 6. What Round 1 Did NOT Explore

These dimensions were not tested and are candidates for Round 2:
- `dropout_rate`: only tried 0.2 — try 0.1, 0.3, 0.4
- `batch_size`: only tried 1024 — try 2048, 4096
- `weight_decay`: only tried 0.0001 — try 0.00001, 0.001
- `cross_layers=0` (pure DNN — no cross network at all)
- `embedding_dim=8` (smaller embeddings)
- Time-based val split (fix temporal leakage to get cleaner HP signal)
- Longer training windows (14-day vs 7-day)
- Feature engineering changes
- Learning rate schedule / warmup

---

## 7. Offline Simulation Plan (Next Step After Round 2)

Run offline sim for **top 3–5 trials** by val_loss:
- Candidates: trials 004, 008, 005 (and best from Round 2)
- `sim_dt = 2026-06-28` (always t+1 from train_end_date)
- Notebook: `notebooks/floor_optimization/offline_simulation_event_level.py`
- Compare to catboost/lgb baseline from same sim_dt

---

## 8. Round 2 — Optimization Points

### Confirmed optimization: Feature Gap Analysis (from FEATURE_GAP_ANALYSIS_PLAN.md)

Source: `north-gemini` branch, same search dir — see full doc there for implementation details.

**Core insight**: Round 1 plateau at ~1.157e-05 is almost certainly a **feature bottleneck**, not an architecture bottleneck. Gradient norms collapsing by ep11–13 across all trials confirms the model has exhausted learnable signal from the current 23 features.

#### Phase 0 — Instrument training notebook (prerequisite, ~1 trial)

Modify `autoresearch_train.py` to save two additional artifacts at the end of training:

```python
# 1. Model checkpoint
torch.save(model.state_dict(), f"{output_dir}/model.pt")
# S3: s3://vungle2-ssp-dev/gminor/hp_sweep/.../trial_NNN/model.pt

# 2. Val predictions with all 61 pipeline-102 schema columns
val_df["prediction"] = val_preds
val_df["label"]      = val_y
val_df["residual"]   = val_preds - val_y
val_df["abs_error"]  = (val_preds - val_y).abs()
val_df.to_parquet(f"{output_dir}/val_predictions.parquet", index=False)
# S3: s3://vungle2-ssp-dev/gminor/hp_sweep/.../trial_NNN/val_predictions.parquet
```

**IMPORTANT**: val DataFrame must retain all 61 pipeline-102 columns (not just the 23 training features). This is what enables the gap analysis.

Re-run T004 config to produce these artifacts. This becomes the new Round 2 control.

#### Phase 1 — Offline gap analysis (no GPU needed, run on CPU cluster)

Three analyses in parallel after Phase 0:

**A. Residual distribution by segment** — for each of 7 categorical dimensions:
- Compute mean/median/p90 abs_error per segment value
- Flag segments where MAE > 2× global_MAE → **high-error segments**
- Output: ranked table of worst segments

**B. Cross weight heatmap** — load `model.pt`, aggregate W_l (128×128) → 23×23:
- Near-zero row/col → feature has weak cross interactions → may be redundant or missing an important interaction partner
- Strong diagonal → model relies on self-interactions, not feature combinations

**C. Schema-driven feature gap** — for each high-error segment:
- Compare distribution of **38 unused pipeline-102 columns** between high-error and low-error samples
- Rank by KL divergence → top-ranked = feature candidates

Key unused column groups:
```
var_hbedsp_highest_price_{ad_type}7d   (7 cols — price VARIANCE by ad type ← flagged as key gap)
max/avg_hbedsp_highest_price_{ad_type}7d (14 cols — per-type price stats)
hbedsp_net_rev_{ad_type}7d             (7 cols — revenue by ad type)
hbedsp_bid_density_{ad_type}7d         (7 cols — bid density by ad type)
device_model, sdk_version, connection_type, do_not_track
hour_of_day, day_of_week               (derived from timestamp)
```

#### Phase 2 — Feature validation trials

After gap analysis identifies top-2 candidate features:
- Add to feature engineering in `autoresearch_train.py`
- **Warm-start from T004 checkpoint** (avoids full retrain for embedding-compatible additions):
  ```python
  new_model.load_state_dict(old_state, strict=False)
  # Phase 1: freeze existing layers, train new feature embeddings only (5 ep)
  # Phase 2: unfreeze all, fine-tune (5–8 ep, lr × 0.1)
  ```
- When to use full retrain instead: if feature engineering changes existing column semantics

**Validation criteria** (do NOT rely on overall val_loss alone):
- Primary: per-segment MAE reduction on the specific high-error segments targeted
- Secondary: offline sim improvement (sim_dt=2026-06-28) vs CatBoost v009
- Guard: overall val_loss should not regress >5% on low-error segments

### Other optimization points

- **offline_decorate feature enrichment** — competitor bids, price variance, supply/demand rates; join key = `event_id`. Full spec in `OFFLINE_DECORATE_FEATURE_EXTRACTION.md` (north-gemini branch).

→ Full Round 2 plan: see `autoresearch/projects/floor_optimization/ROUND2_PLAN.md`

### Structural changes for Round 2
- **New search ID**: e.g., `2026-07-01-dcnv2-hp-round2`
- **Start from best config**: Trial 004 (arch=512-256-128, LR=0.0003, cross=3, emb=16, epochs=20)
- **Skip exhausted axes**: LR, arch width, embedding_dim all confirmed irrelevant in Round 1
- **Fix val split** (optional but recommended): time-based holdout to eliminate temporal leakage
- **Save model.pt + val_predictions.parquet** in every trial from Round 2 onward

---

## 9. Round 2 Checklist

Before starting Round 2:
- [ ] User confirms additional optimization points beyond feature gap analysis
- [ ] Modify `autoresearch_train.py`: add model.pt save + val_predictions.parquet save (all 61 cols)
- [ ] Decide: fix temporal leakage in val split or keep random split
- [ ] Create new search directory: `autoresearch/projects/floor_optimization/searches/<new-search-id>/`
- [ ] Copy and modify `config.yaml` (new search_id, control = trial 004 config)
- [ ] Commit + push before first submission
- [ ] Always use `--cluster-id 0628-085936-cs87thcg` and widget params `trial_json` + `search_config_json`
- [ ] Phase 0 trial (instrument + re-run T004) runs before Phase 1 analysis

---

## 10. File Locations

| Item | Path |
|------|------|
| Round 1 search dir | `autoresearch/projects/floor_optimization/searches/2026-06-27-dcnv2-hp-initial/` |
| Training notebook | `.../notebooks_databricks/autoresearch_train.py` |
| Leaderboard | `.../leaderboard.md` |
| Trial JSONs | `.../trials/000.json` – `009.json` |
| Recovery guide | `.../RECOVERY.md` |
| Model code | `src/gminor/core/models/torch/dcn_v2.py` |
| Offline sim notebook | `notebooks/floor_optimization/offline_simulation_event_level.py` |
| Best model S3 | `s3://vungle2-ssp-dev/gminor/hp_sweep/floor_optimization/dcnv2/2026-06-27-dcnv2-hp-initial/trial_004/result.json` |
