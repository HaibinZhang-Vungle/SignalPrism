# DCN v2 HP Sweep — Recovery & Alignment Guide

**Search**: `2026-06-27-dcnv2-hp-initial`  
**Branch**: `incandescent-mimosa`  
**Date created**: 2026-06-27  
**Last updated**: 2026-06-29

This document records every step, rule, and caveat needed to resume or hand off
this HP search from scratch.

---

## 1. Directory Layout

```
autoresearch/projects/floor_optimization/searches/2026-06-27-dcnv2-hp-initial/
├── config.yaml                    # search config (data paths, stopping, metric)
├── leaderboard.md                 # live trial rankings — keep updated after every trial
├── RECOVERY.md                    # this file
├── notebooks_databricks/
│   └── autoresearch_train.py      # training notebook run via git_source on Databricks
└── trials/
    ├── 000.json                   # control baseline
    ├── 001.json … 006.json        # completed experiments
    └── 007.json                   # queued / in-progress
```

Supporting code:
- Model: `src/gminor/core/models/torch/dcn_v2.py`
- Skill runner: `.claude/skills/databricks-run-notebook/run_notebook.py`
- Cluster config: stored in `CLUSTER_ID` env var (or `~/.zshrc`)

---

## 2. End-to-End Workflow

### Step 1 — Propose Next Trial

Read `leaderboard.md` + all `trials/*.json` to understand current state.  
Write a new `trials/NNN.json` with:
- `status: "queued"`
- `hp_config` derived from prior results (adaptive — NOT a pre-planned queue)
- `rationale` explaining why these HPs were chosen given prior results
- `parent_trial_id` pointing to the closest prior trial

> **Caveats:**
> - Max 20 trials total (`stopping.max_trials` in config.yaml)
> - Trial IDs are zero-padded 3-digit strings: `"008"`, `"009"`, …
> - Always explain the "why" in `rationale` — adaptive decisions are the most
>   valuable record after the search
> - Commit the queued trial JSON before submitting to Databricks

### Step 2 — Submit Trial to Databricks

```bash
# Build trial_json and search_config_json first (see CRITICAL NOTE below)
python3 -c "
import json, yaml
from pathlib import Path
base = Path('autoresearch/projects/floor_optimization/searches/2026-06-27-dcnv2-hp-initial')
trial = json.loads((base / 'trials/NNN.json').read_text())
import datetime
def stringify_dates(obj):
    if isinstance(obj, dict): return {k: stringify_dates(v) for k, v in obj.items()}
    if isinstance(obj, list): return [stringify_dates(v) for v in obj]
    if isinstance(obj, (datetime.date, datetime.datetime)): return obj.isoformat()
    return obj
with open(base / 'config.yaml') as f:
    cfg = stringify_dates(yaml.safe_load(f))
Path('/tmp/tNNN_trial.json').write_text(json.dumps(trial))
Path('/tmp/tNNN_config.json').write_text(json.dumps(cfg))
"

TRIAL_JSON=$(cat /tmp/tNNN_trial.json)
CONFIG_JSON=$(cat /tmp/tNNN_config.json)

python3 /Users/chenliu/.claude/skills/databricks-run-notebook/run_notebook.py \
  --notebook-path autoresearch/projects/floor_optimization/searches/2026-06-27-dcnv2-hp-initial/notebooks_databricks/autoresearch_train \
  --branch incandescent-mimosa \
  --cluster-id 0628-085936-cs87thcg \
  --param "trial_json=${TRIAL_JSON}" \
  --param "search_config_json=${CONFIG_JSON}" \
  --timeout 7200 \
  --no-wait
```

> **CRITICAL: widget names are `trial_json` and `search_config_json` (full JSON blobs).**
> Passing `trial_id=NNN` and `search_id=...` gives the notebook empty dicts and causes
> `KeyError: 'trial_id'` immediately on startup (run fails in <60s). Always pass the full
> JSON of the trial file and config.yaml. Confirmed broken on trial 008 first attempt
> (run 145269347794001, FAILED after 30s).
>
> **Other caveats:**
> - Notebook loads from **GitHub branch** via `git_source` — commit + push before submitting
> - Token auto-resolved from `~/.zshrc` (`DATABRICKS_HOST` in `.envrc` is broken)
> - **Always specify `--cluster-id 0628-085936-cs87thcg`** — the `CLUSTER_ID` env var in
>   `~/.zshrc` points to a different (non-GPU, Iceberg) cluster `0113-063823-4l9ug55c`
> - If cluster is TERMINATED, the script restarts it automatically (~5 min startup)
> - Wall time ≈ 57 min data loading + 7-20 min training = ~65-80 min per trial
> - Set `--timeout 7200` (2 hours) to avoid premature polling timeout

### Step 3 — Retrieve Results

After the run reaches `TERMINATED/SUCCESS`, the notebook calls:
```python
dbutils.notebook.exit(json.dumps(result_summary))
```

Read result via Databricks API (use when AWS token is expired):
```bash
curl -s -H "Authorization: Bearer $DATABRICKS_TOKEN" \
  "https://vungle-prod.cloud.databricks.com/api/2.1/jobs/runs/get-output?run_id=RUN_ID" \
  | python3 -c "import json,sys; d=json.load(sys.stdin); print(d['notebook_output']['result'])"
```

The `result_summary` JSON contains:
```json
{
  "trial_id": "...",
  "final_train_loss": ...,
  "final_val_loss": ...,
  "total_training_time_s": ...,
  "epochs_run": ...,
  "num_rows": ...,
  "result_s3_uri": "s3://vungle2-ssp-dev/..."
}
```

> **Caveats:**
> - AWS S3 `cp` fails with `ExpiredToken` after a few hours. Use the
>   Databricks `get-output` API instead — it always works while the token is valid.
> - `result_s3_uri` always points to **vungle2-ssp-dev** (never vungle2-ssp).
>   If you see vungle2-ssp as the write target, that is a bug — abort immediately.

### Step 4 — Update Trial JSON

Update `trials/NNN.json`:
```json
{
  "status": "success",         // or "failed"
  "training": {
    "started_at": "...",
    "finished_at": "...",
    "duration_s": ...,
    "databricks_run_id": ...,
    "result_s3_uri": "s3://vungle2-ssp-dev/..."
  },
  "metrics": {
    "final_train_loss": ...,
    "final_val_loss": ...,
    "total_training_time_s": ...,
    "device": "cuda",
    "num_rows": ...,
    "epochs_run": ...
  },
  "error": null   // or description if failed
}
```

### Step 5 — Update Leaderboard

Edit `leaderboard.md`:
1. Insert new row in sorted order by `val_loss` (ascending — lower is better)
2. Update `vs control` column: `(val_loss / control_val_loss - 1) × 100%`
3. Add diagnosis note if the trial reveals something about training dynamics
4. Update "Next trials planned" section with the adaptive strategy going forward

> **Caveats:**
> - Control baseline = trial 000: `val_loss = 1.2296e-05`
> - Trial 006 (`val_loss = 3.2305e-05`) is ranked last — it's a failed trial
>   (clipping=1.0 caused underfitting), not a true arch comparison

### Step 6 — Commit and Push

```bash
git add autoresearch/projects/floor_optimization/searches/2026-06-27-dcnv2-hp-initial/
git commit -m "feat(hp-sweep): record trial NNN results — val_loss=X.XXe-05"
git push origin incandescent-mimosa
```

> **Caveats:**
> - Push before proposing the next trial — next trial's `parent_trial_id`
>   and leaderboard state must be on the remote branch

---

## 3. Current State (as of 2026-06-29)

### Cluster
- ID: `0628-085936-cs87thcg` (floor-opt-nn-gpu)
- Type: g4dn.4xlarge — T4 GPU 16GB, 16 vCPU, 64 GB RAM
- Runtime: DBR 15.4.x-gpu-ml-scala2.12
- Access mode: Single User (cliu@liftoff.io)
- Auto-terminate: 10 min

### Trial Status

| Trial | Status | val_loss | vs ctrl | Epochs | Clip | Notes |
|-------|--------|----------|---------|--------|------|-------|
| 000 | success | 1.2296e-05 | — | 20 | — | control |
| 001 | success | 1.1611e-05 | -5.6% | 20 | — | LR=0.0003 baseline |
| 002 | success | 1.1602e-05 | -5.7% | 20 | — | LR=0.003 |
| 003 | success | 1.1588e-05 | -5.8% | 20 | — | arch 128-64-32 |
| 004 | success | 1.1565e-05 | -5.9% | 20 | — | arch 512-256-128 ← current best |
| 005 | success | 1.1583e-05 | -5.8% | 20 | — | cross_layers=5 |
| 006 | failed | 3.2305e-05 | +163% | 12 | 1.0 | UNDERFIT: clip too aggressive |
| 007 | running | — | — | 12 | — | confirm 12ep = 20ep |

Trials used: 8 of 20 (7 done + 007 running)

### Key Findings So Far
1. LR=0.0003 beats control (LR=0.001) by ~5.6%; LR=0.003 ≈ LR=0.0003
2. Architecture width (128-64-32 / 256-128-64 / 512-256-128) has no meaningful effect
3. cross_layers=3 slightly better than cross_layers=5 under no-clip regime
4. gradient_clipping=1.0 is catastrophically bad: grad_norms start at 1316-2808,
   clipping reduces effective step by 1000x, model can't converge in 12 epochs
5. All no-clip trials show grad_norm → 0 by ep11-14; epochs 12-20 are dead compute

### Pending Hypothesis (trial 007)
- **Question**: does epochs=12 (no clip) match epochs=20 (no clip) val_loss?
- **If yes** (~1.156e-05): use epochs=12 for all remaining trials → halve wall time
- **If no** (significantly worse): something needs debugging before reducing epochs

---

## 4. Adaptive HP Exploration Plan (remaining 12 trials)

After trial 007 confirms the 12-epoch baseline, explore in this order:

### Phase A — Cross layer depth (2 trials)
- `cross_layers=1` — minimal cross, mostly DNN
- `cross_layers=2` — between 1 and 3

**Goal**: understand whether cross network contributes at all; narrow from 3 layers

### Phase B — Embedding dimension (2-3 trials)
- `embedding_dim=8` — smaller, faster
- `embedding_dim=32` — larger
- `embedding_dim=64` — maximum, 4x current

**Goal**: 16-dim categorical embeddings may be too small/large for 7 categorical features

### Phase C — Regularization (1-2 trials)
- `dropout_rate=0.1` — less regularization
- `dropout_rate=0.4` — more regularization
- `batch_size=4096` — larger batch (smoother gradient estimate)

**Goal**: current dropout=0.2 and batch=1024 are defaults; explore the space

### Phase D — Combine best findings (2-3 trials)
- Best cross_layers + best embedding_dim + best regularization
- May also try weight_decay reduction (0.00001) if dropout changes don't help

> **Proposal rules:**
> - Never blindly follow this plan — each trial must be proposed after reading
>   prior results. This is a roadmap, not a queue.
> - If any single HP shows a big signal (e.g., embedding_dim=32 beats 16 by >1%),
>   double down on that dimension before moving to the next phase.
> - Stop early if 3+ consecutive trials show no improvement (plateau_k=8).

---

## 5. Data and Model Details

### Input Data
- Source: pipeline 102, event-level floor optimization features
- S3 read path: `s3://vungle2-ssp/gminor/data/floor_optimization/102` ← prod read OK
- Date window: 7 days ending 2026-06-27 (inclusive), ~5M rows
- Features: 16 numerical + 7 categorical = 23 features total
- Categorical embedding dim: 16 (per feature) → total embedding dims = 7×16 = 112
- Input dim to cross network: 16 (numerical) + 112 (categorical) = 128

### Model Architecture (DCNv2)
- Cross network: matrix-parameterized, W_l ∈ ℝ^{128×128}
  - Formula: `x_{l+1} = x0 ⊙ (W_l · x_l + b_l) + x_l`
  - Code: `src/gminor/core/models/torch/dcn_v2.py:156`
- DNN tower: 3 linear layers [hidden1, hidden2, hidden3] with BN + ReLU + Dropout
- Output: `concat(cross_out[128], dnn_out[hidden3]) → Linear → scalar`

### Val Split Warning (TEMPORAL LEAKAGE)
The training notebook uses a **random shuffle** for the val split:
```python
# autoresearch_train.py ~line 6112
shuffled_df = full_df.sample(frac=1, random_state=42)
```
This means val_loss is computed with temporal leakage (future data leaks into train).
- Val_loss **rankings** are internally consistent (use for HP comparison)
- Val_loss **absolute values** are overly optimistic (do NOT compare to catboost/lgb)
- True evaluation metric = **offline simulation** (see Step 7)

### S3 Bucket Rules (CRITICAL)
- **READ**: `vungle2-ssp` (prod) — fine for reading training data and models
- **WRITE**: `vungle2-ssp-dev` ONLY — all model artifacts, result JSONs, sim outputs
- **NEVER** write to `vungle2-ssp` — this is a hard rule, not optional
- HP sweep output: `s3://vungle2-ssp-dev/gminor/hp_sweep/floor_optimization/dcnv2/2026-06-27-dcnv2-hp-initial/trial_NNN/result.json`

---

## 6. Gradient Clipping — Lessons Learned

**Do not use gradient_clipping=1.0 for this problem.**

- At epoch 0, grad_norms are 1316–2808 (measured in trials 000-005, track_grad_norms=true)
- clipping=1.0 reduces effective step size by 1000–2800×
- Result: model barely moves for 12 epochs → massive underfitting (val_loss=3.23e-05)
- Trial 006 confirmed this definitively

**If exploring gradient clipping in future trials, use:**
- clipping=10.0 or clipping=50.0 as a first step (still allows natural descent)
- clipping=5.0 at most for stability experiments
- Track grad_norm trajectory per epoch to verify it stabilizes without collapsing

---

## 7. Post-Sweep: Offline Simulation

After the HP sweep ends (all 20 trials done or plateau hit), run offline sim for the
**top 3-5 trials by val_loss** (excluding trial 006 which is a known failure).

### Simulation Protocol
- Notebook: `notebooks/floor_optimization/offline_simulation_event_level.py`
- **sim_dt**: `2026-06-28` (t+1 where t = train_end_date = 2026-06-27)
- Profile: `dev`
- Source: trial's `result_s3_uri` → load model from S3
- Output: write to `vungle2-ssp-dev` only

### Running via Databricks skill
```bash
python3 .claude/skills/databricks-run-notebook/run_notebook.py \
  --notebook-path notebooks/floor_optimization/offline_simulation_event_level \
  --branch incandescent-mimosa \
  --param profile_name=dev \
  --param sim_dt=2026-06-28 \
  --param model_s3_uri=<result_s3_uri from trial JSON>
```

### Comparison Baseline
- CatBoost / LightGBM baselines from prior searches (same sim_dt=2026-06-28)
- Do NOT compare val_loss directly to catboost val_loss (different split, different leakage)
- Compare only offline sim metrics (revenue, CPM, fill rate changes)

---

## 8. Post-Sweep: Feature-Level Cross Interaction Analysis

After the best trial is identified, add a separate analysis notebook (do NOT modify
the training notebook) that extracts feature-level interaction importance.

### Concept
DCN v2 cross layer weight matrix W_l ∈ ℝ^{128×128} can be aggregated to a
23×23 feature-level interaction matrix:
- Numerical features: dims 0–15 (one dim per feature, already feature-level)
- Categorical features: dims 16–127 (7 features × 16 embedding dims each)
- Aggregate by taking `max` or `mean` of absolute weights over embedding dim blocks

### Implementation Sketch
```python
import torch
import numpy as np
import matplotlib.pyplot as plt

# Load model checkpoint from best trial's result_s3_uri
# model = load DCNv2 from s3://vungle2-ssp-dev/...

num_numerical = 16
num_categorical = 7
embedding_dim = 16
num_features = num_numerical + num_categorical  # 23

def aggregate_to_feature_level(W_abs, num_numerical, num_categorical, embedding_dim):
    """Aggregate 128×128 cross weight to 23×23 feature interaction matrix."""
    n = num_numerical + num_categorical
    F = torch.zeros(n, n)
    
    # numerical × numerical: top-left block (0:16, 0:16)
    F[:num_numerical, :num_numerical] = W_abs[:num_numerical, :num_numerical]
    
    # numerical × categorical: top-right block (0:16, 16:128)
    for cat_j in range(num_categorical):
        j_start = num_numerical + cat_j * embedding_dim
        j_end = j_start + embedding_dim
        F[:num_numerical, num_numerical + cat_j] = W_abs[:num_numerical, j_start:j_end].max(dim=1).values
    
    # categorical × numerical: bottom-left block (16:128, 0:16)
    for cat_i in range(num_categorical):
        i_start = num_numerical + cat_i * embedding_dim
        i_end = i_start + embedding_dim
        F[num_numerical + cat_i, :num_numerical] = W_abs[i_start:i_end, :num_numerical].max(dim=0).values
    
    # categorical × categorical: bottom-right block (16:128, 16:128)
    for cat_i in range(num_categorical):
        for cat_j in range(num_categorical):
            i_start = num_numerical + cat_i * embedding_dim
            i_end = i_start + embedding_dim
            j_start = num_numerical + cat_j * embedding_dim
            j_end = j_start + embedding_dim
            F[num_numerical + cat_i, num_numerical + cat_j] = W_abs[i_start:i_end, j_start:j_end].max()
    
    return F

# Accumulate across cross layers (sum absolute weights)
F_total = torch.zeros(num_features, num_features)
for layer in model.cross_network:   # ModuleList of Linear(128,128)
    W = layer.weight.data  # (128, 128)
    W_abs = W.abs()
    F_total += aggregate_to_feature_level(W_abs, num_numerical, num_categorical, embedding_dim)

# Plot 23×23 heatmap
feature_names = [
    # 16 numerical features (names TBD from feature schema)
    "num_0", ..., "num_15",
    # 7 categorical features
    "country", "app_category", "device_type", "ad_format",
    "placement_id_hash", "os", "publisher_id"
]
plt.figure(figsize=(12, 10))
plt.imshow(F_total.numpy(), cmap='hot', aspect='auto')
plt.colorbar(label='|W| accumulated')
plt.xticks(range(23), feature_names, rotation=90)
plt.yticks(range(23), feature_names)
plt.title(f'DCNv2 Feature Interaction (best trial, {num_cross_layers} cross layers)')
plt.tight_layout()
plt.savefig('cross_interaction_heatmap.png', dpi=150)
```

> **Notes:**
> - Add as a separate notebook, NOT inside autoresearch_train.py
> - Feature name order must match the exact order the training notebook passes
>   features to the model (check `autoresearch_train.py` feature list)
> - Verify the 16 numerical feature names by inspecting pipeline 102 schema

---

## 9. Quick Reference: Commands

### Check run status
```bash
# via Databricks API (preferred when AWS token expired)
curl -s -H "Authorization: Bearer $DATABRICKS_TOKEN" \
  "https://vungle-prod.cloud.databricks.com/api/2.1/jobs/runs/get-output?run_id=RUN_ID" \
  | python3 -c "import json,sys; d=json.load(sys.stdin); print(d['metadata']['state'])"
```

### Submit a trial
```bash
python3 .claude/skills/databricks-run-notebook/run_notebook.py \
  --notebook-path autoresearch/projects/floor_optimization/searches/2026-06-27-dcnv2-hp-initial/notebooks_databricks/autoresearch_train \
  --branch incandescent-mimosa \
  --param trial_id=NNN \
  --param search_id=2026-06-27-dcnv2-hp-initial \
  --param profile_name=dev \
  --timeout 7200
```

### Commit trial results
```bash
git add autoresearch/projects/floor_optimization/searches/2026-06-27-dcnv2-hp-initial/
git commit -m "feat(hp-sweep): record trial NNN — val_loss=X.XXe-05"
git push origin incandescent-mimosa
```

---

## 10. Decisions Log

| Date | Decision | Rationale |
|------|----------|-----------|
| 2026-06-27 | Train_end_date=2026-06-27, sim_dt=2026-06-28 | t+1 protocol; model date = end of training window |
| 2026-06-27 | Use random val split (not time-based) | Expedient for HP search; offline sim is true metric |
| 2026-06-28 | Reduce epochs 50→20 | Trials 000-005 showed convergence by ep8; ep20-50 wasted |
| 2026-06-29 | Switch to fully adaptive trial proposal | Pre-planned queue ignores what we learn from results |
| 2026-06-30 | Clipping=1.0 ruled out | Trial 006: reduces effective step by 1000x, val_loss 2.8× worse |
| 2026-06-30 | Trial 007: test epochs=12 no-clip | Grad norms → 0 by ep11 in all prior trials; ep12-20 may be dead compute |
