# Round 2 — Phase 0: Instrument Training Notebook

**Goal**: Retrain T004 (same HP, same features) to produce `model.pt` + `val_predictions.parquet` for Phase 1 analysis.

**New search directory**: `autoresearch/projects/floor_optimization/searches/2026-07-01-dcnv2-r2-artifacts/`

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

## Notebook Split (implemented 2026-06-30)

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

---

## Key Failure History and Root Causes

| Run | Failure | Root cause |
|-----|---------|------------|
| Runs 1–4 | `IOException: No space left on device` | `val_spark.count()` triggered temporal sort → shuffle spilled to local disk; fixed by clearCache + fresh post-training val read |
| Run 5 (run_id=245304641600382) | Driver OOM exit 137 at ~40 min | `toPandas()` on 129M val rows allocated JVM Arrow buffers over 29 min, exhausted driver RAM |
| Run 6 (run_id=895998113528526) | Same OOM | Comprehensive tensor cleanup insufficient; crash was in val, not training |
| Run 7 (run_id=365298168187074) | `ImportError: Install s3fs` | `pd.read_parquet(s3_path)` needs s3fs; Spark uses Hadoop S3A (different); **fix: use spark.read.parquet() instead** |

---

## Config

- `train_end_date: 2026-06-27`, `train_n_days: 3`
- `model_output.bucket: vungle2-ssp-dev` (NEVER vungle2-ssp)
- Val sample: `val_sample_rows: 10_000_000` (default in autoresearch_val.py)

---

## Trial 000 — HP Config (T004 exact)

| HP | Value |
|----|-------|
| `num_cross_layers` | 3 |
| `hidden_dim1/2/3` | 512 / 256 / 128 |
| `embedding_dim` | 16 |
| `learning_rate` | 0.0003 |
| `epochs` | 20 |
| `val_split` | 0.0 (disabled; temporal split used instead) |

---

## Checklist

- [x] Create search directory: `searches/2026-07-01-dcnv2-r2-artifacts/`
- [x] Write `config.yaml` (`train_end_date: 2026-06-27`, `train_n_days: 3`, `model_output.bucket: vungle2-ssp-dev`)
- [x] Write `trials/000.json` (T004 HP: `num_cross_layers=3, hidden_dim1=512, hidden_dim2=256, hidden_dim3=128, embedding_dim=16, lr=0.0003, epochs=20`)
- [x] `autoresearch_train.py`: `spark.catalog.clearCache()` at start, `val_split=0.0`, saves `model.pt` + `label_encoders.pkl`
- [x] `autoresearch_val.py`: loads artifacts from S3, writes full val to S3 via Spark, reads back with `spark.read.parquet()`, samples 10M rows, `toPandas()`, inference, saves `val_predictions.parquet`
- [x] **`model.pt` saved** ✅ `s3://vungle2-ssp-dev/gminor/hp_sweep/floor_optimization/dcnv2/2026-07-01-dcnv2-r2-artifacts/trial_000/model.pt`
- [x] **`label_encoders.pkl` saved** ✅ same prefix
- [ ] **`val_predictions.parquet`** — autoresearch_val.py submitted, in progress
- [ ] **After val succeeds: run autoresearch_train.py with full data** (remove `MAX_ROWS=5_000_000` cap)

### Phase 0 Done When

- [x] `model.pt` exists ✅
- [x] `label_encoders.pkl` exists ✅
- [ ] `val_predictions.parquet` exists (23 features + event_id + prediction/label/residual/abs_error)
