# DCN v2 Floor Optimization — Round 2 Plan

**Branch**: `incandescent-mimosa`  
**Status**: Phase 0 in progress — `model.pt` + `label_encoders.pkl` saved ✅; `val_predictions.parquet` pending  
**Root cause of Round 1 plateau**: Feature bottleneck, not architecture. Gradient norms collapse by ep11–13 across all trials.

---

## Phases

| Phase | File | Status |
|-------|------|--------|
| Phase 0 — Instrument Training Notebook | [ROUND2_PHASE0.md](ROUND2_PHASE0.md) | 🟡 In progress |
| Phase 1A — Residual Analysis | [ROUND2_PHASE1A.md](ROUND2_PHASE1A.md) | ⬜ Pending |
| Phase 1B — Feature Gap via Trino | [ROUND2_PHASE1B.md](ROUND2_PHASE1B.md) | ⬜ Pending |
| Phase 1C — Cross Weight Interpretability | [ROUND2_PHASE1C.md](ROUND2_PHASE1C.md) | ⬜ Pending |
| Phase 2 — Feature Validation Trials | [ROUND2_PHASE2.md](ROUND2_PHASE2.md) | ⬜ TBD after Phase 1 |

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
| Offline sim reference | `notebooks/floor_optimization/offline_simulation_event_level.py` |
| Best Round 1 result S3 | `s3://vungle2-ssp-dev/gminor/hp_sweep/floor_optimization/dcnv2/2026-06-27-dcnv2-hp-initial/trial_004/result.json` |
| Phase 0 artifact output S3 | `s3://vungle2-ssp-dev/gminor/hp_sweep/floor_optimization/dcnv2/2026-07-01-dcnv2-r2-artifacts/trial_000/` |
| Val intermediate S3 | `s3://vungle2-ssp-dev/chenliu/dcnv2_r2_val/{SEARCH_ID}/{TRIAL_ID}` |
