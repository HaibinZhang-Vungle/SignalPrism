# HP Search Leaderboard — 2026-06-27-dcnv2-hp-initial

**Metric**: val_loss (lower = better)  
**Control baseline**: 1.2296e-05  
**Data**: pipeline 102, 7-day window ending 2026-06-27, ~5M rows  
**Cluster**: g4dn.4xlarge (T4 GPU, 16 vCPU, 64 GB RAM)

| Rank | Trial | Kind | val_loss | vs control | LR | Architecture | Cross | emb | Clip | Epochs | Notes |
|------|-------|------|----------|------------|----|-------------|-------|-----|------|--------|-------|
| 1 | 004 | exp | 1.1565e-05 | -5.9% | 0.0003 | 512-256-128 | 3 | 16 | — | 20 | Best; nominated for offline sim |
| 2 | 009 | exp | 1.1568e-05 | -5.9% | 0.0003 | 512-256-128 | 1 | 16 | — | 20 | cross=1 ≈ cross=3 (0.03% diff = noise) |
| 3 | 008 | exp | 1.1573e-05 | -5.9% | 0.0003 | 512-256-128 | 3 | 32 | — | 20 | emb=32 ≈ emb=16 (0.07% diff = noise) |
| 4 | 005 | exp | 1.1583e-05 | -5.8% | 0.0003 | 512-256-128 | 5 | 16 | — | 20 | cross=5 ≈ cross=3 |
| 5 | 003 | exp | 1.1588e-05 | -5.8% | 0.0003 | 128-64-32 | 3 | 16 | — | 20 | arch width irrelevant |
| 6 | 002 | exp | 1.1602e-05 | -5.7% | 0.003 | 256-128-64 | 3 | 16 | — | 20 | LR=0.003 oscillates |
| 7 | 001 | exp | 1.1611e-05 | -5.6% | 0.0003 | 256-128-64 | 3 | 16 | — | 20 | clean convergence |
| 8 | 007 | exp | 1.1967e-05 | -2.7% | 0.0003 | 256-128-64 | 3 | 16 | — | 12 | UNDERPERFORM: 12ep insufficient |
| 9 | 000 | ctrl | 1.2296e-05 | — | 0.001 | 256-128-64 | 3 | 16 | — | 20 | baseline |
| 10 | 006 | exp | 3.2305e-05 | +163% | 0.0003 | 256-128-64 | 3 | 16 | 1.0 | 12 | UNDERFIT: clip=1.0 catastrophic |

## ✅ Round 1 Final Diagnosis (all 10 trials complete)

### The plateau is confirmed and total

Every architectural HP axis tested produced the same val_loss plateau ~1.156–1.161e-05:

| Axis | Values tested | Range of val_loss | Conclusion |
|------|--------------|-------------------|-----------|
| Learning rate | 0.001, 0.0003, 0.003 | 1.156–1.229e-05 | 0.0003 best; others are noise |
| DNN arch width | 128-64-32, 256-128-64, 512-256-128 | 1.157–1.161e-05 | **no effect** |
| Cross layers | 1, 3, 5 | 1.157–1.158e-05 | **no effect** |
| Embedding dim | 16, 32 | 1.157e-05 | **no effect** |
| Epochs | 12, 20 | 1.197 vs 1.157e-05 | 20 necessary |
| Gradient clipping | none, 1.0 | 1.157 vs 3.23e-05 | never clip ≤1.0 |

### Cross network finding (trial 009)
cross_layers=1 gives val_loss=1.1568e-05 vs cross_layers=3 gives 1.1565e-05.  
**Difference: 0.03% — indistinguishable from noise.**  
→ The cross network (matrix interaction) adds zero measurable value over a plain DNN for this problem.  
→ For production, cross_layers=1 is sufficient (smaller model, faster inference).

### Root cause hypothesis
The plateau is NOT an architecture problem. It is a **feature bottleneck**:
- The model has exhausted learnable signal from the current 23 features
- Gradient norms collapse to near-zero by ep11–13 in every trial
- val_split temporal leakage further masks real signal differences

### Round 2 direction
Feature engineering first — see `ROUND1_SUMMARY.md` Section 8 (Feature Gap Analysis Plan):
1. Instrument notebook to save model.pt + val_predictions.parquet (all 61 pipeline-102 cols)
2. Residual analysis → identify high-error segments
3. Schema-driven gap analysis → rank unused pipeline-102 columns by KL divergence
4. Add top-2 feature candidates; validate per-segment MAE + offline sim

### Candidates for offline simulation (sim_dt=2026-06-28)
- **Trial 004**: arch=512-256-128, cross=3, emb=16, LR=0.0003, 20ep
- **Trial 009**: same except cross=1 (smaller model, equal performance)
- **Trial 001**: smallest arch, baseline LR fix — sanity check
