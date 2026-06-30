# Round 2 — Phase 2: Feature Validation Trials

**Status: TBD — depends on Phase 1 findings.**

Will be designed after Phase 1A/1B/1C conclusions. Candidate feature additions will be finalized then. No feature changes before Phase 1 completes.

---

## Expected Workflow (subject to Phase 1 output)

1. Phase 1A/1B identifies top-2 candidate features correlated with high-error segments
2. Phase 1C confirms no strong cross interaction exists for those features (i.e., they are genuinely missing)
3. Add candidates to feature engineering in `autoresearch_train.py`

---

## Warm-Start Strategy

When new features are embedding-compatible with the existing 23-feature model:

```python
new_model.load_state_dict(old_state, strict=False)
# Phase 1: freeze existing layers, train new feature embeddings only (5 ep)
# Phase 2: unfreeze all, fine-tune (5–8 ep, lr × 0.1)
```

**When to use full retrain instead:** if feature engineering changes existing column semantics.

---

## Validation Criteria

Do NOT rely on overall val_loss alone:

- **Primary:** per-segment MAE reduction on the specific high-error segments targeted by Phase 1
- **Secondary:** offline sim improvement (`sim_dt=2026-06-28`) vs CatBoost v009
- **Guard:** overall val_loss should not regress >5% on low-error segments

---

## Checklist

- [ ] Phase 1A/1B/1C conclusions documented
- [ ] Top candidate features identified and agreed upon
- [ ] Feature engineering changes implemented in `autoresearch_train.py`
- [ ] Warm-start or full-retrain decision made
- [ ] Validation trials submitted and results compared vs Phase 0 control
- [ ] Offline sim run for top trials vs CatBoost baseline
