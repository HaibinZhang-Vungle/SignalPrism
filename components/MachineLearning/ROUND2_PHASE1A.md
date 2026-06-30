# Round 2 — Phase 1A: Residual Analysis

**Input:** `val_predictions.parquet` (23 features + event_id + prediction/label/residual/abs_error)

**Goal:** Identify high-error segments via permutation importance and CART tree on residuals.

---

## Permutation Importance

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

---

## Segment Discovery — CART Tree on Residuals

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

## Checklist

- [ ] Permutation importance table produced
- [ ] CART-tree high-error segments (mae_vs_global > 2.0) identified
- [ ] `event_id` lists extracted per segment → passed to Phase 1B
