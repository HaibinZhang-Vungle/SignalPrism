# Round 2 — Phase 1C: Cross Weight Interpretability

**Input:** `model.pt`  
**Goal:** See which feature pairs the cross network learned; identify near-zero rows (features with no cross signal = missing interaction partner).

---

## Formula

```
I(i,j) = (1/L) × Σ_{l=1}^{L} ||W_l[block_i, block_j]||_F / sqrt(dim_i × dim_j)
```

Where `block_i` = input-vector indices for feature i (scalar for numerical; 16-dim slice for categorical).

---

## Implementation

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

---

## Interpretation Guide

| Pattern | Meaning | Action |
|---------|---------|--------|
| Strong diagonal, weak off-diagonal | Cross ≈ DNN; no feature combinations learned | Add categorical features with more diversity |
| Near-zero row for feature X | Feature X has no cross signal | Either redundant or missing interaction partner |
| Strong I(a, b) for specific pair | Model captures that interaction | Keep; look for similar pairs to add |

---

## Checklist

- [ ] 23×23 cross interaction heatmap saved to `/tmp/cross_interaction_heatmap.png`
- [ ] Top-5 off-diagonal pairs documented
