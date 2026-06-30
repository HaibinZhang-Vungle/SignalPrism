# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 1C — Cross Weight Interpretability
# MAGIC
# MAGIC **Input:** `trial_000/model.pt` from S3
# MAGIC
# MAGIC **Outputs:**
# MAGIC 1. 23×23 feature interaction heatmap (Frobenius norm, averaged across cross layers)
# MAGIC 2. Top off-diagonal pairs ranked by interaction strength
# MAGIC 3. Near-zero rows (features with no cross signal — missing interaction partner)

# COMMAND ----------

import boto3, io, json
import torch
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns

BUCKET       = "vungle2-ssp-dev"
ARTIFACT_KEY = "gminor/hp_sweep/floor_optimization/dcnv2/2026-07-01-dcnv2-r2-artifacts/trial_000"
OUTPUT_KEY   = "chenliu/floor_opt_analysis/phase1c/trial_000"

_s3 = boto3.client("s3")

print("Downloading model.pt ...")
obj = _s3.get_object(Bucket=BUCKET, Key=f"{ARTIFACT_KEY}/model.pt")
state = torch.load(io.BytesIO(obj["Body"].read()), map_location="cpu")

print("State dict keys:")
for k in sorted(state.keys()):
    print(f"  {k}: {tuple(state[k].shape)}")

# COMMAND ----------

# === 1. Feature layout in the 128-dim input vector ===
NUMERICAL_FEATURES = [
    "prediction_floor", "mediation_floor", "avg_device_market_price1d_raw",
    "avg_hbedsp_market_price1d", "count_hb_of_requests1d", "req_order",
    "hbedsp_bid_density_7d", "avg_hbedsp_highest_price_7d", "hbedsp_net_rev_7d",
    "count_of_device_requests1d_raw", "floor_to_hb_avg_ratio", "floor_minus_hb_avg",
    "floor_to_max_hbedsp_ratio", "floor_minus_max_hbedsp", "mediation_minus_hb_avg", "floor_diff",
]
CATEGORICAL_FEATURES = [
    "country", "placement_id", "placement_type",
    "device_os", "supply_name", "ad_type", "sdk_supply_name",
]
ALL_FEATURES = NUMERICAL_FEATURES + CATEGORICAL_FEATURES

EMBEDDING_DIM = 16
N_NUM  = len(NUMERICAL_FEATURES)   # 16
N_CAT  = len(CATEGORICAL_FEATURES) # 7
INPUT_DIM = N_NUM + N_CAT * EMBEDDING_DIM  # 128

print(f"Features: {N_NUM} numerical + {N_CAT} categorical × emb={EMBEDDING_DIM} = input_dim={INPUT_DIM}")

# Block index lists: feature → indices in the 128-dim input vector
blocks = {}
for i, f in enumerate(NUMERICAL_FEATURES):
    blocks[f] = [i]
for j, f in enumerate(CATEGORICAL_FEATURES):
    start = N_NUM + j * EMBEDDING_DIM
    blocks[f] = list(range(start, start + EMBEDDING_DIM))

# COMMAND ----------

# === 2. Extract cross layer weights ===
cross_weights = []
for k in sorted(state.keys()):
    if "cross" in k.lower() and "weight" in k.lower():
        W = state[k].numpy()
        cross_weights.append(W)
        print(f"  {k}: shape={W.shape}")

if not cross_weights:
    # Fallback: any 2D weight matching input_dim
    for k in sorted(state.keys()):
        v = state[k]
        if v.ndim == 2 and INPUT_DIM in v.shape:
            W = v.numpy()
            cross_weights.append(W)
            print(f"  (fallback) {k}: shape={W.shape}")

if not cross_weights:
    raise ValueError("No cross layer weights found. Keys: " + str(list(state.keys())))

print(f"\n{len(cross_weights)} cross layer(s) found")

# COMMAND ----------

# === 3. Compute 23×23 interaction matrix ===
# I(a,b) = mean_over_layers( ||W[block_a, block_b]||_F / sqrt(dim_a * dim_b) )

n_features = len(ALL_FEATURES)
I_matrix = np.zeros((n_features, n_features))

for W in cross_weights:
    for ai, fa in enumerate(ALL_FEATURES):
        for bj, fb in enumerate(ALL_FEATURES):
            sub = W[np.ix_(blocks[fa], blocks[fb])]
            norm = np.linalg.norm(sub, "fro") / np.sqrt(len(blocks[fa]) * len(blocks[fb]))
            I_matrix[ai, bj] += norm

I_matrix /= len(cross_weights)

df_I = pd.DataFrame(I_matrix, index=ALL_FEATURES, columns=ALL_FEATURES)

print("Interaction matrix computed.")
print(f"  Global max (incl. diagonal): {I_matrix.max():.4f}")
off_diag_mask = ~np.eye(n_features, dtype=bool)
print(f"  Mean off-diagonal:           {I_matrix[off_diag_mask].mean():.4f}")
print(f"  Max  off-diagonal:           {I_matrix[off_diag_mask].max():.4f}")

# COMMAND ----------

# === 4. Heatmap ===
fig, ax = plt.subplots(figsize=(16, 13))
sns.heatmap(
    df_I, ax=ax, cmap="viridis", square=True,
    xticklabels=True, yticklabels=True,
    annot=False, linewidths=0.3, linecolor="gray",
)
ax.set_title(
    f"DCNv2 Cross Layer Feature Interaction Strength\n"
    f"trial_000 | {len(cross_weights)} cross layer(s) | avg Frobenius / sqrt(dim_i × dim_j)",
    fontsize=12,
)
plt.xticks(rotation=45, ha="right", fontsize=8)
plt.yticks(rotation=0, fontsize=8)
plt.tight_layout()
plt.savefig("/tmp/phase1c_heatmap.png", dpi=150)
_s3.upload_file("/tmp/phase1c_heatmap.png", BUCKET, f"{OUTPUT_KEY}/cross_heatmap.png")
print(f"Heatmap -> s3://{BUCKET}/{OUTPUT_KEY}/cross_heatmap.png")

# COMMAND ----------

# === 5. Top off-diagonal pairs + per-feature summary ===
df_flat = df_I.stack().reset_index()
df_flat.columns = ["feature_a", "feature_b", "score"]
df_flat = df_flat[df_flat.feature_a != df_flat.feature_b].copy()
df_flat["pair"] = df_flat.apply(lambda r: tuple(sorted([r.feature_a, r.feature_b])), axis=1)
df_flat = (df_flat.drop_duplicates("pair")
           .sort_values("score", ascending=False)
           .reset_index(drop=True))

print("\n=== Top 20 Feature Interaction Pairs (off-diagonal) ===")
print(df_flat[["feature_a", "feature_b", "score"]].head(20).to_string(index=False))

# Per-feature max off-diagonal score
row_max_off = {}
for ai, fa in enumerate(ALL_FEATURES):
    vals = [I_matrix[ai, bj] for bj in range(n_features) if bj != ai]
    row_max_off[fa] = max(vals)

summary_df = pd.DataFrame({
    "feature":          ALL_FEATURES,
    "type":             (["numerical"] * N_NUM + ["categorical"] * N_CAT),
    "emb_dim":          ([1] * N_NUM + [EMBEDDING_DIM] * N_CAT),
    "max_off_diag":     [row_max_off[f] for f in ALL_FEATURES],
    "mean_interaction": df_I.mean(axis=1).values,
}).sort_values("max_off_diag", ascending=False).reset_index(drop=True)

print("\n=== Per-Feature Cross Interaction (sorted by max off-diagonal) ===")
print(summary_df.to_string(index=False))

NEAR_ZERO_THRESHOLD = summary_df["max_off_diag"].quantile(0.25)
weak = summary_df[summary_df["max_off_diag"] < NEAR_ZERO_THRESHOLD]
print(f"\n=== Weak Cross Signal (bottom 25%, max_off_diag < {NEAR_ZERO_THRESHOLD:.4f}) ===")
print(weak[["feature", "type", "max_off_diag"]].to_string(index=False))
print("\nThese features contribute little to cross interactions — "
      "either redundant or missing an interaction partner.")

# COMMAND ----------

# === 6. Save all results to S3 ===
summary_lines = [
    "=== Phase 1C — Cross Weight Interpretability ===",
    f"trial_000 | {len(cross_weights)} cross layer(s) | input_dim={INPUT_DIM}",
    "",
    "--- Top 20 Interaction Pairs ---",
    df_flat[["feature_a", "feature_b", "score"]].head(20).to_string(index=False),
    "",
    "--- Per-Feature Max Off-Diagonal Score (descending) ---",
    summary_df.to_string(index=False),
    "",
    f"--- Weak Cross Signal (bottom 25%, threshold={NEAR_ZERO_THRESHOLD:.4f}) ---",
    weak[["feature", "type", "max_off_diag"]].to_string(index=False),
]
summary_txt = "\n".join(summary_lines)
print("\n" + summary_txt)

_s3.put_object(
    Bucket=BUCKET,
    Key=f"{OUTPUT_KEY}/cross_summary.txt",
    Body=summary_txt.encode("utf-8"),
)
_s3.put_object(
    Bucket=BUCKET,
    Key=f"{OUTPUT_KEY}/cross_pairs.json",
    Body=json.dumps({
        "n_cross_layers": len(cross_weights),
        "input_dim": INPUT_DIM,
        "top_pairs": df_flat[["feature_a", "feature_b", "score"]].head(30).to_dict(orient="records"),
        "feature_summary": summary_df.to_dict(orient="records"),
    }, indent=2).encode("utf-8"),
    ContentType="application/json",
)
print(f"\nSummary -> s3://{BUCKET}/{OUTPUT_KEY}/cross_summary.txt")
print(f"Pairs   -> s3://{BUCKET}/{OUTPUT_KEY}/cross_pairs.json")
