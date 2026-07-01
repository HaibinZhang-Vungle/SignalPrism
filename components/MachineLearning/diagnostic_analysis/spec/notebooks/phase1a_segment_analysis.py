# Databricks notebook source
# MAGIC %md
# MAGIC # Phase 1A — Segment Analysis
# MAGIC
# MAGIC **Input:** `val_predictions.parquet` from trial_000 (23 features + event_id + prediction/label/residual/abs_error)
# MAGIC
# MAGIC **Outputs:**
# MAGIC 1. Proxy permutation importance (GBT on abs_error ~ features)
# MAGIC 2. CART segment discovery — high-error leaf definitions
# MAGIC 3. Per-segment event_id lists saved to S3 for Phase 1B Trino join

# COMMAND ----------

import boto3, io, json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.tree import DecisionTreeRegressor, export_text
from sklearn.ensemble import GradientBoostingRegressor

BUCKET       = "vungle2-ssp-dev"
ARTIFACT_KEY = "gminor/hp_sweep/floor_optimization/dcnv2/2026-07-01-dcnv2-r2-artifacts/trial_000"
OUTPUT_KEY   = "chenliu/floor_opt_analysis/phase1a/trial_000"

_s3 = boto3.client("s3")

def s3_read_parquet(bucket, key):
    obj = _s3.get_object(Bucket=bucket, Key=key)
    return pd.read_parquet(io.BytesIO(obj["Body"].read()))

val_df = s3_read_parquet(BUCKET, f"{ARTIFACT_KEY}/val_predictions.parquet")
print(f"Loaded val_predictions: {len(val_df):,} rows, {val_df.shape[1]} cols")
print(f"Global mean abs_error : {val_df['abs_error'].mean():.6f}")
print(f"Global median abs_error: {val_df['abs_error'].median():.6f}")

# COMMAND ----------

# === 1. Feature definitions ===
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

val_encoded = val_df[ALL_FEATURES].copy()
for col in CATEGORICAL_FEATURES:
    val_encoded[col] = val_encoded[col].astype("category").cat.codes

X = val_encoded.fillna(-1).values.astype(np.float32)
y_err = val_df["abs_error"].values
print(f"Feature matrix: {X.shape}")

# COMMAND ----------

# === 2. Proxy permutation importance (GBT on abs_error ~ features) ===
gbt = GradientBoostingRegressor(
    n_estimators=200,
    max_depth=4,
    learning_rate=0.05,
    subsample=0.8,
    random_state=42,
    verbose=0,
)
gbt.fit(X, y_err)

importance_df = (pd.DataFrame({
    "feature": ALL_FEATURES,
    "importance": gbt.feature_importances_,
})
.sort_values("importance", ascending=False)
.reset_index(drop=True))

print("\n=== Proxy Permutation Importance (GBT on abs_error) ===")
print(importance_df.to_string(index=False))

fig, ax = plt.subplots(figsize=(10, 7))
ax.barh(importance_df["feature"][::-1], importance_df["importance"][::-1])
ax.set_xlabel("GBT Feature Importance")
ax.set_title("Phase 1A — Proxy Feature Importance\n(GBT predicting abs_error)")
plt.tight_layout()
plt.savefig("/tmp/phase1a_importance.png", dpi=150)
_s3.upload_file("/tmp/phase1a_importance.png", BUCKET, f"{OUTPUT_KEY}/phase1a_importance.png")
print(f"\nImportance plot -> s3://{BUCKET}/{OUTPUT_KEY}/phase1a_importance.png")

# COMMAND ----------

# === 3. CART segment discovery ===
CART_DEPTH               = 4
MIN_LEAF_SAMPLES         = 500
MAE_VS_GLOBAL_THRESHOLD  = 2.0

tree = DecisionTreeRegressor(
    max_depth=CART_DEPTH,
    min_samples_leaf=MIN_LEAF_SAMPLES,
    random_state=42,
)
tree.fit(X, y_err)

val_df = val_df.copy()
val_df["leaf_id"] = tree.apply(X)

leaf_stats = (val_df.groupby("leaf_id")["abs_error"]
              .agg(["mean", "median", "count", "std"])
              .rename(columns={"mean": "mae", "median": "mdae", "count": "n", "std": "std_err"})
              .sort_values("mae", ascending=False))
leaf_stats["mae_vs_global"] = leaf_stats["mae"] / val_df["abs_error"].mean()

print("\n=== CART Leaf Statistics ===")
print(leaf_stats.to_string())

print("\n=== Decision Tree Rules ===")
print(export_text(tree, feature_names=ALL_FEATURES, max_depth=CART_DEPTH))

# COMMAND ----------

# === 4. High-error segments — extract event_ids and save ===
high_error_leaves = leaf_stats[leaf_stats["mae_vs_global"] >= MAE_VS_GLOBAL_THRESHOLD].index.tolist()
print(f"\nHigh-error leaves (mae_vs_global >= {MAE_VS_GLOBAL_THRESHOLD}): {high_error_leaves}")

segment_summary = []
for leaf_id in high_error_leaves:
    seg_df = val_df[val_df["leaf_id"] == leaf_id]
    stats = leaf_stats.loc[leaf_id]

    event_ids = seg_df["event_id"].dropna().tolist()
    _s3.put_object(
        Bucket=BUCKET,
        Key=f"{OUTPUT_KEY}/segments/leaf_{leaf_id}_event_ids.txt",
        Body="\n".join(str(e) for e in event_ids).encode("utf-8"),
    )

    cat_summary = {}
    for col in CATEGORICAL_FEATURES:
        if col in seg_df.columns:
            cat_summary[col] = seg_df[col].value_counts().head(3).to_dict()

    segment_summary.append({
        "leaf_id": int(leaf_id),
        "n": int(stats["n"]),
        "mae": float(stats["mae"]),
        "mae_vs_global": float(stats["mae_vs_global"]),
        "cat_breakdown": cat_summary,
        "event_id_s3": f"s3://{BUCKET}/{OUTPUT_KEY}/segments/leaf_{leaf_id}_event_ids.txt",
    })

    print(f"\n--- Leaf {leaf_id} | n={stats['n']:,.0f} | MAE={stats['mae']:.6f} | {stats['mae_vs_global']:.1f}x global ---")
    for col, vals in cat_summary.items():
        print(f"  {col}: {vals}")

_s3.put_object(
    Bucket=BUCKET,
    Key=f"{OUTPUT_KEY}/segment_summary.json",
    Body=json.dumps(segment_summary, indent=2).encode("utf-8"),
    ContentType="application/json",
)
print(f"\nSegment summary -> s3://{BUCKET}/{OUTPUT_KEY}/segment_summary.json")
print(f"Event ID lists  -> s3://{BUCKET}/{OUTPUT_KEY}/segments/")

# COMMAND ----------

# === 5. Fixed-segment MAE breakdown by categorical ===
print("\n=== Fixed Segment MAE Breakdown ===")
for col in ["ad_type", "country", "placement_type", "supply_name", "device_os"]:
    if col in val_df.columns:
        seg_mae = (val_df.groupby(col)["abs_error"]
                   .agg(["mean", "count"])
                   .rename(columns={"mean": "mae", "count": "n"})
                   .sort_values("mae", ascending=False))
        seg_mae["mae_vs_global"] = seg_mae["mae"] / val_df["abs_error"].mean()
        print(f"\n--- {col} ---")
        print(seg_mae.head(10).to_string())

# COMMAND ----------

# === 6. Multi-dimensional hierarchical drill-down ===
# Greedy top-down: at each level, find the (dim, value) with worst MAE within the
# current subset, branch into top TOP_K candidates, recurse with that dim removed.
# Produces a tree of compound segments saved to S3 for direct reading.

DRILL_DIMS     = ["placement_type", "ad_type", "supply_name", "country", "device_os", "sdk_supply_name"]
MIN_N_DRILL    = 200     # skip groups smaller than this
THRESHOLD_X    = 2.0     # only expand branches worse than Nx global
TOP_K          = 2       # how many branches to expand at each level
MAX_DEPTH      = 4

_global_mae = val_df["abs_error"].mean()

def _drill(df, used_dims, depth, path_label):
    if depth >= MAX_DEPTH:
        return []
    avail = [d for d in DRILL_DIMS if d not in used_dims and d in df.columns]
    candidates = []
    for dim in avail:
        grp = df.groupby(dim)["abs_error"].agg(["mean", "count"])
        grp = grp[grp["count"] >= MIN_N_DRILL]
        if grp.empty:
            continue
        grp["ratio"] = grp["mean"] / _global_mae
        best_val = grp["ratio"].idxmax()
        best_row = grp.loc[best_val]
        candidates.append((float(best_row["ratio"]), dim, best_val, int(best_row["count"])))
    candidates.sort(reverse=True)

    results = []
    for ratio, dim, val, n in candidates[:TOP_K]:
        if ratio < THRESHOLD_X:
            continue
        sub = df[df[dim] == val]
        path = f"{path_label} → {dim}={val}" if path_label else f"{dim}={val}"
        entry = {
            "depth": depth,
            "path": path,
            "dim": dim,
            "val": str(val),
            "n": n,
            "mae": float(sub["abs_error"].mean()),
            "mae_vs_global": round(ratio, 2),
        }
        results.append(entry)
        results.extend(_drill(sub, used_dims | {dim}, depth + 1, path))
    return results

drill_results = _drill(val_df, set(), 0, "")

# Print hierarchical view
lines = [
    "=== Multi-Dimensional Hierarchical Drill-Down ===",
    f"Global MAE : {_global_mae:.6f}",
    f"Config     : MIN_N={MIN_N_DRILL}, threshold={THRESHOLD_X}x, top_k={TOP_K}, max_depth={MAX_DEPTH}",
    "",
]
for r in drill_results:
    indent = "  " * r["depth"]
    arrow  = "→ " if r["depth"] > 0 else ""
    lines.append(
        f"{indent}{arrow}{r['dim']}={r['val']}"
        f"  |  n={r['n']:,}  |  MAE={r['mae']:.6f}  |  {r['mae_vs_global']:.1f}x global"
    )

summary_txt = "\n".join(lines)
print(summary_txt)

# Save both formats to S3
_s3.put_object(
    Bucket=BUCKET,
    Key=f"{OUTPUT_KEY}/multidim_drill_down_summary.txt",
    Body=summary_txt.encode("utf-8"),
)
_s3.put_object(
    Bucket=BUCKET,
    Key=f"{OUTPUT_KEY}/multidim_drill_down.json",
    Body=json.dumps({
        "global_mae": _global_mae,
        "config": {"min_n": MIN_N_DRILL, "threshold_x": THRESHOLD_X, "top_k": TOP_K, "max_depth": MAX_DEPTH},
        "segments": drill_results,
    }, indent=2).encode("utf-8"),
    ContentType="application/json",
)
print(f"\nDrill-down summary → s3://{BUCKET}/{OUTPUT_KEY}/multidim_drill_down_summary.txt")
print(f"Drill-down JSON    → s3://{BUCKET}/{OUTPUT_KEY}/multidim_drill_down.json")
