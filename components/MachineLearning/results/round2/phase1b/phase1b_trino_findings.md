# Phase 1B — Trino Findings (2026-06-30)

**Table**: `raw.coba2.ex_jaeger_transaction`  
**Field mapping**:
- `supply_name` → `app.ext.hb_partner`
- `country` → `device.ext.country_iso_2`
- `device_os` → `device.os`
- `placement_type` → `placements[1].placement_type`
- `edsp` → `placement_serve_results[1].edsp_highest_price`
- `bid_spread` → `placement_serve_results[1].edsp_highest_price - placement_serve_results[1].second_place_price`
- `dsp_count` → `cardinality(placement_serve_results[1].rtbconnections)`

**Target segment**: `placement_type=appopen, hb_partner=admob, country_iso_2=US, device_os=android`  
**Date range**: dt=2026-06-28, hr IN ('00','01','02','03') — 4-hour sample to avoid Trino timeout

---

## Query 1 — Price Shape: Target vs Controls

**Purpose**: Confirm whether target segment has structurally different price distribution vs controls.
Validates whether `var/std` features would provide signal the model currently lacks.

```sql
WITH base AS (
  SELECT
    CASE
      WHEN app.ext.hb_partner = 'admob' AND device.ext.country_iso_2 = 'US' AND lower(device.os) = 'android' THEN 'target'
      WHEN app.ext.hb_partner = 'admob' AND device.ext.country_iso_2 = 'US' AND lower(device.os) != 'android' THEN 'ctrl_non_android'
      WHEN app.ext.hb_partner != 'admob' AND device.ext.country_iso_2 = 'US' AND lower(device.os) = 'android' THEN 'ctrl_non_admob'
    END AS grp,
    placement_serve_results[1].edsp_highest_price AS edsp,
    placement_serve_results[1].second_place_price  AS p2,
    cardinality(placement_serve_results[1].rtbconnections) AS dsp_count,
    mediation_floor,
    overwritten_floor
  FROM raw.coba2.ex_jaeger_transaction
  WHERE dt = '2026-06-28'
    AND hr IN ('00','01','02','03')
    AND placements[1].placement_type = 'appopen'
)
SELECT
  grp,
  count(*) AS n,
  round(avg(edsp), 3)                          AS mean_edsp,
  round(approx_percentile(edsp, 0.25), 3)      AS p25_edsp,
  round(approx_percentile(edsp, 0.5), 3)       AS p50_edsp,
  round(approx_percentile(edsp, 0.75), 3)      AS p75_edsp,
  round(approx_percentile(edsp, 0.9), 3)       AS p90_edsp,
  round(stddev(edsp), 3)                       AS std_edsp,
  round(avg(edsp - p2), 3)                     AS mean_spread,
  round(approx_percentile(edsp - p2, 0.9), 3) AS p90_spread,
  round(avg(CAST(dsp_count AS double)), 2)     AS mean_dsp_count,
  round(avg(mediation_floor), 3)               AS mean_med_floor,
  round(sum(CASE WHEN overwritten_floor IS NOT NULL THEN 1.0 ELSE 0 END)/count(*), 4) AS overwrite_rate
FROM base
WHERE grp IS NOT NULL
GROUP BY 1
ORDER BY 1
```

### Results

| grp | n | mean | p25 | p50 | p75 | p90 | std | spread | p90_spread | dsp_count | med_floor | overwrite_rate |
|-----|---|------|-----|-----|-----|-----|-----|--------|------------|-----------|-----------|----------------|
| target | 3,022,185 | 31.799 | 4.118 | 12.573 | 31.273 | 75.712 | 66.048 | 2.729 | 2.778 | 79.02 | 1.508 | 0.1 |
| ctrl_non_android | 1,773,319 | 15.076 | 3.219 | 5.954 | 13.095 | 31.103 | 44.027 | 1.575 | 2.957 | 82.94 | 1.933 | 0.1 |
| ctrl_non_admob | 2,834,520 | 23.291 | 4.803 | 11.216 | 22.876 | 49.942 | 50.647 | 7.932 | 18.484 | 47.91 | 0.207 | 0.1 |

### Analysis

**Finding A — Target has the widest price distribution:**
- Target std=66.0 vs ctrl_non_android=44.0 (+50%) and ctrl_non_admob=50.6 (+30%)
- Target IQR-range (p75−p25) = 27.2 vs ctrl_non_android = 9.9 → **2.7× wider**
- Target IQR ratio = (31.3−4.1)/12.6 = **2.16** — extreme right skew
- Model uses a single `avg_hbedsp_highest_price_7d ≈ $31.8` but actual impressions span $4–$75+

**Finding B — Floor setting is a percentile problem, not a mean problem:**
- Target p90/p50 = 75.7/12.6 = 6.0× (ctrl_non_android = 5.2×)
- Setting floor at the 7d avg misses both ends: too high for $4 impressions, too low for $75 impressions
- `var_hbedsp_highest_price_{type}7d` tells the model which contexts require percentile-aware floor strategy

**Finding C — Bid spread is structurally tight in target:**
- Target mean_spread = 2.7 vs ctrl_non_admob = 7.9
- Despite similar DSP counts (79 vs 48), admob/android/US appopen auctions have tightly clustered bids
- Many DSPs compete near the winner price → floor precision matters more than in ctrl_non_admob

**Finding D — mediation_floor explains p25 floor:**
- Target med_floor=1.5 filters out very low bids → p25=$4.1 (not $0)
- ctrl_non_admob med_floor=0.2 → slightly lower p25=$4.8 but much lower mean (different demand curve)

**Validated feature**: `var_hbedsp_highest_price_{placement_type}7d` — already in pipeline 102 unused cols.
**Future candidate**: `p75_hbedsp_highest_price_7d` (IQR ratio) — needs new pipeline; validate var first.

---

## Query 2 — Device Model Breakdown Within Target

**Purpose**: Find whether a device dimension can stratify the target segment's price level,
giving the model a new axis to differentiate impressions it currently treats as homogeneous.

```sql
SELECT
  CASE
    WHEN device.model IS NULL OR trim(device.model) = '' THEN '__null__'
    ELSE lower(trim(device.model))
  END AS dev_model,
  device.make AS dev_make,
  count(*) AS n,
  round(avg(placement_serve_results[1].edsp_highest_price), 3)              AS mean_edsp,
  round(approx_percentile(placement_serve_results[1].edsp_highest_price, 0.25), 3) AS p25_edsp,
  round(approx_percentile(placement_serve_results[1].edsp_highest_price, 0.75), 3) AS p75_edsp,
  round(approx_percentile(placement_serve_results[1].edsp_highest_price, 0.9), 3)  AS p90_edsp,
  round(stddev(placement_serve_results[1].edsp_highest_price), 3)           AS std_edsp
FROM raw.coba2.ex_jaeger_transaction
WHERE dt = '2026-06-28'
  AND hr IN ('00','01','02','03')
  AND placements[1].placement_type = 'appopen'
  AND device.ext.country_iso_2 = 'US'
  AND lower(device.os) = 'android'
  AND app.ext.hb_partner = 'admob'
GROUP BY 1, 2
ORDER BY n DESC
LIMIT 15
```

### Results

| dev_model | dev_make | n | mean_edsp | p25 | p75 | p90 | std |
|-----------|----------|---|-----------|-----|-----|-----|-----|
| sm-a166u | samsung | 192,915 | 32.658 | 5.277 | 32.630 | 75.655 | 65.461 |
| sm-a156u | samsung | 153,172 | 30.462 | 4.663 | 31.050 | 73.348 | 54.790 |
| moto g - 2025 | motorola | 137,526 | 29.741 | 4.673 | 30.078 | 69.690 | 59.224 |
| moto g 5g - 2024 | motorola | 103,982 | 27.904 | 4.866 | 27.775 | 64.193 | 51.455 |
| sm-a146u | samsung | 68,089 | 32.117 | 4.927 | 32.615 | 78.797 | 55.687 |
| moto g play - 2024 | motorola | 61,588 | 28.145 | 3.732 | 27.429 | 63.839 | 64.284 |
| sm-s938u | samsung | 59,589 | 41.041 | 7.009 | 42.264 | 97.706 | 74.572 |
| sm-a176u | samsung | 55,665 | 36.775 | 5.986 | 37.270 | 80.262 | 82.087 |
| sm-s928u | samsung | 49,933 | 42.662 | 6.864 | 43.219 | 102.733 | 74.801 |
| sm-s921u | samsung | 44,647 | 38.472 | 5.483 | 38.471 | 95.917 | 71.025 |
| sm-s931u | samsung | 44,042 | 36.752 | 4.808 | 37.205 | 91.183 | 68.142 |
| sm-s166v | samsung | 42,429 | 34.896 | 5.012 | 34.969 | 78.143 | 72.910 |
| sm-s721u | samsung | 41,152 | 37.021 | 4.873 | 37.765 | 92.173 | 67.347 |
| sm-s156v | samsung | 40,526 | 31.457 | 3.895 | 30.336 | 74.615 | 62.781 |
| sm-s731u | samsung | 38,228 | 40.813 | 5.742 | 41.015 | 98.287 | 83.594 |

### Analysis

**Finding A — Three stable device tiers with 1.5× price range:**

| Tier | Models | mean_edsp | p90_edsp | CV (std/mean) |
|------|--------|-----------|----------|---------------|
| Samsung Flagship (S-series) | sm-s9x8u, sm-s9x1u, sm-s7x1u | $37–43 | $92–103 | 1.75–2.05 |
| Samsung Mid-range (A-series) | sm-a1x6u, sm-a1x6v | $31–37 | $73–81 | 1.70–2.23 |
| Motorola Budget (G-series) | moto g 20xx | $28–30 | $64–70 | 1.85–2.29 |

- Flagship vs budget p90: $103 vs $70 = **1.47×**; mean: $41 vs $29 = **1.41×**
- Gap is consistent across all models within each tier — not outlier-driven

**Finding B — Device tier affects high-price tail, not low-price floor:**
- p25 range across all models: $3.7–7.0 — nearly identical
- p75 range: $27 (budget) to $43 (flagship) — **1.6× gap**
- p90 range: $64 (budget) to $103 (flagship) — **1.6× gap**
- Device tier is most predictive exactly in the high-CPM region driving 88.6% of error (Phase 1A)

**Finding C — Budget devices have higher price randomness:**
- Motorola CV ≈ 2.0–2.3 vs Samsung Flagship CV ≈ 1.75–2.05
- Flagship auctions are more consistently premium; budget auctions have more variance relative to their mean
- Interaction `device_tier × var_price` may further improve predictions

**Finding D — Coverage:**
- Top-15 models cover ~1.1M of 3.0M target impressions in 4 hours (~37%)
- Long tail is real; top-N=20 bucket + "other" would cover ~50%+ and capture the tier signal

**Validated feature candidates:**

| Feature | Mechanism | Engineering cost |
|---------|-----------|-----------------|
| `avg_hbedsp_highest_price_by_device_make_7d` | Trailing 7d mean grouped by `device.make` → continuous price-level signal per make; mirrors existing `avg_hbedsp_highest_price_{placement_type}7d` pattern | Low — new groupby in existing pipeline |
| `device_make` (categorical) | Direct embedding for Samsung/Motorola/Apple/other | Low — add `device.make` field |
| `android_osv_major` | Parse `device.osv` → major int (12/13/14); newer = premium proxy | Low — string split |
| `device_model_bucket` top-N | Finer granularity; S-series vs A-series within Samsung | Medium — higher cardinality |

**Recommended path**: Add `avg_hbedsp_highest_price_by_device_make_7d` + `device_make` categorical first
(low cost, same trailing-agg pattern as placement_type). Add `device_model_bucket` only if make-level
signal is insufficient after Round 2 trial.

---

## Feature Recommendations for Round 2

| Priority | Feature | Validation source | Engineering | Fills gap |
|----------|---------|-------------------|-------------|-----------|
| 🥇 | `var_hbedsp_highest_price_{placement_type}7d` | Query 1: target std=66 >> controls; Phase 1C: cross pivot needs volatility partner | **Zero — pipeline 102 col** | Price distribution shape |
| 🥈 | `avg_hbedsp_highest_price_by_device_make_7d` | Query 2: 1.41× mean gap flagship vs budget | Low — new groupby | Device-tier price level |
| 🥉 | `device_make` categorical | Query 2: same 1.41× gap | Low — add field | Device-tier embedding |
| 4 | `android_osv_major` | Query 2: implied by tier structure | Low — parse osv | Device recency proxy |
| 5 | `p75_hbedsp_highest_price_7d` (IQR ratio) | Query 1: IQR ratio=2.16, strong right skew | Medium — new pipeline agg | Quantile floor anchor |
