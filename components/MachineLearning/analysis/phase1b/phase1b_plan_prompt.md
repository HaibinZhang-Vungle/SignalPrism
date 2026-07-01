# Phase 1B Plan Prompt

You are running Phase 1B for floor optimization residual analysis.

Goal: Focus only on the Phase 1A top drill-down segment:

```text
placement_type = appopen
country = US
supply_name = admob
device_os = android
n = 203
MAE = 0.012180
MAE vs global = 33.81x
```

Do not broaden the analysis to all Phase 1A segments yet. The purpose is to find whether fields in `ml_shadow.realtime_attributed_event_wide` can split this high-error pocket into more specific subgroups and suggest new feature dimensions.

Use two separate tracks:

1. Track A: no `val_predictions` join required.
2. Track B: requires joining `val_predictions` to the wide table.

Keep these tracks separate in outputs and conclusions.

---

## Inputs

Wide table:

```text
ml_shadow.realtime_attributed_event_wide
grain: one row per (event_id, imp_id)
```

Phase 1A validation predictions:

```text
val_predictions
required columns:
event_id, imp_id, abs_error, residual, prediction, label,
placement_type, country, supply_name, device_os
```

Schema reference:

```text
/Users/cliu/.superset/worktrees/0b91a1f9-b62a-472c-ba93-2f7b0011045e/tall-eye/schemas/realtime_attributed_wide_table_schema.md
```

Important schema constraint:

`realtime_attributed_event_wide` keeps the served/winning HB row only. It does not contain all losing bidders, so do not claim full bidder-landscape coverage.

---

## Track A: No val_predictions Join

Purpose: Check whether the target business segment has unusual wide-table distributions versus peer/control traffic, without using model errors.

This track can answer:

```text
Is appopen + US + admob + android structurally different from comparable appopen/admob traffic?
Which wide-table fields are good candidates before looking at residuals?
```

### A1. Define target and controls directly in wide table

Target:

```sql
w.jgr_placement_type = 'appopen'
AND w.jgr_geo_country = 'US'
AND w.hbn_supply_name = 'admob'
AND lower(w.jgr_dev_os) = 'android'
```

Controls:

1. Same appopen + US + admob, non-Android:

```sql
w.jgr_placement_type = 'appopen'
AND w.jgr_geo_country = 'US'
AND w.hbn_supply_name = 'admob'
AND lower(w.jgr_dev_os) <> 'android'
```

2. Same appopen + US + Android, non-admob:

```sql
w.jgr_placement_type = 'appopen'
AND w.jgr_geo_country = 'US'
AND lower(w.jgr_dev_os) = 'android'
AND coalesce(w.hbn_supply_name, '__null__') <> 'admob'
```

3. Appopen + US overall:

```sql
w.jgr_placement_type = 'appopen'
AND w.jgr_geo_country = 'US'
```

If `jgr_geo_country` is not populated consistently, try `jgr_dev_country_iso2 = 'US'`.

### A2. Candidate fields to scan

Prioritize fields that are plausible for appopen/admob/android and are not current-impression outcome labels.

Device/context:

- `jgr_dev_model` -> bucket to top-N `dev_model_bucket`; raw model is high cardinality.
- `jgr_dev_make`
- `jgr_dev_osv` -> derive OS major version.
- `jgr_dev_connectiontype`
- `jgr_dev_connection_type_detail`
- `jgr_dev_w`, `jgr_dev_h`

Appopen/ad unit/supply:

- `jgr_ad_unit_id`
- `jgr_placement_id`
- `jgr_placement_reference_id`
- `jgr_ad_size`
- `jgr_ad_type`
- `hbn_supply_fee`
- `jgr_publisher_payout_type`
- `jgr_flat_cpm_model_type`
- `jgr_is_header_bidding`
- `jgr_is_gam`

Timing/floor:

- `jgr_auction_timeout`
- `hbn_mediation_tmax`
- `jgr_bid_floor`
- `jgr_mediation_floor`
- `hbn_bidrequest_imp_bidfloor`
- `jgr_overwritten_floor`

Auction/price shape:

- `jgr_edsp_highest_price`
- `jgr_second_place_price`
- `jgr_third_place_price`
- `jgr_min_bid_to_win`
- `jgr_bid_dsp_size`
- `hbn_adx_bid_price`
- `hbn_hbp_bid_price`
- `hbn_bflat_bid_price`
- `hbn_acc_bid_price`

Derived fields:

- `top_bid_spread = jgr_edsp_highest_price - jgr_second_place_price`
- `top_to_second_ratio = jgr_edsp_highest_price / nullif(jgr_second_place_price, 0)`
- `floor_to_highest_bid = jgr_bid_floor / nullif(jgr_edsp_highest_price, 0)`
- `floor_to_mediation_floor = jgr_bid_floor / nullif(jgr_mediation_floor, 0)`
- `has_overwritten_floor = jgr_overwritten_floor is not null`
- `screen_area = jgr_dev_w * jgr_dev_h`

### A3. Metrics for Track A

For each candidate field comparing target vs each control:

Categorical:

- target share by value
- control share by value
- share ratio
- JS divergence
- PSI
- target n by value
- min_n threshold, recommended `min_n >= 10`

Continuous:

- missing rate target/control
- mean, median, p75, p90 target/control
- KS statistic
- Wasserstein distance
- p90 ratio
- median ratio

Output a ranked table:

```text
track = no_join
control_name
candidate_field
field_type
n_target
n_control
js_or_ks
effect_size
top_split
interpretation
candidate_feature_if_validated
```

### A4. Expected Track A interpretation

If `jgr_dev_model` has high JS divergence or concentrated target share:

```text
Device model can split the top appopen/admob/android segment.
Candidate feature: top-N dev_model_bucket plus appopen/admob/android/model trailing aggregates.
```

If timeout fields differ:

```text
Appopen/admob/android errors may reflect auction timing or mediation TTL behavior.
Candidate feature: appopen_supply_os_timeout_bucket and trailing timeout-rate features.
```

If floor/price ratios differ:

```text
The segment may need floor-positioning features.
Candidate feature: floor_to_highest_bid, floor_to_second_bid, floor_to_mediation_floor, trailing floor-position quantiles.
```

---

## Track B: Requires val_predictions Join

Purpose: Within the exact Phase 1A target segment, determine which wide-table fields separate high-error rows from low-error rows.

This track can answer:

```text
Given this segment is already high-error, which wide-table fields explain the residual variation?
Which candidates should enter the model ablation rank list?
```

### B1. Join contract

Join:

```sql
FROM val_predictions v
JOIN ml_shadow.realtime_attributed_event_wide w
  ON v.event_id = w.event_id
 AND v.imp_id = w.imp_id
```

Target filter should come from `v` first if the Phase 1A segment was built on `val_predictions`:

```sql
WHERE v.placement_type = 'appopen'
  AND v.country = 'US'
  AND v.supply_name = 'admob'
  AND lower(v.device_os) = 'android'
```

Also output wide-table values for audit:

```text
w.jgr_placement_type, w.jgr_geo_country, w.hbn_supply_name, w.jgr_dev_os
```

Check whether wide-table dimensions match `v` dimensions. Report mismatch rate.

### B2. Define high/low error groups

Because target segment has only `n=203`, avoid top 10% as the only definition.

Recommended:

```text
high_error = abs_error >= p67 within target segment
low_error = abs_error <= p50 within target segment
```

Also report sensitivity:

```text
high_error_p80 vs low_error_p50
```

### B3. Candidate fields

Use the same candidate fields as Track A, but rank by high-vs-low error separation.

Add existing model-residual fields from `v` for auditing:

- `abs_error`
- `residual`
- `prediction`
- `label`

Do not use these as candidate model features; they are for ranking/error analysis only.

### B4. Metrics for Track B

For categorical candidates:

- high share by value
- low share by value
- high_error_rate by value
- value-level MAE
- residual_mean by value
- JS divergence high vs low
- lift = high_error_rate(value) / high_error_rate(segment)
- min_n threshold: `n >= 10`

For continuous candidates:

- high vs low missing rate
- high vs low median/p75/p90
- KS statistic
- Wasserstein distance
- correlation with `abs_error`
- correlation with signed `residual`
- binned high_error_rate

Output a ranked table:

```text
track = join_required
candidate_field
field_type
n
high_n
low_n
js_or_ks
effect_size
residual_direction
best_split
candidate_feature_if_validated
leakage_status
```

### B5. Device model first-pass

Run `jgr_dev_model` first.

Steps:

1. Normalize raw model:

```text
lowercase
trim
map null/empty to __null__
top-N by target segment count, e.g. N=10
everything else -> other
```

2. Produce table:

```text
dev_model_bucket
n
mae
p90_abs_error
residual_mean
high_error_rate
share_of_segment
```

3. Rank buckets by:

```text
high_error_rate DESC
then n DESC
then p90_abs_error DESC
```

Interpretation rules:

- If a few model buckets have high lift and enough n, add `dev_model_bucket` to rank list.
- If raw model is too sparse but make/OS major is stable, use `jgr_dev_make + os_major`.
- If model only proxies screen size or connection, prefer lower-cardinality derived features.

### B6. Leakage rules

Do not directly add current-impression outcome fields:

- `jgr_settlement_price`
- `jgr_winning_bid_price`
- `jgr_winner_predicted_nr`
- `jgr_settlement_status`
- `jgr_no_serv_reason`
- `jgr_is_realtime`

They may be used only for diagnostics or trailing aggregates with strict point-in-time windows.

---

## Final Output

Write three artifacts:

1. `track_a_no_join_distribution_rank.csv`

Columns:

```text
control_name,candidate_field,field_type,n_target,n_control,metric_name,metric_value,effect_size,top_split,interpretation,candidate_feature
```

2. `track_b_join_high_low_rank.csv`

Columns:

```text
candidate_field,field_type,n,high_n,low_n,metric_name,metric_value,effect_size,residual_direction,best_split,candidate_feature,leakage_status
```

3. `phase1b_top_appopen_us_admob_android_summary.md`

Must include:

- target segment definition and n
- whether wide-table join matched `event_id, imp_id`
- top Track A no-join findings
- top Track B join-required findings
- whether `jgr_dev_model` is useful
- recommended feature ablation list
- fields rejected due to leakage or instability

Keep conclusion scoped:

```text
For the top appopen -> US -> admob -> android pocket, Phase 1B suggests candidate fields that split residual risk more finely. This is a proposal for ablation, not proof until the model is retrained or evaluated offline.
```
