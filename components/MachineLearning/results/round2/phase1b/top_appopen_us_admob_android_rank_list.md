# Phase 1B Rank List: appopen -> US -> admob -> android

Target segment from Phase 1A multidim drill-down:

| path | n | MAE | MAE vs global |
|---|---:|---:|---:|
| `placement_type=appopen -> country=US -> supply_name=admob -> device_os=android` | 203 | 0.012180 | 33.81x |

## Data Contract

Phase 1B still reads `val_predictions` for model-error fields:

- `event_id`, `imp_id`
- `abs_error`, `residual`, `prediction`, `label`
- existing segment fields: `placement_type`, `country`, `supply_name`, `device_os`

The wide table supplies candidate explanatory fields after joining:

```sql
ON v.event_id = w.event_id
AND v.imp_id = w.imp_id
```

Within this top segment, rank candidate fields by whether they split high-error and low-error rows more finely.

Recommended comparison:

- high error: top 33% `abs_error` inside this segment
- low/control: bottom 50% `abs_error` inside this segment
- peer control: `placement_type=appopen AND country=US AND supply_name=admob AND device_os <> 'android'`

Top 33% is preferable to top 10% here because the segment has only 203 rows.

## Candidate Rank List

| rank | candidate | source column(s) | why it belongs | test metric | model form if validated |
|---:|---|---|---|---|---|
| 1 | device model bucket | `jgr_dev_model`, optionally `jgr_dev_make`, `jgr_dev_osv` | User hypothesis: within appopen/US/admob/android, some Android device models may carry higher residual due to SDK/render/performance or demand mix. Schema explicitly says raw model must be bucketed to `dev_model_bucket`. | JS divergence by bucket, high-error rate lift, min_n >= 10 | top-N `dev_model_bucket`; trailing `appopen_admob_android_model_mae_7d` only if point-in-time safe |
| 2 | appopen ad unit / placement identity | `jgr_ad_unit_id`, `jgr_placement_id`, `jgr_placement_reference_id` | Top segment may actually be a few ad units or placements, not all appopen/admob/android traffic. | high-error concentration, entropy, lift over peer control | top-N ad-unit bucket; placement-level trailing price/error aggregates |
| 3 | auction timeout / mediation timeout | `jgr_auction_timeout`, `hbn_mediation_tmax` | Appopen timeout behavior can differ from ordinary video flow and can create missing context for prediction. | KS/Wasserstein, p90 high vs low | timeout bucket; trailing timeout rate by appopen/supply/os |
| 4 | floor lifecycle position | `jgr_bid_floor`, `jgr_mediation_floor`, `hbn_bidrequest_imp_bidfloor`, `jgr_overwritten_floor` | Appopen/admob may be sensitive to mediation-request floor and override behavior. | JS/KS on floor ratios, overwrite flag lift | floor-to-price quantile ratios; `has_overwritten_floor`; mediation floor gap |
| 5 | price / auction shape | `jgr_edsp_highest_price`, `jgr_second_place_price`, `jgr_third_place_price`, `jgr_min_bid_to_win`, `jgr_bid_dsp_size` | If high residual rows have wider bid spread or fewer bidders, the current averages are too smooth. | KS, p90 ratio, high-error lift by spread bucket | top-bid spread, top/second ratio, bidder-count bucket, trailing p90/std/cv |
| 6 | supply economics | `hbn_supply_fee`, `jgr_app_rev_share`, `jgr_app_hosting_cost`, `jgr_publisher_payout_type`, `jgr_flat_cpm_model_type` | Same supply and OS may still differ by fee/rev-share/payout config. | effect size, bucket lift | supply-fee bucket, payout-type interaction, trailing app/supply aggregates |
| 7 | connection / device runtime context | `jgr_dev_connectiontype`, `jgr_dev_connection_type_detail`, `jgr_dev_w`, `jgr_dev_h` | Device model signal may proxy network/device capability; these help separate the root cause. | JS/KS, interaction with device model | connection bucket, screen-size bucket, model x connection interaction |
| 8 | creative/render context | `jgr_ad_size`, `jgr_has_endcard`, `jgr_has_skip_button`, `jgr_is_streaming_video`, `jgr_freeform_type`, `jgr_template_name` | Appopen video residual may vary by creative/render mode. Some fields may be too close to serving outcome and need review. | high-error lift by encoded category | top-N creative/render buckets if point-in-time safe |

## Leakage Rules

Do not directly add current-impression outcome fields:

- `jgr_settlement_price`
- `jgr_winning_bid_price`
- `jgr_winner_predicted_nr`
- `jgr_settlement_status`
- `jgr_no_serv_reason`
- `jgr_is_realtime`

These can be used only as diagnostics or as trailing aggregates with a window ending before the current impression.

## First SQL Shape

```sql
WITH seg AS (
  SELECT
    v.event_id,
    v.imp_id,
    v.abs_error,
    v.residual,
    w.jgr_dev_model,
    w.jgr_dev_make,
    w.jgr_dev_osv,
    w.jgr_ad_unit_id,
    w.jgr_auction_timeout,
    w.hbn_mediation_tmax,
    w.jgr_bid_floor,
    w.jgr_mediation_floor,
    w.hbn_bidrequest_imp_bidfloor,
    w.jgr_edsp_highest_price,
    w.jgr_second_place_price,
    w.jgr_bid_dsp_size
  FROM phase1a_val_predictions v
  JOIN ml_shadow.realtime_attributed_event_wide w
    ON v.event_id = w.event_id
   AND v.imp_id = w.imp_id
  WHERE v.placement_type = 'appopen'
    AND v.country = 'US'
    AND v.supply_name = 'admob'
    AND v.device_os = 'android'
),
thresholds AS (
  SELECT
    approx_percentile(abs_error, 0.67) AS high_cut,
    approx_percentile(abs_error, 0.50) AS low_cut
  FROM seg
)
SELECT
  CASE
    WHEN jgr_dev_model IS NULL THEN '__null__'
    ELSE jgr_dev_model
  END AS dev_model_raw,
  count(*) AS n,
  avg(abs_error) AS mae,
  avg(residual) AS residual_mean,
  sum(CASE WHEN abs_error >= high_cut THEN 1 ELSE 0 END) * 1.0 / count(*) AS high_error_rate
FROM seg
CROSS JOIN thresholds
GROUP BY 1
HAVING count(*) >= 10
ORDER BY high_error_rate DESC, mae DESC;
```

If raw `jgr_dev_model` is too sparse, bucket to top-N models within this segment and group the rest as `other`.
