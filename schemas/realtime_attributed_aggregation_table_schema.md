# Realtime Attributed Aggregation Table Schema

This document specifies the hourly aggregation schemas built from
`ml_shadow.realtime_attributed_event_wide`.

The dimension columns live here, not in the event-grain wide-table schema. The
metric catalog is based on the existing modulo table definition:
`hive_prod.modulo.modulo_dvc_mrg_hly` from
https://github.com/Vungle/schemas/blob/master/datalake/modulo/modulo_dvc_mrg_hly.sql.

---

## 1. Aggregation Tables

Two reviewed dimension families are materialized. They share the same metric
columns in §5, but use different dimension columns.

| table | dimension_family | purpose | primary dimension key |
|---|---|---|---|
| `ml_shadow_feature.realtime_attributed_device_level_hly` | `device_level_v1` | Device / user history features and KVRocks export candidates. | `device_id` |
| `ml_shadow_feature.realtime_attributed_non_device_context_hly` | `non_device_context_v1` | Supply, inventory, auction, creative, experiment, and demand context without device identity. | `context_dim_id` |

Both tables are hourly:

| column | type | role | description |
|---|---|---|---|
| `event_time` | TIMESTAMP | time_key | Hour bucket for the source event time. Equivalent to `hours(source_event_time)`. |
| `ingest_time` | STRING | partition_key | Ingestion date/hour string used for late data and Iceberg partition pruning. |
| `hashid` | STRING | partition_key | Stable hash bucket derived from the primary dimension key. Existing modulo uses this as a partition key. |
| `source_event_count` | BIGINT | audit_metric | Number of wide-table rows contributing to the aggregate row. |
| `first_source_event_time` | TIMESTAMP | audit | Earliest contributing `source_event_time`. |
| `last_source_event_time` | TIMESTAMP | audit | Latest contributing `source_event_time`. |
| `aggregation_version` | STRING | lineage | Version of the aggregation job / metric contract. |

Recommended storage follows the modulo precedent: Iceberg/Parquet, partitioned by
`hours(event_time)`, `ingest_time`, and `hashid`.

---

## 2. Dimension Rules

- Source dimensions only from `realtime_attributed_event_wide` unless this doc explicitly lists a derived value.
- Normalize null dimension values to `__unknown__` before computing `hashid` or `context_dim_id`.
- Do not store raw PII in aggregation tables: `jgr_dev_ifa`, `jgr_dev_ip`, and `jgr_dev_ua` are excluded.
- Bucket high-cardinality text before use: app name, city, device model, carrier, template name, datasci tags, advertiser domains.
- Metric windows must end strictly before the scoring event when used for model features. Current-event labels must not contribute to their own trailing aggregates.
- Aggregation dimensions must not include event/request/bid identifiers or winning-bid identifiers; those remain available only in the event-grain wide table.

---

## 3. `device_level_v1` Dimensions

This table contains the device id and device-related dimensions. Placement,
publisher, app, campaign, creative, RTB, and auction-outcome context belongs in
`non_device_context_v1`.

Grain:
- One row per `(event_time, ingest_time, hashid, device_id, device dimensions...)`.
- `device_id` is sourced from `jgr_lo_id`.
- Rows with null `jgr_lo_id` should be dropped unless an approved anonymous-device policy exists.

| column | type | source / derivation | role | notes |
|---|---|---|---|---|
| `device_id` | STRING | `jgr_lo_id` | primary_dimension | Hashed, non-reversible device identifier. |
| `device_dim_id` | STRING | `sha256(device_id)` | surrogate_key | Stable join/export key. |
| `dev_id_source` | STRING | `jgr_dev_id_source` | dimension | Device-id source. |
| `dev_platform` | STRING | normalized `jgr_dev_os` | dimension | Lowercase platform, e.g. `ios`, `android`. |
| `os_version_major` | STRING | parse major from `jgr_dev_osv` | dimension | Major version only. |
| `os_version_bucket` | STRING | bucketed `jgr_dev_osv` | dimension | Optional finer OS cohort. |
| `dev_make_bucket` | STRING | top-N bucket from `jgr_dev_make` | dimension | Bucket long tail to `other`. |
| `dev_model_bucket` | STRING | top-N bucket from `jgr_dev_model` | dimension | Required bucketing; raw model is too high-cardinality. |
| `dev_type` | LONG | `jgr_dev_devicetype` | dimension | OpenRTB-style device type code. |
| `screen_width_bucket` | STRING | bucketed `jgr_dev_w` | dimension | Coarse screen-width bucket. |
| `screen_height_bucket` | STRING | bucketed `jgr_dev_h` | dimension | Coarse screen-height bucket. |
| `screen_size_bucket` | STRING | derive from `jgr_dev_w`, `jgr_dev_h` | dimension | Coarse small/large/tablet-style cohort. |
| `dev_connection` | LONG | `jgr_dev_connectiontype` | dimension | Connection type code. |
| `dev_connection_detail` | STRING | bucketed `jgr_dev_connection_type_detail` | dimension | Detailed connection label. |
| `dev_carrier_bucket` | STRING | top-N bucket from `jgr_dev_carrier` | dimension | Normalize names and bucket long tail. |
| `dev_language` | STRING | normalized `jgr_dev_language` | dimension | Lowercase language prefix where possible. |
| `device_country_iso2` | STRING | `jgr_dev_country_iso2` | dimension | Device country from resolved device extension. |
| `store_country_iso2` | STRING | `jgr_dev_store_country_iso2` | dimension | App-store country. |
| `geo_country` | STRING | normalized `jgr_geo_country` | dimension | Resolved geo country. |
| `geo_region` | STRING | `jgr_geo_region` | dimension | Region / state. |
| `geo_city_bucket` | STRING | top-N bucket from `jgr_geo_city` | dimension | City must be bucketed. |
| `geo_type` | LONG | `jgr_geo_type` | dimension | Geo source type. |
| `geo_ipservice` | LONG | `jgr_geo_ipservice` | dimension | IP-service provider code. |
| `dnt_flag` | LONG | `jgr_dev_dnt` | privacy_dimension | Do-not-track flag. |
| `lmt_flag` | LONG | `jgr_dev_lmt` | privacy_dimension | Limit-ad-tracking flag. |
| `ccpa_opt_out` | BOOLEAN | `jgr_ccpa_opt_out` | privacy_dimension | Compliance cohort. |
| `coppa_applied` | BOOLEAN | `jgr_coppa_applied` | privacy_dimension | Compliance cohort. |
| `consent_status` | STRING | `jgr_consent_status` | privacy_dimension | Consent status. |
| `consent_source` | STRING | `jgr_consent_source` | privacy_dimension | Consent source. |
| `tcf_result` | INT | `jgr_tcf_result` | privacy_dimension | TCF result. |
| `tcf_cmp_id` | INT | `jgr_tcf_cmp_id` | privacy_dimension | CMP id. |
| `tcf_version` | STRING | `jgr_tcf_ver` | privacy_dimension | TCF version. |
| `tcf_invalid_reason` | INT | `jgr_tcf_invalid_reason` | privacy_dimension | Invalid-TCF reason. |
| `first_party_data_valid` | BOOLEAN | `jgr_is_first_party_data_valid` | privacy_dimension | First-party data validity. |
| `is_anonymous_vpn` | BOOLEAN | `jgr_dev_is_anonymous_vpn` | risk_dimension | Device/IP risk cohort. |
| `is_suspicious_ip` | BOOLEAN | `jgr_dev_is_suspicious_ip` | risk_dimension | Device/IP risk cohort. |
| `battery_level_bucket` | STRING | bucketed `jgr_dev_battery_level` | optional_dimension | Coarse bucket only. |
| `volume_bucket` | STRING | bucketed `jgr_dev_volume` | optional_dimension | Coarse bucket only. |

---

## 4. `non_device_context_v1` Dimensions

This table contains only stable non-device aggregation dimensions. It
intentionally excludes `device_id`, raw device attributes, event identifiers,
bid identifiers, winning-bid fields, campaign/creative ids, and event outcomes.

Grain:
- One row per `(event_time, ingest_time, hashid, context_dim_id, aggregation dimensions...)`.
- `context_dim_id = sha256(concat_ws('|', normalized dimension values in the table order below))`.
- Existing modulo dimensions map as follows: `placement_id`, `placement_type`, `application_id`, and `account_id` live here; modulo `device_id` lives only in `device_level_v1`.

Excluded from this dimension family:
- Event/request/bid identity: `event_id`, `imp_id`, `jgr_transaction_id`, `hbn_bidrequest_id`, `jgr_auction_id`, `jgr_incoming_bid_request_id`, `jgr_dup_key`, `jgr_req_group_id`.
- Bidder/winner/demand identity: `winner_id`, `winning_seat`, `winner_account_id`, `winning_bid_crid`, `winning_bid_cid`, `bidder_id`, `rtb_connection_id`, `rtb_account_id`, `hb_rtb_account_id`.
- Event outcome/status dimensions: `no_serv_reason`, `bid_nsr`, `is_realtime`, `settlement_status`, `filter_result`.
- Creative/campaign/event-rendering dimensions: creative ids, campaign ids, template ids/names, endcard flags, skip/streaming/freeform flags, winning-bid bundle/category fields.
- Per-event counters or mutable serving state: session depth, ordinal view, winner QPS fields, datasci tag blobs/hashes, producer versions.

| column | type | source / derivation | notes |
|---|---|---|---|
| `context_dim_id` | STRING | hash of normalized aggregation dimensions | Stable surrogate key. |
| `source_has_hb` | BOOLEAN | HB join hit / HB columns present | Coarse traffic-path flag, not the bid-request id itself. |
| `supply_traffic_source` | STRING | `jgr_supply_traffic_source` | `sdk`, `hb`, `s2s`. |
| `supply_name` | STRING | `hbn_supply_name` | Supply partner name. |
| `supply_fee_bucket` | STRING | bucketed `hbn_supply_fee` | Coarse bucket; raw rate remains a metric/source field. |
| `account_id` | STRING | `hbn_pub_account_id` | Publisher account id; modulo-compatible alias. |
| `pub_account_id` | STRING | `hbn_pub_account_id` | Publisher account id. |
| `application_id` | STRING | `hbn_pub_app_object_id` | Publisher app ObjectId; modulo-compatible alias. |
| `pub_app_object_id` | STRING | `hbn_pub_app_object_id` | Primary publisher app dimension. |
| `pub_app_id` | STRING | `hbn_pub_app_id` | Publisher app store id. |
| `pub_app_bundle_id` | STRING | `hbn_pub_app_bundle_id` | Publisher app bundle / package. |
| `pub_genre_bucket` | STRING | bucketed `hbn_pub_genre` | Multi-value field; use sorted bucket or MAP primitive. |
| `app_bundle` | STRING | `jgr_app_bundle` | Exchange-independent app bundle. |
| `app_iab_tier1` | STRING | tier-1 rollup from `jgr_app_cat` | Multi-value IAB category rollup. |
| `app_object_id` | STRING | `jgr_app_object_id` | Jaeger app object id. |
| `app_hb_partner` | STRING | `jgr_app_hb_partner` | Third-party HB partner. |
| `app_version_major` | STRING | parse major from `jgr_app_ver` | Keep raw versions out of default keys. |
| `is_header_bidding` | BOOLEAN | `jgr_is_header_bidding` | Header-bidding flag. |
| `is_gam` | BOOLEAN | `jgr_is_gam` | Google Ad Manager flag. |
| `placement_id` | STRING | `jgr_placement_id` | Placement id. |
| `placement_reference_id` | STRING | `jgr_placement_reference_id` | Placement reference id. |
| `placement_type` | STRING | `jgr_placement_type` | Banner / interstitial / rewarded, etc. |
| `ad_size` | STRING | `jgr_ad_size` | MREC / banner / fullscreen, etc. |
| `ad_type` | STRING | `jgr_ad_type` | Ad type. |
| `is_flat_cpm_enabled` | BOOLEAN | `jgr_is_flat_cpm_enabled` | Flat-CPM pricing flag. |
| `flat_cpm_model_type` | STRING | `jgr_flat_cpm_model_type` | TRS / NRG. |
| `is_incentivized` | BOOLEAN | `jgr_is_incentivized` | Rewarded placement flag. |
| `is_max_profit_enabled` | BOOLEAN | `jgr_is_max_profit_enabled` | Max-profit mode flag. |
| `publisher_payout_type` | STRING | `jgr_publisher_payout_type` | Revenue-share / flat-CPM / CPM. |
| `ad_unit_id` | STRING | `jgr_ad_unit_id` | Mediation partner ad-unit id. |
| `geoip_country_code` | STRING | prefer `jgr_dev_country_iso2`, fallback `jgr_geo_country` | Canonical country dimension. |
| `geo_region` | STRING | `jgr_geo_region` | Region / state. |
| `geo_city_bucket` | STRING | top-N bucket from `jgr_geo_city` | City must be bucketed. |
| `geo_type` | LONG | `jgr_geo_type` | Geo source type. |
| `geo_ipservice` | LONG | `jgr_geo_ipservice` | IP-service provider code. |
| `ccpa_opt_out` | BOOLEAN | `jgr_ccpa_opt_out` | Compliance cohort. |
| `coppa_applied` | BOOLEAN | `jgr_coppa_applied` | Compliance cohort. |
| `consent_status` | STRING | `jgr_consent_status` | Consent status. |
| `consent_source` | STRING | `jgr_consent_source` | Consent source. |
| `jaeger_experiment_id` | STRING | normalized key/hash from `jgr_exp_to_bucket` | Stable experiment key only; do not expose arbitrary JSON. |
| `dte_experiment_group` | STRING | `jgr_dte_experiment_group` | DTE experiment group. |
| `ssp_exp_num` | LONG | `jgr_ssp_exp_num` | SSP experiment number. |
| `vxac_exp_id` | STRING | `jgr_vxac_exp_id` | VXAC experiment id. |
| `ml_experiment_id` | LONG | `hbn_experiment_id` | HBP ML experiment id. |
| `recommender_tag` | STRING | `hbn_recommender_tag` | Datasci strategy tag. |
| `qps_type` | STRING | `jgr_qps_type` | QPS experiment type. |
| `qps_exp_name` | STRING | `jgr_qps_exp_name` | QPS experiment name. |
| `is_ad_podding` | BOOLEAN | `jgr_is_ad_podding` | Ad podding flag. |
| `ad_podding_multiplier_bucket` | STRING | bucketed `jgr_ad_podding_multiplier` | Coarse bucket only. |
| `traffic_quality_action` | STRING | `jgr_traffic_quality_action` | Optional traffic-quality action, if applied before auction. |
| `traffic_quality_strategy` | STRING | `jgr_traffic_quality_strategy` | Traffic-quality strategy, if applied before auction. |

---

## 5. Metric Catalog

Metrics are copied from the modulo hourly merge table and generalized so both
dimension families can use the same metric columns.

### 5.1 Distribution metric expansion

For each metric family listed as `distribution`, materialize five columns:

| suffix | type | meaning |
|---|---|---|
| `_sum` | DOUBLE | Sum of observed values. |
| `_count` | BIGINT | Number of non-null observations. |
| `_min` | DOUBLE | Minimum observed value. |
| `_max` | DOUBLE | Maximum observed value. |
| `_squaresum` | DOUBLE | Sum of squared observed values, used for variance/stddev. |

Derived readers can compute `avg = _sum / _count` and
`variance = (_squaresum / _count) - avg * avg`, guarded for zero count.

### 5.2 Distribution Metric Families

| family | generated columns | source / derivation | notes |
|---|---|---|---|
| `min_bid_to_win` | `min_bid_to_win_{sum,count,min,max,squaresum}` | `jgr_min_bid_to_win` | First-price minimum bid to win. |
| `vx_min_bid_to_win` | `vx_min_bid_to_win_{sum,count,min,max,squaresum}` | modulo-compatible VX filter over min bid to win | Exact VX predicate must match the existing modulo job. |
| `second_place_price` | `second_place_price_{sum,count,min,max,squaresum}` | `jgr_second_place_price` | Second-highest auction bid. |
| `edsp_highest_price` | `edsp_highest_price_{sum,count,min,max,squaresum}` | `jgr_edsp_highest_price` | Highest eDSP price in the request. |
| `edsp_highest_price_non_acc` | `edsp_highest_price_non_acc_{sum,count,min,max,squaresum}` | `jgr_edsp_highest_price` filtered to non-Accelerate | Exact non-ACC predicate must match the existing modulo job. |
| `mediation_floor` | `mediation_floor_{sum,count,min,max,squaresum}` | `jgr_mediation_floor` | Mediation floor at impression grain. |
| `mediation_floor_txn` | `mediation_floor_txn_{sum,count,min,max,squaresum}` | transaction-level `jgr_mediation_floor` | Preserve modulo semantics when multiple impressions share a transaction. |
| `min_bid_to_win_med` | `min_bid_to_win_med_{sum,count,min,max,squaresum}` | mediation-filtered `jgr_min_bid_to_win` | Exact mediation predicate must match the existing modulo job. |
| `bid_price` | `bid_price_{sum,count,min,max,squaresum}` | `hbn_hbp_bid_price` | HBP bid price. |
| `settlement_price` | `settlement_price_{sum,count,min,max,squaresum}` | `jgr_settlement_price` | Label-like clearing price; point-in-time care required. |
| `settlement_price_loss` | `settlement_price_loss_{sum,count,min,max,squaresum}` | `jgr_settlement_price` filtered to loss events | Diagnostic/backtest metric. |
| `settlement_price_won` | `settlement_price_won_{sum,count,min,max,squaresum}` | `jgr_settlement_price` filtered to won/delivered events | Diagnostic/backtest metric. |
| `net_revenue` | `net_revenue_{sum,count,min,max,squaresum}` | derived from settlement, supply fee, rev share, and hosting cost | Requires a reviewed revenue formula. |
| `auction_winner_price` | `auction_winner_price_{sum,count,min,max,squaresum}` | `jgr_winning_bid_price` | Label-adjacent auction outcome. |
| `bid_price_moloco` | `bid_price_moloco_{sum,count,min,max,squaresum}` | `hbn_hbp_bid_price` filtered to Moloco bidder | Exact bidder predicate must match existing modulo job. |
| `bid_price_acc` | `bid_price_acc_{sum,count,min,max,squaresum}` | `hbn_acc_bid_price` | Accelerate bid price. |
| `bid_price_all` | `bid_price_all_{sum,count,min,max,squaresum}` | all bid prices from the bid source | Needs bid-grain/all-bid source if wider than served bid. |
| `adv_spend` | `adv_spend_{sum,count,min,max,squaresum}` | derived advertiser spend | Requires reviewed spend formula / billing source. |
| `pub_revenue` | `pub_revenue_{sum,count,min,max,squaresum}` | derived publisher revenue | Requires reviewed payout formula. |
| `unshaded_bid_price` | `unshaded_bid_price_{sum,count,min,max,squaresum}` | `jgr_sr_ocpm` | Unshaded private value / original CPM. |

### 5.3 Count Metric Columns

These columns are materialized exactly as named.

| column | type | source / derivation | notes |
|---|---|---|---|
| `delivery_count` | BIGINT | count rows where `jgr_no_serv_reason = 0` | Delivered impressions. |
| `mediation_loss_count` | BIGINT | hb-notifications / mediation outcome source | Not fully available from the two-topic wide table alone. |
| `mediation_win_count` | BIGINT | hb-notifications / mediation outcome source | Not fully available from the two-topic wide table alone. |
| `mediation_bill_count` | BIGINT | hb-notifications / mediation billing source | Not fully available from the two-topic wide table alone. |
| `event_start_count` | BIGINT | derived TPAT/event-start count | Use point-in-time-safe event source. |
| `mediation_auctions_count` | BIGINT | count mediation auction opportunities | Derive from HB/S2S auction rows. |
| `no_bid_count` | BIGINT | count no-bid events | Align to modulo no-bid predicate. |
| `bid_count` | BIGINT | count bid events | Align to modulo bid predicate. |
| `bid_count_moloco_count` | BIGINT | count Moloco bid events | Existing modulo name is intentionally preserved. |
| `bid_count_acc_count` | BIGINT | count Accelerate bid events | Existing modulo name is intentionally preserved. |
| `sp_at_mediation_floor_count` | BIGINT | count settlement/winner price at mediation floor | Exact modulo predicate required. |
| `hb_bid_count` | DOUBLE | count HB bids | Type preserved from modulo SQL (`DOUBLE`). |

---

## 6. Compatibility With `modulo_dvc_mrg_hly`

The existing modulo table has this core shape:

```sql
event_time TIMESTAMP,
ingest_time STRING,
hashid STRING,
placement_id STRING,
device_id STRING,
placement_type STRING,
application_id STRING,
account_id STRING,
<metric columns>
```

The new split keeps the metric columns compatible but moves dimensions into the
appropriate family:

| modulo column | new location |
|---|---|
| `device_id` | `device_level_v1.device_id` |
| `placement_id` | `non_device_context_v1.placement_id` |
| `placement_type` | `non_device_context_v1.placement_type` |
| `application_id` | `non_device_context_v1.application_id` / `pub_app_object_id` |
| `account_id` | `non_device_context_v1.account_id` / `pub_account_id` |

If exact backwards compatibility is required, a compatibility view can join or
project from the two new tables only for offline analysis. It should not become
the default feature-serving shape because it mixes device identity with
high-cardinality inventory context.
