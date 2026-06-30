# Realtime Attributed Wide-Table Schema

This document specifies `ml_shadow.realtime_attributed_event_wide`, the impression-grain wide table formed by joining the `ex-jaeger-transaction` and `hb-transactions` Kafka topics. It is the foundational data contract for the Chapter 7 end-to-end MLOps demo (`07_end_to_end_mlops_wide_table_demo.md`): the capability scanner (TRD §7.7 Step 1) and the Feature Workbench UI Capability Map (TRD §7.10.1) read column-level metadata from this spec.

The schema is designed to be machine-readable. Every field row carries enough metadata (`semantic_type`, `null_semantics`, `feature_suitability`, `enum_ref`) to be ingested directly into the TRD `feature_capability` object (§7.3.1) without a second pass.

---

## 1. Sources

| Source | Topic | Producer | Version | Grain |
|---|---|---|---|---|
| Jaeger | `ex-jaeger-transaction` | jaeger | v1.2 | One row per ADX transaction; `placement_serve_results[]` is a nested array (one element per served/attempted impression). |
| HB | `hb-transactions` | hbp | v1.2 | One row per HB/S2S bid through the HBP. |

Enumerated value sources:
- `ex-jaeger-transaction/EnumeratedList.md` — `no_serv_reason`, `filter_result`, `ec_type`, `sr_at`, `flat_cpm_model_type`, IAB `cat`, TCF, floor lifecycle.
- `hb-notifications/EnumeratedList.md` — `loss_reason`, `notification_type`, `phase`, `winner_type`.

---

## 2. Grain and Join

### 2.1 Wide-table grain

**One row per `(ad_event_id, imp_id)`** — i.e. one served or attempted impression. This is the most natural grain for floor-model / feature-engineering work (TRD §7.13): each impression is one prediction opportunity.

### 2.2 Building the row

1. **Explode Jaeger.** `ex-jaeger-transaction` is one row per ADX transaction with a `placement_serve_results[]` array. Explode it so each output row is one placement serve result. Within each exploded element, the impression keys are:
   - `placement_serve_results[].ad_event_id`
   - `placement_serve_results[].imp_id`

   Transaction-level Jaeger fields (device, geo, floors, experiment, privacy, timing) are carried down onto every exploded row.

2. **Key HB to the served bid.** `hb-transactions` is keyed on top-level `event_id` + `bidrequest_imp_id`. The wide table keeps the **served / winning** bid row (the bid that maps to Jaeger's `winning_bid`), not every losing bidder. Losing-bidder analysis is out of scope for this table (see §7).

3. **Join.**
   ```sql
   FROM jaeger_exploded j
   LEFT JOIN hb h
     ON  j.ad_event_id = h.event_id
     AND j.imp_id       = h.bidrequest_imp_id
   ```
   `LEFT JOIN` from Jaeger: not all Jaeger traffic is HB/S2S (`supply_traffic_source in (sdk, hb, s2s)`), so HB columns are null for SDK-direct traffic. `hbn_*` null therefore means "not an HB/S2S impression," which is a meaningful signal, not missing data.

### 2.3 Join-rate expectation

The join is expected to hit only on HB/S2S traffic. The capability scanner should record `attribution_hit_rate = count(hbn_bidrequest_id is not null) / count(*)` as a profiling metric (TRD §7.7 Step 1).

---

## 3. Column Naming and Dedup Policy

Overlapping concepts are **deduped to a single authoritative source**. The retained column uses a source prefix so lineage is unambiguous:

- `jgr_*` — value sourced from `ex-jaeger-transaction`.
- `hbn_*` — value sourced from `hb-transactions`.
- Unprefixed — composite/derived keys defined by this wide table (e.g. `event_id`, `imp_id`).

### 3.1 Dedup decisions for overlapping concepts

| Concept | In both as | Authoritative source | Why |
|---|---|---|---|
| Floors (`edsp_floor`, `direct_floor`, `acc_floor`) | jaeger + hb | **Jaeger** (`jgr_*`) | Jaeger documents the full floor lifecycle (EnumeratedList §"Floor Field Lifecycle"); HB carries pre-auction copies. Jaeger reflects post-optimization + per-RTB override. |
| Bid shading (`sr_ocpm`, `sr_at`, `sr_pbtw`) | jaeger (per serve result) + hb | **Jaeger** (`jgr_*`) | Jaeger's is recorded at the placement-serve-result / auction-resolution level, aligned to the served impression grain. |
| Device (`make`, `model`, `os`, `osv`, `ifa`, `lo_id`, `w`, `h`, `connectiontype`, etc.) | jaeger + hb | **Jaeger** (`jgr_*`) | Jaeger device fields are post-resolution/normalization at the ADX. HB device fields are the raw mediation-request copy. |
| Geo (`country`, `city`, region) | jaeger + hb | **Jaeger** (`jgr_*`) | Jaeger geo is enriched (lat/lon/region/ipservice); ISO-3166-1-alpha-2 via `device.ext.country_iso_2`. HB carries alpha-3 country only. |
| `supply_traffic_source` | jaeger + hb | **Jaeger** (`jgr_*`) | Authored at the ADX boundary. |
| `datasci_tags` | jaeger + hb | **HB** (`hbn_*`) | HB/BFlat is the producer of datasci tags; Jaeger's is a pass-through. |
| `req_no`, `req_group_id` | jaeger + hb | **Jaeger** (`jgr_*`) | Duplicate-strategy keys authored in Jaeger. |
| `lo_id` / `device_id` | jaeger `lo_id` + hb `bidrequest_device_lo_id` / `device_id` | **Jaeger** `jgr_lo_id` | Single hashed device identity for the table; HB device_id retained only as fallback note. |
| Experiment buckets (`exp_to_bucket`) | jaeger + hb | **Jaeger** (`jgr_*`) | Jaeger holds the applied bucket map. HB `experiment_id` retained separately (HBP DS experiment, distinct concept). |

When a concept is deduped, the dropped copy is **not** carried as a column; the dedup rationale lives in this table so a reader knows which source won.

### 3.2 Metadata columns per field

Each field below is described with:

- `column` — wide-table column name.
- `type` — physical type (`STRING`, `DOUBLE`, `LONG`, `INT`, `BOOLEAN`, `ARRAY<...>`).
- `source` — `source_table.source_column`.
- `semantic_type` — one of: `id`, `categorical`, `boolean_flag`, `money_cpm`, `rate`, `count`, `duration_ms`, `epoch_ms`, `epoch_s`, `timestamp`, `dimension`, `geo`, `device_attr`, `consent`, `free_text`, `json_blob`, `version`, `enum_code`.
- `null` — null semantics: `not_observed` (data absent / not applicable), `zero_is_meaningful`, `always_present`.
- `feat` — feature suitability for ML: `key` (join/identity, not a feature), `dim` (dimension/grouping key), `feature` (directly usable), `feature_after_encode` (needs bucketing/encoding), `leak_risk` (label-adjacent; usable only with point-in-time care), `exclude` (PII/deprecated/operational).
- `enum_ref` — link to §8 enum table when applicable.

---

## 4. Identity & Keys

These define the row and the joins. None are features.

| column | type | source | semantic_type | null | feat | description |
|---|---|---|---|---|---|---|
| `event_id` | STRING | hb.event_id ↔ jaeger.placement_serve_results[].ad_event_id | id | always_present | key | Unique ad-delivery event id. The primary join key and the stable impression identifier used to combine with GMinor self-served logs and labels (TRD §7.9.3). |
| `imp_id` | STRING | jaeger.placement_serve_results[].imp_id ↔ hb.bidrequest_imp_id | id | always_present | key | Impression id within the request; starts at 1 and increments. Second join key. |
| `jgr_transaction_id` | STRING | jaeger.id | id | always_present | key | Jaeger ADX transaction id (parent of the exploded serve result). One transaction → many `imp_id`. |
| `hbn_bidrequest_id` | STRING | hb.bidrequest_id | id | not_observed | key | HB bid-request id. Null ⇒ impression was not HB/S2S. |
| `jgr_auction_id` | STRING | jaeger.placement_serve_results[].auction_id | id | not_observed | key | Auction id for this serve result. |
| `jgr_incoming_bid_request_id` | STRING | jaeger.incoming_bid_request_id | id | not_observed | key | Original mediation/OEM bid-request id for HB/S2S; needed to communicate with partners. |
| `jgr_dup_key` | STRING | jaeger.dup_key | id | not_observed | dim | Unique key for the duplicate-strategy grouping. |
| `jgr_req_no` | LONG | jaeger.req_no | count | not_observed | feature | Request number within the duplicate strategy. |
| `jgr_req_group_id` | STRING | jaeger.req_group_id | id | not_observed | dim | Request-group id; multiple messages share it when the dup strategy treats them as duplicates. |

---

## 5. Field Catalog by Domain

Domains follow the Capability Map grouping in TRD §7.10.1.

### 5.1 Supply / Inventory Context

| column | type | source | semantic_type | null | feat | description |
|---|---|---|---|---|---|---|
| `jgr_supply_traffic_source` | STRING | jaeger.supply_traffic_source | categorical | always_present | dim | Traffic origin: `sdk`, `hb`, `s2s`. Top-level partition of the demo. |
| `hbn_supply_name` | STRING | hb.supply_name | categorical | not_observed | dim | Supply partner name, e.g. `mopub`, `admob`. Member of `non_device_context_v1`. |
| `hbn_supply_fee` | DOUBLE | hb.supply_fee | rate | not_observed | feature | Supply-partner revenue-share fraction, e.g. 0.05. |
| `hbn_pub_account_id` | STRING | hb.pub_account_id | id | not_observed | dim | Publisher account id. Member of `non_device_context_v1`. |
| `hbn_pub_app_object_id` | STRING | hb.pub_app_object_id | id | not_observed | dim | Publisher app ObjectId — the primary app dimension across all dimension families. |
| `hbn_pub_app_id` | STRING | hb.pub_app_id | id | not_observed | dim | Publisher app store id (marketId). |
| `hbn_pub_app_bundle_id` | STRING | hb.pub_app_bundle_id | id | not_observed | dim | Publisher app bundle / package name. |
| `hbn_pub_genre` | ARRAY\<STRING\> | hb.pub_genre[] | categorical | not_observed | feature_after_encode | Publisher genre tags, e.g. `[GAME, QUIZ]`. Use top-N / multi-hot. |
| `jgr_app_bundle` | STRING | jaeger.app.bundle | id | not_observed | dim | Exchange-independent app bundle (Android package / iOS numeric id). |
| `jgr_app_name` | STRING | jaeger.app.name | free_text | not_observed | dim | App name. |
| `jgr_app_cat` | ARRAY\<STRING\> | jaeger.app.cat[] | enum_code | not_observed | feature_after_encode | IAB content categories of the app. → §8 IAB. |
| `jgr_app_object_id` | STRING | jaeger.app.ext.object_id | id | not_observed | dim | App object id (Jaeger side). |
| `jgr_app_hosting_cost` | DOUBLE | jaeger.app.ext.hosting_cost | rate | not_observed | feature | Hosting-cost fraction. |
| `jgr_app_rev_share` | DOUBLE | jaeger.app.ext.rev_share | rate | not_observed | feature | Effective rev-share fraction. |
| `jgr_app_rev_share_original` | DOUBLE | jaeger.app.ext.rev_share_original | rate | not_observed | feature | Original rev-share before adjustment. |
| `jgr_app_hb_partner` | STRING | jaeger.app.ext.hb_partner | categorical | not_observed | dim | 3rd-party header-bidding partner. |
| `jgr_app_ver` | STRING | jaeger.app.ver | version | not_observed | feature_after_encode | Publisher app version. |
| `jgr_is_header_bidding` | BOOLEAN | jaeger.is_header_bidding | boolean_flag | always_present | dim | Whether this is a header-bidding request. Member of `non_device_context_v1`. |
| `jgr_is_gam` | BOOLEAN | jaeger.is_gam | boolean_flag | not_observed | dim | Whether traffic originates from Google Ad Manager. |
| `hbn_session_depth_mediation` | LONG | hb.session_depth_mediation | count | not_observed | feature | Ordinal session view from the mediator. |
| `hbn_n_ordinal_view` | LONG | hb.n_ordinal_view | count | not_observed | feature | Ordinal view count since SDK session start. |

### 5.2 Placement

| column | type | source | semantic_type | null | feat | description |
|---|---|---|---|---|---|---|
| `jgr_placement_id` | STRING | jaeger.placement_serve_results[].placement_id | id | always_present | dim | Vungle placement id for this serve result. Member of `non_device_context_v1`. |
| `jgr_placement_reference_id` | STRING | jaeger.placement_serve_results[].placement_reference_id | id | not_observed | dim | Placement reference id. |
| `jgr_placement_type` | STRING | jaeger.placements[].placement_type | categorical | not_observed | dim | `banner`, `interstitial`, `rewarded`, etc. Core dimension in every family. |
| `jgr_ad_size` | STRING | jaeger.placements[].ad_size | categorical | not_observed | feature | Impression size: `mrec`/`banner`/`in_line`/`fullscreen`. |
| `jgr_ad_type` | STRING | jaeger.placements[].ad_type | categorical | not_observed | feature | Ad type. |
| `jgr_placement_floor` | DOUBLE | jaeger.placements[].floor | money_cpm | not_observed | feature | DAL base floor — raw, pre-adjustment. → §8 Floor Lifecycle. |
| `jgr_effective_rpm_floor` | DOUBLE | jaeger.placements[].effective_rpm_floor | money_cpm | not_observed | feature | `placement_floor / ((1 - serving_cost) * rev_share)`. |
| `jgr_flat_cpm` | DOUBLE | jaeger.placements[].flat_cpm | money_cpm | not_observed | feature | Flat CPM price if flat-CPM pricing enabled. |
| `jgr_is_flat_cpm_enabled` | BOOLEAN | jaeger.placements[].is_flat_cpm_enabled | boolean_flag | not_observed | dim | Whether flat-CPM pricing is on. |
| `jgr_flat_cpm_model_type` | STRING | jaeger.placements[].flat_cpm_model_type | enum_code | not_observed | dim | `TRS` (Target Rev Share) or `NRG` (Net Revenue Growth). → §8. |
| `jgr_is_incentivized` | BOOLEAN | jaeger.placements[].is_incentivized | boolean_flag | not_observed | dim | Rewarded placement flag. |
| `jgr_is_max_profit_enabled` | BOOLEAN | jaeger.placements[].is_max_profit_enabled | boolean_flag | not_observed | dim | Max-profit auction mode flag. |
| `jgr_publisher_payout_type` | STRING | jaeger.placements[].publisher_payout_type | categorical | not_observed | dim | `REVENUE_SHARE`, `FLAT_CPM`, `CPM`. |
| `jgr_ad_unit_id` | STRING | jaeger.placements[].ad_unit_id | id | not_observed | dim | Mediation-partner ad-unit id. |

### 5.3 Device

Authoritative source: Jaeger (post-resolution at ADX). HB device columns are deduped out per §3.1; the single device identity is `jgr_lo_id`.

| column | type | source | semantic_type | null | feat | description |
|---|---|---|---|---|---|---|
| `jgr_lo_id` | STRING | jaeger.lo_id | id | not_observed | key | Hashed, non-reversible device identifier. Primary key for `device_level_v1`. |
| `jgr_dev_make` | STRING | jaeger.device.make | device_attr | not_observed | feature_after_encode | Device maker, e.g. Apple. |
| `jgr_dev_model` | STRING | jaeger.device.model | device_attr | not_observed | feature_after_encode | Device model. **Bucket to top-N (`dev_model_bucket`) before use** — raw cardinality too high (TRD §7.8). |
| `jgr_dev_os` | STRING | jaeger.device.os | device_attr | not_observed | dim | OS, e.g. iOS/android. `dev_platform` source. |
| `jgr_dev_osv` | STRING | jaeger.device.osv | version | not_observed | feature_after_encode | OS version; derive `os_version_major`. |
| `jgr_dev_w` | LONG | jaeger.device.w | count | not_observed | feature | Screen width px. |
| `jgr_dev_h` | LONG | jaeger.device.h | count | not_observed | feature | Screen height px. |
| `jgr_dev_carrier` | STRING | jaeger.device.carrier | device_attr | not_observed | feature_after_encode | Carrier. |
| `jgr_dev_connectiontype` | LONG | jaeger.device.connectiontype | enum_code | not_observed | feature | Connection type code. `dev_connection` source. |
| `jgr_dev_devicetype` | LONG | jaeger.device.devicetype | enum_code | not_observed | feature | General device type code. |
| `jgr_dev_language` | STRING | jaeger.device.language | device_attr | not_observed | feature_after_encode | Device language. |
| `jgr_dev_id_source` | STRING | jaeger.device.ext.id_source | categorical | not_observed | dim | Device-id source. `dev_id_source` dimension. |
| `jgr_dev_country_iso2` | STRING | jaeger.device.ext.country_iso_2 | geo | not_observed | dim | ISO-3166-1-alpha-2 device country. |
| `jgr_dev_store_country_iso2` | STRING | jaeger.device.ext.store_country_iso_2 | geo | not_observed | dim | App-store country (alpha-2). |
| `jgr_dev_battery_level` | DOUBLE | jaeger.device.ext.battery_level | rate | not_observed | feature | Battery level [0,1]. |
| `jgr_dev_volume` | DOUBLE | jaeger.device.ext.volume | rate | not_observed | feature | Device volume [0,1]. |
| `jgr_dev_is_anonymous_vpn` | BOOLEAN | jaeger.device.ext.is_anonymous_vpn | boolean_flag | not_observed | feature | IP identified as VPN. |
| `jgr_dev_is_suspicious_ip` | BOOLEAN | jaeger.device.ext.is_suspicious_ip | boolean_flag | not_observed | feature | IP flagged suspicious by Accelerate. |
| `jgr_dev_dnt` | LONG | jaeger.device.dnt | boolean_flag | not_observed | feature | Do-not-track. |
| `jgr_dev_lmt` | LONG | jaeger.device.lmt | boolean_flag | not_observed | feature | Limit-ad-tracking. |
| `jgr_dev_ifa` | STRING | jaeger.device.ifa | id | not_observed | exclude | Advertiser id (raw, PII). Excluded from features; use hashed `jgr_lo_id`. |
| `jgr_dev_ip` | STRING | jaeger.device.ip | id | not_observed | exclude | IP address (PII). Operational only. |
| `jgr_dev_ua` | STRING | jaeger.device.ua | free_text | not_observed | exclude | User agent. Not a feature; parse upstream if needed. |
| `jgr_dev_connection_type_detail` | STRING | jaeger.device.ext.connection_type_detail | categorical | not_observed | feature_after_encode | Detailed connection type. |

### 5.4 Geo

| column | type | source | semantic_type | null | feat | description |
|---|---|---|---|---|---|---|
| `jgr_geo_country` | STRING | jaeger.device.geo.country | geo | not_observed | dim | Country (Jaeger geo). Canonical `geoip_country_code` after alpha-2 normalization via `jgr_dev_country_iso2`. |
| `jgr_geo_region` | STRING | jaeger.device.geo.region | geo | not_observed | dim | Region/state. |
| `jgr_geo_city` | STRING | jaeger.device.geo.city | geo | not_observed | feature_after_encode | City. High cardinality — bucket before use. |
| `jgr_geo_type` | LONG | jaeger.device.geo.type | enum_code | not_observed | feature | Geo source type. |
| `jgr_geo_ipservice` | LONG | jaeger.device.geo.ipservice | enum_code | not_observed | feature | IP-service provider code. |

### 5.5 Privacy / Consent

All consent fields are `exclude` for direct feature use by default (regulatory); retained for filtering/cohorting. TCF purpose codes are enum-backed (§8).

| column | type | source | semantic_type | null | feat | description |
|---|---|---|---|---|---|---|
| `jgr_ccpa_opt_out` | BOOLEAN | jaeger.ccpa_opt_out | consent | not_observed | dim | User opted out of data sale. |
| `jgr_coppa_applied` | BOOLEAN | jaeger.coppa_applied | consent | not_observed | dim | COPPA (children) applies. |
| `jgr_consent_status` | STRING | jaeger.consent_status | consent | not_observed | dim | Consent status. |
| `jgr_consent_source` | STRING | jaeger.consent_source | consent | not_observed | dim | Consent source. |
| `jgr_tcf_result` | INT | jaeger.tcf.tcf_result | enum_code | not_observed | dim | TCF match result. → §8 tcf_result. |
| `jgr_tcf_cmp_id` | INT | jaeger.tcf.cmp_id | enum_code | not_observed | dim | CMP id. |
| `jgr_tcf_ver` | STRING | jaeger.tcf.tcf_ver | version | not_observed | dim | TCF version. |
| `jgr_tcf_invalid_reason` | INT | jaeger.tcf.invalid_reason | enum_code | not_observed | dim | Invalid-TCF reason. → §8. |
| `jgr_is_first_party_data_valid` | BOOLEAN | jaeger.is_first_party_data_valid | boolean_flag | not_observed | feature | First-party data validity. |

> TCF purpose / special-feature / LI / publisher-restriction fields (`tcf.purposes.p1..p11`, `tcf.sf.f1`, `tcf.li_purposes.*`, `tcf.pub_restriction.*`) are retained as a JSON blob `jgr_tcf_detail` (`json_blob`, `exclude`) rather than ~40 individual columns, to keep the wide table manageable. Expand only if a privacy-cohort analysis needs them.

### 5.6 Floor Lifecycle

Authoritative source: Jaeger (full lifecycle documented in EnumeratedList §"Floor Field Lifecycle"). HB floor copies deduped out. See §8 for the stage diagram.

| column | type | source | semantic_type | null | feat | description |
|---|---|---|---|---|---|---|
| `jgr_bid_floor` | DOUBLE | jaeger.placement_serve_results[].bid_floor | money_cpm | not_observed | feature | **Stage 4** final effective floor used in the auction for this serve result (post all adjustments). Downstream `bid_floor_at_delivery`. Primary floor feature. |
| `jgr_edsp_floor` | DOUBLE | jaeger.edsp_floor | money_cpm | not_observed | feature | **Stage 2** floor sent to external DSPs (post optimization + VXAC). For HB/S2S defaults to incoming imp bid floor. |
| `jgr_direct_floor` | DOUBLE | jaeger.direct_floor | money_cpm | not_observed | feature | **Stage 2** floor to Direct DSP (DMX Meister). |
| `jgr_acc_floor` | DOUBLE | jaeger.acc_floor | money_cpm | not_observed | feature | **Stage 2** Accelerate DSP base (pre-adjustment) floor. |
| `jgr_mediation_floor` | DOUBLE | jaeger.mediation_floor | money_cpm | not_observed | feature | Mediation partner floor × HB programmatic-floor experiment multiplier. Null for SDK-direct. Feeds `edsp_floor` via max(). |
| `jgr_overwritten_floor` | DOUBLE | jaeger.overwritten_floor | money_cpm | not_observed | feature | **Stage 3** per-RTB override floor for the winning RTB. Null when no override. |
| `hbn_bidrequest_imp_bidfloor` | DOUBLE | hb.bidrequest_imp_bidfloor | money_cpm | not_observed | feature | Impression-level minimum bid (CPM) from the incoming HB bid request. The HB-side floor input. Retained (not a Jaeger dup — it is the upstream request floor). |
| `jgr_auction_timeout` | LONG | jaeger.auction_timeout | duration_ms | not_observed | feature | Auction timeout (ms). |
| `hbn_mediation_tmax` | LONG | jaeger.mediation_tmax | duration_ms | not_observed | feature | Mediation timeout (ms). (Jaeger-sourced; HB-domain concept.) |

### 5.7 Auction & Bid Economics

| column | type | source | semantic_type | null | feat | description |
|---|---|---|---|---|---|---|
| `hbn_adx_bid_price` | DOUBLE | hb.adx_bid_price | money_cpm | not_observed | feature | DSP bid price (CPM) from the ad network. |
| `hbn_hbp_bid_price` | DOUBLE | hb.hbp_bid_price | money_cpm | not_observed | feature | HBP bid price = `bflat_bid_price` or `bflat_bid_price × yield_enhancement_bid_multiplier`. |
| `hbn_bflat_bid_price` | DOUBLE | hb.bflat_bid_price | money_cpm | not_observed | feature | BFlat predicted price before yield enhancement (shaded eDSP price). Recorded even when eDSP is not final winner. |
| `hbn_yield_enhancement_bid_multiplier` | DOUBLE | hb.yield_enhancement_bid_multiplier | rate | not_observed | feature | Yield-enhancement multiplier, default 1.0, range 1.0–2.2. |
| `hbn_nostr_bid_price` | DOUBLE | hb.nostr_bid_price | money_cpm | not_observed | feature | Nostradamus / Meister bid price when Meister wins. |
| `hbn_acc_bid_price` | DOUBLE | hb.acc_bid_price | money_cpm | not_observed | feature | Accelerate DSP bid price (win or lose). |
| `hbn_max_bid_price` | LONG | hb.max_bid_price | money_cpm | not_observed | feature | PM hard cap on bid price. |
| `hbn_bidrequest_auction_type` | LONG | hb.bidrequest_auction_type | enum_code | not_observed | dim | 1 = First Price, 2 = Second Price. |
| `hbn_bid_nsr` | LONG | hb.bid_nsr | enum_code | not_observed | feature | No-serve reason for the HB bid. → §8 loss_reason / no_serv context. |
| `jgr_bid_dsp_size` | INT | jaeger.bid_dsp_size | count | not_observed | feature | Number of DSPs that bid successfully in the auction. |
| `jgr_no_serv_reason` | LONG | jaeger.placement_serve_results[].no_serv_reason | enum_code | always_present | feature | No-serve reason for the serve result. 0 = delivered. → §8 no_serv_reason. **Label-adjacent — see §6.** |
| `jgr_filter_result` | LONG | jaeger.filter_result | enum_code | not_observed | feature | Pre-auction filter outcome. 0 = passed. → §8 filter_result. |
| `jgr_is_realtime` | BOOLEAN | jaeger.is_realtime | boolean_flag | always_present | feature | Whether served with an ad in realtime. **Label-adjacent — see §6.** |
| `jgr_min_bid_to_win` | DOUBLE | jaeger.placement_serve_results[].min_bid_to_win | money_cpm | not_observed | feature | First-price auction: minimum bid to win sent to eDSP on auction loss after filter. |
| `jgr_second_place_price` | DOUBLE | jaeger.placement_serve_results[].second_place_price | money_cpm | not_observed | feature | Second-highest auction bid. |
| `jgr_third_place_price` | DOUBLE | jaeger.placement_serve_results[].third_place_price | money_cpm | not_observed | feature | Third-highest auction bid. |
| `jgr_edsp_highest_price` | DOUBLE | jaeger.placement_serve_results[].edsp_highest_price | money_cpm | not_observed | feature | Highest eDSP price in the bid request. |
| `jgr_pd_cl` | DOUBLE | jaeger.placement_serve_results[].pd_cl | rate | not_observed | feature | Accelerate Price-Data conversion likelihood. |
| `jgr_pd_cpx` | DOUBLE | jaeger.placement_serve_results[].pd_cpx | money_cpm | not_observed | feature | Accelerate Price-Data bid CPX (cost per conversion). |
| `jgr_pd_nm` | DOUBLE | jaeger.placement_serve_results[].pd_nm | money_cpm | not_observed | feature | Accelerate Price-Data net margin. |
| `jgr_pd_obp` | DOUBLE | jaeger.placement_serve_results[].pd_obp | money_cpm | not_observed | feature | Accelerate original bid price in bid response. |
| `hbn_predicted_user_value` | DOUBLE | hb.predicted_user_value | money_cpm | not_observed | feature | Predicted user value. |
| `hbn_adv_erpm` | DOUBLE | hb.adv_erpm | money_cpm | not_observed | feature | Predicted eRPM of the advertiser app. |
| `hbn_adv_predicted_cvr` | DOUBLE | hb.adv_predicted_cvr | rate | not_observed | feature | Predicted conversion rate of advertiser app. |
| `hbn_multiplier` | DOUBLE | hb.multiplier | rate | not_observed | feature | Datasci price-model multiplier. |
| `hbn_target_margin` | DOUBLE | hb.target_margin | rate | not_observed | feature | Datasci price-model target margin. |

### 5.8 Bid Shading

Authoritative source: Jaeger (serve-result level). HB `sr_*` deduped out.

| column | type | source | semantic_type | null | feat | description |
|---|---|---|---|---|---|---|
| `jgr_sr_ocpm` | LONG | jaeger.placement_serve_results[].sr_ocpm | money_cpm | not_observed | feature | Unshaded private value (original CPM) of the shading result. |
| `jgr_sr_at` | LONG | jaeger.placement_serve_results[].sr_at | enum_code | not_observed | feature | Shading adjustment type. → §8 sr_at. |
| `jgr_sr_pbtw` | DOUBLE | jaeger.placement_serve_results[].sr_pbtw | rate | not_observed | feature | Predicted bid-to-win rate of the shading result. |

### 5.9 Settlement & Win

| column | type | source | semantic_type | null | feat | description |
|---|---|---|---|---|---|---|
| `jgr_settlement_price` | DOUBLE | jaeger.placement_serve_results[].settlement_price | money_cpm | not_observed | leak_risk | Settlement (clearing) price for the impression. Common **label** for floor/NR models — exclude from features, use as target (§6). |
| `jgr_settlement_status` | LONG | jaeger.placement_serve_results[].settlement_status | enum_code | not_observed | feature | Settlement status code. |
| `jgr_winner_id` | STRING | jaeger.placement_serve_results[].winner_id | id | not_observed | dim | Winning bidder id. |
| `jgr_winning_seat` | STRING | jaeger.placement_serve_results[].winning_seat | id | not_observed | dim | Winning seat. |
| `jgr_winner_account_id` | STRING | derived: jaeger winning rtbconnection account_id | id | not_observed | dim | Winning account id. Member of `non_device_context_v1`. |
| `jgr_winning_bid_price` | DOUBLE | jaeger.placement_serve_results[].winning_bid.price | money_cpm | not_observed | leak_risk | Winning bid price. Label-adjacent. |
| `jgr_winning_bid_adomain` | ARRAY\<STRING\> | jaeger.placement_serve_results[].winning_bid.adomain[] | categorical | not_observed | feature_after_encode | Winning advertiser domain(s). Use top-N MAP, not raw (TRD §7.4.2). |
| `jgr_winning_bid_bundle` | STRING | jaeger.placement_serve_results[].winning_bid.bundle | id | not_observed | dim | Winning bid app bundle. |
| `jgr_winning_bid_cat` | ARRAY\<STRING\> | jaeger.placement_serve_results[].winning_bid.cat[] | enum_code | not_observed | feature_after_encode | Winning bid IAB categories. → §8 IAB. |
| `jgr_winning_bid_crid` | STRING | jaeger.placement_serve_results[].winning_bid.crid | id | not_observed | dim | Winning creative id. |
| `jgr_winning_bid_cid` | STRING | jaeger.placement_serve_results[].winning_bid.cid | id | not_observed | dim | Winning campaign id. |
| `jgr_winner_predicted_nr` | DOUBLE | jaeger.winner_predicted_nr | money_cpm | not_observed | leak_risk | Predicted net revenue of the winning bid. Strongly label-adjacent. |
| `jgr_is_winner_acc` | BOOLEAN | jaeger.is_winner_acc | boolean_flag | not_observed | feature | Whether Accelerate is the internal auction winner. |
| `jgr_vungle_price` | DOUBLE | jaeger.placement_serve_results[].vungle_price | money_cpm | not_observed | feature | Direct-demand price. |
| `jgr_erpmtarget` | DOUBLE | jaeger.placement_serve_results[].erpmtarget | money_cpm | not_observed | feature | eRPM target for the serve result. |
| `hbn_adv_is_internal` | BOOLEAN | hb.adv_is_internal | boolean_flag | not_observed | dim | Ad from Vungle (internal) vs xDSP. |
| `hbn_adv_campaign_id` | STRING | hb.adv_campaign_id | id | not_observed | dim | Advertiser campaign id. |
| `hbn_adv_creative_id` | STRING | hb.adv_creative_id | id | not_observed | dim | Advertiser creative id. |
| `hbn_adv_app_bundle_id` | STRING | hb.adv_app_bundle_id | id | not_observed | dim | Advertiser app bundle. |
| `hbn_adv_genre` | ARRAY\<STRING\> | hb.adv_genre[] | categorical | not_observed | feature_after_encode | Advertiser genre tags. |

### 5.10 Creative / Endcard

| column | type | source | semantic_type | null | feat | description |
|---|---|---|---|---|---|---|
| `jgr_ec_type` | INT | jaeger.placement_serve_results[].ec_type | enum_code | not_observed | feature | Endcard combination type. → §8 ec_type. |
| `jgr_has_endcard` | BOOLEAN | jaeger.placement_serve_results[].has_endcard | boolean_flag | not_observed | feature | Has non-VX end card. |
| `jgr_has_vungle_endcard` | BOOLEAN | jaeger.placement_serve_results[].has_vungle_endcard | boolean_flag | not_observed | feature | Has VX end card. |
| `jgr_has_skip_button` | BOOLEAN | jaeger.placement_serve_results[].has_skip_button | boolean_flag | not_observed | feature | Has skip button. |
| `jgr_is_streaming_video` | BOOLEAN | jaeger.placement_serve_results[].is_streaming_video | boolean_flag | not_observed | feature | Streaming video. |
| `jgr_freeform_type` | STRING | jaeger.placement_serve_results[].freeform_type | categorical | not_observed | feature | Freeform ad type: modular_ui/split_screen/ad_pod/static. |
| `jgr_edsp_fs_cr_type` | STRING | jaeger.placement_serve_results[].edsp_fs_cr_type | enum_code | not_observed | feature_after_encode | eDSP full-screen creative type. → §8 edsp_fsc_cr_type. |
| `jgr_is_creative_interactive` | BOOLEAN | jaeger.placement_serve_results[].winning_bid.is_creative_interactive | boolean_flag | not_observed | feature | Winning creative is interactive. |
| `jgr_ai_disclosure` | BOOLEAN | jaeger.placement_serve_results[].ai_disclosure | boolean_flag | not_observed | feature | Winning creative disclosed as AI-generated. |
| `jgr_template_id` | STRING | jaeger.placement_serve_results[].template_id | id | not_observed | dim | Template id. |
| `jgr_template_name` | STRING | jaeger.placement_serve_results[].template_name | categorical | not_observed | feature_after_encode | Template name. |
| `hbn_full_screen_creative_type` | STRING | hb.full_screen_creative_type | categorical | not_observed | feature_after_encode | HB full-screen creative type. |

### 5.11 TPAT (Tracking / Event Pixels)

TPAT arrays are pixel URL lists. They are not features directly; the demo derives **counts** (e.g. `tpat_event_start_count`) per TRD §7.5.

| column | type | source | semantic_type | null | feat | description |
|---|---|---|---|---|---|---|
| `jgr_tpat_video_start` | ARRAY\<STRING\> | jaeger.placement_serve_results[].tpat.video_start[] | json_blob | not_observed | feature_after_encode | Video-start tracking URLs. Derive `tpat_event_start_count = size(...)`. |
| `jgr_tpat_video_click` | ARRAY\<STRING\> | jaeger.placement_serve_results[].tpat.video_click[] | json_blob | not_observed | feature_after_encode | Video-click tracking URLs. |
| `jgr_tpat_video_close` | ARRAY\<STRING\> | jaeger.placement_serve_results[].tpat.video_close[] | json_blob | not_observed | feature_after_encode | Video-close tracking URLs. |
| `jgr_tpat_postroll_view` | ARRAY\<STRING\> | jaeger.placement_serve_results[].tpat.postroll_view[] | json_blob | not_observed | feature_after_encode | Postroll-view tracking URLs. |
| `jgr_tpat_postroll_click` | ARRAY\<STRING\> | jaeger.placement_serve_results[].tpat.postroll_click[] | json_blob | not_observed | feature_after_encode | Postroll-click tracking URLs. |

### 5.12 Experiment / QPS / Throttling

| column | type | source | semantic_type | null | feat | description |
|---|---|---|---|---|---|---|
| `jgr_exp_to_bucket` | STRING | jaeger.exp_to_bucket | json_blob | not_observed | dim | Applied experiment→bucket map (raw JSON). Parse to dims. |
| `jgr_dte_experiment_group` | STRING | jaeger.dte_experiment_group | categorical | not_observed | dim | DTE experiment group, e.g. `T`/`C`. |
| `jgr_ssp_exp_num` | LONG | jaeger.ssp_exp_num | count | not_observed | dim | SSP DTO experiment number. |
| `jgr_vxac_exp_id` | STRING | jaeger.vxac_exp_id | id | not_observed | dim | Auction Controller experiment id. |
| `hbn_experiment_id` | LONG | hb.experiment_id | id | not_observed | dim | HBP datasci experiment id. `ml_experiment_id` source. |
| `hbn_recommender_tag` | STRING | hb.recommender_tag | categorical | not_observed | dim | Datasci strategy tag. |
| `jgr_qps_type` | STRING | jaeger.placement_serve_results[].qps_type | categorical | not_observed | feature | QPS experiment type: `seq`/`dup`. |
| `jgr_qps_exp_name` | STRING | jaeger.placement_serve_results[].qps_exp_name | categorical | not_observed | dim | QPS experiment name. |
| `jgr_winner_qps_index` | INT | jaeger.placement_serve_results[].winner_qps_index | count | not_observed | feature | Winner request index in RTB requests; 0 = original. |
| `jgr_winner_qps_round` | INT | jaeger.placement_serve_results[].winner_qps_round | count | not_observed | feature | QPS experiment round. |
| `jgr_winner_rtb_dup_strategy` | STRING | jaeger.placement_serve_results[].winner_rtb_dup_strategy | categorical | not_observed | feature | Winner RTB duplicate strategy, e.g. `dup_1|1`, `seq_0|0`. |
| `jgr_winner_applied_multiplier` | INT | jaeger.winner_applied_multiplier | count | not_observed | feature | Multiplier applied by auction-dynamic experiment to winning RTB. |
| `jgr_winner_is_using_albatross_model` | BOOLEAN | jaeger.winner_is_using_albatross_model | boolean_flag | not_observed | feature | Winning RTB processed by Albatross dynamic-throttling model. |
| `jgr_winner_qps_cap` | LONG | jaeger.winner_qps_cap | count | not_observed | feature | Hard QPS cap for the winning bidder. |
| `jgr_winner_rpma_bar` | DOUBLE | jaeger.winner_rpma_bar | money_cpm | not_observed | feature | Dynamic RPMA pre-throttle threshold for the winning bidder. |
| `jgr_is_ad_podding` | BOOLEAN | jaeger.placement_serve_results[].is_ad_podding | boolean_flag | not_observed | feature | Ad podding flag. |
| `jgr_ad_podding_multiplier` | DOUBLE | jaeger.placement_serve_results[].ad_podding_multiplier | rate | not_observed | feature | Pod multiplier (Stage 2). |

### 5.13 RTB Connection

| column | type | source | semantic_type | null | feat | description |
|---|---|---|---|---|---|---|
| `jgr_rtb_connection_id` | STRING | jaeger.placement_serve_results[].rtbconnections[].id (winning) | id | not_observed | dim | Winning RTB connection id. Member of every dimension family. |
| `jgr_rtb_account_id` | STRING | jaeger.placement_serve_results[].rtbconnections[].account_id (winning) | id | not_observed | dim | Winning RTB account id. |
| `jgr_rtb_adm_type` | STRING | jaeger.placement_serve_results[].rtbconnections[].adm_type (winning) | categorical | not_observed | feature | AdMarkup type of winning RTB. |
| `jgr_rtb_is_internal` | BOOLEAN | jaeger.placement_serve_results[].rtbconnections[].is_internal (winning) | boolean_flag | not_observed | dim | Winning RTB is internal. |
| `jgr_rtb_fanout_source` | LONG | jaeger.placement_serve_results[].rtbconnections[].fanout_source (winning) | enum_code | not_observed | feature | Fanout source of winning RTB. |
| `hbn_bidder_id` | STRING | hb.bidder_id | id | not_observed | dim | Vungle bidder id competing in the auction. |
| `hbn_rtb_account_id` | STRING | hb.rtb_account_id | id | not_observed | dim | RTB account id in the HB auction (HB-side copy; retained for HB-only analysis). |

### 5.14 Timing & Operational

| column | type | source | semantic_type | null | feat | description |
|---|---|---|---|---|---|---|
| `source_event_time` | TIMESTAMP | jaeger.timestamp | timestamp | always_present | key | Canonical event time. Drives all aggregation windows (`time_column` in TRD §7.3.2) and point-in-time joins (§7.9.3). |
| `hbn_timestamp` | TIMESTAMP | hb.timestamp | timestamp | not_observed | key | HB transaction time (RFC 3339). |
| `jgr_process_duration` | LONG | jaeger.process_duration | duration_ms | not_observed | feature | Transaction process duration (ms). |
| `hbn_bidrequest_time` | LONG | hb.bidrequest_time | epoch_ms | not_observed | feature | HB bid-request time. |
| `jgr_request_phase` | STRING | jaeger.request_phase | categorical | not_observed | feature | External-auction lifecycle phase: `bid`/`postbid`/`priorityaccess`. |
| `jgr_traffic_quality_action` | STRING | jaeger.traffic_quality_action | categorical | not_observed | feature | Optional traffic-quality action applied. |
| `jgr_traffic_quality_strategy` | STRING | jaeger.traffic_quality_strategy | categorical | not_observed | dim | Strategy that tagged the device. |
| `jgr_double_verify_fraud_reason` | STRING | jaeger.double_verify_fraud_reason | categorical | not_observed | feature | DoubleVerify fraud reason. |
| `hbn_datasci_tags` | STRING | hb.datasci_tags | json_blob | not_observed | feature_after_encode | BFlat datasci tags (authoritative datasci source). Parse to features. |
| `hbn_bflat_features` | STRING | hb.bflat_features | json_blob | not_observed | feature_after_encode | Pass-through BFlat user/profile features (JSON map). |
| `jgr_jaeger_version` | STRING | jaeger.jaeger_version | version | not_observed | dim | Jaeger build version (lineage). |
| `hbn_hbp_version` | STRING | hb.hbp_version | version | not_observed | dim | HBP version (lineage). |

### 5.15 Device-History Counters (HB)

These are pre-computed device/campaign history counters — directly useful for `device_level_v1` features.

| column | type | source | semantic_type | null | feat | description |
|---|---|---|---|---|---|---|
| `hbn_n_lifetime_views_campaign` | LONG | hb.n_lifetime_views_campaign | count | not_observed | feature | Lifetime campaign views on device. |
| `hbn_n_24h_views_campaign` | LONG | hb.n_24h_views_campaign | count | not_observed | feature | Campaign views in last 24h. |
| `hbn_n_lifetime_application_views_campaign` | LONG | hb.n_lifetime_application_views_campaign | count | not_observed | feature | Lifetime views of campaign's app on device. |
| `hbn_n_past_vungle_installs` | LONG | hb.n_past_vungle_installs | count | not_observed | feature | Lifetime Vungle installs on device. |
| `hbn_time_since_last_vungle_delivery` | LONG | hb.time_since_last_vungle_delivery | epoch_s | not_observed | feature | Last install-contributing delivery time (unix s). Derive recency. |
| `hbn_time_since_this_campaign_delivered` | LONG | hb.time_since_this_campaign_delivered | epoch_s | not_observed | feature | Last delivery of this campaign on device (unix s). |
| `hbn_time_since_this_creative_delivered` | LONG | hb.time_since_this_creative_delivered | epoch_s | not_observed | feature | Last delivery of this creative on device (unix s). |

---

## 6. Label / Leakage Guidance

For the offline simulation (TRD §7.9.4), the following fields are **outcomes**, not inputs. They must be used as targets or excluded from feature formulas; using them as trailing-window features for their own impression is leakage (TRD §7.6 validation rule "Formula cannot reference future labels").

| Field | Used as | Note |
|---|---|---|
| `jgr_settlement_price` | label (NR / settlement / floor models) | Primary continuous label. |
| `jgr_winning_bid_price` | label-adjacent | Outcome of the auction. |
| `jgr_winner_predicted_nr` | label-adjacent | Model-internal prediction of NR. |
| `jgr_no_serv_reason` | label (binary serve / no-serve) | `0` = delivered; used for event-start / serve classification. |
| `jgr_is_realtime` | label-adjacent | Realtime-serve outcome. |
| `jgr_settlement_status` | label-adjacent | Settlement outcome. |

Aggregated/trailing-window versions of these (e.g. `avg_settlement_price_7d` over a device/placement) **are** valid features as long as the window ends strictly before the current `source_event_time` and the current impression does not contribute to its own trailing aggregate (TRD §7.9.3).

---

## 7. Out of Scope

- The Spark/Trino join job that materializes this table (sub-project B / TRD §7.7 Step 4).
- The `feature_capabilities` catalog table itself (sub-project A / TRD §7.3.1) — this doc is its *input*.
- Losing-bidder rows: the table keeps only the served/winning bid per `(event_id, imp_id)`. Multi-bidder competition analysis needs a separate bid-grain table.
- Non-HB/S2S enrichment beyond these two topics (e.g. install/postback attribution, IAP).
- The full hb-notifications win/loss event stream (only its enum definitions are referenced here for `bid_nsr` interpretation).

---

## 8. Enumerated Value Appendix

Referenced by `enum_ref` in the catalog. Source files cited per table.

### 8.1 `jgr_no_serv_reason` (jaeger EnumeratedList)

| Value | Description | Triggers auction |
|---|---|---|
| 0 | ad delivered successfully | yes |
| 1 | unknown | - |
| 3 | mal-formatted payload | no |
| 5 | rejected by filter | no |
| 6 | bad auction invitation | no |
| 7 | no bid response | yes |
| 8 | invalid VAST | yes |
| 9 | cannot parse AdMarkup | yes |
| 10 | placement not found | no |
| 13 | request cancelled | no |
| 15 | AdMarkup is incompatible | yes |
| 16 | application is blocked | no |
| 17 | device is incompatible | yes |
| 18 | failed to record hbp bid token | yes |
| 19 | failed to compose S2S partner bid response | yes |
| 20 | failed to build ADM | yes |
| 21 | incompatible SDK version with ad type | no |
| 22 | invalid native bid response | yes |
| 23 | incompatible SDK version with OS version | no |
| 24 | specify connection is required | no |
| 25 | device OS does not match required OS | no |
| 26 | publisher not found | no |
| 27 | delivery limit exceeded | no |
| 28 | technical error | - |
| 29 | test mode not match prod traffic | no |
| 30 | publisher account is blocked | no |
| 31 | publisher battery saver mode not supported | no |
| 32 | publisher block on SDK incompatible | no |
| 33 | publisher not supported for S2S partner (on-boarding) | no |
| 34 | precache ads are disabled | no |
| 35 | traffic is throttled | no |
| 36 | low SDK version rejected by header-bidding | no |
| 37 | IP is fraud | no |
| 38 | publisher account not found | no |
| 39 | publisher identified as fraud | no |
| 40 | hash(ip+ua) identified as fraud | no |
| 41 | hash(IP) identified as fraud | no |
| 42 | hash(ua) identified as fraud | no |
| 43 | network error | no |
| 44 | header-bidding placement not support waterfall traffic | no |
| 46 | admob partner realtime placement not support precache mode | no |
| 47 | failed to build nautilus request | yes |
| 48 | failed to get response from nautilus | yes |
| 49 | nautilus timeout | yes |
| 1002 | time cost exceeds mediation TTL (headerbidding only) | yes |
| 1015 | rejected by filter (headerbidding only) | no |

### 8.2 `jgr_filter_result` (jaeger EnumeratedList)

Key codes (full list in source): `-1` ResultUnspecified (do not use), `0` ResultOk (passed; only auctioned when this), `1` ResultInvalidContext, `2` ResultCompletedContext, `3` ResultInternalError, `4` RejectAdmobHBPrecache, `10000` PublisherNotFound, `10001` PublisherConnectionMismatch, `10002` PublisherOSMismatch, `10003` NoEligibleRTBConnections, `10008` NoEligiblePlacements, `10009` AccountBlocked, `10014` RequestThrottled, `10017` FraudIP, `10019`–`10022` DoubleVerify fraud (App/ADID/HIP/HUA), `20000` IncompatibleSDKWithOSVersion, `30001` DeliveryLimitExceeded, `30002` PauseWaterfall, `30003` PauseHeaderBidding.

### 8.3 `jgr_sr_at` — bid-shading adjustment type (jaeger EnumeratedList)

| Value | Description |
|---|---|
| 0 | Unknown adjustment type |
| 1 | No adjustment; recommended price submitted as is |
| 2 | Set to 1st-price cap when recommendation exceeds cap |
| 3 | Set to floor when recommendation falls below floor |
| 4 | Recommended price ignored; no shading applied |
| 5 | Recommended price ignored; price set from market data |
| 6 | Set to remaining revenue when price exceeded it |
| 7 | Set to MPM distribution-exploration price cap when exceeded |

### 8.4 `jgr_ec_type` — endcard type (jaeger EnumeratedList)

| Value | Description |
|---|---|
| 0 | Video only |
| 1 | Video + DSP HTML Static EC |
| 2 | Video + DSP Static EC |
| 3 | Video + DSP Playable EC |
| 4 | Video + VX Static EC |
| 5 | Video + DSP Playable EC + VX Static EC |
| 6 | Video + DSP Playable EC + DSP HTML Static EC |
| 7 | Video + DSP Playable EC + DSP Static EC |
| 8 | Video + DSP HTML Static EC + VX Static EC |
| 9 | Video + DSP Static EC + VX Static EC |
| 10 | Video + DSP HTML Static EC + DSP HTML Static EC |
| 11 | Video + DSP HTML Static EC + DSP Static EC |
| 12 | Video + DSP Static EC + DSP Static EC |

### 8.5 `jgr_flat_cpm_model_type` (jaeger EnumeratedList)

| Value | Description |
|---|---|
| TRS | Target Rev Share |
| NRG | Net Revenue Growth |

### 8.6 `jgr_edsp_fs_cr_type` — eDSP full-screen creative type (jaeger EnumeratedList)

`MRAID Playable`, `MRAID Static`, `VAST Static EC`, `VAST HTML Interactive EC`, `VAST HTML Static EC`, `VAST No EC`, and combinations (`VAST HTML Interactive EC And Static EC`, etc.). See source for the full 11-value list.

### 8.7 `jgr_app_cat` / `jgr_winning_bid_cat` — IAB content categories

IAB tier-1/tier-2 codes per OpenRTB 2.5 List 5.1 (`IAB1`…`IAB26` with sub-codes). Full mapping in jaeger `EnumeratedList.md` §`winning_bid.cat`. Treat as `feature_after_encode` (tier-1 rollup recommended).

### 8.8 `jgr_tcf_result` (jaeger EnumeratedList)

`0` tcf_consent, `1` reject_by_pub_res, `2` reject_by_purposes, `3` reject_by_sf, `4` reject_by_parse_issue, `5` reject_by_li_purposes, `6` reject_by_miss_vendor, `7` reject_by_miss_li_vendor, `8` reject_by_miss_disclosed_vendors.

### 8.9 `hbn_bidrequest_auction_type` (hb schema)

| Value | Description |
|---|---|
| 1 | First Price |
| 2 | Second Price |

### 8.10 `hbn_bid_nsr` context — `loss_reason` (hb-notifications EnumeratedList)

`0` Bid Won, `1` Internal Error, `2` Impression Opportunity Expired, `3` Invalid Bid Response, `100` Below Auction Floor, `101` Below Deal Floor, `102` Lost to Higher Bid, `103` Lost to PMP Deal, `104` Buyer Seat Blocked, `200`–`213` Creative Filtered (various), `≥1000` exchange-specific. (`bid_nsr` semantics align with this loss-reason space.)

### 8.11 `notification_type` (hb-notifications EnumeratedList)

`0` Loss, `1` Win, `2` Bill, `3` Bill_Ext. (Referenced for downstream notification joins; not a column in this table.)

### 8.12 Floor Field Lifecycle (jaeger EnumeratedList)

```
placements[].floor (DAL config)            → jgr_placement_floor
    ├──→ effective_rpm_floor               → jgr_effective_rpm_floor
    ├──→ edsp_floor (opt + VXAC)           → jgr_edsp_floor
    │       ├──→ max(edsp_floor, mediation_floor)  ← jgr_mediation_floor
    │       └──→ overwritten_floor (per-RTB)       → jgr_overwritten_floor
    ├──→ direct_floor (direct rev share)   → jgr_direct_floor
    └──→ acc_floor (base, pre-adjustment)  → jgr_acc_floor
Final auction floor → placement_serve_results[].bid_floor → jgr_bid_floor
```

---

## 9. Summary

This wide table flattens two upstream topics into one impression-grain row with ~150 source-tagged, metadata-rich columns spanning supply, device, geo, privacy, floor lifecycle, auction economics, bid shading, settlement, creative, TPAT, experiment/QPS, and device history. Every column carries the metadata needed for the capability scanner to register it as a `feature_capability` and for the Capability Map UI to render it grouped by domain. Labels and label-adjacent fields are flagged so the formula validator can enforce point-in-time correctness during offline simulation.
