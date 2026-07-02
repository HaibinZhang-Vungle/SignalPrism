# GMinor Log Schema

This document specifies the event-grain GMinor prediction log used as model
sample data. The log is joined to the hourly aggregation tables in
`realtime_attributed_aggregation_table_schema.md` to attach historical features
for offline simulation, training, and evaluation.

---

## 1. Grain

One row per GMinor prediction sample.

The stable sample key is `event_id`. Depending on the producer and project,
`event_id` can represent an auction id, bid-request id, or another model-serving
sample id. It should be treated as an event-grain join key, not as an
aggregation dimension.

---

## 2. Column Catalog

| column | type | null | role | description | example |
|---|---|---|---|---|---|
| `project_name` | VARCHAR | always_present | dimension | Project name. Only alphabetic characters and underscore are accepted. | `dynamic_throttling`, `floor_optimization`, `ad` |
| `experiment_id` | BIGINT | always_present | dimension | Unique experiment id within a project. Starts from zero. | `95` |
| `features` | VARCHAR | not_observed | json_blob | JSON string from which feature names and values can be extracted. If the transport stores an encoded payload, decode it before feature extraction. | `CAQiGDYx...` |
| `predictions` | VARCHAR | not_observed | json_blob | JSON string from which prediction names and values can be extracted. If the transport stores an encoded payload, decode it before prediction extraction. | `CgoaCAAA...` |
| `event_id` | VARCHAR | always_present | join_key | Unique id for the sample. Usually used to join with wide/event tables. | `69d44dafe6da778b40d784d4` |
| `timestamp` | VARCHAR | always_present | event_time | Event timestamp. The documented format is `yyyy-MM-dd HH:mm:ss.SSS`; producers may also emit RFC3339/RFC3339Nano. Parse into `source_event_time`. | `2026-04-07T00:19:59.501949527Z` |
| `traffic_allocation` | DOUBLE | not_observed | feature | Traffic allocation of this experiment at event time. Range `[0, 1]`. | `0.0` |
| `downsampling_rate` | DOUBLE | not_observed | feature | Downsample rate by dimensions at event time. Range `[0, 1]`. | `1.0` |
| `tags` | VARCHAR | not_observed | json_blob | JSON string for extra metadata. | null |
| `device_id` | VARCHAR | not_observed | join_key | Normalized device id in VX traffic. Prefer `lo_id` for internal device-level aggregate joins when present. | `e7829900-636e-4d81-bd5e-f157af758346` |
| `version` | VARCHAR | not_observed | lineage | Version of the service that produced the message. | `v1.528.0` |
| `cloud_provider` | VARCHAR | not_observed | lineage | Cluster name where the producing service is running. | `eks-prod-us-east-1d` |
| `lo_id` | VARCHAR | not_observed | join_key | Hashed, non-reversible internal identifier for the device identifier. | `317404a8-18e2-48a5-9a0f-9352335e699a` |
| `feature_schema_version` | BIGINT | not_observed | version | Version of the feature schema used to encode/decode `features`. | `4` |
| `dt` | VARCHAR | always_present | partition_key | Date the data was written to the `vungle2-logs` S3 bucket, `YYYY-MM-DD`. | `2026-04-07` |
| `hr` | VARCHAR | always_present | partition_key | Hour the data was written to the bucket, `HH`. | `00` |
| `mn` | VARCHAR | always_present | partition_key | Minute the data was written to the bucket, `mm`. | `30` |
| `az` | VARCHAR | not_observed | lineage | Source Kafka EKS cluster where the data originated. | `us-east-1b-eks-3` |

---

## 3. Derived Columns

These columns are not necessarily present in the raw GMinor log, but downstream
jobs should materialize them before joining to aggregate features.

| column | type | derivation | description |
|---|---|---|---|
| `source_event_time` | TIMESTAMP | parse `timestamp` | Canonical event timestamp. Accept documented `yyyy-MM-dd HH:mm:ss.SSS` and observed RFC3339/RFC3339Nano formats. |
| `event_hour` | TIMESTAMP | `date_trunc('hour', source_event_time)` | Event hour for coarse partition pruning. Do not use same-hour aggregates for point-in-time features unless the aggregate row is known to contain only prior data. |
| `project_experiment_key` | VARCHAR | `concat(project_name, ':', cast(experiment_id as varchar))` | Stable experiment key for grouping and diagnostics. |
| `effective_device_id` | VARCHAR | `coalesce(lo_id, device_id)` | Device join key fallback. Use `lo_id` when available because aggregate `device_level_v1.device_id` is based on `jgr_lo_id`. |
| `sample_weight` | DOUBLE | `1.0 / nullif(downsampling_rate, 0.0)` | Optional inverse-probability weight for downsampled training/evaluation. |

---

## 4. Join Contract With Aggregation Tables

GMinor rows are event-grain samples. Aggregate rows are historical feature
snapshots keyed by hour and reviewed dimension families.

### 4.1 Preferred Enrichment Flow

1. Parse `timestamp` into `source_event_time`.
2. Join GMinor to `ml_shadow.realtime_attributed_event_wide` by `event_id` when
   the wide-table event exists.
3. Derive the same `device_level_v1` and `non_device_context_v1` dimension keys
   that are defined in `realtime_attributed_aggregation_table_schema.md`.
4. Join to hourly aggregate tables with a strict point-in-time predicate.

### 4.2 Device-Level Join

Use this path for device history features.

```sql
SELECT
  g.*,
  a.*
FROM gminor_enriched g
LEFT JOIN ml_shadow_feature.realtime_attributed_device_level_hly a
  ON a.device_id = g.effective_device_id
 AND a.event_time < g.source_event_time
QUALIFY row_number() OVER (
  PARTITION BY g.event_id
  ORDER BY a.event_time DESC
) = 1
```

If the aggregate table contains multiple device dimensions beyond `device_id`,
derive those dimensions from the wide-table enrichment and include them in the
join. Do not join on raw `device_id` when `lo_id` is present.

> **Note (device key updated 2026-07-02):** the aggregate `device_level_v1.device_id` is now
> `normalize_device_id(jgr_dev_normalized_id)` (not `jgr_lo_id`, which is empty upstream). The
> `gminor_attributed_join` job therefore joins on **`device_dim_id`** derived from the wide bridge
> using the same `agg_specs` recipe as the aggregation job — GMinor's own `device_id`/`lo_id` are
> carried as columns, not used as the join key. See design spec 2026-07-02-gminor-attributed-join.

### 4.3 Non-Device Context Join

Use this path for inventory/supply/context history features.

```sql
SELECT
  g.*,
  a.*
FROM gminor_enriched g
LEFT JOIN ml_shadow_feature.realtime_attributed_non_device_context_hly a
  ON a.context_dim_id = g.context_dim_id
 AND a.event_time < g.source_event_time
QUALIFY row_number() OVER (
  PARTITION BY g.event_id
  ORDER BY a.event_time DESC
) = 1
```

`context_dim_id` must be computed from the normalized aggregation dimensions in
`non_device_context_v1`. Do not use event-level ids, bid ids, winner ids,
campaign ids, creative ids, or outcome fields as context dimensions.

### 4.4 Fallback Without Wide-Table Join

If the wide-table row is unavailable, limited joins are still possible:

- Device aggregate: use `lo_id`, falling back to `device_id`.
- Non-device aggregate: extract only approved aggregation dimensions from
  `features` if `feature_schema_version` defines a stable parser for them.

Rows without enough dimension data should remain unmatched rather than joining
to an overly broad or incorrect aggregate key.

---

## 5. Validation Rules

- `project_name` must match `^[A-Za-z_]+$`.
- `experiment_id` must be `>= 0`.
- `traffic_allocation` and `downsampling_rate` must be in `[0, 1]` when present.
- `event_id` must be non-null for event/wide-table joins.
- At least one of `lo_id` or `device_id` is required for device-level aggregate joins.
- `features`, `predictions`, and `tags` must be parseable according to `feature_schema_version` and project-specific decoders before feature extraction.
- Aggregate feature joins must use historical rows only: `aggregate.event_time < source_event_time`.
- **Point-in-time uses strict prior hour:** the implementation joins with
  `aggregate.event_time < date_trunc('hour', source_event_time)` (prior hour only), which supersedes
  the `< source_event_time` shown in the §4.2/§4.3 inline examples (that would admit the same hour,
  whose aggregate can contain at/after-event data).

