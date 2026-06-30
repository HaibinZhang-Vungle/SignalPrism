# Chapter 7: End-to-End MLOps Demo on the Realtime Wide Table

本章在 realtime attribution wide table 的基础上，设计一个 demo 级别的 end-to-end MLOps 方案。目标不是一次性做到 production-grade feature platform，而是证明一个闭环：

```
wide event table
  -> configurable aggregation
  -> feature capability registry
  -> derived feature formula
  -> offline simulation with GMinor self-served logs
  -> visual feature workbench
  -> candidate feature set for later productionization
```

核心原则：

- 新 feature 的大部分数据准备工作通过配置完成，不需要为每个 feature 写一条新 pipeline。
- 自由 dimension 不能完全开放，否则成本会失控；demo 只允许几个固定 dimension family。
- ML 可以快速挑 feature 做 offline simulation，但 demo 阶段不承诺等价于 online A/B。
- UI 展示的是 feature 能力、数据质量、成本、simulation 结果和 lineage，而不是只做一个表单。

## 7.1 Demo Scope

### In scope

- 从 `ml_shadow.realtime_attributed_event_wide` 或等价 shadow wide table 读取字段能力。
- 用户通过配置选择 source columns、aggregation strategy、dimension family、window、公式。
- 自动生成 shadow aggregation table / feature view。
- 自动注册 feature metadata，展示 lineage、coverage、null rate、cost estimate。
- ML 用户选择 feature set，构建离线训练/模拟数据集。
- 与 GMinor self-served log 或 sampled served feature log 按 `event_id` 组合。
- UI 支持 feature discovery、formula builder、simulation launcher、结果对比。

### Out of scope for the demo

- 不直接替换 production aggregation pipeline。
- 不自动发布到 GMinor production serving。
- 不承诺完全可靠的 counterfactual simulation。真实 production counterfactual 仍需要完整 bid response array、shading curve、served feature log、policy replay semantics。
- 不开放任意 dimension 组合。

## 7.2 End-to-End Architecture

```
Realtime attributed wide table
  |
  | 1. scan schema + profiling
  v
Feature Capability Catalog
  |
  | 2. user picks columns, windows, dimension family, aggregation strategy
  v
Aggregation Config Compiler
  |
  | 3. generates Spark/Trino/dbt SQL from templates
  v
Shadow Aggregation Tables
  |
  | 4. formula compiler builds derived features
  v
Shadow Feature Views / Feature Sets
  |
  | 5. point-in-time dataset builder
  v
Offline Simulation Dataset
  |
  | 6. combine with GMinor self-served logs and labels
  v
Simulation Runner
  |
  | 7. metrics, SHAP, lift, cohort diagnostics
  v
Feature Workbench UI
```

The important architectural split:

- **Wide table** stores event-level facts after realtime attribution.
- **Capability catalog** records what can be aggregated.
- **Aggregation config** defines reusable primitive materialization.
- **Feature formula** derives ML features from primitives.
- **Feature set** is the selected list used by one simulation.
- **Simulation run** records one offline experiment result.

## 7.3 Core Metadata Objects

### 7.3.1 Feature Capability

A capability is a raw or attributed wide-table column that can become a primitive or derived feature.

```
feature_capability:
  capability_id: hbn_settlement_price
  source_table: ml_shadow.realtime_attributed_event_wide
  source_column: hbn_settlement_price
  data_type: DOUBLE
  semantic_type: money_cpm
  source_event_type: hbn
  allowed_aggregation_strategies:
    - numeric_distribution
    - numeric_sum_count
  allowed_dimension_families:
    - non_device_context_v1
    - device_level_v1
  default_windows:
    - 1h
    - 1d
    - 7d
  null_semantics: null_means_not_observed
  owner: supply_ai
  status: available
```

Capability is not a feature yet. It only says: this data exists, it is understood, and the platform knows how to aggregate it safely.

### 7.3.2 Aggregation Spec

An aggregation spec turns capabilities into mergeable primitives.

```
aggregation_spec:
  agg_id: hbn_price_context_v1
  source: ml_shadow.realtime_attributed_event_wide
  time_column: source_event_time
  event_filter: source_event_type in ('hbn', 'delivery')
  dimension_family: non_device_context_v1
  windows: [1h, 1d, 7d]
  output_namespace: ml_shadow_feature
  measures:
    - capability_id: hbn_settlement_price
      strategy: numeric_distribution
    - capability_id: hbn_bid_price
      strategy: numeric_distribution
    - capability_id: delivery_count
      strategy: count_event
    - capability_id: tpat_event_start_count
      strategy: numeric_sum_count
```

The compiler produces primitive columns like:

```
hbn_settlement_price_sum
hbn_settlement_price_count
hbn_settlement_price_sum_sq
hbn_settlement_price_min
hbn_settlement_price_max
hbn_settlement_price_ddsketch
delivery_count
tpat_event_start_count_sum
tpat_event_start_count_count
```

### 7.3.3 Derived Feature Spec

A derived feature is formula + source primitives + time window + dimension family.

```
derived_feature:
  feature_id: avg_hbn_settlement_price_7d
  display_name: Avg HBN settlement price, 7d
  dimension_family: non_device_context_v1
  window: 7d
  formula: safe_div(hbn_settlement_price_sum, hbn_settlement_price_count)
  source_primitives:
    - hbn_settlement_price_sum
    - hbn_settlement_price_count
  fill_policy:
    default: null
    model_input: nan
  status: proposed
```

### 7.3.4 Feature Set

A feature set is what ML actually tests.

```
feature_set:
  feature_set_id: floor_model_candidate_2026_06_demo
  base_feature_set: production_floor_model_current
  added_features:
    - avg_hbn_settlement_price_7d
    - hbn_settlement_price_cv_7d
    - tpat_start_rate_7d
    - bid_premium_rate_1d
  removed_features: []
  owner: mle
  purpose: offline_floor_simulation
```

### 7.3.5 Simulation Run

```
simulation_run:
  run_id: sim_20260628_001
  feature_set_id: floor_model_candidate_2026_06_demo
  dataset_id: ds_20260620_20260627_sample_001pct
  model_family: catboost
  baseline: production_logged_prediction
  treatment: retrained_with_candidate_features
  metrics:
    r2_delta: 0.012
    top_decile_lift: 0.034
    nr_proxy_delta: 0.008
  status: completed
```

## 7.4 Fixed Dimension Families

The UI must not allow arbitrary dimensions. It should offer a small set of reviewed dimension families. This keeps cost predictable and makes materialized tables reusable.

### 7.4.1 `device_level_v1`

Purpose: user/device history features.

```
dimension_family: device_level_v1
keys:
  - lo_id_or_device_id
  - dev_platform
  - placement_type
  - geoip_country_code
optional_buckets:
  - pub_app_object_id_top_bucket
```

Use cases:

- device-level bid/settlement history
- device-level TPAT/event-start tendency
- device-level win/loss history
- KVRocks export demo

Cost profile:

- highest row count
- strict sample required for demo
- limited measure count
- no high-cardinality extra dimensions

### 7.4.2 `non_device_context_v1`

Purpose: the main non-device feature plane. This is the dimension family the user described: all important context except device identity.

```
dimension_family: non_device_context_v1
keys:
  - pub_app_object_id
  - placement_id
  - placement_type
  - pub_account_id
  - supply_name
  - is_header_bidding
  - rtb_connection_id
  - winner_account_id
  - jaeger_experiment_id
  - ml_experiment_id
  - geoip_country_code
  - dev_platform
  - os_version_major
  - dev_model_bucket
  - dev_connection
  - dev_id_source
```

Notes:

- `dev_model_bucket` should be normalized/top-N bucketed, not raw free-text model.
- `adomain` should not be included by default; for demo it can be a separate top-N MAP primitive or a separate advertiser-focused dimension family.
- This family is still high-cardinality, but much cheaper than per-device.

### 7.4.3 `inventory_context_lite_v1`

Purpose: fallback and cost-safe broad coverage.

```
dimension_family: inventory_context_lite_v1
keys:
  - pub_app_object_id
  - placement_id
  - placement_type
  - supply_name
  - rtb_connection_id
  - geoip_country_code
  - dev_platform
```

Use cases:

- fast preview
- broad simulation
- backoff features when `non_device_context_v1` is sparse
- productionization candidate after demo

### 7.4.4 `global_baseline_v1`

Purpose: low-cardinality priors and fallback.

```
dimension_family: global_baseline_v1
keys:
  - placement_type
  - supply_name
  - rtb_connection_id
  - geoip_country_code
  - dev_platform
```

Use cases:

- cold start
- null backfill
- dashboard baseline
- cheap always-on comparison

## 7.5 Aggregation Strategy Library

The user should choose strategies, not write Spark code.

| Strategy | Input type | Stored primitives | Derived examples |
|---|---|---|---|
| `count_event` | any event | `count` | delivery count, HBN count |
| `count_if` | boolean predicate | `count_true`, `count_total` | win rate, no-serv rate |
| `numeric_sum_count` | numeric | `sum`, `count`, `min`, `max` | average, rate numerator |
| `numeric_distribution` | numeric | `sum`, `count`, `sum_sq`, `min`, `max`, `ddsketch` | avg, stddev, CV, p50, p90 |
| `money_cpm` | numeric money | same as distribution + winsorized sum | robust CPM features |
| `cardinality_hll` | id | `hll_sketch` | unique devices, unique apps |
| `topk_map` | categorical + metric | `topk_map` | top adomain share, DSP concentration |
| `latest_value` | ordered event | `last_value`, `last_event_time` | latest floor, latest model version |
| `ratio_pair` | numerator/denominator | numerator primitives + denominator primitives | bid density, tpat rate |

The compiler can infer a default strategy from `semantic_type`, but the UI should show and allow reviewed alternatives.

## 7.6 Formula DSL

Do not allow arbitrary SQL in the UI. Use a small expression DSL that compiles to SQL AST.

Allowed functions for the demo:

```
safe_div(a, b)
coalesce(a, b)
log1p(x)
sqrt(x)
abs(x)
least(a, b)
greatest(a, b)
clip(x, min, max)
avg(sum_x, count_x)
variance(sum_x, sum_sq_x, count_x)
stddev(sum_x, sum_sq_x, count_x)
cv(sum_x, sum_sq_x, count_x)
rate(count_true, count_total)
dd_percentile(sketch, p)
hll_count(sketch)
case_when(condition, value, else_value)
```

Example derived features:

```
avg_bid_price_7d =
  safe_div(hbn_bid_price_sum_7d, hbn_bid_price_count_7d)

hbn_settlement_price_cv_7d =
  cv(hbn_settlement_price_sum_7d,
     hbn_settlement_price_sum_sq_7d,
     hbn_settlement_price_count_7d)

bid_premium_rate_1d =
  safe_div(sr_pbtw_sum_1d - settlement_price_sum_1d,
           settlement_price_sum_1d)

tpat_start_rate_7d =
  safe_div(tpat_event_start_count_sum_7d,
           delivery_count_7d)

floor_headroom_7d =
  safe_div(hbn_bid_price_sum_7d, hbn_bid_price_count_7d)
  - safe_div(mediation_floor_sum_7d, mediation_floor_count_7d)
```

Validation rules:

- Every referenced primitive must exist for the same dimension family and window.
- Division must use `safe_div`.
- Numeric output type must be known.
- Feature must declare null/fill policy.
- Formula cannot reference future labels.
- Formula cannot reference raw event columns unless marked as realtime/direct feature.

## 7.7 Config-Driven Aggregation Flow

### Step 1: Capability scan

The system profiles the wide table:

```
columns
data types
source event type coverage
null rate
distinct count
p50/p95 payload size if nested
event-time freshness
join/attribution hit rate
```

Only columns passing basic profiling become `available` capabilities.

### Step 2: User creates aggregation config

The UI writes a YAML/JSON config, not pipeline code.

```
agg_id: demo_tpat_hbn_context_v1
source: ml_shadow.realtime_attributed_event_wide
event_filter: source_event_type in ('hbn', 'tpat', 'delivery')
time_column: source_event_time
sample:
  type: event_id_hash
  rate: 0.01pct
dimension_family: non_device_context_v1
windows:
  - 1d
  - 7d
measures:
  - capability_id: hbn_bid_price
    strategy: numeric_distribution
  - capability_id: hbn_settlement_price
    strategy: money_cpm
  - capability_id: delivery_count
    strategy: count_event
  - capability_id: tpat_event_start_count
    strategy: numeric_sum_count
output:
  table_prefix: ml_shadow_feature.demo_tpat_hbn_context
```

### Step 3: Compiler produces plan

Compiler outputs:

- generated SQL/Spark plan
- estimated row count
- estimated bytes/day
- fixed dimension family validation
- source capability validation
- backfill range
- table names

Example output:

```
ml_shadow_feature.demo_tpat_hbn_context_non_device_daily
ml_shadow_feature.demo_tpat_hbn_context_non_device_7d
ml_shadow_feature.demo_tpat_hbn_context_non_device_features
```

### Step 4: Materialize primitives

For demo, use a sampled Argo/Spark job or Trino CTAS.

The job is generic:

```
GenericAggregationRunner(config_id)
```

No feature-specific code path should be added.

### Step 5: Build derived feature view

The formula compiler creates a feature view:

```
feature_time
dimension keys
feature columns
quality columns
```

Quality columns:

```
feature_coverage
source_event_count
null_rate
sample_rate
window_start
window_end
```

## 7.8 Cost Controls

Cost guardrails must be part of the demo. Otherwise a self-serve UI will create tables nobody can afford.

Controls:

- fixed dimension families only
- row-count estimate before materialization
- sample-first execution
- max measures per aggregation spec
- max windows per spec
- max sketch columns per spec
- cardinality cap per dimension key
- top-N bucketing for high-cardinality strings
- no raw `device_id` in non-device family
- no arbitrary free-text dimensions
- preview on 1 day before 7-day or 30-day

Cost preview displayed in UI:

```
estimated rows/day
estimated GB/day
estimated files/day
estimated Spark CPU hours
expected null rate
expected feature coverage
warnings:
  - dev_model raw cardinality too high, use dev_model_bucket
  - adomain not allowed in non_device_context_v1, use topk_map
  - 7d window requires daily primitive backfill
```

Hard gate example:

```
if estimated_rows_per_day > 500M for demo:
  block materialization
  suggest inventory_context_lite_v1 or lower sample rate
```

## 7.9 Offline Simulation Design

The demo simulation goal is to answer:

> If ML adds these candidate features, do offline metrics improve enough to justify productionizing the feature pipeline?

It is not a perfect online counterfactual engine.

### 7.9.1 Data inputs

```
1. Feature views from configurable aggregation
2. Realtime attributed wide table labels/outcomes
3. GMinor self-served log or sampled served feature log
4. Current production feature set metadata
5. Optional current production model predictions
```

### 7.9.2 GMinor self-served log requirement

The current docs note that exact served feature logging does not exist yet. For this demo, add a sampled log with the smallest useful schema:

```
gminor_self_served_log_sampled

event_id
request_time
endpoint                         -- RunFloorOptimization, RunDynamicThrottling, RunAuctionDynamics
model_key
model_version
feature_schema_version
jaeger_experiment_id
ml_experiment_id
pub_app_object_id
placement_id
rtb_connection_id
geoip_country_code
dev_platform
lo_id_or_device_id_hash
served_feature_vector            -- protobuf/json/map for demo
served_feature_names
prediction_value
decision_value                   -- floor, throttle decision, multiplier, etc.
fallback_used
lookup_status
latency_ms
```

Sampling should use the same deterministic event-id cohort as the wide-table demo.

Why this matters:

- It gives the baseline feature vector actually used by GMinor.
- It captures model version and experiment assignment.
- It allows offline replay to compare current logged prediction against treatment prediction.
- It prevents feature leakage from reconstructing baseline features incorrectly.

### 7.9.3 Dataset builder

The dataset builder performs point-in-time joins:

```
gminor_self_served_log_sampled s
  LEFT JOIN feature_view f
    ON dimension keys match
   AND f.feature_time <= s.request_time
  LEFT JOIN wide_table labels l
    ON s.event_id = l.event_id
```

Point-in-time correctness is mandatory:

- feature windows must end before or at request time
- labels must not be used in feature formulas
- same event should not contribute to its own trailing feature unless explicitly allowed for direct realtime features

Output:

```
ml_shadow_simulation.dataset_<dataset_id>
```

Columns:

```
event_id
request_time
baseline model metadata
baseline served features
candidate features
labels
cohort dimensions
quality flags
```

### 7.9.4 Simulation modes

#### Mode A: Feature importance smoke test

Train a small CatBoost model on sampled data:

```
baseline = current feature set reconstructed from logs
treatment = baseline + candidate features
```

Compare:

- R2 / MAE for continuous labels such as NR, settlement, bid price
- AUC / logloss for binary labels such as event_start, win/loss, no-serv
- top-decile lift
- SHAP rank of new features
- feature coverage

This is the fastest demo and does not require exact policy replay.

#### Mode B: Production prediction replay

Use logged production prediction as baseline:

```
baseline_score = prediction_value from GMinor log
treatment_score = batch_score(model trained with candidate features)
```

Compare prediction quality by cohort:

- app
- placement
- rtb connection
- geo
- platform
- experiment

This is useful even if the production model artifact is not locally replayable.

#### Mode C: Shadow GMinor batch scoring

If a GMinor model artifact can be run offline:

```
served feature vector + candidate features
  -> batch GMinor/CatBoost scorer
  -> treatment prediction
```

This is closer to real serving, but still demo-level unless all online feature transformations and fallback paths are identical.

#### Mode D: Simple policy simulation

For demo only:

- floor model: compare predicted optimal floor against observed settlement/win outcome
- dynamic throttling: estimate saved QPS vs lost revenue proxy
- auction dynamics: compare duplicate/sequential strategy labels if available

Keep the assumptions visible in the UI. Do not present this as final revenue truth.

### 7.9.5 Simulation metrics

Model quality:

```
r2
mae
auc
logloss
calibration error
top decile lift
SHAP rank
permutation importance
```

Business proxy:

```
net_revenue_proxy_delta
adv_spend_proxy_delta
pub_revenue_proxy_delta
win_rate_delta
tpat_event_start_rate_delta
qps_saved_proxy
```

Data quality:

```
feature_coverage
null_rate
point_in_time_join_rate
gminor_log_join_rate
wide_table_label_join_rate
sample_size
cohort skew
```

Cost and serving readiness:

```
aggregation_storage_estimate
kv_export_size_estimate
lookup_key_count
estimated serving latency
feature freshness
```

## 7.10 UI Design

The UI should feel like an ML feature cockpit, not a BI dashboard.

### 7.10.1 Screen 1: Capability Map

Purpose: show all available data from the wide table and where it can go.

Visual elements:

- source event tabs: delivery, no-serv, HBN, TPAT
- capability cards grouped by domain: price, floor, settlement, TPAT, device, privacy, creative, experiment
- lineage graph:

```
wide column -> primitive -> derived feature -> feature set -> simulation run
```

Each capability card shows:

```
coverage
null rate
freshness
allowed strategies
allowed dimension families
latest profiling status
sample values/distribution
```

### 7.10.2 Screen 2: Aggregation Builder

Purpose: configure a new aggregation without coding.

Controls:

- dimension family selector
- window selector
- capability multi-select
- aggregation strategy dropdown per capability
- sample rate selector
- preview cost button
- generated plan preview

Important UX:

- If the user selects a costly capability/dimension, show cost warnings immediately.
- If a dimension is not allowed, explain which fixed dimension family supports the closest use case.
- Show estimated output table names before run.

### 7.10.3 Screen 3: Formula Studio

Purpose: build derived features.

Layout:

- left: available primitives
- center: formula editor with autocomplete
- right: validation panel
- bottom: sample output distribution

Validation panel:

```
type check: pass
point-in-time risk: pass
division safety: pass
source primitive availability: pass
coverage estimate: 82.4%
correlation with existing features: 0.61
```

Formula UI can support two modes:

- guided mode: choose avg/rate/CV/percentile from menus
- advanced mode: DSL editor

### 7.10.4 Screen 4: Feature Registry View

Purpose: show all feature capabilities and lifecycle state.

Views:

- all possible capabilities
- currently materialized primitives
- proposed derived features
- approved features
- production features
- rejected/deprecated features

Each feature row:

```
feature_id
formula
dimension family
window
coverage
SHAP rank
simulation runs
serving status
owner
```

### 7.10.5 Screen 5: Simulation Lab

Purpose: ML chooses feature set and launches offline simulation.

Inputs:

- model endpoint: Floor / DTO / AuctionDynamics
- baseline feature set
- candidate features
- date range
- sample rate
- target label
- simulation mode

Output while running:

```
dataset build
point-in-time join
training/scoring
metric computation
SHAP computation
cohort report
```

### 7.10.6 Screen 6: Simulation Result

Make this screen visually strong:

- baseline vs treatment metric tiles
- SHAP beeswarm / bar chart
- lift curve
- calibration curve
- cohort heatmap
- feature coverage map
- prediction delta distribution
- event replay timeline for sampled examples

Result summary:

```
Recommended action:
  promote to approved candidate

Why:
  +1.2% R2
  +3.4% top-decile lift
  86% feature coverage
  stable across top 20 placements

Risks:
  dev_model_bucket has 28% OTHER
  geo=IN cohort shows negative lift
  7d window freshness requires daily materialization
```

### 7.10.7 Screen 7: Promotion Plan

The demo should produce a promotion checklist, not directly push production:

```
1. Create registry entry
2. Save aggregation config
3. Save feature formula
4. Attach simulation run result
5. Estimate production storage/serving cost
6. Mark as productionization candidate
```

For production later, this would become PR generation into the Lena schema/feature registry repo and a GMinor feature contract review.

## 7.11 Demo Data Model

Suggested shadow tables:

```
ml_shadow.feature_capabilities
ml_shadow.aggregation_specs
ml_shadow.aggregation_runs
ml_shadow.derived_feature_specs
ml_shadow.feature_sets
ml_shadow.simulation_datasets
ml_shadow.simulation_runs
ml_shadow.simulation_metrics
```

Materialized outputs:

```
ml_shadow_feature.<agg_id>_<dimension_family>_daily
ml_shadow_feature.<agg_id>_<dimension_family>_7d
ml_shadow_feature.<agg_id>_<dimension_family>_features
ml_shadow_simulation.dataset_<dataset_id>
ml_shadow_simulation.predictions_<run_id>
```

## 7.12 Minimal Demo Plan

### Week 1: Wide table + capability catalog

- Use a sampled realtime attributed wide table.
- Profile 20-40 candidate columns from delivery/HBN/TPAT.
- Build initial capability catalog.
- Define the four dimension families.

### Week 2: Config compiler + first aggregations

- Implement config schema.
- Build generic aggregation runner.
- Support `numeric_sum_count`, `numeric_distribution`, `count_event`, `count_if`.
- Materialize one non-device and one device-level aggregation on sampled data.

### Week 3: Formula + feature registry

- Implement Formula DSL validation.
- Create 10-20 derived features.
- Show feature lineage and coverage in UI.

### Week 4: GMinor log + simulation

- Add sampled GMinor self-served log or simulate it from available request/prediction logs.
- Build point-in-time simulation dataset.
- Run baseline vs treatment CatBoost smoke test.
- Produce SHAP/lift/cohort reports.

### Week 5: UI polish

- Build Capability Map, Aggregation Builder, Formula Studio, Simulation Lab.
- Add cost preview and result dashboard.
- Prepare demo script:
  1. pick HBN/TPAT capabilities
  2. create 7d non-device aggregation
  3. derive `bid_premium_rate_7d`
  4. add it to a floor-model feature set
  5. run offline simulation
  6. show improvement/risks

## 7.13 Example Demo Story

Question:

> Does HBN settlement behavior by placement/RTB/geo improve floor model offline prediction?

Steps:

1. In Capability Map, select:
   - `hbn_settlement_price`
   - `hbn_bid_price`
   - `delivery_count`
   - `tpat_event_start_count`
2. In Aggregation Builder:
   - dimension family: `non_device_context_v1`
   - window: `7d`
   - strategy: `numeric_distribution`
3. In Formula Studio, create:

```
avg_hbn_settlement_price_7d
hbn_settlement_price_cv_7d
bid_premium_rate_7d
tpat_start_rate_7d
```

4. In Simulation Lab:
   - baseline: current floor model feature set
   - treatment: baseline + four candidate features
   - data: sampled GMinor floor requests from last 7 days
   - label: observed settlement / net revenue proxy
5. Result page shows:
   - R2 delta
   - top-decile lift
   - SHAP rank of new features
   - cohort heatmap by geo/platform/RTB
   - feature coverage and cost estimate

This demonstrates the whole idea without requiring production rollout.

## 7.14 Productionization Path After Demo

If the demo succeeds, productionization should be deliberate:

1. Promote dimension families to reviewed contracts.
2. Move capability catalog into the existing Feature Registry model.
3. Replace demo GMinor log with real served feature logging.
4. Add CI validation for formula DSL and aggregation configs.
5. Generate PRs for Lena schema/dbt changes from approved configs.
6. Add feature freshness and drift monitoring.
7. Add GMinor protobuf/feature gateway review before serving any new feature.

The demo proves speed of feature discovery. Productionization must still enforce reliability, cost, privacy, and serving latency.

## 7.15 Recommended Demo Build

Build the demo around three artifacts:

1. **Configurable aggregation engine**: generic runner that turns capability + strategy + fixed dimension family into primitive tables.
2. **Feature workbench UI**: capability map, formula studio, feature registry, simulation lab.
3. **Offline simulation loop**: point-in-time dataset builder combining candidate features with GMinor self-served logs and wide-table labels.

The highest-value first demo is:

```
HBN/TPAT wide table
  -> non_device_context_v1 7d aggregation
  -> 4 derived features
  -> floor model offline simulation
  -> UI result dashboard
```

This directly shows why realtime wide-table attribution matters: once the attribution burden is moved upstream, ML feature creation becomes a configuration and validation workflow rather than a multi-week pipeline engineering project.
