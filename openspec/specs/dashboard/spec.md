# dashboard Specification

## Purpose
TBD - created by archiving change add-dashboard-feature-workbench. Update Purpose after archive.
## Requirements
### Requirement: Capability Map surfaces available wide-table columns

The dashboard SHALL present a Capability Map that lists the **full wide-table schema catalog** — every field row from `schemas/realtime_attributed_wide_table_schema.md` — grouped by domain, showing for each field its schema metadata (`semantic_type`, `feat`, `null`, `enum_ref`) sourced without a second manual pass, plus profiling status, coverage, freshness, and (for feature candidates) allowed aggregation strategies and dimension families. Each field SHALL display its role from `feat`: `key`/`dim` fields are shown as identity/dimension entries (not feature candidates), `exclude` (PII) fields are shown but not selectable, and `feature`/`feature_after_encode`/`leak_risk` fields are the candidate pool. Only distribution-profiled feature candidates SHALL flow into the Distribution Screen; the full catalog SHALL NOT flood the screen.

#### Scenario: Operator browses the full catalog by domain

- **WHEN** the operator opens the Capability Map
- **THEN** the dashboard shows every schema field grouped by domain (supply, placement, device, geo, privacy, floor, auction, shading, settlement, creative, tpat, experiment, timing, identity, rtb)
- **AND** each field shows its `feat` role and schema metadata

#### Scenario: PII and excluded columns are shown but not offered

- **WHEN** a field is marked `feat = exclude` (e.g. raw IFA/IP/UA)
- **THEN** the dashboard shows it in the catalog marked as excluded/PII but does not offer it as a selectable feature candidate

#### Scenario: Dimensions and keys are catalogued but not feature candidates

- **WHEN** a field is marked `feat = dim` or `feat = key`
- **THEN** the dashboard shows it in the catalog as a dimension/identity entry and does not treat it as a feature candidate

#### Scenario: Only distribution-profiled candidates reach the screen

- **WHEN** the catalog contains feature candidates that have not been distribution-profiled
- **THEN** those fields appear in the Capability Map but are not listed in the Distribution Screen

### Requirement: Aggregation Builder configures aggregations without code

The dashboard SHALL let operators configure an aggregation by selecting a dimension family, one or more windows, capabilities, an aggregation strategy per capability, and a sample rate, and SHALL preview the generated output table names and an estimated cost before any materialization runs. It SHALL NOT require the operator to write pipeline or SQL code. The Aggregation Builder SHALL offer only capabilities that have been promoted from the Distribution Screen — nothing reaches aggregation (and therefore model training) until it has passed the distribution screen.

#### Scenario: Operator composes and previews an aggregation

- **WHEN** the operator selects a dimension family, windows, promoted capabilities, per-capability strategies, and a sample rate
- **THEN** the dashboard displays the generated output table names and an estimated cost (rows/day, bytes/day) before materialization

#### Scenario: Costly selection warns before running

- **WHEN** a selected capability or dimension exceeds a configured cost/cardinality threshold (e.g. raw high-cardinality `dev_model`)
- **THEN** the dashboard shows a warning and names the safer alternative (e.g. bucketed field or a lighter dimension family) before allowing the run

#### Scenario: Un-promoted fields are not available for aggregation

- **WHEN** no fields have been promoted from the Distribution Screen
- **THEN** the Aggregation Builder offers no capabilities and directs the operator to screen and promote fields first

### Requirement: Aggregation and dimension choices are restricted to reviewed families

The dashboard SHALL offer only the reviewed fixed dimension families (per TRD §7.4) and SHALL NOT allow arbitrary dimension combinations or arbitrary free-text dimensions.

#### Scenario: Arbitrary dimension is rejected

- **WHEN** the operator attempts to aggregate on a dimension outside the reviewed dimension families
- **THEN** the dashboard blocks the selection and directs the operator to the closest supported dimension family

### Requirement: Formula Studio composes validated derived features

The dashboard SHALL provide a Formula Studio for composing derived features from materialized primitives, and SHALL validate each formula for type correctness, point-in-time safety, division safety, and source-primitive availability, showing a coverage estimate and a sample output distribution. It SHALL reject formulas that reference future labels or label-adjacent (`leak_risk`) fields as inputs.

#### Scenario: Valid formula passes validation

- **WHEN** the operator composes a formula referencing existing primitives for the same dimension family and window using safe operators
- **THEN** the validation panel reports type check, point-in-time, and division-safety as passing and shows a coverage estimate

#### Scenario: Formula references a label field

- **WHEN** a formula references a field flagged as label / `leak_risk` (e.g. `jgr_settlement_price`) as an input rather than as a trailing-window aggregate ending before the current event
- **THEN** the dashboard fails validation with a point-in-time / leakage error

### Requirement: Simulation Lab compares baseline and treatment runs

The dashboard SHALL let operators assemble a feature set and launch an offline simulation, then present baseline-vs-treatment results including model-quality metrics, lift, SHAP-style feature importance, and cohort diagnostics.

#### Scenario: Operator launches and reviews a simulation

- **WHEN** the operator selects a baseline feature set, candidate features, date range, sample rate, target label, and simulation mode, and launches the run
- **THEN** the dashboard reports baseline-vs-treatment metrics, a lift curve, SHAP-style importance, and a cohort breakdown when the run completes

### Requirement: Dashboard surfaces end-to-end lineage

The dashboard SHALL surface lineage tracing each artifact from wide-table column to primitive to derived feature to feature set to simulation run.

#### Scenario: Operator traces a feature's lineage

- **WHEN** the operator inspects a derived feature
- **THEN** the dashboard shows the chain: wide-table column → primitive → derived feature → feature set → simulation run

### Requirement: Residual diagnostics surface high-error pockets for feature search

The dashboard SHALL provide a Residual Diagnostics surface that lists high-error residual pockets (subgroups where the current model underperforms), each showing its residual error versus the baseline, its traffic share, and the wide-table field families proposed for searching that pocket. Selecting a pocket SHALL scope the Distribution Screen to that pocket.

#### Scenario: Operator inspects residual pockets

- **WHEN** the operator opens Residual Diagnostics
- **THEN** the dashboard lists pockets with residual-vs-baseline error, traffic share, and proposed field families
- **AND** each pocket highlights the families worth searching (e.g. price shape, floor lifecycle, supply economics)

#### Scenario: Selecting a pocket drives the screen

- **WHEN** the operator selects a residual pocket
- **THEN** the Distribution Screen computes subgroup separation against that pocket and ranks fields for it

### Requirement: Distribution Screen ranks raw fields on distribution evidence

The dashboard SHALL provide a Distribution Screen that, for the selected residual pocket, shows each candidate raw field's distribution evidence — coverage, missingness, bucket concentration, KL/PSI-style drift, and subgroup separation — and ranks fields by that evidence. This screen SHALL be reachable and reviewable before any aggregation or model training.

#### Scenario: Operator reviews ranked distribution evidence

- **WHEN** the operator opens the Distribution Screen for a pocket
- **THEN** each field shows coverage, missingness, bucket concentration, KL/PSI drift, and subgroup separation
- **AND** fields are ranked so the strongest distribution evidence appears first

#### Scenario: Drift is flagged

- **WHEN** a field's PSI exceeds the stability threshold
- **THEN** the dashboard flags the field as drifting without necessarily blocking it

### Requirement: Only fields with strong distribution evidence are promoted

The dashboard SHALL assign each screened field a verdict of strong, weak, or blocked from its distribution evidence, and SHALL allow only strong fields to be promoted. Fields with insufficient coverage SHALL be blocked; fields that are near-constant (high bucket concentration) or that fail to separate the pocket SHALL be weak. Only promoted fields SHALL become available downstream.

#### Scenario: Strong field is promotable

- **WHEN** a field has adequate coverage, is not near-constant, and separates the residual pocket
- **THEN** the dashboard marks it strong and allows the operator to promote it

#### Scenario: Weak or low-coverage field cannot be promoted

- **WHEN** a field has low coverage, near-constant values, or low subgroup separation
- **THEN** the dashboard marks it blocked or weak and does not allow promotion, stating the reason

