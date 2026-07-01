## ADDED Requirements

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

## MODIFIED Requirements

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
