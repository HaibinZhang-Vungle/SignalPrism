## ADDED Requirements

### Requirement: Feature Registry browses existing aggregate features

The dashboard SHALL provide a Feature Registry surface that browses the aggregate features defined in the aggregation table schema: the reviewed aggregation tables and their dimension family, the shared metric catalog (distribution metric families and count metrics), and each table's dimension columns. Distribution metrics SHALL display their generated `_sum/_count/_min/_max/_squaresum` columns; metrics that are label-like / point-in-time sensitive SHALL be flagged. The registry catalog SHALL be sourced from the schema, not hand-maintained per field.

#### Scenario: Operator browses aggregate features by table

- **WHEN** the operator opens the Feature Registry and selects an aggregation table (`device_level_v1` or `non_device_context_v1`)
- **THEN** the dashboard lists the metric catalog and that table's dimension columns
- **AND** each metric shows its kind (distribution or count) and source/derivation

#### Scenario: Distribution metric shows its generated columns

- **WHEN** a metric is a distribution family (e.g. `settlement_price`)
- **THEN** the registry shows its five generated columns (`_sum`, `_count`, `_min`, `_max`, `_squaresum`)

#### Scenario: Label-like metric is flagged

- **WHEN** a metric is label-like / point-in-time sensitive (e.g. `settlement_price`, `auction_winner_price`)
- **THEN** the registry flags it so it is not naively used as a feature input
