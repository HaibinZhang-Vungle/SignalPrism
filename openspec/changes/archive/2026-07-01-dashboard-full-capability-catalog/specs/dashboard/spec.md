## MODIFIED Requirements

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
