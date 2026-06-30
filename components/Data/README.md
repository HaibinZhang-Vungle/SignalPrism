# Data

This component owns the demo data contracts, metadata objects, and materialized
feature data products.

In the TRD, it covers the Feature Capability Catalog, Aggregation Config
Compiler outputs, Shadow Aggregation Tables, feature views, feature sets, and
simulation datasets. It is the layer that turns profiled wide-table columns into
reviewed, reusable feature primitives.

Core functions:

- Maintain schema contracts for wide-table-derived aggregation tables and GMinor
  log joins.
- Define reviewed dimension families such as `device_level_v1` and
  `non_device_context_v1`.
- Store aggregation specs, aggregation runs, derived feature specs, feature
  sets, simulation datasets, and simulation metrics.
- Support generic materialization plans for numeric distributions, sum/count
  metrics, event counts, and conditional counts.
- Enforce point-in-time joins between GMinor samples, aggregation tables, and
  wide-table labels.

This component should prefer declarative configs and schemas over
feature-specific pipeline code.

