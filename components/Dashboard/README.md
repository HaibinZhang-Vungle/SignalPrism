# Dashboard

This component owns the Feature Workbench user experience for the demo.

In the TRD, the dashboard is the operator-facing surface for exploring
capabilities, configuring aggregations, building formulas, and reviewing
simulation results. It should make the feature workflow visible without asking
users to write pipeline code.

Core functions:

- Capability Map: show available wide-table columns, profiling status,
  coverage, freshness, allowed aggregation strategies, and allowed dimension
  families.
- Aggregation Builder: let users choose dimensions, windows, capabilities,
  strategies, sample rates, and preview generated table names and costs.
- Formula Studio: help users compose derived features with type checks,
  point-in-time validation, coverage estimates, and sample distributions.
- Simulation Lab: compare baseline and treatment runs with metrics, lift,
  SHAP-style importance, and cohort diagnostics.
- Surface lineage from wide-table column to primitive, derived feature, feature
  set, and simulation run.

The dashboard should stay focused on the reviewed demo workflow and should not
allow arbitrary dimension combinations.

