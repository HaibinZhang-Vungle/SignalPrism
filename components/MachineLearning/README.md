# MachineLearning

This component owns the offline modeling and simulation loop for candidate
features.

In the TRD, it corresponds to the Offline Simulation Dataset, Simulation Runner,
baseline-vs-treatment comparison, SHAP/lift analysis, and optional shadow
GMinor batch scoring. Its goal is to prove whether new wide-table-derived
features improve model quality before any production serving change.

Core functions:

- Build point-in-time training and evaluation datasets from GMinor logs,
  historical aggregate features, and wide-table labels.
- Compare baseline logged GMinor predictions against treatment models or
  feature sets.
- Run lightweight smoke tests such as CatBoost training, prediction replay, and
  simple policy simulations.
- Report metrics such as R2, MAE, AUC, logloss, top-decile lift, coverage, and
  cohort diagnostics.
- Validate that candidate features do not use labels or same-event future
  information.

Production GMinor serving remains out of scope for this demo component.

