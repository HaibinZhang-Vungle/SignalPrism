## 1. Tooling & Scaffolding

- [x] 1.1 Establish frontend tooling in `components/Dashboard/` per design D6 (package manifest, TypeScript config, Vite dev/build, lint) — resolve the stack Open Question first
- [x] 1.2 Add a charting dependency capable of SHAP beeswarm / lift curve / cohort heatmap
- [x] 1.3 Set up a test runner and a single smoke test that renders the app shell
- [x] 1.4 Create the app shell with navigation across the five surfaces

## 2. Data Access Layer

- [x] 2.1 Define the `WorkbenchDataSource` interface (list capabilities, preview aggregation cost, validate formula, list/launch/read simulation runs) per design D1
- [x] 2.2 Define TypeScript types for the `feature_capability` catalog shape (TRD §7.3.1) mapped from schema `semantic_type`/`feat`/`null`/`enum_ref` (design D2)
- [x] 2.3 Author fixture JSON snapshots (capabilities catalog, primitives, simulation runs) derived from the schema and TRD shapes
- [x] 2.4 Implement the fixture adapter against `WorkbenchDataSource`
- [x] 2.5 Ship the four fixed dimension families as checked-in config (TRD §7.4, design D4)

## 3. Capability Map (spec: Capability Map surfaces available wide-table columns)

- [x] 3.1 Render capability cards grouped by domain with coverage, null rate, freshness, allowed strategies, allowed dimension families
- [x] 3.2 Exclude `feat = exclude` (PII) capabilities from the selectable list
- [x] 3.3 Exclude columns that have not passed profiling (not `available`)
- [x] 3.4 Add source-event tabs (delivery / no-serv / HBN / TPAT)

## 4. Aggregation Builder (spec: configures aggregations without code + reviewed-family restriction)

- [x] 4.1 Build selectors for dimension family, windows, capabilities, per-capability strategy, and sample rate
- [x] 4.2 Show generated output table names and estimated cost (rows/day, bytes/day) before materialization
- [x] 4.3 Emit cost/cardinality warnings naming the safer alternative (e.g. bucketed field / lighter family)
- [x] 4.4 Restrict dimensions to the reviewed families; block arbitrary dimensions and point to the closest supported family
- [x] 4.5 Serialize the builder state to the §7.7 Step 2 aggregation config object (no materialization performed)

## 5. Formula Studio (spec: composes validated derived features)

- [x] 5.1 Implement the §7.6 DSL parser → AST (design D3)
- [x] 5.2 Validate type correctness, division safety, and source-primitive availability for the same (dimension family, window)
- [x] 5.3 Enforce the point-in-time / no-future-label rule; reject label & `leak_risk` fields as direct inputs
- [x] 5.4 Show coverage estimate and sample output distribution in the validation panel
- [x] 5.5 Provide guided (menu) and advanced (DSL editor) modes

## 6. Simulation Lab (spec: compares baseline and treatment runs)

- [x] 6.1 Build the feature-set assembler (baseline, candidate features, date range, sample rate, target label, mode)
- [x] 6.2 Wire launch/read through `WorkbenchDataSource` (resolved: display-only from fixtures; launch stubbed/disabled)
- [x] 6.3 Render baseline-vs-treatment metric tiles, lift curve, SHAP-style importance, and cohort breakdown

## 7. Lineage (spec: surfaces end-to-end lineage)

- [x] 7.1 Render the lineage chain: wide-table column → primitive → derived feature → feature set → simulation run
- [x] 7.2 Link lineage nodes to their originating surface (capability card, formula, feature set, run)

## 8. Verification

- [x] 8.1 Add tests covering each spec scenario (excluded PII, un-profiled hidden, arbitrary-dim rejected, label-in-formula rejected, cost warning)
- [x] 8.2 Add a fixture-vs-`WorkbenchDataSource`-types check to guard against fixture drift (design risk)
- [x] 8.3 Run lint + tests + build green; confirm the demo runs against fixtures with no live pipeline dependency
