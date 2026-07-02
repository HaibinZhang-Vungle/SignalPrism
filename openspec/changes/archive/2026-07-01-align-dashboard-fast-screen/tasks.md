## 1. Data Contract

- [x] 1.1 Extend `FeatureCapability` with `family`, `bucketConcentration`, `psi`, `klDivergence`, `baseSeparation`
- [x] 1.2 Add `ResidualPocket`, `FieldFamily`, `DistributionStats`, `ScreenVerdict`, `ScreenedField` types
- [x] 1.3 Add `listResidualPockets`, `listFieldFamilies`, `screenFields(pocketId)` to `WorkbenchDataSource`

## 2. Screening Logic

- [x] 2.1 Implement pure `screenField(cap, pocket)` → stats + verdict + reasons (design D1, D2)
- [x] 2.2 Unit-test verdict thresholds (strong / weak / blocked, drift flag, pocket-relative separation)

## 3. Fixtures

- [x] 3.1 Author `residualPockets.json` with proposed field families
- [x] 3.2 Extend `capabilities.json` with family + distribution stats
- [x] 3.3 Implement the new adapter methods; keep fixture-drift guard green

## 4. Residual Diagnostics surface (spec: residual diagnostics)

- [x] 4.1 List pockets with residual-vs-baseline error, traffic share, proposed families
- [x] 4.2 Select a pocket → scope the Distribution Screen (shared selected-pocket state)

## 5. Distribution Screen surface (spec: distribution screen + promotion gate)

- [x] 5.1 Show per-field coverage, missingness, bucket concentration, KL/PSI drift, subgroup separation
- [x] 5.2 Rank fields by evidence; flag drift (PSI over threshold)
- [x] 5.3 Verdict badges (strong/weak/blocked) with reasons; promote only strong fields
- [x] 5.4 Lift promotion state to `App` as a promoted `Set` (design D3)

## 6. Reframe downstream

- [x] 6.1 Reorder nav around the fast screen (design D4)
- [x] 6.2 Gate Aggregation Builder to promoted capabilities only; empty-state directs to the screen

## 7. Verification

- [x] 7.1 Tests for the promotion gate and Aggregation Builder empty/promoted states
- [x] 7.2 Run lint + tests + build green; screenshot the new fast-screen flow
