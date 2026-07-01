## 1. Types & contract

- [x] 1.1 Add `screenProfiled?: boolean` to `FeatureCapability`; add domains `identity`, `rtb`
- [x] 1.2 Screen-candidate filter requires `screenProfiled` (fixture adapter)

## 2. Generator

- [x] 2.1 Author `curatedProfiling.json` overlay (the ~12 distribution-profiled fields, keyed by wide column)
- [x] 2.2 Write `scripts/genCapabilities.mjs`: parse schema field rows, infer domain/event-type/family/strategies, overlay curated, write `capabilities.json`
- [x] 2.3 Add `npm run gen:capabilities`; regenerate `capabilities.json` (~200 fields)

## 3. Capability Map UI

- [x] 3.1 Render the full catalog grouped by domain with `feat` role badges + profiling status
- [x] 3.2 Show `exclude`/`dim`/`key` as non-selectable catalog entries; keep feature candidates traceable

## 4. Verification

- [x] 4.1 Update Capability Map tests (full catalog shown; PII/dim shown but not offered; screen unaffected)
- [x] 4.2 Assert the Distribution Screen still lists only curated candidates (not the full catalog)
- [x] 4.3 Run gen + typecheck + lint + tests + build green; screenshot the full catalog
