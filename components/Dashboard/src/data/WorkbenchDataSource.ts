// The single interface the UI talks to (design D1). Two implementations:
// a fixture adapter (offline demo) and, later, a live Trino/Iceberg adapter.
// Isolating the contract here documents exactly what the dashboard needs from
// the scanner / aggregation-runner / simulation-runner sub-projects.

import type {
  AggregateFeatureCatalog,
  AggregationConfig,
  AggregationPreview,
  DerivedFeature,
  DimensionFamily,
  FeatureCapability,
  FeatureSet,
  FieldFamily,
  LineageChain,
  Primitive,
  ResidualPocket,
  ScreenedField,
  SimulationRun,
} from './types'

export interface WorkbenchDataSource {
  /** Capabilities registered by the scanner. Only `available` ones should be shown selectable. */
  listCapabilities(): Promise<FeatureCapability[]>

  /** High-error residual pockets that drive feature search (fast-screen step 1). */
  listResidualPockets(): Promise<ResidualPocket[]>

  /** Wide-table field families the scanner proposes (fast-screen step 2). */
  listFieldFamilies(): Promise<FieldFamily[]>

  /** Screen candidate raw fields against a pocket, ranked by evidence (fast-screen steps 3-4). */
  screenFields(pocketId?: string): Promise<ScreenedField[]>

  /** The four reviewed dimension families (TRD §7.4). */
  listDimensionFamilies(): Promise<DimensionFamily[]>

  /** Materialized primitives available for formula composition. */
  listPrimitives(): Promise<Primitive[]>

  /** Existing aggregate features (metric catalog + dimensions) from the aggregation table schema. */
  listAggregateFeatures(): Promise<AggregateFeatureCatalog>

  /** Cost/plan preview for a proposed aggregation config (no materialization). */
  previewAggregation(config: AggregationConfig): Promise<AggregationPreview>

  /** Derived features composed in Formula Studio (TRD §7.3.3). */
  listDerivedFeatures(): Promise<DerivedFeature[]>
  /** Persist a derived feature. Upserts by `featureId`; returns the stored record. */
  saveDerivedFeature(feature: DerivedFeature): Promise<DerivedFeature>

  /** Candidate/base feature sets ML tests (TRD §7.3.4). A set is a delta on a base. */
  listFeatureSets(): Promise<FeatureSet[]>
  readFeatureSet(featureSetId: string): Promise<FeatureSet | undefined>

  /** Completed simulation runs. Display-only for the demo (launch is stubbed). */
  listSimulationRuns(): Promise<SimulationRun[]>
  readSimulationRun(runId: string): Promise<SimulationRun | undefined>

  /** Lineage chain rooted at a capability, primitive, feature, set, or run id. */
  getLineage(nodeId: string): Promise<LineageChain>
}
