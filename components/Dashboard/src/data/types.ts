// Types for the Feature Workbench data contract.
//
// These mirror the wide-table schema metadata
// (schemas/realtime_attributed_wide_table_schema.md §3.2) and the MLOps TRD
// metadata objects (proj_trd/end_to_end_mlops_wide_table_demo.md §7.3).
// The dashboard consumes these shapes; it does not derive them from prose.

/** semantic_type vocabulary from schema §3.2. */
export type SemanticType =
  | 'id'
  | 'categorical'
  | 'boolean_flag'
  | 'money_cpm'
  | 'rate'
  | 'count'
  | 'duration_ms'
  | 'epoch_ms'
  | 'epoch_s'
  | 'timestamp'
  | 'dimension'
  | 'geo'
  | 'device_attr'
  | 'consent'
  | 'free_text'
  | 'json_blob'
  | 'version'
  | 'enum_code'

/** feature-suitability flag from schema §3.2 (`feat`). Drives selectability. */
export type FeatureSuitability =
  | 'key'
  | 'dim'
  | 'feature'
  | 'feature_after_encode'
  | 'leak_risk'
  | 'exclude'

/** null semantics from schema §3.2 (`null`). */
export type NullSemantics = 'not_observed' | 'zero_is_meaningful' | 'always_present'

/** Source event families (Capability Map tabs, TRD §7.10.1). */
export type SourceEventType = 'delivery' | 'no_serv' | 'hbn' | 'tpat'

/** Capability domains for grouping (TRD §7.10.1). */
export type Domain =
  | 'identity'
  | 'supply'
  | 'placement'
  | 'device'
  | 'geo'
  | 'privacy'
  | 'floor'
  | 'auction'
  | 'shading'
  | 'settlement'
  | 'creative'
  | 'tpat'
  | 'experiment'
  | 'rtb'
  | 'timing'

/** Aggregation strategy library (TRD §7.5). */
export type AggregationStrategy =
  | 'count_event'
  | 'count_if'
  | 'numeric_sum_count'
  | 'numeric_distribution'
  | 'money_cpm'
  | 'cardinality_hll'
  | 'topk_map'
  | 'latest_value'
  | 'ratio_pair'

/** Profiling status; only `available` capabilities are selectable (TRD §7.7 Step 1). */
export type ProfilingStatus = 'available' | 'profiling' | 'failed' | 'not_profiled'

/** Wide-table field families the scanner proposes for feature search (fast-screen step 2). */
export type FieldFamily =
  | 'device'
  | 'ad_unit'
  | 'timeout'
  | 'price_shape'
  | 'floor_lifecycle'
  | 'supply_economics'

export type WindowSpec = '1h' | '1d' | '7d' | '30d'

/** Fixed dimension family names (TRD §7.4). */
export type DimensionFamilyId =
  | 'device_level_v1'
  | 'non_device_context_v1'
  | 'inventory_context_lite_v1'
  | 'global_baseline_v1'

/**
 * A feature capability: one wide-table column the platform understands and
 * knows how to aggregate safely (TRD §7.3.1). Not a feature yet.
 */
export interface FeatureCapability {
  capabilityId: string
  sourceTable: string
  sourceColumn: string
  dataType: string
  semanticType: SemanticType
  sourceEventType: SourceEventType
  domain: Domain
  /** feature-suitability flag; `exclude` is never offered, `leak_risk` is label-only. */
  feat: FeatureSuitability
  nullSemantics: NullSemantics
  profilingStatus: ProfilingStatus
  allowedAggregationStrategies: AggregationStrategy[]
  allowedDimensionFamilies: DimensionFamilyId[]
  defaultWindows: WindowSpec[]
  enumRef?: string
  owner: string
  // Profiling metrics surfaced on the capability card.
  coverage: number // [0,1]
  nullRate: number // [0,1]
  freshnessMinutes: number
  distinctCount?: number
  sampleValues?: string[]
  // Fast-screen distribution profiling (fast-screen step 3, design D1).
  family: FieldFamily
  bucketConcentration: number // top-bucket share [0,1]; high = near-constant / low info
  psi: number // population stability index vs a prior window
  klDivergence: number // KL vs the global distribution
  baseSeparation: number // [0,1] intrinsic discriminative power before pocket weighting
  /** True only for fields with real distribution profiling — the Distribution Screen candidate set. */
  screenProfiled?: boolean
}

/** A high-error residual pocket to search for features (fast-screen step 1). */
export interface ResidualPocket {
  pocketId: string
  label: string
  description: string
  residualRmse: number
  baselineRmse: number
  share: number // fraction of traffic [0,1]
  proposedFamilies: FieldFamily[]
}

/** Distribution evidence for one field against a pocket (fast-screen step 3). */
export interface DistributionStats {
  coverage: number
  missingness: number
  bucketConcentration: number
  klDivergence: number
  psi: number
  subgroupSeparation: number // [0,1] pocket-relative; high = discriminates the pocket
}

/** Screen verdict; only `strong` fields may be promoted (fast-screen step 4). */
export type ScreenVerdict = 'strong' | 'weak' | 'blocked'

/** A field after screening against a pocket. */
export interface ScreenedField {
  capabilityId: string
  family: FieldFamily
  stats: DistributionStats
  verdict: ScreenVerdict
  drifting: boolean
  reasons: string[]
}

/** A fixed, reviewed dimension family (TRD §7.4). Arbitrary dimensions are disallowed. */
export interface DimensionFamily {
  id: DimensionFamilyId
  purpose: string
  keys: string[]
  optionalBuckets?: string[]
  costTier: 'highest' | 'high' | 'medium' | 'low'
  notes?: string[]
}

/** A materialized primitive column produced by an aggregation spec (TRD §7.3.2). */
export interface Primitive {
  primitiveId: string
  capabilityId: string
  strategy: AggregationStrategy
  dimensionFamily: DimensionFamilyId
  window: WindowSpec
}

/** Cost/plan preview for a proposed aggregation (TRD §7.7 Step 3, §7.8). */
export interface AggregationPreview {
  outputTables: string[]
  estimatedRowsPerDay: number
  estimatedBytesPerDay: number
  warnings: string[]
  blocked: boolean
  blockReason?: string
}

/** The §7.7 Step 2 aggregation config object the builder serializes. */
export interface AggregationConfig {
  aggId: string
  source: string
  eventFilter: string
  timeColumn: string
  sample: { type: 'event_id_hash'; rate: string }
  dimensionFamily: DimensionFamilyId
  windows: WindowSpec[]
  measures: { capabilityId: string; strategy: AggregationStrategy }[]
  output: { tablePrefix: string }
}

/** Formula validation result (Formula Studio panel, TRD §7.6 / §7.10.3). */
export interface FormulaValidation {
  ok: boolean
  typeCheck: 'pass' | 'fail'
  divisionSafety: 'pass' | 'fail'
  pointInTime: 'pass' | 'fail'
  primitiveAvailability: 'pass' | 'fail'
  coverageEstimate: number // [0,1]
  errors: string[]
  outputType?: 'numeric'
}

/** A simulation run result (TRD §7.3.5 / §7.9.5). */
export interface SimulationRun {
  runId: string
  featureSetId: string
  datasetId: string
  modelFamily: string
  baseline: string
  treatment: string
  status: 'completed' | 'running' | 'failed'
  metrics: {
    r2Delta: number
    topDecileLift: number
    nrProxyDelta: number
    featureCoverage: number
  }
  liftCurve: { decile: number; baseline: number; treatment: number }[]
  shap: { feature: string; importance: number }[]
  cohorts: { dimension: string; value: string; lift: number }[]
}

/** A lineage edge in the chain column → primitive → feature → set → run. */
export interface LineageNode {
  id: string
  kind: 'column' | 'primitive' | 'derived_feature' | 'feature_set' | 'simulation_run'
  label: string
  /** Which surface owns this node (for deep-linking, task 7.2). */
  surface: 'capability-map' | 'aggregation-builder' | 'formula-studio' | 'simulation-lab'
}

export interface LineageChain {
  nodes: LineageNode[]
  edges: { from: string; to: string }[]
}
