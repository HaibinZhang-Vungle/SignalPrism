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

/**
 * An existing aggregate feature from the aggregation table schema (§5 metric catalog).
 * Distribution metrics expand to 5 columns; count metrics are a single column;
 * `count_if` metrics are predicate-based conditional counts (§5.4), not raw counts.
 */
export interface AggregateMetric {
  metricId: string
  kind: 'distribution' | 'count' | 'count_if'
  generatedColumns: string[]
  dataType: string
  source: string
  notes?: string
  /** Label-like / point-in-time sensitive — must not be used naively as a feature input. */
  labelLike?: boolean
  /** `count_if` only (§5.4): the two-field predicate this metric counts. */
  predicate?: string
  /** `count_if` only (§5.4): denominator count for the derived rate feature. */
  denominator?: string
}

/** Base time grain of an aggregation table (aggregation schema §1: "Both tables are hourly"). */
export type AggregateGrain = 'hourly'

/** A reviewed hourly aggregation table (§1) with its dimension columns (§3/§4). */
export interface AggregateTable {
  tableId: string
  dimensionFamily: DimensionFamilyId
  purpose: string
  primaryKey: string
  dimensionColumns: string[]
  /**
   * Base bucket grain (aggregation schema §1). Metrics are hourly buckets; any
   * feature use composes a trailing window that must END BEFORE the scoring
   * event for point-in-time correctness (§2, rule "Metric windows must end
   * strictly before the scoring event").
   */
  grain: AggregateGrain
}

/** The aggregate-feature catalog browsed by the Feature Registry. */
export interface AggregateFeatureCatalog {
  tables: AggregateTable[]
  metrics: AggregateMetric[]
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

/** Derived-feature lifecycle state (TRD §7.10.4). New saves start `proposed`. */
export type DerivedFeatureStatus = 'proposed' | 'approved' | 'production' | 'rejected'

/**
 * Null / fill policy a derived feature must declare (TRD §7.3.3, §7.6 rule
 * "Feature must declare null/fill policy"). `default` is the stored value;
 * `modelInput` is what the model sees.
 */
export interface FillPolicy {
  default: 'null' | 'zero'
  modelInput: 'nan' | 'zero'
}

/**
 * A derived feature: formula + source primitives + dimension family + window
 * (TRD §7.3.3). This is what Formula Studio produces and saves.
 */
export interface DerivedFeature {
  featureId: string
  displayName: string
  dimensionFamily: DimensionFamilyId
  window: WindowSpec
  formula: string
  sourcePrimitives: string[]
  fillPolicy: FillPolicy
  status: DerivedFeatureStatus
}

/**
 * A feature set: the selected list ML actually tests in one simulation
 * (TRD §7.3.4). A candidate set is defined as a delta on a base set —
 * `addedFeatures` minus `removedFeatures` — so runs stay comparable.
 */
export interface FeatureSet {
  featureSetId: string
  /** The set this one is diffed against; `null` for a root/production set. */
  baseFeatureSet: string | null
  addedFeatures: string[]
  removedFeatures: string[]
  owner: string
  purpose: string
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
