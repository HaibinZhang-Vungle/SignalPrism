// The single interface the UI talks to (design D1). Two implementations:
// a fixture adapter (offline demo) and, later, a live Trino/Iceberg adapter.
// Isolating the contract here documents exactly what the dashboard needs from
// the scanner / aggregation-runner / simulation-runner sub-projects.

import type {
  AggregationConfig,
  AggregationPreview,
  DimensionFamily,
  FeatureCapability,
  LineageChain,
  Primitive,
  SimulationRun,
} from './types'

export interface WorkbenchDataSource {
  /** Capabilities registered by the scanner. Only `available` ones should be shown selectable. */
  listCapabilities(): Promise<FeatureCapability[]>

  /** The four reviewed dimension families (TRD §7.4). */
  listDimensionFamilies(): Promise<DimensionFamily[]>

  /** Materialized primitives available for formula composition. */
  listPrimitives(): Promise<Primitive[]>

  /** Cost/plan preview for a proposed aggregation config (no materialization). */
  previewAggregation(config: AggregationConfig): Promise<AggregationPreview>

  /** Completed simulation runs. Display-only for the demo (launch is stubbed). */
  listSimulationRuns(): Promise<SimulationRun[]>
  readSimulationRun(runId: string): Promise<SimulationRun | undefined>

  /** Lineage chain rooted at a capability, primitive, feature, set, or run id. */
  getLineage(nodeId: string): Promise<LineageChain>
}
