// Fixture adapter (design D1): serves the WorkbenchDataSource from checked-in
// JSON snapshots so the demo runs with no live pipeline dependency.

import type { WorkbenchDataSource } from './WorkbenchDataSource'
import type {
  AggregateFeatureCatalog,
  AggregationConfig,
  AggregationPreview,
  DimensionFamily,
  FeatureCapability,
  FieldFamily,
  LineageChain,
  LineageNode,
  Primitive,
  ResidualPocket,
  ScreenedField,
  SimulationRun,
} from './types'
import { DIMENSION_FAMILIES } from '../config/dimensionFamilies'
import { screenAndRank } from '../screen/screen'
import capabilitiesFixture from '../fixtures/capabilities.json'
import primitivesFixture from '../fixtures/primitives.json'
import simulationRunsFixture from '../fixtures/simulationRuns.json'
import residualPocketsFixture from '../fixtures/residualPockets.json'
import aggregateFeaturesFixture from '../fixtures/aggregateFeatures.json'

const capabilities = capabilitiesFixture as FeatureCapability[]
const primitives = primitivesFixture as Primitive[]
const simulationRuns = simulationRunsFixture as SimulationRun[]
const residualPockets = residualPocketsFixture as ResidualPocket[]

/**
 * Feature candidates worth screening: available, distribution-profiled, and can
 * become features. The full catalog (200 fields) is NOT screened — only the
 * profiled subset, so the Distribution Screen stays focused (design D3).
 */
function screenCandidates(): FeatureCapability[] {
  return capabilities.filter(
    (c) =>
      c.profilingStatus === 'available' &&
      c.screenProfiled === true &&
      (c.feat === 'feature' || c.feat === 'feature_after_encode' || c.feat === 'leak_risk'),
  )
}

/** Hard gate from TRD §7.8: block demo materialization above this row estimate. */
const DEMO_ROW_GATE = 500_000_000

/** Nominal daily rows before sampling, by dimension-family cost tier. */
const BASE_ROWS_BY_TIER: Record<DimensionFamily['costTier'], number> = {
  highest: 60_000_000_000,
  high: 6_000_000_000,
  medium: 1_200_000_000,
  low: 40_000_000,
}

function sampleFraction(rate: string): number {
  // e.g. "0.01pct" -> 0.0001, "0.1pct" -> 0.001, "1pct" -> 0.01
  const pct = Number(rate.replace('pct', ''))
  return Number.isFinite(pct) ? pct / 100 : 1
}

export class FixtureWorkbenchDataSource implements WorkbenchDataSource {
  async listCapabilities(): Promise<FeatureCapability[]> {
    return capabilities
  }

  async listResidualPockets(): Promise<ResidualPocket[]> {
    return residualPockets
  }

  async listFieldFamilies(): Promise<FieldFamily[]> {
    return [...new Set(capabilities.map((c) => c.family))]
  }

  async screenFields(pocketId?: string): Promise<ScreenedField[]> {
    const pocket = residualPockets.find((p) => p.pocketId === pocketId)
    return screenAndRank(screenCandidates(), pocket)
  }

  async listDimensionFamilies(): Promise<DimensionFamily[]> {
    return DIMENSION_FAMILIES
  }

  async listPrimitives(): Promise<Primitive[]> {
    return primitives
  }

  async listAggregateFeatures(): Promise<AggregateFeatureCatalog> {
    return aggregateFeaturesFixture as AggregateFeatureCatalog
  }

  async previewAggregation(config: AggregationConfig): Promise<AggregationPreview> {
    const family = DIMENSION_FAMILIES.find((f) => f.id === config.dimensionFamily)
    const warnings: string[] = []
    let blocked = false
    let blockReason: string | undefined

    if (!family) {
      return {
        outputTables: [],
        estimatedRowsPerDay: 0,
        estimatedBytesPerDay: 0,
        warnings: [`Unknown dimension family "${config.dimensionFamily}".`],
        blocked: true,
        blockReason: 'Dimension family is not one of the reviewed families.',
      }
    }

    // Cardinality / strategy guardrails (TRD §7.8).
    for (const m of config.measures) {
      const cap = capabilities.find((c) => c.capabilityId === m.capabilityId)
      if (!cap) {
        warnings.push(`Capability "${m.capabilityId}" not found in catalog.`)
        continue
      }
      const highCard = (cap.distinctCount ?? 0) > 10_000
      const usesRawHighCard =
        highCard && m.strategy !== 'topk_map' && m.strategy !== 'cardinality_hll'
      if (usesRawHighCard) {
        warnings.push(
          `${cap.sourceColumn} raw cardinality too high (${cap.distinctCount}) — ` +
            `use a bucketed field or topk_map instead of ${m.strategy}.`,
        )
      }
      if (!cap.allowedDimensionFamilies.includes(config.dimensionFamily)) {
        warnings.push(
          `${cap.capabilityId} is not allowed in ${config.dimensionFamily}; ` +
            `allowed: ${cap.allowedDimensionFamilies.join(', ') || 'none'}.`,
        )
      }
    }

    const frac = sampleFraction(config.sample.rate)
    const windowFanout = config.windows.length || 1
    const estimatedRowsPerDay = Math.round(BASE_ROWS_BY_TIER[family.costTier] * frac)
    const bytesPerRow = 48 * config.measures.length + 64 // rough: measures + keys
    const estimatedBytesPerDay = estimatedRowsPerDay * bytesPerRow * windowFanout

    if (estimatedRowsPerDay > DEMO_ROW_GATE) {
      blocked = true
      blockReason =
        `Estimated ${estimatedRowsPerDay.toLocaleString()} rows/day exceeds the ` +
        `${DEMO_ROW_GATE.toLocaleString()} demo gate — lower the sample rate or use ` +
        `inventory_context_lite_v1 / global_baseline_v1.`
    }

    const outputTables = [
      `${config.output.tablePrefix}_${family.id}_daily`,
      ...config.windows.map((w) => `${config.output.tablePrefix}_${family.id}_${w}`),
      `${config.output.tablePrefix}_${family.id}_features`,
    ]

    return { outputTables, estimatedRowsPerDay, estimatedBytesPerDay, warnings, blocked, blockReason }
  }

  async listSimulationRuns(): Promise<SimulationRun[]> {
    return simulationRuns
  }

  async readSimulationRun(runId: string): Promise<SimulationRun | undefined> {
    return simulationRuns.find((r) => r.runId === runId)
  }

  async getLineage(nodeId: string): Promise<LineageChain> {
    // Resolve the chain column -> primitive -> derived feature -> feature set -> run.
    // The demo derives a representative chain from the fixtures around the node.
    const prim = primitives.find(
      (p) => p.primitiveId === nodeId || p.capabilityId === nodeId,
    )
    const cap = capabilities.find(
      (c) => c.capabilityId === nodeId || c.capabilityId === prim?.capabilityId,
    )
    const run = simulationRuns[0]
    const feature = run?.shap[0]?.feature ?? 'avg_hbn_settlement_price_7d'

    const nodes: LineageNode[] = []
    const edges: { from: string; to: string }[] = []
    const push = (n: LineageNode, prev?: string) => {
      nodes.push(n)
      if (prev) edges.push({ from: prev, to: n.id })
      return n.id
    }

    let prev: string | undefined
    if (cap) {
      prev = push(
        { id: cap.capabilityId, kind: 'column', label: cap.sourceColumn, surface: 'capability-map' },
      )
    }
    const primId = prim?.primitiveId ?? `${cap?.capabilityId ?? nodeId}_sum_7d`
    prev = push(
      { id: primId, kind: 'primitive', label: primId, surface: 'aggregation-builder' },
      prev,
    )
    prev = push(
      { id: feature, kind: 'derived_feature', label: feature, surface: 'formula-studio' },
      prev,
    )
    if (run) {
      prev = push(
        {
          id: run.featureSetId,
          kind: 'feature_set',
          label: run.featureSetId,
          surface: 'simulation-lab',
        },
        prev,
      )
      push(
        { id: run.runId, kind: 'simulation_run', label: run.runId, surface: 'simulation-lab' },
        prev,
      )
    }

    return { nodes, edges }
  }
}
