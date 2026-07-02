import { describe, it, expect } from 'vitest'
import { FixtureWorkbenchDataSource } from './fixtureAdapter'
import type { AggregationConfig, FeatureCapability } from './types'
import { DIMENSION_FAMILY_IDS } from '../config/dimensionFamilies'

const ds = new FixtureWorkbenchDataSource()

const baseConfig = (over: Partial<AggregationConfig> = {}): AggregationConfig => ({
  aggId: 'test',
  source: 'ml_shadow.realtime_attributed_event_wide',
  eventFilter: "source_event_type in ('hbn')",
  timeColumn: 'source_event_time',
  sample: { type: 'event_id_hash', rate: '0.01pct' },
  dimensionFamily: 'non_device_context_v1',
  windows: ['7d'],
  measures: [{ capabilityId: 'hbn_bid_price', strategy: 'numeric_distribution' }],
  output: { tablePrefix: 'ml_shadow_feature.test' },
  ...over,
})

describe('previewAggregation', () => {
  it('emits output tables and a cost estimate', async () => {
    const p = await ds.previewAggregation(baseConfig())
    expect(p.outputTables.length).toBeGreaterThan(0)
    expect(p.estimatedRowsPerDay).toBeGreaterThan(0)
  })

  it('warns when a raw high-cardinality field uses a non-bucketing strategy (cost warning scenario)', async () => {
    const p = await ds.previewAggregation(
      baseConfig({
        dimensionFamily: 'device_level_v1',
        measures: [{ capabilityId: 'dev_model', strategy: 'numeric_distribution' }],
      }),
    )
    expect(p.warnings.join(' ')).toMatch(/cardinality too high/i)
  })

  it('blocks materialization above the demo row gate', async () => {
    const p = await ds.previewAggregation(
      baseConfig({ dimensionFamily: 'device_level_v1', sample: { type: 'event_id_hash', rate: '1pct' } }),
    )
    expect(p.blocked).toBe(true)
    expect(p.blockReason).toMatch(/demo gate/i)
  })
})

describe('screenFields scope', () => {
  it('screens only the distribution-profiled candidates, not the full catalog', async () => {
    const caps = await ds.listCapabilities()
    const fields = await ds.screenFields('pocket_in_rewarded_ios')
    // Far fewer than the full catalog; equals the screenProfiled candidate count.
    expect(caps.length).toBeGreaterThan(100)
    expect(fields.length).toBe(caps.filter((c) => c.screenProfiled).length)
    expect(fields.every((f) => caps.find((c) => c.capabilityId === f.capabilityId)?.screenProfiled)).toBe(true)
  })
})

describe('getLineage', () => {
  it('returns a chain ending in a simulation run', async () => {
    const chain = await ds.getLineage('hbn_settlement_price')
    const kinds = chain.nodes.map((n) => n.kind)
    expect(kinds).toContain('column')
    expect(kinds).toContain('primitive')
    expect(kinds).toContain('derived_feature')
    expect(kinds).toContain('simulation_run')
    expect(chain.edges.length).toBe(chain.nodes.length - 1)
  })
})

// Task 8.2: guard against fixture drift from the WorkbenchDataSource types.
describe('fixture shape (drift guard)', () => {
  const SEMANTIC = new Set([
    'id', 'categorical', 'boolean_flag', 'money_cpm', 'rate', 'count', 'duration_ms',
    'epoch_ms', 'epoch_s', 'timestamp', 'dimension', 'geo', 'device_attr', 'consent',
    'free_text', 'json_blob', 'version', 'enum_code',
  ])
  const FEAT = new Set(['key', 'dim', 'feature', 'feature_after_encode', 'leak_risk', 'exclude'])

  it('every capability has valid required fields', async () => {
    const caps = await ds.listCapabilities()
    for (const c of caps as FeatureCapability[]) {
      expect(typeof c.capabilityId, c.capabilityId).toBe('string')
      expect(SEMANTIC.has(c.semanticType), `${c.capabilityId}.semanticType`).toBe(true)
      expect(FEAT.has(c.feat), `${c.capabilityId}.feat`).toBe(true)
      expect(c.coverage, `${c.capabilityId}.coverage`).toBeGreaterThanOrEqual(0)
      expect(c.coverage).toBeLessThanOrEqual(1)
      for (const f of c.allowedDimensionFamilies) {
        expect(DIMENSION_FAMILY_IDS as string[], `${c.capabilityId} family ${f}`).toContain(f)
      }
    }
  })

  it('every simulation run resolves to a real feature set, and bases resolve too', async () => {
    const sets = await ds.listFeatureSets()
    const setIds = new Set(sets.map((s) => s.featureSetId))
    for (const s of sets) {
      if (s.baseFeatureSet !== null) {
        expect(setIds.has(s.baseFeatureSet), `${s.featureSetId} base ${s.baseFeatureSet}`).toBe(true)
      }
    }
    const runs = await ds.listSimulationRuns()
    for (const r of runs) {
      expect(setIds.has(r.featureSetId), `${r.runId} -> ${r.featureSetId}`).toBe(true)
      expect(await ds.readFeatureSet(r.featureSetId)).toBeDefined()
    }
  })

  it('every primitive points to a real capability', async () => {
    const caps = await ds.listCapabilities()
    const capIds = new Set(caps.map((c) => c.capabilityId))
    const prims = await ds.listPrimitives()
    for (const p of prims) {
      expect(capIds.has(p.capabilityId), `${p.primitiveId} -> ${p.capabilityId}`).toBe(true)
      expect(DIMENSION_FAMILY_IDS as string[]).toContain(p.dimensionFamily)
    }
  })
})
