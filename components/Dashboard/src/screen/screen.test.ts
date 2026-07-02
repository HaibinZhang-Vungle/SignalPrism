import { describe, it, expect } from 'vitest'
import { screenField, screenAndRank, canPromote, pocketSeparation } from './screen'
import type { FeatureCapability, ResidualPocket } from '../data/types'

const cap = (over: Partial<FeatureCapability>): FeatureCapability => ({
  capabilityId: 'x', sourceTable: 't', sourceColumn: 'c', dataType: 'DOUBLE',
  semanticType: 'money_cpm', sourceEventType: 'hbn', domain: 'auction', feat: 'feature',
  nullSemantics: 'not_observed', profilingStatus: 'available', allowedAggregationStrategies: [],
  allowedDimensionFamilies: [], defaultWindows: [], owner: 'o', coverage: 0.7, nullRate: 0.3,
  freshnessMinutes: 1, family: 'price_shape', bucketConcentration: 0.4, psi: 0.1,
  klDivergence: 0.2, baseSeparation: 0.7, ...over,
})

const pocket: ResidualPocket = {
  pocketId: 'p', label: 'p', description: '', residualRmse: 0.4, baselineRmse: 0.3,
  share: 0.05, proposedFamilies: ['price_shape', 'floor_lifecycle'],
}

describe('pocketSeparation', () => {
  it('boosts fields whose family the pocket proposes and damps others', () => {
    expect(pocketSeparation(cap({ family: 'price_shape', baseSeparation: 0.6 }), pocket)).toBeCloseTo(0.75)
    expect(pocketSeparation(cap({ family: 'device', baseSeparation: 0.6 }), pocket)).toBeCloseTo(0.45)
  })
  it('clamps to [0,1]', () => {
    expect(pocketSeparation(cap({ family: 'price_shape', baseSeparation: 0.95 }), pocket)).toBe(1)
  })
})

describe('screenField verdicts', () => {
  it('blocks low-coverage fields', () => {
    const f = screenField(cap({ coverage: 0.2 }), pocket)
    expect(f.verdict).toBe('blocked')
  })
  it('marks near-constant (high concentration) fields weak', () => {
    const f = screenField(cap({ bucketConcentration: 0.95 }), pocket)
    expect(f.verdict).toBe('weak')
  })
  it('marks low-separation fields weak', () => {
    const f = screenField(cap({ family: 'device', baseSeparation: 0.4 }), pocket)
    expect(f.verdict).toBe('weak') // 0.4 * 0.75 = 0.30 < 0.35
    expect(canPromote(f)).toBe(false)
  })
  it('marks strong, promotable fields', () => {
    const f = screenField(cap({ family: 'price_shape', baseSeparation: 0.72, coverage: 0.71 }), pocket)
    expect(f.verdict).toBe('strong')
    expect(canPromote(f)).toBe(true)
  })
  it('flags drift when PSI exceeds threshold without blocking a strong field', () => {
    const f = screenField(cap({ family: 'floor_lifecycle', baseSeparation: 0.66, psi: 0.24 }), pocket)
    expect(f.drifting).toBe(true)
    expect(f.verdict).toBe('strong')
    expect(f.reasons.join(' ')).toMatch(/drift/i)
  })
})

describe('screenAndRank', () => {
  it('orders strong before weak before blocked', () => {
    const ranked = screenAndRank(
      [
        cap({ capabilityId: 'blocked', coverage: 0.1 }),
        cap({ capabilityId: 'strong', family: 'price_shape', baseSeparation: 0.8 }),
        cap({ capabilityId: 'weak', family: 'device', baseSeparation: 0.3 }),
      ],
      pocket,
    )
    expect(ranked.map((f) => f.capabilityId)).toEqual(['strong', 'weak', 'blocked'])
  })
})
