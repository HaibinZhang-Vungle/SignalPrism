import { describe, it, expect } from 'vitest'
import {
  DIMENSION_FAMILIES,
  ALLOWED_DIMENSION_KEYS,
  familiesSupportingKey,
} from './dimensionFamilies'

describe('fixed dimension families (guardrail)', () => {
  it('ships exactly the four reviewed families', () => {
    expect(DIMENSION_FAMILIES.map((f) => f.id).sort()).toEqual(
      ['device_level_v1', 'global_baseline_v1', 'inventory_context_lite_v1', 'non_device_context_v1'],
    )
  })

  it('rejects an arbitrary dimension not in any family', () => {
    expect(ALLOWED_DIMENSION_KEYS.has('some_arbitrary_column')).toBe(false)
    expect(familiesSupportingKey('some_arbitrary_column')).toEqual([])
  })

  it('points a supported key to the family/families that contain it', () => {
    expect(familiesSupportingKey('dev_model_bucket')).toContain('non_device_context_v1')
    expect(familiesSupportingKey('placement_type').length).toBeGreaterThan(1)
  })
})
