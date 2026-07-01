import { describe, it, expect } from 'vitest'
import { parseFormula, validateFormula, usesRawDivision, referencedIdentifiers } from './dsl'
import type { ValidationContext } from './dsl'

const ctx: ValidationContext = {
  availablePrimitives: new Set([
    'hbn_settlement_price_sum_7d',
    'hbn_settlement_price_count_7d',
    'hbn_settlement_price_sum_sq_7d',
    'delivery_count_7d',
    'tpat_event_start_count_sum_7d',
  ]),
  forbiddenRawColumns: new Set(['jgr_settlement_price', 'jgr_winner_predicted_nr']),
  coverageByPrimitive: {
    hbn_settlement_price_sum_7d: 0.71,
    hbn_settlement_price_count_7d: 0.71,
    delivery_count_7d: 1.0,
    tpat_event_start_count_sum_7d: 0.44,
  },
}

describe('parser', () => {
  it('parses nested function calls and arithmetic', () => {
    const ast = parseFormula('safe_div(a_sum, b_count) - 1')
    expect(ast.type).toBe('binary')
  })

  it('collects referenced identifiers, excluding function names', () => {
    const ast = parseFormula('safe_div(delivery_count_7d, tpat_event_start_count_sum_7d)')
    expect(referencedIdentifiers(ast).sort()).toEqual(
      ['delivery_count_7d', 'tpat_event_start_count_sum_7d'].sort(),
    )
  })

  it('detects raw division', () => {
    expect(usesRawDivision(parseFormula('a_sum / b_count'))).toBe(true)
    expect(usesRawDivision(parseFormula('safe_div(a_sum, b_count)'))).toBe(false)
  })
})

describe('validateFormula', () => {
  it('passes a valid trailing-aggregate formula', () => {
    const r = validateFormula(
      'safe_div(hbn_settlement_price_sum_7d, hbn_settlement_price_count_7d)',
      ctx,
    )
    expect(r.ok).toBe(true)
    expect(r.coverageEstimate).toBeCloseTo(0.71)
  })

  it('rejects raw division (must use safe_div)', () => {
    const r = validateFormula('hbn_settlement_price_sum_7d / hbn_settlement_price_count_7d', ctx)
    expect(r.ok).toBe(false)
    expect(r.divisionSafety).toBe('fail')
  })

  it('rejects a direct label / leak_risk reference (point-in-time)', () => {
    const r = validateFormula('safe_div(jgr_settlement_price, delivery_count_7d)', ctx)
    expect(r.ok).toBe(false)
    expect(r.pointInTime).toBe('fail')
    expect(r.errors.join(' ')).toMatch(/leak_risk/)
  })

  it('rejects unknown functions', () => {
    const r = validateFormula('frobnicate(delivery_count_7d)', ctx)
    expect(r.ok).toBe(false)
    expect(r.typeCheck).toBe('fail')
  })

  it('rejects unavailable primitives', () => {
    const r = validateFormula('safe_div(nonexistent_sum_7d, delivery_count_7d)', ctx)
    expect(r.ok).toBe(false)
    expect(r.primitiveAvailability).toBe('fail')
  })

  it('reports a parse error gracefully without throwing', () => {
    const r = validateFormula('safe_div(a_sum,', ctx)
    expect(r.ok).toBe(false)
    expect(r.errors.length).toBeGreaterThan(0)
  })
})
