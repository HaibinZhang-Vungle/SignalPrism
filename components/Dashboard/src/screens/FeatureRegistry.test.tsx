import { describe, it, expect } from 'vitest'
import { render, screen, waitFor, within } from '@testing-library/react'
import { FeatureRegistry } from './FeatureRegistry'
import { FixtureWorkbenchDataSource } from '../data/fixtureAdapter'

const ds = new FixtureWorkbenchDataSource()

describe('aggregate-feature catalog', () => {
  it('distribution metrics expand to five columns; counts are single-column', async () => {
    const cat = await ds.listAggregateFeatures()
    const dist = cat.metrics.find((m) => m.kind === 'distribution')!
    expect(dist.generatedColumns).toHaveLength(5)
    expect(dist.generatedColumns).toEqual(
      ['_sum', '_count', '_min', '_max', '_squaresum'].map((s) => dist.metricId + s),
    )
    const count = cat.metrics.find((m) => m.kind === 'count')!
    expect(count.generatedColumns).toEqual([count.metricId])
  })

  it('separates sp_at_mediation_floor as a count_if metric, not a raw count (§5.4)', async () => {
    const cat = await ds.listAggregateFeatures()
    // No longer a base count metric.
    expect(cat.metrics.find((m) => m.metricId === 'sp_at_mediation_floor_count')).toBeUndefined()
    expect(cat.metrics.some((m) => m.kind === 'count' && m.metricId.includes('sp_at_mediation_floor'))).toBe(false)
    // Present as a count_if with a predicate, a denominator, and the modulo-named count column.
    const cif = cat.metrics.find((m) => m.metricId === 'sp_at_mediation_floor')
    expect(cif?.kind).toBe('count_if')
    expect(cif?.predicate).toBeTruthy()
    expect(cif?.denominator).toBe('settlement_price_count')
    expect(cif?.generatedColumns).toContain('sp_at_mediation_floor_count')
  })

  it('flags label-like metrics and carries both tables with dimensions', async () => {
    const cat = await ds.listAggregateFeatures()
    expect(cat.metrics.find((m) => m.metricId === 'settlement_price')?.labelLike).toBe(true)
    expect(cat.tables.map((t) => t.dimensionFamily).sort()).toEqual([
      'device_level_v1',
      'non_device_context_v1',
    ])
    expect(cat.tables.every((t) => t.dimensionColumns.length > 0)).toBe(true)
  })
})

describe('FeatureRegistry render', () => {
  it('renders distribution metrics with their 5 generated columns and a label flag', async () => {
    render(<FeatureRegistry ds={ds} />)
    await waitFor(() => expect(screen.getByTestId('metric-settlement_price')).toBeInTheDocument())
    const card = screen.getByTestId('metric-settlement_price')
    expect(within(card).getByText('settlement_price_squaresum')).toBeInTheDocument()
    expect(within(card).getByText(/label \/ point-in-time/)).toBeInTheDocument()
    // A count metric renders too.
    expect(screen.getByTestId('metric-delivery_count')).toBeInTheDocument()
  })

  it('renders the conditional (count_if) group with the derived rate', async () => {
    render(<FeatureRegistry ds={ds} />)
    const group = await screen.findByTestId('count-if-group')
    expect(within(group).getByTestId('metric-sp_at_mediation_floor')).toBeInTheDocument()
    // The derived rate is surfaced as a code chip (also mentioned in notes → match the <code>).
    const rate = within(group).getByText(
      (_t, el) => el?.tagName === 'CODE' && el.textContent === 'safe_div(sp_at_mediation_floor_count, settlement_price_count)',
    )
    expect(rate).toBeInTheDocument()
  })

  it('flags the hourly grain and the point-in-time trailing-window rule', async () => {
    render(<FeatureRegistry ds={ds} />)
    const flag = await screen.findByTestId('window-flag')
    expect(flag).toHaveTextContent(/hourly/i)
    expect(flag).toHaveTextContent(/end strictly before the scoring event/i)
  })
})
