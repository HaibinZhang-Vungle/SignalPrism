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
})
