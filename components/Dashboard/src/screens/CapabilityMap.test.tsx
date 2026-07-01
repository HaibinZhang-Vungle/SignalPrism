import { describe, it, expect } from 'vitest'
import { render, screen, waitFor, within } from '@testing-library/react'
import { CapabilityMap, isSelectable, isFeatureCandidate } from './CapabilityMap'
import { FixtureWorkbenchDataSource } from '../data/fixtureAdapter'
import type { FeatureCapability } from '../data/types'

const ds = new FixtureWorkbenchDataSource()
const noop = () => {}

const cap = (over: Partial<FeatureCapability>): FeatureCapability => ({
  capabilityId: 'x', sourceTable: 't', sourceColumn: 'c', dataType: 'DOUBLE',
  semanticType: 'money_cpm', sourceEventType: 'hbn', domain: 'auction', feat: 'feature',
  nullSemantics: 'not_observed', profilingStatus: 'available', allowedAggregationStrategies: [],
  allowedDimensionFamilies: [], defaultWindows: [], owner: 'o', coverage: 1, nullRate: 0,
  freshnessMinutes: 1, family: 'price_shape', bucketConcentration: 0.4, psi: 0.1,
  klDivergence: 0.2, baseSeparation: 0.7, ...over,
})

describe('classification helpers', () => {
  it('isSelectable rejects PII and un-profiled', () => {
    expect(isSelectable(cap({ feat: 'exclude' }))).toBe(false)
    expect(isSelectable(cap({ profilingStatus: 'not_profiled' }))).toBe(false)
    expect(isSelectable(cap({}))).toBe(true)
  })
  it('isFeatureCandidate excludes dims and keys', () => {
    expect(isFeatureCandidate(cap({ feat: 'dim' }))).toBe(false)
    expect(isFeatureCandidate(cap({ feat: 'key' }))).toBe(false)
    expect(isFeatureCandidate(cap({ feat: 'feature' }))).toBe(true)
    expect(isFeatureCandidate(cap({ feat: 'leak_risk' }))).toBe(true)
  })
})

describe('CapabilityMap full catalog', () => {
  it('renders the full schema catalog (well over the 9 curated fields)', async () => {
    render(<CapabilityMap ds={ds} onTrace={noop} />)
    await waitFor(() => expect(screen.getByTestId('cap-hbn_settlement_price')).toBeInTheDocument())
    const cards = screen.getAllByTestId(/^cap-/)
    expect(cards.length).toBeGreaterThan(100)
  })

  it('shows a PII field but does not offer it (no trace, marked PII)', async () => {
    render(<CapabilityMap ds={ds} onTrace={noop} />)
    await waitFor(() => expect(screen.getByTestId('cap-jgr_dev_ifa')).toBeInTheDocument())
    const piiCard = screen.getByTestId('cap-jgr_dev_ifa')
    expect(within(piiCard).getByText(/raw PII/)).toBeInTheDocument()
    expect(within(piiCard).queryByRole('button', { name: /Trace lineage/ })).not.toBeInTheDocument()
  })

  it('catalogs a dimension without treating it as a feature candidate', async () => {
    render(<CapabilityMap ds={ds} onTrace={noop} />)
    await waitFor(() => expect(screen.getByTestId('cap-hbn_supply_name')).toBeInTheDocument())
    const dimCard = screen.getByTestId('cap-hbn_supply_name')
    expect(within(dimCard).getByText(/not a feature candidate/)).toBeInTheDocument()
  })
})
