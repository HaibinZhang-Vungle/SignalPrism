import { describe, it, expect } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import { CapabilityMap, isSelectable } from './CapabilityMap'
import { FixtureWorkbenchDataSource } from '../data/fixtureAdapter'
import type { FeatureCapability } from '../data/types'

const ds = new FixtureWorkbenchDataSource()
const noop = () => {}

const cap = (over: Partial<FeatureCapability>): FeatureCapability => ({
  capabilityId: 'x', sourceTable: 't', sourceColumn: 'c', dataType: 'DOUBLE',
  semanticType: 'money_cpm', sourceEventType: 'hbn', domain: 'auction', feat: 'feature',
  nullSemantics: 'not_observed', profilingStatus: 'available', allowedAggregationStrategies: [],
  allowedDimensionFamilies: [], defaultWindows: [], owner: 'o', coverage: 1, nullRate: 0,
  freshnessMinutes: 1, ...over,
})

describe('isSelectable', () => {
  it('rejects PII / exclude columns', () => {
    expect(isSelectable(cap({ feat: 'exclude' }))).toBe(false)
  })
  it('rejects columns that have not passed profiling', () => {
    expect(isSelectable(cap({ profilingStatus: 'not_profiled' }))).toBe(false)
    expect(isSelectable(cap({ profilingStatus: 'profiling' }))).toBe(false)
  })
  it('accepts available, non-excluded columns', () => {
    expect(isSelectable(cap({ feat: 'feature', profilingStatus: 'available' }))).toBe(true)
  })
})

describe('CapabilityMap render', () => {
  it('shows available capabilities but not PII or un-profiled ones', async () => {
    render(<CapabilityMap ds={ds} onTrace={noop} />)
    // available:
    await waitFor(() => expect(screen.getByTestId('cap-hbn_settlement_price')).toBeInTheDocument())
    // PII (jgr_dev_ifa) excluded, un-profiled (adv_erpm), profiling (predicted_user_value):
    expect(screen.queryByTestId('cap-dev_ifa')).not.toBeInTheDocument()
    expect(screen.queryByTestId('cap-adv_erpm')).not.toBeInTheDocument()
    expect(screen.queryByTestId('cap-predicted_user_value')).not.toBeInTheDocument()
  })
})
