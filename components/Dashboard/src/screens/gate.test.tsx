import { describe, it, expect } from 'vitest'
import { render, screen, waitFor } from '@testing-library/react'
import { AggregationBuilder } from './AggregationBuilder'
import { DistributionScreen } from './DistributionScreen'
import { FixtureWorkbenchDataSource } from '../data/fixtureAdapter'

const ds = new FixtureWorkbenchDataSource()
const noop = () => {}

describe('Aggregation Builder promotion gate', () => {
  it('shows an empty-state directing to the screen when nothing is promoted', async () => {
    render(<AggregationBuilder ds={ds} promoted={new Set()} />)
    await waitFor(() => expect(screen.getByTestId('no-promoted')).toBeInTheDocument())
    // No capability checkboxes offered.
    expect(screen.queryByText('hbn_bid_price')).not.toBeInTheDocument()
  })

  it('offers a promoted capability once it has passed the screen', async () => {
    render(<AggregationBuilder ds={ds} promoted={new Set(['hbn_bid_price'])} />)
    await waitFor(() => expect(screen.getByText('hbn_bid_price')).toBeInTheDocument())
    expect(screen.queryByTestId('no-promoted')).not.toBeInTheDocument()
  })
})

describe('Distribution Screen', () => {
  it('ranks fields and disables Promote on weak/blocked verdicts', async () => {
    render(
      <DistributionScreen
        ds={ds}
        selectedPocket="pocket_in_rewarded_ios"
        onSelectPocket={noop}
        promoted={new Set()}
        onTogglePromote={noop}
      />,
    )
    // A strong field for this pocket (price_shape) is promotable.
    await waitFor(() => expect(screen.getByTestId('screen-hbn_settlement_price')).toBeInTheDocument())
    const strongRow = screen.getByTestId('screen-hbn_settlement_price')
    const strongBtn = strongRow.querySelector('button')!
    expect(strongBtn).not.toBeDisabled()

    // A near-constant leak field (no_serv_reason, concentration 0.93) is weak → not promotable.
    const weakRow = screen.getByTestId('screen-no_serv_reason')
    const weakBtn = weakRow.querySelector('button')!
    expect(weakBtn).toBeDisabled()
  })
})
