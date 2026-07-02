import { describe, it, expect } from 'vitest'
import { render, screen, waitFor, within, fireEvent } from '@testing-library/react'
import { FormulaStudio } from './FormulaStudio'
import { FixtureWorkbenchDataSource } from '../data/fixtureAdapter'

describe('FormulaStudio save-as-derived-feature', () => {
  it('saves a valid formula as a derived feature and lists it', async () => {
    const ds = new FixtureWorkbenchDataSource()
    render(<FormulaStudio ds={ds} />)

    // Seeded derived features load into the list.
    await waitFor(() =>
      expect(screen.getByTestId('derived-avg_hbn_settlement_price_7d')).toBeInTheDocument(),
    )

    // The default template formula is valid; give it an id and save.
    fireEvent.change(screen.getByPlaceholderText(/e\.g\. bid_premium_rate/), {
      target: { value: 'avg_settlement_test_7d' },
    })
    const saveBtn = screen.getByTestId('save-derived')
    expect(saveBtn).toBeEnabled()
    fireEvent.click(saveBtn)

    await waitFor(() =>
      expect(screen.getByRole('status')).toHaveTextContent(/Saved "avg_settlement_test_7d"/),
    )
    const row = await screen.findByTestId('derived-avg_settlement_test_7d')
    expect(within(row).getByText(/proposed/)).toBeInTheDocument()

    // Persisted to the data source with resolved source primitives and defaults.
    const saved = (await ds.listDerivedFeatures()).find((f) => f.featureId === 'avg_settlement_test_7d')
    expect(saved).toBeDefined()
    expect(saved!.sourcePrimitives).toEqual([
      'hbn_settlement_price_sum_7d',
      'hbn_settlement_price_count_7d',
    ])
    expect(saved!.fillPolicy).toEqual({ default: 'null', modelInput: 'nan' })
    expect(saved!.status).toBe('proposed')
  })

  it('disables save until a feature id is provided', async () => {
    const ds = new FixtureWorkbenchDataSource()
    render(<FormulaStudio ds={ds} />)
    await waitFor(() => expect(screen.getByTestId('save-derived')).toBeInTheDocument())
    // Valid formula but empty id → disabled.
    expect(screen.getByTestId('save-derived')).toBeDisabled()
  })
})
