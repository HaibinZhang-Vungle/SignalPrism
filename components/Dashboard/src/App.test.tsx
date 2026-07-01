import { describe, it, expect } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import { App } from './App'

describe('App shell (smoke)', () => {
  it('renders the brand and navigation across the five surfaces', () => {
    render(<App />)
    expect(screen.getByText('SignalPrism')).toBeInTheDocument()
    for (const label of ['Capability Map', 'Aggregation Builder', 'Formula Studio', 'Simulation Lab', 'Lineage']) {
      expect(screen.getByRole('button', { name: label })).toBeInTheDocument()
    }
  })

  it('navigates to Formula Studio', async () => {
    render(<App />)
    fireEvent.click(screen.getByRole('button', { name: 'Formula Studio' }))
    await waitFor(() =>
      expect(screen.getByRole('heading', { name: 'Formula Studio' })).toBeInTheDocument(),
    )
  })
})
