import { describe, it, expect } from 'vitest'
import { render, screen, fireEvent, waitFor } from '@testing-library/react'
import { App } from './App'

describe('App shell (smoke)', () => {
  it('renders the fast-screen nav with the screen leading', () => {
    render(<App />)
    expect(screen.getByText('SignalPrism')).toBeInTheDocument()
    for (const label of [
      'Residual Diagnostics',
      'Distribution Screen',
      'Capability Map',
      'Aggregation Builder',
      'Formula Studio',
      'Simulation Lab',
      'Lineage',
    ]) {
      expect(screen.getByRole('button', { name: new RegExp(label) })).toBeInTheDocument()
    }
  })

  it('opens on Residual Diagnostics (the fast-screen entry point)', async () => {
    render(<App />)
    await waitFor(() =>
      expect(screen.getByRole('heading', { name: 'Residual Diagnostics' })).toBeInTheDocument(),
    )
  })

  it('navigates to Formula Studio', async () => {
    render(<App />)
    fireEvent.click(screen.getByRole('button', { name: /Formula Studio/ }))
    await waitFor(() =>
      expect(screen.getByRole('heading', { name: 'Formula Studio' })).toBeInTheDocument(),
    )
  })
})
