import { useCallback, useMemo, useState } from 'react'
import { FixtureWorkbenchDataSource } from './data/fixtureAdapter'
import type { WorkbenchDataSource } from './data/WorkbenchDataSource'
import { ResidualDiagnostics } from './screens/ResidualDiagnostics'
import { DistributionScreen } from './screens/DistributionScreen'
import { CapabilityMap } from './screens/CapabilityMap'
import { AggregationBuilder } from './screens/AggregationBuilder'
import { FormulaStudio } from './screens/FormulaStudio'
import { SimulationLab } from './screens/SimulationLab'
import { Lineage } from './screens/Lineage'

export type SurfaceId =
  | 'residual-diagnostics'
  | 'distribution-screen'
  | 'capability-map'
  | 'aggregation-builder'
  | 'formula-studio'
  | 'simulation-lab'
  | 'lineage'

// Nav ordered around the fast screen (design D4): screen leads, model pipeline is downstream.
const SURFACES: { id: SurfaceId; label: string; step?: string }[] = [
  { id: 'residual-diagnostics', label: 'Residual Diagnostics', step: '1' },
  { id: 'distribution-screen', label: 'Distribution Screen', step: '2–4' },
  { id: 'capability-map', label: 'Capability Map' },
  { id: 'aggregation-builder', label: 'Aggregation Builder', step: '5' },
  { id: 'formula-studio', label: 'Formula Studio' },
  { id: 'simulation-lab', label: 'Simulation Lab', step: '6–7' },
  { id: 'lineage', label: 'Lineage' },
]

export interface AppProps {
  /** Injectable for tests; defaults to the fixture adapter. */
  dataSource?: WorkbenchDataSource
}

export function App({ dataSource }: AppProps) {
  const ds = useMemo(() => dataSource ?? new FixtureWorkbenchDataSource(), [dataSource])
  const [active, setActive] = useState<SurfaceId>('residual-diagnostics')
  const [lineageSeed, setLineageSeed] = useState<string | undefined>(undefined)
  const [selectedPocket, setSelectedPocket] = useState<string | undefined>(undefined)
  // Promotion gate: only fields promoted from the Distribution Screen reach aggregation.
  const [promoted, setPromoted] = useState<Set<string>>(new Set())

  const goLineage = (nodeId: string) => {
    setLineageSeed(nodeId)
    setActive('lineage')
  }

  const selectPocket = (pocketId: string) => {
    setSelectedPocket(pocketId)
    setActive('distribution-screen')
  }

  const togglePromote = useCallback((capabilityId: string) => {
    setPromoted((prev) => {
      const next = new Set(prev)
      if (next.has(capabilityId)) next.delete(capabilityId)
      else next.add(capabilityId)
      return next
    })
  }, [])

  return (
    <div className="app">
      <aside className="sidebar">
        <div className="brand">
          SignalPrism
          <small>Feature Workbench · fast screen</small>
        </div>
        <nav className="nav">
          {SURFACES.map((s) => (
            <button
              key={s.id}
              className={active === s.id ? 'active' : ''}
              onClick={() => setActive(s.id)}
            >
              {s.label}
              {s.step && <span className="step">{s.step}</span>}
            </button>
          ))}
        </nav>
        <div className="promoted-count">{promoted.size} field(s) promoted</div>
      </aside>
      <main className="main">
        {active === 'residual-diagnostics' && (
          <ResidualDiagnostics ds={ds} selectedPocket={selectedPocket} onSelectPocket={selectPocket} />
        )}
        {active === 'distribution-screen' && (
          <DistributionScreen
            ds={ds}
            selectedPocket={selectedPocket}
            onSelectPocket={setSelectedPocket}
            promoted={promoted}
            onTogglePromote={togglePromote}
          />
        )}
        {active === 'capability-map' && <CapabilityMap ds={ds} onTrace={goLineage} />}
        {active === 'aggregation-builder' && <AggregationBuilder ds={ds} promoted={promoted} />}
        {active === 'formula-studio' && <FormulaStudio ds={ds} />}
        {active === 'simulation-lab' && <SimulationLab ds={ds} onTrace={goLineage} />}
        {active === 'lineage' && <Lineage ds={ds} seed={lineageSeed} />}
      </main>
    </div>
  )
}
