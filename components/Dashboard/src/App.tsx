import { useMemo, useState } from 'react'
import { FixtureWorkbenchDataSource } from './data/fixtureAdapter'
import type { WorkbenchDataSource } from './data/WorkbenchDataSource'
import { CapabilityMap } from './screens/CapabilityMap'
import { AggregationBuilder } from './screens/AggregationBuilder'
import { FormulaStudio } from './screens/FormulaStudio'
import { SimulationLab } from './screens/SimulationLab'
import { Lineage } from './screens/Lineage'

export type SurfaceId =
  | 'capability-map'
  | 'aggregation-builder'
  | 'formula-studio'
  | 'simulation-lab'
  | 'lineage'

const SURFACES: { id: SurfaceId; label: string }[] = [
  { id: 'capability-map', label: 'Capability Map' },
  { id: 'aggregation-builder', label: 'Aggregation Builder' },
  { id: 'formula-studio', label: 'Formula Studio' },
  { id: 'simulation-lab', label: 'Simulation Lab' },
  { id: 'lineage', label: 'Lineage' },
]

export interface AppProps {
  /** Injectable for tests; defaults to the fixture adapter. */
  dataSource?: WorkbenchDataSource
}

export function App({ dataSource }: AppProps) {
  const ds = useMemo(() => dataSource ?? new FixtureWorkbenchDataSource(), [dataSource])
  const [active, setActive] = useState<SurfaceId>('capability-map')
  const [lineageSeed, setLineageSeed] = useState<string | undefined>(undefined)

  const goLineage = (nodeId: string) => {
    setLineageSeed(nodeId)
    setActive('lineage')
  }

  return (
    <div className="app">
      <aside className="sidebar">
        <div className="brand">
          SignalPrism
          <small>Feature Workbench</small>
        </div>
        <nav className="nav">
          {SURFACES.map((s) => (
            <button
              key={s.id}
              className={active === s.id ? 'active' : ''}
              onClick={() => setActive(s.id)}
            >
              {s.label}
            </button>
          ))}
        </nav>
      </aside>
      <main className="main">
        {active === 'capability-map' && <CapabilityMap ds={ds} onTrace={goLineage} />}
        {active === 'aggregation-builder' && <AggregationBuilder ds={ds} />}
        {active === 'formula-studio' && <FormulaStudio ds={ds} />}
        {active === 'simulation-lab' && <SimulationLab ds={ds} onTrace={goLineage} />}
        {active === 'lineage' && <Lineage ds={ds} seed={lineageSeed} />}
      </main>
    </div>
  )
}
