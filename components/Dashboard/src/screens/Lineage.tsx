import { useState } from 'react'
import type { WorkbenchDataSource } from '../data/WorkbenchDataSource'
import type { SurfaceId } from '../App'
import { useAsync } from '../hooks/useAsync'
import type { LineageNode } from '../data/types'

const SURFACE_LABEL: Record<LineageNode['surface'], string> = {
  'capability-map': 'Capability Map',
  'aggregation-builder': 'Aggregation Builder',
  'formula-studio': 'Formula Studio',
  'simulation-lab': 'Simulation Lab',
}

export function Lineage({
  ds,
  seed,
}: {
  ds: WorkbenchDataSource
  seed?: string
}) {
  const [rootId] = useState(seed ?? 'hbn_settlement_price')
  const chain = useAsync(() => ds.getLineage(rootId), [ds, rootId])

  return (
    <div className="screen">
      <h1>Lineage</h1>
      <p className="sub">
        Trace from wide-table column → primitive → derived feature → feature set → simulation run.
        Rooted at <code>{rootId}</code>.
      </p>

      <div className="panel">
        <div className="lineage">
          {(chain?.nodes ?? []).map((n, i) => (
            <span key={n.id} style={{ display: 'contents' }}>
              {i > 0 && <span className="arrow">→</span>}
              <div className="node" title={`Owned by ${SURFACE_LABEL[n.surface]}`}>
                <div className="kind">{n.kind.replace(/_/g, ' ')}</div>
                <div className="lbl">{n.label}</div>
                <div className="col" style={{ marginTop: 4 }}>{SURFACE_LABEL[n.surface as Exclude<SurfaceId, 'lineage'>]}</div>
              </div>
            </span>
          ))}
        </div>
      </div>
    </div>
  )
}
