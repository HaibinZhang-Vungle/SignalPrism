import type { WorkbenchDataSource } from '../data/WorkbenchDataSource'
import { useAsync } from '../hooks/useAsync'

const FAMILY_LABEL: Record<string, string> = {
  device: 'device',
  ad_unit: 'ad unit',
  timeout: 'timeout',
  price_shape: 'price shape',
  floor_lifecycle: 'floor lifecycle',
  supply_economics: 'supply economics',
}

export function ResidualDiagnostics({
  ds,
  selectedPocket,
  onSelectPocket,
}: {
  ds: WorkbenchDataSource
  selectedPocket?: string
  onSelectPocket: (pocketId: string) => void
}) {
  const pockets = useAsync(() => ds.listResidualPockets(), [ds])

  return (
    <div className="screen">
      <h1>Residual Diagnostics</h1>
      <p className="sub">
        The current model has stalled. These are the high-error pockets worth searching for new
        features — screen fields against a pocket before spending a training cycle.
      </p>

      <div className="grid">
        {(pockets ?? []).map((p) => {
          const excess = ((p.residualRmse - p.baselineRmse) / p.baselineRmse) * 100
          const selected = p.pocketId === selectedPocket
          return (
            <div
              className={`card pocket ${selected ? 'selected' : ''}`}
              key={p.pocketId}
              data-testid={`pocket-${p.pocketId}`}
            >
              <h3>{p.label}</h3>
              <p className="sub" style={{ margin: '4px 0 10px' }}>{p.description}</p>
              <div className="metrics">
                <span>residual RMSE <b className="fail">{p.residualRmse.toFixed(2)}</b></span>
                <span>baseline <b>{p.baselineRmse.toFixed(2)}</b></span>
                <span>+{excess.toFixed(0)}% error</span>
                <span>share <b>{(p.share * 100).toFixed(0)}%</b></span>
              </div>
              <div className="col" style={{ marginTop: 10 }}>proposed field families</div>
              <div className="chips">
                {p.proposedFamilies.map((f) => (
                  <span className="chip fam" key={f}>{FAMILY_LABEL[f] ?? f}</span>
                ))}
              </div>
              <div style={{ marginTop: 12 }}>
                <button className="primary" onClick={() => onSelectPocket(p.pocketId)}>
                  Screen fields for this pocket →
                </button>
              </div>
            </div>
          )
        })}
      </div>
    </div>
  )
}
