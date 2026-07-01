import { useMemo } from 'react'
import type { WorkbenchDataSource } from '../data/WorkbenchDataSource'
import type { ScreenedField } from '../data/types'
import { useAsync } from '../hooks/useAsync'
import { canPromote } from '../screen/screen'

function bar(value: number, tone: 'good' | 'bad' | 'neutral' = 'neutral') {
  const color = tone === 'good' ? 'var(--good)' : tone === 'bad' ? 'var(--bad)' : 'var(--accent)'
  return (
    <div className="statbar">
      <div className="statbar-fill" style={{ width: `${Math.round(value * 100)}%`, background: color }} />
    </div>
  )
}

const VERDICT_TONE: Record<ScreenedField['verdict'], string> = {
  strong: 'pass',
  weak: 'warn',
  blocked: 'fail',
}

export function DistributionScreen({
  ds,
  selectedPocket,
  onSelectPocket,
  promoted,
  onTogglePromote,
}: {
  ds: WorkbenchDataSource
  selectedPocket?: string
  onSelectPocket: (pocketId: string) => void
  promoted: Set<string>
  onTogglePromote: (capabilityId: string) => void
}) {
  const pockets = useAsync(() => ds.listResidualPockets(), [ds])
  const fields = useAsync(() => ds.screenFields(selectedPocket), [ds, selectedPocket])

  const pocket = useMemo(
    () => (pockets ?? []).find((p) => p.pocketId === selectedPocket),
    [pockets, selectedPocket],
  )

  const strongCount = (fields ?? []).filter((f) => f.verdict === 'strong').length

  return (
    <div className="screen">
      <h1>Distribution Screen</h1>
      <p className="sub">
        Cheap evidence before training: rank raw fields on distribution shape for the selected
        residual pocket. Only strong signals may be promoted to aggregation. (Demo profiling.)
      </p>

      <div className="tabs">
        <span style={{ alignSelf: 'center', color: 'var(--muted)', fontSize: 12, marginRight: 4 }}>Pocket:</span>
        {(pockets ?? []).map((p) => (
          <button
            key={p.pocketId}
            className={p.pocketId === selectedPocket ? 'active' : ''}
            onClick={() => onSelectPocket(p.pocketId)}
          >
            {p.label}
          </button>
        ))}
      </div>

      {!selectedPocket && (
        <div className="warn-box">
          No pocket selected — showing pocket-agnostic separation. Pick a pocket above (or start from
          Residual Diagnostics) to rank fields against a high-error subgroup.
        </div>
      )}
      {pocket && (
        <p className="sub">
          Ranking for <b>{pocket.label}</b>. {strongCount} strong signal(s) found.
        </p>
      )}

      <table className="tbl screen-tbl">
        <thead>
          <tr>
            <th>Field</th>
            <th>Family</th>
            <th>Coverage</th>
            <th>Missing</th>
            <th>Bucket conc.</th>
            <th>KL</th>
            <th>PSI</th>
            <th>Subgroup separation</th>
            <th>Verdict</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          {(fields ?? []).map((f) => (
            <tr key={f.capabilityId} data-testid={`screen-${f.capabilityId}`}>
              <td><code>{f.capabilityId}</code></td>
              <td className="col">{f.family.replace(/_/g, ' ')}</td>
              <td>{(f.stats.coverage * 100).toFixed(0)}%</td>
              <td>{(f.stats.missingness * 100).toFixed(0)}%</td>
              <td>{(f.stats.bucketConcentration * 100).toFixed(0)}%</td>
              <td>{f.stats.klDivergence.toFixed(2)}</td>
              <td className={f.drifting ? 'fail' : ''}>
                {f.stats.psi.toFixed(2)}{f.drifting ? ' ⚠' : ''}
              </td>
              <td style={{ minWidth: 120 }}>
                {(f.stats.subgroupSeparation * 100).toFixed(0)}%
                {bar(f.stats.subgroupSeparation, f.verdict === 'strong' ? 'good' : 'neutral')}
              </td>
              <td>
                <span className={`pill ${VERDICT_TONE[f.verdict]}`} title={f.reasons.join('; ')}>
                  {f.verdict}
                </span>
              </td>
              <td>
                <button
                  className={promoted.has(f.capabilityId) ? 'ghost' : 'primary'}
                  disabled={!canPromote(f)}
                  title={canPromote(f) ? '' : f.reasons.join('; ')}
                  onClick={() => onTogglePromote(f.capabilityId)}
                >
                  {promoted.has(f.capabilityId) ? 'Promoted ✓' : 'Promote'}
                </button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>

      <p className="sub" style={{ marginTop: 14 }}>
        Weak / blocked fields cannot be promoted — hover a verdict for the reason. Only promoted
        fields become selectable in the Aggregation Builder (steps 5–7).
      </p>
    </div>
  )
}
