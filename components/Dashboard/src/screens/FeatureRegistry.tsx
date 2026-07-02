import { useMemo, useState } from 'react'
import type { WorkbenchDataSource } from '../data/WorkbenchDataSource'
import type { AggregateMetric } from '../data/types'
import { useAsync } from '../hooks/useAsync'

const SUFFIX = ['_sum', '_count', '_min', '_max', '_squaresum']

export function FeatureRegistry({ ds }: { ds: WorkbenchDataSource }) {
  const catalog = useAsync(() => ds.listAggregateFeatures(), [ds])
  const [tableId, setTableId] = useState<string | undefined>(undefined)

  const table = useMemo(() => {
    const tables = catalog?.tables ?? []
    return tables.find((t) => t.tableId === tableId) ?? tables[0]
  }, [catalog, tableId])

  const metrics = catalog?.metrics ?? []
  const distribution = metrics.filter((m) => m.kind === 'distribution')
  const counts = metrics.filter((m) => m.kind === 'count')

  const metricCard = (m: AggregateMetric) => (
    <div className="card" key={m.metricId} data-testid={`metric-${m.metricId}`}>
      <h3>{m.metricId}</h3>
      <div className="col">{m.source}</div>
      <div className="chips">
        <span className={`chip role ${m.kind === 'distribution' ? 'role-muted' : 'strat'}`}>{m.kind}</span>
        <span className="chip">{m.dataType}</span>
        {m.labelLike && <span className="chip role role-bad" title="Label-like / point-in-time sensitive">label / point-in-time</span>}
      </div>
      {m.kind === 'distribution' ? (
        <div className="chips" style={{ marginTop: 8 }}>
          {SUFFIX.map((s) => (
            <span className="chip strat" key={s}><code>{m.metricId}{s}</code></span>
          ))}
        </div>
      ) : (
        <div className="chips" style={{ marginTop: 8 }}>
          <span className="chip strat"><code>{m.metricId}</code></span>
        </div>
      )}
      {m.notes && <div className="sub" style={{ marginTop: 8, fontSize: 11 }}>{m.notes}</div>}
    </div>
  )

  return (
    <div className="screen">
      <h1>Feature Registry</h1>
      <p className="sub">
        Existing aggregate features from the aggregation table schema — {distribution.length} distribution
        metric families (each → 5 columns), {counts.length} count metrics, shared across both tables. Select
        a table to see its dimensions.
      </p>

      <div className="tabs" role="tablist">
        {(catalog?.tables ?? []).map((t) => (
          <button
            key={t.tableId}
            role="tab"
            className={table?.tableId === t.tableId ? 'active' : ''}
            onClick={() => setTableId(t.tableId)}
          >
            {t.dimensionFamily}
          </button>
        ))}
      </div>

      {table && (
        <p className="sub">
          <code>ml_shadow_feature.{table.tableId}</code> · key <code>{table.primaryKey}</code> · {table.purpose}
        </p>
      )}

      <div className="domain-group">
        <h2>Distribution metrics · {distribution.length}</h2>
        <div className="grid">{distribution.map(metricCard)}</div>
      </div>

      <div className="domain-group">
        <h2>Count metrics · {counts.length}</h2>
        <div className="grid">{counts.map(metricCard)}</div>
      </div>

      <div className="domain-group">
        <h2>Dimensions · {table?.dimensionColumns.length ?? 0} <span className="col">({table?.dimensionFamily})</span></h2>
        <div className="chips">
          {(table?.dimensionColumns ?? []).map((d) => (
            <span className="chip fam" key={d}><code>{d}</code></span>
          ))}
        </div>
      </div>
    </div>
  )
}
