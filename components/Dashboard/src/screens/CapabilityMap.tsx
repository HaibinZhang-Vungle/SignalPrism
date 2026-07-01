import { useMemo, useState } from 'react'
import type { WorkbenchDataSource } from '../data/WorkbenchDataSource'
import type { Domain, FeatureCapability, SourceEventType } from '../data/types'
import { useAsync } from '../hooks/useAsync'

const SOURCE_TABS: { id: SourceEventType | 'all'; label: string }[] = [
  { id: 'all', label: 'All' },
  { id: 'delivery', label: 'Delivery' },
  { id: 'no_serv', label: 'No-serv' },
  { id: 'hbn', label: 'HBN' },
  { id: 'tpat', label: 'TPAT' },
]

/**
 * A capability is selectable only when it is profiled `available` and not
 * `exclude` (PII). This is the single gate the Capability Map enforces
 * (spec: "PII and excluded columns are not offered" / "not passed profiling").
 */
export function isSelectable(cap: FeatureCapability): boolean {
  return cap.profilingStatus === 'available' && cap.feat !== 'exclude'
}

export function CapabilityMap({
  ds,
  onTrace,
}: {
  ds: WorkbenchDataSource
  onTrace: (nodeId: string) => void
}) {
  const capabilities = useAsync(() => ds.listCapabilities(), [ds])
  const [tab, setTab] = useState<SourceEventType | 'all'>('all')

  const selectable = useMemo(
    () => (capabilities ?? []).filter(isSelectable),
    [capabilities],
  )

  const visible = useMemo(
    () => selectable.filter((c) => tab === 'all' || c.sourceEventType === tab),
    [selectable, tab],
  )

  const byDomain = useMemo(() => {
    const m = new Map<Domain, FeatureCapability[]>()
    for (const c of visible) {
      const list = m.get(c.domain) ?? []
      list.push(c)
      m.set(c.domain, list)
    }
    return [...m.entries()].sort((a, b) => a[0].localeCompare(b[0]))
  }, [visible])

  const hidden = (capabilities?.length ?? 0) - selectable.length

  return (
    <div className="screen">
      <h1>Capability Map</h1>
      <p className="sub">
        Available wide-table columns registered as feature capabilities, grouped by domain.
        {hidden > 0 && ` ${hidden} column(s) hidden (PII-excluded or not profiled).`}
      </p>

      <div className="tabs" role="tablist">
        {SOURCE_TABS.map((t) => (
          <button
            key={t.id}
            role="tab"
            className={tab === t.id ? 'active' : ''}
            onClick={() => setTab(t.id)}
          >
            {t.label}
          </button>
        ))}
      </div>

      {byDomain.map(([domain, caps]) => (
        <div className="domain-group" key={domain}>
          <h2>{domain}</h2>
          <div className="grid">
            {caps.map((c) => (
              <div className="card" key={c.capabilityId} data-testid={`cap-${c.capabilityId}`}>
                <h3>{c.capabilityId}</h3>
                <div className="col">{c.sourceColumn}</div>
                <div className="metrics">
                  <span>coverage <b>{(c.coverage * 100).toFixed(0)}%</b></span>
                  <span>null <b>{(c.nullRate * 100).toFixed(0)}%</b></span>
                  <span>fresh <b>{c.freshnessMinutes}m</b></span>
                  {c.feat === 'leak_risk' && <span className="fail">label / leak_risk</span>}
                </div>
                <div className="chips">
                  {c.allowedAggregationStrategies.map((s) => (
                    <span className="chip strat" key={s}>{s}</span>
                  ))}
                </div>
                <div className="chips">
                  {c.allowedDimensionFamilies.map((f) => (
                    <span className="chip fam" key={f}>{f}</span>
                  ))}
                </div>
                <div style={{ marginTop: 10 }}>
                  <button className="ghost" onClick={() => onTrace(c.capabilityId)}>
                    Trace lineage →
                  </button>
                </div>
              </div>
            ))}
          </div>
        </div>
      ))}
    </div>
  )
}
