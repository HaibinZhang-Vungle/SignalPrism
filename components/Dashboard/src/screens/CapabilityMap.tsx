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
 * A capability is selectable as a feature only when it is profiled `available`
 * and not `exclude` (PII). Dimensions/keys are catalogued but not feature candidates.
 */
export function isSelectable(cap: FeatureCapability): boolean {
  return cap.profilingStatus === 'available' && cap.feat !== 'exclude'
}

/** Feature candidates (the pool the fast screen draws from). */
export function isFeatureCandidate(cap: FeatureCapability): boolean {
  return cap.feat === 'feature' || cap.feat === 'feature_after_encode' || cap.feat === 'leak_risk'
}

const FEAT_BADGE: Record<FeatureCapability['feat'], { label: string; cls: string }> = {
  key: { label: 'key', cls: 'muted' },
  dim: { label: 'dimension', cls: 'muted' },
  feature: { label: 'feature', cls: 'ok' },
  feature_after_encode: { label: 'feature·encode', cls: 'ok' },
  leak_risk: { label: 'label / leak_risk', cls: 'bad' },
  exclude: { label: 'PII · excluded', cls: 'bad' },
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
  const [candidatesOnly, setCandidatesOnly] = useState(false)

  const all = useMemo(() => capabilities ?? [], [capabilities])

  const visible = useMemo(
    () =>
      all
        .filter((c) => tab === 'all' || c.sourceEventType === tab)
        .filter((c) => !candidatesOnly || isFeatureCandidate(c)),
    [all, tab, candidatesOnly],
  )

  const byDomain = useMemo(() => {
    const m = new Map<Domain, FeatureCapability[]>()
    for (const c of visible) {
      const list = m.get(c.domain) ?? []
      list.push(c)
      m.set(c.domain, list)
    }
    return [...m.entries()]
  }, [visible])

  const candidateCount = all.filter(isFeatureCandidate).length
  const screenedCount = all.filter((c) => c.screenProfiled).length

  return (
    <div className="screen">
      <h1>Capability Map</h1>
      <p className="sub">
        Full wide-table schema catalog — {all.length} fields, {candidateCount} feature candidate(s),{' '}
        {screenedCount} distribution-profiled for the screen. Grouped by domain; scanned from the
        schema without a second pass.
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
        <span style={{ flex: 1 }} />
        <label style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 12, color: 'var(--muted)' }}>
          <input type="checkbox" checked={candidatesOnly} onChange={(e) => setCandidatesOnly(e.target.checked)} />
          Feature candidates only
        </label>
      </div>

      {byDomain.map(([domain, caps]) => (
        <div className="domain-group" key={domain}>
          <h2>{domain} · {caps.length}</h2>
          <div className="grid">
            {caps.map((c) => {
              const badge = FEAT_BADGE[c.feat]
              const candidate = isFeatureCandidate(c)
              return (
                <div className="card" key={c.capabilityId} data-testid={`cap-${c.capabilityId}`}>
                  <h3>{c.capabilityId}</h3>
                  <div className="col">{c.sourceColumn} · {c.dataType} · {c.semanticType}</div>
                  <div className="chips">
                    <span className={`chip role role-${badge.cls}`}>{badge.label}</span>
                    <span className="chip">{c.profilingStatus}</span>
                    {c.screenProfiled && <span className="chip strat">screened</span>}
                  </div>
                  {candidate && c.profilingStatus === 'available' && (
                    <>
                      <div className="metrics">
                        <span>coverage <b>{(c.coverage * 100).toFixed(0)}%</b></span>
                        <span>null <b>{(c.nullRate * 100).toFixed(0)}%</b></span>
                      </div>
                      <div className="chips">
                        {c.allowedAggregationStrategies.slice(0, 3).map((s) => (
                          <span className="chip strat" key={s}>{s}</span>
                        ))}
                      </div>
                      <div style={{ marginTop: 10 }}>
                        <button className="ghost" onClick={() => onTrace(c.capabilityId)}>
                          Trace lineage →
                        </button>
                      </div>
                    </>
                  )}
                  {!candidate && (
                    <div className="sub" style={{ marginTop: 8, fontSize: 11 }}>
                      {c.feat === 'exclude' ? 'Not offered — raw PII.' : 'Catalog entry — not a feature candidate.'}
                    </div>
                  )}
                </div>
              )
            })}
          </div>
        </div>
      ))}
    </div>
  )
}
