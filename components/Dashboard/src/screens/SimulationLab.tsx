import { useMemo, useState } from 'react'
import {
  Bar,
  BarChart,
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'
import type { WorkbenchDataSource } from '../data/WorkbenchDataSource'
import { useAsync } from '../hooks/useAsync'

const TARGET_LABELS = ['settlement_price', 'net_revenue_proxy', 'win_loss', 'event_start']
const MODES = ['A: importance smoke test', 'B: prediction replay', 'C: shadow scoring', 'D: policy sim']

function heatColor(lift: number): string {
  if (lift < 0) return 'rgba(229,103,95,0.55)'
  const t = Math.min(lift / 0.05, 1)
  return `rgba(63,191,127,${0.2 + t * 0.6})`
}

export function SimulationLab({
  ds,
  onTrace,
}: {
  ds: WorkbenchDataSource
  onTrace: (nodeId: string) => void
}) {
  const runs = useAsync(() => ds.listSimulationRuns(), [ds])
  const featureSets = useAsync(() => ds.listFeatureSets(), [ds])
  const [selectedId, setSelectedId] = useState<string | undefined>(undefined)

  const selected = useMemo(() => {
    const list = runs ?? []
    return list.find((r) => r.runId === selectedId) ?? list[0]
  }, [runs, selectedId])

  // The candidate set under test for the selected run, resolved from §7.3.4 metadata.
  const activeSet = useMemo(
    () => (featureSets ?? []).find((fs) => fs.featureSetId === selected?.featureSetId),
    [featureSets, selected],
  )

  const cohortDims = useMemo(() => {
    const set = new Map<string, { value: string; lift: number }[]>()
    for (const c of selected?.cohorts ?? []) {
      const arr = set.get(c.dimension) ?? []
      arr.push({ value: c.value, lift: c.lift })
      set.set(c.dimension, arr)
    }
    return [...set.entries()]
  }, [selected])

  return (
    <div className="screen">
      <h1>Simulation Lab</h1>
      <p className="sub">Assemble a feature set and review baseline-vs-treatment results. (Demo: results are read from fixtures; launch is stubbed.)</p>

      <div className="layout-2">
        <div className="panel">
          <h2>Feature set{activeSet && <span className="col"> · {activeSet.featureSetId}</span>}</h2>
          {activeSet && (
            <p className="sub" style={{ marginTop: -4 }}>
              owner <code>{activeSet.owner}</code> · purpose <code>{activeSet.purpose}</code>
            </p>
          )}
          <label className="field">
            Model endpoint
            <select defaultValue="Floor">
              <option>Floor</option>
              <option>DynamicThrottling</option>
              <option>AuctionDynamics</option>
            </select>
          </label>
          <label className="field">
            Baseline feature set
            <select value={activeSet?.baseFeatureSet ?? ''} disabled>
              {(featureSets ?? []).map((fs) => (
                <option key={fs.featureSetId} value={fs.featureSetId}>{fs.featureSetId}</option>
              ))}
            </select>
          </label>
          <label className="field">
            Candidate features (added)
            <div className="multi">
              {(activeSet?.addedFeatures ?? []).map((f) => (
                <label key={f}><input type="checkbox" defaultChecked /> {f}</label>
              ))}
              {activeSet && activeSet.addedFeatures.length === 0 && (
                <span className="sub">No added features (baseline set).</span>
              )}
            </div>
          </label>
          {activeSet && activeSet.removedFeatures.length > 0 && (
            <label className="field">
              Removed features
              <div className="multi">
                {activeSet.removedFeatures.map((f) => (
                  <label key={f}><input type="checkbox" defaultChecked disabled /> {f}</label>
                ))}
              </div>
            </label>
          )}
          <label className="field">
            Target label
            <select defaultValue={TARGET_LABELS[0]}>
              {TARGET_LABELS.map((l) => <option key={l}>{l}</option>)}
            </select>
          </label>
          <label className="field">
            Simulation mode
            <select defaultValue={MODES[0]}>
              {MODES.map((m) => <option key={m}>{m}</option>)}
            </select>
          </label>
          <button className="primary" disabled title="Launch is stubbed in the fixture demo">
            Launch (stubbed in demo)
          </button>
          <p className="sub" style={{ marginTop: 8 }}>Select an existing run to view results:</p>
          <select value={selected?.runId} onChange={(e) => setSelectedId(e.target.value)}>
            {(runs ?? []).map((r) => (
              <option key={r.runId} value={r.runId}>{r.runId}</option>
            ))}
          </select>
        </div>

        <div className="panel" style={{ gridColumn: 'auto' }}>
          <h2>Result — {selected?.runId}</h2>
          {selected && (
            <>
              <div className="tiles">
                <div className="tile">
                  <div className={`v ${selected.metrics.r2Delta >= 0 ? 'up' : 'down'}`}>
                    {(selected.metrics.r2Delta * 100).toFixed(1)}%
                  </div>
                  <div className="l">R² delta</div>
                </div>
                <div className="tile">
                  <div className={`v ${selected.metrics.topDecileLift >= 0 ? 'up' : 'down'}`}>
                    {(selected.metrics.topDecileLift * 100).toFixed(1)}%
                  </div>
                  <div className="l">Top-decile lift</div>
                </div>
                <div className="tile">
                  <div className="v">{(selected.metrics.featureCoverage * 100).toFixed(0)}%</div>
                  <div className="l">Feature coverage</div>
                </div>
              </div>

              <p className="sub">Lift curve (treatment vs baseline)</p>
              <div style={{ height: 160 }}>
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={selected.liftCurve}>
                    <CartesianGrid stroke="#2a2f3a" />
                    <XAxis dataKey="decile" stroke="#9aa4b2" fontSize={11} />
                    <YAxis stroke="#9aa4b2" fontSize={11} domain={['auto', 'auto']} />
                    <Tooltip contentStyle={{ background: '#1e222b', border: '1px solid #2a2f3a' }} />
                    <Line type="monotone" dataKey="baseline" stroke="#9aa4b2" dot={false} />
                    <Line type="monotone" dataKey="treatment" stroke="#5b8cff" dot={false} />
                  </LineChart>
                </ResponsiveContainer>
              </div>

              <p className="sub">SHAP-style feature importance</p>
              <div style={{ height: 150 }}>
                <ResponsiveContainer width="100%" height="100%">
                  <BarChart data={selected.shap} layout="vertical" margin={{ left: 40 }}>
                    <XAxis type="number" stroke="#9aa4b2" fontSize={11} />
                    <YAxis type="category" dataKey="feature" stroke="#9aa4b2" fontSize={9} width={150} />
                    <Tooltip contentStyle={{ background: '#1e222b', border: '1px solid #2a2f3a' }} />
                    <Bar dataKey="importance" fill="#5b8cff" />
                  </BarChart>
                </ResponsiveContainer>
              </div>

              <p className="sub">Cohort lift heatmap</p>
              {cohortDims.map(([dim, cells]) => (
                <div key={dim} style={{ marginBottom: 6 }}>
                  <div className="col">{dim}</div>
                  <div className="heatmap" style={{ gridTemplateColumns: `repeat(${cells.length}, 1fr)` }}>
                    {cells.map((c) => (
                      <div key={c.value} className="heatcell" style={{ background: heatColor(c.lift) }}>
                        {c.value}<br />{(c.lift * 100).toFixed(1)}%
                      </div>
                    ))}
                  </div>
                </div>
              ))}

              <div style={{ marginTop: 12 }}>
                <button className="ghost" onClick={() => onTrace(selected.featureSetId)}>
                  Trace lineage →
                </button>
              </div>
            </>
          )}
        </div>
      </div>
    </div>
  )
}
