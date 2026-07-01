import { useMemo, useState } from 'react'
import type { WorkbenchDataSource } from '../data/WorkbenchDataSource'
import type {
  AggregationConfig,
  AggregationPreview,
  AggregationStrategy,
  DimensionFamilyId,
  WindowSpec,
} from '../data/types'
import { useAsync } from '../hooks/useAsync'
import { isSelectable } from './CapabilityMap'
import { ALLOWED_DIMENSION_KEYS, familiesSupportingKey } from '../config/dimensionFamilies'

const ALL_WINDOWS: WindowSpec[] = ['1h', '1d', '7d', '30d']
const SAMPLE_RATES = ['0.01pct', '0.1pct', '1pct']

export function AggregationBuilder({ ds }: { ds: WorkbenchDataSource }) {
  const capabilities = useAsync(() => ds.listCapabilities(), [ds])
  const families = useAsync(() => ds.listDimensionFamilies(), [ds])

  const [family, setFamily] = useState<DimensionFamilyId>('non_device_context_v1')
  const [windows, setWindows] = useState<WindowSpec[]>(['7d'])
  const [sampleRate, setSampleRate] = useState('0.01pct')
  const [measures, setMeasures] = useState<Record<string, AggregationStrategy>>({})
  const [customDim, setCustomDim] = useState('')
  const [dimError, setDimError] = useState<string | null>(null)
  const [preview, setPreview] = useState<AggregationPreview | null>(null)

  const selectable = useMemo(
    () => (capabilities ?? []).filter(isSelectable),
    [capabilities],
  )
  // Only capabilities allowed in the chosen family can be measured there.
  const eligible = selectable.filter((c) => c.allowedDimensionFamilies.includes(family))

  const toggleMeasure = (capId: string, strategy: AggregationStrategy) => {
    setMeasures((prev) => {
      const next = { ...prev }
      if (next[capId]) delete next[capId]
      else next[capId] = strategy
      return next
    })
    setPreview(null)
  }

  const setStrategy = (capId: string, strategy: AggregationStrategy) => {
    setMeasures((prev) => ({ ...prev, [capId]: strategy }))
    setPreview(null)
  }

  const toggleWindow = (w: WindowSpec) => {
    setWindows((prev) => (prev.includes(w) ? prev.filter((x) => x !== w) : [...prev, w]))
    setPreview(null)
  }

  // Guardrail: arbitrary dimensions are rejected; point to the closest family.
  const tryAddDimension = () => {
    const key = customDim.trim()
    if (!key) return
    if (ALLOWED_DIMENSION_KEYS.has(key)) {
      const fams = familiesSupportingKey(key)
      setDimError(null)
      setCustomDim('')
      // A supported key just means "pick the family that has it".
      if (fams.length && !fams.includes(family)) {
        setDimError(`"${key}" is supported by: ${fams.join(', ')}. Switch to one of those families.`)
      }
    } else {
      setDimError(
        `Arbitrary dimension "${key}" is not allowed. Only reviewed dimension families are permitted; ` +
          `no fixed family contains this key.`,
      )
    }
  }

  const config: AggregationConfig = useMemo(
    () => ({
      aggId: `demo_${family}`,
      source: 'ml_shadow.realtime_attributed_event_wide',
      eventFilter: "source_event_type in ('hbn', 'tpat', 'delivery')",
      timeColumn: 'source_event_time',
      sample: { type: 'event_id_hash', rate: sampleRate },
      dimensionFamily: family,
      windows,
      measures: Object.entries(measures).map(([capabilityId, strategy]) => ({
        capabilityId,
        strategy,
      })),
      output: { tablePrefix: `ml_shadow_feature.demo_${family}` },
    }),
    [family, windows, sampleRate, measures],
  )

  const runPreview = async () => {
    setPreview(await ds.previewAggregation(config))
  }

  return (
    <div className="screen">
      <h1>Aggregation Builder</h1>
      <p className="sub">Configure a sampled aggregation without code. Preview cost before materializing.</p>

      <div className="layout-2">
        <div className="panel">
          <h2>Configuration</h2>

          <label className="field">
            Dimension family (reviewed only)
            <select
              value={family}
              onChange={(e) => {
                setFamily(e.target.value as DimensionFamilyId)
                setMeasures({})
                setPreview(null)
              }}
            >
              {(families ?? []).map((f) => (
                <option key={f.id} value={f.id}>
                  {f.id} — {f.costTier} cost
                </option>
              ))}
            </select>
          </label>

          <div className="field">
            Windows
            <div className="tabs">
              {ALL_WINDOWS.map((w) => (
                <button
                  key={w}
                  className={windows.includes(w) ? 'active' : ''}
                  onClick={() => toggleWindow(w)}
                >
                  {w}
                </button>
              ))}
            </div>
          </div>

          <label className="field">
            Sample rate (event-id hash)
            <select value={sampleRate} onChange={(e) => { setSampleRate(e.target.value); setPreview(null) }}>
              {SAMPLE_RATES.map((r) => (
                <option key={r} value={r}>{r}</option>
              ))}
            </select>
          </label>

          <div className="field">
            Capabilities & strategy ({eligible.length} eligible in this family)
            <div className="multi">
              {eligible.map((c) => {
                const on = !!measures[c.capabilityId]
                return (
                  <div key={c.capabilityId}>
                    <label>
                      <input
                        type="checkbox"
                        checked={on}
                        onChange={() => toggleMeasure(c.capabilityId, c.allowedAggregationStrategies[0])}
                      />
                      {c.capabilityId}
                    </label>
                    {on && (
                      <select
                        value={measures[c.capabilityId]}
                        onChange={(e) => setStrategy(c.capabilityId, e.target.value as AggregationStrategy)}
                        style={{ marginTop: 4 }}
                      >
                        {c.allowedAggregationStrategies.map((s) => (
                          <option key={s} value={s}>{s}</option>
                        ))}
                      </select>
                    )}
                  </div>
                )
              })}
            </div>
          </div>

          <label className="field">
            Try adding a custom dimension (guardrail demo)
            <div style={{ display: 'flex', gap: 8 }}>
              <input
                value={customDim}
                placeholder="e.g. jgr_dev_ip"
                onChange={(e) => setCustomDim(e.target.value)}
              />
              <button className="ghost" onClick={tryAddDimension}>Add</button>
            </div>
          </label>
          {dimError && <div className="err-box" role="alert">{dimError}</div>}

          <button
            className="primary"
            disabled={config.measures.length === 0 || windows.length === 0}
            onClick={runPreview}
          >
            Preview cost
          </button>
        </div>

        <div className="panel">
          <h2>Plan preview</h2>
          {!preview && <p className="sub">Configure measures and click “Preview cost”.</p>}
          {preview && (
            <>
              <div className="metrics">
                <span>rows/day <b>{preview.estimatedRowsPerDay.toLocaleString()}</b></span>
                <span>bytes/day <b>{(preview.estimatedBytesPerDay / 1e9).toFixed(2)} GB</b></span>
              </div>
              <p className="sub" style={{ marginTop: 12 }}>Output tables</p>
              <pre className="code">{preview.outputTables.join('\n')}</pre>
              {preview.warnings.map((w, i) => (
                <div className="warn-box" key={i} role="alert">⚠ {w}</div>
              ))}
              {preview.blocked && (
                <div className="block-box" role="alert">⛔ {preview.blockReason}</div>
              )}
              <p className="sub" style={{ marginTop: 12 }}>Serialized config (§7.7 Step 2)</p>
              <pre className="code">{JSON.stringify(config, null, 2)}</pre>
            </>
          )}
        </div>
      </div>
    </div>
  )
}
