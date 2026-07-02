import { useEffect, useMemo, useState } from 'react'
import type { WorkbenchDataSource } from '../data/WorkbenchDataSource'
import type { DerivedFeature, DimensionFamilyId, FillPolicy, WindowSpec } from '../data/types'
import { useAsync } from '../hooks/useAsync'
import { validateFormula, ALLOWED_FUNCTIONS, parseFormula, referencedIdentifiers } from '../formula/dsl'
import type { ValidationContext } from '../formula/dsl'

/** Referenced identifiers that resolve to real primitives in scope (§7.3.3 source_primitives). */
function sourcePrimitivesOf(formula: string, available: ReadonlySet<string>): string[] {
  try {
    return referencedIdentifiers(parseFormula(formula)).filter((id) => available.has(id))
  } catch {
    return []
  }
}

const GUIDED_TEMPLATES: { label: string; formula: string }[] = [
  { label: 'avg(sum, count)', formula: 'safe_div(hbn_settlement_price_sum_7d, hbn_settlement_price_count_7d)' },
  { label: 'CV(sum, sumsq, count)', formula: 'cv(hbn_settlement_price_sum_7d, hbn_settlement_price_sum_sq_7d, hbn_settlement_price_count_7d)' },
  { label: 'rate(a, b)', formula: 'safe_div(tpat_event_start_count_sum_7d, delivery_count_7d)' },
]

export function FormulaStudio({ ds }: { ds: WorkbenchDataSource }) {
  const primitives = useAsync(() => ds.listPrimitives(), [ds])
  const capabilities = useAsync(() => ds.listCapabilities(), [ds])

  const [family, setFamily] = useState<DimensionFamilyId>('non_device_context_v1')
  const [win, setWin] = useState<WindowSpec>('7d')
  const [mode, setMode] = useState<'guided' | 'advanced'>('advanced')
  const [formula, setFormula] = useState(GUIDED_TEMPLATES[0].formula)

  // Save-as-derived-feature form (TRD §7.3.3).
  const [featureId, setFeatureId] = useState('')
  const [displayName, setDisplayName] = useState('')
  const [fillDefault, setFillDefault] = useState<FillPolicy['default']>('null')
  const [fillModelInput, setFillModelInput] = useState<FillPolicy['modelInput']>('nan')
  const [saved, setSaved] = useState<DerivedFeature[]>([])
  const [savedMsg, setSavedMsg] = useState<string | undefined>(undefined)

  // Seed the saved list from the data source once loaded.
  useEffect(() => {
    ds.listDerivedFeatures().then(setSaved)
  }, [ds])

  const scopedPrimitives = useMemo(
    () => (primitives ?? []).filter((p) => p.dimensionFamily === family && p.window === win),
    [primitives, family, win],
  )

  const ctx: ValidationContext = useMemo(() => {
    const forbidden = new Set(
      (capabilities ?? [])
        .filter((c) => c.feat === 'leak_risk')
        .map((c) => c.sourceColumn),
    )
    const coverageByPrimitive: Record<string, number> = {}
    for (const p of scopedPrimitives) {
      const cap = (capabilities ?? []).find((c) => c.capabilityId === p.capabilityId)
      coverageByPrimitive[p.primitiveId] = cap?.coverage ?? 1
    }
    return {
      availablePrimitives: new Set(scopedPrimitives.map((p) => p.primitiveId)),
      forbiddenRawColumns: forbidden,
      coverageByPrimitive,
    }
  }, [capabilities, scopedPrimitives])

  const result = useMemo(() => validateFormula(formula, ctx), [formula, ctx])

  const sourcePrimitives = useMemo(
    () => sourcePrimitivesOf(formula, ctx.availablePrimitives),
    [formula, ctx],
  )
  const trimmedId = featureId.trim()
  const canSave = result.ok && trimmedId.length > 0 && sourcePrimitives.length > 0

  const onSave = async () => {
    if (!canSave) return
    const feature: DerivedFeature = {
      featureId: trimmedId,
      displayName: displayName.trim() || trimmedId,
      dimensionFamily: family,
      window: win,
      formula: formula.trim(),
      sourcePrimitives,
      fillPolicy: { default: fillDefault, modelInput: fillModelInput },
      status: 'proposed',
    }
    const stored = await ds.saveDerivedFeature(feature)
    setSaved(await ds.listDerivedFeatures())
    setSavedMsg(`Saved "${stored.featureId}" (${stored.status}).`)
  }

  const checkRow = (label: string, status: 'pass' | 'fail') => (
    <div className="check-row">
      <span>{label}</span>
      <span className={`pill ${status}`}>{status}</span>
    </div>
  )

  return (
    <div className="screen">
      <h1>Formula Studio</h1>
      <p className="sub">Compose derived features from materialized primitives. Validated against the DSL rules (§7.6).</p>

      <div className="tabs">
        <button className={mode === 'guided' ? 'active' : ''} onClick={() => setMode('guided')}>Guided</button>
        <button className={mode === 'advanced' ? 'active' : ''} onClick={() => setMode('advanced')}>Advanced (DSL)</button>
        <span style={{ flex: 1 }} />
        <select value={family} onChange={(e) => setFamily(e.target.value as DimensionFamilyId)}>
          <option value="non_device_context_v1">non_device_context_v1</option>
          <option value="inventory_context_lite_v1">inventory_context_lite_v1</option>
          <option value="device_level_v1">device_level_v1</option>
          <option value="global_baseline_v1">global_baseline_v1</option>
        </select>
        <select value={win} onChange={(e) => setWin(e.target.value as WindowSpec)}>
          <option value="1h">1h</option>
          <option value="1d">1d</option>
          <option value="7d">7d</option>
        </select>
      </div>

      <div className="layout-2">
        <div className="panel">
          <h2>Available primitives ({family} · {win})</h2>
          <div className="multi" style={{ marginBottom: 14 }}>
            {scopedPrimitives.length === 0 && <span className="sub">No primitives for this scope.</span>}
            {scopedPrimitives.map((p) => (
              <label key={p.primitiveId} className="disabled">
                <code>{p.primitiveId}</code>
              </label>
            ))}
          </div>

          {mode === 'guided' ? (
            <div className="field">
              Pick a template
              <div className="multi">
                {GUIDED_TEMPLATES.map((t) => (
                  <label key={t.label}>
                    <input
                      type="radio"
                      name="tmpl"
                      checked={formula === t.formula}
                      onChange={() => setFormula(t.formula)}
                    />
                    {t.label}
                  </label>
                ))}
              </div>
            </div>
          ) : (
            <label className="field">
              Formula
              <textarea value={formula} onChange={(e) => setFormula(e.target.value)} spellCheck={false} />
            </label>
          )}
          <p className="sub">Functions: {Object.keys(ALLOWED_FUNCTIONS).join(', ')}</p>
        </div>

        <div className="panel">
          <h2>Validation</h2>
          {checkRow('Type check', result.typeCheck)}
          {checkRow('Division safety', result.divisionSafety)}
          {checkRow('Point-in-time (no future labels)', result.pointInTime)}
          {checkRow('Source primitive availability', result.primitiveAvailability)}
          <div className="check-row">
            <span>Coverage estimate</span>
            <b>{(result.coverageEstimate * 100).toFixed(1)}%</b>
          </div>

          {result.errors.length > 0 && (
            <div className="err-box" role="alert">
              {result.errors.map((e, i) => (
                <div key={i}>• {e}</div>
              ))}
            </div>
          )}

          {/* Sample output distribution (illustrative, demo-level). */}
          <p className="sub" style={{ marginTop: 14 }}>Sample output distribution</p>
          <div style={{ display: 'flex', alignItems: 'flex-end', gap: 3, height: 60 }}>
            {[3, 8, 16, 22, 18, 11, 6, 3].map((h, i) => (
              <div
                key={i}
                style={{
                  flex: 1,
                  height: `${result.ok ? h * 2.6 : 4}px`,
                  background: result.ok ? 'var(--accent)' : 'var(--border)',
                  borderRadius: 2,
                }}
              />
            ))}
          </div>

          {/* Save as a derived feature (TRD §7.3.3). Requires a valid formula. */}
          <div className="save-derived" style={{ marginTop: 16, borderTop: '1px solid var(--border)', paddingTop: 14 }}>
            <h2>Save as derived feature</h2>
            <label className="field">
              Feature ID
              <input
                value={featureId}
                onChange={(e) => setFeatureId(e.target.value)}
                placeholder={`e.g. bid_premium_rate_${win}`}
                spellCheck={false}
              />
            </label>
            <label className="field">
              Display name
              <input
                value={displayName}
                onChange={(e) => setDisplayName(e.target.value)}
                placeholder="optional; defaults to Feature ID"
              />
            </label>
            <div className="check-row">
              <span>Fill policy</span>
              <span>
                default{' '}
                <select value={fillDefault} onChange={(e) => setFillDefault(e.target.value as FillPolicy['default'])}>
                  <option value="null">null</option>
                  <option value="zero">zero</option>
                </select>
                {'  '}model_input{' '}
                <select value={fillModelInput} onChange={(e) => setFillModelInput(e.target.value as FillPolicy['modelInput'])}>
                  <option value="nan">nan</option>
                  <option value="zero">zero</option>
                </select>
              </span>
            </div>
            <p className="sub">
              Source primitives:{' '}
              {sourcePrimitives.length ? sourcePrimitives.map((p) => <code key={p} style={{ marginRight: 6 }}>{p}</code>) : '—'}
              {' · '}{family} · {win}
            </p>
            <button className="primary" onClick={onSave} disabled={!canSave} data-testid="save-derived">
              Save derived feature
            </button>
            {!result.ok && <p className="sub">Fix validation errors before saving.</p>}
            {savedMsg && <p className="sub" role="status" style={{ color: 'var(--good, #3fbf7f)' }}>{savedMsg}</p>}
          </div>
        </div>
      </div>

      <div className="panel" style={{ marginTop: 16 }}>
        <h2>Saved derived features · {saved.length}</h2>
        <table className="tbl" data-testid="derived-list">
          <thead>
            <tr>
              <th>feature_id</th>
              <th>formula</th>
              <th>dimension family</th>
              <th>window</th>
              <th>status</th>
            </tr>
          </thead>
          <tbody>
            {saved.map((f) => (
              <tr key={f.featureId} data-testid={`derived-${f.featureId}`}>
                <td>{f.featureId}</td>
                <td><code style={{ fontSize: 11 }}>{f.formula}</code></td>
                <td>{f.dimensionFamily}</td>
                <td>{f.window}</td>
                <td><span className="pill">{f.status}</span></td>
              </tr>
            ))}
            {saved.length === 0 && (
              <tr><td colSpan={5} className="sub">No derived features saved yet.</td></tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  )
}
