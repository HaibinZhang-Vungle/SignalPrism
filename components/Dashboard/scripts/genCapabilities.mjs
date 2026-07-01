// One-time fixture generator (design D1/D2): parse the wide-table schema's field
// tables into a full capability catalog, overlay curated distribution profiling,
// and write src/fixtures/capabilities.json. Re-run with `npm run gen:capabilities`.
//
// This plays the role of the capability scanner offline — the app still only
// *consumes* the catalog, it never parses the schema markdown at runtime.

import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const here = path.dirname(fileURLToPath(import.meta.url))
const SCHEMA = path.resolve(here, '../../../schemas/realtime_attributed_wide_table_schema.md')
const CURATED = path.resolve(here, '../src/fixtures/curatedProfiling.json')
const OUT = path.resolve(here, '../src/fixtures/capabilities.json')

const FEATS = new Set(['key', 'dim', 'feature', 'feature_after_encode', 'leak_risk', 'exclude'])

function domainFromHeader(text) {
  const t = text.toLowerCase()
  if (t.includes('identity')) return 'identity'
  if (t.includes('supply') || t.includes('inventory')) return 'supply'
  if (t.includes('placement')) return 'placement'
  if (t.includes('device-history') || t.includes('device')) return 'device'
  if (t.includes('geo')) return 'geo'
  if (t.includes('privacy') || t.includes('consent')) return 'privacy'
  if (t.includes('floor')) return 'floor'
  if (t.includes('auction') || t.includes('economics')) return 'auction'
  if (t.includes('shading')) return 'shading'
  if (t.includes('settlement') || t.includes('win')) return 'settlement'
  if (t.includes('creative') || t.includes('endcard')) return 'creative'
  if (t.includes('tpat')) return 'tpat'
  if (t.includes('experiment') || t.includes('qps') || t.includes('throttl')) return 'experiment'
  if (t.includes('rtb')) return 'rtb'
  if (t.includes('timing') || t.includes('operational')) return 'timing'
  return 'timing'
}

function eventTypeFor(col) {
  if (col.startsWith('hbn_')) return 'hbn'
  if (col.startsWith('jgr_tpat')) return 'tpat'
  return 'delivery'
}

function familyFor(domain) {
  switch (domain) {
    case 'floor': return 'floor_lifecycle'
    case 'auction':
    case 'settlement':
    case 'shading': return 'price_shape'
    case 'device': return 'device'
    case 'placement':
    case 'creative': return 'ad_unit'
    case 'timing': return 'timeout'
    default: return 'supply_economics'
  }
}

function strategiesFor(semantic, feat) {
  const featureish = feat === 'feature' || feat === 'feature_after_encode' || feat === 'leak_risk'
  if (!featureish) return []
  switch (semantic) {
    case 'money_cpm': return ['numeric_distribution', 'money_cpm', 'numeric_sum_count']
    case 'rate': return ['numeric_distribution', 'numeric_sum_count']
    case 'count': return ['numeric_sum_count', 'count_event']
    case 'boolean_flag': return ['count_if']
    case 'enum_code': return ['topk_map', 'count_if']
    case 'categorical':
    case 'device_attr':
    case 'geo':
    case 'version': return ['topk_map']
    case 'id': return ['cardinality_hll']
    case 'duration_ms': return ['numeric_distribution', 'numeric_sum_count']
    default: return []
  }
}

const clean = (s) => s.replace(/`/g, '').replace(/\\/g, '').trim()

function parseSchema(md) {
  const lines = md.split('\n')
  let domain = 'timing'
  const out = []
  for (const line of lines) {
    const h = line.match(/^#{2,3}\s+(.*)$/)
    if (h) {
      domain = domainFromHeader(h[1])
      continue
    }
    if (!line.startsWith('| `')) continue
    const cells = line.split('|').map((c) => c.trim())
    // cells[0] is '' (leading pipe). Field rows: col, type, source, semantic, null, feat, desc
    const [, col, type, , semantic, nul, feat] = cells
    if (!feat || !FEATS.has(feat)) continue
    const column = clean(col)
    const featureish = feat === 'feature' || feat === 'feature_after_encode' || feat === 'leak_risk'
    const coverage = nul === 'always_present' ? 1.0 : 0.85
    out.push({
      capabilityId: column,
      sourceTable: 'ml_shadow.realtime_attributed_event_wide',
      sourceColumn: column,
      dataType: clean(type) || 'STRING',
      semanticType: clean(semantic) || 'categorical',
      sourceEventType: eventTypeFor(column),
      domain,
      feat,
      nullSemantics: clean(nul) || 'not_observed',
      profilingStatus: 'available',
      allowedAggregationStrategies: strategiesFor(clean(semantic), feat),
      allowedDimensionFamilies: featureish
        ? ['non_device_context_v1', 'inventory_context_lite_v1']
        : [],
      defaultWindows: featureish ? ['7d'] : [],
      owner: 'supply_ai',
      coverage,
      nullRate: Number((1 - coverage).toFixed(2)),
      freshnessMinutes: 0,
      family: familyFor(domain),
      bucketConcentration: 0,
      psi: 0,
      klDivergence: 0,
      baseSeparation: 0,
      screenProfiled: false,
      ...(clean(semantic) === 'enum_code' ? { enumRef: column } : {}),
    })
  }
  return out
}

const md = fs.readFileSync(SCHEMA, 'utf8')
const generated = parseSchema(md)
const curated = JSON.parse(fs.readFileSync(CURATED, 'utf8'))
const curatedColumns = new Set(curated.map((c) => c.sourceColumn))

// Curated wins; drop any generated row for the same wide-table column.
const merged = [...curated, ...generated.filter((g) => !curatedColumns.has(g.sourceColumn))]

const DOMAIN_ORDER = [
  'identity', 'supply', 'placement', 'device', 'geo', 'privacy', 'floor', 'auction',
  'shading', 'settlement', 'creative', 'tpat', 'experiment', 'rtb', 'timing',
]
merged.sort((a, b) => {
  const d = DOMAIN_ORDER.indexOf(a.domain) - DOMAIN_ORDER.indexOf(b.domain)
  return d !== 0 ? d : a.capabilityId.localeCompare(b.capabilityId)
})

fs.writeFileSync(OUT, JSON.stringify(merged, null, 2) + '\n')
console.log(`Wrote ${merged.length} capabilities (${curated.length} curated + ${merged.length - curated.length} generated) to ${path.relative(process.cwd(), OUT)}`)
