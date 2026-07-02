// One-time generator: parse the aggregation table schema into the aggregate-feature
// catalog (metric families + count metrics + per-table dimension columns), and write
// src/fixtures/aggregateFeatures.json. Re-run with `npm run gen:aggregate-features`.
// Plays the offline scanner role — the app only consumes the catalog.

import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const here = path.dirname(fileURLToPath(import.meta.url))
const SCHEMA = path.resolve(here, '../../../schemas/realtime_attributed_aggregation_table_schema.md')
const OUT = path.resolve(here, '../src/fixtures/aggregateFeatures.json')

const DIST_SUFFIXES = ['sum', 'count', 'min', 'max', 'squaresum']
const clean = (s) => (s || '').replace(/`/g, '').replace(/\\/g, '').trim()

// Section identity is stable; dimension columns are filled from §3/§4 parsing.
const TABLES = [
  {
    tableId: 'realtime_attributed_device_level_hly',
    dimensionFamily: 'device_level_v1',
    purpose: 'Device / user history features and KVRocks export candidates.',
    primaryKey: 'device_id',
    dimensionColumns: [],
  },
  {
    tableId: 'realtime_attributed_non_device_context_hly',
    dimensionFamily: 'non_device_context_v1',
    purpose: 'Supply, inventory, geo, privacy/cohort, and experiment context without device identity.',
    primaryKey: 'context_dim_id',
    dimensionColumns: [],
  },
]

function sectionOf(header) {
  const t = header.toLowerCase()
  if (t.includes('device_level_v1` dimensions') || t.includes('device_level_v1 dimensions')) return 'device_dims'
  if (t.includes('non_device_context_v1` dimensions') || t.includes('non_device_context_v1 dimensions')) return 'nondevice_dims'
  if (t.includes('distribution metric families')) return 'dist'
  if (t.includes('count metric columns')) return 'count'
  return null
}

const md = fs.readFileSync(SCHEMA, 'utf8')
const lines = md.split('\n')
let section = null
const metrics = []
const deviceDims = []
const nonDeviceDims = []

for (const line of lines) {
  const h = line.match(/^#{2,4}\s+(.*)$/)
  if (h) { section = sectionOf(h[1]); continue }
  if (!section || !line.startsWith('| `')) continue

  const cells = line.split('|').map((c) => c.trim())
  const col1 = clean(cells[1])
  if (!col1) continue

  if (section === 'device_dims') {
    deviceDims.push(col1)
  } else if (section === 'nondevice_dims') {
    nonDeviceDims.push(col1)
  } else if (section === 'dist') {
    const source = clean(cells[3])
    const notes = clean(cells[4])
    metrics.push({
      metricId: col1,
      kind: 'distribution',
      generatedColumns: DIST_SUFFIXES.map((s) => `${col1}_${s}`),
      dataType: 'DOUBLE',
      source,
      notes,
      labelLike: /label|point-in-time/i.test(notes),
    })
  } else if (section === 'count') {
    const dataType = clean(cells[2]) || 'BIGINT'
    const source = clean(cells[3])
    const notes = clean(cells[4])
    metrics.push({
      metricId: col1,
      kind: 'count',
      generatedColumns: [col1],
      dataType,
      source,
      notes,
      labelLike: /label|point-in-time/i.test(notes),
    })
  }
}

TABLES[0].dimensionColumns = deviceDims
TABLES[1].dimensionColumns = nonDeviceDims

const catalog = { tables: TABLES, metrics }
fs.writeFileSync(OUT, JSON.stringify(catalog, null, 2) + '\n')
const dist = metrics.filter((m) => m.kind === 'distribution').length
const cnt = metrics.filter((m) => m.kind === 'count').length
console.log(
  `Wrote ${metrics.length} metrics (${dist} distribution + ${cnt} count), ` +
  `${deviceDims.length} device dims, ${nonDeviceDims.length} non-device dims to ${path.relative(process.cwd(), OUT)}`,
)
