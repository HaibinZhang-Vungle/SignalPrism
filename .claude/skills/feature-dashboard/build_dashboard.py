#!/usr/bin/env python3
"""
Build the Signal Prism Feature Workbench dashboard from the schema markdown.

The schemas in `schemas/*.md` are the single source of truth. Every feature the
dashboard offers comes from a metadata-bearing markdown table in those files
(see CLAUDE.md "How the docs interlock"). This script parses those tables and
emits ONE self-contained static HTML file (no build step, no CDN, opens in any
browser) that presents the Capability Map and lets a user *choose* features into
a feature set and export the selection.

Re-run this whenever the schemas change. It is fully deterministic.

Usage:
    python3 build_dashboard.py [--schemas-dir DIR] [--out FILE]

Defaults are resolved relative to the repo root (three levels above this file),
so it can be run from anywhere.
"""

from __future__ import annotations

import argparse
import html
import json
import os
import re
import sys
from pathlib import Path

# Repo root = .../<repo>/.claude/skills/feature-dashboard/build_dashboard.py
REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_SCHEMAS = REPO_ROOT / "schemas"
DEFAULT_OUT = REPO_ROOT / "components" / "Dashboard" / "feature_dashboard.html"

# Human labels for the schema files we know about.
SCHEMA_TITLES = {
    "realtime_attributed_wide_table_schema": "Wide Table (impression grain)",
    "realtime_attributed_aggregation_table_schema": "Aggregation Tables (hourly)",
    "gminor_log_schema": "GMinor Prediction Log",
}

# `feat` / `role` values, ordered + described for the legend and facet.
SUITABILITY = {
    "feature": ("Feature", "Directly usable as an ML feature."),
    "feature_after_encode": ("Feature (encode)", "Needs bucketing / encoding first."),
    "leak_risk": ("Leak risk", "Label-adjacent; only with point-in-time care."),
    "dim": ("Dimension", "Grouping / dimension key, not a raw feature."),
    "dimension": ("Dimension", "Aggregation dimension."),
    "privacy_dimension": ("Privacy dim", "Regulatory cohort dimension."),
    "risk_dimension": ("Risk dim", "Fraud / risk cohort dimension."),
    "optional_dimension": ("Optional dim", "Coarse optional dimension."),
    "key": ("Key", "Join / identity key, not a feature."),
    "join_key": ("Join key", "Event-grain join key."),
    "exclude": ("Exclude", "PII / deprecated / operational — not a feature."),
    "metric": ("Metric", "Aggregated metric column."),
    "surrogate_key": ("Surrogate key", "Derived stable key."),
    "primary_dimension": ("Primary dim", "Primary dimension key of the family."),
    "time_key": ("Time key", "Time bucket key."),
    "partition_key": ("Partition key", "Storage partition key."),
    "audit_metric": ("Audit", "Audit / bookkeeping metric."),
    "audit": ("Audit", "Audit / bookkeeping field."),
    "lineage": ("Lineage", "Lineage / versioning field."),
    "event_time": ("Event time", "Canonical event timestamp."),
}


def clean_cell(text: str) -> str:
    """Normalize a markdown table cell to plain text."""
    t = text.strip()
    # Markdown links [label](url) -> label
    t = re.sub(r"\[([^\]]+)\]\([^)]*\)", r"\1", t)
    # Escaped angle brackets used in types e.g. ARRAY\<STRING\>
    t = t.replace(r"\<", "<").replace(r"\>", ">")
    # Strip inline code backticks and bold/italic markers.
    t = t.replace("`", "").replace("**", "")
    t = re.sub(r"\s+", " ", t).strip()
    return t


def is_separator_row(cells: list[str]) -> bool:
    return bool(cells) and all(re.fullmatch(r":?-{2,}:?", c.strip()) for c in cells)


def split_row(line: str) -> list[str]:
    line = line.strip()
    if line.startswith("|"):
        line = line[1:]
    if line.endswith("|"):
        line = line[:-1]
    return [c.strip() for c in line.split("|")]


def source_system(name: str) -> str:
    if name.startswith("jgr_"):
        return "Jaeger"
    if name.startswith("hbn_"):
        return "HB"
    return "Derived / Composite"


def parse_schema_file(path: Path) -> list[dict]:
    """Extract every column-bearing table row, tagged with its section heading."""
    stem = path.stem
    schema_title = SCHEMA_TITLES.get(stem, stem.replace("_", " ").title())
    lines = path.read_text(encoding="utf-8").splitlines()

    rows: list[dict] = []
    section = ""        # nearest ## heading
    subsection = ""     # nearest ### (or deeper) heading
    i = 0
    n = len(lines)
    while i < n:
        line = lines[i]
        m = re.match(r"^(#{2,6})\s+(.*)$", line.strip())
        if m:
            level = len(m.group(1))
            text = clean_cell(m.group(2))
            # Drop a leading numbering like "5.3" / "7."
            text = re.sub(r"^[\d.]+\s+", "", text)
            if level == 2:
                section = text
                subsection = ""
            else:
                subsection = text
            i += 1
            continue

        # Table header?
        if line.lstrip().startswith("|") and i + 1 < n and \
                is_separator_row(split_row(lines[i + 1])):
            headers = [clean_cell(h).lower() for h in split_row(line)]
            first = headers[0] if headers else ""
            # Only genuine catalog tables: keyed by a column or metric-family name.
            # Deliberately excludes guidance tables ("Field …", "Modulo column …"),
            # enum appendices ("Value …"), and source/dedup tables.
            keyed = first in ("column", "family")
            if not keyed:
                # skip this table entirely
                i += 2
                while i < n and lines[i].lstrip().startswith("|"):
                    i += 1
                continue

            group = subsection or section
            i += 2  # move past header + separator
            while i < n and lines[i].lstrip().startswith("|"):
                cells = [clean_cell(c) for c in split_row(lines[i])]
                i += 1
                if not cells or not cells[0]:
                    continue
                record = {headers[k]: cells[k] for k in range(min(len(headers), len(cells)))}
                name = record.get(first, "").strip()
                if not name:
                    continue
                # Unify the feature/role/metric notion into `suitability`.
                grp_l = (subsection or section).lower()
                if "feat" in record:
                    suit = record["feat"]
                elif "role" in record:
                    suit = record["role"]
                elif first == "family":
                    suit = "metric"
                elif "metric" in grp_l:
                    suit = "metric"
                elif "dimension" in grp_l:
                    suit = "dimension"
                else:
                    suit = "field"
                rows.append({
                    "schema": schema_title,
                    "schema_id": stem,
                    "group": group or section or "General",
                    "section": section,
                    "name": name,
                    "type": record.get("type", ""),
                    "semantic_type": record.get("semantic_type", ""),
                    "suitability": suit.strip() or "field",
                    "null": record.get("null", ""),
                    "source": record.get("source", "") or record.get("source / derivation", ""),
                    "source_system": source_system(name),
                    "desc": (record.get("description", "") or record.get("notes", "")
                             or record.get("meaning", "")),
                    "raw": record,
                })
            continue
        i += 1
    return rows


def build_catalog(schemas_dir: Path) -> dict:
    files = sorted(schemas_dir.glob("*.md"))
    if not files:
        raise SystemExit(f"No schema markdown found in {schemas_dir}")
    all_rows: list[dict] = []
    per_file: dict[str, int] = {}
    for f in files:
        rows = parse_schema_file(f)
        per_file[f.name] = len(rows)
        all_rows.extend(rows)
    return {"rows": all_rows, "per_file": per_file, "files": [f.name for f in files]}


# ---------------------------------------------------------------------------
# HTML rendering. The catalog is injected as JSON; all UI logic is vanilla JS
# building DOM via textContent, so schema text is never interpreted as HTML.
# ---------------------------------------------------------------------------

HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Signal Prism — Feature Workbench</title>
<style>
  :root {
    --bg:#0f1420; --panel:#171d2b; --panel2:#1e2536; --line:#2b3346;
    --text:#e6ebf5; --muted:#8a96ad; --accent:#5b9dff; --accent2:#7ee0c0;
    --warn:#ffb454; --danger:#ff6b81; --chip:#283044;
  }
  * { box-sizing:border-box; }
  body { margin:0; font:14px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;
         background:var(--bg); color:var(--text); }
  header { padding:16px 22px; border-bottom:1px solid var(--line); background:var(--panel);
           display:flex; align-items:center; gap:18px; flex-wrap:wrap; position:sticky; top:0; z-index:5; }
  header h1 { font-size:17px; margin:0; font-weight:600; }
  header .sub { color:var(--muted); font-size:12px; }
  .counts { color:var(--muted); font-size:12px; margin-left:auto; }
  .counts b { color:var(--accent2); }
  .wrap { display:flex; align-items:flex-start; }
  aside { width:268px; flex:0 0 268px; padding:18px; border-right:1px solid var(--line);
          position:sticky; top:61px; height:calc(100vh - 61px); overflow:auto; background:var(--panel); }
  aside h3 { font-size:11px; text-transform:uppercase; letter-spacing:.08em; color:var(--muted);
             margin:18px 0 8px; }
  aside h3:first-child { margin-top:0; }
  .facet { display:flex; align-items:center; gap:8px; padding:3px 0; cursor:pointer; color:var(--text); }
  .facet input { accent-color:var(--accent); }
  .facet .n { margin-left:auto; color:var(--muted); font-size:11px; }
  main { flex:1; padding:18px 22px; min-width:0; }
  .toolbar { display:flex; gap:10px; align-items:center; margin-bottom:16px; flex-wrap:wrap; }
  input[type=search] { flex:1; min-width:220px; padding:9px 12px; border-radius:8px;
       border:1px solid var(--line); background:var(--panel2); color:var(--text); font-size:14px; }
  button { cursor:pointer; border:1px solid var(--line); background:var(--panel2); color:var(--text);
           padding:8px 12px; border-radius:8px; font-size:13px; }
  button:hover { border-color:var(--accent); }
  button.primary { background:var(--accent); border-color:var(--accent); color:#0a0f1a; font-weight:600; }
  .group { margin-bottom:8px; border:1px solid var(--line); border-radius:10px; overflow:hidden;
           background:var(--panel); }
  .group > summary { padding:11px 14px; cursor:pointer; font-weight:600; list-style:none;
           display:flex; align-items:center; gap:10px; background:var(--panel2); }
  .group > summary::-webkit-details-marker { display:none; }
  .group .schema-tag { font-size:11px; color:var(--muted); font-weight:400; }
  .group .gcount { margin-left:auto; font-size:11px; color:var(--muted); }
  table { width:100%; border-collapse:collapse; }
  th, td { text-align:left; padding:8px 12px; border-top:1px solid var(--line); vertical-align:top; }
  th { font-size:11px; text-transform:uppercase; letter-spacing:.05em; color:var(--muted); font-weight:600; }
  td.col-name { font-family:ui-monospace,SFMono-Regular,Menlo,monospace; color:var(--accent2);
                white-space:nowrap; }
  td .desc { color:var(--muted); font-size:12.5px; }
  td .meta { color:var(--muted); font-size:11px; margin-top:2px; }
  .pill { display:inline-block; padding:1px 7px; border-radius:10px; font-size:11px;
          background:var(--chip); color:var(--text); white-space:nowrap; }
  .pill.feature { background:#16402f; color:var(--accent2); }
  .pill.feature_after_encode { background:#14303f; color:#7ec8ff; }
  .pill.leak_risk { background:#3f2a14; color:var(--warn); }
  .pill.exclude { background:#3f1c24; color:var(--danger); }
  .pill.key, .pill.join_key { background:#2a2440; color:#c3a6ff; }
  .pill.metric { background:#1c3340; color:#7ee0e0; }
  tr.row { }
  tr.row:hover td { background:var(--panel2); }
  tr.row.picked td { background:#13243a; }
  .pick { accent-color:var(--accent2); transform:scale(1.15); }
  .empty { color:var(--muted); padding:30px; text-align:center; }
  /* selection tray */
  .tray { position:fixed; right:0; top:61px; width:340px; height:calc(100vh - 61px);
          background:var(--panel); border-left:1px solid var(--line); transform:translateX(100%);
          transition:transform .18s ease; display:flex; flex-direction:column; z-index:6; }
  .tray.open { transform:translateX(0); }
  .tray header { position:static; border-bottom:1px solid var(--line); }
  .tray .body { flex:1; overflow:auto; padding:12px 16px; }
  .tray .selitem { display:flex; gap:8px; align-items:center; padding:6px 0; border-bottom:1px solid var(--line); }
  .tray .selitem code { color:var(--accent2); font-size:12px; word-break:break-all; }
  .tray .selitem button { padding:1px 7px; margin-left:auto; }
  .tray .foot { padding:12px 16px; border-top:1px solid var(--line); display:flex; gap:8px; flex-wrap:wrap; }
  .legend { display:flex; gap:8px; flex-wrap:wrap; margin-bottom:14px; }
  .legend .pill { cursor:default; }
  textarea { width:100%; height:120px; background:var(--panel2); color:var(--text);
             border:1px solid var(--line); border-radius:8px; font:12px ui-monospace,monospace; padding:8px; }
  a.reset { color:var(--accent); cursor:pointer; font-size:12px; }
  /* round mode (dormant unless a server injects round globals) */
  .round-banner { padding:8px 22px; background:#13243a; border-bottom:1px solid var(--line);
                  font-size:12.5px; color:var(--text); }
  .round-banner b { color:var(--accent2); }
  .round-warn { padding:8px 22px; background:#3a2414; border-bottom:1px solid var(--line);
                color:var(--warn); font-size:12.5px; }
  .delta { display:flex; gap:14px; padding:8px 0 10px; border-bottom:1px solid var(--line);
           margin-bottom:10px; font-size:12.5px; }
  .delta .added { color:var(--accent2); } .delta .removed { color:var(--danger); }
  .delta .unchanged { color:var(--muted); }
  .delta-lists { font-size:11.5px; color:var(--muted); padding-bottom:10px;
                 border-bottom:1px solid var(--line); margin-bottom:10px; word-break:break-all; }
  button.save { background:var(--accent2); border-color:var(--accent2); color:#06231a; font-weight:600; }
</style>
</head>
<body>
<header>
  <div>
    <h1>Signal Prism · Feature Workbench</h1>
    <div class="sub">Capability Map — choose features from the schema contracts</div>
  </div>
  <div class="counts" id="counts"></div>
  <button class="primary" id="trayBtn">🛒 Selection (<span id="selCount">0</span>)</button>
</header>

<div class="wrap">
  <aside id="facets"></aside>
  <main>
    <div class="toolbar">
      <input type="search" id="search" placeholder="Search column name, description, source, semantic type…">
      <button id="expandAll">Expand all</button>
      <button id="collapseAll">Collapse all</button>
      <a class="reset" id="clearFilters">Clear filters</a>
    </div>
    <div class="legend" id="legend"></div>
    <div id="results"></div>
  </main>
</div>

<div class="tray" id="tray">
  <header><div><h1 style="font-size:15px">Feature Set</h1>
    <div class="sub">Selected capabilities to export</div></div></header>
  <div class="body" id="trayBody"></div>
  <div class="foot">
    <button id="exportJson" class="primary">Export JSON</button>
    <button id="copyNames">Copy column names</button>
    <button id="clearSel">Clear</button>
    <textarea id="exportArea" placeholder="Export output appears here…" readonly style="display:none"></textarea>
  </div>
</div>

<script>
const CATALOG = __CATALOG__;
const SUIT = __SUIT__;
const GENERATED_AT = "__GENERATED__";

// Round globals are injected ONLY by serve_round.py. Absent on a plain file:// open,
// so everything below stays dormant and standalone behavior is unchanged.
const ROUND = window.__ROUND__ || null;            // {round, base_feature_set_id, shared_dir, save_url}
const PRESELECTED = window.__PRESELECTED__ || null; // baseline features[] from the previous round
const SERVER_MODE = !!(ROUND && ROUND.save_url);

const state = {
  search: "",
  filters: { suitability:new Set(), schema:new Set(), source_system:new Set(), semantic_type:new Set() },
  selected: new Map(),  // id -> row
  baseline: new Set(),       // baseline column names (round mode)
  missingBaseline: [],       // baseline columns no longer in the catalog (schema drift)
};

const FACET_DEFS = [
  ["suitability", "Suitability / Role"],
  ["schema", "Schema"],
  ["source_system", "Source"],
  ["semantic_type", "Semantic type"],
];

function rowId(r){ return r.schema_id + "::" + r.name; }
function suitLabel(s){ return (SUIT[s] && SUIT[s][0]) || s; }

function passesFilters(r){
  for (const [key] of FACET_DEFS){
    const sel = state.filters[key];
    if (sel.size && !sel.has(r[key] || "")) return false;
  }
  if (state.search){
    const q = state.search.toLowerCase();
    const hay = [r.name, r.desc, r.source, r.semantic_type, r.group, r.type].join(" ").toLowerCase();
    if (!hay.includes(q)) return false;
  }
  return true;
}

function buildFacets(){
  const aside = document.getElementById("facets");
  aside.innerHTML = "";
  for (const [key, label] of FACET_DEFS){
    const counts = {};
    for (const r of CATALOG.rows){ const v = r[key] || "(none)"; counts[v] = (counts[v]||0)+1; }
    const h = document.createElement("h3"); h.textContent = label; aside.appendChild(h);
    Object.keys(counts).sort((a,b)=>counts[b]-counts[a]).forEach(v=>{
      const lab = document.createElement("label"); lab.className = "facet";
      const cb = document.createElement("input"); cb.type="checkbox";
      cb.checked = state.filters[key].has(v);
      cb.onchange = ()=>{ cb.checked ? state.filters[key].add(v) : state.filters[key].delete(v); render(); };
      const span = document.createElement("span");
      span.textContent = key==="suitability" ? suitLabel(v) : v;
      const n = document.createElement("span"); n.className="n"; n.textContent = counts[v];
      lab.append(cb, span, n); aside.appendChild(lab);
    });
  }
}

function buildLegend(){
  const el = document.getElementById("legend");
  el.innerHTML = "";
  const seen = new Set(CATALOG.rows.map(r=>r.suitability));
  Object.keys(SUIT).filter(k=>seen.has(k)).forEach(k=>{
    const p = document.createElement("span"); p.className = "pill " + k;
    p.textContent = SUIT[k][0]; p.title = SUIT[k][1]; el.appendChild(p);
  });
}

function render(){
  const rows = CATALOG.rows.filter(passesFilters);
  // group by schema + group
  const groups = new Map();
  for (const r of rows){
    const k = r.schema + " ▸ " + r.group;
    if (!groups.has(k)) groups.set(k, {schema:r.schema, group:r.group, rows:[]});
    groups.get(k).rows.push(r);
  }
  const container = document.getElementById("results");
  container.innerHTML = "";
  if (!rows.length){
    container.innerHTML = '<div class="empty">No capabilities match the current filters.</div>';
  }
  for (const g of groups.values()){
    const det = document.createElement("details"); det.className="group"; det.open = true;
    const sum = document.createElement("summary");
    const title = document.createElement("span"); title.textContent = g.group;
    const tag = document.createElement("span"); tag.className="schema-tag"; tag.textContent = g.schema;
    const cnt = document.createElement("span"); cnt.className="gcount"; cnt.textContent = g.rows.length + " cols";
    sum.append(title, tag, cnt); det.appendChild(sum);

    const tbl = document.createElement("table");
    tbl.innerHTML = "<thead><tr><th></th><th>Column</th><th>Type</th><th>Suitability</th><th>Details</th></tr></thead>";
    const tb = document.createElement("tbody");
    for (const r of g.rows){
      const id = rowId(r);
      const tr = document.createElement("tr"); tr.className = "row" + (state.selected.has(id)?" picked":"");
      const tdC = document.createElement("td");
      const cb = document.createElement("input"); cb.type="checkbox"; cb.className="pick";
      cb.checked = state.selected.has(id);
      cb.onchange = ()=>{ cb.checked ? state.selected.set(id,r) : state.selected.delete(id); syncSel(); render(); };
      tdC.appendChild(cb);
      const tdN = document.createElement("td"); tdN.className="col-name"; tdN.textContent = r.name;
      const tdT = document.createElement("td"); tdT.innerHTML = '<span class="pill">'+escapeHtml(r.type||"—")+'</span>';
      const tdS = document.createElement("td");
      const p = document.createElement("span"); p.className="pill "+r.suitability;
      p.textContent = suitLabel(r.suitability); p.title = (SUIT[r.suitability]&&SUIT[r.suitability][1])||"";
      tdS.appendChild(p);
      const tdD = document.createElement("td");
      const d = document.createElement("div"); d.className="desc"; d.textContent = r.desc || "";
      tdD.appendChild(d);
      const meta = [];
      if (r.semantic_type) meta.push("semantic: "+r.semantic_type);
      if (r.null) meta.push("null: "+r.null);
      if (r.source) meta.push("src: "+r.source);
      if (meta.length){ const m=document.createElement("div"); m.className="meta"; m.textContent = meta.join("  ·  "); tdD.appendChild(m); }
      tr.append(tdC, tdN, tdT, tdS, tdD); tb.appendChild(tr);
    }
    tbl.appendChild(tb); det.appendChild(tbl); container.appendChild(det);
  }
  document.getElementById("counts").innerHTML =
    "Showing <b>"+rows.length+"</b> of "+CATALOG.rows.length+" capabilities · "+groups.size+" groups";
}

function escapeHtml(s){ const d=document.createElement("div"); d.textContent=s; return d.innerHTML; }

function syncSel(){
  document.getElementById("selCount").textContent = state.selected.size;
  const body = document.getElementById("trayBody");
  body.innerHTML = "";
  if (SERVER_MODE) body.appendChild(deltaBlock());
  if (!state.selected.size){
    const e = document.createElement("div"); e.className="empty";
    e.innerHTML = 'No features selected yet.<br>Tick the checkboxes to build a feature set.';
    body.appendChild(e); return;
  }
  for (const [id, r] of state.selected){
    const div = document.createElement("div"); div.className="selitem";
    const c = document.createElement("code"); c.textContent = r.name;
    const rm = document.createElement("button"); rm.textContent="✕";
    rm.onclick = ()=>{ state.selected.delete(id); syncSel(); render(); };
    div.append(c, rm); body.appendChild(div);
  }
}

// --- Round mode (dormant unless serve_round.py injected round globals) ---

function seedBaseline(){
  if (!PRESELECTED) return;
  const byId = new Map(CATALOG.rows.map(r=>[rowId(r), r]));
  const byName = new Map();
  for (const r of CATALOG.rows){ if (!byName.has(r.name)) byName.set(r.name, r); }
  for (const f of PRESELECTED){
    const col = f && f.column; if (!col) continue;
    state.baseline.add(col);
    let row = null;
    if (f.schema && byId.has(f.schema + "::" + col)) row = byId.get(f.schema + "::" + col);
    else if (byName.has(col)) row = byName.get(col);
    if (row) state.selected.set(rowId(row), row);
    else state.missingBaseline.push(col);
  }
}

function deltaBlock(){
  const selCols = new Set([...state.selected.values()].map(r=>r.name));
  const added = [...selCols].filter(c=>!state.baseline.has(c));
  const removed = [...state.baseline].filter(c=>!selCols.has(c));
  const unchanged = [...selCols].filter(c=>state.baseline.has(c));
  const wrap = document.createElement("div");
  const row = document.createElement("div"); row.className="delta";
  const a = document.createElement("span"); a.className="added"; a.textContent = "+"+added.length+" added";
  const r = document.createElement("span"); r.className="removed"; r.textContent = "−"+removed.length+" removed";
  const u = document.createElement("span"); u.className="unchanged"; u.textContent = unchanged.length+" unchanged";
  row.append(a, r, u); wrap.appendChild(row);
  if (added.length || removed.length){
    const dl = document.createElement("div"); dl.className="delta-lists";
    if (added.length){ const d=document.createElement("div"); d.textContent="added: "+added.join(", "); dl.appendChild(d); }
    if (removed.length){ const d=document.createElement("div"); d.textContent="removed: "+removed.join(", "); dl.appendChild(d); }
    wrap.appendChild(dl);
  }
  return wrap;
}

function buildRoundUI(){
  if (!ROUND) return;
  const wrap = document.querySelector(".wrap");
  const banner = document.createElement("div"); banner.className="round-banner";
  banner.innerHTML = "Round <b>"+escapeHtml(String(ROUND.round))+"</b> · base: <b>"
    + escapeHtml(ROUND.base_feature_set_id || "(none)") + "</b> · shared dir: "
    + escapeHtml(ROUND.shared_dir || "");
  wrap.before(banner);
  if (state.missingBaseline.length){
    const w = document.createElement("div"); w.className="round-warn";
    w.textContent = state.missingBaseline.length + " baseline feature(s) no longer exist in the schemas "
      + "and were dropped: " + state.missingBaseline.join(", ");
    wrap.before(w);
  }
  if (SERVER_MODE){
    const foot = document.querySelector(".tray .foot");
    const save = document.createElement("button"); save.id="saveRound"; save.className="save primary";
    save.textContent = "Save new feature list";
    foot.insertBefore(save, foot.firstChild);
    const status = document.createElement("div"); status.id="roundStatus";
    status.style.cssText = "width:100%;font-size:12px;color:var(--muted);margin-top:6px;";
    foot.appendChild(status);
    save.onclick = saveRound;
    const sub = document.querySelector(".tray .sub");
    if (sub) sub.textContent = "Add / remove vs the previous round, then save";
  }
}

async function saveRound(){
  const btn = document.getElementById("saveRound");
  const status = document.getElementById("roundStatus");
  btn.disabled = true; status.style.color = "var(--muted)"; status.textContent = "Saving…";
  const payload = {
    features: exportPayload().features,
    round: ROUND.round,
    base_feature_set: ROUND.base_feature_set_id,
  };
  try {
    const res = await fetch(ROUND.save_url, {
      method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify(payload),
    });
    const j = await res.json();
    if (res.ok && j.ok){
      status.style.color = "var(--accent2)";
      status.textContent = "Saved " + j.feature_set_id + " · +" + ((j.added||[]).length)
        + " / −" + ((j.removed||[]).length) + " · wrote " + ((j.written||[]).length)
        + " file(s). You may close this tab.";
    } else {
      status.style.color = "var(--danger)";
      status.textContent = "Save failed: " + (j.error || res.status)
        + (j.details ? (" — " + JSON.stringify(j.details)) : "");
    }
  } catch(e){
    status.style.color = "var(--danger)"; status.textContent = "Save failed: " + e;
  }
  btn.disabled = false;
}

function exportPayload(){
  return {
    feature_set: "feature_set_draft",
    generated_from: "Signal Prism schemas",
    schema_snapshot: GENERATED_AT,
    count: state.selected.size,
    features: [...state.selected.values()].map(r=>({
      column:r.name, schema:r.schema_id, group:r.group, type:r.type,
      semantic_type:r.semantic_type, suitability:r.suitability,
      null_semantics:r.null, source:r.source,
    })),
  };
}

// wiring
document.getElementById("search").oninput = e=>{ state.search=e.target.value; render(); };
document.getElementById("expandAll").onclick = ()=>document.querySelectorAll("details.group").forEach(d=>d.open=true);
document.getElementById("collapseAll").onclick = ()=>document.querySelectorAll("details.group").forEach(d=>d.open=false);
document.getElementById("clearFilters").onclick = ()=>{ for(const k in state.filters) state.filters[k].clear(); state.search=""; document.getElementById("search").value=""; buildFacets(); render(); };
document.getElementById("trayBtn").onclick = ()=>document.getElementById("tray").classList.toggle("open");
document.getElementById("clearSel").onclick = ()=>{ state.selected.clear(); syncSel(); render(); };
document.getElementById("exportJson").onclick = ()=>{
  const ta=document.getElementById("exportArea"); ta.style.display="block";
  ta.value = JSON.stringify(exportPayload(), null, 2); ta.select();
};
document.getElementById("copyNames").onclick = ()=>{
  const names=[...state.selected.values()].map(r=>r.name).join("\n");
  const ta=document.getElementById("exportArea"); ta.style.display="block"; ta.value=names; ta.select();
  navigator.clipboard && navigator.clipboard.writeText(names);
};

seedBaseline(); buildFacets(); buildLegend(); buildRoundUI(); syncSel(); render();
</script>
</body>
</html>
"""


def render_html(catalog: dict, generated_at: str) -> str:
    return (HTML_TEMPLATE
            .replace("__CATALOG__", json.dumps(catalog, ensure_ascii=False))
            .replace("__SUIT__", json.dumps(SUITABILITY, ensure_ascii=False))
            .replace("__GENERATED__", html.escape(generated_at)))


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description="Build the Feature Workbench dashboard from schemas.")
    ap.add_argument("--schemas-dir", default=str(DEFAULT_SCHEMAS))
    ap.add_argument("--out", default=str(DEFAULT_OUT))
    args = ap.parse_args(argv)

    schemas_dir = Path(args.schemas_dir).resolve()
    out = Path(args.out).resolve()
    catalog = build_catalog(schemas_dir)

    # Deterministic snapshot label: schema file list, no wall-clock.
    snapshot = "schemas: " + ", ".join(catalog["files"])
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(render_html(catalog, snapshot), encoding="utf-8")

    print(f"Wrote {out}")
    print(f"Parsed {len(catalog['rows'])} capabilities from {len(catalog['files'])} schema file(s):")
    for fname, n in catalog["per_file"].items():
        print(f"  {fname}: {n} columns")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
