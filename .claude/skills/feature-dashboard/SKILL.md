---
name: feature-dashboard
description: >-
  Generate the Signal Prism Feature Workbench dashboard — an interactive
  "Capability Map" where a user browses and *chooses* features. Every feature
  comes from the schema contracts in `schemas/*.md` (wide table, aggregation
  tables, GMinor log); the schemas are the single source of truth. The skill
  parses those markdown tables and emits one self-contained static HTML file
  (no build step, no CDN) into `components/Dashboard/`. It runs in two modes:
  STANDALONE (generate the HTML, open it, export a feature set) and ROUND — an
  interactive pipeline step that pre-loads the previous round's feature list
  from a shared folder, lets the user add/remove features, and writes the new
  feature_set back. Use whenever the user says "feature dashboard", "feature
  picker", "capability map", "feature chooser", "dashboard for choosing
  features", "/feature-dashboard", "build the feature workbench", "show the
  features from the schemas", "run a feature-selection round", "feature round",
  "pick features for this round", "特征看板", "特征选择面板", or asks to
  (re)generate / refresh the dashboard after the schemas change.
allowed-tools: Bash, Read, Edit, Glob, Grep
argument-hint: "[--schemas-dir DIR] [--out FILE]  |  round [--shared-dir DIR] [--port N]"
---

# Feature Dashboard — Capability Map & Feature Chooser

This skill turns the Signal Prism data contracts into an operator-facing
**Feature Workbench** so ML can pick features by browsing, not by reading SQL.
It realizes the Dashboard component's "Capability Map" (`components/Dashboard/README.md`):
show available columns grouped by domain, with profiling metadata, allowed
suitability/role, source lineage, and a selection tray that exports a feature set.

**The schemas are the source of truth.** Do not hand-author feature lists. Every
capability shown is parsed directly from the metadata tables in `schemas/*.md`
(see CLAUDE.md "How the docs interlock"). When the schemas change, re-run the
generator — the output is fully deterministic.

## What it produces

A single self-contained file: `components/Dashboard/feature_dashboard.html`.
It opens in any browser with no tooling (consistent with CLAUDE.md: this repo
has no build step). It contains:

- **Capability Map** — every schema column, grouped by schema → domain section,
  with type, suitability/role, semantic type, null semantics, and source.
- **Facet filters** — Suitability/Role, Schema, Source (Jaeger / HB / Derived),
  Semantic type. Counts update live.
- **Search** — across column name, description, source, semantic type, domain.
- **Selection tray** — tick features to build a feature set, then **Export JSON**
  (a `feature_set` draft) or **Copy column names**.
- **Legend** — explains the `feat`/`role` suitability codes from the schema.

## Workflow

### Step 1 — Locate the schemas

Default is the repo's `schemas/` directory. The three current contracts are the
wide table, the aggregation tables, and the GMinor log. If the user points at a
different directory, pass `--schemas-dir`.

### Step 2 — Generate

Run the generator from anywhere (paths resolve relative to the repo root):

```bash
python3 .claude/skills/feature-dashboard/build_dashboard.py
```

Optional overrides:

```bash
python3 .claude/skills/feature-dashboard/build_dashboard.py \
  --schemas-dir schemas \
  --out components/Dashboard/feature_dashboard.html
```

The script prints how many capabilities it parsed per schema file. Sanity-check
that the total is non-trivial (the current schemas yield ~350 columns). A sudden
drop usually means a schema table's header row changed.

### Step 3 — Report and open

Tell the user the output path and the per-schema counts. To open it:

```bash
open components/Dashboard/feature_dashboard.html   # macOS
```

If asked to visually verify, use the `gstack` skill to load
`file://<abs-path>/components/Dashboard/feature_dashboard.html` and screenshot.

The two steps above are **standalone mode**: generate → open → export. For the
interactive pipeline step, use round mode below.

## Round mode (interactive pipeline step)

Round mode makes the dashboard a step in the MLOps config-driven flow
(capability → aggregation → formula → **feature set** → simulation). It reads the
**previous round's** feature list, lets the user **add/remove** features, and
writes the **new** feature list back — all via files in a shared folder.

```bash
# First time only — seed the shared folder from the schemas:
#   round_000.json (empty genesis) + current_feature_set.json (the "normal"
#   directly-usable features, suitability == feature) for the UI to pre-load.
python3 .claude/skills/feature-dashboard/seed_feature_set.py

# Round N: reads the latest feature_set from the shared folder, serves the
# dashboard with it pre-selected, writes the new one back on Save.
python3 .claude/skills/feature-dashboard/serve_round.py
```

The first interactive Save commits as **round 0** → `round_000_feature_set.json`
(the seed is marked `round: -1`); each later round increments. Use
`--suitability feature,feature_after_encode` on the seed to widen the starter set.

Then open the printed `http://127.0.0.1:8765/`, adjust the selection (a live
**+added / −removed / unchanged** delta vs the baseline shows in the tray), and
click **Save new feature list**. By default the server **exits after the first
save** so the pipeline can proceed; pass `--no-exit` to keep it up for several
saves (stop with Ctrl-C).

A browser `file://` page can't write to disk, so round mode runs a tiny stdlib
`http.server` on **127.0.0.1 only**. Because that server serves the page *and*
receives the Save POST, the request is same-origin — **no CORS, no auth**.
The same `feature_dashboard.html` opened directly via `file://` stays the plain
export-only standalone tool (the round globals this server injects are absent
there); `serve_round.py` re-renders the catalog from the live schemas each run.

### CLI

| flag | default | meaning |
|---|---|---|
| `--shared-dir` | `components/Data/feature_sets/` | folder holding the feature_set files (created if missing) |
| `--schemas-dir` | `schemas/` | catalog source, passed to `build_dashboard` |
| `--port` | `8765` | loopback port (host is fixed to `127.0.0.1`) |
| `--id-stem` | `feature_set_candidate` | `feature_set_id` = `<stem>_round_<NNN>` |
| `--owner` | OS user | written into the feature_set |
| `--purpose` | `offline_floor_simulation` | written into the feature_set |
| `--from-file PATH` | — | serve a prebuilt HTML instead of re-rendering (stale-catalog fallback) |
| `--force` | off | allow overwriting an existing `round_<N>.json` snapshot |
| `--no-exit` | off | keep serving after a save |
| `--open` | off | open the page in a browser on startup |

Exit code: `0` after a save (or a Ctrl-C in `--no-exit`); `2` if stopped before
any save (so a pipeline can detect an abandoned round).

### Shared-folder contract (`components/Data/feature_sets/`)

The Data component owns "feature sets"; this folder is created at runtime.

| file | role |
|---|---|
| `round_000.json` | empty genesis baseline (round 0), written by the seed step. |
| `current_feature_set.json` | **rolling pointer** — the latest committed feature_set; the input for the next round and what downstream stages read. Created by the seed step, atomically replaced each save. |
| `round_<NNN>_feature_set.json` | immutable per-round snapshot (zero-padded, e.g. `round_000_feature_set.json`). Collision-guarded unless `--force`. |
| `CHANGELOG.md` | appended human log: per round — timestamp, round N, base id, counts, added/removed lists. |

Round numbering is automatic: the server reads `current_feature_set.json`, sets
`this_round = base.round + 1` (the seed's `round: -1` → round 0), sets
`base_feature_set` to the baseline's id, and rolls `current_feature_set.json`
forward on save. With no `current_feature_set.json` present at all it
**bootstraps Round 0** from an empty baseline (`base_feature_set: null`).

### Output shape (aligned to MLOps TRD §7.3.4 `feature_set`)

```jsonc
{
  "feature_set_id": "feature_set_candidate_round_000",
  "base_feature_set": "feature_set_candidate_seed",   // seed on round 0; prior round id thereafter
  "added_features":   ["...columns added this round..."],
  "removed_features": ["...columns removed this round..."],
  "owner": "...", "purpose": "...", "round": 0,
  "generated_from": "Signal Prism schemas",
  "schema_snapshot": "schemas: ...",
  "saved_at": "2026-..Z", "count": 37,
  "features": [ /* resolved list: column, schema, group, type, semantic_type,
                   suitability, null_semantics, source — same fields as Export JSON */ ]
}
```

`added_features` / `removed_features` are **column-name** lists (the TRD shape);
`features[]` is the fully-resolved list so the next stage consumes it directly.
The server is the source of truth for per-feature metadata: on Save it validates
every submitted column against the live catalog (unknown columns → `400`, nothing
written) and re-fills the metadata from the schema.

### Verify round mode end-to-end

```bash
python3 .claude/skills/feature-dashboard/seed_feature_set.py --force          # round_000.json + current
python3 .claude/skills/feature-dashboard/serve_round.py --no-exit --port 8771 &
curl -s http://127.0.0.1:8771/ | grep -c 'window.__ROUND__='        # -> 1
curl -s -X POST http://127.0.0.1:8771/save -H 'Content-Type: application/json' \
  -d '{"features":[{"column":"jgr_bid_floor"}]}'                     # -> {"ok":true,...round_000...}
ls components/Data/feature_sets/   # round_000.json, current_feature_set.json, round_000_feature_set.json, CHANGELOG.md
```

## How parsing works (so you can debug it)

`build_dashboard.py` scans each `schemas/*.md`, tracks the current `##`/`###`
heading as the domain group, and extracts every markdown table whose first
header cell is **`column`** or **`family`** (the genuine catalog tables). It
deliberately skips guidance tables (`Field …`, `Modulo column …`), enum
appendices (`Value …`), and the source/dedup tables, which would otherwise
inject duplicates or non-features.

For each row it captures `type`, `semantic_type`, `null`, `source`, the
description, and a unified **suitability** value:

- the schema's `feat` column when present (wide table: `feature`,
  `feature_after_encode`, `leak_risk`, `dim`, `key`, `exclude`, …),
- else the `role` column (GMinor / aggregation dims),
- else inferred from the section heading (`metric` / `dimension`),
- else `field`.

It also derives a `source_system` facet from the column prefix
(`jgr_` → Jaeger, `hbn_` → HB, otherwise Derived/Composite), matching the
source-prefix convention in CLAUDE.md.

The HTML injects the parsed catalog as JSON and builds all DOM with
`textContent`, so schema prose is never interpreted as markup.

## Editing the dashboard

- To change parsing (new metadata column, a new schema table shape), edit
  `build_dashboard.py` and re-run.
- To change the UI (styling, facets, export format), edit the `HTML_TEMPLATE`
  string in the same script. Keep it dependency-free and self-contained. The
  round-mode behavior (baseline pre-select, delta view, Save button) is dormant
  JS gated on `window.__ROUND__`, so standalone output is unaffected.
- Suitability labels/colors live in the `SUITABILITY` dict — extend it when a
  schema introduces a new `feat`/`role` value rather than leaving it unlabeled.
- Round mode lives in `serve_round.py` (the loopback server + `/save` write-back).
  It imports `build_dashboard` and reuses `build_catalog` / `render_html`, then
  injects `window.__ROUND__` + `window.__PRESELECTED__` before serving. The
  feature_set assembly + delta logic is in `build_feature_set` / `save_round`.
