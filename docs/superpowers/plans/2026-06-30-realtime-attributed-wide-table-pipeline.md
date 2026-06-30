# Realtime Attributed Wide Table Pipeline — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a lena-style 3-job Spark pipeline (under `components/Data/`) that materializes `ml_shadow.realtime_attributed_event_wide` from the `ex-jaeger-transaction` and `hb-transactions` Kafka topics, per `schemas/realtime_attributed_wide_table_schema.md`.

**Architecture:** Two coba2→Iceberg ingestion jobs consume the existing coba2 raw landing tables `coba2.ex_jaeger_transaction` (exploded on `placement_serve_results[]`/`placements[]`/winning `rtbconnections[]`) and `coba2.hb_transactions` (served/winning bid), each applying the same deterministic event-id hash sample, then a join job `LEFT JOIN`s the two staging tables on `(event_id, imp_id)` into the content-only wide table. The 150-column resource artifacts (columns JSON, col_maps, DDL) are generated from the schema md by a Python codegen tool; the schema md is the single source of truth.

**Tech Stack:** Scala 2.12 / Spark 3.5 (`object SparkMain extends BoilerplateSparkMain`, lena conventions); Python 3 + pytest for codegen and validation.

## Global Constraints

- **Authoritative contract:** `schemas/realtime_attributed_wide_table_schema.md`. Every output column name, physical type, and source expression comes from its field tables (the tables whose header is `| column | type | source | semantic_type | null | feat | description |`). Never invent columns.
- **Content-only:** emit exactly the schema-md columns. Do NOT add the §7.5 realtime operational columns (`lookup_status`, `attribution_store_layer`, `source_cluster`, `lookup_cluster`, `attribution_delay_seconds`, `context_schema_version`).
- **Column→staging assignment is by `source` identifier, NOT by name prefix.** `source` starting `hb.` → hb staging; starting `jaeger.` or `derived:` → jaeger staging. Known anomaly to honor: `hbn_mediation_tmax` has source `jaeger.mediation_tmax` → it is produced by the **jaeger** job despite its `hbn_` name.
- **Dedup decisions (schema §3.1):** HB copies of floors/device/geo/shading/`supply_traffic_source`/`req_no`/`exp_to_bucket` are dropped — they are simply never listed; jaeger wins. `datasci_tags` keeps the HB copy (`hbn_datasci_tags`).
- **Sampling:** both ingestion jobs gate on `in_user_sample(sha1(event_id), <sample_rate>)`, identical rate, so both sides cover the same event-id cohort (schema §2.3, TRD §7.8). Default rate `0.0001`.
- **Source = coba2 raw landing tables, NOT Kafka.** `coba/ingestion2` is the existing upstream Kafka→S3 lander (out of scope) that produces `coba2.ex_jaeger_transaction` and `coba2.hb_transactions` with the full nested payload. Our ingestion jobs consume that coba2 output via `withCoba2TempViewInRange(S3base, topic, start, till, tempView, storeS3IngestTime = true)`. Do NOT write a Kafka consumer (`saveKafkaTopic`) and do NOT read the slim domain tables (e.g. `edsp_deliveries`). Verified live (2026-06-30): catalog `raw`, schema `coba2`, partitions `dt,hr,mn` (no `az`), both fresh; HB ≈ 487M rows/min (~700B/day) → sampling mandatory.
- **Field existence: the landed coba2 parquet is truth; BOTH the coba YAMLs and the Trino metastore are INCOMPLETE declarations of it.** Verified examples: the Trino Hive tables omit `edsp_floor`/`direct_floor`/`acc_floor`/`bid_dsp_size`/`vxac_exp_id` (jaeger) and `bidrequest_imp_id` (HB) that ARE in the YAML + parquet; conversely the coba YAMLs omit `incoming_bid_request_id`, `dup_key`, `double_verify_fraud_reason`, `device.geo.ipservice`, `serve_result.pd_cl`/`pd_cpx`/`ad_podding_multiplier` (jaeger) and `bidrequest_time` (HB) that ARE in Trino + parquet. The Spark job reads parquet via schema-on-read, so it sees the union. Therefore: do NOT delete a col_map field just because it's missing from Trino OR from the YAML — cross-check both, and ultimately the live coba2 parquet. The schema-md join key `h.bidrequest_imp_id` is valid (HB YAML line 63).
  - `check_against_coba_yaml()` (Task 6) is therefore **advisory only** (prints a heads-up, exit 0) — the YAML being incomplete means it produces false positives. The hard gate is `validate()` (artifacts vs schema md).
- **Confirmed boilerplate helper:** `getColsMapInJson("col_maps/<file>.json")` returns an ordered map of `source-expr -> target`; build the SELECT via `.map{ case (k,v) => s"$k AS $v" }.mkString(",\n")`. (Confirmed in `edsp/deliveries_ingestion`.) Column-name-only lists use `getColsInJson(...)`.
- **Transforms via `UDFUtil` only.** No ad-hoc UDFs. Reference: `/Users/twang/Projects/lena/src/main/scala/com/vungle/lena/UDFUtil.scala`.
- **lena reference files** (read before writing Scala): **`edsp/deliveries_ingestion/SparkMain.scala` (THE template for Job 1 — reads `coba2.ex_jaeger_transaction`, multi-stage explode of serve_result/placements/rtbconnections, winning-RTB filter, `getColsMapInJson`)**, `hbp/auctions_served_ingestion/SparkMain.scala` (Job 2 template — reads `coba2.hb_transactions`, `row_number` dedup, merge), `auction/notifications_attribution/SparkMain.scala` (Job 3 template — two-upstream watermark + LEFT JOIN), `src/main/resources/lena/col_maps/edsp_deliveries.json` (col_map format), `src/main/resources/lena/sql/auction/auction_notifications_enriched.template` (DDL format).
- **Winning-RTB selection (Job 1):** after `explode(serve_result.rtbconnections) AS rtb_conn`, filter `serve_result.winner_id = rtb_conn.id`. `jgr_rtb_*` and `jgr_winner_account_id` map from `rtb_conn.*`. Do NOT add edsp's `rtb_conn.is_internal = FALSE` filter — `jgr_rtb_is_internal` is a wanted column.
- **`jgr_winner_account_id` special-case:** it shares the source expression `rtb_conn.account_id` with `jgr_rtb_account_id`. Because the col_map is a `{source_expr: target}` dict (one value per key), `jgr_winner_account_id` is EXCLUDED from the col_map (like `event_id`/`imp_id`) and emitted directly in the jaeger SELECT as `rtb_conn.account_id AS jgr_winner_account_id`. The Task 6 validator counts it as produced-directly. This is the only such collision (verified).
- **Build decoupling:** `components/Data/` is NOT part of an SBT build in this repo. There is no `sbt compile`/`sbt test`. Verification = the Python catalog validator + structural-lint checks defined in each task. These files are designed to drop into lena later (then real compilation applies).
- **Package base:** `com.vungle.signalprism.data` (placeholder). **Arg namespace:** `spark.app.signalprism.data.<job>.*`.
- **Staging tables:** `ml_shadow.jaeger_transaction_wide_staging`, `ml_shadow.hb_transactions_wide_staging`. **Output:** `ml_shadow.realtime_attributed_event_wide`.
- **Commits:** this repo's default branch is `main`. Create a feature branch before Task 1 (`git switch -c feat/realtime-attributed-wide-table`). Commit after each task.

---

### Task 0: Branch + scaffolding

**Files:**
- Create: `components/Data/codegen/__init__.py` (empty)
- Create: `components/Data/.gitignore` (ignore `__pycache__/`, `.pytest_cache/`)

- [ ] **Step 1: Create the feature branch**

Run:
```bash
cd /Users/twang/Projects/SignalPrism && git switch -c feat/realtime-attributed-wide-table
```
Expected: `Switched to a new branch 'feat/realtime-attributed-wide-table'`

- [ ] **Step 2: Create scaffolding files**

Create `components/Data/codegen/__init__.py` (empty file).

Create `components/Data/.gitignore`:
```
__pycache__/
.pytest_cache/
*.pyc
```

- [ ] **Step 3: Verify pytest is available**

Run: `python3 -m pytest --version`
Expected: prints a pytest version (e.g. `pytest 7.x`). If missing: `python3 -m pip install pytest`.

- [ ] **Step 4: Commit**

```bash
git add components/Data/.gitignore components/Data/codegen/__init__.py
git commit -m "chore: scaffold components/Data codegen package"
```

---

### Task 1: Schema-md catalog parser

**Files:**
- Create: `components/Data/codegen/schema_catalog.py`
- Test: `components/Data/codegen/test_schema_catalog.py`

**Interfaces:**
- Produces:
  - `Column = namedtuple("Column", ["name", "type", "source", "semantic", "null", "feat"])`
  - `parse_catalog(md_path: str) -> list[Column]` — every row of every field table.
  - `assign_source(col: Column) -> str` — returns `"hb"`, `"jaeger"`, or `"key"` (for `event_id`/`imp_id`).

- [ ] **Step 1: Write the failing test**

Create `components/Data/codegen/test_schema_catalog.py`:
```python
import os
from schema_catalog import parse_catalog, assign_source, Column

MD = os.path.join(os.path.dirname(__file__), "..", "..", "..",
                  "schemas", "realtime_attributed_wide_table_schema.md")

def test_parses_full_catalog():
    cols = parse_catalog(MD)
    names = {c.name for c in cols}
    # Spot-check representative columns from different sections.
    assert "event_id" in names
    assert "imp_id" in names
    assert "jgr_bid_floor" in names
    assert "hbn_adx_bid_price" in names
    assert "source_event_time" in names
    assert "jgr_winner_account_id" in names
    # Schema §9 states ~150 columns.
    assert len(cols) >= 140

def test_types_captured():
    by_name = {c.name: c for c in parse_catalog(MD)}
    assert by_name["jgr_bid_floor"].type == "DOUBLE"
    assert by_name["event_id"].type == "STRING"
    assert by_name["jgr_app_cat"].type.startswith("ARRAY")

def test_source_assignment_is_by_source_not_prefix():
    by_name = {c.name: c for c in parse_catalog(MD)}
    assert assign_source(by_name["jgr_bid_floor"]) == "jaeger"
    assert assign_source(by_name["hbn_adx_bid_price"]) == "hb"
    assert assign_source(by_name["event_id"]) == "key"
    # Anomaly: hbn_ name but jaeger source.
    assert assign_source(by_name["hbn_mediation_tmax"]) == "jaeger"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd components/Data/codegen && python3 -m pytest test_schema_catalog.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'schema_catalog'`.

- [ ] **Step 3: Write minimal implementation**

Create `components/Data/codegen/schema_catalog.py`:
```python
import re
from collections import namedtuple

Column = namedtuple("Column", ["name", "type", "source", "semantic", "null", "feat"])

# Field tables share this exact header (after stripping markdown backticks/spaces).
_HEADER = ["column", "type", "source", "semantic_type", "null", "feat", "description"]


def _clean(cell: str) -> str:
    # Strip backticks, markdown escapes, surrounding whitespace.
    return cell.replace("\\", "").replace("`", "").strip()


def parse_catalog(md_path: str) -> list:
    with open(md_path, encoding="utf-8") as fh:
        lines = fh.readlines()

    cols, in_table = [], False
    for line in lines:
        if not line.lstrip().startswith("|"):
            in_table = False
            continue
        cells = [_clean(c) for c in line.strip().strip("|").split("|")]
        # Detect the field-table header row.
        if [c.lower() for c in cells] == _HEADER:
            in_table = True
            continue
        # Skip the markdown separator row (|---|---|...).
        if set("".join(cells)) <= set("-: "):
            continue
        if in_table and len(cells) >= 6 and cells[0]:
            cols.append(Column(cells[0], cells[1], cells[2], cells[3], cells[4], cells[5]))
    # De-dup by name (a column appears once); keep first occurrence.
    seen, out = set(), []
    for c in cols:
        if c.name not in seen:
            seen.add(c.name)
            out.append(c)
    return out


def assign_source(col: Column) -> str:
    if col.name in ("event_id", "imp_id"):
        return "key"
    src = col.source.lower()
    if src.startswith("hb."):
        return "hb"
    if src.startswith("jaeger.") or src.startswith("derived"):
        return "jaeger"
    # Fallback: prefix-based, but log-worthy. jgr_->jaeger, hbn_->hb.
    if col.name.startswith("jgr_"):
        return "jaeger"
    if col.name.startswith("hbn_"):
        return "hb"
    return "jaeger"
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd components/Data/codegen && python3 -m pytest test_schema_catalog.py -v`
Expected: PASS (3 passed). If `test_parses_full_catalog` finds < 140, inspect which sections were missed (header variants) and adjust `_HEADER` matching.

- [ ] **Step 5: Commit**

```bash
git add components/Data/codegen/schema_catalog.py components/Data/codegen/test_schema_catalog.py
git commit -m "feat: parse schema-md field catalog into Column model"
```

---

### Task 2: Source-expression + SQL-type mappers

**Files:**
- Modify: `components/Data/codegen/schema_catalog.py` (add `sql_type`, `source_expr`)
- Test: `components/Data/codegen/test_mappers.py`

**Interfaces:**
- Consumes: `Column`, `assign_source` from Task 1.
- Produces:
  - `sql_type(physical_type: str) -> str` — `"DOUBLE"`→`"double"`, `"ARRAY<STRING>"`→`"array<string>"`, `"TIMESTAMP"`→`"timestamp"`, etc.
  - `source_expr(col: Column, staging: str) -> str` — the Spark SQL expression used as the col_map KEY for the given staging (`"jaeger"` or `"hb"`), after explode-alias rewriting.

- [ ] **Step 1: Write the failing test**

Create `components/Data/codegen/test_mappers.py`:
```python
import os
from schema_catalog import parse_catalog, sql_type, source_expr

MD = os.path.join(os.path.dirname(__file__), "..", "..", "..",
                  "schemas", "realtime_attributed_wide_table_schema.md")
BY = {c.name: c for c in parse_catalog(MD)}

def test_sql_type():
    assert sql_type("DOUBLE") == "double"
    assert sql_type("STRING") == "string"
    assert sql_type("LONG") == "bigint"
    assert sql_type("INT") == "int"
    assert sql_type("BOOLEAN") == "boolean"
    assert sql_type("TIMESTAMP") == "timestamp"
    assert sql_type("ARRAY<STRING>") == "array<string>"

def test_source_expr_rewrites_explode_aliases():
    # placement_serve_results[].X -> serve_result.X
    assert source_expr(BY["jgr_bid_floor"], "jaeger") == "serve_result.bid_floor"
    # placements[].X -> placement_.X
    assert source_expr(BY["jgr_placement_floor"], "jaeger") == "placement_.floor"
    # device.X stays
    assert source_expr(BY["jgr_dev_make"], "jaeger") == "device.make"
    # hb side strips hb.
    assert source_expr(BY["hbn_adx_bid_price"], "hb") == "adx_bid_price"

def test_source_expr_keys_are_staging_aware():
    assert source_expr(BY["event_id"], "jaeger") == "serve_result.ad_event_id"
    assert source_expr(BY["event_id"], "hb") == "event_id"
    assert source_expr(BY["imp_id"], "jaeger") == "serve_result.imp_id"
    assert source_expr(BY["imp_id"], "hb") == "bidrequest_imp_id"
    assert source_expr(BY["source_event_time"], "jaeger") == "timestamp"

def test_source_expr_winning_rtbconnection():
    # placement_serve_results[].rtbconnections[].X (winning) -> rtb_conn.X
    assert source_expr(BY["jgr_rtb_connection_id"], "jaeger") == "rtb_conn.id"
    assert source_expr(BY["jgr_rtb_account_id"], "jaeger") == "rtb_conn.account_id"
    assert source_expr(BY["jgr_rtb_is_internal"], "jaeger") == "rtb_conn.is_internal"
    # derived winner account id
    assert source_expr(BY["jgr_winner_account_id"], "jaeger") == "rtb_conn.account_id"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd components/Data/codegen && python3 -m pytest test_mappers.py -v`
Expected: FAIL — `ImportError: cannot import name 'sql_type'`.

- [ ] **Step 3: Write minimal implementation**

Append to `components/Data/codegen/schema_catalog.py`:
```python
_TYPE_MAP = {
    "STRING": "string", "DOUBLE": "double", "LONG": "bigint",
    "INT": "int", "BOOLEAN": "boolean", "TIMESTAMP": "timestamp",
}

# Staging-aware overrides for join keys / canonical event time.
_KEY_EXPR = {
    ("event_id", "jaeger"): "serve_result.ad_event_id",
    ("event_id", "hb"): "event_id",
    ("imp_id", "jaeger"): "serve_result.imp_id",
    ("imp_id", "hb"): "bidrequest_imp_id",
    ("source_event_time", "jaeger"): "timestamp",
}


def sql_type(physical_type: str) -> str:
    t = physical_type.strip()
    if t.upper().startswith("ARRAY<"):
        inner = t[t.index("<") + 1:t.rindex(">")].strip().upper()
        return "array<%s>" % _TYPE_MAP.get(inner, inner.lower())
    return _TYPE_MAP.get(t.upper(), t.lower())


def source_expr(col, staging: str) -> str:
    if (col.name, staging) in _KEY_EXPR:
        return _KEY_EXPR[(col.name, staging)]
    # jgr_winner_account_id has source "derived: jaeger winning rtbconnection account_id".
    if col.name == "jgr_winner_account_id":
        return "rtb_conn.account_id"
    # Take the staging-relevant side of a "a ↔ b" source, else the whole thing.
    raw = col.source
    if "↔" in raw:
        parts = [p.strip() for p in raw.split("↔")]
        raw = next((p for p in parts if p.lower().startswith(staging[:2])), parts[0])
    raw = raw.strip()
    # Drop trailing parenthetical annotations like " (winning)".
    raw = re.sub(r"\s*\([^)]*\)\s*$", "", raw).strip()
    # Strip leading topic qualifier.
    for prefix in ("jaeger.", "hb."):
        if raw.startswith(prefix):
            raw = raw[len(prefix):]
            break
    # Explode-alias rewrites. Order matters: rtbconnections before the generic
    # placement_serve_results[] rewrite, so the winning RTB element resolves to rtb_conn.
    raw = raw.replace("placement_serve_results[].rtbconnections[].", "rtb_conn.")
    raw = raw.replace("placement_serve_results[].", "serve_result.")
    raw = raw.replace("placements[].", "placement_.")
    raw = raw.replace("[].", ".")  # any remaining nested array element access
    return raw
```

> `re` is already imported in `schema_catalog.py` (Task 1). If not, add `import re` at the top.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd components/Data/codegen && python3 -m pytest test_mappers.py -v`
Expected: PASS (3 passed).

- [ ] **Step 5: Commit**

```bash
git add components/Data/codegen/schema_catalog.py components/Data/codegen/test_mappers.py
git commit -m "feat: add sql_type and staging-aware source_expr mappers"
```

---

### Task 3: Generate columns JSON (3 files)

**Files:**
- Create: `components/Data/codegen/generate.py`
- Create (generated): `components/Data/src/main/resources/columns/jaeger_transaction_wide.json`, `.../hb_transactions_wide.json`, `.../realtime_attributed_event_wide.json`
- Test: `components/Data/codegen/test_generate_columns.py`

**Interfaces:**
- Consumes: `parse_catalog`, `assign_source` (Tasks 1–2).
- Produces:
  - `jaeger_columns(cols) -> list[str]` — names where `assign_source in {jaeger, key}`, plus `source_event_time`.
  - `hb_columns(cols) -> list[str]` — names where `assign_source in {hb, key}`, plus `hbn_bidrequest_imp_id` join key handling.
  - `all_columns(cols) -> list[str]` — every catalog name (final wide table).
  - `write_columns(out_dir)` — writes the three JSON files.

- [ ] **Step 1: Write the failing test**

Create `components/Data/codegen/test_generate_columns.py`:
```python
import os
from schema_catalog import parse_catalog
from generate import jaeger_columns, hb_columns, all_columns

MD = os.path.join(os.path.dirname(__file__), "..", "..", "..",
                  "schemas", "realtime_attributed_wide_table_schema.md")
COLS = parse_catalog(MD)

def test_jaeger_columns_include_keys_and_jgr_and_anomaly():
    jc = set(jaeger_columns(COLS))
    assert "event_id" in jc and "imp_id" in jc
    assert "jgr_bid_floor" in jc
    assert "source_event_time" in jc
    assert "hbn_mediation_tmax" in jc          # jaeger-sourced anomaly
    assert "hbn_adx_bid_price" not in jc        # belongs to hb

def test_hb_columns_include_keys_and_hbn():
    hc = set(hb_columns(COLS))
    assert "event_id" in hc and "imp_id" in hc
    assert "hbn_adx_bid_price" in hc
    assert "jgr_bid_floor" not in hc

def test_all_columns_is_union_and_unique():
    ac = all_columns(COLS)
    assert len(ac) == len(set(ac))               # no dups
    assert set(ac) >= set(jaeger_columns(COLS)) | set(hb_columns(COLS))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd components/Data/codegen && python3 -m pytest test_generate_columns.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'generate'`.

- [ ] **Step 3: Write minimal implementation**

Create `components/Data/codegen/generate.py`:
```python
import json
import os
from schema_catalog import parse_catalog, assign_source, sql_type, source_expr

MD = os.path.join(os.path.dirname(__file__), "..", "..", "..",
                  "schemas", "realtime_attributed_wide_table_schema.md")
RES = os.path.join(os.path.dirname(__file__), "..", "src", "main", "resources")


def jaeger_columns(cols):
    return [c.name for c in cols if assign_source(c) in ("jaeger", "key")]


def hb_columns(cols):
    return [c.name for c in cols if assign_source(c) in ("hb", "key")]


def all_columns(cols):
    return [c.name for c in cols]


def _write_json(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(obj, fh, indent=2)
        fh.write("\n")


def write_columns(out_dir=RES):
    cols = parse_catalog(MD)
    _write_json(os.path.join(out_dir, "columns", "jaeger_transaction_wide.json"),
                jaeger_columns(cols))
    _write_json(os.path.join(out_dir, "columns", "hb_transactions_wide.json"),
                hb_columns(cols))
    _write_json(os.path.join(out_dir, "columns", "realtime_attributed_event_wide.json"),
                all_columns(cols))


if __name__ == "__main__":
    write_columns()
    print("columns written")
```

- [ ] **Step 4: Run test, then generate the files**

Run: `cd components/Data/codegen && python3 -m pytest test_generate_columns.py -v`
Expected: PASS (3 passed).

Run: `cd components/Data/codegen && python3 generate.py`
Expected: prints `columns written`; three files now exist under `components/Data/src/main/resources/columns/`.

Run: `cat ../src/main/resources/columns/jaeger_transaction_wide.json | python3 -m json.tool | head`
Expected: a JSON array starting with `"event_id"`.

- [ ] **Step 5: Commit**

```bash
git add components/Data/codegen/generate.py components/Data/codegen/test_generate_columns.py \
        components/Data/src/main/resources/columns/
git commit -m "feat: generate columns JSON for staging + wide tables"
```

---

### Task 4: Generate col_maps (2 files)

**Files:**
- Modify: `components/Data/codegen/generate.py` (add `write_col_maps`, `jaeger_col_map`, `hb_col_map`)
- Create (generated): `components/Data/src/main/resources/col_maps/jaeger_transaction_wide.json`, `.../hb_transactions_wide.json`
- Test: `components/Data/codegen/test_generate_colmaps.py`

**Interfaces:**
- Consumes: `source_expr`, `jaeger_columns`, `hb_columns`.
- Produces:
  - `jaeger_col_map(cols) -> dict` — `{source_expr: target_name}` for jaeger-staging columns (excluding the two raw join keys which are emitted as `event_id`/`imp_id` directly by the job).
  - `hb_col_map(cols) -> dict` — same for hb-staging columns.
  - `write_col_maps(out_dir)`.

- [ ] **Step 1: Write the failing test**

Create `components/Data/codegen/test_generate_colmaps.py`:
```python
import os
from schema_catalog import parse_catalog
from generate import jaeger_col_map, hb_col_map

MD = os.path.join(os.path.dirname(__file__), "..", "..", "..",
                  "schemas", "realtime_attributed_wide_table_schema.md")
COLS = parse_catalog(MD)

def test_jaeger_col_map_maps_source_expr_to_target():
    m = jaeger_col_map(COLS)
    assert m["serve_result.bid_floor"] == "jgr_bid_floor"
    assert m["device.make"] == "jgr_dev_make"
    assert m["placement_.floor"] == "jgr_placement_floor"

def test_hb_col_map():
    m = hb_col_map(COLS)
    assert m["adx_bid_price"] == "hbn_adx_bid_price"

def test_dropped_hb_dups_absent_from_hb_map():
    # §3.1: HB floor/device/geo copies are NOT carried.
    targets = set(hb_col_map(COLS).values())
    assert not any(t.startswith("hbn_dev_") for t in targets)
    assert "hbn_edsp_floor" not in targets
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd components/Data/codegen && python3 -m pytest test_generate_colmaps.py -v`
Expected: FAIL — `ImportError: cannot import name 'jaeger_col_map'`.

- [ ] **Step 3: Write minimal implementation**

Append to `components/Data/codegen/generate.py`:
```python
def _col_map(cols, staging, want_names):
    keys = ("event_id", "imp_id")  # emitted directly by the job, not via col_map
    m = {}
    for c in cols:
        if c.name in want_names and c.name not in keys:
            m[source_expr(c, staging)] = c.name
    return m


def jaeger_col_map(cols):
    return _col_map(cols, "jaeger", set(jaeger_columns(cols)))


def hb_col_map(cols):
    return _col_map(cols, "hb", set(hb_columns(cols)))


def write_col_maps(out_dir=RES):
    cols = parse_catalog(MD)
    _write_json(os.path.join(out_dir, "col_maps", "jaeger_transaction_wide.json"),
                jaeger_col_map(cols))
    _write_json(os.path.join(out_dir, "col_maps", "hb_transactions_wide.json"),
                hb_col_map(cols))
```

Also extend the `__main__` block at the bottom of `generate.py`:
```python
if __name__ == "__main__":
    write_columns()
    write_col_maps()
    print("columns + col_maps written")
```

> Note on §3.1 dropped dups: because `hb_columns` only includes columns whose `source` is `hb.*`, and the schema md never lists `hbn_edsp_floor`/`hbn_dev_*` (those copies were deliberately omitted by the schema author), they cannot appear. The test asserts this invariant holds.

- [ ] **Step 4: Run test, then generate**

Run: `cd components/Data/codegen && python3 -m pytest test_generate_colmaps.py -v`
Expected: PASS (3 passed).

Run: `cd components/Data/codegen && python3 generate.py`
Expected: prints `columns + col_maps written`.

Run: `python3 -m json.tool ../src/main/resources/col_maps/jaeger_transaction_wide.json | head`
Expected: a JSON object mapping source expressions to `jgr_*`/key targets.

- [ ] **Step 5: Commit**

```bash
git add components/Data/codegen/generate.py components/Data/codegen/test_generate_colmaps.py \
        components/Data/src/main/resources/col_maps/
git commit -m "feat: generate col_maps (source-expr -> target) for both ingestions"
```

---

### Task 5: Generate Iceberg DDL templates (3 files)

**Files:**
- Modify: `components/Data/codegen/generate.py` (add `ddl_template`, `write_ddl`)
- Create (generated): `components/Data/src/main/resources/sql/jaeger_transaction_wide_staging.template`, `.../hb_transactions_wide_staging.template`, `.../realtime_attributed_event_wide.template`
- Test: `components/Data/codegen/test_generate_ddl.py`

**Interfaces:**
- Consumes: `sql_type`, `jaeger_columns`, `hb_columns`, `all_columns`, and the catalog `Column.type`.
- Produces: `ddl_template(cols, names, partition_col="source_event_time") -> str`; `write_ddl(out_dir)`.

- [ ] **Step 1: Write the failing test**

Create `components/Data/codegen/test_generate_ddl.py`:
```python
import os
from schema_catalog import parse_catalog
from generate import ddl_template, all_columns

MD = os.path.join(os.path.dirname(__file__), "..", "..", "..",
                  "schemas", "realtime_attributed_wide_table_schema.md")
COLS = parse_catalog(MD)

def test_ddl_has_typed_columns_and_partition():
    ddl = ddl_template(COLS, all_columns(COLS))
    assert "CREATE TABLE IF NOT EXISTS ?table?" in ddl
    assert "event_id string" in ddl
    assert "jgr_bid_floor double" in ddl
    assert "jgr_app_cat array<string>" in ddl
    assert "USING iceberg" in ddl
    assert "PARTITIONED BY (hours(source_event_time))" in ddl
    assert 'LOCATION "?location?"' in ddl

def test_every_wide_column_appears_in_ddl():
    ddl = ddl_template(COLS, all_columns(COLS))
    for name in all_columns(COLS):
        assert ("\n    %s " % name) in ddl, "missing column in DDL: %s" % name
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd components/Data/codegen && python3 -m pytest test_generate_ddl.py -v`
Expected: FAIL — `ImportError: cannot import name 'ddl_template'`.

- [ ] **Step 3: Write minimal implementation**

Append to `components/Data/codegen/generate.py`:
```python
def ddl_template(cols, names, partition_col="source_event_time"):
    by_name = {c.name: c for c in cols}
    lines = []
    for n in names:
        lines.append("    %s %s" % (n, sql_type(by_name[n].type)))
    body = ",\n".join(lines)
    return (
        "CREATE TABLE IF NOT EXISTS ?table? (\n"
        + body + "\n)\n"
        "USING iceberg\n"
        "PARTITIONED BY (hours(%s))\n" % partition_col
        + 'LOCATION "?location?"\n'
        "TBLPROPERTIES (\n"
        "  'sort-order' = '%s ASC NULLS FIRST, event_id ASC NULLS FIRST'\n" % partition_col
        + ")\n"
    )


def write_ddl(out_dir=RES):
    cols = parse_catalog(MD)
    specs = [
        ("jaeger_transaction_wide_staging.template", jaeger_columns(cols)),
        ("hb_transactions_wide_staging.template", hb_columns(cols)),
        ("realtime_attributed_event_wide.template", all_columns(cols)),
    ]
    for fname, names in specs:
        path = os.path.join(out_dir, "sql", fname)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        # hb staging has no source_event_time; partition on a present time col.
        pcol = "source_event_time" if "source_event_time" in names else "hbn_timestamp"
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(ddl_template(cols, names, pcol))
```

Update the `__main__` block:
```python
if __name__ == "__main__":
    write_columns()
    write_col_maps()
    write_ddl()
    print("columns + col_maps + ddl written")
```

- [ ] **Step 4: Run test, then generate**

Run: `cd components/Data/codegen && python3 -m pytest test_generate_ddl.py -v`
Expected: PASS (2 passed).

Run: `cd components/Data/codegen && python3 generate.py`
Expected: prints `columns + col_maps + ddl written`.

Run: `head -5 ../src/main/resources/sql/realtime_attributed_event_wide.template`
Expected: `CREATE TABLE IF NOT EXISTS ?table? (` then typed columns.

- [ ] **Step 5: Commit**

```bash
git add components/Data/codegen/generate.py components/Data/codegen/test_generate_ddl.py \
        components/Data/src/main/resources/sql/
git commit -m "feat: generate Iceberg DDL templates for staging + wide tables"
```

---

### Task 6: End-to-end artifact validator

**Files:**
- Create: `components/Data/codegen/validate.py`
- Test: `components/Data/codegen/test_validate.py`

**Interfaces:**
- Consumes: all generators.
- Produces: `validate() -> list[str]` (list of human-readable problems; empty == valid). CLI: `python3 validate.py` exits non-zero if problems found.

- [ ] **Step 1: Write the failing test**

Create `components/Data/codegen/test_validate.py`:
```python
from validate import validate

def test_generated_artifacts_are_valid():
    problems = validate()
    assert problems == [], "validation problems:\n" + "\n".join(problems)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd components/Data/codegen && python3 -m pytest test_validate.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'validate'`.

- [ ] **Step 3: Write minimal implementation**

Create `components/Data/codegen/validate.py`:
```python
import json
import os
import sys
from schema_catalog import parse_catalog, sql_type
from generate import (MD, RES, jaeger_columns, hb_columns, all_columns,
                       jaeger_col_map, hb_col_map)


def _load_json(rel):
    with open(os.path.join(RES, rel), encoding="utf-8") as fh:
        return json.load(fh)


def validate():
    cols = parse_catalog(MD)
    by_name = {c.name: c for c in cols}
    problems = []

    # 1. columns JSON match the generators.
    checks = [
        ("columns/jaeger_transaction_wide.json", jaeger_columns(cols)),
        ("columns/hb_transactions_wide.json", hb_columns(cols)),
        ("columns/realtime_attributed_event_wide.json", all_columns(cols)),
    ]
    for rel, expected in checks:
        got = _load_json(rel)
        if got != expected:
            problems.append("columns mismatch in %s" % rel)

    # 2. every non-key target in the wide table is produced by a col_map or emitted directly.
    #    event_id/imp_id are emitted directly by the jobs; jgr_winner_account_id is special-cased
    #    (shares source expr rtb_conn.account_id with jgr_rtb_account_id) and emitted directly in
    #    the jaeger SELECT, so it is not in the col_map.
    jm, hm = jaeger_col_map(cols), hb_col_map(cols)
    produced = set(jm.values()) | set(hm.values()) | {"event_id", "imp_id", "jgr_winner_account_id"}
    for name in all_columns(cols):
        if name not in produced:
            problems.append("wide column not produced by any col_map: %s" % name)

    # 3. DDL contains every column with the correct sql type.
    for rel, names in [
        ("sql/jaeger_transaction_wide_staging.template", jaeger_columns(cols)),
        ("sql/hb_transactions_wide_staging.template", hb_columns(cols)),
        ("sql/realtime_attributed_event_wide.template", all_columns(cols)),
    ]:
        with open(os.path.join(RES, rel), encoding="utf-8") as fh:
            ddl = fh.read()
        for n in names:
            frag = "    %s %s" % (n, sql_type(by_name[n].type))
            if frag not in ddl:
                problems.append("DDL %s missing/typed-wrong: %s" % (rel, frag.strip()))

    # 4. §7.5 operational columns must be ABSENT (content-only).
    forbidden = {"lookup_status", "attribution_store_layer", "source_cluster",
                 "lookup_cluster", "attribution_delay_seconds", "context_schema_version"}
    for name in all_columns(cols):
        if name in forbidden:
            problems.append("forbidden §7.5 column present: %s" % name)

    return problems


if __name__ == "__main__":
    probs = validate()
    if probs:
        print("\n".join(probs))
        sys.exit(1)
    print("OK: all artifacts consistent with schema md")
```

- [ ] **Step 4: Run test + CLI**

Run: `cd components/Data/codegen && python3 -m pytest test_validate.py -v`
Expected: PASS (1 passed).

Run: `cd components/Data/codegen && python3 validate.py`
Expected: `OK: all artifacts consistent with schema md` (exit 0).

- [ ] **Step 5: Add a best-effort coba-YAML field check (authoritative source, not Trino)**

Append to `components/Data/codegen/validate.py`:
```python
import os as _os
import re as _re

# Authoritative field source = the coba schema YAMLs (NOT Trino, which under-declares).
LENA = _os.environ.get("LENA_REPO", "/Users/twang/Projects/lena")
COBA_YAMLS = [
    _os.path.join(LENA, "automation", "schemas", "ex-jaeger-transaction.yaml"),
    _os.path.join(LENA, "automation", "schemas", "hb-transactions.yaml"),
]


def _yaml_field_names(paths):
    # Lightweight: collect every `name: <field>` leaf at any nesting depth.
    names = set()
    for p in paths:
        if not _os.path.exists(p):
            return None  # YAMLs not available in this checkout; skip the check.
        with open(p, encoding="utf-8") as fh:
            for line in fh:
                m = _re.match(r"\s*name:\s*([A-Za-z0-9_]+)\s*$", line)
                if m:
                    names.add(m.group(1))
    return names


def check_against_coba_yaml():
    """Assert each col_map source's LEAF field exists somewhere in the coba YAMLs.
    Returns [] if YAMLs are absent (skipped) or all leaves resolve."""
    names = _yaml_field_names(COBA_YAMLS)
    if names is None:
        return []
    cols = parse_catalog(MD)
    problems = []
    for staging, cmap in (("jaeger", jaeger_col_map(cols)), ("hb", hb_col_map(cols))):
        for expr in cmap:
            leaf = expr.split(".")[-1]          # e.g. serve_result.bid_floor -> bid_floor
            leaf = _re.sub(r"\[.*$", "", leaf)   # strip any array indexing
            if leaf and leaf not in names:
                problems.append("%s col_map source leaf not in coba YAMLs: %s" % (staging, expr))
    return problems
```

Extend the `__main__` block of `validate.py`:
```python
if __name__ == "__main__":
    probs = validate() + check_against_coba_yaml()
    if probs:
        print("\n".join(probs))
        sys.exit(1)
    print("OK: all artifacts consistent with schema md")
```

- [ ] **Step 6: Run the check**

Run: `cd components/Data/codegen && LENA_REPO=/Users/twang/Projects/lena python3 validate.py`
Expected: `OK: all artifacts consistent with schema md`. If it lists "source leaf not in coba YAMLs", that field is genuinely missing from the topic schema — fix the schema md / col_map mapping (do NOT trust Trino). With `LENA_REPO` unset or absent, the coba check is skipped and only the schema-md checks run.

- [ ] **Step 7: Commit**

```bash
git add components/Data/codegen/validate.py components/Data/codegen/test_validate.py
git commit -m "feat: end-to-end validator + coba-YAML field check (authoritative source)"
```

---

### Task 7: Jaeger ingestion SparkMain

**Files:**
- Create: `components/Data/src/main/scala/com/vungle/signalprism/data/jaeger_transaction_ingestion/SparkMain.scala`
- Test: `components/Data/codegen/test_lint_jaeger_job.py`

**Interfaces:**
- Consumes: `columns/jaeger_transaction_wide.json`, `col_maps/jaeger_transaction_wide.json`, `sql/jaeger_transaction_wide_staging.template`.
- Produces: writes `ml_shadow.jaeger_transaction_wide_staging`.

- [ ] **Step 1: Write the failing structural-lint test**

Create `components/Data/codegen/test_lint_jaeger_job.py`:
```python
import os

SCALA = os.path.join(os.path.dirname(__file__), "..", "src", "main", "scala",
                     "com", "vungle", "signalprism", "data",
                     "jaeger_transaction_ingestion", "SparkMain.scala")

def _read():
    with open(SCALA, encoding="utf-8") as fh:
        return fh.read()

def test_extends_boilerplate_and_uses_required_machinery():
    s = _read()
    assert "object SparkMain extends BoilerplateSparkMain" in s
    assert "def requiredArgs" in s and "def defaultArgs" in s and "def run" in s
    # coba2 read (NOT a Kafka consumer) + sample + multi-stage explode + projection + write.
    assert "withCoba2TempViewInRange" in s
    assert "saveKafkaTopic" not in s          # must not re-consume Kafka
    assert "in_user_sample(sha1(serve_result.ad_event_id)" in s
    assert "explode(placement_serve_results)" in s
    assert "explode(placements)" in s
    assert "explode(serve_result.rtbconnections)" in s
    assert "serve_result.winner_id = rtb_conn.id" in s
    assert "getColsMapInJson" in s
    assert "col_maps/jaeger_transaction_wide.json" in s
    # jgr_winner_account_id is special-cased (shares source expr with jgr_rtb_account_id),
    # emitted directly rather than via the col_map.
    assert "rtb_conn.account_id AS jgr_winner_account_id" in s
    assert "mergeToIcebergTable" in s

def test_no_adhoc_udf_registration():
    s = _read()
    # All UDFs come from UDFUtil; no inline spark.udf.register(...).
    assert "spark.udf.register" not in s
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd components/Data/codegen && python3 -m pytest test_lint_jaeger_job.py -v`
Expected: FAIL — `FileNotFoundError` (Scala file not created yet).

- [ ] **Step 3: Write the Scala job**

Create `components/Data/src/main/scala/com/vungle/signalprism/data/jaeger_transaction_ingestion/SparkMain.scala`. This is modeled directly on lena's `edsp/deliveries_ingestion` (which reads the **same** `coba2.ex_jaeger_transaction` table). It reads coba2 output via `withCoba2TempViewInRange` — it does NOT consume Kafka.
```scala
package com.vungle.signalprism.data.jaeger_transaction_ingestion

import com.vungle.lena.{BoilerplateSparkMain, UDFUtil}
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

/**
 * Consume coba2.ex_jaeger_transaction (the full-payload coba2 landing of the
 * ex-jaeger-transaction topic, produced upstream by coba/ingestion2). Explode
 * placement_serve_results[] -> placements[] -> winning rtbconnections[], event-id sample,
 * project all jaeger-sourced wide-table columns, and write
 * ml_shadow.jaeger_transaction_wide_staging at (event_id, imp_id) grain.
 *
 * Projection is driven by col_maps/jaeger_transaction_wide.json (source-expr -> target),
 * generated from schemas/realtime_attributed_wide_table_schema.md.
 */
object SparkMain extends BoilerplateSparkMain {
  private val NS = "spark.app.signalprism.data.jaeger_transaction_ingestion"

  def requiredArgs: List[String] = List(
    "spark.app.env",
    "spark.app.batch_jobs.db.url",
    s"$NS.input.s3Dir",       // coba2 S3 base, e.g. s3a://.../coba2
    s"$NS.input.topic",       // "ex-jaeger-transaction"
    s"$NS.output.tableName"   // ml_shadow.jaeger_transaction_wide_staging
  )

  def defaultArgs: Map[String, String] = Map(
    "spark.rdd.compress" -> "true",
    "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
    s"$NS.test.create.table" -> "false",
    s"$NS.output.s3Dir" -> "",
    s"$NS.merge.lookback.days" -> "1",
    s"$NS.sample_rate" -> "0.0001",
    "spark.speculation" -> "false",
    "spark.hadoop.fs.s3a.fast.upload" -> "true",
    "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version" -> "2"
  )

  lazy val testCreateTable = args(s"$NS.test.create.table").toBoolean
  lazy val outputTable     = args(s"$NS.output.tableName")
  lazy val S3base          = args(s"$NS.input.s3Dir")
  lazy val outputS3Base    = args(s"$NS.output.s3Dir")
  lazy val topic           = args(s"$NS.input.topic")
  lazy val lookbackDays    = args(s"$NS.merge.lookback.days").toInt
  lazy val SAMPLE_RATE     = args(s"$NS.sample_rate").toDouble

  def registerSparkUDF(spark: SparkSession): Unit = {
    UDFUtil.registerNormalizeDeviceId(spark)
    UDFUtil.registerFormatCountry(spark)
    UDFUtil.registerFormatId(spark)
    UDFUtil.registerExtractAdomain(spark)
    UDFUtil.registerMappingTemplateName(spark)
    UDFUtil.registerInUserSample(spark)
    UDFUtil.registerMongoIdToTimestamp(spark)
  }

  // scalastyle:off
  def process(next: DateTime, tempTable: String): Unit = {
    val startMillis = System.currentTimeMillis()

    // source-expr -> target, e.g. "serve_result.bid_floor" -> "jgr_bid_floor",
    // "rtb_conn.account_id" -> "jgr_rtb_account_id", "device.make" -> "jgr_dev_make".
    // NB: jgr_winner_account_id is NOT in this map (it shares source expr rtb_conn.account_id
    // with jgr_rtb_account_id); it is emitted directly in the SELECT below.
    val columnMap = getColsMapInJson("col_maps/jaeger_transaction_wide.json")
    val colTransSpec = columnMap.map { case (k, v) => s"$k AS $v" }.mkString(",\n")

    val transformSql =
      s"""
        WITH served AS (
            SELECT *,
                   explode(placement_serve_results) AS serve_result
              FROM $tempTable
        ),
        served_sampled AS (
            SELECT *
              FROM served
             WHERE in_user_sample(sha1(serve_result.ad_event_id), $SAMPLE_RATE)
               AND serve_result.ad_event_id IS NOT NULL
        ),
        served_placement AS (
            SELECT *,
                   explode(placements) AS placement_
              FROM served_sampled
        ),
        served_rtb AS (
            SELECT *,
                   explode(serve_result.rtbconnections) AS rtb_conn
              FROM served_placement
             WHERE serve_result.winner_id IS NOT NULL
               AND serve_result.placement_reference_id = placement_.reference_id
        ),
        served_winner AS (
            SELECT *,
                   format_id(serve_result.ad_event_id) AS event_id,
                   serve_result.imp_id                 AS imp_id,
                   timestamp                           AS source_event_time
              FROM served_rtb
             WHERE serve_result.winner_id = rtb_conn.id
        )
        SELECT event_id,
               imp_id,
               rtb_conn.account_id AS jgr_winner_account_id,
               $colTransSpec,
               CAST('${toMinutelyTimeStr(next)}' AS timestamp) AS ingest_time
          FROM served_winner
         WHERE event_id IS NOT NULL
      """
    logExplain(transformSql, s"jaeger ingestion from coba2 $topic")
    val out = spark.sql(transformSql).dropDuplicates("event_id", "imp_id")

    mergeToIcebergTable(outputTable, out, lookbackDays, mergeKeysAllowNull = Array("imp_id"))
    reportStatsMetric(s"$appName.write.seconds", (System.currentTimeMillis() - startMillis) / 1000)
  }
  // scalastyle:on

  def run: Unit = {
    assertTestTableName(outputTable)
    if (isTest && testCreateTable) {
      assertTestS3(outputS3Base)
      createTestTable(outputTable, outputS3Base, "sql/jaeger_transaction_wide_staging.template")
    }
    registerSparkUDF(spark)

    val tillTimeStr = getTillTime(s"$NS.till", "yyyy-MM-dd HH:mm")
    val tillTime = DateTime.parse(tillTimeStr, DateTimeFormat.forPattern("yyyy-MM-dd HH:mm"))
    val tillDay = tillTime.toString("yyyy-MM-dd")
    val tillHour = tillTime.toString("HH")
    val tillMinute = tillTime.toString("mm")

    var nextStartAdjusted: DateTime = null
    for ((nextStart, nextTill) <- hourlyPeriodsForCurrentBatchMinute(tillDay, tillHour, tillMinute)) {
      assert(isTimeBeforeNow(nextTill))
      if (nextStartAdjusted == null) nextStartAdjusted = nextStart

      var hasDataProcessed = false
      withCoba2TempViewInRange(S3base, topic, nextStartAdjusted, nextTill, "_jaeger", storeS3IngestTime = true) {
        case (tempTable, upperTimeBound) =>
          process(nextStart, tempTable)
          nextStartAdjusted = upperTimeBound.plusMinutes(1)
          hasDataProcessed = true
      }
      if (isNotBackfill && hasDataProcessed) recordProgressMinute(nextStartAdjusted)
      reportStatsMetric(s"$appName.success", 1)
    }
  }
}
```

> The authoritative sample gate is the per-serve-result `in_user_sample(sha1(serve_result.ad_event_id), ...)` in `served_sampled` (no coarse pre-explode prune — dropped as schema-fragile). The placement-reference predicate `serve_result.placement_reference_id = placement_.reference_id` lives in `served_rtb`, NOT `served_placement`: `placement_` is an explode alias only in scope in the *next* CTE — Spark cannot resolve a project-list alias in the same block's `WHERE`. This matches lena's `edsp/deliveries_ingestion` `edsp_served_rtb`. Confirm `getColsMapInJson`, `withCoba2TempViewInRange`, `getTillTime`, `toMinutelyTimeStr`, and `recordProgressMinute` signatures against `edsp/deliveries_ingestion/SparkMain.scala` (they are used verbatim there).

- [ ] **Step 4: Run lint test**

Run: `cd components/Data/codegen && python3 -m pytest test_lint_jaeger_job.py -v`
Expected: PASS (2 passed).

- [ ] **Step 5: Commit**

```bash
git add components/Data/src/main/scala/com/vungle/signalprism/data/jaeger_transaction_ingestion/SparkMain.scala \
        components/Data/codegen/test_lint_jaeger_job.py
git commit -m "feat: jaeger_transaction_ingestion SparkMain (explode + sample + project)"
```

---

### Task 8: HB ingestion SparkMain

**Files:**
- Create: `components/Data/src/main/scala/com/vungle/signalprism/data/hb_transactions_ingestion/SparkMain.scala`
- Test: `components/Data/codegen/test_lint_hb_job.py`

**Interfaces:**
- Consumes: `col_maps/hb_transactions_wide.json`, `sql/hb_transactions_wide_staging.template`.
- Produces: writes `ml_shadow.hb_transactions_wide_staging` (served/winning bid per `(event_id, bidrequest_imp_id)`).

- [ ] **Step 1: Write the failing structural-lint test**

Create `components/Data/codegen/test_lint_hb_job.py`:
```python
import os

SCALA = os.path.join(os.path.dirname(__file__), "..", "src", "main", "scala",
                     "com", "vungle", "signalprism", "data",
                     "hb_transactions_ingestion", "SparkMain.scala")

def _read():
    with open(SCALA, encoding="utf-8") as fh:
        return fh.read()

def test_hb_job_contract():
    s = _read()
    assert "object SparkMain extends BoilerplateSparkMain" in s
    # coba2 read (NOT a Kafka consumer).
    assert "withCoba2TempViewInRange" in s
    assert "saveKafkaTopic" not in s
    assert "in_user_sample(sha1(event_id)" in s
    # Keep only the served/winning bid via row_number dedup.
    assert "row_number() OVER" in s
    assert "PARTITION BY event_id, bidrequest_imp_id" in s
    assert "getColsMapInJson" in s
    assert "col_maps/hb_transactions_wide.json" in s
    assert "mergeToIcebergTable" in s or "appendToIcebergTable" in s
    assert "spark.udf.register" not in s
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd components/Data/codegen && python3 -m pytest test_lint_hb_job.py -v`
Expected: FAIL — `FileNotFoundError`.

- [ ] **Step 3: Write the Scala job**

Create `components/Data/src/main/scala/com/vungle/signalprism/data/hb_transactions_ingestion/SparkMain.scala`:
```scala
package com.vungle.signalprism.data.hb_transactions_ingestion

import com.vungle.lena.{BoilerplateSparkMain, UDFUtil}
import org.joda.time.DateTime
import org.joda.time.DateTimeZone.UTC
import org.joda.time.format.DateTimeFormat

/**
 * Consume coba2.hb_transactions (the full-payload coba2 landing of the hb-transactions topic,
 * produced upstream by coba/ingestion2). Event-id sample, keep the served/winning bid per
 * (event_id, bidrequest_imp_id) via row_number, project all hb-sourced wide-table columns,
 * and write ml_shadow.hb_transactions_wide_staging. Modeled on hbp/auctions_served_ingestion.
 */
object SparkMain extends BoilerplateSparkMain {
  private val NS = "spark.app.signalprism.data.hb_transactions_ingestion"

  def requiredArgs: List[String] = List(
    "spark.app.env",
    "spark.app.batch_jobs.db.url",
    s"$NS.input.s3Dir",
    s"$NS.input.topic",
    s"$NS.output.tableName"
  )

  def defaultArgs: Map[String, String] = Map(
    "spark.rdd.compress" -> "true",
    "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
    s"$NS.test.create.table" -> "false",
    s"$NS.output.s3Dir" -> "",
    s"$NS.merge.lookback.days" -> "7",
    s"$NS.sample_rate" -> "0.0001",
    "spark.speculation" -> "false",
    "spark.hadoop.fs.s3a.fast.upload" -> "true",
    "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version" -> "2"
  )

  lazy val testCreateTable = args(s"$NS.test.create.table").toBoolean
  lazy val outputTable     = args(s"$NS.output.tableName")
  lazy val S3base          = args(s"$NS.input.s3Dir")
  lazy val outputS3Base    = args(s"$NS.output.s3Dir")
  lazy val topic           = args(s"$NS.input.topic")
  lazy val lookbackDays    = args(s"$NS.merge.lookback.days").toInt
  lazy val SAMPLE_RATE     = args(s"$NS.sample_rate").toDouble

  lazy val projection = getColsMapInJson("col_maps/hb_transactions_wide.json")
    .map { case (k, v) => s"$k AS $v" }.mkString(",\n  ")

  // scalastyle:off
  def process(next: DateTime, tempTable: String): Unit = {
    val startMillis = System.currentTimeMillis()
    val merged = spark.sql(
      s"""
        SELECT event_id, bidrequest_imp_id AS imp_id, $projection, timestamp AS hbn_timestamp
        FROM (
          SELECT *,
                 row_number() OVER (
                   PARTITION BY event_id, bidrequest_imp_id ORDER BY timestamp
                 ) AS _rn
          FROM $tempTable
          WHERE in_user_sample(sha1(event_id), $SAMPLE_RATE)
        )
        WHERE _rn = 1
      """)
    mergeToIcebergTable(outputTable, merged, lookbackDays, mergeKeysAllowNull = Array("imp_id"))
    reportStatsMetric(s"$appName.write.seconds", (System.currentTimeMillis() - startMillis) / 1000)
  }
  // scalastyle:on

  def run: Unit = {
    UDFUtil.registerCommonUDF(spark)
    UDFUtil.registerInUserSample(spark)

    assertTestTableName(outputTable)
    if (isTest && testCreateTable) {
      assertTestS3(outputS3Base)
      createTestTable(outputTable, outputS3Base, "sql/hb_transactions_wide_staging.template")
    }

    val pattern = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm")
    val tillTimeStr = getTillTime(s"$NS.till", "yyyy-MM-dd HH:mm")
    val tillTime = DateTime.parse(tillTimeStr, pattern).toDateTime(UTC)

    withCoba2TempViewInRange(S3base, topic, tillTime.minusHours(1), tillTime, "_hb_served") {
      process(tillTime, "_hb_served")
    }
    if (isNotBackfill) recordProgressMinute(tillTime)
    reportStatsMetric(s"$appName.success", 1)
  }
}
```

> Confirm `withCoba2TempViewInRange` / `getTillTime` / `recordProgressMinute` signatures against `Boilerplate.scala` and `hbp/auctions_served_ingestion/SparkMain.scala`. The progress/watermark wiring should match that reference job; the lint test does not assert exact watermark code, only the dedup + sample + write contract.

- [ ] **Step 4: Run lint test**

Run: `cd components/Data/codegen && python3 -m pytest test_lint_hb_job.py -v`
Expected: PASS (1 passed).

- [ ] **Step 5: Commit**

```bash
git add components/Data/src/main/scala/com/vungle/signalprism/data/hb_transactions_ingestion/SparkMain.scala \
        components/Data/codegen/test_lint_hb_job.py
git commit -m "feat: hb_transactions_ingestion SparkMain (sample + row_number dedup + project)"
```

---

### Task 9: Wide-table join SparkMain

**Files:**
- Create: `components/Data/src/main/scala/com/vungle/signalprism/data/realtime_attributed_wide/SparkMain.scala`
- Test: `components/Data/codegen/test_lint_join_job.py`

**Interfaces:**
- Consumes: `ml_shadow.jaeger_transaction_wide_staging`, `ml_shadow.hb_transactions_wide_staging`, `columns/*.json`, `sql/realtime_attributed_event_wide.template`.
- Produces: writes `ml_shadow.realtime_attributed_event_wide`.

- [ ] **Step 1: Write the failing structural-lint test**

Create `components/Data/codegen/test_lint_join_job.py`:
```python
import os

SCALA = os.path.join(os.path.dirname(__file__), "..", "src", "main", "scala",
                     "com", "vungle", "signalprism", "data",
                     "realtime_attributed_wide", "SparkMain.scala")

def _read():
    with open(SCALA, encoding="utf-8") as fh:
        return fh.read()

def test_join_contract():
    s = _read()
    assert "object SparkMain extends BoilerplateSparkMain" in s
    assert "LEFT JOIN" in s
    assert "j.event_id = h.event_id" in s
    assert "j.imp_id = h.imp_id" in s
    # Hit-rate metric per schema §2.3.
    assert "attribution_hit_rate" in s
    assert "reportStatsMetric" in s
    assert "mergeToIcebergTable" in s
    # Two-upstream watermark gating (pattern from notifications_attribution).
    assert "checkoutProgress" in s
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd components/Data/codegen && python3 -m pytest test_lint_join_job.py -v`
Expected: FAIL — `FileNotFoundError`.

- [ ] **Step 3: Write the Scala job**

Create `components/Data/src/main/scala/com/vungle/signalprism/data/realtime_attributed_wide/SparkMain.scala`:
```scala
package com.vungle.signalprism.data.realtime_attributed_wide

import com.vungle.lena.{BoilerplateSparkMain, UDFUtil}
import org.joda.time.DateTime
import org.joda.time.DateTimeZone.UTC
import org.joda.time.format.DateTimeFormat

/**
 * Join ml_shadow.jaeger_transaction_wide_staging (LEFT) to ml_shadow.hb_transactions_wide_staging
 * on (event_id, imp_id) and write ml_shadow.realtime_attributed_event_wide at (event_id, imp_id)
 * grain. Content-only: emits exactly the schema-md columns. HB §3.1 dup copies are already absent
 * from the hb staging table, so jaeger columns win automatically.
 */
object SparkMain extends BoilerplateSparkMain {
  private val NS = "spark.app.signalprism.data.realtime_attributed_wide"

  def requiredArgs: List[String] = List(
    "spark.app.env",
    "spark.app.batch_jobs.db.url",
    s"$NS.input.jaegerTable",
    s"$NS.input.hbTable",
    s"$NS.output.tableName"
  )

  def defaultArgs: Map[String, String] = Map(
    "spark.rdd.compress" -> "true",
    "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
    s"$NS.test.create.table" -> "false",
    s"$NS.output.s3Dir" -> "",
    s"$NS.merge.lookback.days" -> "1",
    s"$NS.lookback_valid_hours" -> "4",
    "spark.speculation" -> "false",
    "spark.hadoop.fs.s3a.fast.upload" -> "true",
    "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version" -> "2"
  )

  lazy val testCreateTable = args(s"$NS.test.create.table").toBoolean
  lazy val jaegerTable     = args(s"$NS.input.jaegerTable")
  lazy val hbTable         = args(s"$NS.input.hbTable")
  lazy val outputTable     = args(s"$NS.output.tableName")
  lazy val outputS3Base    = args(s"$NS.output.s3Dir")
  lazy val lookbackDays    = args(s"$NS.merge.lookback.days").toInt
  lazy val lookbackHours   = args(s"$NS.lookback_valid_hours").toInt

  // Columns sourced from the jaeger side vs the hb side (excluding shared keys).
  lazy val jaegerCols = getColsInJson("columns/jaeger_transaction_wide.json")
    .diff(List("event_id", "imp_id"))
  lazy val hbCols = getColsInJson("columns/hb_transactions_wide.json")
    .diff(List("event_id", "imp_id"))

  lazy val jaegerSelect = jaegerCols.map(c => s"j.$c AS $c").mkString(",\n  ")
  lazy val hbSelect     = hbCols.map(c => s"h.$c AS $c").mkString(",\n  ")

  // scalastyle:off
  def process(start: DateTime, till: DateTime): Unit = {
    val startMillis = System.currentTimeMillis()
    val s = start.toString("yyyy-MM-dd HH:mm:ss")
    val t = till.toString("yyyy-MM-dd HH:mm:ss")

    withEnableStoragePartitionJoin() { () =>
      val sql =
        s"""
          SELECT
            j.event_id AS event_id,
            j.imp_id   AS imp_id,
            $jaegerSelect,
            $hbSelect
          FROM $jaegerTable j
          LEFT JOIN $hbTable h
            ON j.event_id = h.event_id AND j.imp_id <=> h.imp_id
          WHERE j.source_event_time >= '$s' AND j.source_event_time < '$t'
        """
      logExplain(sql, s"join $jaegerTable with $hbTable")
      val wide = spark.sql(sql).dropDuplicates("event_id", "imp_id")

      // Go/no-go metric: attribution hit rate (schema §2.3).
      wide.createOrReplaceTempView("_wide")
      val hr = spark.sql(
        """SELECT
             SUM(CASE WHEN hbn_bidrequest_id IS NOT NULL THEN 1 ELSE 0 END) AS hit,
             COUNT(*) AS total
           FROM _wide""").collect()(0)
      val total = hr.getLong(1)
      if (total > 0) {
        reportStatsMetric(s"$appName.attribution_hit_rate", (hr.getLong(0) * 1000 / total))
      }

      mergeToIcebergTable(outputTable, wide, lookbackDays, mergeKeysAllowNull = Array("imp_id"))
      reportStatsMetric(s"$appName.write.seconds", (System.currentTimeMillis() - startMillis) / 1000)
    }
  }
  // scalastyle:on

  def run: Unit = {
    UDFUtil.registerCommonUDF(spark)

    assertTestTableName(outputTable)
    if (isTest && testCreateTable) {
      assertTestS3(outputS3Base)
      createTestTable(outputTable, outputS3Base, "sql/realtime_attributed_event_wide.template")
    }

    val pattern = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
    val finalTill = if (isNotBackfill) {
      val progressTime = checkoutProgressTimeCompatible.get
      val jaegerProgress = checkoutProgress("signalprism.data.jaeger_transaction_ingestion", "default")
        .map(x => parseTimeCompatible(x).get)
        .getOrElse(throw new Exception("jaeger_transaction_ingestion progress not found!"))
      val hbProgress = checkoutProgress("signalprism.data.hb_transactions_ingestion", "default")
        .map(x => parseTimeCompatible(x).get)
        .getOrElse(throw new Exception("hb_transactions_ingestion progress not found!"))
      val safeTill = hbProgress.minusHours(lookbackHours)
      val till = if (jaegerProgress.isBefore(safeTill)) jaegerProgress else safeTill
      if (!progressTime.isBefore(till)) {
        _logger.warn(s"ProgressTime [$progressTime] >= till [$till]. Skipping.")
        return
      }
      till
    } else {
      DateTime.parse(getTillTime(s"$NS.till", "yyyy-MM-dd HH:mm:ss"), pattern)
    }

    val tillDay = finalTill.toString("yyyy-MM-dd")
    val tillHour = finalTill.toString("HH")
    val tillMinute = finalTill.toString("mm")
    for ((nextStart, nextTill) <- hourlyPeriodsForCurrentBatchMinute(tillDay, tillHour, tillMinute)) {
      assert(isTimeBeforeNow(nextTill))
      process(nextStart, nextTill)
      if (isNotBackfill) recordProgressMinute(nextTill)
    }
    reportStatsMetric(s"$appName.success", 1)
  }
}
```

> The upstream job-name strings passed to `checkoutProgress` (`signalprism.data.jaeger_transaction_ingestion` / `...hb_transactions_ingestion`) must match the `appName` the ingestion jobs register progress under. Confirm how `appName` is derived in `Boilerplate.scala` and align these strings before running in a real cluster.

- [ ] **Step 4: Run lint test**

Run: `cd components/Data/codegen && python3 -m pytest test_lint_join_job.py -v`
Expected: PASS (1 passed).

- [ ] **Step 5: Commit**

```bash
git add components/Data/src/main/scala/com/vungle/signalprism/data/realtime_attributed_wide/SparkMain.scala \
        components/Data/codegen/test_lint_join_job.py
git commit -m "feat: realtime_attributed_wide join SparkMain (LEFT JOIN + hit-rate metric)"
```

---

### Task 10: README + full test sweep

**Files:**
- Create: `components/Data/README.md`
- Test: run the whole `components/Data/codegen` suite.

- [ ] **Step 1: Write the README**

Create `components/Data/README.md`:
```markdown
# components/Data — Realtime Attributed Wide Table Pipeline

Materializes `ml_shadow.realtime_attributed_event_wide` per
`schemas/realtime_attributed_wide_table_schema.md`. This is the **offline batch join**
producer of the table — both (a) the MLOps feature/training source for the Chapter 7 demo
and (b) the Phase-0 golden dataset the realtime attribution POC is graded against
(`proj_trd/realtime_attribution_wide_table_demo.md` §7.11 `offline_join_match_rate`).

## Source: coba2 raw landing tables

The two ingestion jobs **consume the existing coba2 output** — `coba2.ex_jaeger_transaction` and
`coba2.hb_transactions`, the full-payload S3 landings produced upstream by lena's `coba/ingestion2`
Kafka→S3 lander. We read them via `withCoba2TempViewInRange(S3base, topic, …)`. We do **not** run a
Kafka consumer and we do **not** read the slim domain tables (e.g. `edsp_deliveries`).

## Jobs (lena-style, Scala/Spark)

1. `jaeger_transaction_ingestion` — `coba2.ex_jaeger_transaction` → explode
   `placement_serve_results[]`/`placements[]`/winning `rtbconnections[]`, event-id sample,
   project all `jgr_*` → `ml_shadow.jaeger_transaction_wide_staging`. (Models `edsp/deliveries_ingestion`.)
2. `hb_transactions_ingestion` — `coba2.hb_transactions` → event-id sample, keep served/winning
   bid (`row_number`), project all `hbn_*` → `ml_shadow.hb_transactions_wide_staging`.
   (Models `hbp/auctions_served_ingestion`.)
3. `realtime_attributed_wide` — `LEFT JOIN` the two staging tables on `(event_id, imp_id)` →
   `ml_shadow.realtime_attributed_event_wide`. (Models `auction/notifications_attribution`.)

All three extend lena's `BoilerplateSparkMain` and use only `UDFUtil` UDFs.

## Resource artifacts are generated

`columns/*.json`, `col_maps/*.json`, and `sql/*.template` are generated from the schema md —
the single source of truth. Regenerate and validate:

```bash
cd codegen
python3 generate.py        # regenerate the 8 resource files
python3 validate.py        # assert artifacts match the schema md (exit 0 == OK)
python3 -m pytest -v       # run all parser/generator/lint tests
```

## Sampling

Both ingestion jobs gate on `in_user_sample(sha1(event_id), <sample_rate>)` with the **same**
rate (default `0.0001`) so both sides cover the same event-id cohort (schema §2.3, TRD §7.8).
Raise per demo phase: 0.001% → 0.01% → 0.1%.

## Out of scope

Realtime KV writer/lookup path (Phases 1–2), §7.5 operational columns, feature/aggregation/
simulation jobs, losing-bidder grain, CI fixtures, CD YAML. See the design spec:
`docs/superpowers/specs/2026-06-30-realtime-attributed-wide-table-pipeline-design.md`.

## Promotion into lena

These files are structured to drop into `/Users/twang/Projects/lena` (move `src/main/scala`
+ `src/main/resources` into lena's tree, switch package base to `com.vungle.lena.<domain>`,
confirm boilerplate helper names, then `sbt compile`).
```

- [ ] **Step 2: Run the full suite + validator**

Run: `cd components/Data/codegen && python3 -m pytest -v`
Expected: ALL pass (parser, mappers, columns, colmaps, ddl, validate, 3 lint suites).

Run: `cd components/Data/codegen && python3 validate.py`
Expected: `OK: all artifacts consistent with schema md`.

- [ ] **Step 3: Commit**

```bash
git add components/Data/README.md
git commit -m "docs: components/Data README for realtime attributed wide-table pipeline"
```

---

## Self-Review

**1. Spec coverage**

| Spec item | Task |
|---|---|
| 3-job architecture (2 ingestion + 1 join) | 7, 8, 9 |
| Consume coba2 output tables (`coba2.ex_jaeger_transaction`/`coba2.hb_transactions`), all schema-md fields | 7, 8 (`withCoba2TempViewInRange`), 3 (all columns) |
| Explode `placement_serve_results[]`/`placements[]`/`rtbconnections[]`, carry-down | 7 |
| Winning-RTB selection / `jgr_winner_account_id` | 7 (`served_winner` CTE, `winner_id = rtb_conn.id`) + Task 2 (`source_expr` → `rtb_conn.*`) |
| HB served/winning-bid `row_number` dedup | 8 |
| LEFT JOIN on `(event_id, imp_id)` | 9 |
| §3.1 dedup (jaeger wins) | enforced by `hb_columns` excluding non-`hb.` sources; validated Task 4/6 |
| `hbn_mediation_tmax` jaeger-source anomaly | Task 1 test + `assign_source` |
| Event-id sampling, same rate both sides | 7, 8 (default 0.0001) |
| Content-only (no §7.5 cols) | Task 6 forbidden-column check |
| `attribution_hit_rate` + go/no-go metrics | 9 |
| Two-upstream watermark gating | 9 |
| Full lena artifacts minus CI/CD | columns(3)/col_maps(2)/sql(3)/scala(3)/README; no CI fixtures/CD YAML |
| Iceberg DDL with `hours(source_event_time)` partition | 5 |

**2. Placeholder scan:** No "TBD/TODO/implement later". Every code step shows complete code. The two boilerplate-helper-name caveats (Tasks 7–9) are explicit verification instructions against named lena files, not placeholders.

**3. Type consistency:** `Column`, `parse_catalog`, `assign_source`, `sql_type`, `source_expr`, `jaeger_columns`/`hb_columns`/`all_columns`, `jaeger_col_map`/`hb_col_map`, `ddl_template`, `validate` are defined once and reused with the same signatures across tasks. Scala arg namespace `spark.app.signalprism.data.<job>.*` and table names are consistent across Tasks 7–9.

**Known follow-ups for the implementer (not blockers):**
- Source mechanism, helper name (`getColsMapInJson`), and winning-RTB predicate (`serve_result.winner_id = rtb_conn.id`) are all confirmed against `edsp/deliveries_ingestion` (reads the same `coba2.ex_jaeger_transaction`). No open question remains on these.
- The `served` CTE's coarse pre-explode sample prune (Task 7) is an optimization only; if its array-access expression doesn't typecheck against the live coba2 struct, drop it and keep the authoritative `served_sampled` gate (Task 7 note).
- Align `checkoutProgress` job-name strings with the ingestion jobs' registered `appName` (Task 9 note).
- **Verified (2026-06-30):** tables `raw.coba2.ex_jaeger_transaction` + `raw.coba2.hb_transactions`, partitions `dt,hr,mn` (no `az`), both fresh. All schema-md source fields — including `bidrequest_imp_id` and the 5 jaeger floor/misc fields — exist in the coba schema YAMLs even though the Trino metastore under-declares them. The join key `j.imp_id = h.bidrequest_imp_id` is valid; no join-logic change.
- `withCoba2TempViewInRange` handles partition selection by time range — confirm the `S3base` arg points at the coba2 root containing `ex_jaeger_transaction/` and `hb_transactions/` for your environment.
