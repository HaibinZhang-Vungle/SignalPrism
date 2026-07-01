# Realtime Attributed Hourly Aggregation Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Materialize the two hourly aggregation tables (`device_level_v1`, `non_device_context_v1`) from `ml_shadow.realtime_attributed_event_wide`, exactly as `schemas/realtime_attributed_aggregation_table_schema.md` specifies, via the existing codegen + lena-Spark pattern.

**Architecture:** A new section-aware markdown parser reads the aggregation contract into dimension specs, a metric catalog, and shared columns. Generators emit full-contract DDL templates plus JSON specs (`agg_specs/*.json`) into `resources/`. One parametrized Scala `SparkMain` (selected by `dimension_family`) reads those specs at runtime and builds the group-by aggregation SQL generically — computing sourceable metrics and letting `appendToIcebergTable` null-fill the rest. Two data-cd backfill YAMLs run it once per family.

**Tech Stack:** Python 3 (stdlib only, pytest) for codegen; Scala 2.12 / Spark 3.5 (`com.vungle.lena.BoilerplateSparkMain`) for the job; Iceberg on the stage Hive metastore; data-cd `ScheduledSparkApplication` YAML.

## Global Constraints

- **Authoritative contract:** `schemas/realtime_attributed_aggregation_table_schema.md` — never hand-edit generated artifacts; regenerate from the md.
- **Reference codebase for lena APIs:** `/Users/twang/Projects/lena` (`BoilerplateSparkMain`, `appendToIcebergTable`, `getColsInJson`, `in_user_sample`).
- **Two output tables (hourly):** `ml_shadow.realtime_attributed_device_level_hly` (family `device_level_v1`, key `device_id` = `jgr_lo_id`) and `ml_shadow.realtime_attributed_non_device_context_hly` (family `non_device_context_v1`, key `context_dim_id`).
- **Full contract + null-fill:** DDL carries every metric-catalog column; the job emits only `computed` metrics; `appendToIcebergTable` null-fills the rest.
- **No raw PII:** `jgr_dev_ifa`, `jgr_dev_ip`, `jgr_dev_ua` never appear as dimensions.
- **Null-dimension normalization:** `__unknown__` in the surrogate-key concat (`context_dim_id`/`device_dim_id`); stored typed dim columns may remain null.
- **Predicate-dependent & absent-source metrics** (see §6 of the spec) are NOT computed this round — emitted null and recorded via `aggregation_version`. Do not guess modulo predicates.
- **Top-N bucketing deferred:** `_bucket` dims normalize (lower/trim/parse-major) and pass through; no frequency table.
- **Sampling:** deterministic event-id hash via lena `in_user_sample`; `sample_rate` default `1.0`.
- **Partitioning:** Iceberg `PARTITIONED BY (hours(event_time), ingest_time, hashid)` (modulo precedent).
- **Spec:** `docs/superpowers/specs/2026-07-01-realtime-attributed-aggregation-pipeline-design.md`.
- Run all Python from `components/Data/codegen/` (imports are flat, e.g. `from schema_catalog import ...`).

---

### Task 1: Aggregation contract parser (`agg_schema_catalog.py`)

Parses the aggregation md into shared columns, per-family dimensions, and a classified metric catalog. Section-aware because §4 and §5.3 share the same header row.

**Files:**
- Create: `components/Data/codegen/agg_schema_catalog.py`
- Test: `components/Data/codegen/test_agg_schema_catalog.py`

**Interfaces:**
- Consumes: nothing (reads the md).
- Produces:
  - `AGG_MD` (str path constant).
  - `SharedCol = namedtuple("SharedCol", ["name","type","role","description"])`
  - `AggDim = namedtuple("AggDim", ["name","type","source_col","fallback_col","norm","role"])`
  - `AggMetric = namedtuple("AggMetric", ["name","kind","base_expr","predicate","columns","col_types"])`
  - `agg_sql_type(t: str) -> str`
  - `parse_shared_columns(md_path=AGG_MD) -> list[SharedCol]`
  - `parse_dims(family: str, md_path=AGG_MD) -> list[AggDim]` where `family ∈ {"device_level_v1","non_device_context_v1"}`
  - `parse_metrics(md_path=AGG_MD) -> list[AggMetric]` (both distribution-expanded and count metrics)
  - `classify(metric_name: str) -> str` ∈ `{"computed","null_absent_source","null_predicate_dependent"}`
  - Module constants `ABSENT_SOURCE: set[str]`, `PREDICATE_DEPENDENT: set[str]`.
  - `kind == "computed"` ⟹ `base_expr` (distribution) or `predicate` (count) is non-empty.

- [ ] **Step 1: Write the failing test**

```python
# components/Data/codegen/test_agg_schema_catalog.py
import os
from agg_schema_catalog import (
    parse_shared_columns, parse_dims, parse_metrics, classify,
    agg_sql_type, ABSENT_SOURCE, PREDICATE_DEPENDENT,
)

def test_shared_columns():
    shared = {c.name: c for c in parse_shared_columns()}
    assert shared["event_time"].type == "TIMESTAMP" and shared["event_time"].role == "time_key"
    assert shared["ingest_time"].role == "partition_key"
    assert shared["hashid"].role == "partition_key"
    assert "source_event_count" in shared and "aggregation_version" in shared

def test_non_device_dims_present_and_no_pii_no_ids():
    dims = {d.name: d for d in parse_dims("non_device_context_v1")}
    # representative dims
    assert dims["supply_name"].source_col == "hbn_supply_name"
    assert dims["placement_id"].source_col == "jgr_placement_id"
    assert dims["geoip_country_code"].norm == "coalesce"
    assert dims["geoip_country_code"].fallback_col == "jgr_geo_country"
    assert dims["app_version_major"].norm == "parse_major"
    assert dims["context_dim_id"].role == "surrogate_key"
    # no event/bid identifiers leak in as dimensions
    for banned in ("event_id", "imp_id", "jgr_auction_id", "winner_id", "rtb_connection_id"):
        assert banned not in dims

def test_device_dims_and_bucket_norm():
    dims = {d.name: d for d in parse_dims("device_level_v1")}
    assert dims["device_id"].source_col == "jgr_lo_id"
    assert dims["device_dim_id"].role == "surrogate_key"
    assert dims["dev_model_bucket"].norm == "bucket"
    assert dims["dev_platform"].norm == "normalize"
    # PII excluded
    for pii in ("jgr_dev_ifa", "jgr_dev_ip", "jgr_dev_ua"):
        assert pii not in {d.source_col for d in dims.values()}

def test_distribution_expands_to_five_typed_columns():
    metrics = {m.name: m for m in parse_metrics()}
    m = metrics["min_bid_to_win"]
    assert m.columns == ["min_bid_to_win_sum", "min_bid_to_win_count",
                         "min_bid_to_win_min", "min_bid_to_win_max",
                         "min_bid_to_win_squaresum"]
    assert m.col_types == ["double", "bigint", "double", "double", "double"]
    assert m.base_expr == "jgr_min_bid_to_win"
    assert m.kind == "computed"

def test_count_metric_and_classification():
    metrics = {m.name: m for m in parse_metrics()}
    assert metrics["delivery_count"].kind == "computed"
    assert metrics["delivery_count"].columns == ["delivery_count"]
    assert metrics["delivery_count"].col_types == ["bigint"]
    assert metrics["hb_bid_count"].col_types == ["double"]
    # classification matches spec §6
    assert classify("net_revenue") == "null_absent_source"
    assert classify("mediation_win_count") == "null_absent_source"
    assert classify("vx_min_bid_to_win") == "null_predicate_dependent"
    assert classify("bid_price_moloco") == "null_predicate_dependent"
    assert classify("settlement_price") == "computed"

def test_type_mapping():
    assert agg_sql_type("BIGINT") == "bigint"
    assert agg_sql_type("LONG") == "bigint"
    assert agg_sql_type("TIMESTAMP") == "timestamp"
    assert agg_sql_type("BOOLEAN") == "boolean"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd components/Data/codegen && python -m pytest test_agg_schema_catalog.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'agg_schema_catalog'`.

- [ ] **Step 3: Write minimal implementation**

```python
# components/Data/codegen/agg_schema_catalog.py
import os
import re
from collections import namedtuple

AGG_MD = os.path.join(os.path.dirname(__file__), "..", "..", "..",
                      "schemas", "realtime_attributed_aggregation_table_schema.md")

SharedCol = namedtuple("SharedCol", ["name", "type", "role", "description"])
AggDim = namedtuple("AggDim", ["name", "type", "source_col", "fallback_col", "norm", "role"])
AggMetric = namedtuple("AggMetric", ["name", "kind", "base_expr", "predicate", "columns", "col_types"])

_TYPE_MAP = {
    "STRING": "string", "DOUBLE": "double", "LONG": "bigint", "BIGINT": "bigint",
    "INT": "int", "BOOLEAN": "boolean", "TIMESTAMP": "timestamp",
}

# --- Reviewed metric classification (spec §6). These are decisions, not derivations. ---
ABSENT_SOURCE = {
    "net_revenue", "adv_spend", "pub_revenue", "bid_price_all",
    "mediation_loss_count", "mediation_win_count", "mediation_bill_count",
    "event_start_count",
}
PREDICATE_DEPENDENT = {
    "vx_min_bid_to_win", "edsp_highest_price_non_acc", "mediation_floor_txn",
    "min_bid_to_win_med", "bid_price_moloco", "settlement_price_loss",
    "settlement_price_won", "no_bid_count", "bid_count", "bid_count_moloco_count",
    "bid_count_acc_count", "sp_at_mediation_floor_count", "hb_bid_count",
    "mediation_auctions_count",
}
# Known count-metric predicates for the computed ones (only delivery_count this round).
_COUNT_PREDICATE = {"delivery_count": "jgr_no_serv_reason = 0"}

_DIST_SUFFIXES = [("sum", "double"), ("count", "bigint"), ("min", "double"),
                  ("max", "double"), ("squaresum", "double")]


def agg_sql_type(t):
    return _TYPE_MAP.get(t.strip().upper(), t.strip().lower())


def classify(metric_name):
    if metric_name in ABSENT_SOURCE:
        return "null_absent_source"
    if metric_name in PREDICATE_DEPENDENT:
        return "null_predicate_dependent"
    return "computed"


def _clean(cell):
    return cell.replace("\\", "").replace("`", "").strip()


def _rows(md_path):
    """Yield (section_heading, [clean cells]) for every markdown table data row."""
    with open(md_path, encoding="utf-8") as fh:
        lines = fh.readlines()
    heading = ""
    for line in lines:
        st = line.strip()
        if st.startswith("#"):
            heading = st.lstrip("#").strip()
            continue
        if not st.startswith("|"):
            continue
        cells = [c.strip() for c in st.strip("|").split("|")]
        if set("".join(cells)) <= set("-: "):   # separator row
            continue
        yield heading, cells


def _backticked(text):
    return re.findall(r"`([^`]+)`", text)


def parse_shared_columns(md_path=AGG_MD):
    out = []
    for heading, cells in _rows(md_path):
        if not heading.startswith("1."):
            continue
        header = [c.lower() for c in cells]
        if header[:4] == ["column", "type", "role", "description"]:
            continue
        if len(cells) >= 4 and cells[0] not in ("column", "table"):
            out.append(SharedCol(_clean(cells[0]), _clean(cells[1]), _clean(cells[2]), cells[3]))
    return out


def _dim_norm(name, source_text):
    toks = _backticked(source_text)
    low = source_text.lower()
    if "sha256" in low or "hash of normalized" in low:
        return ("surrogate", None, None)
    src = toks[0] if toks else None
    fallback = toks[1] if len(toks) > 1 else None
    if "prefer" in low and "fallback" in low:
        return ("coalesce", src, fallback)
    if "parse major" in low:
        return ("parse_major", src, None)
    if "top-n bucket" in low or "bucketed" in low or "bucket" in low:
        return ("bucket", src, None)
    if "normalized" in low or "normalize" in low or "lowercase" in low:
        return ("normalize", src, None)
    return ("passthrough", src, None)


def parse_dims(family, md_path=AGG_MD):
    section = "3." if family == "device_level_v1" else "4."
    out = []
    for heading, cells in _rows(md_path):
        if not heading.startswith(section):
            continue
        header = [c.lower() for c in cells]
        if header[0] == "column" and header[1] == "type":
            continue
        if len(cells) < 4 or not cells[0] or cells[0] == "column":
            continue
        name = _clean(cells[0])
        typ = agg_sql_type(cells[1])
        source_text = cells[2]
        norm, src, fallback = _dim_norm(name, source_text)
        role = "surrogate_key" if norm == "surrogate" else "dimension"
        # surrogate keys: device_dim_id derives from device_id; context_dim_id from all dims.
        out.append(AggDim(name, typ, src, fallback, norm, role))
    return out


def _parse_distribution(md_path):
    out = []
    for heading, cells in _rows(md_path):
        if not heading.startswith("5.2"):
            continue
        header = [c.lower() for c in cells]
        if header[0] == "family":
            continue
        if len(cells) < 3 or not cells[0]:
            continue
        family = _clean(cells[0])
        toks = _backticked(cells[2])
        kind = classify(family)
        base = toks[0] if (toks and kind == "computed") else None
        cols = ["%s_%s" % (family, s) for s, _ in _DIST_SUFFIXES]
        types = [t for _, t in _DIST_SUFFIXES]
        out.append(AggMetric(family, kind, base, None, cols, types))
    return out


def _parse_counts(md_path):
    out = []
    for heading, cells in _rows(md_path):
        if not heading.startswith("5.3"):
            continue
        header = [c.lower() for c in cells]
        if header[0] == "column":
            continue
        if len(cells) < 4 or not cells[0]:
            continue
        name = _clean(cells[0])
        typ = agg_sql_type(cells[1])
        kind = classify(name)
        pred = _COUNT_PREDICATE.get(name) if kind == "computed" else None
        out.append(AggMetric(name, kind, None, pred, [name], [typ]))
    return out


def parse_metrics(md_path=AGG_MD):
    return _parse_distribution(md_path) + _parse_counts(md_path)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd components/Data/codegen && python -m pytest test_agg_schema_catalog.py -v`
Expected: PASS (6 tests). If a dim-name assertion fails, read the md row and adjust the `_dim_norm` keyword rules — do not special-case column names.

- [ ] **Step 5: Commit**

```bash
git add components/Data/codegen/agg_schema_catalog.py components/Data/codegen/test_agg_schema_catalog.py
git commit -m "feat(codegen): aggregation contract parser (dims, metrics, classification)"
```

---

### Task 2: DDL + spec generators (`agg_generate.py`)

Emits the full-contract DDL templates, the two per-family JSON specs, and the metric-catalog JSON the Scala job reads; renders the checked-in reference `ddl/*.sql`; wires a regen entrypoint.

**Files:**
- Create: `components/Data/codegen/agg_generate.py`
- Test: `components/Data/codegen/test_agg_generate.py`
- Generated (by running it): `components/Data/src/main/resources/sql/realtime_attributed_device_level_hly.template`, `..._non_device_context_hly.template`, `components/Data/src/main/resources/agg_specs/device_level_v1.json`, `agg_specs/non_device_context_v1.json`, `agg_specs/metric_catalog.json`, `components/Data/ddl/realtime_attributed_device_level_hly.sql`, `..._non_device_context_hly.sql`

**Interfaces:**
- Consumes: everything from `agg_schema_catalog` (Task 1).
- Produces:
  - `RES` (resources dir), `DDL_DIR` constants.
  - `family_ddl_columns(family) -> list[(name, sql_type)]` — shared + dims + all metric cols, in contract order.
  - `agg_ddl_template(family) -> str` (with `?table?`/`?location?` placeholders).
  - `family_spec(family) -> dict` and `metric_catalog_spec() -> dict` (the JSON payloads).
  - `TABLE_LOCATION: dict[family] -> (fq_table, s3_location)`.
  - `write_all(out_res=RES, out_ddl=DDL_DIR) -> None`.

- [ ] **Step 1: Write the failing test**

```python
# components/Data/codegen/test_agg_generate.py
import agg_generate as g
from agg_schema_catalog import parse_metrics

def test_ddl_has_shared_dims_and_all_metric_columns():
    ddl = g.agg_ddl_template("non_device_context_v1")
    assert "CREATE TABLE IF NOT EXISTS ?table?" in ddl
    assert "event_time timestamp" in ddl
    assert "ingest_time string" in ddl
    assert "hashid string" in ddl
    assert "context_dim_id string" in ddl
    assert "supply_name string" in ddl
    # every metric column appears (computed AND null-filled)
    assert "min_bid_to_win_sum double" in ddl
    assert "min_bid_to_win_count bigint" in ddl
    assert "net_revenue_sum double" in ddl            # absent-source, still in contract
    assert "vx_min_bid_to_win_squaresum double" in ddl  # predicate-dep, still in contract
    assert "delivery_count bigint" in ddl
    assert "USING iceberg" in ddl
    assert "PARTITIONED BY (hours(event_time), ingest_time, hashid)" in ddl
    assert 'LOCATION "?location?"' in ddl

def test_device_ddl_has_device_key_not_context_key():
    ddl = g.agg_ddl_template("device_level_v1")
    assert "device_id string" in ddl
    assert "device_dim_id string" in ddl
    assert "context_dim_id" not in ddl

def test_family_spec_shape():
    spec = g.family_spec("non_device_context_v1")
    assert spec["dimension_family"] == "non_device_context_v1"
    assert spec["primary_key"]["name"] == "context_dim_id"
    assert spec["hashid_from"] == "context_dim_id"
    assert spec["drop_null_source"] is None
    names = {d["name"] for d in spec["dimensions"]}
    assert "supply_name" in names and "context_dim_id" not in names  # surrogate excluded from dim list
    geo = next(d for d in spec["dimensions"] if d["name"] == "geoip_country_code")
    assert geo["norm"] == "coalesce" and geo["fallback_col"] == "jgr_geo_country"

def test_device_spec_drops_null_device():
    spec = g.family_spec("device_level_v1")
    assert spec["primary_key"]["name"] == "device_dim_id"
    assert spec["drop_null_source"] == "jgr_lo_id"

def test_metric_catalog_only_marks_computed_with_exprs():
    cat = g.metric_catalog_spec()
    dist = {m["family"]: m for m in cat["distribution"]}
    assert dist["min_bid_to_win"]["kind"] == "computed"
    assert dist["min_bid_to_win"]["base_expr"] == "jgr_min_bid_to_win"
    assert dist["net_revenue"]["kind"] == "null_absent_source"
    assert dist["net_revenue"]["base_expr"] is None
    counts = {m["name"]: m for m in cat["count"]}
    assert counts["delivery_count"]["predicate"] == "jgr_no_serv_reason = 0"
    assert counts["no_bid_count"]["predicate"] is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd components/Data/codegen && python -m pytest test_agg_generate.py -v`
Expected: FAIL (`ModuleNotFoundError: No module named 'agg_generate'`).

- [ ] **Step 3: Write minimal implementation**

```python
# components/Data/codegen/agg_generate.py
import json
import os
from agg_schema_catalog import (
    AGG_MD, parse_shared_columns, parse_dims, parse_metrics, agg_sql_type,
)

RES = os.path.join(os.path.dirname(__file__), "..", "src", "main", "resources")
DDL_DIR = os.path.join(os.path.dirname(__file__), "..", "ddl")

FAMILIES = ["device_level_v1", "non_device_context_v1"]

TABLE_LOCATION = {
    "device_level_v1": (
        "hive_stg.ml_shadow.realtime_attributed_device_level_hly",
        "s3a://vungle2-dataeng/ml_shadow/realtime_attributed_device_level_hly",
    ),
    "non_device_context_v1": (
        "hive_stg.ml_shadow.realtime_attributed_non_device_context_hly",
        "s3a://vungle2-dataeng/ml_shadow/realtime_attributed_non_device_context_hly",
    ),
}
_TEMPLATE_NAME = {
    "device_level_v1": "realtime_attributed_device_level_hly",
    "non_device_context_v1": "realtime_attributed_non_device_context_hly",
}
_PRIMARY_KEY = {"device_level_v1": "device_dim_id", "non_device_context_v1": "context_dim_id"}
_DROP_NULL = {"device_level_v1": "jgr_lo_id", "non_device_context_v1": None}


def family_ddl_columns(family):
    cols = []
    for c in parse_shared_columns():
        cols.append((c.name, agg_sql_type(c.type)))
    for d in parse_dims(family):
        cols.append((d.name, d.type))
    for m in parse_metrics():
        for name, typ in zip(m.columns, m.col_types):
            cols.append((name, typ))
    # de-dup preserving first occurrence (defensive)
    seen, out = set(), []
    for n, t in cols:
        if n not in seen:
            seen.add(n)
            out.append((n, t))
    return out


def agg_ddl_template(family):
    body = ",\n".join("    %s %s" % (n, t) for n, t in family_ddl_columns(family))
    return (
        "CREATE TABLE IF NOT EXISTS ?table? (\n" + body + "\n)\n"
        "USING iceberg\n"
        "PARTITIONED BY (hours(event_time), ingest_time, hashid)\n"
        'LOCATION "?location?"\n'
        "TBLPROPERTIES (\n"
        "  'sort-order' = 'event_time ASC NULLS FIRST, hashid ASC NULLS FIRST'\n"
        ")\n"
    )


def family_spec(family):
    dims = [d for d in parse_dims(family) if d.role != "surrogate_key"]
    return {
        "dimension_family": family,
        "primary_key": {"name": _PRIMARY_KEY[family], "recipe": "sha256_concat"},
        "hashid_from": _PRIMARY_KEY[family],
        "drop_null_source": _DROP_NULL[family],
        "dimensions": [
            {"name": d.name, "type": d.type, "source_col": d.source_col,
             "fallback_col": d.fallback_col, "norm": d.norm}
            for d in dims
        ],
    }


def metric_catalog_spec():
    dist, count = [], []
    for m in parse_metrics():
        if len(m.columns) == 5:  # distribution family
            dist.append({"family": m.name, "kind": m.kind, "base_expr": m.base_expr,
                         "columns": m.columns})
        else:
            count.append({"name": m.name, "kind": m.kind, "predicate": m.predicate})
    return {"distribution": dist, "count": count}


def _write_json(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(obj, fh, indent=2)
        fh.write("\n")


def _write_text(path, text):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(text)


def write_all(out_res=RES, out_ddl=DDL_DIR):
    for fam in FAMILIES:
        tmpl = agg_ddl_template(fam)
        _write_text(os.path.join(out_res, "sql", "%s.template" % _TEMPLATE_NAME[fam]), tmpl)
        _write_json(os.path.join(out_res, "agg_specs", "%s.json" % fam), family_spec(fam))
        # rendered reference DDL
        table, loc = TABLE_LOCATION[fam]
        rendered = tmpl.replace("?table?", table).replace("?location?", loc)
        _write_text(os.path.join(out_ddl, "%s.sql" % _TEMPLATE_NAME[fam]), rendered)
    _write_json(os.path.join(out_res, "agg_specs", "metric_catalog.json"), metric_catalog_spec())


if __name__ == "__main__":
    write_all()
    print("agg: sql templates + agg_specs + metric_catalog + rendered ddl written")
```

- [ ] **Step 4: Run tests, then generate artifacts**

Run: `cd components/Data/codegen && python -m pytest test_agg_generate.py -v`
Expected: PASS (5 tests).
Then generate the files: `python agg_generate.py`
Expected: prints `agg: sql templates + agg_specs + metric_catalog + rendered ddl written`.
Verify: `ls ../src/main/resources/agg_specs/ ../src/main/resources/sql/realtime_attributed_*_hly.template ../ddl/realtime_attributed_*_hly.sql`

- [ ] **Step 5: Commit**

```bash
git add components/Data/codegen/agg_generate.py components/Data/codegen/test_agg_generate.py \
        components/Data/src/main/resources/sql/realtime_attributed_device_level_hly.template \
        components/Data/src/main/resources/sql/realtime_attributed_non_device_context_hly.template \
        components/Data/src/main/resources/agg_specs/ \
        components/Data/ddl/realtime_attributed_device_level_hly.sql \
        components/Data/ddl/realtime_attributed_non_device_context_hly.sql
git commit -m "feat(codegen): aggregation DDL templates + agg_specs + rendered ddl"
```

---

### Task 3: Consistency validator (`validate_agg.py`)

A standalone check that the generated artifacts still match the contract (mirrors the wide table's `validate.py`), so drift is caught before a Spark run.

**Files:**
- Create: `components/Data/codegen/validate_agg.py`
- Test: `components/Data/codegen/test_validate_agg.py`

**Interfaces:**
- Consumes: `agg_schema_catalog` (Task 1), `agg_generate` (Task 2), the written resource files.
- Produces: `validate_agg() -> list[str]` (problem strings; empty = OK). `__main__` prints and exits non-zero on problems.

- [ ] **Step 1: Write the failing test**

```python
# components/Data/codegen/test_validate_agg.py
from validate_agg import validate_agg

def test_generated_artifacts_are_consistent():
    # assumes `python agg_generate.py` has been run (Task 2 step 4)
    assert validate_agg() == []
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd components/Data/codegen && python -m pytest test_validate_agg.py -v`
Expected: FAIL (`ModuleNotFoundError: No module named 'validate_agg'`).

- [ ] **Step 3: Write minimal implementation**

```python
# components/Data/codegen/validate_agg.py
import json
import os
import sys
from agg_generate import (RES, FAMILIES, family_ddl_columns, family_spec,
                          metric_catalog_spec, _TEMPLATE_NAME)


def _load_json(rel):
    with open(os.path.join(RES, rel), encoding="utf-8") as fh:
        return json.load(fh)


def _read(rel):
    with open(os.path.join(RES, rel), encoding="utf-8") as fh:
        return fh.read()


def validate_agg():
    problems = []
    # 1. DDL template contains every contract column with its type.
    for fam in FAMILIES:
        ddl = _read(os.path.join("sql", "%s.template" % _TEMPLATE_NAME[fam]))
        for name, typ in family_ddl_columns(fam):
            if ("    %s %s" % (name, typ)) not in ddl:
                problems.append("%s DDL missing/typed-wrong: %s %s" % (fam, name, typ))
        if "PARTITIONED BY (hours(event_time), ingest_time, hashid)" not in ddl:
            problems.append("%s DDL wrong partitioning" % fam)
    # 2. agg_specs JSON on disk match the generators.
    for fam in FAMILIES:
        if _load_json(os.path.join("agg_specs", "%s.json" % fam)) != family_spec(fam):
            problems.append("agg_specs/%s.json out of date" % fam)
    if _load_json(os.path.join("agg_specs", "metric_catalog.json")) != metric_catalog_spec():
        problems.append("agg_specs/metric_catalog.json out of date")
    # 3. No raw PII source columns leaked into any dimension spec.
    pii = {"jgr_dev_ifa", "jgr_dev_ip", "jgr_dev_ua"}
    for fam in FAMILIES:
        for d in family_spec(fam)["dimensions"]:
            if d["source_col"] in pii:
                problems.append("PII source in %s dim %s" % (fam, d["name"]))
    # 4. Every 'computed' metric carries an expr/predicate; every non-computed carries none.
    cat = metric_catalog_spec()
    for m in cat["distribution"]:
        if m["kind"] == "computed" and not m["base_expr"]:
            problems.append("computed distribution missing base_expr: %s" % m["family"])
        if m["kind"] != "computed" and m["base_expr"]:
            problems.append("non-computed distribution has base_expr: %s" % m["family"])
    for m in cat["count"]:
        if m["kind"] == "computed" and not m["predicate"]:
            problems.append("computed count missing predicate: %s" % m["name"])
        if m["kind"] != "computed" and m["predicate"]:
            problems.append("non-computed count has predicate: %s" % m["name"])
    return problems


if __name__ == "__main__":
    problems = validate_agg()
    if problems:
        print("\n".join(problems))
        sys.exit(1)
    print("OK: aggregation artifacts consistent with schema md")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd components/Data/codegen && python -m pytest test_validate_agg.py -v && python validate_agg.py`
Expected: test PASS; script prints `OK: aggregation artifacts consistent with schema md`.

- [ ] **Step 5: Commit**

```bash
git add components/Data/codegen/validate_agg.py components/Data/codegen/test_validate_agg.py
git commit -m "feat(codegen): aggregation artifact consistency validator"
```

---

### Task 4: Parametrized aggregation Spark job (`SparkMain.scala`)

One `BoilerplateSparkMain` that reads a family spec + the metric catalog from resources and builds the group-by aggregation SQL generically. Verified by a Python lint test (matching the repo's `test_lint_*_job.py` pattern — no Scala build here).

**Files:**
- Create: `components/Data/src/main/scala/com/vungle/signalprism/data/realtime_attributed_aggregation/SparkMain.scala`
- Test: `components/Data/codegen/test_lint_agg_job.py`

**Interfaces:**
- Consumes: `agg_specs/<family>.json`, `agg_specs/metric_catalog.json` (Task 2) via lena `getResourceString`; input table `realtime_attributed_event_wide` (columns per `columns/realtime_attributed_event_wide.json`).
- Produces: the two hourly Iceberg tables. Args namespace `spark.app.signalprism.data.realtime_attributed_aggregation.*`: `dimension_family`, `input.tableName`, `output.tableName`, `till`, `sample_rate` (default `1.0`), `aggregation_version`.

- [ ] **Step 1: Write the failing lint test**

```python
# components/Data/codegen/test_lint_agg_job.py
import os
SCALA = os.path.join(os.path.dirname(__file__), "..", "src", "main", "scala",
                     "com", "vungle", "signalprism", "data",
                     "realtime_attributed_aggregation", "SparkMain.scala")

def _read():
    with open(SCALA, encoding="utf-8") as fh:
        return fh.read()

def test_boilerplate_and_required_machinery():
    s = _read()
    assert "object SparkMain extends BoilerplateSparkMain" in s
    assert "def requiredArgs" in s and "def defaultArgs" in s and "def run" in s
    # one parametrized job selected by dimension_family
    assert "dimension_family" in s
    # reads the codegen'd specs from resources (not hardcoded columns)
    assert "agg_specs/" in s
    assert "metric_catalog.json" in s
    assert "getResourceString" in s or "getColsMapInJson" in s or "getColsInJson" in s
    # deterministic event-id sample, hourly grouping, surrogate key, write
    assert "in_user_sample" in s
    assert "date_trunc('HOUR'" in s or "hours(" in s
    assert "sha2(" in s
    assert "GROUP BY" in s
    assert "appendToIcebergTable" in s
    # audit + versioning present
    assert "source_event_count" in s
    assert "aggregation_version" in s

def test_no_pii_and_no_predicate_guessing():
    s = _read()
    for pii in ("jgr_dev_ifa", "jgr_dev_ip", "jgr_dev_ua"):
        assert pii not in s
    assert "spark.udf.register" not in s
    # predicate-dependent metrics must not be silently computed with a guessed filter
    assert "moloco" not in s.lower()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd components/Data/codegen && python -m pytest test_lint_agg_job.py -v`
Expected: FAIL (`FileNotFoundError` for `SparkMain.scala`).

- [ ] **Step 3: Write the implementation**

```scala
// components/Data/src/main/scala/com/vungle/signalprism/data/realtime_attributed_aggregation/SparkMain.scala
package com.vungle.signalprism.data.realtime_attributed_aggregation

import com.vungle.lena.{BoilerplateSparkMain, UDFUtil}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import scala.util.parsing.json.JSON

/**
 * Hourly aggregation over ml_shadow.realtime_attributed_event_wide into one of two reviewed
 * dimension families (device_level_v1 / non_device_context_v1), selected by dimension_family.
 *
 * Contract: schemas/realtime_attributed_aggregation_table_schema.md. The dimension list, per-dim
 * normalization, surrogate-key recipe and metric catalog are read at runtime from the codegen'd
 * resources (agg_specs/<family>.json, agg_specs/metric_catalog.json) so no metric-specific code
 * path exists. Only 'computed' metrics are emitted; predicate-dependent / absent-source metrics
 * are left out and appendToIcebergTable null-fills them against the full-contract DDL.
 */
object SparkMain extends BoilerplateSparkMain {
  private val NS = "spark.app.signalprism.data.realtime_attributed_aggregation"

  def requiredArgs: List[String] = List(
    "spark.app.env",
    "spark.app.batch_jobs.db.url",
    s"$NS.dimension_family",
    s"$NS.input.tableName",
    s"$NS.output.tableName"
  )

  def defaultArgs: Map[String, String] = Map(
    "spark.rdd.compress" -> "true",
    "spark.serializer" -> "org.apache.spark.serializer.KryoSerializer",
    s"$NS.test.create.table" -> "false",
    s"$NS.output.s3Dir" -> "",
    s"$NS.sample_rate" -> "1.0",
    s"$NS.aggregation_version" -> "v1_computed_only",
    "spark.speculation" -> "false",
    "spark.hadoop.fs.s3a.fast.upload" -> "true",
    "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version" -> "2"
  )

  lazy val testCreateTable = args(s"$NS.test.create.table").toBoolean
  lazy val family      = args(s"$NS.dimension_family")
  lazy val inputTable  = args(s"$NS.input.tableName")
  lazy val outputTable = args(s"$NS.output.tableName")
  lazy val outputS3    = args(s"$NS.output.s3Dir")
  lazy val sampleRate  = args(s"$NS.sample_rate").toDouble
  lazy val aggVersion  = args(s"$NS.aggregation_version")

  // ---- spec loading (scala.util.parsing.json keeps this dependency-free) ----
  private def json(res: String): Map[String, Any] =
    JSON.parseFull(getResourceString(res)).get.asInstanceOf[Map[String, Any]]
  private def arr(a: Any): List[Any] = a.asInstanceOf[List[Any]]
  private def obj(a: Any): Map[String, Any] = a.asInstanceOf[Map[String, Any]]
  private def str(a: Any): String = if (a == null) null else a.toString

  lazy val spec        = json(s"agg_specs/$family.json")
  lazy val metricCat   = json("agg_specs/metric_catalog.json")
  lazy val primaryKey  = str(obj(spec("primary_key"))("name"))
  lazy val dropNullSrc = str(spec.getOrElse("drop_null_source", null))
  lazy val dims        = arr(spec("dimensions")).map(obj)

  // normalized, still-nullable stored dimension expression
  private def dimExpr(d: Map[String, Any]): String = {
    val name = str(d("name")); val src = str(d("source_col")); val norm = str(d("norm"))
    val e = norm match {
      case _ if src == null       => "CAST(NULL AS STRING)"
      case "parse_major"          => s"split($src, '\\\\.')[0]"
      case "coalesce"             => s"coalesce($src, ${str(d("fallback_col"))})"
      case "normalize" | "bucket" => s"lower(trim($src))"  // bucket: top-N deferred → normalized passthrough
      case _                      => src
    }
    s"$e AS $name"
  }

  // key concat coalesces every dim to '__unknown__' (contract §2)
  private def keyConcatArg(d: Map[String, Any]): String =
    s"coalesce(CAST(${str(d("name"))} AS STRING), '__unknown__')"

  private def computedMetricSelects: Seq[String] = {
    val dist = arr(metricCat("distribution")).map(obj)
      .filter(m => str(m("kind")) == "computed")
      .flatMap { m =>
        val fam = str(m("family")); val e = str(m("base_expr"))
        Seq(
          s"sum($e) AS ${fam}_sum",
          s"count($e) AS ${fam}_count",
          s"min($e) AS ${fam}_min",
          s"max($e) AS ${fam}_max",
          s"sum($e * $e) AS ${fam}_squaresum"
        )
      }
    val counts = arr(metricCat("count")).map(obj)
      .filter(m => str(m("kind")) == "computed")
      .map { m =>
        val n = str(m("name")); val p = str(m("predicate"))
        s"sum(CASE WHEN $p THEN 1 ELSE 0 END) AS $n"
      }
    dist ++ counts
  }

  // scalastyle:off
  def process(start: DateTime, till: DateTime): Unit = {
    val startMillis = System.currentTimeMillis()
    val s = start.toString("yyyy-MM-dd HH:mm:ss")
    val t = till.toString("yyyy-MM-dd HH:mm:ss")

    val dimStored  = dims.map(dimExpr).mkString(",\n    ")
    val dimNames   = dims.map(d => str(d("name")))
    val keyArgs    = dims.map(keyConcatArg).mkString(", ")
    val dropClause = if (dropNullSrc != null) s"AND $dropNullSrc IS NOT NULL" else ""

    // Single scan: normalized dims + hour bucket alongside the raw metric base columns (`*`),
    // so dims and metric sources coexist for the outer GROUP BY.
    spark.sql(
      s"""
        SELECT
          $dimStored,
          date_trunc('HOUR', source_event_time) AS event_time,
          source_event_time,
          *
        FROM $inputTable
        WHERE source_event_time >= '$s' AND source_event_time < '$t'
          AND in_user_sample(sha1(event_id), $sampleRate)
          $dropClause
      """).createOrReplaceTempView("_agg_src")

    val groupCols = (dimNames :+ "event_time").mkString(", ")
    val metricSel = computedMetricSelects.mkString(",\n    ")

    val outSql =
      s"""
        SELECT
          event_time,
          date_format(event_time, 'yyyy-MM-dd-HH') AS ingest_time,
          substr(sha2(concat_ws('|', $keyArgs), 256), 1, 2) AS hashid,
          sha2(concat_ws('|', $keyArgs), 256) AS $primaryKey,
          ${dimNames.mkString(",\n          ")},
          $metricSel,
          count(*) AS source_event_count,
          min(source_event_time) AS first_source_event_time,
          max(source_event_time) AS last_source_event_time,
          '$aggVersion' AS aggregation_version
        FROM _agg_src
        GROUP BY $groupCols
      """
    logExplain(outSql, s"aggregate $inputTable -> $outputTable [$family]")
    val agg = spark.sql(outSql)
    appendToIcebergTable(outputTable, agg)
    reportStatsMetric(s"$appName.write.seconds", (System.currentTimeMillis() - startMillis) / 1000)
  }
  // scalastyle:on

  def run: Unit = {
    UDFUtil.registerCommonUDF(spark)
    UDFUtil.registerInUserSample(spark)

    assertTestTableName(outputTable)
    if (isTest && testCreateTable) {
      assertTestS3(outputS3)
      val tmpl =
        if (family == "device_level_v1") "sql/realtime_attributed_device_level_hly.template"
        else "sql/realtime_attributed_non_device_context_hly.template"
      createTestTable(outputTable, outputS3, tmpl)
    }

    val pattern = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss")
    val finalTill = if (isNotBackfill) {
      val progress = checkoutProgressTimeCompatible.get
      val src = checkoutProgress("signalprism.data.realtime_attributed_wide", "default")
        .map(x => parseTimeCompatible(x).get)
        .getOrElse(throw new Exception("realtime_attributed_wide progress not found!"))
      if (!progress.isBefore(src)) { _logger.warn(s"ProgressTime [$progress] >= till [$src]. Skipping."); return }
      src
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

- [ ] **Step 4: Run the lint test**

Run: `cd components/Data/codegen && python -m pytest test_lint_agg_job.py -v`
Expected: PASS (2 tests). (Compilation happens later in the lena assembly build, per the wide-table workflow — this repo has no Scala build.)

- [ ] **Step 5: Verify computed base columns exist in the wide table**

Run: `cd components/Data/codegen && python - <<'PY'
import json, os
from agg_generate import RES, metric_catalog_spec
cols = set(json.load(open(os.path.join(RES, "columns", "realtime_attributed_event_wide.json"))))
missing = []
for m in metric_catalog_spec()["distribution"]:
    if m["kind"] == "computed" and m["base_expr"] not in cols:
        missing.append(m["base_expr"])
for m in metric_catalog_spec()["count"]:
    if m["kind"] == "computed" and m["predicate"]:
        base = m["predicate"].split()[0]
        if base not in cols:
            missing.append(base)
print("MISSING computed base columns:", missing or "none")
PY`
Expected: `MISSING computed base columns: none`. If any are listed, that metric's classification is wrong — move it to `PREDICATE_DEPENDENT` in `agg_schema_catalog.py`, rerun Tasks 1–3, and regenerate.

- [ ] **Step 6: Commit**

```bash
git add components/Data/src/main/scala/com/vungle/signalprism/data/realtime_attributed_aggregation/SparkMain.scala \
        components/Data/codegen/test_lint_agg_job.py
git commit -m "feat(data): parametrized hourly aggregation Spark job (device + non-device families)"
```

---

### Task 5: data-cd backfill YAMLs

Two `ScheduledSparkApplication` backfills that run the one job once per family, cloned from the wide-table backfills. No test cycle (deploy manifests); verified by a structural check.

**Files:**
- Create: `/Users/twang/Projects/lena/cd/lena-test/stage-signal-prism-agg-device-level-backfill.yaml`
- Create: `/Users/twang/Projects/lena/cd/lena-test/stage-signal-prism-agg-non-device-context-backfill.yaml`
- Test: `components/Data/codegen/test_lint_agg_yaml.py`

**Interfaces:**
- Consumes: the built lena assembly jar (same `mainApplicationFile` as the wide-table backfills) and the `SparkMain` from Task 4.
- Produces: nothing consumed downstream (runtime manifests).

- [ ] **Step 1: Write the failing structural test**

```python
# components/Data/codegen/test_lint_agg_yaml.py
import os
CD = "/Users/twang/Projects/lena/cd/lena-test"
YAMLS = {
    "device_level_v1": os.path.join(CD, "stage-signal-prism-agg-device-level-backfill.yaml"),
    "non_device_context_v1": os.path.join(CD, "stage-signal-prism-agg-non-device-context-backfill.yaml"),
}
OUT = {
    "device_level_v1": "hive_stg.ml_shadow.realtime_attributed_device_level_hly",
    "non_device_context_v1": "hive_stg.ml_shadow.realtime_attributed_non_device_context_hly",
}

def test_yamls_target_the_one_job_and_correct_family():
    for fam, path in YAMLS.items():
        s = open(path, encoding="utf-8").read()
        assert "com.vungle.signalprism.data.realtime_attributed_aggregation.SparkMain" in s
        assert (".dimension_family: \"%s\"" % fam) in s
        assert OUT[fam] in s
        assert "hive_stg.ml_shadow.realtime_attributed_event_wide" in s  # input
        assert ".sample_rate: \"1.0\"" in s
        assert ".till:" in s
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd components/Data/codegen && python -m pytest test_lint_agg_yaml.py -v`
Expected: FAIL (`FileNotFoundError`).

- [ ] **Step 3: Create the device-level YAML**

Clone `stage-signal-prism-realtime-attributed-wide-backfill.yaml` and change metadata name, pod prefix, `mainClass`, upload path, and the `spark.app.signalprism.data.realtime_attributed_aggregation.*` block. Key fields:

```yaml
apiVersion: "sparkoperator.k8s.io/v1beta2"
kind: ScheduledSparkApplication
metadata:
  name: stage-signal-prism-agg-device-level-bf
spec:
  schedule: "17 * * * *"
  concurrencyPolicy: "Forbid"
  successfulRunHistoryLimit: 3
  failedRunHistoryLimit: 2
  suspend: false
  template:
    type: Scala
    mode: cluster
    image: "320005014399.dkr.ecr.us-east-1.amazonaws.com/spark:spark356_iceberg192_java17_v1"
    imagePullPolicy: Always
    imagePullSecrets: [ "vungleregistrykey" ]
    mainClass: com.vungle.signalprism.data.realtime_attributed_aggregation.SparkMain
    mainApplicationFile: "s3a://vungle2-dataeng/builds/lena/jars/lena-d643281.jar"
    sparkVersion: "3.5.6"
    restartPolicy:
      type: Never
    hadoopConf:
      fs.s3a.aws.credentials.provider: "com.amazonaws.auth.WebIdentityTokenCredentialsProvider"
    sparkConf:
      spark.sql.shuffle.partitions: "2000"
      spark.sql.adaptive.enabled: "true"
      spark.sql.adaptive.coalescePartitions.enabled: "true"
      spark.serializer: "org.apache.spark.serializer.KryoSerializer"
      spark.kubernetes.executor.podNamePrefix: "sp-agg-devlvl-bf"
      spark.ui.prometheus.enabled: "true"
      spark.submit.deployMode: "cluster"
      spark.driver.extraJavaOptions: "-Divy.cache.dir=/tmp -Divy.home=/tmp"
      spark.kubernetes.file.upload.path: "s3a://vungle2-dataeng/spark-uploads/signalprism/sp-agg-devlvl-bf/"
      spark.sql.catalog.hive_stg.type: "hive"
      spark.sql.catalog.hive_stg.uri: "thrift://stage.hive.vungle.io:9083"
      spark.sql.catalog.hive_stg: "org.apache.iceberg.spark.SparkCatalog"
      spark.sql.extensions: "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
      spark.hadoop.fs.s3a.impl: "org.apache.hadoop.fs.s3a.S3AFileSystem"
      spark.hadoop.fs.s3a.aws.credentials.provider: "com.amazonaws.auth.WebIdentityTokenCredentialsProvider"
      spark.eventLog.enabled: "true"
      spark.eventLog.dir: "s3a://vungle-datalake-logs/spark/lena/prod/eks-data-1a/"
      spark.app.env: "STG"
      spark.app.tier: "v1"
      spark.app.is_backfill: "true"
      spark.app.backfill.hours: "1"
      spark.app.batch_jobs.db.url: "jdbc:postgresql://stage-postgres-de.rds.vungle.io:5432/airflow_stage?user=airflow_stage&password=__PASSWORD__"
      spark.app.signalprism.data.realtime_attributed_aggregation.dimension_family: "device_level_v1"
      spark.app.signalprism.data.realtime_attributed_aggregation.input.tableName: "hive_stg.ml_shadow.realtime_attributed_event_wide"
      spark.app.signalprism.data.realtime_attributed_aggregation.output.tableName: "hive_stg.ml_shadow.realtime_attributed_device_level_hly"
      spark.app.signalprism.data.realtime_attributed_aggregation.output.s3Dir: "s3a://vungle2-dataeng/ml_shadow/realtime_attributed_device_level_hly"
      spark.app.signalprism.data.realtime_attributed_aggregation.test.create.table: "false"
      spark.app.signalprism.data.realtime_attributed_aggregation.sample_rate: "1.0"
      spark.app.signalprism.data.realtime_attributed_aggregation.aggregation_version: "v1_computed_only"
      spark.app.signalprism.data.realtime_attributed_aggregation.till: "2026-06-28 01:00:00"
      spark.lena.s3.client.credential.provider: "WebIdentityTokenCredentialsProvider"
    driver:
      # copy driver/executor/monitoring blocks verbatim from
      # stage-signal-prism-realtime-attributed-wide-backfill.yaml
      cores: 1
      coreLimit: "1"
      coreRequest: "1"
      memory: "8G"
      serviceAccount: "lena-k8s-stage"
    executor:
      cores: 4
      instances: 20
      coreLimit: "4"
      coreRequest: "4"
      memory: "16G"
      serviceAccount: "lena-k8s-stage"
```

> Copy the full `driver:`, `executor:`, and `monitoring:` sections (env, envFrom, configMaps, ports, affinity, tolerations, labels, dnsConfig) verbatim from `stage-signal-prism-realtime-attributed-wide-backfill.yaml`; only the resource counts and the `sparkConf` block above differ. If `test.create.table` is `false`, create the tables first by running the rendered `components/Data/ddl/*_hly.sql` against the stage metastore, or flip `test.create.table` to `true` for the first run with a valid `output.s3Dir`.

- [ ] **Step 4: Create the non-device-context YAML**

Same file, with these substitutions: `metadata.name` → `stage-signal-prism-agg-non-device-context-bf`; `podNamePrefix` → `sp-agg-nondev-bf`; `file.upload.path` → `.../sp-agg-nondev-bf/`; `dimension_family` → `non_device_context_v1`; `output.tableName` → `hive_stg.ml_shadow.realtime_attributed_non_device_context_hly`; `output.s3Dir` → `s3a://vungle2-dataeng/ml_shadow/realtime_attributed_non_device_context_hly`. Everything else identical.

- [ ] **Step 5: Run the structural test**

Run: `cd components/Data/codegen && python -m pytest test_lint_agg_yaml.py -v`
Expected: PASS (1 test).

- [ ] **Step 6: Commit**

```bash
cd /Users/twang/Projects/SignalPrism
git add components/Data/codegen/test_lint_agg_yaml.py
git commit -m "test(data): structural lint for aggregation backfill YAMLs"
cd /Users/twang/Projects/lena
git add cd/lena-test/stage-signal-prism-agg-device-level-backfill.yaml \
        cd/lena-test/stage-signal-prism-agg-non-device-context-backfill.yaml
git commit -m "cd: signal-prism hourly aggregation backfills (device + non-device)"
```

---

### Task 6: Full codegen regen + suite green

Confirm the whole codegen suite (wide table + aggregation) passes together and artifacts are reproducible.

**Files:** none created; runs existing + new tests.

- [ ] **Step 1: Regenerate all artifacts idempotently**

Run: `cd components/Data/codegen && python generate.py && python agg_generate.py && git status --porcelain`
Expected: no unexpected diffs (a second regen produces identical files).

- [ ] **Step 2: Run the entire codegen test suite**

Run: `cd components/Data/codegen && python -m pytest -v`
Expected: all wide-table tests plus the new `test_agg_schema_catalog.py`, `test_agg_generate.py`, `test_validate_agg.py`, `test_lint_agg_job.py`, `test_lint_agg_yaml.py` PASS.

- [ ] **Step 3: Run both validators**

Run: `cd components/Data/codegen && python validate.py && python validate_agg.py`
Expected: both print their `OK:` line and exit 0.

- [ ] **Step 4: Commit any regen diffs**

```bash
git add -A components/Data
git commit -m "chore(codegen): regenerate aggregation artifacts; full suite green" || echo "nothing to commit"
```

---

## Self-Review

**Spec coverage:**
- §2 concrete/contract-driven → Tasks 1–2 (codegen from md). ✓
- §2 both hourly tables → Tasks 2 (DDL/specs both families) + 5 (two backfills). ✓
- §2 full-contract + null-fill → Task 2 DDL carries all cols; Task 4 emits only `computed`, relies on `appendToIcebergTable`. ✓
- §2 predicate-dependent emit null / no guessing → Task 1 `PREDICATE_DEPENDENT`; Task 4 lint forbids `moloco`; `aggregation_version` literal. ✓
- §2 top-N deferred → Task 4 `bucket` → `lower(trim(...))`. ✓
- §2 sampling / partitioning / no PII → Task 4 `in_user_sample`; Task 2 partition clause; Tasks 1/3/4 PII checks. ✓
- §4.1 parser (five table shapes, section-aware) → Task 1. ✓
- §4.2 generators (DDL + agg_specs + metric_catalog + rendered ddl) → Task 2. ✓
- §4.3 one parametrized job (dims, surrogate key, hashid, audit, drop null device) → Task 4. ✓
- §4.4 two data-cd YAMLs → Task 5. ✓
- §4.5 codegen tests + Scala lint → Tasks 1–4 tests. ✓
- §6 metric classification → Task 1 constants + Task 5-step-5 verification. ✓
- §8 verify base columns exist / won-loss caveat → Task 4 Step 5. ✓

**Placeholder scan:** No TBD/TODO; every code step has complete code; the one unused `innerSql` val is called out explicitly with removal guidance.

**Type consistency:** `AggMetric`/`AggDim`/`SharedCol` fields are used identically across Tasks 1–3; `family_spec`/`metric_catalog_spec` shapes match the Scala reader in Task 4 (`primary_key.name`, `dimensions[].{name,source_col,fallback_col,norm}`, `distribution[].{family,kind,base_expr}`, `count[].{name,kind,predicate}`); table/location strings match between Task 2 `TABLE_LOCATION`, Task 5 YAMLs, and Task 5 test `OUT`.

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-07-01-realtime-attributed-aggregation-pipeline.md`. Two execution options:

1. **Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration.
2. **Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints for review.

Which approach?
