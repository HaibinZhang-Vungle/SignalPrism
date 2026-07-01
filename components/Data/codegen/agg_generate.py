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
# device_id keys on the SDK normalized id (agg_schema_catalog._DERIVED_DIM_EXPR); drop rows
# whose normalized device id is null, matching lena's device-feature `WHERE dev_id IS NOT NULL`.
# jgr_lo_id is empty upstream, so it is not used.
_DROP_NULL = {
    "device_level_v1": "normalize_device_id(jgr_dev_normalized_id)",
    "non_device_context_v1": None,
}

# Dimensions kept in the CONTRACT/DDL but deferred from the RUNTIME dimension set. Empty now
# that device_id keys on a populated source; kept as a hook for any future deferral.
_DEFERRED_DIMS = {"device_level_v1": set(), "non_device_context_v1": set()}


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
    deferred = _DEFERRED_DIMS.get(family, set())
    dims = [d for d in parse_dims(family)
            if d.role != "surrogate_key" and d.name not in deferred]
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
