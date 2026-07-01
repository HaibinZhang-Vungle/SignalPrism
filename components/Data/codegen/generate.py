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


def _col_map(cols, staging, want_names):
    # jgr_winner_account_id shares source expr rtb_conn.account_id with jgr_rtb_account_id;
    # emitted directly in the jaeger SELECT, not via col_map
    keys = ("event_id", "imp_id", "jgr_winner_account_id")  # emitted directly by the job, not via col_map
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


if __name__ == "__main__":
    write_columns()
    write_col_maps()
    write_ddl()
    print("columns + col_maps + ddl written")
