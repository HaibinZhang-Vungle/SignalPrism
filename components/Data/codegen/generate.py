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


if __name__ == "__main__":
    write_columns()
    write_col_maps()
    print("columns + col_maps written")
