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
