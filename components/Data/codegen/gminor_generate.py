import json, os
from gminor_schema_catalog import (GMINOR_BASE_COLS, LABEL_COLS, WIDE_DDL, DEVICE_DDL,
                                    CONTEXT_DDL, AGG_NON_METRIC)

HERE = os.path.dirname(__file__)
RES = os.path.join(HERE, "..", "src", "main", "resources")
DDL_DIR = os.path.join(HERE, "..", "ddl")
OUT = "gminor_attributed_training"
TABLE = "hive_stg.ml_shadow." + OUT
LOCATION = "s3a://vungle2-dataeng/ml_shadow/" + OUT

def _ddl_cols(path):
    body = open(os.path.join(HERE, path)).read()
    body = body[body.index("(") + 1: body.rindex(")")]
    out = []
    for line in body.splitlines():
        line = line.strip().rstrip(",")
        if not line or line.split()[0] in ("USING","PARTITIONED","LOCATION","TBLPROPERTIES","'sort-order'"):
            continue
        parts = line.split(None, 1)
        if len(parts) == 2:
            out.append((parts[0], parts[1].lower()))
    return out

def _wide_type(name):
    for n, t in _ddl_cols(WIDE_DDL):
        if n == name:
            return t
    raise KeyError(name)

def _agg_metrics(path):
    return [(n, t) for n, t in _ddl_cols(path) if n not in AGG_NON_METRIC]

def build_output_columns():
    cols = list(GMINOR_BASE_COLS)
    cols.append(("wide_join_hit", "boolean"))
    for c in LABEL_COLS:
        cols.append(("lbl_" + c, _wide_type(c)))
    for n, t in _agg_metrics(DEVICE_DDL):
        cols.append(("dl_" + n, t))
    cols += [("dl_agg_hit", "boolean"), ("dl_agg_event_time", "timestamp")]
    for n, t in _agg_metrics(CONTEXT_DDL):
        cols.append(("ndc_" + n, t))
    cols += [("ndc_agg_hit", "boolean"), ("ndc_agg_event_time", "timestamp")]
    return cols

def _render_ddl(table, location):
    body = ",\n".join("    %s %s" % (n, t) for n, t in build_output_columns())
    return ("CREATE TABLE IF NOT EXISTS %s (\n" % table + body + "\n)\n"
            "USING iceberg\n"
            "PARTITIONED BY (hours(source_event_time))\n"
            'LOCATION "%s"\n'
            "TBLPROPERTIES (\n  'sort-order' = 'source_event_time ASC NULLS FIRST, event_id ASC NULLS FIRST'\n)\n"
            % location)

def _write(path, text):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    open(path, "w").write(text)

def write_all():
    _write(os.path.join(DDL_DIR, OUT + ".sql"), _render_ddl(TABLE, LOCATION))
    _write(os.path.join(RES, "sql", OUT + ".template"), _render_ddl("?table?", "?location?"))
    _write(os.path.join(RES, "columns", OUT + ".json"),
           json.dumps([n for n, _ in build_output_columns()], indent=2) + "\n")

if __name__ == "__main__":
    write_all(); print("gminor: ddl + template + columns written")
