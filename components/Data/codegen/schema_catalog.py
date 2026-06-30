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
