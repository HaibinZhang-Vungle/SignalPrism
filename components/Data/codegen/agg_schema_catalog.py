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

# Dimensions with no single source column: a reviewed derived expression (emitted verbatim).
# device_id is keyed on the SDK normalized id (jgr_lo_id is empty upstream), normalized the
# same way lena's device-feature pipelines do (normalize_device_id: trim/lowercase, nil-UUID->null).
_DERIVED_DIM_EXPR = {
    "source_has_hb": "hbn_bidrequest_id IS NOT NULL",
    "device_id": "normalize_device_id(jgr_dev_normalized_id)",
}

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
    in_table = False
    for heading, cells in _rows(md_path):
        if not heading.startswith("1."):
            continue
        header = [c.lower() for c in cells]
        # Gate: only start collecting rows after we see the shared-columns header
        if header[:4] == ["column", "type", "role", "description"]:
            in_table = True
            continue
        # Only append data rows if we've seen the header and it's not a header/skip row
        if in_table and len(cells) >= 4 and cells[0] not in ("column", "table"):
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
        if name in _DERIVED_DIM_EXPR:
            norm, src, fallback = "expr", _DERIVED_DIM_EXPR[name], None
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
