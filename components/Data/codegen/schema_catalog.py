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
