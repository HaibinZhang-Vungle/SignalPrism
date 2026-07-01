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
    """Advisory check: report col_map source leaves absent from the coba schema YAMLs.

    The coba YAMLs (automation/schemas/*.yaml) are an INCOMPLETE declaration of the
    actual coba2 parquet topic schema — some real fields that exist in live coba2/Trino
    are simply absent from the YAML files.  A flagged leaf therefore means
    "not found in the YAML — cross-check against live coba2 (Trino) before assuming a
    real gap"; it does NOT mean the field is missing from the actual data.

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


if __name__ == "__main__":
    problems = validate()
    advisories = check_against_coba_yaml()
    if advisories:
        print("ADVISORY: %d col_map source leaf(es) not found in the coba schema YAMLs." % len(advisories))
        print("The YAMLs are an INCOMPLETE declaration of the coba2 topic schema, so this is a")
        print("heads-up, not a failure — cross-check each against live coba2 (Trino) before")
        print("treating it as a real gap:")
        for a in advisories:
            print("  - " + a)
    if problems:
        print("\n".join(problems))
        sys.exit(1)
    print("OK: all artifacts consistent with schema md")
