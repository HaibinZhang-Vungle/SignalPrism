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
