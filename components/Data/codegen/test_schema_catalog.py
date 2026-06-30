import os
from schema_catalog import parse_catalog, assign_source, Column

MD = os.path.join(os.path.dirname(__file__), "..", "..", "..",
                  "schemas", "realtime_attributed_wide_table_schema.md")

def test_parses_full_catalog():
    cols = parse_catalog(MD)
    names = {c.name for c in cols}
    # Spot-check representative columns from different sections.
    assert "event_id" in names
    assert "imp_id" in names
    assert "jgr_bid_floor" in names
    assert "hbn_adx_bid_price" in names
    assert "source_event_time" in names
    assert "jgr_winner_account_id" in names
    # Schema §9 states ~150 columns.
    assert len(cols) >= 140

def test_types_captured():
    by_name = {c.name: c for c in parse_catalog(MD)}
    assert by_name["jgr_bid_floor"].type == "DOUBLE"
    assert by_name["event_id"].type == "STRING"
    assert by_name["jgr_app_cat"].type.startswith("ARRAY")

def test_source_assignment_is_by_source_not_prefix():
    by_name = {c.name: c for c in parse_catalog(MD)}
    assert assign_source(by_name["jgr_bid_floor"]) == "jaeger"
    assert assign_source(by_name["hbn_adx_bid_price"]) == "hb"
    assert assign_source(by_name["event_id"]) == "key"
    # Anomaly: hbn_ name but jaeger source.
    assert assign_source(by_name["hbn_mediation_tmax"]) == "jaeger"
