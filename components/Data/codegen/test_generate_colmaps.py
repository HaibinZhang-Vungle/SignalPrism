import os
from schema_catalog import parse_catalog
from generate import jaeger_col_map, hb_col_map

MD = os.path.join(os.path.dirname(__file__), "..", "..", "..",
                  "schemas", "realtime_attributed_wide_table_schema.md")
COLS = parse_catalog(MD)

def test_jaeger_col_map_maps_source_expr_to_target():
    m = jaeger_col_map(COLS)
    assert m["serve_result.bid_floor"] == "jgr_bid_floor"
    assert m["device.make"] == "jgr_dev_make"
    assert m["placement_.floor"] == "jgr_placement_floor"

def test_hb_col_map():
    m = hb_col_map(COLS)
    assert m["adx_bid_price"] == "hbn_adx_bid_price"

def test_dropped_hb_dups_absent_from_hb_map():
    # §3.1: HB floor/device/geo copies are NOT carried.
    targets = set(hb_col_map(COLS).values())
    assert not any(t.startswith("hbn_dev_") for t in targets)
    assert "hbn_edsp_floor" not in targets
