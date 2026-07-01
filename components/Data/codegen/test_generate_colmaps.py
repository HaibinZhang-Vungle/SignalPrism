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

def test_winner_account_id_special_cased_out_of_jaeger_col_map():
    m = jaeger_col_map(COLS)
    # jgr_winner_account_id shares source expr rtb_conn.account_id with jgr_rtb_account_id,
    # so it is excluded from the col_map and emitted directly in the jaeger SELECT.
    assert "jgr_winner_account_id" not in m.values()
    # jgr_rtb_account_id keeps the rtb_conn.account_id mapping.
    assert m["rtb_conn.account_id"] == "jgr_rtb_account_id"
