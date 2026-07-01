import os
from schema_catalog import parse_catalog
from generate import jaeger_columns, hb_columns, all_columns

MD = os.path.join(os.path.dirname(__file__), "..", "..", "..",
                  "schemas", "realtime_attributed_wide_table_schema.md")
COLS = parse_catalog(MD)

def test_jaeger_columns_include_keys_and_jgr_and_anomaly():
    jc = set(jaeger_columns(COLS))
    assert "event_id" in jc and "imp_id" in jc
    assert "jgr_bid_floor" in jc
    assert "source_event_time" in jc
    assert "hbn_mediation_tmax" in jc          # jaeger-sourced anomaly
    assert "hbn_adx_bid_price" not in jc        # belongs to hb

def test_hb_columns_include_keys_and_hbn():
    hc = set(hb_columns(COLS))
    assert "event_id" in hc and "imp_id" in hc
    assert "hbn_adx_bid_price" in hc
    assert "jgr_bid_floor" not in hc

def test_all_columns_is_union_and_unique():
    ac = all_columns(COLS)
    assert len(ac) == len(set(ac))               # no dups
    assert set(ac) >= set(jaeger_columns(COLS)) | set(hb_columns(COLS))
