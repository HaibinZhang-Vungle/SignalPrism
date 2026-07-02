import gminor_generate as g

def test_output_has_gminor_base_keys_labels_and_prefixed_aggs():
    cols = dict(g.build_output_columns())
    # gminor base + derived
    for c in ["event_id","project_name","experiment_id","source_event_time","event_hour",
              "sample_weight","device_id","lo_id","device_dim_id","context_dim_id"]:
        assert c in cols, f"missing {c}"
    # labels prefixed lbl_
    assert "lbl_jgr_settlement_price" in cols
    assert "lbl_jgr_no_serv_reason" in cols
    # every device_level metric appears once, prefixed dl_
    assert "dl_min_bid_to_win_sum" in cols and "dl_delivery_count" in cols
    assert "dl_agg_hit" in cols and cols["dl_agg_hit"] == "boolean"
    assert "dl_agg_event_time" in cols and cols["dl_agg_event_time"] == "timestamp"
    # every non_device metric appears once, prefixed ndc_
    assert "ndc_min_bid_to_win_sum" in cols
    assert "ndc_agg_hit" in cols and "ndc_agg_event_time" in cols
    # no raw (unprefixed) agg metric leaked
    assert "min_bid_to_win_sum" not in cols

def test_no_duplicate_columns():
    names = [n for n, _ in g.build_output_columns()]
    assert len(names) == len(set(names)), "duplicate output columns"
