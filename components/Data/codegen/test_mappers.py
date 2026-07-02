import os
from schema_catalog import parse_catalog, sql_type, source_expr

MD = os.path.join(os.path.dirname(__file__), "..", "..", "..",
                  "schemas", "realtime_attributed_wide_table_schema.md")
BY = {c.name: c for c in parse_catalog(MD)}

def test_sql_type():
    assert sql_type("DOUBLE") == "double"
    assert sql_type("STRING") == "string"
    assert sql_type("LONG") == "bigint"
    assert sql_type("INT") == "int"
    assert sql_type("BOOLEAN") == "boolean"
    assert sql_type("TIMESTAMP") == "timestamp"
    assert sql_type("ARRAY<STRING>") == "array<string>"

def test_source_expr_rewrites_explode_aliases():
    # placement_serve_results[].X -> serve_result.X
    assert source_expr(BY["jgr_bid_floor"], "jaeger") == "serve_result.bid_floor"
    # placements[].X -> placement_.X
    assert source_expr(BY["jgr_placement_floor"], "jaeger") == "placement_.floor"
    # device.X stays
    assert source_expr(BY["jgr_dev_make"], "jaeger") == "device.make"
    # hb side strips hb.
    assert source_expr(BY["hbn_adx_bid_price"], "hb") == "adx_bid_price"

def test_source_expr_keys_are_staging_aware():
    assert source_expr(BY["event_id"], "jaeger") == "serve_result.ad_event_id"
    assert source_expr(BY["event_id"], "hb") == "event_id"
    assert source_expr(BY["imp_id"], "jaeger") == "serve_result.imp_id"
    assert source_expr(BY["imp_id"], "hb") == "bidrequest_imp_id"
    assert source_expr(BY["source_event_time"], "jaeger") == "timestamp"

def test_source_expr_winning_rtbconnection():
    # placement_serve_results[].rtbconnections[].X (winning) -> rtb_conn.X
    assert source_expr(BY["jgr_rtb_connection_id"], "jaeger") == "rtb_conn.id"
    assert source_expr(BY["jgr_rtb_account_id"], "jaeger") == "rtb_conn.account_id"
    assert source_expr(BY["jgr_rtb_is_internal"], "jaeger") == "rtb_conn.is_internal"
    # derived winner account id
    assert source_expr(BY["jgr_winner_account_id"], "jaeger") == "rtb_conn.account_id"

def test_source_expr_strips_array_notation():
    # array-valued leaf columns must NOT keep the schema-md `[]` notation
    assert source_expr(BY["hbn_pub_genre"], "hb") == "pub_genre"
    assert source_expr(BY["hbn_adv_genre"], "hb") == "adv_genre"
    assert source_expr(BY["jgr_app_cat"], "jaeger") == "app.cat"
    assert source_expr(BY["jgr_winning_bid_adomain"], "jaeger") == "serve_result.winning_bid.adomain"
    assert source_expr(BY["jgr_tpat_video_start"], "jaeger") == "serve_result.tpat.video_start"
    for c in parse_catalog(MD):
        for st in ("jaeger", "hb"):
            assert "[]" not in source_expr(c, st), (c.name, st)
