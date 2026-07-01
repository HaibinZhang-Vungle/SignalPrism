import agg_generate as g
from agg_schema_catalog import parse_metrics

def test_ddl_has_shared_dims_and_all_metric_columns():
    ddl = g.agg_ddl_template("non_device_context_v1")
    assert "CREATE TABLE IF NOT EXISTS ?table?" in ddl
    assert "event_time timestamp" in ddl
    assert "ingest_time string" in ddl
    assert "hashid string" in ddl
    assert "context_dim_id string" in ddl
    assert "supply_name string" in ddl
    # every metric column appears (computed AND null-filled)
    assert "min_bid_to_win_sum double" in ddl
    assert "min_bid_to_win_count bigint" in ddl
    assert "net_revenue_sum double" in ddl            # absent-source, still in contract
    assert "vx_min_bid_to_win_squaresum double" in ddl  # predicate-dep, still in contract
    assert "delivery_count bigint" in ddl
    assert "USING iceberg" in ddl
    assert "PARTITIONED BY (hours(event_time), ingest_time, hashid)" in ddl
    assert 'LOCATION "?location?"' in ddl

def test_device_ddl_has_device_key_not_context_key():
    ddl = g.agg_ddl_template("device_level_v1")
    assert "device_id string" in ddl
    assert "device_dim_id string" in ddl
    assert "context_dim_id" not in ddl

def test_family_spec_shape():
    spec = g.family_spec("non_device_context_v1")
    assert spec["dimension_family"] == "non_device_context_v1"
    assert spec["primary_key"]["name"] == "context_dim_id"
    assert spec["hashid_from"] == "context_dim_id"
    assert spec["drop_null_source"] is None
    names = {d["name"] for d in spec["dimensions"]}
    assert "supply_name" in names and "context_dim_id" not in names  # surrogate excluded from dim list
    geo = next(d for d in spec["dimensions"] if d["name"] == "geoip_country_code")
    assert geo["norm"] == "coalesce" and geo["fallback_col"] == "jgr_geo_country"

def test_device_spec_keys_on_normalized_id():
    # device_id keys on normalize_device_id(jgr_dev_normalized_id) (jgr_lo_id is empty upstream),
    # is a real runtime dimension again, and drops rows whose normalized device id is null.
    spec = g.family_spec("device_level_v1")
    assert spec["primary_key"]["name"] == "device_dim_id"
    assert spec["drop_null_source"] == "normalize_device_id(jgr_dev_normalized_id)"
    dims = {d["name"]: d for d in spec["dimensions"]}
    assert dims["device_id"]["source_col"] == "normalize_device_id(jgr_dev_normalized_id)"
    assert dims["device_id"]["norm"] == "expr"
    assert "device_id string" in g.agg_ddl_template("device_level_v1")

def test_metric_catalog_only_marks_computed_with_exprs():
    cat = g.metric_catalog_spec()
    dist = {m["family"]: m for m in cat["distribution"]}
    assert dist["min_bid_to_win"]["kind"] == "computed"
    assert dist["min_bid_to_win"]["base_expr"] == "jgr_min_bid_to_win"
    assert dist["net_revenue"]["kind"] == "null_absent_source"
    assert dist["net_revenue"]["base_expr"] is None
    counts = {m["name"]: m for m in cat["count"]}
    assert counts["delivery_count"]["predicate"] == "jgr_no_serv_reason = 0"
    assert counts["no_bid_count"]["predicate"] is None
