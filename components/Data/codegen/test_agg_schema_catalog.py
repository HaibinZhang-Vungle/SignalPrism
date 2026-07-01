import os
from agg_schema_catalog import (
    parse_shared_columns, parse_dims, parse_metrics, classify,
    agg_sql_type, ABSENT_SOURCE, PREDICATE_DEPENDENT,
)

def test_shared_columns():
    shared = {c.name: c for c in parse_shared_columns()}
    assert shared["event_time"].type == "TIMESTAMP" and shared["event_time"].role == "time_key"
    assert shared["ingest_time"].role == "partition_key"
    assert shared["hashid"].role == "partition_key"
    assert "source_event_count" in shared and "aggregation_version" in shared

def test_non_device_dims_present_and_no_pii_no_ids():
    dims = {d.name: d for d in parse_dims("non_device_context_v1")}
    # representative dims
    assert dims["supply_name"].source_col == "hbn_supply_name"
    assert dims["placement_id"].source_col == "jgr_placement_id"
    assert dims["geoip_country_code"].norm == "coalesce"
    assert dims["geoip_country_code"].fallback_col == "jgr_geo_country"
    assert dims["app_version_major"].norm == "parse_major"
    assert dims["context_dim_id"].role == "surrogate_key"
    # no event/bid identifiers leak in as dimensions
    for banned in ("event_id", "imp_id", "jgr_auction_id", "winner_id", "rtb_connection_id"):
        assert banned not in dims

def test_device_dims_and_bucket_norm():
    dims = {d.name: d for d in parse_dims("device_level_v1")}
    # device_id keys on the SDK normalized id (jgr_lo_id is empty upstream), normalized like lena
    assert dims["device_id"].source_col == "normalize_device_id(jgr_dev_normalized_id)"
    assert dims["device_id"].norm == "expr"
    assert dims["device_dim_id"].role == "surrogate_key"
    assert dims["dev_model_bucket"].norm == "bucket"
    assert dims["dev_platform"].norm == "normalize"
    # PII excluded
    for pii in ("jgr_dev_ifa", "jgr_dev_ip", "jgr_dev_ua"):
        assert pii not in {d.source_col for d in dims.values()}

def test_distribution_expands_to_five_typed_columns():
    metrics = {m.name: m for m in parse_metrics()}
    m = metrics["min_bid_to_win"]
    assert m.columns == ["min_bid_to_win_sum", "min_bid_to_win_count",
                         "min_bid_to_win_min", "min_bid_to_win_max",
                         "min_bid_to_win_squaresum"]
    assert m.col_types == ["double", "bigint", "double", "double", "double"]
    assert m.base_expr == "jgr_min_bid_to_win"
    assert m.kind == "computed"

def test_count_metric_and_classification():
    metrics = {m.name: m for m in parse_metrics()}
    assert metrics["delivery_count"].kind == "computed"
    assert metrics["delivery_count"].columns == ["delivery_count"]
    assert metrics["delivery_count"].col_types == ["bigint"]
    assert metrics["hb_bid_count"].col_types == ["double"]
    # classification matches spec §6
    assert classify("net_revenue") == "null_absent_source"
    assert classify("mediation_win_count") == "null_absent_source"
    assert classify("vx_min_bid_to_win") == "null_predicate_dependent"
    assert classify("bid_price_moloco") == "null_predicate_dependent"
    assert classify("settlement_price") == "computed"

def test_type_mapping():
    assert agg_sql_type("BIGINT") == "bigint"
    assert agg_sql_type("LONG") == "bigint"
    assert agg_sql_type("TIMESTAMP") == "timestamp"
    assert agg_sql_type("BOOLEAN") == "boolean"

def test_source_has_hb_is_derived_expr():
    dims = {d.name: d for d in parse_dims("non_device_context_v1")}
    d = dims["source_has_hb"]
    assert d.norm == "expr"
    assert d.source_col == "hbn_bidrequest_id IS NOT NULL"
    assert d.fallback_col is None

def test_shared_columns_excludes_table_listing():
    shared = parse_shared_columns()
    assert len(shared) == 7
    # the §1 table-listing rows (fully-qualified table names) must not leak in
    assert all("." not in c.name for c in shared)
    assert {c.name for c in shared} == {
        "event_time", "ingest_time", "hashid", "source_event_count",
        "first_source_event_time", "last_source_event_time", "aggregation_version",
    }
