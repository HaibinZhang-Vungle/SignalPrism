# GMinor base columns (from schemas/gminor_log_schema.md §2 + §3 derived), plus the two keys.
GMINOR_BASE_COLS = [
    ("event_id", "string"), ("project_name", "string"), ("experiment_id", "bigint"),
    ("project_experiment_key", "string"),
    ("source_event_time", "timestamp"), ("event_hour", "timestamp"),
    ("traffic_allocation", "double"), ("downsampling_rate", "double"), ("sample_weight", "double"),
    ("feature_schema_version", "bigint"), ("version", "string"), ("cloud_provider", "string"),
    ("device_id", "string"), ("lo_id", "string"),
    ("features", "string"), ("predictions", "string"),
    ("device_dim_id", "string"), ("context_dim_id", "string"),
]

# Wide-table label/outcome columns to surface (schema §7 leak_risk/label-adjacent set). Surfaced as lbl_*.
LABEL_COLS = [
    "jgr_settlement_price", "jgr_settlement_status", "jgr_winning_bid_price",
    "jgr_min_bid_to_win", "jgr_second_place_price", "jgr_no_serv_reason",
    "jgr_winner_predicted_nr", "jgr_vungle_price", "jgr_is_winner_acc",
]

WIDE_DDL = "../ddl/realtime_attributed_event_wide.sql"
DEVICE_DDL = "../ddl/realtime_attributed_device_level_hly.sql"
CONTEXT_DDL = "../ddl/realtime_attributed_non_device_context_hly.sql"

# Aggregate rows carry these non-metric columns; exclude from the prefixed metric copy.
AGG_NON_METRIC = {
    "event_time", "ingest_time", "hashid", "device_dim_id", "context_dim_id", "device_id",
    "source_event_count", "first_source_event_time", "last_source_event_time", "aggregation_version",
}
