import os
from schema_catalog import parse_catalog
from generate import ddl_template, all_columns

MD = os.path.join(os.path.dirname(__file__), "..", "..", "..",
                  "schemas", "realtime_attributed_wide_table_schema.md")
COLS = parse_catalog(MD)

def test_ddl_has_typed_columns_and_partition():
    ddl = ddl_template(COLS, all_columns(COLS))
    assert "CREATE TABLE IF NOT EXISTS ?table?" in ddl
    assert "event_id string" in ddl
    assert "jgr_bid_floor double" in ddl
    assert "jgr_app_cat array<string>" in ddl
    assert "USING iceberg" in ddl
    assert "PARTITIONED BY (hours(source_event_time))" in ddl
    assert 'LOCATION "?location?"' in ddl

def test_every_wide_column_appears_in_ddl():
    ddl = ddl_template(COLS, all_columns(COLS))
    for name in all_columns(COLS):
        assert ("\n    %s " % name) in ddl, "missing column in DDL: %s" % name
