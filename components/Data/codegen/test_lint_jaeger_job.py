import os

SCALA = os.path.join(os.path.dirname(__file__), "..", "src", "main", "scala",
                     "com", "vungle", "signalprism", "data",
                     "jaeger_transaction_ingestion", "SparkMain.scala")

def _read():
    with open(SCALA, encoding="utf-8") as fh:
        return fh.read()

def test_extends_boilerplate_and_uses_required_machinery():
    s = _read()
    assert "object SparkMain extends BoilerplateSparkMain" in s
    assert "def requiredArgs" in s and "def defaultArgs" in s and "def run" in s
    # coba2 read (NOT a Kafka consumer) + sample + multi-stage explode + projection + write.
    assert "withCoba2TempViewInRange" in s
    assert "saveKafkaTopic" not in s          # must not re-consume Kafka
    assert "in_user_sample(sha1(serve_result.ad_event_id)" in s
    assert "explode(placement_serve_results)" in s
    assert "explode(placements)" in s
    assert "explode(serve_result.rtbconnections)" in s
    assert "serve_result.winner_id = rtb_conn.id" in s
    assert "getColsMapInJson" in s
    assert "col_maps/jaeger_transaction_wide.json" in s
    # jgr_winner_account_id is special-cased (shares source expr with jgr_rtb_account_id),
    # emitted directly rather than via the col_map.
    assert "rtb_conn.account_id AS jgr_winner_account_id" in s
    assert "mergeToIcebergTable" in s

def test_no_adhoc_udf_registration():
    s = _read()
    # All UDFs come from UDFUtil; no inline spark.udf.register(...).
    assert "spark.udf.register" not in s
