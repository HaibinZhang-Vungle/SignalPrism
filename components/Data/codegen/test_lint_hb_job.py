import os

SCALA = os.path.join(os.path.dirname(__file__), "..", "src", "main", "scala",
                     "com", "vungle", "signalprism", "data",
                     "hb_transactions_ingestion", "SparkMain.scala")

def _read():
    with open(SCALA, encoding="utf-8") as fh:
        return fh.read()

def test_hb_job_contract():
    s = _read()
    assert "object SparkMain extends BoilerplateSparkMain" in s
    # coba2 read (NOT a Kafka consumer).
    assert "withCoba2TempViewInRange" in s
    assert "saveKafkaTopic" not in s
    assert "in_user_sample(sha1(event_id)" in s
    # Keep only the served/winning bid via row_number dedup.
    assert "row_number() OVER" in s
    assert "PARTITION BY event_id, bidrequest_imp_id" in s
    assert "getColsMapInJson" in s
    assert "col_maps/hb_transactions_wide.json" in s
    assert "mergeToIcebergTable" in s or "appendToIcebergTable" in s
    assert "spark.udf.register" not in s
