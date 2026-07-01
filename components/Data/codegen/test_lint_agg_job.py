import os
SCALA = os.path.join(os.path.dirname(__file__), "..", "src", "main", "scala",
                     "com", "vungle", "signalprism", "data",
                     "realtime_attributed_aggregation", "SparkMain.scala")

def _read():
    with open(SCALA, encoding="utf-8") as fh:
        return fh.read()

def test_boilerplate_and_required_machinery():
    s = _read()
    assert "object SparkMain extends BoilerplateSparkMain" in s
    assert "def requiredArgs" in s and "def defaultArgs" in s and "def run" in s
    # one parametrized job selected by dimension_family
    assert "dimension_family" in s
    # reads the codegen'd specs from resources (not hardcoded columns)
    assert "agg_specs/" in s
    assert "metric_catalog.json" in s
    assert "getResourceString" in s or "getColsMapInJson" in s or "getColsInJson" in s
    # deterministic event-id sample, hourly grouping, surrogate key, write
    assert "in_user_sample" in s
    assert "date_trunc('HOUR'" in s or "hours(" in s
    assert "sha2(" in s
    assert "GROUP BY" in s
    assert "appendToIcebergTable" in s
    # audit + versioning present
    assert "source_event_count" in s
    assert "aggregation_version" in s

def test_no_pii_and_no_predicate_guessing():
    s = _read()
    for pii in ("jgr_dev_ifa", "jgr_dev_ip", "jgr_dev_ua"):
        assert pii not in s
    assert "spark.udf.register" not in s
    # predicate-dependent metrics must not be silently computed with a guessed filter
    assert "moloco" not in s.lower()
