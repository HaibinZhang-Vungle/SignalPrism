import os

SCALA = os.path.join(os.path.dirname(__file__), "..", "src", "main", "scala",
                     "com", "vungle", "signalprism", "data",
                     "realtime_attributed_wide", "SparkMain.scala")

def _read():
    with open(SCALA, encoding="utf-8") as fh:
        return fh.read()

def test_join_contract():
    s = _read()
    assert "object SparkMain extends BoilerplateSparkMain" in s
    assert "LEFT JOIN" in s
    assert "j.event_id = h.event_id" in s
    # hb has no impression-id column, so the join is on event_id only; imp_id comes from jaeger.
    # Hit-rate metric per schema §2.3.
    assert "attribution_hit_rate" in s
    assert "reportStatsMetric" in s
    assert "appendToIcebergTable" in s
    # Two-upstream watermark gating (pattern from notifications_attribution).
    assert "checkoutProgress" in s
