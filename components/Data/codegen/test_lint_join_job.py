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
    assert "j.imp_id <=> h.imp_id" in s   # null-safe equality (lena notifications_attribution idiom)
    # Hit-rate metric per schema §2.3.
    assert "attribution_hit_rate" in s
    assert "reportStatsMetric" in s
    assert "mergeToIcebergTable" in s
    assert 'mergeKeysAllowNull = Array("imp_id")' in s
    # Two-upstream watermark gating (pattern from notifications_attribution).
    assert "checkoutProgress" in s
