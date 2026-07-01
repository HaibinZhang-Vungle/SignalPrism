import os
CD = "/Users/twang/Projects/lena/cd/lena-test"
YAMLS = {
    "device_level_v1": os.path.join(CD, "stage-signal-prism-agg-device-level-backfill.yaml"),
    "non_device_context_v1": os.path.join(CD, "stage-signal-prism-agg-non-device-context-backfill.yaml"),
}
OUT = {
    "device_level_v1": "hive_stg.ml_shadow_feature.realtime_attributed_device_level_hly",
    "non_device_context_v1": "hive_stg.ml_shadow_feature.realtime_attributed_non_device_context_hly",
}

def test_yamls_target_the_one_job_and_correct_family():
    for fam, path in YAMLS.items():
        s = open(path, encoding="utf-8").read()
        assert "com.vungle.signalprism.data.realtime_attributed_aggregation.SparkMain" in s
        assert (".dimension_family: \"%s\"" % fam) in s
        assert OUT[fam] in s
        assert "hive_stg.ml_shadow.realtime_attributed_event_wide" in s  # input
        assert ".sample_rate: \"1.0\"" in s
        assert ".till:" in s
