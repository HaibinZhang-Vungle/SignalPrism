from validate_agg import validate_agg

def test_generated_artifacts_are_consistent():
    # assumes `python agg_generate.py` has been run (Task 2 step 4)
    assert validate_agg() == []
