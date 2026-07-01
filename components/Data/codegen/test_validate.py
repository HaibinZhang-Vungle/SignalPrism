from validate import validate

def test_generated_artifacts_are_valid():
    problems = validate()
    assert problems == [], "validation problems:\n" + "\n".join(problems)
