from walleye.workflow_revisions import Revisions


def record(revisions, number, distance):
    change = {"before": 10, "after": 10 - distance, "delta": -distance}
    card = {
        "maintainability": {
            "target_quality": change,
            "region": {"quality": change},
            "repository": {"score": change},
        }
    }
    metrics = {"passed": False, "failures": ["Quality regression"], "violation_distance": distance}
    return revisions.record(number, {"replacement": str(number)}, card, metrics, {}, "artifacts")


def test_metric_progress_allows_more_than_three_attempts_and_preserves_the_best():
    revisions = Revisions()
    for number, distance in enumerate([10, 9, 8, 7, 6], 1):
        assert record(revisions, number, distance)
        assert not revisions.exhausted
    assert not record(revisions, 6, 9)
    feedback = revisions.feedback()
    assert feedback["best_candidate"]["attempt"] == 5
    assert feedback["last_attempt"]["attempt"] == 6
    assert len(feedback["history"]) == 6
    assert not record(revisions, 7, 9)
    assert not record(revisions, 8, 8)
    assert revisions.exhausted


def test_unchanged_candidates_stop_without_repeating_measurements():
    revisions = Revisions()
    assert not revisions.repeated("original")
    for _ in range(3):
        assert revisions.repeated("original")
    assert revisions.exhausted


def test_passing_more_project_checks_counts_as_progress():
    revisions = Revisions()
    for number in range(4):
        metrics = {
            "passed": False,
            "failures": ["Project checks must pass"],
            "checks": [{"exit_code": 0, "command": ["check"]}] * number,
        }
        assert revisions.record(number + 1, {}, None, metrics, {}, "artifacts")
    assert not revisions.exhausted
