from walleye.workflow_revisions import Revisions


def test_cardless_record_appends_history_and_tracks_stalling():
    revisions = Revisions()
    metrics = {
        "tests": {"passed": 0},
        "frozen": {"cases": []},
        "checks": [],
    }

    try:
        first = revisions.record(1, None, None, metrics, {}, [])
    except KeyError as error:
        raise AssertionError(
            "card-less records should append history without top-level metrics"
        ) from error

    assert first is True
    assert revisions.stalled == 0
    assert len(revisions.history) == 1
    first_entry = revisions.history[0]
    assert first_entry["status"] == "evaluated"
    assert first_entry["progress"] is True
    assert first_entry["metrics_passed"] == 0
    assert first_entry["failures"] == []

    second = revisions.record(2, None, None, metrics, {}, [])

    assert second is False
    assert revisions.stalled == 1
    assert len(revisions.history) == 2
    second_entry = revisions.history[1]
    assert second_entry["attempt"] == 2
    assert second_entry["progress"] is False
    assert second_entry["metrics_passed"] == 0
    assert second_entry["failures"] == []


def test_card_backed_record_keeps_existing_history_fields():
    revisions = Revisions()
    change = {"before": 10, "after": 9, "delta": -1}
    card = {
        "maintainability": {
            "target_quality": change,
            "region": {"quality": change},
            "repository": {"score": change},
        }
    }
    metrics = {
        "passed": False,
        "failures": ["Quality regression"],
        "violation_distance": 1,
    }
    review = {"checks": [{"passed": False, "reason": "Needs review"}]}

    improved = revisions.record(1, {"replacement": "candidate"}, card, metrics, review, "artifacts")

    assert improved is True
    entry = revisions.history[0]
    assert entry["attempt"] == 1
    assert entry["status"] == "evaluated"
    assert entry["progress"] is True
    assert entry["metrics_passed"] is False
    assert entry["failures"] == ["Quality regression"]
    assert entry["review_failures"] == ["Needs review"]
    assert entry["measurements"] == {
        "target_quality": change,
        "region_quality": change,
        "repository_quality": change,
    }
    assert entry["artifacts"] == "artifacts"
