import pytest

from walleye.review import ReviewConfig, prepare_review


def oversized_repo(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    body = "\n".join(f"    if value == {number}: value += 1" for number in range(800))
    (repo / "large.py").write_text(
        "def oversized(value):\n" + body + "\n    return value\n",
        encoding="utf-8",
    )
    return repo


def target_from_initial_review(repo, tmp_path):
    _, packets, _, _ = prepare_review(repo, issues=1, objective="bug", output=tmp_path / "initial")
    assert len(packets) == 1
    return packets[0]["target"]


def test_targeted_packet_failure_is_reported(tmp_path):
    repo = oversized_repo(tmp_path)
    target = target_from_initial_review(repo, tmp_path)

    with pytest.raises(ValueError) as caught:
        prepare_review(
            repo,
            issues=1,
            objective="bug",
            config=ReviewConfig(context_tokens=2500),
            output=tmp_path / "targeted",
            targets=[target],
        )

    message = str(caught.value)
    assert "Complete target exceeds the context window" in message
    assert f"{target['path']}:{target['line']}" in message


def test_automatic_packet_failure_is_recorded_as_skipped(tmp_path):
    repo = oversized_repo(tmp_path)

    manifest, packets, _, _ = prepare_review(
        repo,
        issues=1,
        objective="bug",
        config=ReviewConfig(context_tokens=2500),
        output=tmp_path / "automatic",
    )

    assert packets == []
    skipped = manifest["ranking"]["skipped_packets"]
    assert len(skipped) == 1
    assert skipped[0]["task_id"] == "001"
    assert "Complete target exceeds the context window: large.py:1" in skipped[0]["reason"]
