import json
import sys
from dataclasses import replace

import pytest

from walleye.review import ReviewConfig
from walleye.review_agent import invoke_codex


def _fake_codex(tmp_path, event):
    response = json.dumps(
        {
            "status": "no_finding",
            "summary": "No supported defect found.",
            "finding": None,
            "context_requests": [],
        }
    )
    fake = tmp_path / "fake-codex"
    fake.write_text(
        f"#!{sys.executable}\n"
        "import sys\n"
        "from pathlib import Path\n"
        "sys.stdin.read()\n"
        "output = Path(sys.argv[sys.argv.index('--output-last-message') + 1])\n"
        f"output.write_text({response!r})\n"
        f"print({json.dumps(event)!r})\n"
    )
    fake.chmod(0o755)
    return fake


def _invoke_fake(tmp_path, event):
    fake = _fake_codex(tmp_path, event)
    try:
        return invoke_codex(
            "REVIEW PACKET {}",
            replace(ReviewConfig(), codex=str(fake)),
            1000,
        )
    except Exception as error:
        pytest.fail(f"invoke_codex raised instead of returning an error: {error}")


def test_non_object_json_event_returns_structured_error(tmp_path):
    result = _invoke_fake(tmp_path, None)

    assert result["error"]
    assert result["response"] is None


def test_null_item_returns_structured_error(tmp_path):
    result = _invoke_fake(tmp_path, {"item": None})

    assert result["error"]
    assert result["response"] is None
