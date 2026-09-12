from io import StringIO

from walleye import cli


def test_display_value_formats_overall_score_to_one_decimal():
    assert cli._display_value(87.5, "overall_score") == "87.5"
    assert cli._display_value(87, "overall_score") == "87.0"


def test_render_table_formats_language_overall_score_like_summary(monkeypatch):
    monkeypatch.setattr(cli, "render_callgraph_details", lambda _report, _console: None)
    report = {
        "tool": {"version": "test"},
        "root": ".",
        "summary": {
            "scanned_files": 1,
            "languages": {"python": 1},
            "sloc": 1,
            "records": 0,
            "skipped": {},
        },
        "duration_seconds": 0.0,
        "scores": {
            "overall_score": 87.5,
            "by_language": {"python": {"overall_score": 87.5}},
        },
        "coverage": {},
        "ranking": {"sort": "risk_score"},
        "records": [],
        "options": {"functions": True},
    }

    stream = StringIO()
    cli.render_table(report, stream)
    rendered = stream.getvalue()

    assert "Overall 87.5/100" in rendered
    language_row = next(line for line in rendered.splitlines() if "python" in line)
    assert "87.5" in language_row
    assert "87.50" not in language_row


def test_display_value_preserves_existing_precision_boundaries():
    assert cli._display_value(None, "overall_score") == "—"
    assert cli._display_value(1.23456, "bugs") == "1.2346"
    assert cli._display_value(9.5, "risk_score") == "9.5"
    assert cli._display_value(12345, "fan_in") == "12,345"
    assert cli._display_value(9.5, "volume") == "9.50"
