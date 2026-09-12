import io

from walleye import cli


def _make_report(records, selected):
    return {
        "tool": {"version": "test"},
        "root": ".",
        "duration_seconds": 0.0,
        "summary": {
            "scanned_files": len(records),
            "languages": {},
            "sloc": 0,
            "records": len(records),
            "skipped": {},
        },
        "scores": {},
        "ranking": {"sort": selected},
        "records": records,
        "options": {"functions": False},
    }


def _table_header(output):
    return next(line for line in output.splitlines() if "Cyclomatic" in line)


def test_wide_empty_report_uses_unique_default_metric_columns(monkeypatch):
    monkeypatch.setenv("COLUMNS", "120")
    stream = io.StringIO()

    cli.render_table(_make_report([], "sloc"), stream)

    assert _table_header(stream.getvalue()).split() == [
        "#",
        "Source",
        "Risk",
        "MI",
        "Cyclomatic",
        "Nesting",
    ]


def test_wide_report_includes_available_selected_metric_once(monkeypatch):
    monkeypatch.setenv("COLUMNS", "120")
    monkeypatch.setattr(cli, "render_callgraph_details", lambda _report, _console: None)
    stream = io.StringIO()
    row = {
        "kind": "function",
        "path": "example.py",
        "line": 1,
        "end_line": 1,
        "name": "sample",
        "language": "python",
        "rank": 1,
        "sloc": 1,
        "risk_score": 50.0,
        "maintainability_index": 80.0,
        "cyclomatic_complexity": 1,
        "max_nesting": 0,
    }

    cli.render_table(_make_report([row], "sloc"), stream)

    assert _table_header(stream.getvalue()).split() == [
        "#",
        "Source",
        "Risk",
        "MI",
        "Cyclomatic",
        "SLOC",
        "Nesting",
    ]
