import hashlib
import os
from pathlib import Path

import pytest

from walleye.discovery import ScanOptions
from walleye.scanner import scan


def _write_sources(root: Path, sources: dict[str, str | bytes]) -> None:
    for relative, source in sources.items():
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(source, bytes):
            path.write_bytes(source)
        else:
            path.write_bytes(source.encode("utf-8"))


def _without_timing(report: dict) -> dict:
    return {
        key: value for key, value in report.items() if key not in {"scanned_at", "duration_seconds"}
    }


def _boundary_reports(root: Path):
    function_source = (
        "def helper(value):\n"
        "    if value:\n"
        "        return value\n"
        "    return 0\n"
        "\n"
        "def caller():\n"
        "    return helper(1)\n"
    )
    plain_source = "value = 1\n"
    max_bytes = len(function_source.encode("utf-8")) + 1
    _write_sources(
        root,
        {
            "app.py": function_source,
            "plain.py": plain_source,
            "bad.py": "def broken(",
            "binary.py": b"def binary():\n    return \x00\n",
            "generated.py": "# @generated\nvalue = 1\n",
            "large.py": b"x=1\n" * (max_bytes // 4 + 1),
        },
    )
    options = ScanOptions(
        functions=True,
        max_bytes=max_bytes,
        respect_gitignore=False,
    )
    first_snapshot = {}
    second_snapshot = {}
    first = scan(root, options, source_snapshot=first_snapshot)
    second = scan(root, options, source_snapshot=second_snapshot)
    return function_source, plain_source, first, second, first_snapshot, second_snapshot


def test_scan_keeps_file_policy_and_report_contract(tmp_path):
    (
        function_source,
        plain_source,
        first,
        second,
        first_snapshot,
        second_snapshot,
    ) = _boundary_reports(tmp_path)

    assert _without_timing(first) == _without_timing(second)
    assert set(first) == {
        "schema_version",
        "tool",
        "root",
        "scanned_at",
        "duration_seconds",
        "source_fingerprint",
        "source_hashes",
        "discovery",
        "options",
        "summary",
        "coverage",
        "callgraph",
        "scores",
        "records",
        "function_hotspots",
        "areas",
        "issues",
        "complete",
        "notes",
        "health",
    }

    summary = first["summary"]
    assert summary["candidate_files"] == 6
    assert summary["scanned_files"] == 2
    assert summary["records"] == 2
    assert summary["file_records"] == 2
    assert summary["function_records"] == 2
    assert summary["top_level_functions"] == 2
    assert summary["nested_functions"] == 0
    assert summary["languages"] == {"python": 2}
    assert summary["parsers"] == {"tree-sitter": 2}
    assert summary["skipped"] == {
        "generated-header": 1,
        "oversized": 1,
        "parse-error": 1,
        "read-or-parser-error": 1,
    }
    assert summary["issue_count"] == len(first["issues"])

    assert {issue["path"] for issue in first["issues"]} == {
        "bad.py",
        "binary.py",
        "large.py",
    }
    binary_issue = next(issue for issue in first["issues"] if issue["path"] == "binary.py")
    assert binary_issue["kind"] == "read-or-parser"
    assert binary_issue["line"] == 2
    assert binary_issue["message"] == "Binary content (NUL byte); file left unscored"
    size_issue = next(issue for issue in first["issues"] if issue["path"] == "large.py")
    assert size_issue["kind"] == "size"
    assert size_issue["message"] == "Exceeds --max-bytes"

    assert first["source_hashes"] == {
        "app.py": hashlib.sha256(function_source.encode("utf-8")).hexdigest(),
        "plain.py": hashlib.sha256(plain_source.encode("utf-8")).hexdigest(),
    }
    assert (
        first_snapshot
        == second_snapshot
        == {
            "app.py": function_source.encode("utf-8"),
            "plain.py": plain_source.encode("utf-8"),
        }
    )
    assert {row["path"] for row in first["records"]} == {"app.py"}
    assert {row["name"] for row in first["records"]} == {"caller", "helper"}
    assert {row["name"] for row in first["function_hotspots"]} == {"caller", "helper"}
    assert {row["function_rank"] for row in first["function_hotspots"]} == {1, 2}
    assert [(area["path"], area["files"]) for area in first["areas"]] == [(".", 2)]

    language_score = first["scores"]["by_language"]["python"]
    assert language_score["files"] == 2
    assert language_score["sloc"] == summary["sloc"]

    coverage = first["coverage"]
    assert coverage["candidate_files"] == 6
    assert coverage["parsed_files"] == 2
    assert coverage["scanned_files"] == 2
    assert coverage["file_parse_share"] == 0.3333
    assert coverage["files_with_functions"] == 1
    assert coverage["recognized_functions"] == 2
    assert coverage["nested_functions"] == 0
    assert coverage["top_level_functions"] == 2

    graph_coverage = first["callgraph"]["coverage"]
    assert graph_coverage["functions_analyzed"] == 2
    assert graph_coverage["resolved_calls"] == 1
    assert graph_coverage["unresolved_calls"] == 0
    assert len(first["callgraph"]["edges"]) == 1
    assert first["complete"] is False
    assert first["health"] == second["health"]


def test_scan_preserves_invalid_target_messages(tmp_path):
    missing = tmp_path / "missing.py"
    with pytest.raises(ValueError) as error:
        scan(missing)
    assert str(error.value) == f"Path does not exist: {missing.expanduser().absolute()}"

    real = tmp_path / "real.py"
    real.write_text("value = 1\n")
    link = tmp_path / "link.py"
    link.symlink_to(real)
    with pytest.raises(ValueError) as error:
        scan(link)
    assert str(error.value) == "Scan a real file or directory, not a symbolic link"

    non_regular = tmp_path / "pipe"
    os.mkfifo(non_regular)
    try:
        with pytest.raises(ValueError) as error:
            scan(non_regular)
        assert str(error.value) == (
            f"Not a regular file or directory: {non_regular.expanduser().absolute()}"
        )
    finally:
        non_regular.unlink()
