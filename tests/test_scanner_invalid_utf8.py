import pytest

from walleye.discovery import ScanOptions
from walleye.scanner import analyze, scan


def _expected_decode_message(source: bytes) -> str:
    with pytest.raises(UnicodeDecodeError) as error:
        source.decode("utf-8")
    return str(error.value)


def test_analyze_returns_diagnostic_for_invalid_utf8():
    source = b"def valid():\n    return 1\n\xff"
    path = "broken.py"
    expected_message = _expected_decode_message(source)

    for functions in (False, True):
        try:
            records, errors = analyze(source, "python", path, functions=functions)
        except UnicodeError as error:
            raise AssertionError(
                "analyze propagated UnicodeError instead of returning diagnostics"
            ) from error

        assert records == []
        assert errors == [
            {
                "path": path,
                "kind": "read-or-parser",
                "message": expected_message,
            }
        ]


def test_analyze_keeps_parse_diagnostics_for_decodable_malformed_source():
    path = "broken.py"
    records, errors = analyze(b"def broken(\n", "python", path)

    assert records == []
    assert errors
    assert all(error["path"] == path for error in errors)
    assert all(error["kind"] == "parse" for error in errors)


def test_scan_preserves_read_or_parser_classification_for_invalid_utf8(tmp_path):
    source = b"def valid():\n    return 1\n\xff"
    path = "broken.py"
    (tmp_path / path).write_bytes(source)

    report = scan(
        tmp_path,
        ScanOptions(functions=True, respect_gitignore=False),
    )

    assert report["summary"]["skipped"]["read-or-parser-error"] == 1
    assert report["issues"] == [
        {
            "path": path,
            "kind": "read-or-parser",
            "message": _expected_decode_message(source),
        }
    ]
