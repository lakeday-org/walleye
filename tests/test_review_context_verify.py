from pathlib import Path

import pytest

from walleye.discovery import ScanOptions
from walleye.review_context import SourceIndex
from walleye.scanner import scan


def indexed_sample(tmp_path):
    base = tmp_path / "repo"
    base.mkdir()
    indexed = base / "sample.py"
    source = b"def value():\n    return 7\n"
    indexed.write_bytes(source)

    snapshot = {}
    report = scan(base, ScanOptions(functions=True), source_snapshot=snapshot)
    index = SourceIndex(report, snapshot)
    return index, index.resource("sample.py"), indexed, source


def test_verify_accepts_unchanged_indexed_source(tmp_path):
    index, resource, _, _ = indexed_sample(tmp_path)

    index.verify([resource])


def test_verify_rejects_changed_indexed_source(tmp_path):
    index, resource, indexed, source = indexed_sample(tmp_path)
    indexed.write_bytes(source.replace(b"7", b"8"))

    with pytest.raises(ValueError, match="Source changed since scanning; rescan"):
        index.verify([resource])


def test_verify_rejects_path_swap_before_open(tmp_path, monkeypatch):
    index, resource, indexed, source = indexed_sample(tmp_path)
    outside = tmp_path / "outside.py"
    outside.write_bytes(source)
    original_open = Path.open
    swapped = False
    outside_opened = False

    def swap_before_open(candidate, *args, **kwargs):
        nonlocal outside_opened, swapped
        if candidate == indexed and not swapped:
            indexed.unlink()
            indexed.symlink_to(outside)
            swapped = True
        stream = original_open(candidate, *args, **kwargs)
        if candidate == indexed and candidate.resolve() == outside:
            outside_opened = True
        return stream

    monkeypatch.setattr(Path, "open", swap_before_open)

    with pytest.raises(ValueError):
        index.verify([resource])

    assert swapped
    assert not outside_opened
