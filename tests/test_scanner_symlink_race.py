import walleye.scanner as scanner
from walleye.discovery import ScanOptions


def test_scan_skips_file_replaced_by_symlink_after_discovery(tmp_path, monkeypatch):
    replaced = tmp_path / "a.py"
    stable = tmp_path / "stable.py"
    replaced.write_text("def replaced():\n    return 1\n")
    stable.write_text("def stable():\n    return 2\n")

    outside = tmp_path.parent / f"{tmp_path.name}-outside.py"
    outside.write_text("def linked():\n    return 3\n")
    original_discover = scanner.discover

    def discover_then_replace(root, options):
        result = original_discover(root, options)
        replaced.unlink()
        replaced.symlink_to(outside)
        return result

    monkeypatch.setattr(scanner, "discover", discover_then_replace)
    try:
        report = scanner.scan(
            tmp_path,
            ScanOptions(functions=True, respect_gitignore=False),
        )
    finally:
        outside.unlink(missing_ok=True)

    assert [row["path"] for row in report["records"]] == ["stable.py"]
    assert set(report["source_hashes"]) == {"stable.py"}
    assert report["summary"]["skipped"].get("symlink") == 1
    assert report["summary"]["scanned_files"] == 1


def test_scan_scans_unchanged_regular_file(tmp_path):
    path = tmp_path / "a.py"
    path.write_text("def keep():\n    return 1\n")

    report = scanner.scan(
        tmp_path,
        ScanOptions(functions=True, respect_gitignore=False),
    )

    assert [row["path"] for row in report["records"]] == ["a.py"]
    assert "a.py" in report["source_hashes"]
    assert report["summary"]["scanned_files"] == 1
