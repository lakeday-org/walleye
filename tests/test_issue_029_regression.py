import pytest

from walleye.discovery import ScanOptions
from walleye.scanner import scan


def scanned(tmp_path, sources):
    for name, source in sources.items():
        (tmp_path / name).write_text(source)
    return scan(tmp_path, ScanOptions(functions=True))


@pytest.mark.parametrize("comment", ["// line", "/* block */", "/// doc"])
def test_rust_test_comments_do_not_change_exclusion(tmp_path, comment):
    result = scanned(
        tmp_path, {"lib.rs": f"fn production() {{}}\n#[test]\n{comment}\nfn check() {{}}\n"}
    )
    assert not result["issues"]
    assert {r["name"] for r in result["records"]} == {"production"}
