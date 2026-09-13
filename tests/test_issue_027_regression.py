from walleye.discovery import ScanOptions
from walleye.scanner import scan


def scanned(tmp_path, sources):
    for name, source in sources.items():
        (tmp_path / name).write_text(source)
    return scan(tmp_path, ScanOptions(functions=True))


def test_go_closures_keep_receiver_identity(tmp_path):
    result = scanned(
        tmp_path,
        {
            "a.go": """package app
type A struct{}
type B struct{}
func (a A) fetch() { f := func() int { return 1 }; f() }
func (b B) fetch() { f := func() int { return 2 }; f() }
"""
        },
    )
    assert not result["issues"]
    closures = [r for r in result["records"] if r["name"] == "<anonymous>"]
    assert {r["qualified_name"] for r in closures} == {"A.fetch.<anonymous>", "B.fetch.<anonymous>"}
