import json

import pytest

from walleye.cli import main, record_sort_key
from walleye.discovery import ScanOptions
from walleye.scanner import analyze, risk_sort_key, scan


def function(source: str, language: str = "python", name: str | None = None) -> dict:
    rows, issues = analyze(source.encode(), language, "sample", functions=True)
    assert not issues
    if name is None:
        return rows[0]
    return next(row for row in rows if row["qualified_name"] == name)


def test_branching_and_nesting_raise_control_complexity():
    sequential = function("def f(x):\n    y = x + 1\n    return y\n")
    branching = function(
        "def f(x):\n    if x:\n        if x > 1:\n            return 1\n    return 0\n"
    )
    assert sequential["cyclomatic_complexity"] == 1
    assert sequential["control_branch_count"] == 0
    assert sequential["max_nesting"] == 0
    assert branching["cyclomatic_complexity"] == 3
    assert branching["control_branch_count"] == 2
    assert branching["max_nesting"] == 2
    assert branching["structure_hotspots"][0]["type"] == "if"
    assert branching["structure_hotspots"][0]["subtree_branches"] == 2


@pytest.mark.parametrize(
    ("language", "source", "expected_complexity", "expected_branches"),
    [
        (
            "python",
            "def f(x):\n"
            "    try:\n"
            "        if x:\n"
            "            return 1\n"
            "    except ValueError:\n"
            "        return 2\n"
            "    return 0\n",
            3,
            2,
        ),
        (
            "javascript",
            "function f(x) {\n"
            "    switch (x) { case 1: return 1; case 2: return 2; default: return 0; }\n"
            "}\n",
            3,
            2,
        ),
        (
            "typescript",
            "function f(x: number) {\n"
            "    switch (x) { case 1: return 1; case 2: return 2; default: return 0; }\n"
            "}\n",
            3,
            2,
        ),
        (
            "go",
            "package p\n"
            "func f(x int) int {\n"
            "    switch x { case 1: return 1; case 2: return 2; default: return 0 }\n"
            "}\n",
            3,
            2,
        ),
        (
            "java",
            "class A {\n"
            "    int f(int x) {\n"
            "        try { if (x > 0) return 1; }\n"
            "        catch (RuntimeException e) { return 2; }\n"
            "        return 0;\n"
            "    }\n"
            "}\n",
            3,
            2,
        ),
        (
            "c",
            "int f(int x) {\n"
            "    switch (x) { case 1: return 1; case 2: return 2; default: return 0; }\n"
            "}\n",
            3,
            2,
        ),
        (
            "cpp",
            "int f(int x) {\n"
            "    switch (x) { case 1: return 1; case 2: return 2; default: return 0; }\n"
            "}\n",
            3,
            2,
        ),
        (
            "rust",
            "fn f(x: i32) -> i32 { match x { 1 => 1, 2 => 2, _ => 0 } }\n",
            3,
            2,
        ),
    ],
)
def test_core_adapters_count_decisions_without_structural_overcount(
    language, source, expected_complexity, expected_branches
):
    row = function(source, language)
    assert row["complexity_status"] == "supported"
    assert row["cyclomatic_complexity"] == expected_complexity
    assert row["control_branch_count"] == expected_branches
    assert any(not item["counts_toward_cyclomatic"] for item in row["structure_hotspots"])


def test_bare_rust_loop_is_structural_only():
    row = function("fn f() { loop { break; } }\n", "rust")
    assert row["cyclomatic_complexity"] == 1
    assert row["control_branch_count"] == 0
    assert row["structure_hotspots"][0]["type"] == "loop"
    assert not row["structure_hotspots"][0]["counts_toward_cyclomatic"]


@pytest.mark.parametrize(
    ("language", "source", "expected_complexity", "expected_branches", "expected_nesting"),
    [
        (
            "go",
            "package p\n"
            "func f(x int) int {\n"
            "    if x > 0 { return 1 } else if x < 0 { return -1 } else { return 0 }\n"
            "}\n",
            3,
            2,
            1,
        ),
        (
            "go",
            "package p\n"
            "func f(x interface{}) int {\n"
            "    switch x.(type) { case int: return 1; case string: return 2; default: return 0 }\n"
            "}\n",
            3,
            2,
            2,
        ),
        (
            "java",
            "class A {\n"
            "    int f(int[] xs) {\n"
            "        int y = 0; for (int x : xs) { y += x; } return y;\n"
            "    }\n"
            "}\n",
            2,
            1,
            1,
        ),
        (
            "java",
            "class A {\n"
            "    int f(int x) {\n"
            "        return switch (x) { case 1 -> 1; case 2 -> 2; default -> 0; };\n"
            "    }\n"
            "}\n",
            3,
            2,
            2,
        ),
        (
            "python",
            "def f(xs):\n    return [x for x in xs if x > 0]\n",
            3,
            2,
            1,
        ),
        (
            "rust",
            "fn f(x: i32) -> i32 { match x { n if n > 0 => n, _ => 0 } }\n",
            3,
            2,
            2,
        ),
    ],
)
def test_language_specific_control_shapes_are_counted(
    language, source, expected_complexity, expected_branches, expected_nesting
):
    row = function(source, language)
    assert row["cyclomatic_complexity"] == expected_complexity
    assert row["control_branch_count"] == expected_branches
    assert row["max_nesting"] == expected_nesting
    if language == "rust":
        assert any(item["guard_branches"] == 1 for item in row["structure_hotspots"])


def test_cpp_range_loop_counts_a_decision():
    row = function(
        "int f() { int xs[] = {1, 2}; int total = 0; "
        "for (int x : xs) { total += x; } return total; }",
        "cpp",
    )
    assert row["cyclomatic_complexity"] == 2
    assert row["max_nesting"] == 1
    assert row["structure_hotspots"][0]["type"] == "for-range"


def test_tsx_render_branches_are_measured():
    row = function(
        "function View({ok}: {ok: boolean}) { if (ok) return <div />; return null; }",
        "tsx",
    )
    assert row["cyclomatic_complexity"] == 2
    assert row["max_nesting"] == 1


def test_qualified_names_and_nested_function_ownership():
    rows, issues = analyze(
        b"class Service:\n"
        b"    def outer(self, x):\n"
        b"        def inner(y):\n"
        b"            if y:\n"
        b"                return y\n"
        b"            return 0\n"
        b"        return inner(x)\n",
        "python",
        "service.py",
        functions=True,
    )
    assert not issues
    outer = next(row for row in rows if row["qualified_name"] == "Service.outer")
    inner = next(row for row in rows if row["qualified_name"] == "Service.outer.inner")
    assert outer["parent_function"] is None
    assert inner["parent_function"] == "Service.outer"
    assert outer["function_depth"] == 0
    assert inner["function_depth"] == 1
    assert inner["ownership"] == "nested-function"
    assert inner["line"] == 3
    assert inner["end_line"] == 6


def test_nested_closure_is_an_exclusive_quality_unit(tmp_path):
    path = tmp_path / "sample.py"
    path.write_text(
        "def outer(x):\n"
        "    def inner(y):\n"
        "        if y:\n"
        "            if y > 1:\n"
        "                if y > 2:\n"
        "                    return y\n"
        "        return 0\n"
        "    return inner(x)\n"
    )
    with_closure = scan(tmp_path, ScanOptions(functions=True))
    path.write_text("def outer(x):\n    return x\n")
    without_closure = scan(tmp_path, ScanOptions(functions=True))
    assert with_closure["scores"]["ownership"]["nested_functions_in_aggregate"]
    assert (
        with_closure["scores"]["complexity_score"] < without_closure["scores"]["complexity_score"]
    )
    assert with_closure["summary"]["sloc"] != without_closure["summary"]["sloc"]


def test_scores_and_totals_are_stable_across_level_and_top(tmp_path, capsys):
    (tmp_path / "a.py").write_text("def a(x):\n    if x:\n        return 1\n    return 0\n")
    (tmp_path / "b.py").write_text("def b(x):\n    return x + 1\n")
    assert main(["scan", str(tmp_path), "--format", "json", "--top", "1"]) == 0
    function_report = json.loads(capsys.readouterr().out)
    assert main(["scan", str(tmp_path), "--format", "json", "--level", "file"]) == 0
    file_report = json.loads(capsys.readouterr().out)
    assert function_report["scores"] == file_report["scores"]
    assert function_report["summary"]["sloc"] == file_report["summary"]["sloc"]
    assert function_report["summary"]["estimated_bugs_sum"] == pytest.approx(
        file_report["summary"]["estimated_bugs_sum"]
    )
    assert len(function_report["records"]) == 1
    assert function_report["summary"]["records"] == 2


def test_unsupported_complexity_is_explicit(tmp_path):
    (tmp_path / "known.py").write_text("def f(x):\n    if x:\n        return 1\n    return 0\n")
    (tmp_path / "other.rb").write_text("def f(x)\n  x + 1\nend\n")
    report = scan(tmp_path, ScanOptions(functions=True))
    ruby = next(row for row in report["function_hotspots"] if row["language"] == "ruby")
    python = next(row for row in report["function_hotspots"] if row["language"] == "python")
    assert ruby["complexity_status"] == "unsupported"
    assert ruby["cyclomatic_complexity"] is None
    assert ruby["complexity_score"] is None
    assert ruby["maintainability_index"] is None
    assert python["complexity_status"] == "supported"
    assert report["scores"]["status"] == "partial"
    assert report["scores"]["coverage"]["unsupported"] == 1


def test_rust_cfg_test_subtree_is_excluded_from_quality_units(tmp_path):
    path = tmp_path / "lib.rs"
    path.write_text(
        "fn prod(x: i32) -> i32 { x }\n\n"
        "#[cfg(test)]\n"
        "mod tests {\n"
        "    #[test]\n"
        "    fn branchy(x: i32) -> i32 {\n"
        "        if x > 1 { if x > 2 { if x > 3 { return x; } } }\n"
        "        x\n"
        "    }\n"
        "}\n"
    )
    production = scan(tmp_path, ScanOptions(functions=True))
    with_tests = scan(tmp_path, ScanOptions(functions=True, include_tests=True))
    prod = next(row for row in production["function_hotspots"] if row["name"] == "prod")
    prod_with_tests = next(row for row in with_tests["function_hotspots"] if row["name"] == "prod")
    assert production["summary"]["function_records"] == 1
    assert prod["cyclomatic_complexity"] == prod_with_tests["cyclomatic_complexity"] == 1
    assert prod["risk_score"] == prod_with_tests["risk_score"]
    assert with_tests["summary"]["function_records"] > production["summary"]["function_records"]
    assert with_tests["scores"]["overall_score"] < production["scores"]["overall_score"]


def test_capped_risk_ties_prefer_raw_complexity_before_path():
    rows = [
        {
            "path": "a-easy.py",
            "line": 10,
            "risk_score": 100.0,
            "cyclomatic_complexity": 4,
            "max_nesting": 2,
            "volume": 500,
        },
        {
            "path": "z-hard.py",
            "line": 2,
            "risk_score": 100.0,
            "cyclomatic_complexity": 40,
            "max_nesting": 8,
            "volume": 400,
        },
    ]
    assert sorted(rows, key=risk_sort_key)[0]["path"] == "z-hard.py"
    assert sorted(rows, key=lambda row: record_sort_key(row, "risk_score"))[0]["path"] == (
        "z-hard.py"
    )
