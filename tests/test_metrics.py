import math

import pytest

from walleye.metrics import halstead
from walleye.scanner import analyze


def one(source: str, language="python"):
    records, errors = analyze(source.encode(), language, "sample")
    assert not errors
    return records[0]


def test_radon_formulas_against_hand_calculated_example():
    h = halstead(2, 3, 4, 6)
    assert h.vocabulary == 5
    assert h.length == 10
    assert h.calculated_length == pytest.approx(2 + 3 * math.log2(3))
    assert h.volume == pytest.approx(10 * math.log2(5))
    assert h.difficulty == 2
    assert h.effort == pytest.approx(20 * math.log2(5))
    assert h.time_seconds == pytest.approx(h.effort / 18)
    assert h.bugs == pytest.approx(h.volume / 3000)


@pytest.mark.parametrize("counts", [(0, 0, 0, 0), (0, 1, 0, 1), (1, 0, 1, 0)])
def test_degenerate_counts_are_finite(counts):
    assert all(math.isfinite(n) for n in halstead(*counts).to_dict().values())


@pytest.mark.parametrize("counts", [(-1, 1, 1, 1), (2, 1, 1, 1), (0, 1, 2, 1), (1.5, 1, 2, 1)])
def test_invalid_counts_rejected(counts):
    with pytest.raises(ValueError):
        halstead(*counts)


def test_known_python_token_counts():
    row = one("x = a + 1\n")
    assert (row["distinct_operators"], row["distinct_operands"]) == (2, 3)
    assert (row["total_operators"], row["total_operands"]) == (2, 3)
    assert row["volume"] == pytest.approx(5 * math.log2(5))


def test_known_javascript_token_counts():
    row = one("const x = a + 1;", "javascript")
    assert (row["distinct_operators"], row["distinct_operands"]) == (4, 3)
    assert (row["total_operators"], row["total_operands"]) == (4, 3)


def test_comments_and_formatting_do_not_change_halstead():
    plain = one("x = a + 1\n")
    comments = one("# fake = x * y\n\nx   = a + 1  # while if +\n")
    for key in halstead(0, 0, 0, 0).to_dict():
        assert comments[key] == plain[key]
    assert comments["sloc"] == plain["sloc"] == 1
    assert comments["comment_lines"] == 2


@pytest.mark.parametrize(
    "language,source",
    [
        ("python", "# x = 4\n"),
        ("javascript", "/* x += 1; */\n"),
        ("rust", "// fn f() {}\n"),
    ],
)
def test_comment_only_files(language, source):
    row = one(source, language)
    assert row["length"] == row["volume"] == row["bugs"] == row["sloc"] == 0


def test_static_string_is_one_operand_regardless_of_fake_code_inside():
    short = one('const s = "hello";', "javascript")
    fake = one('const s = "if (x) { ++x; } // comment";', "javascript")
    for key in halstead(0, 0, 0, 0).to_dict():
        assert short[key] == fake[key]


def test_interpolation_is_analyzed_as_code():
    plain = one("const s = `hello`;", "javascript")
    expression = one("const s = `hello ${a + b}`;", "javascript")
    assert expression["total_operators"] > plain["total_operators"]
    assert expression["total_operands"] > plain["total_operands"]


def test_unicode_crlf_and_empty_files():
    assert one("π = 3\r\n")["sloc"] == 1
    assert one("")["end_line"] == 0
    assert one("")["length"] == 0


def test_malformed_file_has_diagnostics_and_no_score():
    records, issues = analyze(b"def broken(\n", "python", "bad.py")
    assert not records
    assert issues and issues[0]["path"] == "bad.py"
    assert issues[0]["line"] == 1


def test_nested_functions_do_not_inflate_outer_function_metrics():
    small = b"def outer(a):\n    def inner(b):\n        return b\n    return a\n"
    large = small.replace(b"return b", b"return b + b * b - b")
    before, _ = analyze(small, "python", "a.py", functions=True)
    after, _ = analyze(large, "python", "a.py", functions=True)
    assert [row["name"] for row in before] == ["outer", "inner"]
    assert before[0]["length"] == after[0]["length"]
    assert before[1]["length"] < after[1]["length"]
    assert before[1]["line"] == 2


@pytest.mark.parametrize(
    "language,source",
    [
        ("typescript", "const add = (a: number, b: number) => a + b;"),
        ("rust", "fn add(a: i32, b: i32) -> i32 { a + b }"),
        ("cpp", "int add(int a, int b) { return a + b; }"),
        ("java", "class A { int add(int a, int b) { return a + b; } }"),
        ("go", "package main\nfunc add(a int, b int) int { return a + b }"),
        ("ruby", "def add(a, b)\n a + b\nend"),
        ("kotlin", "fun add(a: Int, b: Int): Int { return a + b }"),
        ("swift", "func add(_ a: Int, _ b: Int) -> Int { return a + b }"),
    ],
)
def test_function_locations_and_names(language, source):
    records, issues = analyze(source.encode(), language, "sample", functions=True)
    assert not issues
    assert len(records) == 1
    assert records[0]["name"] == "add"
    assert records[0]["length"] > 0


def test_deep_syntax_tree_does_not_use_python_recursion():
    row = one("x = " + "(" * 1500 + "1" + ")" * 1500)
    assert row["total_operands"] == 2


def test_repeated_parser_reuse_and_line_access():
    # Regresses the native crash observed with py-tree-sitter 0.26.0.
    source = "\n".join(f"const x{i} = `value ${{{i} + 1}}`; // comment" for i in range(500))
    for _ in range(3):
        row = one(source, "typescript")
        assert row["sloc"] == 500
        assert row["comment_lines"] == 500
