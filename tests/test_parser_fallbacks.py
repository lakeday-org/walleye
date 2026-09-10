from pathlib import Path

import pytest

from walleye import javascript, shell
from walleye.metrics import is_function, measure, walk
from walleye.scanner import analyze, parser_for


@pytest.mark.parametrize(
    "language,source",
    [
        ("typescript", "export type * from './types';"),
        ("typescript", "type T = import('./types').Result<string>;"),
        ("typescript", "type T = { <K extends string>(k: K): K\n<K extends number>(k: K): K };"),
        ("tsx", "export function Panel(){return <div>Execution & runtime</div>}"),
        (
            "javascript",
            "import { delete as remove } from 'storage'; export function run(){ remove() }",
        ),
    ],
)
def test_valid_syntax_rejected_by_tree_sitter_has_complete_babel_ast(language, source):
    rows, errors = analyze(source.encode(), language, "example." + language)
    assert not errors
    assert rows[0]["parser"] == "babel"
    assert rows[0]["profile"] == javascript.PROFILE
    assert rows[0]["length"] > 0


@pytest.mark.parametrize(
    "source", ["const a = ;", "function f(", "type T = import('x').Y<;", "const a = <div>;"]
)
def test_babel_does_not_score_malformed_code(source):
    rows, issues = analyze(source.encode(), "tsx", "bad.tsx")
    assert not rows and issues


def test_babel_comments_unicode_and_nested_function_positions():
    source = (
        '// 😀\nexport type * from "x";\nfunction outer(){\n'
        " const inner = () => 2;\n return inner();\n}\n"
    )
    rows, errors = analyze(source.encode(), "typescript", "a.ts", functions=True)
    assert not errors
    assert [(r["name"], r["line"], r["end_line"]) for r in rows] == [
        ("outer", 3, 6),
        ("inner", 4, 4),
    ]
    assert rows[1]["parent_function"] == "outer"
    root, _ = javascript.parse(source.encode(), "typescript", "a.ts")
    assert measure(root, source.encode()).comment_lines == 1


@pytest.mark.parametrize(
    "source",
    [
        "x = 1 + 2;",
        "// comment\nconst text='/* 5 + 6 */';",
        "function f(){ return 1 + 2; }",
        "function f(x, y){ return x + y; }",
    ],
)
def test_babel_token_adapter_matches_lexical_counts_on_shared_syntax(source):
    data = source.encode()
    root, issues = javascript.parse(data, "javascript", "a.js")
    assert not issues
    expected = measure(parser_for("javascript").parse(data).root_node, data)
    actual = measure(root, data)
    assert actual == expected


def test_babel_template_interpolation_is_visited_as_code():
    data = b"const message = `a ${work(2 + 3)} b`;"
    root, _ = javascript.parse(data, "javascript", "a.js")
    assert any(node.type == "call_expression" for node in walk(root))
    assert measure(root, data).halstead.total_operators > 3


def test_babel_anonymous_arrow_does_not_get_named_async():
    data = b"register(async (value) => value);"
    root, _ = javascript.parse(data, "typescript", "a.ts")
    from walleye.metrics import function_name

    node = next(node for node in walk(root) if is_function(node))
    assert function_name(node, data) == "<anonymous>"


@pytest.mark.parametrize(
    "source",
    [
        'cat >/dev/null <<<"value"',
        'value="${value%]}"',
        'value="${value#[}"; value="${value%]}"',
    ],
)
def test_bash_grammar_compatibility_keeps_original_tokens(source):
    if shell.shutil.which("bash") is None:
        pytest.skip("Bash is needed for parse-only compatibility validation")
    data = source.encode()
    root = shell.parse_compatible(data, parser_for("bash"))
    assert root is not None
    assert b"x}" not in b" ".join(n.text for n in walk(root))
    if b"<<<" in data:
        assert any(n.text == b"<<<" and n.child_count == 0 for n in walk(root))
    rows, errors = analyze(data, "bash", "a.sh")
    assert not errors and rows[0]["profile"] == shell.PROFILE


def test_bash_validation_never_executes_source_or_environment(tmp_path, monkeypatch):
    marker = tmp_path / "executed"
    startup = tmp_path / "startup"
    startup.write_text(f"touch '{marker}'\n")
    monkeypatch.setenv("BASH_ENV", str(startup))
    source = f"touch '{marker}'; cat >/dev/null <<<'text'".encode()
    root = shell.parse_compatible(source, parser_for("bash"))
    assert root is not None
    assert not marker.exists()


def test_bash_invalid_source_stays_unscored_and_missing_bash_keeps_diagnostics(monkeypatch):
    rows, errors = analyze(b"if ; cat >/dev/null <<<'text'", "bash", "a.sh")
    assert not rows and errors
    monkeypatch.setattr(shell.shutil, "which", lambda *a, **kw: None)
    rows, errors = analyze(b"cat >/dev/null <<<'text'", "bash", "a.sh")
    assert not rows and errors


def test_vendored_parser_asset_is_present():
    assert Path(javascript.__file__).with_name("vendor").joinpath("babel-parser.cjs").is_file()
