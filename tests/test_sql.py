import json
import math

import pytest
from sqlfluff.core import FluffConfig
from sqlfluff.core.parser import Lexer, Parser

from declank.cli import main
from declank.scanner import analyze
from declank.sql import SQL_PROFILE


def parsed(source, dialect="auto"):
    records, errors = analyze(source.encode(), "sql", "schema.sql", sql_dialect=dialect)
    assert not errors, errors
    assert len(records) == 1
    return records[0]


SQLITE_STATEMENTS = [
    "PRAGMA foreign_keys = ON;",
    "PRAGMA journal_mode = WAL; PRAGMA user_version = 18;",
    "BEGIN; COMMIT; ROLLBACK; END TRANSACTION;",
    "BEGIN DEFERRED TRANSACTION; BEGIN IMMEDIATE; BEGIN EXCLUSIVE;",
    "SAVEPOINT checkpoint; ROLLBACK TO checkpoint; RELEASE checkpoint;",
    "ROLLBACK TRANSACTION TO SAVEPOINT checkpoint;",
    "CREATE TABLE events (id INTEGER PRIMARY KEY, data ANY NOT NULL) STRICT;",
    "CREATE TABLE events (id TEXT PRIMARY KEY, data TEXT) WITHOUT ROWID;",
    "CREATE TABLE events (id INTEGER PRIMARY KEY AUTOINCREMENT, data TEXT);",
    "CREATE TABLE configs (data TEXT DEFAULT '[]' CHECK (json_valid(data)), "
    "warm INTEGER NOT NULL CHECK (warm IN (0, 1)), "
    "size INTEGER CHECK (size BETWEEN 1 AND 64));",
    "INSERT INTO events(id, data) VALUES (?, ?) "
    "ON CONFLICT(id) DO UPDATE SET data = excluded.data;",
    "SELECT json_extract(data, '$.key') FROM events WHERE id = :id LIMIT ?;",
    "CREATE TRIGGER reject_deletion BEFORE INSERT ON organizations "
    "WHEN EXISTS (SELECT 1 FROM receipts WHERE id = NEW.id) "
    "BEGIN SELECT RAISE(ABORT, 'deletion pending'); END;",
    "CREATE INDEX IF NOT EXISTS live_items ON items(id) WHERE deleted_at IS NULL;",
]


@pytest.mark.parametrize("source", SQLITE_STATEMENTS)
def test_real_sqlite_constructs_with_auto_and_explicit_dialects(source):
    automatic = parsed(source)
    explicit = parsed(source, "sqlite")
    assert automatic == explicit
    assert automatic["parser"] == "sqlfluff"
    assert automatic["profile"] == SQL_PROFILE
    assert automatic["dialect"] == "sqlite"
    assert automatic["volume"] > 0


def test_multistatement_migration_regression():
    source = "\n-- statement\n".join(SQLITE_STATEMENTS)
    row = parsed(source)
    assert row["comment_lines"] == len(SQLITE_STATEMENTS) - 1
    assert row["sloc"] == len(SQLITE_STATEMENTS)
    assert row["total_operators"] > 100


def test_sql_hand_calculated_counts():
    row = parsed("SELECT 1 + 2;")
    assert (row["distinct_operators"], row["total_operators"]) == (3, 3)
    assert (row["distinct_operands"], row["total_operands"]) == (2, 2)
    assert row["bugs"] == pytest.approx(5 * math.log2(5) / 3000)


def test_sql_comments_whitespace_and_keyword_case_do_not_change_counts():
    first = parsed("SELECT a + 2 FROM data;")
    second = parsed("/* SELECT x / 6 */\nselect a+2\nfrom data; -- fake syntax\n")
    for metric in ("volume", "difficulty", "length", "distinct_operands", "distinct_operators"):
        assert first[metric] == second[metric]
    assert second["comment_lines"] == 2
    assert second["sloc"] == 2


def test_quoted_identifiers_and_literals_are_operands():
    row = parsed("SELECT \"a;b\", 'literal /* + ; */';")
    assert row["total_operators"] == 3  # SELECT, comma, semicolon.
    assert row["total_operands"] == 2


def test_unicode_and_crlf_line_positions():
    row = parsed("-- π\r\nSELECT 'λ';\r\n/* α\r\nβ */\r\nSELECT 2;\r\n")
    assert row["sloc"] == 2
    assert row["comment_lines"] == 3
    assert row["end_line"] == 5


@pytest.mark.parametrize("source", ["", "-- just a comment\n", "/* a\n\nb */\n"])
def test_empty_or_comment_only_sql(source):
    row = parsed(source)
    assert row["volume"] == row["length"] == row["sloc"] == 0


@pytest.mark.parametrize(
    "dialect,source",
    [
        ("postgres", "SELECT value::jsonb ->> 'key' FROM events;"),
        ("mysql", "SELECT `id` FROM `events` LIMIT 1;"),
        ("bigquery", "SELECT x FROM UNNEST([1, 2, 3]) AS x;"),
        ("tsql", "SELECT TOP 1 [id] FROM [events];"),
    ],
)
def test_other_sql_dialects(dialect, source):
    assert parsed(source, dialect)["dialect"] == dialect


@pytest.mark.parametrize(
    "source",
    [
        "CREATE TABLE broken (id INTEGER;",
        "SELECT 'unterminated;",
        "INSERT INTO ;",
        "COMMIT IMMEDIATE;",
    ],
)
def test_invalid_sql_never_receives_a_score(source):
    records, issues = analyze(source.encode(), "sql", "broken.sql")
    assert not records
    assert issues and all(issue["kind"] == "parse" for issue in issues)
    assert "tried_dialects" in issues[0]
    assert "unterminated" not in json.dumps(issues)  # No source snippets in diagnostics.


def test_sqlite_extension_is_isolated_from_upstream_dialect():
    parsed("BEGIN IMMEDIATE;", "sqlite")
    config = FluffConfig(overrides={"dialect": "sqlite"}, ignore_local_config=True)
    tokens, _ = Lexer(config=config).lex("BEGIN IMMEDIATE;")
    assert list(Parser(config=config).parse(tokens).iter_unparsables())


def test_sql_does_not_process_local_or_inline_configuration(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".sqlfluff").write_text("[sqlfluff]\ndialect = invalid\ntemplater = jinja\n")
    parsed("-- sqlfluff:dialect:not_a_dialect\n-- sqlfluff:templater:jinja\nSELECT 1;")
    records, issues = analyze(b"SELECT {{ 1 + 2 }};", "sql", "template.sql")
    assert not records and issues  # Raw braces are never evaluated as a template.


def test_sql_functions_not_claimed_as_file_records():
    records, issues = analyze(b"SELECT 1;", "sql", "a.sql", functions=True)
    assert not records and not issues


def test_sql_cli_and_parser_metadata(tmp_path, capsys):
    (tmp_path / "schema.sql").write_text("PRAGMA foreign_keys=ON; BEGIN IMMEDIATE;")
    assert (
        main(
            [
                "scan",
                str(tmp_path),
                "--language",
                "sql",
                "--sql-dialect",
                "sqlite",
                "--format",
                "json",
            ]
        )
        == 0
    )
    report = json.loads(capsys.readouterr().out)
    assert report["complete"]
    assert report["options"]["sql_dialect"] == "sqlite"
    assert report["tool"]["sqlfluff_version"] == "4.3.0"
    assert report["records"][0]["profile"] == SQL_PROFILE


def test_invalid_sql_dialect_is_a_cli_error():
    with pytest.raises(SystemExit) as error:
        main(["scan", "--sql-dialect", "made-up"])
    assert error.value.code == 2


def test_terminal_groups_diagnostics_but_json_keeps_them(tmp_path, capsys):
    (tmp_path / "good.py").write_text("a=1")
    (tmp_path / "bad.py").write_text("a = $\nb = $\nc = $\n")
    assert main(["scan", str(tmp_path), "--format", "json"]) == 2
    captured = capsys.readouterr()
    report = json.loads(captured.out)
    assert len(report["issues"]) > 1
    assert captured.err.count("bad.py:") == 1
    assert "more diagnostics" in captured.err
