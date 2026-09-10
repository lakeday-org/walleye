"""SQLFluff CST adapter. No templating, local config loading, linting or execution."""

from bisect import bisect_right
from collections import Counter
from functools import lru_cache

from sqlfluff.core import FluffConfig
from sqlfluff.core.dialects import dialect_readout, load_raw_dialect
from sqlfluff.core.errors import SQLBaseError
from sqlfluff.core.parser import Lexer, OneOf, Parser, Ref, Sequence
from sqlfluff.dialects.dialect_sqlite import TransactionStatementSegment

from .metrics import OPEN_PAIRS, halstead

SQL_PROFILE = "sqlfluff-lexical-v1"
AUTO_DIALECTS = ("sqlite", "ansi", "postgres", "mysql")


class SQLiteTransactionStatementSegment(TransactionStatementSegment):
    """Complete SQLite transaction and savepoint syntax missing in SQLFluff 4.3.

    Follow https://www.sqlite.org/lang_transaction.html without rewriting source.
    """

    match_grammar = OneOf(
        Sequence(
            "BEGIN",
            OneOf("DEFERRED", "IMMEDIATE", "EXCLUSIVE", optional=True),
            Ref.keyword("TRANSACTION", optional=True),
        ),
        Sequence(OneOf("COMMIT", "END"), Ref.keyword("TRANSACTION", optional=True)),
        Sequence("SAVEPOINT", Ref("ObjectReferenceSegment")),
        Sequence(
            "RELEASE",
            Ref.keyword("SAVEPOINT", optional=True),
            Ref("ObjectReferenceSegment"),
        ),
        Sequence(
            "ROLLBACK",
            Ref.keyword("TRANSACTION", optional=True),
            Sequence(
                "TO",
                Ref.keyword("SAVEPOINT", optional=True),
                Ref("ObjectReferenceSegment"),
                optional=True,
            ),
        ),
    )


def validate_dialect(value: str) -> str:
    choices = {dialect.label for dialect in dialect_readout()} | {"auto"}
    if value not in choices:
        raise ValueError(
            f"Unknown SQL dialect {value!r}; choose from: {', '.join(sorted(choices))}"
        )
    return value


@lru_cache(maxsize=40)
def _config(dialect: str) -> FluffConfig:
    config = FluffConfig(
        overrides={"dialect": dialect, "templater": "raw"},
        ignore_local_config=True,
    )
    if dialect == "sqlite":
        sqlite = load_raw_dialect("sqlite").copy_as("declank_sqlite")
        sqlite.replace(TransactionStatementSegment=SQLiteTransactionStatementSegment)
        # set_value coerces scalar config strings; install the isolated object
        # directly into this configuration's public core section instead.
        core = config.get_section("core")
        assert isinstance(core, dict)
        core["dialect_obj"] = sqlite.expand()
    return config


def _parse(text: str, path: str, dialect: str):
    config = _config(dialect)
    try:
        # Low-level APIs avoid Linter's templating and inline-config processing.
        tokens, violations = Lexer(config=config).lex(text)
        if violations:
            return None, [
                {
                    "path": path,
                    "kind": "parse",
                    "line": error.line_no,
                    "column": error.line_pos,
                    "message": f"Invalid SQL token ({dialect})",
                }
                for error in violations
            ]
        tree = Parser(config=config).parse(tokens, fname=path)
        errors = []
        if tree:
            for node in tree.iter_unparsables():
                line, column = node.pos_marker.source_position()
                errors.append(
                    {
                        "path": path,
                        "kind": "parse",
                        "line": line,
                        "column": column,
                        "message": f"SQL syntax is unsupported or incomplete ({dialect})",
                    }
                )
        return tree, errors
    except (SQLBaseError, RecursionError) as error:
        return None, [
            {
                "path": path,
                "kind": "parse",
                "line": getattr(error, "line_no", 1),
                "column": getattr(error, "line_pos", 1),
                "message": f"SQL parser could not complete analysis ({dialect})",
            }
        ]


def analyze_sql(source: bytes, path: str, *, dialect: str = "auto", functions: bool = False):
    text = source.decode("utf-8")
    dialects = AUTO_DIALECTS if dialect == "auto" else (validate_dialect(dialect),)
    attempts = []
    for selected in dialects:
        tree, errors = _parse(text, path, selected)
        if not errors:
            break
        attempts.append(errors)
    else:
        errors = min(attempts, key=len)
        for error in errors:
            error["parser"] = "sqlfluff"
            error["tried_dialects"] = list(dialects)
            error["message"] += "; select a dialect with --sql-dialect"
        return [], errors[:25]
    if functions:
        # Procedural SQL function bodies need their own language adapters.
        return [], []
    operators, operands = Counter(), Counter()
    code_lines, comment_lines = set(), set()
    line_starts = [0] + [i + 1 for i, char in enumerate(text) if char == "\n"]
    for segment in tree.raw_segments if tree else ():
        if not segment.raw or segment.is_meta or segment.pos_marker is None:
            continue
        span = segment.pos_marker.source_slice
        start = bisect_right(line_starts, span.start) - 1
        end = bisect_right(line_starts, max(span.start, span.stop - 1)) - 1
        if segment.is_type("comment"):
            comment_lines.update(range(start, end + 1))
        elif segment.is_code:
            code_lines.update(range(start, end + 1))
            token = segment.raw
            if segment.is_type("literal", "identifier", "data_type_identifier"):
                operands[token] += 1
            elif token.upper() in {"NULL", "TRUE", "FALSE"}:
                operands[token.upper()] += 1
            elif token in OPEN_PAIRS:
                operators[OPEN_PAIRS[token]] += 1
            elif token not in {")", "]", "}"}:
                if segment.is_type("keyword", "symbol"):
                    operators[token.upper()] += 1
                else:
                    operands[token] += 1
    nonblank = {i for i, line in enumerate(text.splitlines()) if line.strip()}
    metrics = halstead(len(operators), len(operands), operators.total(), operands.total())
    return [
        {
            "path": path,
            "language": "sql",
            "kind": "file",
            "name": None,
            "line": 1,
            "end_line": len(text.splitlines()),
            "sloc": len(code_lines & nonblank),
            "comment_lines": len(comment_lines),
            "opaque_bytes": 0,
            "parser": "sqlfluff",
            "profile": SQL_PROFILE,
            "dialect": selected,
            **metrics.to_dict(),
        }
    ], []
