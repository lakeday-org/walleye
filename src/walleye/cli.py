"""The walleye command line; report data goes to stdout, diagnostics to stderr."""

import argparse
import csv
import json
import math
import os
import sys
import tempfile
from collections import defaultdict
from dataclasses import replace
from pathlib import Path

from rich.console import Console
from rich.table import Table
from rich.text import Text

from . import __version__
from .discovery import ScanOptions
from .languages import EXTENSIONS, LANGUAGES, parse_mapping
from .scanner import risk_sort_key, scan

SORT_KEYS = (
    "refactor_priority",
    "scenario_gain",
    "fan_in",
    "dependent_count",
    "risk_score",
    "bugs",
    "complexity_score",
    "cyclomatic_complexity",
    "max_nesting",
    "maintainability_index",
    "effort",
    "difficulty",
    "volume",
    "sloc",
    "length",
    "time_seconds",
)
CSV_FIELDS = [
    "rank",
    "path",
    "language",
    "kind",
    "name",
    "qualified_name",
    "parent_function",
    "ownership",
    "function_depth",
    "function_rank",
    "line",
    "end_line",
    "sloc",
    "comment_lines",
    "distinct_operators",
    "distinct_operands",
    "total_operators",
    "total_operands",
    "vocabulary",
    "length",
    "calculated_length",
    "volume",
    "difficulty",
    "effort",
    "time_seconds",
    "bugs",
    "opaque_bytes",
    "complexity_status",
    "complexity_adapter",
    "cyclomatic_complexity",
    "control_branch_count",
    "logical_branch_count",
    "max_nesting",
    "complexity_score",
    "maintainability_index",
    "halstead_risk_score",
    "risk_score",
    "structure_hotspots",
    "risk_components",
    "refactor_priority",
    "scenario_gain",
    "fan_in",
    "fan_out",
    "dependent_count",
    "betweenness",
    "cycle_size",
    "graph_status",
    "callers",
    "callees",
    "refactor_actions",
    "parser",
    "profile",
    "dialect",
]


def _sort_value(row: dict, key: str) -> float:
    value = row.get(key)
    return float(value) if isinstance(value, (int, float)) else float("-inf")


def _descending_component(row: dict, key: str) -> float:
    value = row.get(key)
    return -float(value) if isinstance(value, (int, float)) else float("inf")


def record_sort_key(row: dict, key: str) -> tuple:
    """Return a stable descending key, preserving useful risk ties."""

    if key == "risk_score":
        return risk_sort_key(row)
    return (_descending_component(row, key), row.get("path", ""), row.get("line", 0))


def _display_value(value, metric: str) -> str:
    if value is None:
        return "—"
    if metric == "bugs":
        return f"{value:.4f}"
    if metric in {"risk_score", "maintainability_index", "complexity_score"}:
        return f"{value:.1f}"
    if isinstance(value, int):
        return f"{value:,}"
    return f"{value:,.2f}"


def _format_function_location(row: dict) -> str:
    if row["kind"] != "function":
        return row["path"]
    name = row.get("qualified_name") or row.get("name") or "<anonymous>"
    return f"{row['path']}:{row['line']}-{row['end_line']} ({name})"


def nonnegative(value: str) -> int:
    number = int(value)
    if number < 0:
        raise argparse.ArgumentTypeError("must be nonnegative")
    return number


def positive(value: str) -> int:
    number = nonnegative(value)
    if number == 0:
        raise argparse.ArgumentTypeError("must be positive")
    return number


def dollar_budget(value: str) -> float:
    from .review_cost import dollars

    try:
        number = float(dollars(value))
        if not math.isfinite(number) or number <= 0:
            raise ValueError("Dollar amounts must be finite positive numbers")
        return number
    except ValueError as error:
        raise argparse.ArgumentTypeError(str(error)) from error


def threshold(value: str) -> tuple[str, float]:
    key, separator, limit = value.partition("=")
    try:
        number = float(limit)
    except ValueError as error:
        raise argparse.ArgumentTypeError("expected metric=number, e.g. bugs=5") from error
    if not separator or key not in SORT_KEYS or not math.isfinite(number) or number < 0:
        raise argparse.ArgumentTypeError("use a sortable metric and a finite nonnegative limit")
    return key, number


def sql_dialect(value: str) -> str:
    from .sql import validate_dialect

    try:
        return validate_dialect(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(str(error)) from error


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="walleye",
        description="Scan code and rank function-level maintenance and control-flow hotspots.",
    )
    parser.add_argument("--version", action="version", version=f"walleye {__version__}")
    commands = parser.add_subparsers(dest="command", required=True)
    languages = commands.add_parser("languages", help="List auto-detected languages and grammars")
    languages.add_argument("--all", action="store_true", help="Include all 173 bundled grammars")
    languages.add_argument("--format", choices=("table", "json"), default="table")
    scanner = commands.add_parser("scan", help="Scan any local codebase or source file")
    scanner.add_argument("path", nargs="?", default=".", help="Local path or GitHub OWNER/REPO")
    scanner.add_argument("--ref", help="GitHub branch to scan (default: repository default branch)")
    scanner.add_argument("--format", choices=("table", "json", "csv"), default="table")
    scanner.add_argument("-o", "--output", type=Path, help="Write report atomically to a file")
    scanner.add_argument(
        "--sort",
        choices=SORT_KEYS,
        default="refactor_priority",
        help="Worst first (default: graph-weighted refactor_priority)",
    )
    scanner.add_argument(
        "--top",
        type=nonnegative,
        default=None,
        help="Rows to emit; 0 = all (default: 20 table, all JSON/CSV)",
    )
    scanner.add_argument(
        "--level",
        choices=("file", "function"),
        default="function",
        help="Report files or owned functions (default: function)",
    )
    scanner.add_argument(
        "--language",
        action="append",
        choices=sorted(LANGUAGES),
        default=[],
        metavar="LANG",
        help="Only scan this language; repeat to include others",
    )
    scanner.add_argument(
        "--map",
        action="append",
        default=[],
        metavar=".EXT=LANG",
        help="Override extension detection, e.g. .h=cpp or .v=verilog",
    )
    scanner.add_argument(
        "--exclude",
        action="append",
        default=[],
        metavar="PATTERN",
        help="Additional gitignore-style exclusion; repeat as needed",
    )
    scanner.add_argument("--include-tests", action="store_true")
    scanner.add_argument("--include-vendor", action="store_true")
    scanner.add_argument("--include-generated", action="store_true")
    scanner.add_argument(
        "--focus",
        metavar="PATH:LINE",
        help="Inspect the function containing a repository-relative source line",
    )
    scanner.add_argument(
        "--sql-dialect",
        type=sql_dialect,
        default="auto",
        metavar="DIALECT",
        help="SQLFluff dialect, e.g. sqlite, postgres, mysql; default auto tries common dialects",
    )
    scanner.add_argument(
        "--no-gitignore",
        action="store_true",
        help="Walk the filesystem without applying .gitignore",
    )
    scanner.add_argument(
        "--max-bytes",
        type=positive,
        default=2 * 1024 * 1024,
        help="Maximum bytes per file (default: 2097152)",
    )
    scanner.add_argument(
        "--fail-above",
        action="append",
        type=threshold,
        default=[],
        metavar="METRIC=LIMIT",
        help="CI exit 1 if ANY record exceeds a limit",
    )
    scanner.add_argument(
        "--allow-partial",
        action="store_true",
        help="Allow exit 0 despite reported parse/read/size issues",
    )
    reviewer = commands.add_parser(
        "review", help="Review distinct hotspots with agents and a dollar budget"
    )
    reviewer.add_argument("path", nargs="?", default=".", help="Local path or GitHub OWNER/REPO")
    reviewer.add_argument(
        "--ref", help="GitHub branch to review (default: repository default branch)"
    )
    reviewer.add_argument(
        "--issues", type=positive, default=10, help="Maximum findings (default: 10)"
    )
    reviewer.add_argument("--objective", choices=("bug", "refactor"), default="bug")
    reviewer.add_argument(
        "--budget",
        type=dollar_budget,
        metavar="USD",
        help="Run spending limit in US dollars (default: $5)",
    )
    reviewer.add_argument(
        "--prepare", action="store_true", help="Write packets without model calls"
    )
    reviewer.add_argument("--config", type=Path, help="Override ~/.config/walleye/config.json")
    reviewer.add_argument("-o", "--output", type=Path, help="New directory for packets and results")
    improver = commands.add_parser(
        "improve", help="Reproduce findings, propose fixes, verify tests, and compare scores"
    )
    improver.add_argument("path", help="Local repository, review.json, or GitHub issue URL")
    improver.add_argument(
        "--ref", help="GitHub base branch (default: branch recorded in the issue)"
    )
    improver.add_argument(
        "--issue", type=positive, help="Finding issue number when passing OWNER/REPO"
    )
    improver.add_argument(
        "--issues", type=positive, default=1, help="Maximum findings (default: 1)"
    )
    improver.add_argument("--objective", choices=("bug", "refactor"), default="bug")
    improver.add_argument(
        "--budget", type=dollar_budget, metavar="USD", help="Whole-run dollar budget"
    )
    improver.add_argument("--config", type=Path)
    improver.add_argument("-o", "--output", type=Path)
    applier = commands.add_parser("apply", help="Reverify and apply a saved candidate, then rescan")
    applier.add_argument("proposal", type=Path, help="Verified proposal directory or proposal.json")
    return parser


def render_table(report: dict, stream, *, terminal: bool = False):
    console = Console(file=stream, force_terminal=terminal, highlight=False)
    summary = report["summary"]
    scores = report.get("scores", {})
    console.print(Text(f"walleye {report['tool']['version']} · {report['root']}", style="bold"))
    console.print(
        Text(
            f"{summary['scanned_files']:,} files · {len(summary['languages'])} languages · "
            f"{summary['sloc']:,} source lines · {report['duration_seconds']:.2f}s"
        )
    )
    overall = scores.get("overall_score")
    mi = scores.get("maintainability_index")
    control = scores.get("complexity_score")
    score_text = f"Overall {overall:.1f}/100" if overall is not None else "Overall unavailable"
    score_text += f" ({scores.get('status', 'unknown')})"
    score_text += f" · Maintainability (MI) {_display_value(mi, 'maintainability_index')}"
    score_text += f" · control {_display_value(control, 'complexity_score')}"
    console.print(Text(score_text))
    console.print("Overall measures structure. Correctness is unreviewed; architecture is partial.")
    console.print(
        Text(
            "MI: 0–100, higher is easier to maintain (Visual Studio formula). "
            "Risk: 0–100 capped maintenance pressure, not bug probability. "
            "Priority: risk weighted by dependency impact."
        )
    )
    parser_counts = summary.get("parsers", {})
    if parser_counts:
        console.print(
            Text(
                "Parsed with: "
                + ", ".join(f"{name}={count:,}" for name, count in parser_counts.items())
            )
        )
    score_coverage = scores.get("coverage", {})
    parse_coverage = report.get("coverage", {})
    supported = score_coverage.get("supported_share")
    supported_text = "—" if supported is None else f"{supported * 100:.1f}%"
    owned = score_coverage.get("owned_sloc_share")
    owned_text = "—" if owned is None else f"{owned * 100:.1f}%"
    console.print(
        Text(
            "Heuristic scores (higher is better); risk is lower is better. "
            f"Parsed {parse_coverage.get('parsed_files', 0):,}/"
            f"{parse_coverage.get('candidate_files', 0):,} files · "
            f"complexity measured for {supported_text} of units; "
            f"units cover {owned_text} of source lines"
        )
    )
    distribution = scores.get("risk_distribution", {})
    high_risk = distribution.get("high_risk", {})
    console.print(
        Text(
            f"Risk tail: p95 {_display_value(distribution.get('p95'), 'risk_score')} · "
            f"max {_display_value(distribution.get('max'), 'risk_score')} · "
            f"high-risk ≥{high_risk.get('threshold', 70):g}: {high_risk.get('count', 0):,}"
        )
    )
    table = Table(show_header=True, header_style="bold", box=None, padding=(0, 1))
    wide = console.width >= 120
    selected = report["ranking"]["sort"]
    fields = report["records"][0] if report["records"] else {}
    metrics = (
        ["risk_score", "maintainability_index", "cyclomatic_complexity", "max_nesting"]
        if wide
        else [
            selected if selected in fields else "risk_score",
            "maintainability_index",
            "cyclomatic_complexity",
        ]
    )
    labels = {
        "sloc": "SLOC",
        "volume": "Volume",
        "difficulty": "Difficulty",
        "effort": "Effort",
        "bugs": "B estimate",
        "length": "Length",
        "time_seconds": "Time (s)",
        "risk_score": "Risk",
        "maintainability_index": "MI",
        "complexity_score": "Control score",
        "cyclomatic_complexity": "Cyclomatic",
        "control_branch_count": "Branches",
        "logical_branch_count": "Logical",
        "max_nesting": "Nesting",
        "refactor_priority": "Priority",
        "scenario_gain": "Gain",
        "fan_in": "Callers",
        "dependent_count": "Reach",
    }
    if wide and selected in fields and selected not in metrics:
        metrics.insert(-1, selected)
    table.add_column("#", justify="right", no_wrap=True)
    table.add_column("Source", overflow="fold", ratio=1)
    for metric in metrics:
        table.add_column(labels[metric], justify="right", no_wrap=True)
    for row in report["records"]:
        location = _format_function_location(row)
        source = Text(location)
        source.append(f"\n{row['language']}", style="dim")
        values = [_display_value(row.get(metric), metric) for metric in metrics]
        table.add_row(str(row["rank"]), source, *values)
    console.print(table)
    console.print(
        Text(
            f"Showing {len(report['records']):,} of {summary['records']:,} records, "
            f"sorted by {report['ranking']['sort']} descending."
        )
    )
    if summary["skipped"]:
        console.print(
            Text(
                "Excluded/skipped: "
                + ", ".join(f"{key}={value}" for key, value in summary["skipped"].items())
            )
        )
    render_callgraph_details(report, console)
    hotspots = (
        report.get("function_hotspots", []) if report["options"]["functions"] is False else []
    )
    if hotspots:
        console.print(Text("Function logic hotspots (highest risk first):", style="bold"))
        hotspot_table = Table(show_header=True, header_style="bold", box=None, padding=(0, 1))
        hotspot_table.add_column("#", justify="right", no_wrap=True)
        hotspot_table.add_column("Function", overflow="fold", ratio=1)
        hotspot_table.add_column("Risk", justify="right", no_wrap=True)
        hotspot_table.add_column("MI", justify="right", no_wrap=True)
        hotspot_table.add_column("Cyclomatic", justify="right", no_wrap=True)
        hotspot_table.add_column("Nesting", justify="right", no_wrap=True)
        hotspot_table.add_column("Structure", overflow="fold", ratio=1)
        for row in hotspots[:8]:
            structure = (
                "; ".join(
                    f"{item['type']}@{item['line']} d{item['nesting']}"
                    f" ({item['subtree_branches']} branches)"
                    for item in row.get("structure_hotspots", [])[:3]
                )
                or "no control branches"
            )
            hotspot_table.add_row(
                str(row.get("function_rank", "")),
                _format_function_location(row),
                _display_value(row.get("risk_score"), "risk_score"),
                _display_value(row.get("maintainability_index"), "maintainability_index"),
                _display_value(row.get("cyclomatic_complexity"), "cyclomatic_complexity"),
                _display_value(row.get("max_nesting"), "max_nesting"),
                structure,
            )
        console.print(hotspot_table)
        if len(hotspots) > 8:
            console.print(Text(f"Showing 8 of {len(hotspots):,} function hotspots."))
    language_scores = scores.get("by_language", {})
    if language_scores:
        console.print(Text("Scores by language:", style="bold"))
        language_table = Table(show_header=True, header_style="bold", box=None, padding=(0, 1))
        language_table.add_column("Language", no_wrap=True)
        language_table.add_column("Overall", justify="right", no_wrap=True)
        language_table.add_column("MI", justify="right", no_wrap=True)
        language_table.add_column("Control", justify="right", no_wrap=True)
        language_table.add_column("Coverage", justify="right", no_wrap=True)
        for language, values in language_scores.items():
            coverage = values.get("coverage", {}).get("supported_share")
            coverage_value = "—" if coverage is None else f"{coverage * 100:.1f}%"
            language_table.add_row(
                language,
                _display_value(values.get("overall_score"), "overall_score"),
                _display_value(values.get("maintainability_index"), "maintainability_index"),
                _display_value(values.get("complexity_score"), "complexity_score"),
                coverage_value,
            )
        console.print(language_table)
    console.print(
        Text("B = V / 3000 is a historical estimate, not a count of observed bugs.", style="dim")
    )
    console.print(
        Text(
            "Compare within one language and counting profile. "
            "Volume measures lexical information size; "
            "entropy and bug probability are not measured.",
            style="dim",
        )
    )


def render_metric_details(row: dict, console: Console):
    """Keep all twelve Halstead fields and MI visible at every terminal width."""

    def value(key):
        return _display_value(row.get(key), key)

    console.print(
        Text(
            f"  Maintainability (MI): {value('maintainability_index')}/100 · "
            f"Risk: {value('risk_score')}/100 · "
            f"SLOC: {value('sloc')} · Comment lines: {value('comment_lines')}"
        )
    )
    if row.get("cyclomatic_complexity") is not None:
        console.print(
            Text(
                f"  Cyclomatic: {value('cyclomatic_complexity')} = 1 + "
                f"{value('control_branch_count')} control decisions + "
                f"{value('logical_branch_count')} logical decisions · "
                f"Nesting: {value('max_nesting')}"
            )
        )
    else:
        console.print(Text("  MI/control-flow unavailable; risk uses Halstead size pressure only."))
    console.print(
        Text(
            f"  Halstead counts: distinct operators={value('distinct_operators')}, "
            f"distinct operands={value('distinct_operands')}; "
            f"total operators={value('total_operators')}, total operands={value('total_operands')}"
        )
    )
    console.print(
        Text(
            f"  Vocabulary: {value('vocabulary')} · Length: {value('length')} · "
            f"Calculated length: {value('calculated_length')}"
        )
    )
    console.print(
        Text(
            f"  Volume: {value('volume')} · Difficulty: {value('difficulty')} · "
            f"Effort: {value('effort')}"
        )
    )
    console.print(
        Text(
            f"  Time estimate: {value('time_seconds')} s · "
            f"Delivered-bug estimate (B): {value('bugs')}"
        )
    )


def render_callgraph_details(report: dict, console: Console):
    graph = report.get("callgraph")
    if not graph:
        return
    coverage = graph["coverage"]
    console.print(
        Text(
            f"Callgraph: {coverage.get('resolved_calls', 0):,}/{coverage['call_sites']:,} "
            "call sites resolved; "
            f"{coverage['unresolved_calls']:,} dynamic/external/ambiguous calls unresolved.",
            style="bold",
        )
    )
    index = {node["id"]: node for node in graph["nodes"]}
    quality = graph["functions"]["quality_score"]
    if quality is not None:
        console.print(
            Text(f"Graph-weighted function quality: {quality:.2f}/100 (scenario baseline).")
        )
    for row in report["records"]:
        console.print(Text(f"\n#{row['rank']} {_format_function_location(row)}", style="bold"))
        render_metric_details(row, console)
        if row["kind"] != "function":
            continue
        if row.get("graph_status") == "unsupported":
            console.print(Text("  Callgraph adapter unavailable for this language."))
            continue
        console.print(
            Text(
                f"  {row['fan_in']} callers · {row['fan_out']} callees · "
                f"{row['dependent_count']} transitive dependents · "
                f"cycle size {row['cycle_size']} · "
                f"hypothetical gain {row['scenario_gain']:.6f} points"
            )
        )
        for label, key, endpoint in (
            ("Called by", "callers", "source"),
            ("Calls", "callees", "target"),
        ):
            items = row.get(key, [])
            for edge in items[:3]:
                other = index.get(edge[endpoint])
                name = other["name"] if other else "<module>"
                location = f"{other['path']}:{other['line']}" if other else edge["path"]
                console.print(
                    Text(f"  {label}: {name} ({location}); call site {edge['path']}:{edge['line']}")
                )
            if len(items) > 3:
                console.print(Text(f"  {label}: +{len(items) - 3} call sites in JSON"))
        for branch in row.get("structure_hotspots", [])[:3]:
            console.print(
                Text(
                    f"  Branch: {row['path']}:{branch['line']}-{branch['end_line']} "
                    f"{branch['type']}; nesting {branch['nesting']}; "
                    f"{branch['subtree_branches']} branches"
                )
            )
        for action in row.get("refactor_actions", [])[:2]:
            console.print(Text(f"  Refactor @{action['line']}: {action['reason']}"))
        if row.get("unresolved_calls"):
            console.print(Text(f"  {row['unresolved_calls']} unresolved calls in this function."))
    console.print(
        Text(
            "Gain assumes this candidate's risk drops 25%, with the dependency graph fixed. "
            "It is a refactor scenario, not a measured improvement.",
            style="dim",
        )
    )


def render(report: dict, format: str, stream, *, terminal: bool = False):
    if format == "json":
        json.dump(report, stream, indent=2, ensure_ascii=True, allow_nan=False)
        stream.write("\n")
    elif format == "csv":
        writer = csv.DictWriter(stream, fieldnames=CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        rows = []
        for row in report["records"]:
            serialized = dict(row)
            for key in (
                "structure_hotspots",
                "risk_components",
                "callers",
                "callees",
                "refactor_actions",
            ):
                if key in serialized:
                    serialized[key] = json.dumps(serialized[key], separators=(",", ":"))
            rows.append(serialized)
        writer.writerows(rows)
    else:
        render_table(report, stream, terminal=terminal)


def write_report(report: dict, args):
    if args.output is None:
        render(report, args.format, sys.stdout, terminal=sys.stdout.isatty())
        return
    args.output.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="",
            dir=args.output.parent,
            delete=False,
        ) as stream:
            temporary = Path(stream.name)
            render(report, args.format, stream)
        os.replace(temporary, args.output)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "languages":
        names = sorted(LANGUAGES if args.all else EXTENSIONS)
        rows = [
            {"language": name, "extensions": EXTENSIONS.get(name, "").split()} for name in names
        ]
        if args.format == "json":
            print(json.dumps(rows, indent=2))
        else:
            for row in rows:
                print(f"{row['language']:18} {' '.join(row['extensions']) or '(use --map)'}")
            print(f"\n{len(rows)} languages; 173 grammars bundled, no runtime downloads.")
        return 0
    try:
        from .github_cli import prepare_remote, run_improve

        remote = prepare_remote(args)
        if remote and args.command == "improve":
            return run_improve(args, *remote)
        if args.command in {"improve", "apply"}:
            from .review import load_config
            from .workflow import apply_proposal, improve

            console = Console(highlight=False)
            if args.command == "apply":
                proposal, card = apply_proposal(args.proposal)
                console.print(
                    Text(f"Applied {proposal['target']['path']} · verified and rescanned")
                )
                delta = card["maintainability"]["observed_repository"]["score"]
                console.print(
                    f"Repository structural quality: {delta['before']} → {delta['after']}"
                )
                return 0
            config = load_config(args.config)
            if args.budget is not None:
                config = replace(config, budget_usd=args.budget)
            manifest, output = improve(
                args.path,
                issues=args.issues,
                objective=args.objective,
                config=config,
                output=args.output,
                progress=lambda message: console.print(Text(message)),
            )
            for proposal in manifest["proposals"]:
                console.print(
                    Text(
                        f"\n{proposal['target']['path']}:{proposal['target']['line']} "
                        f"· {proposal['status']}"
                    )
                )
                if proposal.get("scorecard"):
                    card = json.loads(
                        (output / "proposals" / proposal["id"] / "scorecard.json").read_text()
                    )
                    table = Table("Measurement", "Before", "Candidate", "Change")
                    measurements = [
                        (
                            "Confirmed open defects (this finding)",
                            card["correctness"]["confirmed_open"],
                        ),
                        ("Targeted tests passing", card["correctness"]["tests_passing"]),
                        ("Target structural quality ↑", card["maintainability"]["target_quality"]),
                        (
                            "Target maintainability index ↑",
                            card["maintainability"]["target"]["maintainability_index"],
                        ),
                        (
                            "Target cyclomatic complexity ↓",
                            card["maintainability"]["target"]["cyclomatic_complexity"],
                        ),
                        (
                            "Repository structural quality ↑",
                            card["maintainability"]["repository"]["score"],
                        ),
                        (
                            "Resolved function cycles",
                            card["architecture"]["changes"]["function_cycles"],
                        ),
                        (
                            "Resolved module cycles",
                            card["architecture"]["changes"]["module_cycles"],
                        ),
                    ]
                    for label, value in measurements:
                        table.add_row(
                            label, str(value["before"]), str(value["after"]), str(value["delta"])
                        )
                    console.print(table)
                    console.print(Text(card["scope"]["quality_comparison"] + "."))
                    console.print(
                        "Architecture is partial. Project integration tests were not run."
                    )
                    if proposal["status"] == "verified-candidate":
                        console.print(
                            Text(f"Apply: walleye apply {output / 'proposals' / proposal['id']}")
                        )
                if proposal.get("error"):
                    console.print(Text(proposal["error"]))
            cost = manifest["cost"]
            console.print(
                f"\n{manifest['usage']['calls']} model calls · "
                f"{manifest['usage']['total_tokens']:,} measured tokens · "
                f"${cost['spent_usd']:.6f} / ${cost['budget_usd']:.2f} ({cost['backend']})"
            )
            if cost["unknown"] or manifest["usage"]["unknown"]:
                console.print(
                    "Total tokens and cost are unknown; figures above include reported usage only."
                )
            console.print(Text(f"Report: {output / 'report.md'}"))
            console.print(Text(f"Full workflow: {output / 'workflow.json'}"))
            if not manifest["scan"]["complete"]:
                console.print(
                    f"Scan incomplete: {len(manifest['scan']['issues'])} diagnostics retained."
                )
            if manifest.get("workflow_stop_reason"):
                console.print(Text(f"Stopped: {manifest['workflow_stop_reason']}"))
            verified = any(p["status"] == "verified-candidate" for p in manifest["proposals"])
            return (
                0
                if verified
                and manifest["scan"]["complete"]
                and not manifest.get("workflow_stop_reason")
                else 2
            )
        if args.command == "review":
            from .review import load_config, prepare_review
            from .review_agent import run_review

            config = load_config(args.config)
            if args.budget is not None:
                config = replace(config, budget_usd=args.budget)
            console = Console(stderr=True, highlight=False)

            def progress(message):
                console.print(Text(message))

            manifest, packets, index, output = prepare_review(
                args.path,
                args.issues,
                args.objective,
                config,
                args.output,
                progress,
            )
            if not args.prepare and packets:
                manifest = run_review(manifest, packets, index, output, config, progress=progress)
            if remote:
                from .github_cli import publish_review

                publish_review(remote, manifest, packets, output, prepared=args.prepare)
            table = Table()
            table.add_column("Task")
            table.add_column("Target", overflow="fold")
            table.add_column("Priority")
            table.add_column("Packet ≈tokens")
            table.add_column("Status")
            statuses = {i["task_id"]: i["status"] for i in manifest["investigations"]}
            for task in manifest["tasks"]:
                target = task["target"]
                table.add_row(
                    task["task_id"],
                    Text(
                        f"{target['path']}:{target['line']}-{target['end_line']} ({target['name']})"
                    ),
                    f"{task['priority']:.1f}",
                    str(task["estimated_context_tokens"]),
                    statuses.get(task["task_id"], "prepared" if args.prepare else "queued"),
                )
            Console(highlight=False).print(table)
            for finding in manifest["findings"]:
                print(f"\n[{finding['severity']}] {finding['title']}")
                print(finding["root_cause"])
                for key, label in (
                    ("trigger", "Trigger"),
                    ("expected_behavior", "Expected"),
                    ("actual_behavior", "Actual"),
                    ("proposed_change", "Change"),
                    ("preserved_behavior", "Preserves"),
                    ("expected_benefit", "Benefit"),
                ):
                    if finding[key]:
                        print(f"  {label}: {finding[key]}")
                for evidence in finding["evidence"]:
                    print(f"  {evidence['path']}:{evidence['line']}-{evidence['end_line']}")
                print(f"  Validation: {finding['validation']}")
            print(
                f"\n{len(manifest['findings'])}/{args.issues} findings · "
                f"{manifest['usage']['calls']} model calls · "
                f"{manifest['usage']['total_tokens']:,} measured tokens"
            )
            cost = manifest["cost"]
            cost_label = (
                "Estimated API cost"
                if cost["backend"] == "api"
                else "API-equivalent cost (Codex; soft limit)"
            )
            print(f"{cost_label}: ${cost['spent_usd']:.4f} / ${cost['budget_usd']:.2f} budget")
            print(
                "Prepared; no agents started."
                if args.prepare
                else f"Stopped: {manifest['stop_reason'] or 'no eligible targets'}."
            )
            print(f"Packets and full results: {output / 'review.json'}")
            if manifest["usage"]["unknown"]:
                progress("Usage is incomplete; reported tokens do not include the failed call.")
                progress(f"Cost is incomplete; ${cost['reserved_usd']:.4f} remains reserved.")
            if not manifest["scan"]["complete"]:
                progress(
                    f"Scan incomplete: {len(manifest['scan']['issues'])} diagnostics saved. "
                    "Only successfully parsed source was eligible."
                )
            if manifest["status"] == "incomplete":
                for investigation in manifest["investigations"]:
                    if investigation.get("error"):
                        progress(investigation["error"])
            if not packets:
                progress(
                    "No eligible review targets; see ranking and scan coverage in review.json."
                )
            return (
                2
                if not packets
                or manifest["status"] == "incomplete"
                or not manifest["scan"]["complete"]
                else 0
            )
        mappings = dict(parse_mapping(mapping) for mapping in args.map)
        options = ScanOptions(
            include_tests=args.include_tests,
            include_vendor=args.include_vendor,
            include_generated=args.include_generated,
            respect_gitignore=not args.no_gitignore,
            exclude=args.exclude,
            languages=set(args.language),
            mappings=mappings,
            max_bytes=args.max_bytes,
            functions=args.level == "function",
            sql_dialect=args.sql_dialect,
        )
        report = scan(args.path, options)
        if remote:
            report["github"] = {
                "repository": remote[0].repository.full_name,
                "commit": remote[1].sha,
                "branch": remote[1].base_branch,
            }
        if args.focus:
            focus_path, separator, focus_line = args.focus.rpartition(":")
            if not separator or not focus_line.isdigit() or int(focus_line) < 1:
                raise ValueError("--focus expects a repository-relative PATH:LINE")
            report["records"] = [
                row
                for row in report["records"]
                if row["path"] == focus_path and row["line"] <= int(focus_line) <= row["end_line"]
            ]
            if not report["records"]:
                raise ValueError(f"No analyzed function at {args.focus}")
        report["records"].sort(key=lambda row: record_sort_key(row, args.sort))
        for rank, row in enumerate(report["records"], 1):
            row["rank"] = rank
        breaches = [
            {
                "path": row["path"],
                "line": row["line"],
                "metric": metric,
                "value": row[metric],
                "limit": limit,
            }
            for row in report["records"]
            for metric, limit in args.fail_above
            if row.get(metric) is not None and row[metric] > limit
        ]
        top = args.top if args.top is not None else (20 if args.format == "table" else 0)
        report["ranking"] = {"sort": args.sort, "order": "descending", "top": top}
        report["threshold_breaches"] = breaches
        if top:
            report["records"] = report["records"][:top]
        write_report(report, args)
        console = Console(stderr=True, highlight=False)
        grouped = defaultdict(list)
        for issue in report["issues"]:
            grouped[(issue["kind"], issue["path"])].append(issue)
        for (kind, path), issues in list(grouped.items())[:20]:
            issue = issues[0]
            suffix = f" (+{len(issues) - 1} more diagnostics)" if len(issues) > 1 else ""
            console.print(
                Text(f"{kind}: {path}:{issue.get('line', 1)}: {issue['message']}{suffix}")
            )
        if len(grouped) > 20:
            console.print(
                Text(f"… {len(grouped) - 20} more affected files (full diagnostics in JSON).")
            )
        if args.output:
            console.print(
                Text(
                    f"Wrote {len(report['records'])} records to {args.output} "
                    f"({report['summary']['scanned_files']} files, "
                    f"{len(report['issues'])} diagnostics)."
                )
            )
        if breaches:
            console.print(Text(f"{len(breaches)} threshold breaches across the full scan."))
        if not report["summary"]["records"]:
            console.print("No analyzable records found; check language filters and exclusions.")
            return 2
        if report["issues"] and not args.allow_partial:
            return 2
        return 1 if breaches else 0
    except (OSError, ValueError) as error:
        if isinstance(error, BrokenPipeError):
            return 0
        Console(stderr=True).print(Text(f"walleye: {error}"))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
