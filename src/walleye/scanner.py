"""Scanner API shared by terminal and structured reports."""

import hashlib
import re
import time
from collections import Counter, defaultdict
from dataclasses import asdict
from datetime import datetime, timezone
from functools import lru_cache
from importlib.metadata import version
from math import log
from pathlib import Path

from tree_sitter_language_pack import get_parser

from . import __version__
from .callgraph import build_graph, collect_facts
from .complexity import Complexity, measure_complexity
from .discovery import GENERATED_HEADER, ScanOptions, discover
from .metrics import (
    PROFILE,
    function_depth,
    function_name,
    function_owner,
    function_parent,
    is_comment,
    is_function,
    measure,
    qualified_function_name,
    walk,
)
from .sql import analyze_sql

QUALITY_PROFILE = "declank-quality-v1"
HIGH_RISK_THRESHOLD = 70.0


@lru_cache(maxsize=200)
def parser_for(language: str):
    return get_parser(language)


def _rust_test_nodes(root, source: bytes) -> set[int]:
    """Omit #[cfg(test)] items and explicit test functions, including attributes."""
    excluded = set()
    test_attribute = re.compile(
        rb"^#\[\s*(?:cfg\s*\(\s*test\s*\)|(?:[\w]+::)?test(?:\([^]]*\))?)\s*\]$"
    )
    for node in walk(root):
        if node.type != "attribute_item":
            continue
        if not test_attribute.match(source[node.start_byte : node.end_byte]):
            continue
        sibling = node
        while sibling is not None:
            excluded.add(sibling.id)
            if sibling.type != "attribute_item":
                break
            sibling = sibling.next_named_sibling
    return excluded


def _end_line(node) -> int:
    return node.end_point.row + (1 if node.end_point.column else 0)


def _bounded(value: float) -> float:
    return min(100.0, max(0.0, value))


def _descending_number(row: dict, field: str) -> float:
    value = row.get(field)
    return -float(value) if isinstance(value, (int, float)) else float("inf")


def risk_sort_key(row: dict) -> tuple:
    """Sort risk rows with useful deterministic tie-breakers.

    Risk is deliberately capped at 100.  Once the cap is reached, raw control
    pressure and size keep the most difficult methods ahead of an alphabetic
    path tie.
    """

    return (
        _descending_number(row, "risk_score"),
        _descending_number(row, "cyclomatic_complexity"),
        _descending_number(row, "max_nesting"),
        _descending_number(row, "volume"),
        row.get("path", ""),
        row.get("line", 0),
    )


def maintainability_index(volume: float, sloc: int, cyclomatic: int | None) -> float | None:
    """Return the Visual Studio 0..100 maintainability-index form.

    The zero-volume case is treated as a clean empty unit because the original
    logarithmic formula has no defined value at zero.  A missing cyclomatic
    value means MI is unknown; unsupported control-flow adapters must not be
    silently turned into ``M=1``.
    """

    if cyclomatic is None:
        return None
    if volume <= 0 and sloc <= 0:
        return 100.0
    raw = 171.0 - 5.2 * log(max(volume, 1.0)) - 0.23 * max(cyclomatic, 1) - 16.2 * log(max(sloc, 1))
    return _bounded(raw * 100.0 / 171.0)


def complexity_score(complexity: Complexity) -> float | None:
    """Convert supported control-flow pressure to a 0..100 readability score."""

    if complexity.cyclomatic is None or complexity.max_nesting is None:
        return None
    branch_pressure = min(1.0, max(0.0, (complexity.cyclomatic - 1) / 10.0))
    nesting_pressure = min(1.0, max(0.0, complexity.max_nesting / 5.0))
    return _bounded(100.0 * (1.0 - 0.70 * branch_pressure - 0.30 * nesting_pressure))


def _decorate_scores(record: dict) -> None:
    mi = maintainability_index(
        record["volume"], record["sloc"], record.get("cyclomatic_complexity")
    )
    control_score = complexity_score(
        Complexity(
            record["complexity_status"],
            record.get("cyclomatic_complexity"),
            record.get("control_branch_count"),
            record.get("logical_branch_count"),
            record.get("max_nesting"),
            (),
            record.get("complexity_adapter"),
        )
    )
    # A separately named size pressure keeps unsupported languages rankable
    # without presenting it as a maintainability-index or complexity score.
    halstead_risk = _bounded(100.0 * min(1.0, log(1.0 + record["volume"]) / log(10001.0)))
    maintainability_risk = None if mi is None else 100.0 - mi
    complexity_risk = None if control_score is None else 100.0 - control_score
    # Unsupported complexity still gets a useful size-risk ranking, but the
    # missing MI and complexity components remain visible in the row.
    if maintainability_risk is None:
        risk = halstead_risk
    elif complexity_risk is None:
        risk = maintainability_risk
    else:
        risk = 0.65 * maintainability_risk + 0.35 * complexity_risk
    record.update(
        {
            "maintainability_index": None if mi is None else round(mi, 4),
            "halstead_risk_score": round(halstead_risk, 4),
            "complexity_score": None if control_score is None else round(control_score, 4),
            "risk_score": round(_bounded(risk), 4),
            "risk_components": {
                "maintainability_risk": (
                    None if maintainability_risk is None else round(maintainability_risk, 4)
                ),
                "halstead_risk": round(halstead_risk, 4),
                "complexity_risk": None if complexity_risk is None else round(complexity_risk, 4),
            },
        }
    )


def _function_nodes(root, excluded: set[int]) -> list:
    nodes = []
    stack = [root]
    while stack:
        node = stack.pop()
        if is_comment(node) or node.id in excluded:
            continue
        if is_function(node):
            nodes.append(node)
        stack.extend(reversed(node.named_children))
    return nodes


def _decorate_sql_record(record: dict) -> dict:
    """Add the common report fields to the SQLFluff file adapter's row."""

    unsupported = Complexity("unsupported", None, None, None, None, (), None)
    record.update(
        {
            "qualified_name": None,
            "parent_function": None,
            "owner_function": None,
            "function_depth": None,
            "ownership": "file",
            **unsupported.to_dict(),
        }
    )
    _decorate_scores(record)
    return record


def _record_from_measurement(
    node,
    source: bytes,
    path: str,
    language: str,
    measurement,
    complexity: Complexity,
    *,
    functions: bool,
) -> dict:
    record = {
        "path": path,
        "language": language,
        "kind": "function" if functions else "file",
        "name": function_name(node, source) if functions else None,
        "qualified_name": qualified_function_name(node, source) if functions else None,
        "parent_function": function_parent(node, source) if functions else None,
        "owner_function": None,
        "function_depth": function_depth(node) if functions else None,
        "ownership": (
            "nested-function" if functions and function_depth(node) else "top-level-function"
        )
        if functions
        else "file",
        "line": node.start_point.row + 1 if functions else 1,
        "end_line": max(node.start_point.row + 1, _end_line(node))
        if functions
        else len(source.splitlines()),
        "sloc": measurement.sloc,
        "comment_lines": measurement.comment_lines,
        "opaque_bytes": measurement.opaque_bytes,
        **measurement.halstead.to_dict(),
        **complexity.to_dict(),
    }
    _decorate_scores(record)
    return record


def _function_record(node, source: bytes, path: str, language: str, nonblank, excluded) -> dict:
    measurement = measure(
        node,
        source,
        exclude_nested=True,
        nonblank=nonblank,
        excluded_nodes=excluded,
    )
    record = _record_from_measurement(
        node,
        source,
        path,
        language,
        measurement,
        measure_complexity(
            node,
            language,
            exclude_nested=True,
            excluded_nodes=excluded,
        ),
        functions=True,
    )
    record["owner_function"] = function_owner(node, source) or record["qualified_name"]
    return record


def _file_record(root, source: bytes, path: str, language: str, nonblank, excluded) -> dict:
    measurement = measure(
        root,
        source,
        exclude_nested=False,
        nonblank=nonblank,
        excluded_nodes=excluded,
    )
    return _record_from_measurement(
        root,
        source,
        path,
        language,
        measurement,
        measure_complexity(
            root,
            language,
            exclude_nested=False,
            excluded_nodes=excluded,
        ),
        functions=False,
    )


def _parse_errors(root, path: str) -> list[dict]:
    if not root.has_error:
        return []
    errors = [
        {
            "path": path,
            "kind": "parse",
            "line": node.start_point.row + 1,
            "column": node.start_point.column + 1,
            "message": f"{'Missing' if node.is_missing else 'Unexpected'} {node.type}",
        }
        for node in walk(root)
        if node.is_error or node.is_missing
    ]
    return errors[:25] or [{"path": path, "kind": "parse", "message": "Incomplete syntax tree"}]


def analyze_units(
    source: bytes,
    language: str,
    path: str,
    *,
    include_tests: bool = False,
    sql_dialect: str = "auto",
    graph_facts: list | None = None,
) -> tuple[list, list, list]:
    """Parse once and return file records, function records and diagnostics."""

    if language == "sql":
        file_records, errors = analyze_sql(
            source,
            path,
            dialect=sql_dialect,
            functions=False,
        )
        if errors:
            return [], [], errors
        return [_decorate_sql_record(record) for record in file_records], [], []
    source.decode("utf-8")  # Fail explicitly instead of silently replacing undecodable source.
    tree = parser_for(language).parse(source)
    root = tree.root_node
    errors = _parse_errors(root, path)
    parser_name, profile = "tree-sitter", PROFILE
    if errors and language == "bash":
        from .shell import PROFILE as SHELL_PROFILE
        from .shell import parse_compatible

        compatible = parse_compatible(source, parser_for(language))
        if compatible is not None:
            root, errors = compatible, []
            parser_name, profile = "tree-sitter+bash-validation", SHELL_PROFILE
    if errors and language in {"javascript", "typescript", "tsx"}:
        from .javascript import PROFILE as BABEL_PROFILE
        from .javascript import parse as parse_javascript

        root, errors = parse_javascript(source, language, path)
        parser_name, profile = "babel", BABEL_PROFILE
    if errors:
        return [], [], errors
    nonblank = {i for i, line in enumerate(source.splitlines()) if line.strip()}
    excluded = _rust_test_nodes(root, source) if language == "rust" and not include_tests else set()
    file_records = [_file_record(root, source, path, language, nonblank, excluded)]
    function_nodes = _function_nodes(root, excluded)
    function_records = [
        _function_record(node, source, path, language, nonblank, excluded)
        for node in function_nodes
    ]
    for record in file_records + function_records:
        record.update(parser=parser_name, profile=profile, dialect=None)
    if graph_facts is not None:
        graph_facts.append(
            collect_facts(root, source, path, language, function_nodes, function_records, excluded)
        )
    return file_records, function_records, []


def analyze(
    source: bytes,
    language: str,
    path: str,
    *,
    functions: bool = False,
    include_tests: bool = False,
    sql_dialect: str = "auto",
) -> tuple[list, list]:
    """Return records and diagnostics. A malformed file never receives a clean score."""

    files, function_records, errors = analyze_units(
        source,
        language,
        path,
        include_tests=include_tests,
        sql_dialect=sql_dialect,
    )
    return (function_records if functions else files), errors


def _percentile(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    values = sorted(values)
    if len(values) == 1:
        return round(values[0], 4)
    position = (len(values) - 1) * percentile
    lower = int(position)
    upper = min(lower + 1, len(values) - 1)
    fraction = position - lower
    value = values[lower] + (values[upper] - values[lower]) * fraction
    return round(value, 4)


def _distribution(records: list[dict], metric: str = "risk_score") -> dict:
    values = [float(row[metric]) for row in records if row.get(metric) is not None]
    high_risk = [value for value in values if value >= HIGH_RISK_THRESHOLD]
    return {
        "count": len(values),
        "p50": _percentile(values, 0.50),
        "p90": _percentile(values, 0.90),
        "p95": _percentile(values, 0.95),
        "max": round(max(values), 4) if values else None,
        "high_risk": {
            "threshold": HIGH_RISK_THRESHOLD,
            "count": len(high_risk),
            "share": round(len(high_risk) / len(values), 4) if values else None,
        },
    }


def _weighted_average(records: list[dict], metric: str, weight: str = "sloc") -> float | None:
    usable = [row for row in records if row.get(metric) is not None]
    if not usable:
        return None
    weights = [max(float(row.get(weight) or 0), 1.0) for row in usable]
    total = sum(weights)
    return round(
        sum(float(row[metric]) * w for row, w in zip(usable, weights, strict=True)) / total,
        4,
    )


def _complexity_status(records: list[dict]) -> str:
    statuses = {row.get("complexity_status") for row in records}
    if not statuses or statuses == {"unsupported"}:
        return "unsupported"
    if statuses == {"supported"}:
        return "supported"
    return "partial"


def _score_bundle(files: list[dict], functions: list[dict]) -> dict:
    # Every function record owns an exclusive body because its measurement
    # excludes nested function subtrees. Include nested records here: they are
    # disjoint units and a difficult closure must affect repository scores. A
    # file with no recognized function is the only case where its file scope
    # becomes a quality unit.
    function_counts = Counter(row["path"] for row in functions)
    functionless_files = [row for row in files if not function_counts[row["path"]]]
    quality_units = functions + functionless_files
    complexity_units = quality_units
    supported_complexity = [
        row for row in complexity_units if row.get("complexity_score") is not None
    ]
    supported_mi = [row for row in quality_units if row.get("maintainability_index") is not None]
    unit_mi = _weighted_average(quality_units, "maintainability_index")
    control_score = _weighted_average(supported_complexity, "complexity_score")
    status = _complexity_status(complexity_units)
    if unit_mi is None:
        overall = None
    elif control_score is None:
        overall = unit_mi
    else:
        overall = round(0.65 * unit_mi + 0.35 * control_score, 4)
    owned_sloc = sum(row["sloc"] for row in quality_units)
    file_sloc = sum(row["sloc"] for row in files)
    nested_count = sum(row.get("function_depth", 0) > 0 for row in functions)
    return {
        "profile": QUALITY_PROFILE,
        "overall_score": overall,
        "maintainability_index": unit_mi,
        "complexity_score": control_score,
        "status": status,
        "components": {
            "maintainability_index": {
                "score": unit_mi,
                "weight": 0.65 if control_score is not None else 1.0,
                "formula": (
                    "171 - 5.2 ln(volume) - 0.23 cyclomatic - 16.2 ln(sloc), scaled to 0..100"
                ),
                "aggregation": (
                    "SLOC-weighted mean of exclusive function units; "
                    "functionless files use file scope"
                ),
            },
            "complexity": {
                "score": control_score,
                "weight": 0.35 if control_score is not None else 0.0,
                "formula": "100 * (1 - 0.70*min((M-1)/10,1) - 0.30*min(max_nesting/5,1))",
                "aggregation": "SLOC-weighted mean of the same exclusive ownership units",
                "status": status,
            },
        },
        "coverage": {
            "complexity_units": len(complexity_units),
            "supported": sum(
                row.get("complexity_status") == "supported" for row in complexity_units
            ),
            "partial": sum(row.get("complexity_status") == "partial" for row in complexity_units),
            "unsupported": sum(
                row.get("complexity_status") == "unsupported" for row in complexity_units
            ),
            "supported_share": round(len(supported_complexity) / len(complexity_units), 4)
            if complexity_units
            else None,
            "maintainability_units": len(quality_units),
            "maintainability_supported": len(supported_mi),
            "maintainability_unknown": len(quality_units) - len(supported_mi),
            "maintainability_supported_share": (
                round(len(supported_mi) / len(quality_units), 4) if quality_units else None
            ),
            "owned_sloc": owned_sloc,
            "file_sloc": file_sloc,
            "owned_sloc_share": round(owned_sloc / file_sloc, 4) if file_sloc else None,
        },
        "risk_distribution": _distribution(quality_units),
        "ownership": {
            "aggregate_units": "exclusive function scopes plus files without recognized functions",
            "top_level_functions": sum(row.get("function_depth") == 0 for row in functions),
            "nested_functions_reported": nested_count,
            "nested_functions_in_aggregate": True,
            "nested_functions_are_disjoint": True,
        },
    }


def _language_scores(files: list[dict], functions: list[dict]) -> dict:
    languages = sorted({row["language"] for row in files})
    result = {}
    for language in languages:
        language_files = [row for row in files if row["language"] == language]
        language_functions = [row for row in functions if row["language"] == language]
        result[language] = _score_bundle(language_files, language_functions)
        result[language]["files"] = len(language_files)
        result[language]["sloc"] = sum(row["sloc"] for row in language_files)
    return result


def _coverage(discovery, scanned: Counter, files: list[dict], functions: list[dict]) -> dict:
    function_files = {row["path"] for row in functions}
    nested = sum(row.get("function_depth", 0) > 0 for row in functions)
    statuses = Counter(row.get("complexity_status") for row in functions)
    return {
        "candidate_files": len(discovery.files),
        "parsed_files": len(files),
        "scanned_files": sum(scanned.values()),
        "file_parse_share": round(len(files) / len(discovery.files), 4)
        if discovery.files
        else None,
        "files_with_functions": len(function_files),
        "recognized_functions": len(functions),
        "nested_functions": nested,
        "top_level_functions": len(functions) - nested,
        "complexity_by_function_status": dict(sorted(statuses.items())),
        "languages": dict(sorted(scanned.items())),
    }


def scan(
    target: Path,
    options: ScanOptions | None = None,
    *,
    source_snapshot: dict[str, bytes] | None = None,
) -> dict:
    options = options or ScanOptions()
    target = target.expanduser().absolute()
    if target.is_symlink():
        raise ValueError("Scan a real file or directory, not a symbolic link")
    if not target.exists():
        raise ValueError(f"Path does not exist: {target}")
    if not (target.is_file() or target.is_dir()):
        raise ValueError(f"Not a regular file or directory: {target}")
    target = target.resolve()
    started = time.monotonic()
    discovery = discover(target, options)
    files, functions, issues = [], [], list(discovery.issues)
    graph_facts = []
    skipped = discovery.skipped.copy()
    scanned = Counter()
    fingerprint = hashlib.sha256()
    source_hashes = {}
    for path, relative, language in discovery.files:
        try:
            # Bound the actual read, including files that grow after discovery.
            with path.open("rb") as stream:
                source = stream.read(options.max_bytes + 1)
            if len(source) > options.max_bytes:
                skipped["oversized"] += 1
                issues.append({"path": relative, "kind": "size", "message": "Exceeds --max-bytes"})
                continue
            if not options.include_generated and GENERATED_HEADER.search(source[:2048]):
                skipped["generated-header"] += 1
                continue
            if not options.include_generated and language in {"javascript", "typescript", "tsx"}:
                lengths = [len(line) for line in source.splitlines() if line.strip()]
                if lengths and max(lengths) > 5000 and sum(lengths) / len(lengths) > 200:
                    skipped["minified-heuristic"] += 1
                    continue
            if b"\0" in source:
                offset = source.index(b"\0")
                skipped["read-or-parser-error"] += 1
                issues.append(
                    {
                        "path": relative,
                        "kind": "read-or-parser",
                        "line": source[:offset].count(b"\n") + 1,
                        "column": offset - source.rfind(b"\n", 0, offset),
                        "message": "Binary content (NUL byte); file left unscored",
                    }
                )
                continue
            fingerprint.update(relative.encode("utf-8", errors="surrogateescape") + b"\0")
            fingerprint.update(hashlib.sha256(source).digest())
            found_files, found_functions, errors = analyze_units(
                source,
                language,
                relative,
                include_tests=options.include_tests,
                sql_dialect=options.sql_dialect,
                graph_facts=graph_facts,
            )
            if errors:
                skipped["parse-error"] += 1
                issues.extend(errors)
            else:
                scanned[language] += 1
                files.extend(found_files)
                functions.extend(found_functions)
                source_hashes[relative] = hashlib.sha256(source).hexdigest()
                if source_snapshot is not None:
                    source_snapshot[relative] = source
        except (OSError, UnicodeError, ValueError, RuntimeError, LookupError) as error:
            skipped["read-or-parser-error"] += 1
            issues.append({"path": relative, "kind": "read-or-parser", "message": str(error)})

    graph = build_graph(graph_facts, files, functions)
    by_file_functions = Counter(row["path"] for row in functions)
    by_file_top_level = Counter(row["path"] for row in functions if row.get("function_depth") == 0)
    for row in files:
        row["function_count"] = by_file_functions[row["path"]]
        row["top_level_function_count"] = by_file_top_level[row["path"]]
    for row in functions:
        row["function_rank"] = None

    # Areas are always based on one file record per file, regardless of report
    # level, so directory totals cannot change merely by switching views.
    directories = defaultdict(lambda: {"files": set(), "bugs": 0.0, "volume": 0.0, "sloc": 0})
    for record in files:
        directory = str(Path(record["path"]).parent)
        entry = directories[directory]
        entry["files"].add(record["path"])
        for metric in ("bugs", "volume", "sloc"):
            entry[metric] += record[metric]
    areas = sorted(
        [
            {"path": path, **entry, "files": len(entry["files"])}
            for path, entry in directories.items()
        ],
        key=lambda entry: (-entry["bugs"], entry["path"]),
    )

    # The function hotspot list is intentionally independent of CLI --top and
    # --sort. It gives the terminal report a debugging view even when selected
    # records remain files for API compatibility.
    function_hotspots = sorted(
        functions,
        key=risk_sort_key,
    )
    for rank, row in enumerate(function_hotspots, 1):
        row["function_rank"] = rank

    serialized_options = asdict(options)
    serialized_options["languages"] = sorted(options.languages)
    selected_records = functions if options.functions and functions else files
    score_bundle = _score_bundle(files, functions)
    language_scores = _language_scores(files, functions)
    profiles = sorted({row["profile"] for row in files})
    parsers = sorted({row["parser"] for row in files})
    report = {
        "schema_version": 1,
        "tool": {
            "name": "walleye",
            "version": __version__,
            "profile": profiles[0] if len(profiles) == 1 else "mixed",
            "profiles": profiles,
            "quality_profile": QUALITY_PROFILE,
            "parser": " + ".join(parsers),
            "parsers": parsers,
            "parser_version": version("tree-sitter"),
            "language_pack_version": version("tree-sitter-language-pack"),
            "sqlfluff_version": version("sqlfluff"),
            "babel_version": "7.28.4",
            "networkx_version": version("networkx"),
        },
        "root": str(target),
        "scanned_at": datetime.now(timezone.utc).isoformat(),
        "duration_seconds": round(time.monotonic() - started, 3),
        "source_fingerprint": fingerprint.hexdigest(),
        "source_hashes": source_hashes,
        "discovery": discovery.method,
        "options": serialized_options,
        "summary": {
            "candidate_files": len(discovery.files),
            "scanned_files": sum(scanned.values()),
            "records": len(selected_records),
            "file_records": len(files),
            "function_records": len(functions),
            "top_level_functions": sum(row.get("function_depth") == 0 for row in functions),
            "nested_functions": sum(row.get("function_depth", 0) > 0 for row in functions),
            "languages": dict(sorted(scanned.items())),
            "parsers": dict(sorted(Counter(row["parser"] for row in files).items())),
            # Summary totals always refer to one row per parsed file, so they
            # remain stable across --level file/function.
            "sloc": sum(row["sloc"] for row in files),
            "estimated_bugs_sum": sum(row["bugs"] for row in files),
            "skipped": dict(sorted(skipped.items())),
            "issue_count": len(issues),
        },
        "coverage": _coverage(discovery, scanned, files, functions),
        "callgraph": graph,
        "scores": {
            **score_bundle,
            "by_language": language_scores,
        },
        "records": selected_records,
        "function_hotspots": function_hotspots[:50],
        "areas": areas,
        "issues": issues,
        "complete": not issues,
        "notes": [
            "B = volume / 3000 is a historical estimate, not observed bugs or a probability.",
            "Rows identify their parser and lexical profile; "
            "counts differ from Radon's Python AST visitor.",
            (
                "Function risk combines standard-form maintainability index with a "
                "transparent control-flow heuristic."
            ),
            (
                "Cyclomatic complexity is M = 1 + control decisions + short-circuit "
                "logical branches for core adapters."
            ),
            (
                "Function aggregates use exclusive function scopes (including nested "
                "closures) and functionless files; nested bodies are never counted "
                "inside their parent."
            ),
            (
                "Complexity coverage is explicit. Unsupported languages have null "
                "complexity scores, never a false zero."
            ),
            (
                "Language scores are directional within a language; grammar and style "
                "differences make cross-language comparisons weak."
            ),
            "Macros are not expanded, code is not executed, and embedded raw text is not parsed.",
        ],
    }
    from .workflow_scores import health_snapshot

    report["health"] = health_snapshot(report)
    return report
