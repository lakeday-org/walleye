"""Objective-specific review queues, explicit budgets, and durable review artifacts."""

import json
import math
import os
import tempfile
from bisect import bisect_left, bisect_right
from collections import defaultdict
from dataclasses import asdict, dataclass, fields
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from .discovery import ScanOptions
from .review_context import SourceIndex, build_packet, estimate_tokens
from .review_cost import PRICING_DATE, PRICING_SOURCE, backend_for, dollars, pricing_for
from .scanner import scan

PROFILE = "declank-review-v2"


@dataclass(frozen=True)
class ReviewConfig:
    model: str = "gpt-5.6-luna"
    reasoning_effort: str = "max"
    codex: str = "codex"
    backend: str = "auto"
    budget_usd: float = 5.0
    pricing: dict | None = None
    context_tokens: int = 200000
    expansion_tokens: int | None = None
    max_expansions: int | None = None
    tasks_per_issue: int = 2
    total_tokens: int | None = None
    per_call_tokens: int | None = None
    max_output_tokens: int = 128000
    timeout_seconds: int = 1800

    def __post_init__(self):
        for field in fields(self):
            value = getattr(self, field.name)
            if field.type is int and (type(value) is not int or value < 1):
                if field.name != "max_expansions" or type(value) is not int or value != 0:
                    raise ValueError(f"review config {field.name} must be a positive integer")
            elif field.type is str and (not isinstance(value, str) or not value.strip()):
                raise ValueError(f"review config {field.name} must be a nonempty string")
        if self.context_tokens < 2500:
            raise ValueError("context_tokens must be at least 2500")
        if self.reasoning_effort not in {"low", "medium", "high", "xhigh", "max"}:
            raise ValueError("Invalid reasoning_effort")
        if self.backend not in {"auto", "api", "codex"}:
            raise ValueError("backend must be auto, api, or codex")
        dollars(self.budget_usd)
        pricing_for(self)
        for name in ("expansion_tokens", "max_expansions", "total_tokens", "per_call_tokens"):
            value = getattr(self, name)
            minimum = 0 if name == "max_expansions" else 1
            if value is not None and (type(value) is not int or value < minimum):
                raise ValueError(f"{name} must be null or an integer >= {minimum}")


def load_config(path: Path | None = None) -> ReviewConfig:
    explicit = path is not None
    path = (
        path
        or Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config")) / "walleye/config.json"
    )
    if not path.exists() and not explicit:
        return ReviewConfig()
    data = json.loads(path.read_text())
    if not isinstance(data, dict) or set(data) - {f.name for f in fields(ReviewConfig)}:
        raise ValueError("Review config must contain only documented ReviewConfig keys")
    return ReviewConfig(**data)


def _percentiles(values):
    ordered = sorted(v for v in values if v is not None)

    def percentile(value):
        if value is None or not ordered:
            return None
        # Midrank keeps ties equal. Groups of one have neutral rank, not maximal pressure.
        return (bisect_left(ordered, value) + bisect_right(ordered, value)) / (2 * len(ordered))

    return percentile


def rank_candidates(report, index: SourceIndex, objective="bug"):
    if objective not in {"bug", "refactor"}:
        raise ValueError("objective must be bug or refactor")
    groups = defaultdict(list)
    for key, row in index.rows.items():
        groups[row["language"], row["profile"]].append((key, row))
    cycles = {}
    for cycle in report["callgraph"]["functions"]["cycles"]:
        for key in cycle:
            cycles[key] = cycle
    result = []
    for (language, profile), rows in sorted(groups.items()):
        raw = {}
        for key, row in rows:
            raw[key] = {
                "decisions": None
                if row.get("cyclomatic_complexity") is None
                else max(0, row["cyclomatic_complexity"] - 1),
                "nesting": row.get("max_nesting"),
                "branch_pressure": max(
                    (b.get("subtree_branches", 0) for b in row.get("structure_hotspots", [])),
                    default=0,
                ),
                "maintenance": None
                if row.get("maintainability_index") is None
                else 100 - row["maintainability_index"],
                "difficulty": row["difficulty"],
                "cycles": row.get("cycle_size"),
            }
        ranks = {
            key: _percentiles(r[key] for r in raw.values()) for key in next(iter(raw.values()))
        }
        weights = (
            {"decisions": 0.5, "nesting": 0.2, "branch_pressure": 0.3}
            if objective == "bug"
            else {"maintenance": 0.4, "decisions": 0.3, "difficulty": 0.15, "cycles": 0.15}
        )
        for key, row in rows:
            # Unknown complexity cannot be silently treated as measured bug evidence.
            if objective == "bug" and not raw[key]["decisions"]:
                continue
            signal = {metric: ranks[metric](raw[key][metric]) for metric in weights}
            available = {k: v for k, v in signal.items() if v is not None}
            if not available:
                continue
            # Zero-valued indicators exert zero pressure despite ties in a percentile group.
            pressure = sum(weights[k] * (v if raw[key][k] else 0) for k, v in available.items())
            pressure /= sum(weights[k] for k in available)
            if pressure <= 0:
                continue
            reach = row.get("dependent_count") or 0
            impact = 1 + min(4, math.log2(1 + reach)) / 4
            if objective == "refactor":
                impact += min(3, math.log2(max(1, row.get("cycle_size") or 1))) / 3
            body = "\n".join(index.lines(row["path"])[row["line"] - 1 : row["end_line"]])
            cost = 800 + estimate_tokens(body) + min(6, (row.get("fan_in") or 0)) * 80
            cost += min(6, (row.get("fan_out") or 0)) * 160
            priority = 100 * pressure * impact
            neighbors = {edge["target"] for edge in row.get("callees", [])}
            neighbors.update(edge["source"] for edge in row.get("callers", []))
            result.append(
                {
                    "id": key,
                    "objective": objective,
                    "row": row,
                    "pressure": round(pressure, 6),
                    "percentiles": signal,
                    "raw_signals": raw[key],
                    "comparison_group": {
                        "language": language,
                        "profile": profile,
                        "functions": len(rows),
                    },
                    "impact": round(impact, 6),
                    "estimated_context_cost": cost,
                    "base_priority": round(priority, 6),
                    "priority": round(priority, 6),
                    "cycle_members": cycles.get(key, []),
                    "neighbors": neighbors | {key},
                }
            )
    return result


def select_queue(candidates, limit):
    selected = []
    remaining = list(candidates)
    while remaining and len(selected) < limit:
        choices = []
        for candidate in remaining:
            row = candidate["row"]
            overlap = False
            similarity = 0.0
            for previous in selected:
                other = previous["row"]
                if candidate["id"] in previous["cycle_members"] or (
                    row["path"] == other["path"]
                    and max(row["line"], other["line"]) <= min(row["end_line"], other["end_line"])
                ):
                    overlap = True
                    break
                union = candidate["neighbors"] | previous["neighbors"]
                similarity = max(
                    similarity,
                    len(candidate["neighbors"] & previous["neighbors"]) / max(1, len(union)),
                )
            if overlap:
                continue
            adjusted = {
                **candidate,
                "novelty": round(1 - 0.75 * similarity, 6),
                "priority": round(candidate["base_priority"] * (1 - 0.75 * similarity), 6),
            }
            choices.append(adjusted)
        if not choices:
            break
        chosen = min(
            choices, key=lambda c: (-c["priority"], c["row"]["path"], c["row"]["line"], c["id"])
        )
        chosen["task_id"] = f"{len(selected) + 1:03d}"
        selected.append(chosen)
        remaining = [c for c in remaining if c["id"] != chosen["id"]]
    return selected


def write_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=path.parent, delete=False
        ) as stream:
            temporary = Path(stream.name)
            json.dump(data, stream, ensure_ascii=True, indent=2, allow_nan=False)
            stream.write("\n")
        os.replace(temporary, path)
    finally:
        if temporary:
            temporary.unlink(missing_ok=True)


def _raise_packet_failure(failure):
    raise ValueError(failure["reason"])


def _select_review_queue(candidates, issues, config, targets):
    skipped = []
    if targets is not None:
        selected = []
        for target in targets:
            matches = [
                c
                for c in candidates
                if c["row"]["path"] == target["path"]
                and c["row"]["line"] == target["line"]
                and c["row"].get("qualified_name", c["row"]["name"]) == target["name"]
            ]
            if len(matches) != 1:
                raise ValueError(f"Saved target no longer matches the scan: {target['path']}")
            selected.append(matches[0])
        queue = selected[:issues]
        for number, candidate in enumerate(queue, 1):
            candidate["task_id"] = f"{number:03}"
        return queue, skipped, _raise_packet_failure
    return select_queue(candidates, issues * config.tasks_per_issue), skipped, skipped.append


def _build_review_packets(queue, index, graph, context_tokens, failure_handler):
    packets = []
    for candidate in queue:
        try:
            packets.append(build_packet(candidate, index, graph, context_tokens))
        except ValueError as error:
            failure_handler({"task_id": candidate["task_id"], "reason": str(error)})
    return packets


def prepare_review(
    path, issues=10, objective="bug", config=None, output=None, progress=None, *, targets=None
):
    if type(issues) is not int or issues < 1:
        raise ValueError("issues must be positive")
    config = config or ReviewConfig()
    if output is not None and output.exists():
        raise ValueError(f"Review output directory already exists: {output}")
    sources = {}
    report = scan(path, ScanOptions(functions=True), source_snapshot=sources)
    index = SourceIndex(report, sources)
    candidates = rank_candidates(report, index, objective)
    queue, skipped, failure_handler = _select_review_queue(candidates, issues, config, targets)
    if progress:
        progress(
            f"Scanned {report['summary']['scanned_files']:,} files; "
            f"selected {len(queue)} distinct {objective} investigations."
        )
    index.index_tests({c["row"]["name"] for c in queue if c["row"]["name"]})
    unresolved = defaultdict(list)
    for call in report["callgraph"]["unresolved"]:
        unresolved[call["source"]].append(call)
    modules = defaultdict(list)
    for edge in report["callgraph"]["module_edges"]:
        modules[edge["source"].removeprefix("file:")].append(edge["target"].removeprefix("file:"))
    graph = {"unresolved_by_source": unresolved, "modules_by_source": modules}
    packets = _build_review_packets(queue, index, graph, config.context_tokens, failure_handler)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    output = (
        output
        or Path.cwd()
        / ".walleye/reviews"
        / f"{Path(report['root']).name}-{timestamp}-{uuid4().hex[:6]}"
    ).absolute()
    output.mkdir(parents=True, exist_ok=False)
    from .review_agent import make_prompt, response_schema

    for packet in packets:
        write_json(output / "tasks" / f"{packet['task_id']}.json", packet)
        (output / "tasks" / f"{packet['task_id']}.prompt.txt").write_text(make_prompt(packet))
    write_json(output / "response.schema.json", response_schema())
    manifest = {
        "schema_version": 1,
        "profile": PROFILE,
        "root": report["root"],
        "scanned_at": report["scanned_at"],
        "source_fingerprint": report["source_fingerprint"],
        "objective": objective,
        "status": "prepared",
        "stop_reason": None,
        "budgets": {
            **asdict(config),
            "issues": issues,
            "max_tasks": issues * config.tasks_per_issue,
            "cost_limit_semantics": "API mode counts input and reserves affordable maximum output "
            "before generation. Codex mode uses a soft API-equivalent dollar estimate.",
        },
        "cost": {
            "budget_usd": config.budget_usd,
            "spent_usd": 0.0,
            "reserved_usd": 0.0,
            "backend": backend_for(config),
            "pricing": pricing_for(config),
            "pricing_source": PRICING_SOURCE if config.pricing is None else "configuration",
            "pricing_date": PRICING_DATE if config.pricing is None else None,
            "basis": "API-rate estimate from reported usage; excludes tax and account discounts",
            "unknown": False,
        },
        "ranking": {
            "formula": "100 * within-language pressure * impact * novelty",
            "meaning": "Review-order heuristic, not bug probability or predicted savings",
            "candidate_functions": len(candidates),
            "selected_tasks": len(packets),
            "skipped_packets": skipped,
        },
        "scan": {
            "complete": report["complete"],
            "summary": report["summary"],
            "issues": report["issues"],
            "graph_coverage": report["callgraph"]["coverage"],
            "test_index": index.test_coverage,
        },
        "tasks": [
            {
                "task_id": p["task_id"],
                "target": p["target"],
                "priority": p["selection"]["priority"],
                "estimated_context_tokens": p["context_budget"]["estimated_tokens"],
                "packet": f"tasks/{p['task_id']}.json",
            }
            for p in packets
        ],
        "investigations": [],
        "findings": [],
        "usage": {
            "input_tokens": 0,
            "output_tokens": 0,
            "cached_input_tokens": 0,
            "total_tokens": 0,
            "calls": 0,
            "unknown": False,
        },
    }
    write_json(output / "review.json", manifest)
    return manifest, packets, index, output
