"""Versioned scorecards keep measured structure separate from verified findings."""

from .workflow_comparison import RULE, paired_quality, paired_totals

PROFILE = "declank-health-v1"


def health_snapshot(report):
    graph = report["callgraph"]
    functions, modules = graph["functions"], graph["modules"]
    cyclic = sum(len(cycle) for cycle in functions["cycles"])
    count = functions["nodes"]
    coverage = graph["coverage"]
    return {
        "profile": PROFILE,
        "maintainability": {
            "score": report["scores"]["overall_score"],
            "mi": report["scores"]["maintainability_index"],
            "control": report["scores"]["complexity_score"],
            "meaning": "Structural quality; higher is better, not a correctness probability",
            "coverage": report["scores"]["coverage"],
        },
        "correctness": {
            "score": None,
            "status": "not-reviewed",
            "confirmed_open": None,
            "meaning": "Unreviewed source is unknown; no findings does not prove correctness",
        },
        "architecture": {
            "score": None,
            "status": "partial-static",
            "cycle_health": round(100 * (1 - cyclic / count), 4) if count else None,
            "function_cycles": len(functions["cycles"]),
            "cyclic_functions": cyclic,
            "module_cycles": len(modules["cycles"]),
            "dependency_edges": modules["edges"],
            "resolved_call_share": round(
                coverage.get("resolved_calls", 0) / coverage["call_sites"], 4
            )
            if coverage["call_sites"]
            else None,
            "coverage": coverage,
            "meaning": "Cycle health measures participation in resolved function cycles only; "
            "centrality indicates impact, not an architecture defect. Layer rules unassessed.",
        },
    }


def _change(before, after):
    return {
        "before": before,
        "after": after,
        "delta": round(after - before, 4) if before is not None and after is not None else None,
    }


def _region_rows(report, target):
    return [
        r
        for r in report["records"]
        if r["path"] == target["path"]
        and r["kind"] == "function"
        and target["line"] <= r["line"] <= r["end_line"] <= target["end_line"]
        and (
            r["graph_id"] == target["graph_id"]
            or r["qualified_name"].startswith(target["qualified_name"] + ".")
        )
    ]


def _region(report, target):
    rows = _region_rows(report, target)
    weight = sum(max(1, r["sloc"]) for r in rows)
    return {
        "quality": round(
            sum((100 - r["risk_score"]) * max(1, r["sloc"]) for r in rows) / weight, 4
        ),
        "decisions": sum(r["cyclomatic_complexity"] - 1 for r in rows),
        "max_nesting": max(r["max_nesting"] for r in rows),
        "functions": len(rows),
    }


def scorecard(
    baseline,
    candidate,
    target,
    finding,
    verification,
    *,
    applied=False,
    accepted=False,
    allow_line_shift=False,
):
    if set(baseline["source_hashes"]) != set(candidate["source_hashes"]):
        raise ValueError("Comparison source scope changed; scores are not comparable")
    keys = (
        "quality_profile",
        "profiles",
        "parser_version",
        "language_pack_version",
        "sqlfluff_version",
        "babel_version",
        "networkx_version",
    )
    if any(baseline["tool"][k] != candidate["tool"][k] for k in keys):
        raise ValueError("Scoring/parser versions changed; scores are not comparable")

    def local(report):
        rows = [
            r
            for r in report["records"]
            if r["path"] == target["path"]
            and (allow_line_shift or r["line"] == target["line"])
            and r.get("qualified_name", r["name"]) == target["name"]
        ]
        if len(rows) != 1:
            raise ValueError("Cannot match target identity across the patch")
        return rows[0]

    old, new = local(baseline), local(candidate)
    old_region, new_region = _region(baseline, old), _region(candidate, new)
    before, after = health_snapshot(baseline), health_snapshot(candidate)
    old_region["quality"], new_region["quality"] = paired_quality(
        _region_rows(baseline, old), _region_rows(candidate, new), target
    )
    totals, _ = paired_totals(
        [row for row in baseline["records"] if row["kind"] == "function"],
        [row for row in candidate["records"] if row["kind"] == "function"],
        target,
    )
    repository = {}
    weight = baseline["scores"]["coverage"]["owned_sloc"]
    for key, (left, right) in totals.items():
        previous = before["maintainability"][key]
        compared = (
            round(previous + (right - left) / weight, 4)
            if previous is not None and left is not None and right is not None and weight
            else None
        )
        repository[key] = _change(previous, compared)
    bug = finding["objective"] == "bug"
    return {
        "profile": PROFILE,
        "state": "applied"
        if applied
        else "verified-candidate"
        if accepted
        else "measured-candidate",
        "scope": {
            "target": target,
            "source_files": len(baseline["source_hashes"]),
            "baseline_fingerprint": baseline["source_fingerprint"],
            "candidate_fingerprint": candidate["source_fingerprint"],
            "baseline_scan_complete": baseline["complete"],
            "baseline_diagnostics": baseline["issues"],
            "rule": "Same successfully parsed source cohort and scoring versions",
            "quality_comparison": RULE,
        },
        "correctness": {
            "score": None,
            "scope": "This one finding and its frozen test cases",
            "confirmed_open": _change(1, 0 if accepted or applied else 1) if bug else _change(0, 0),
            "verified_resolutions": int(bug and (accepted or applied)),
            "severity": finding["severity"],
            "unreviewed_source": "unknown",
            "tests_passing": _change(
                verification["baseline"]["passed"], verification["candidate"]["passed"]
            ),
            "tests_total": verification["candidate"]["total"],
            "assurance": "Isolated function regression checks; project integration tests not run",
        },
        "maintainability": {
            "region": {k: _change(old_region[k], new_region[k]) for k in old_region},
            "region_scope": "Changed function and every nested function, with exclusive ownership",
            "target": {
                k: _change(old[k], new[k])
                for k in (
                    "maintainability_index",
                    "complexity_score",
                    "cyclomatic_complexity",
                    "max_nesting",
                    "sloc",
                    "volume",
                    "risk_score",
                )
            },
            "target_quality": _change(
                round(100 - old["risk_score"], 4), round(100 - new["risk_score"], 4)
            ),
            "repository": repository,
            "observed_repository": {
                k: _change(before["maintainability"][k], after["maintainability"][k])
                for k in ("score", "mi", "control")
            },
        },
        "architecture": {
            "score": None,
            "status": "partial-static",
            "changes": {
                k: _change(before["architecture"][k], after["architecture"][k])
                for k in (
                    "cycle_health",
                    "function_cycles",
                    "cyclic_functions",
                    "module_cycles",
                    "dependency_edges",
                    "resolved_call_share",
                )
            },
            "coverage_before": before["architecture"]["coverage"],
            "coverage_after": after["architecture"]["coverage"],
            "meaning": before["architecture"]["meaning"],
        },
    }
