"""Joint correctness and maintainability acceptance, bound to the tested candidate."""

from .workflow_validation import canonical, digest

PROFILE = "declank-acceptance-v3"
CRITERIA = {"correctness", "relevance", "readability", "simplicity"}
QUALITY_EPSILON = 0.0001
BUG_QUALITY_TOLERANCE = 0.01


def quality_policy(objective):
    if objective not in ("bug", "refactor"):
        raise ValueError("Quality policy requires a bug or refactor objective")
    return {
        "profile": PROFILE,
        "objective": objective,
        "quality_tolerance_points": BUG_QUALITY_TOLERANCE
        if objective == "bug"
        else QUALITY_EPSILON,
        "requires_quality_improvement": objective == "refactor",
    }


def metric_gate(card, objective):
    policy = quality_policy(objective)
    maintainability = card["maintainability"]
    target = maintainability["target"]
    region = maintainability["region"]
    checks = [
        ("Target quality", maintainability["target_quality"], 1),
        ("Target cyclomatic complexity", target["cyclomatic_complexity"], -1),
        ("Target nesting", target["max_nesting"], -1),
        ("Changed region quality (including helpers)", region["quality"], 1),
        ("Changed region decisions (including helpers)", region["decisions"], -1),
        ("Changed region nesting (including helpers)", region["max_nesting"], -1),
        ("Repository quality", maintainability["repository"]["score"], 1),
        ("Function cycles", card["architecture"]["changes"]["function_cycles"], -1),
        ("Module cycles", card["architecture"]["changes"]["module_cycles"], -1),
    ]
    failures = []
    distance = 0.0
    for label, change, direction in checks:
        tolerance = policy["quality_tolerance_points"] if direction == 1 else QUALITY_EPSILON
        if change["delta"] is None or direction * change["delta"] < -tolerance:
            failures.append(f"{label}: {change['before']} -> {change['after']}")
            distance += 100 if change["delta"] is None else -direction * change["delta"] - tolerance
    if policy["requires_quality_improvement"] and (
        region["quality"]["delta"] is None or region["quality"]["delta"] <= QUALITY_EPSILON
    ):
        failures.append("The whole changed region must show a measured quality improvement")
        distance += (
            100
            if region["quality"]["delta"] is None
            else (QUALITY_EPSILON - region["quality"]["delta"])
        )
    return {
        **policy,
        "passed": not failures,
        "failures": failures,
        "violation_distance": round(distance, 6),
    }


def review_gate(review):
    checks = review.get("checks", [])
    return (
        review.get("status") == "ready"
        and not review.get("context_requests")
        and len(checks) == len(CRITERIA)
        and {c["criterion"] for c in checks} == CRITERIA
        and all(c["passed"] is True and c["reason"].strip() for c in checks)
    )


def acceptance(candidate, tests_hash, baseline_fingerprint, metrics, review):
    return {
        "profile": PROFILE,
        "candidate_sha256": digest(candidate),
        "tests_sha256": tests_hash,
        "baseline_fingerprint": baseline_fingerprint,
        "metrics": metrics,
        "review": review,
        "passed": metrics["passed"] and review_gate(review),
    }


def verify_acceptance(proposal, record, candidate, tests_hash, baseline, card):
    expected = acceptance(
        candidate,
        tests_hash,
        baseline["source_fingerprint"],
        metric_gate(card, proposal["finding"]["objective"]),
        record["review"],
    )
    if (
        not expected["passed"]
        or record != expected
        or digest(canonical(record).encode()) != proposal.get("acceptance_sha256")
    ):
        raise ValueError(
            "Candidate must pass the current maintainability and independent review gates"
        )
