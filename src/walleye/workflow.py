"""Baseline -> frozen tests -> reproduction -> candidate -> verification -> rescan -> apply."""

import difflib
import json
import os
import shutil
import stat
import tempfile
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath

from .discovery import ScanOptions
from .review import ReviewConfig, prepare_review, write_json
from .review_agent import _check_shape, run_review, stored_finding, validate_response
from .review_context import encode
from .scanner import scan
from .workflow_agent import WorkflowAgent, WorkflowStopped, stage_schema
from .workflow_quality import (
    acceptance,
    metric_gate,
    quality_policy,
    review_gate,
    verify_acceptance,
)
from .workflow_revisions import NO_PROGRESS_LIMIT, Revisions
from .workflow_scores import health_snapshot, scorecard
from .workflow_validation import (
    baseline_gate,
    canonical,
    digest,
    function_node,
    replace_function,
    run_cases,
    source_bundle,
    validate_cases,
)

PROFILE = "declank-improve-v2"


def _measure_candidate(index, proposal, original, updated, cases, baseline, directory):
    name, relative = proposal["target"]["name"], proposal["target"]["path"]
    (directory / "candidate.source").write_bytes(updated)
    (directory / "patch.diff").write_text(
        "".join(
            difflib.unified_diff(
                original.decode().splitlines(True),
                updated.decode().splitlines(True),
                fromfile="a/" + relative,
                tofile="b/" + relative,
            )
        )
    )
    bundle = source_bundle(updated, proposal["language"], name)
    (directory / "candidate-bundle.js").write_text(bundle)
    result = run_cases(bundle, name, cases)
    verification = {
        "tests_sha256": proposal["tests_sha256"],
        "baseline": baseline,
        "candidate": result,
    }
    write_json(directory / "verification.json", verification)
    if result["passed"] != result["total"]:
        return None, {
            "passed": False,
            "failures": ["Every frozen regression and control must pass"],
            "tests": result,
        }
    verify_snapshot(index.base, index.hashes)
    candidate_scan = scan_candidate(index.report, index.sources, relative, updated)
    card = scorecard(
        index.report, candidate_scan, proposal["target"], proposal["finding"], verification
    )
    write_json(directory / "scorecard.json", card)
    write_json(
        directory / "candidate-scores.json",
        {
            "health": health_snapshot(candidate_scan),
            "scores": candidate_scan["scores"],
            "source_fingerprint": candidate_scan["source_fingerprint"],
            "source_hashes": candidate_scan["source_hashes"],
        },
    )
    return card, metric_gate(card, proposal["finding"]["objective"])


def _independent_review(agent, packet, finding, index, directory, attempt, cases, card):
    # A fresh call receives evidence, not the writer's rationale or previous critiques.
    return agent.phase(
        "quality-review",
        packet,
        finding,
        index,
        directory,
        "INDEPENDENT REVIEW: decide whether this exact patch meets its objective's quality policy "
        "and addresses the evidenced objective. Do not author a patch. Inspect the original "
        "and candidate, contracts, callers, and frozen test coverage. Passing tests and metrics "
        "are necessary but insufficient. Check all four criteria exactly once: correctness "
        "(general fix and preserved behavior, boundary cases), relevance (real evidenced "
        "problem, focused change), readability (clear names, ordinary formatting, readable "
        "parsing expressions), simplicity (no unnecessary state/abstraction "
        "or complexity merely hidden in helpers). Reject a fix that is harder to understand. "
        "Flag missing explanations of non-obvious rules or invariants; do not demand comments "
        "that narrate obvious code or reward comment quantity. "
        "Give concrete reasons grounded in the code, including any missing behavior checks.\n"
        "PATCH\n"
        + (attempt / "patch.diff").read_text()
        + "\nFROZEN TESTS\n"
        + encode(cases)
        + "\nVERIFICATION\n"
        + (attempt / "verification.json").read_text()
        + "\nMEASURED CHANGES\n"
        + encode(card),
        [],
    )


def _publish_attempt(attempt, directory):
    for filename in (
        "candidate.source",
        "patch.diff",
        "candidate-bundle.js",
        "verification.json",
        "scorecard.json",
        "candidate-scores.json",
        "acceptance.json",
    ):
        if (attempt / filename).exists():
            shutil.copyfile(attempt / filename, directory / filename)
        else:
            (directory / filename).unlink(missing_ok=True)


def _refine_candidate(
    agent, packet, finding, index, directory, proposal, cases, baseline, detail, expansion
):
    original = index.sources[proposal["target"]["path"]]
    revisions = Revisions()
    number = 1
    while not revisions.exhausted:
        request = detail
        if revisions.history:
            request += (
                "\nREVISION HISTORY: revise the best candidate, keep every frozen test.\n"
                + encode(revisions.feedback())
            )
        patch = agent.phase("patch", packet, finding, index, directory, request, expansion)
        if patch["status"] != "ready":
            transition(proposal, patch["status"], directory, error=patch["summary"])
            return
        updated = replace_function(
            original, proposal["language"], proposal["target"]["name"], patch["replacement"].strip()
        )
        fingerprint = digest(updated)
        if updated == original:
            revisions.seen.add(fingerprint)
        if revisions.repeated(fingerprint):
            proposal["revisions"] = revisions.feedback()
            transition(proposal, "revising", directory, error="Writer repeated a checked candidate")
            number += 1
            continue
        if (
            digest(canonical(json.loads((directory / "tests.json").read_text())).encode())
            != proposal["tests_sha256"]
        ):
            raise ValueError("Frozen tests changed during the workflow")
        attempt = directory / "attempts" / f"{number:03}"
        attempt.mkdir(parents=True)
        transition(
            proposal,
            "candidate",
            directory,
            attempt=number,
            candidate_file_sha256=fingerprint,
            patch_summary=patch["summary"],
        )
        if agent.progress:
            agent.progress(f"{directory.name} · revision {number} · verifying tests and quality")
        card, metrics = _measure_candidate(
            index, proposal, original, updated, cases, baseline, attempt
        )
        review = {"status": "skipped", "checks": [], "summary": "Measured gate failed"}
        if metrics["passed"]:
            review = _independent_review(
                agent, packet, finding, index, directory, attempt, cases, card
            )
        record = acceptance(
            updated, proposal["tests_sha256"], index.report["source_fingerprint"], metrics, review
        )
        write_json(attempt / "acceptance.json", record)
        if card and record["passed"]:
            card["state"] = "verified-candidate"
            if finding["objective"] == "bug":
                card["correctness"].update(
                    confirmed_open={"before": 1, "after": 0, "delta": -1}, verified_resolutions=1
                )
            write_json(attempt / "scorecard.json", card)
        improved = revisions.record(number, patch, card, metrics, review, attempt)
        proposal["revisions"] = revisions.feedback()
        if improved or record["passed"]:
            _publish_attempt(attempt, directory)
            proposal["scorecard"] = "scorecard.json" if card else None
            proposal["patch"] = "patch.diff"
        verify_snapshot(index.base, index.hashes)
        if record["passed"]:
            proposal.pop("error", None)
            transition(
                proposal,
                "verified-candidate",
                directory,
                acceptance_sha256=digest(canonical(record).encode()),
                verification="Frozen function tests, objective-specific quality policy, "
                "and independent quality review passed; project integration tests not run",
            )
            return
        reasons = metrics["failures"] + [c["reason"] for c in review["checks"] if not c["passed"]]
        if not review_gate(review) and not reasons:
            reasons.append(
                review["summary"] or "Independent review did not approve every criterion"
            )
        transition(
            proposal,
            "revising",
            directory,
            error="; ".join(reasons),
        )
        number += 1
    transition(
        proposal,
        "no-progress",
        directory,
        error="No progress across three consecutive revisions",
        stop_reason="no_progress",
    )


def safe_path(base, relative):
    parts = PurePosixPath(relative)
    if parts.is_absolute() or ".." in parts.parts or "\\" in relative or not parts.parts:
        raise ValueError("Invalid repository-relative path")
    base = base.resolve()
    path = base.joinpath(*parts.parts)
    if not path.resolve().is_relative_to(base):
        raise ValueError(f"Source escaped its repository: {relative}")
    for parent in [path, *path.parents]:
        if parent == base:
            break
        if parent.is_symlink():
            raise ValueError(f"Source uses a symlink: {relative}")
    return path


def verify_snapshot(base, hashes):
    sources = {}
    for relative, expected in hashes.items():
        value = safe_path(base, relative).read_bytes()
        if digest(value) != expected:
            raise ValueError(f"Source changed; rescan before continuing: {relative}")
        sources[relative] = value
    return sources


def scan_candidate(baseline, sources, relative, candidate):
    # Rebuild the same parsed cohort, not a changing git working tree or relative rank sample.
    with tempfile.TemporaryDirectory(prefix="walleye-candidate-") as temporary:
        directory = Path(temporary)
        for name in baseline["source_hashes"]:
            destination = safe_path(directory, name)
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(candidate if name == relative else sources[name])
        result = scan(directory, ScanOptions(functions=True, respect_gitignore=False))
        if not result["complete"]:
            raise ValueError("Candidate has parse/read errors; no improvement credit awarded")
        return result


def transition(proposal, status, directory, **details):
    proposal.update(status=status, **details)
    proposal.setdefault("history", []).append(
        {"status": status, "at": datetime.now(timezone.utc).isoformat()}
    )
    write_json(directory / "proposal.json", proposal)


def _run_proposal(agent, packet, finding, index, directory):
    directory.mkdir(parents=True)
    target = packet["target"]
    name, relative = target["name"], target["path"]
    row = next(
        r for r in index.rows.values() if r["path"] == relative and r["line"] == target["line"]
    )
    language = row["language"]
    proposal = {
        "profile": PROFILE,
        "root": str(index.base),
        "target": target,
        "language": language,
        "finding": finding,
        "baseline_file_sha256": index.hashes[relative],
        "baseline_report": "../../baseline.json",
        "status": "pending",
        "application": "Candidate only; original source not modified",
    }
    transition(proposal, "baseline", directory)
    original = index.sources[relative]
    (directory / "before.source").write_bytes(original)
    write_json(directory / "context-hashes.json", index.hashes)
    write_json(directory / "packet.json", packet)
    expansion = []
    try:
        bundle = source_bundle(original, language, name)
        (directory / "baseline-bundle.js").write_text(bundle)
        detail = (
            "FIRST STAGE: write tests only, not a patch. Call only " + name + ".\n"
            "The adapter accepts JSON argument arrays and checks a JSON value, undefined, or "
            "an error name (e.g. TypeError). It has standard ECMAScript globals but no imports, "
            "network, filesystem, DOM or async runtime. Include normal-behavior controls. "
            "For a bug, include regression cases which SHOULD pass under the intended contract "
            "and are expected "
            "to fail on the original. For a refactor all characterization tests must already pass. "
            "expected_json must always be valid JSON; use null for undefined outcomes. "
            "Cover realistic boundaries and malformed inputs supported by the evidenced contract, "
            "not just a single happy path. Each case invokes the exact source in a fresh runtime.\n"
            "EXECUTABLE SCOPE\n" + bundle
        )
        plan = agent.phase("test-plan", packet, finding, index, directory, detail, expansion)
        if plan["status"] != "ready":
            transition(proposal, plan["status"], directory, error=plan["summary"])
            return proposal
        cases = plan["tests"]
        validate_cases(cases, finding["objective"])
        case_hash = digest(canonical(cases).encode())
        write_json(directory / "tests.json", cases)
        transition(proposal, "tests-frozen", directory, tests_sha256=case_hash)
        baseline = run_cases(bundle, name, cases)
        write_json(directory / "baseline-tests.json", baseline)
        if not baseline_gate(baseline, finding["objective"]):
            transition(
                proposal,
                "not-reproduced" if not baseline["harness_errors"] else "unsupported",
                directory,
                error="Baseline must fail the regression cases and pass controls; "
                "harness failures do not reproduce a defect",
            )
            return proposal
        transition(
            proposal, "reproduced" if finding["objective"] == "bug" else "characterized", directory
        )
        detail = (
            "SECOND STAGE: produce the entire replacement declaration for ONLY " + name + ". "
            "Preserve its exact signature; no imports, extra top-level declarations "
            "or edits to tests. "
            "Fix the root cause generally and preserve documented behavior. Return replacement "
            "source as plain text, without markdown fences. The test cases are now immutable. "
            "Acceptance requires the objective-specific quality policy and a separate readability "
            "review. Preserve a clear implementation; "
            "do not just append more branches or state. Return unsupported if you cannot satisfy "
            "both correctness and maintainability. The coordinator computes metrics and returns "
            "measured feedback; do not hand-calculate Halstead scores.\n"
            "FROZEN TESTS\n"
            + encode(cases)
            + "\nBASELINE RESULTS\n"
            + encode(baseline)
            + "\nEXECUTABLE SCOPE\n"
            + bundle
        )
        _refine_candidate(
            agent, packet, finding, index, directory, proposal, cases, baseline, detail, expansion
        )
    except WorkflowStopped as error:
        transition(proposal, "stopped", directory, error=str(error))
        raise
    except (ValueError, OSError) as error:
        transition(proposal, "unverified", directory, error=str(error))
    return proposal


def render_workflow(manifest, output):
    lines = [
        "# Verified improvement report",
        "",
        "Repository source changes only after an explicit apply.",
        "",
    ]
    for proposal in manifest.get("proposals", []):
        target = proposal["target"]
        lines += [
            f"## {target['path']}:{target['line']} — {target['name']}",
            "",
            f"Status: **{proposal['status']}**",
            "",
            proposal["finding"]["title"],
            "",
        ]
        directory = output / "proposals" / proposal["id"]
        if proposal.get("scorecard"):
            card = json.loads((directory / "scorecard.json").read_text())
            rows = [
                ("Confirmed open defects (this finding)", card["correctness"]["confirmed_open"]),
                ("Target structural quality ↑", card["maintainability"]["target_quality"]),
                (
                    "Target maintainability index ↑",
                    card["maintainability"]["target"]["maintainability_index"],
                ),
                (
                    "Target cyclomatic complexity ↓",
                    card["maintainability"]["target"]["cyclomatic_complexity"],
                ),
                ("Repository structural quality ↑", card["maintainability"]["repository"]["score"]),
                ("Resolved function cycles", card["architecture"]["changes"]["function_cycles"]),
                ("Resolved module cycles", card["architecture"]["changes"]["module_cycles"]),
                ("Targeted tests passing", card["correctness"]["tests_passing"]),
            ]
            lines += ["| Measurement | Before | After | Change |", "| --- | ---: | ---: | ---: |"]
            lines += [
                f"| {label} | {data['before']} | {data['after']} | {data['delta']} |"
                for label, data in rows
            ]
            lines += [
                "",
                card["scope"]["quality_comparison"] + ".",
                "Raw scan scores are included in the scorecard.",
                "Architecture is partial static analysis; unreviewed correctness is unknown.",
                "Integration tests have not been run. Test variants count as one finding.",
                "",
                f"[Patch](proposals/{proposal['id']}/patch.diff) · "
                f"[Scorecard](proposals/{proposal['id']}/scorecard.json)",
                "",
            ]
            if (directory / "acceptance.json").exists():
                record = json.loads((directory / "acceptance.json").read_text())
                lines += [
                    "Acceptance: "
                    + ("passed" if record["passed"] else "rejected")
                    + ". "
                    + record["review"]["summary"],
                    f"[Gate and review evidence](proposals/{proposal['id']}/acceptance.json)",
                    "",
                ]
        if proposal.get("error"):
            lines += [proposal["error"], ""]
    lines += [
        f"Model calls: {manifest['usage']['calls']}. "
        f"Reported tokens: {manifest['usage']['total_tokens']:,}. "
        f"API-equivalent cost: ${manifest['cost']['spent_usd']:.6f} "
        f"({manifest['cost']['backend']}).",
        "",
    ]
    if manifest["usage"]["unknown"] or manifest["cost"]["unknown"]:
        lines += [
            "Total tokens and cost are unknown: the figures above include reported usage only. "
            "An unreported call stopped further generation and cannot be treated as free.",
            "",
        ]
    (output / "report.md").write_text("\n".join(lines))


def improve(
    source,
    *,
    issues=1,
    objective="bug",
    config=None,
    output=None,
    progress=None,
    invoke=None,
    review_invoke=None,
):
    config = config or ReviewConfig()
    source = Path(source).resolve()
    saved = None
    if source.is_file() and source.suffix == ".json":
        saved = json.loads(source.read_text())
        if not isinstance(saved, dict) or not all(
            key in saved for key in ("root", "objective", "tasks", "findings")
        ):
            raise ValueError("Expected a saved review.json containing targets and findings")
        if not saved.get("findings"):
            raise ValueError("Saved review contains no findings")
        objective = saved["objective"]
        chosen = saved["findings"][:issues]
        old_targets = {t["task_id"]: t["target"] for t in saved["tasks"]}
        targets = [old_targets[f["task_id"]] for f in chosen]
        root = Path(saved["root"])
    else:
        root, targets = source, None
    manifest, packets, index, output = prepare_review(
        root, issues, objective, config, output, progress, targets=targets
    )
    write_json(output / "baseline.json", index.report)
    manifest["baseline_health"] = health_snapshot(index.report)
    manifest["workflow_profile"] = PROFILE
    manifest["acceptance_policy"] = {
        **quality_policy(objective),
        "requires": [
            "frozen tests",
            "objective-specific quality policy",
            "independent quality review",
        ],
        "max_attempts_without_progress": NO_PROGRESS_LIMIT,
        "budget": "All stages and revisions share the run budget",
    }
    manifest["proposals"] = []
    if saved is not None:
        manifest["source_review"] = str(source)
        manifest["findings"] = []
        for finding, packet in zip(chosen, packets, strict=True):
            if finding["source_sha256"] != packet["target"]["sha256"]:
                raise ValueError("Saved finding source changed; run a fresh review")
            # Revalidate the original evidence against the fresh source packet.
            response = {
                "status": "finding",
                "summary": finding["title"],
                "context_requests": [],
                "finding": stored_finding(finding),
            }
            validate_response(response, packet, index, require_detail=False)
            manifest["findings"].append({**finding, "task_id": packet["task_id"]})
    else:
        run_review(
            manifest, packets, index, output, config, invoke=review_invoke, progress=progress
        )
    write_json(output / "review.json", manifest)
    agent = WorkflowAgent(manifest, output, config, invoke=invoke, progress=progress)
    agent.save()
    by_id = {p["task_id"]: p for p in packets}
    for finding in manifest["findings"]:
        packet = by_id[finding["task_id"]]
        directory = output / "proposals" / finding["task_id"]
        try:
            proposal = _run_proposal(agent, packet, finding, index, directory)
        except WorkflowStopped as error:
            manifest["workflow_stop_reason"] = str(error)
            proposal = json.loads((directory / "proposal.json").read_text())
            manifest["proposals"].append({**proposal, "id": directory.name})
            break
        manifest["proposals"].append({**proposal, "id": directory.name})
        agent.save()
    manifest["workflow_status"] = (
        "finished" if not manifest.get("workflow_stop_reason") else "incomplete"
    )
    manifest["proposal_counts"] = dict(Counter(p["status"] for p in manifest["proposals"]))
    manifest["verified_resolutions"] = sum(
        p["status"] == "verified-candidate" and p["finding"]["objective"] == "bug"
        for p in manifest["proposals"]
    )
    confirmed = sum(
        any(h["status"] == "reproduced" for h in p["history"]) for p in manifest["proposals"]
    )
    manifest["correctness"] = {
        "scope": "Findings investigated in this run; all other source is unknown",
        "confirmed_open_in_original": confirmed,
        "verified_candidate_resolutions": manifest["verified_resolutions"],
        "applied_resolutions": 0,
        "score": None,
    }
    agent.save()
    render_workflow(manifest, output)
    return manifest, output


def apply_proposal(directory):
    directory = Path(directory).resolve()
    if directory.is_file():
        directory = directory.parent
    proposal = json.loads((directory / "proposal.json").read_text())
    if not isinstance(proposal, dict) or proposal.get("profile") != PROFILE:
        raise ValueError("Proposal uses an obsolete or unknown acceptance policy; rerun improve")
    if proposal.get("status") != "verified-candidate":
        raise ValueError("Only a verified candidate can be applied")
    base = Path(proposal["root"]).resolve()
    hashes = json.loads((directory / "context-hashes.json").read_text())
    sources = verify_snapshot(base, hashes)
    target, language = proposal["target"], proposal["language"]
    relative, name = target["path"], target["name"]
    original, candidate = sources[relative], (directory / "candidate.source").read_bytes()
    if (
        digest(original) != proposal["baseline_file_sha256"]
        or digest(candidate) != proposal["candidate_file_sha256"]
    ):
        raise ValueError("Proposal source hashes do not match")
    cases = json.loads((directory / "tests.json").read_text())
    if digest(canonical(cases).encode()) != proposal["tests_sha256"]:
        raise ValueError("Frozen test cases changed")
    _check_shape(cases, stage_schema("test-plan")["properties"]["tests"])
    validate_cases(cases, proposal["finding"]["objective"])
    updated = function_node(candidate, language, name)
    rebuilt = replace_function(
        original, language, name, candidate[updated.start_byte : updated.end_byte].decode()
    )
    if rebuilt != candidate:
        raise ValueError("Candidate contains changes outside the assigned function")
    before = run_cases(source_bundle(original, language, name), name, cases)
    after = run_cases(source_bundle(candidate, language, name), name, cases)
    if (
        not baseline_gate(before, proposal["finding"]["objective"])
        or after["passed"] != after["total"]
    ):
        raise ValueError("Reverification failed; original source was not changed")
    baseline = json.loads((directory / proposal["baseline_report"]).read_text())
    candidate_scan = scan_candidate(baseline, sources, relative, candidate)
    verification = {
        "baseline": before,
        "candidate": after,
        "tests_sha256": proposal["tests_sha256"],
    }
    card = scorecard(baseline, candidate_scan, target, proposal["finding"], verification)
    record = json.loads((directory / "acceptance.json").read_text())
    _check_shape(record["review"], stage_schema("quality-review"))
    verify_acceptance(proposal, record, candidate, proposal["tests_sha256"], baseline, card)
    current_scan = scan(Path(baseline["root"]), ScanOptions(functions=True))
    if current_scan["source_hashes"] != baseline["source_hashes"]:
        raise ValueError("Repository source cohort changed; rescan before applying")
    # Check again after execution/scanning; preserve permissions and replace only one file.
    verify_snapshot(base, hashes)
    path = safe_path(base, relative)
    mode = stat.S_IMODE(path.stat().st_mode)
    descriptor, temporary = tempfile.mkstemp(prefix=".walleye-apply-", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(candidate)
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(temporary, mode)
        os.replace(temporary, path)
    finally:
        Path(temporary).unlink(missing_ok=True)
    transition(
        proposal,
        "applied",
        directory,
        application="Applied to original source after hash checks and fresh test execution",
    )
    regression = safe_path(base, f".walleye/regressions/{proposal['tests_sha256']}.json")
    write_json(
        regression,
        {
            "source": relative,
            "function": name,
            "language": language,
            "cases": cases,
            "proposal": str(directory),
        },
    )
    actual = scan(Path(baseline["root"]), ScanOptions(functions=True))
    write_json(directory / "applied-scan.json", actual)
    card = scorecard(baseline, actual, target, proposal["finding"], verification, applied=True)
    write_json(directory / "scorecard.json", card)
    write_json(directory / "applied-verification.json", verification)
    output = directory.parent.parent
    manifest = json.loads((output / "workflow.json").read_text())
    manifest["proposals"] = [
        ({**proposal, "id": p["id"]} if p["id"] == directory.name else p)
        for p in manifest["proposals"]
    ]
    manifest["applied_health"] = health_snapshot(actual)
    if proposal["finding"]["objective"] == "bug":
        manifest["correctness"]["confirmed_open_in_original"] -= 1
        manifest["correctness"]["applied_resolutions"] += 1
    manifest["proposal_counts"] = dict(Counter(p["status"] for p in manifest["proposals"]))
    write_json(output / "workflow.json", manifest)
    render_workflow(manifest, output)
    return proposal, card
