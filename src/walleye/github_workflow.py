"""GitHub findings become native-tested, measured, independently reviewed pull requests."""

from urllib.parse import quote

from .discovery import ScanOptions
from .git_workspace import git
from .github_publication import pull_body, read_metadata
from .project_context import native_packet
from .project_tests import ProjectTests, checks_passed, freeze_test, reproduced, test_unchanged
from .review import prepare_review, write_json
from .review_agent import response_schema, validate_response
from .review_context import encode
from .scanner import scan
from .workflow import safe_path
from .workflow_agent import WorkflowAgent
from .workflow_quality import MAX_ATTEMPTS, acceptance, metric_gate
from .workflow_scores import scorecard
from .workflow_validation import digest


def apply_edits(original, edits):
    text = original.decode()
    if not edits:
        raise ValueError("Patch contains no edits")
    for edit in edits:
        if not edit["old"] or text.count(edit["old"]) != 1:
            raise ValueError("Each edit must match exactly once in the assigned source file")
        text = text.replace(edit["old"], edit["new"], 1)
    if text.encode() == original:
        raise ValueError("Patch does not change the source")
    return text.encode()


def module_measurement(report, path):
    rows = [r for r in report["records"] if r["path"] == path and r["kind"] == "function"]
    weight = sum(max(1, r["sloc"]) for r in rows)
    if not weight or any(r["cyclomatic_complexity"] is None for r in rows):
        raise ValueError("Changed module needs measured function complexity")
    return {
        "quality": round(
            sum((100 - r["risk_score"]) * max(1, r["sloc"]) for r in rows) / weight, 4
        ),
        "decisions": sum(r["cyclomatic_complexity"] - 1 for r in rows),
        "max_nesting": max(r["max_nesting"] for r in rows),
        "functions": len(rows),
    }


def project_scorecard(baseline, candidate, target, finding, before, after):
    verification = {
        "baseline": {"passed": sum(c["passed"] for c in before["cases"])},
        "candidate": {
            "passed": sum(c["passed"] for c in after["cases"]),
            "total": len(after["cases"]),
        },
    }
    card = scorecard(baseline, candidate, target, finding, verification, allow_line_shift=True)
    old = module_measurement(baseline, target["path"])
    new = module_measurement(candidate, target["path"])
    card["maintainability"]["region"] = {
        k: {"before": old[k], "after": new[k], "delta": round(new[k] - old[k], 4)} for k in old
    }
    card["maintainability"]["region_scope"] = (
        "All functions in the changed file, including new helpers"
    )
    card["correctness"]["assurance"] = (
        "Frozen native cases and all configured project checks passed"
    )
    return card


def review_candidate(agent, packet, finding, index, directory, diff, card, results, patch):
    return agent.phase(
        "quality-review",
        packet,
        finding,
        index,
        directory,
        "Review this exact patch independently. Do not author changes. Assess each criterion once: "
        "correctness (general fix, meaningful tests, preserved behavior), relevance (the evidenced "
        "issue is addressed), readability (clear code and explanations of non-obvious rules), "
        "simplicity (less cognitive load; no compression or hiding tangled code in helpers). All "
        "must pass. Reject tests that mirror implementation or only check a hard-coded example. "
        "The proposed PR description must accurately describe the change and contain no test "
        "execution claims; the coordinator adds verified results separately. Reject misleading "
        "public text under correctness.\nPROPOSED PR TEXT\n"
        + encode({k: patch[k] for k in ("title", "description")})
        + "\n"
        "PATCH\n"
        + diff
        + "\nMEASUREMENTS\n"
        + encode(card)
        + "\nNATIVE CHECK RESULTS\n"
        + encode(results),
        [],
    )


def candidate(agent, root, runner, packet, index, finding, plan, before, output, feedback):
    target = packet["target"]
    original = index.sources[target["path"]]
    detail = (
        "Produce a focused patch for this issue. Return exact old/new text edits applied "
        "sequentially to ONLY " + target["path"] + ". Preserve public signatures and contracts. "
        "You may add cohesive helpers in this file. Frozen tests and other files cannot change. "
        "Improve maintainability while addressing the objective. Do not compress code or bolt on "
        "nested special cases. The coordinator measures scores; do not calculate Halstead scores. "
        "title and description will be used in a public pull request. Describe only the concrete "
        "problem and changed behavior in description. Do not put test status or execution claims "
        "there; the coordinator adds verified results after running checks. summary is an internal "
        "author note and is not published.\nSOURCE FILE\n"
        + original.decode()
        + "\nFROZEN TEST FILE\n"
        + safe_path(root, plan["test_path"]).read_text()
        + "\nPREVIOUS ATTEMPT FEEDBACK\n"
        + encode(feedback)
    )
    patch = agent.phase("native-patch", packet, finding, index, output, detail, [])
    if patch["status"] != "ready":
        raise ValueError(patch["summary"])
    safe_path(root, target["path"]).write_bytes(apply_edits(original, patch["edits"]))
    runner.format_file(target["path"], output.name + "-format")
    after = runner.frozen(plan["test_path"], output.name + "-frozen")
    checks = runner.commands("checks", output.name + "-checks")
    if (
        after["exit_code"]
        or not after["cases"]
        or not all(c["passed"] for c in after["cases"])
        or not checks_passed(checks)
    ):
        return (
            patch,
            None,
            {
                "passed": False,
                "failures": ["Frozen tests and project checks must pass"],
                "frozen": after,
                "checks": checks,
            },
        )
    scanned = scan(root, ScanOptions(functions=True))
    if not scanned["complete"]:
        return patch, None, {"passed": False, "failures": ["Candidate scan is incomplete"]}
    card = project_scorecard(index.report, scanned, target, finding, before, after)
    write_json(output / "scorecard.json", card)
    return patch, card, {**metric_gate(card), "checks": checks}


def publish_candidate(
    workspace, root, branch, issue, relative, plan, patch, card, metrics, manifest, directory
):
    test_unchanged(root, plan["test_path"], manifest["tests_sha256"])
    if digest(safe_path(root, relative).read_bytes()) != manifest["candidate_sha256"]:
        raise ValueError("Candidate changed after independent review")
    remote = workspace.github.api("GET", "/commits/" + quote(workspace.base_branch, safe=""))
    if remote["sha"] != workspace.sha:
        raise ValueError("Base branch changed during validation; rerun before publishing")
    commit = workspace.publish(root, branch, [relative, plan["test_path"]], patch["title"])
    manifest.update(status="branch-pushed", head_commit=commit, branch=branch)
    write_json(directory.parent.parent / "workflow.json", manifest)
    payload = {
        "title": patch["title"],
        "body": pull_body(issue, patch["description"], card, metrics["checks"]),
        "head": branch,
        "base": workspace.base_branch,
    }
    write_json(directory / "pull-request.json", payload)
    query = "/pulls?state=open&head=" + quote(
        workspace.github.repository.owner + ":" + branch, safe=""
    )
    existing = list(workspace.github.pages(query))
    pull = existing[0] if existing else workspace.github.api("POST", "/pulls", payload)
    manifest.update(
        status="pull-request", pull_request={"number": pull["number"], "url": pull["html_url"]}
    )
    card["state"] = "pull-request"
    write_json(directory / "scorecard.json", card)


def iterate(
    agent,
    workspace,
    root,
    branch,
    runner,
    packet,
    index,
    finding,
    plan,
    before,
    manifest,
    output,
    issue,
    progress,
):
    relative = packet["target"]["path"]
    original = index.sources[relative]
    seen, feedback = set(), None
    for number in range(1, MAX_ATTEMPTS + 1):
        safe_path(root, relative).write_bytes(original)
        directory = output / "attempts" / f"{number:03}"
        directory.mkdir(parents=True)
        progress(f"Attempt {number}/{MAX_ATTEMPTS}: patch, native tests, quality checks")
        patch, card, metrics = candidate(
            agent, root, runner, packet, index, finding, plan, before, directory, feedback
        )
        updated = safe_path(root, relative).read_bytes()
        fingerprint = digest(updated)
        if fingerprint in seen:
            raise ValueError("Writer repeated a rejected candidate")
        seen.add(fingerprint)
        test_unchanged(root, plan["test_path"], manifest["tests_sha256"])
        changed = git(root, "diff", "--name-only", "HEAD").splitlines()
        if set(changed) - {relative, plan["test_path"]}:
            raise ValueError("Project commands changed files outside the assigned source")
        git(root, "add", "--intent-to-add", "--", plan["test_path"])
        diff = git(root, "diff", "--", relative, plan["test_path"])
        (directory / "patch.diff").write_text(diff + "\n")
        review = {"status": "skipped", "summary": "Measured gates failed", "checks": []}
        if metrics["passed"]:
            review = review_candidate(
                agent, packet, finding, index, directory, diff, card, metrics["checks"], patch
            )
        record = acceptance(
            updated, manifest["tests_sha256"], index.report["source_fingerprint"], metrics, review
        )
        write_json(directory / "acceptance.json", record)
        manifest["attempts"].append(
            {"number": number, "passed": record["passed"], "artifacts": str(directory)}
        )
        agent.save()
        if record["passed"]:
            manifest["candidate_sha256"] = fingerprint
            card["state"] = "verified-candidate"
            if finding["objective"] == "bug":
                card["correctness"].update(
                    confirmed_open={"before": 1, "after": 0, "delta": -1},
                    verified_resolutions=1,
                )
            write_json(directory / "scorecard.json", card)
            return publish_candidate(
                workspace,
                root,
                branch,
                issue,
                relative,
                plan,
                patch,
                card,
                metrics,
                manifest,
                directory,
            )
        feedback = {"patch": patch, "metrics": metrics, "review": review}
    manifest["status"] = "rejected"


def freeze_native_tests(agent, runner, root, packet, finding, index, output, manifest):
    plan = agent.phase(
        "native-tests",
        packet,
        finding,
        index,
        output,
        "Write one new native test file using this project's test framework. No production patch "
        "yet. The coordinator formats and freezes it before any patch. Exercise the evidenced "
        "contract through the real code, including boundaries. For bugs, list regression_tests "
        "as exact JUnit case names: these must fail with assertions on the baseline while other "
        "new tests pass. For refactors, regression_tests must be empty and all characterization "
        "tests must already pass. Avoid parameterized names unless exact JUnit names are supplied. "
        "Do not mirror implementation or weaken existing tests.\nPROJECT COMMANDS\n"
        + encode(runner.config),
        [],
    )
    if plan["status"] != "ready":
        raise ValueError(plan["summary"])
    freeze_test(root, plan)
    runner.format_file(plan["test_path"], "format-test")
    plan["test_content"] = safe_path(root, plan["test_path"]).read_text()
    manifest["tests_sha256"] = digest(plan["test_content"].encode())
    write_json(output / "test-plan.json", plan)
    before = runner.frozen(plan["test_path"], "baseline-frozen")
    if not reproduced(before, plan["regression_tests"], finding["objective"]):
        raise ValueError("Native tests did not reproduce the bug or preserve baseline behavior")
    manifest["status"] = "tests-frozen"
    agent.save()
    return plan, before


def improve_issue(
    github, workspace, issue_number, *, config, output=None, progress=print, invoke=None
):
    for pull in github.pages("/pulls?state=open"):
        if f"<!-- walleye-issue:{issue_number} -->" in (pull.get("body") or ""):
            raise ValueError(f"This finding already has an open pull request: {pull['html_url']}")
    issue = github.api("GET", f"/issues/{issue_number}")
    if issue.get("state") != "open" or "pull_request" in issue:
        raise ValueError("Improve requires an open finding issue")
    metadata = read_metadata(issue.get("body"), github.repository)
    identity = github.commit_identity()
    root, branch = workspace.worktree(issue_number)
    finding, target = metadata["finding"], metadata["target"]
    if digest(safe_path(root, target["path"]).read_bytes()) != finding["source_sha256"]:
        raise ValueError("Issue source changed; review the current commit before improving")
    manifest, packets, index, output = prepare_review(
        root, 1, finding["objective"], config, output, progress, targets=[target]
    )
    native_packet(packets[0], index, config.context_tokens)
    write_json(output / "tasks/001.json", packets[0])
    fields = response_schema()["properties"]["finding"]["anyOf"][0]["properties"]
    validate_response(
        {
            "status": "finding",
            "summary": finding["title"],
            "context_requests": [],
            "finding": {k: finding[k] for k in fields},
        },
        packets[0],
        index,
    )
    manifest.update(
        workflow_profile="declank-github-v1",
        status="baseline",
        issue=issue["html_url"],
        repository=github.repository.full_name,
        commit_author=identity,
        base_commit=workspace.sha,
        base_branch=workspace.base_branch,
        worktree=str(root),
        branch=branch,
        attempts=[],
    )
    write_json(output / "baseline.json", index.report)
    agent = WorkflowAgent(manifest, output, config, invoke=invoke, progress=progress)
    agent.save()
    try:
        runner = ProjectTests(root, output / "checks")
        progress("Installing dependencies and running the existing project checks")
        setup = runner.commands("setup", "setup")
        if setup and not checks_passed(setup):
            raise ValueError("Project setup failed")
        if not checks_passed(runner.commands("checks", "baseline")):
            raise ValueError("Existing project checks fail before the patch")
        plan, before = freeze_native_tests(
            agent, runner, root, packets[0], finding, index, output, manifest
        )
        # Validate agent context against the immutable clone, while editing its worktree.
        index.base = workspace.repo
        iterate(
            agent,
            workspace,
            root,
            branch,
            runner,
            packets[0],
            index,
            finding,
            plan,
            before,
            manifest,
            output,
            issue_number,
            progress,
        )
    except (ValueError, OSError) as error:
        manifest.update(status="stopped", error=str(error))
    agent.save()
    return manifest, output
