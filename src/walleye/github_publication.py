"""Plain issue/PR text and durable finding metadata for later improvement jobs."""

import base64
import hashlib
import json
import re
from urllib.parse import quote

WRITING = """Write issue and pull request text as a concise engineer speaking to another engineer.
Lead with the concrete problem and resulting behavior. Use plain words, precise filenames,
and evidence. No hype, canned AI phrases, flattery, emojis, theatrical headings, marketing,
or claims of verification you did not receive. Avoid 'leverage', 'delve', 'robust', 'seamless',
'enhance', 'comprehensive', and 'it's worth noting'. Explain why the change matters and what
was actually tested. Do not mention these writing instructions in the result.
"""
MARKER = "walleye-finding-v1"


def finding_metadata(repository, sha, branch, target, finding):
    identity = {k: finding[k] for k in ("objective", "root_cause")}
    identity.update(repository=repository.full_name, path=target["path"], name=target["name"])
    key = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
    return {
        "version": 1,
        "key": key,
        "repository": repository.full_name,
        "commit": sha,
        "base_branch": branch,
        "target": target,
        "finding": finding,
    }


def issue_body(repository, metadata):
    finding = metadata["finding"]
    paragraphs = [finding["root_cause"]]
    fields = (
        ("trigger", "Trigger"),
        ("expected_behavior", "Expected"),
        ("actual_behavior", "Actual"),
        ("proposed_change", "Change"),
        ("preserved_behavior", "Preserves"),
        ("validation", "Validation"),
    )
    paragraphs.extend(f"**{label}:** {finding[key]}" for key, label in fields if finding[key])
    for item in finding["evidence"]:
        url = (
            repository.url
            + "/blob/"
            + metadata["commit"]
            + "/"
            + quote(item["path"], safe="/")
            + f"#L{item['line']}-L{item['end_line']}"
        )
        paragraphs.append(f"[{item['path']}:{item['line']}]({url})\n\n```\n{item['quote']}\n```")
    paragraphs.append(
        f"{finding['objective'].capitalize()} · {finding['severity']} severity · "
        f"{finding['confidence']} confidence. Source evidence checked; "
        "tests have not been run for this finding."
    )
    payload = base64.b64encode(json.dumps(metadata, separators=(",", ":")).encode()).decode()
    paragraphs.append(f"<!-- {MARKER}:{payload} -->")
    body = "\n\n".join(paragraphs)
    if len(body) > 60000:
        raise ValueError("Finding is too large for a GitHub issue")
    return body


def read_metadata(body, repository):
    matches = re.findall(r"<!-- " + MARKER + r":([A-Za-z0-9+/=]+) -->", body or "")
    if len(matches) != 1:
        raise ValueError("Issue needs one walleye finding record; start with walleye review")
    try:
        value = json.loads(base64.b64decode(matches[0], validate=True))
        if value["version"] != 1 or value["repository"].lower() != repository.full_name.lower():
            raise ValueError("Issue metadata belongs to another repository or version")
        if not re.fullmatch(r"[a-f0-9]{40}", value["commit"]):
            raise ValueError("Issue metadata has an invalid commit")
        return value
    except (KeyError, TypeError, json.JSONDecodeError) as error:
        raise ValueError("Invalid walleye issue metadata") from error


def publish_findings(github, manifest, packets, workspace, *, output=None):
    from .review import write_json

    existing = {}
    for issue in github.pages("/issues?state=all"):
        if "pull_request" in issue:
            continue
        try:
            data = read_metadata(issue.get("body"), github.repository)
            existing[data["key"]] = issue
        except ValueError:
            continue
    by_id = {p["task_id"]: p for p in packets}
    published = []
    for finding in manifest["findings"]:
        packet = by_id[finding["task_id"]]
        metadata = finding_metadata(
            github.repository, workspace.sha, workspace.base_branch, packet["target"], finding
        )
        issue = existing.get(metadata["key"])
        if issue is None:
            if output:
                write_json(
                    output / f"issue-{finding['task_id']}-request.json",
                    {"title": finding["title"], "body": issue_body(github.repository, metadata)},
                )
            issue = github.api(
                "POST",
                "/issues",
                {"title": finding["title"], "body": issue_body(github.repository, metadata)},
            )
            existing[metadata["key"]] = issue
        published.append(
            {
                "task_id": finding["task_id"],
                "number": issue["number"],
                "url": issue["html_url"],
                "key": metadata["key"],
            }
        )
        if output:
            manifest["github"]["issues"] = published
            write_json(output / "review.json", manifest)
    return published


def pull_body(issue, description, card, tests):
    quality = card["maintainability"]["repository"]["score"]
    target = card["maintainability"]["target_quality"]
    module = card["maintainability"]["region"]["quality"]
    return (
        description.strip() + f"\n\nCloses #{issue}.\n<!-- walleye-issue:{issue} -->\n\n"
        f"Validation: frozen native tests and all {len(tests)} configured project checks passed. "
        "The independent correctness and maintainability review passed.\n\n"
        "| Structural quality | Before | After |\n| --- | ---: | ---: |\n"
        f"| Changed function | {target['before']:.4f} | {target['after']:.4f} |\n"
        f"| Changed module, including helpers | {module['before']:.4f} | {module['after']:.4f} |\n"
        f"| Repository | {quality['before']:.4f} | {quality['after']:.4f} |\n\n"
        "Checks run:\n"
        + "\n".join("- `" + " ".join(t["command"]) + "`" for t in tests)
        + "\n\nScores compare the same parsed source files. These are candidate scores; "
        "the base branch changes only after merge. "
        "Unresolved calls and unreviewed behavior remain unknown."
    )
