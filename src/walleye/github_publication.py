"""Plain issue/PR text and durable finding metadata for later improvement jobs."""

import base64
import hashlib
import json
import re
from pathlib import Path
from urllib.parse import quote

from .languages import detect

WRITING = """Write issue and pull request text as a concise engineer speaking to another engineer.
Assume the reader has never seen the review packet or this conversation. Explain the component's
role, the relevant caller or data flow, the concrete problem, and its consequence before the fix.
Use exact symbols and examples from the supplied source; never invent product context or callers.
Titles must name the affected behavior or responsibility, not 'improve method', 'clean up code',
or a score increase. Bug issue titles describe the failure; PR titles describe the concrete fix.
Refactor titles name the responsibility being separated and why.
Explain maintenance benefits as specific changes that become easier
to make or verify, not 'improves maintainability'. Scores alone do not justify a change.
Do not pad a refactor with statements denying unrelated crashes, data loss, or timing bugs.
Use Markdown paragraphs, short lists, and descriptive headings. Include enough substance to
review the proposal without opening another issue. No hype, canned AI phrases, flattery,
or decorative emojis outside source callouts,
theatrical headings, marketing,
or claims of verification you did not receive. Avoid 'leverage', 'delve', 'robust', 'seamless',
'enhance', 'comprehensive', and 'it's worth noting'. Explain why the change matters and what
was actually tested. Do not mention these writing instructions in the result.
"""
PR_WRITING = """The title and description will be used in a public pull request.
The title must name the component and the specific behavioral fix or responsibility separated.
Write description as Markdown with ## Problem, ## Changes, ## Compatibility, and ## Tests added.
In Problem, explain the component's role in the program, the relevant caller or data flow when
supported by source, and what fails or makes future changes difficult. Give a concrete input/output
example for bugs. In Changes, name the symbols changed, explain the approach and why it addresses
that causal chain, including any meaningful tradeoff. Explain why the fix is scoped this way
instead of changing unrelated behavior. In Compatibility, name the contracts and edge
cases preserved. In Tests added, name the tests and the behavior each exercises; distinguish
regression from characterization coverage. For bugs, explain how tests trigger the failure and
check recovery or isolation where relevant, not just a happy path. Use the frozen test file
supplied. Use one bullet per test or behavior group instead of a list of test names without reasons.
The frozen test file is NEW and will be included in this PR together with the production patch.
Describe the complete PR, not just the edits returned in this turn. Do not say no tests were added
or describe internal workflow restrictions. If review rejects only the public text, correct the
text while preserving code that already passed; do not make gratuitous source changes.
Keep detail proportional to the change, but do not reduce the description to 'improves this method'
or a score claim. Do not put test status or execution claims in description; the coordinator adds
verified results after running checks. summary is an internal author note and is not published.
"""
MARKER = "walleye-finding-v1"
LABELS = {
    "bug": {
        "name": "bugs",
        "color": "d73a4a",
        "description": "Behavioral defects with source evidence",
    },
    "refactor": {
        "name": "architecture",
        "color": "5319e7",
        "description": "Code structure and maintainability improvements",
    },
}
COMMENT_STYLES = {
    **dict.fromkeys(
        "python bash fish ruby perl r julia elixir powershell yaml toml nim nix starlark".split(),
        ("#", ""),
    ),
    **dict.fromkeys("sql lua luau haskell purescript ada vhdl".split(), ("--", "")),
    **dict.fromkeys("clojure commonlisp scheme racket emacs_lisp assembly".split(), (";", "")),
    **dict.fromkeys("html xml vue svelte".split(), ("<!--", " -->")),
    **dict.fromkeys("css scss".split(), ("/*", " */")),
    **dict.fromkeys("ocaml ocaml_interface fsharp".split(), ("(*", " *)")),
    **dict.fromkeys("erlang matlab".split(), ("%", "")),
    "fortran": ("!", ""),
    "vim": ('"', ""),
    "tcl": ("; #", ""),
}


def finding_metadata(repository, sha, branch, target, finding, *, graph=None):
    identity = {k: finding[k] for k in ("objective", "root_cause")}
    identity.update(repository=repository.full_name, path=target["path"], name=target["name"])
    key = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
    metadata = {
        "version": 1,
        "key": key,
        "repository": repository.full_name,
        "commit": sha,
        "base_branch": branch,
        "target": target,
        "finding": finding,
    }
    if graph:
        # Keep a few source-backed relationships, not the entire repository graph.
        edges = graph.get("edges", [])[:6]
        endpoints = {e[k] for e in edges for k in ("source", "target")}
        metadata["graph"] = {
            "edges": edges,
            "nodes": [n for n in graph.get("nodes", []) if n["id"] in endpoints],
        }
    return metadata


def source_link(repository, commit, item, label=None):
    location = f"{item['path']}:{item['line']}-{item['end_line']}"
    url = (
        f"{repository.url}/blob/{commit}/{quote(item['path'], safe='/')}"
        f"#L{item['line']}-L{item['end_line']}"
    )
    return f"[{label or location}]({url})"


def call_context(repository, metadata):
    graph = metadata.get("graph", {})
    nodes = {n["id"]: n for n in graph.get("nodes", []) if "path" in n}
    lines = []
    for edge in graph.get("edges", []):
        if edge["source"] not in nodes or edge["target"] not in nodes:
            continue
        links = [
            source_link(
                repository, metadata["commit"], nodes[edge[k]], nodes[edge[k]]["qualified_name"]
            )
            for k in ("source", "target")
        ]
        line = "- " + " → ".join(links)
        if line not in lines:
            lines.append(line)
    if not lines:
        return ""
    return "## Call context\n\nResolved static references:\n\n" + "\n".join(lines)


def issue_summary(finding):
    paragraphs = ["## Summary"]
    if finding.get("context"):
        paragraphs.append("**Context:** " + finding["context"])
    label = "Bug" if finding["objective"] == "bug" else "Architecture"
    paragraphs.append(f"**{label}:** " + finding["root_cause"])
    if finding["objective"] == "bug":
        paragraphs.extend(
            [
                "**Actual:** " + finding["actual_behavior"],
                "**Expected:** " + finding["expected_behavior"],
            ]
        )
    if impact := finding.get("impact") or finding.get("expected_benefit"):
        paragraphs.append("**Impact:** " + impact)
    return "\n\n".join(paragraphs)


def annotated_quote(item, language, objective):
    """Add display-only notes; the stored source quote remains exact."""
    annotations = {a["quote_line"]: a["text"] for a in item.get("annotations", [])}
    marker = "BUG 🔴" if objective == "bug" else "ARCHITECTURE 🟡"
    opening, closing = COMMENT_STYLES.get(language, ("//", ""))
    lines = item["quote"].splitlines()
    for number, note in annotations.items():
        lines[number - 1] += f"  {opening} <-- {marker} {note}{closing}"
    return "\n".join(lines)


def source_evidence(repository, metadata):
    excerpts = []
    for number, item in enumerate(metadata["finding"]["evidence"], 1):
        language = detect(Path(item["path"])) or ""
        code = annotated_quote(item, language, metadata["finding"]["objective"])
        fence = "`" * max(3, max((len(m[0]) + 1 for m in re.finditer(r"`+", code)), default=0))
        excerpts.append(
            f"**{number}.** "
            + source_link(repository, metadata["commit"], item)
            + f"\n\n{fence}{language}\n{code}\n{fence}"
        )
    title = "Code with bug" if metadata["finding"]["objective"] == "bug" else "Source evidence"
    note = (
        "Callouts added by Walleye.\n\n"
        if any(item.get("annotations") for item in metadata["finding"]["evidence"])
        else ""
    )
    return f"## {title}\n\n" + note + "\n\n".join(excerpts)


def explanation_body(repository, metadata):
    lines = []
    for number, step in enumerate(metadata["finding"].get("explanation", []), 1):
        citations = [
            source_link(
                repository,
                metadata["commit"],
                metadata["finding"]["evidence"][i - 1],
                f"source {i}",
            )
            for i in step["evidence"]
        ]
        lines.append(f"{number}. {step['text']} " + ", ".join(citations))
    return "## Explanation\n\n" + "\n".join(lines) if lines else ""


def issue_body(repository, metadata):
    finding, target = metadata["finding"], metadata["target"]
    paragraphs = [
        issue_summary(finding),
        f"**Location:** `{target['name']}` in "
        + source_link(repository, metadata["commit"], target),
    ]
    if finding["objective"] == "bug":
        paragraphs.append("**Trigger:** " + finding["trigger"])
    paragraphs.append(source_evidence(repository, metadata))
    if explanation := explanation_body(repository, metadata):
        paragraphs.append(explanation)
    if context := call_context(repository, metadata):
        paragraphs.append(context)
    if finding.get("proposed_change"):
        title = "Recommended fix" if finding["objective"] == "bug" else "Proposed change"
        paragraphs.append(f"## {title}\n\n" + finding["proposed_change"])
    if finding.get("expected_benefit"):
        paragraphs.append("**Expected benefit:** " + finding["expected_benefit"])
    if finding.get("preserved_behavior"):
        paragraphs.append("**Behavior to preserve:** " + finding["preserved_behavior"])
    paragraphs.append("## Validation plan\n\n" + finding["validation"])
    paragraphs.append(
        "Source evidence checked. This finding has not yet been reproduced by running tests."
        if finding["objective"] == "bug"
        else "Source evidence checked. The proposed refactor and its tests have not been run."
    )
    assessment = (
        f"Potential bug · {finding['severity']} severity"
        if finding["objective"] == "bug"
        else "Architecture improvement"
    )
    paragraphs.append(
        f"{assessment} · {finding['confidence']} confidence. "
        f"Reviewed at [{metadata['commit'][:7]}]({repository.url}/commit/{metadata['commit']})."
    )
    payload = base64.b64encode(json.dumps(metadata, separators=(",", ":")).encode()).decode()
    paragraphs.append(f"<!-- {MARKER}:{payload} -->")
    body = "\n\n".join(paragraphs)
    if len(body) > 60000:
        raise ValueError("Finding is too large for a GitHub issue")
    return body


def ensure_label(github, objective):
    label = LABELS[objective]

    def existing():
        return next(
            (
                item["name"]
                for item in github.pages("/labels")
                if item["name"].casefold() == label["name"]
            ),
            None,
        )

    if name := existing():
        return name
    try:
        return github.api("POST", "/labels", label)["name"]
    except ValueError:
        # Another job may have created it. Re-read, without retrying the mutation.
        if name := existing():
            return name
        raise


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
    labels = {}
    for finding in manifest["findings"]:
        packet = by_id[finding["task_id"]]
        metadata = finding_metadata(
            github.repository,
            workspace.sha,
            workspace.base_branch,
            packet["target"],
            finding,
            graph=packet.get("graph"),
        )
        objective = finding["objective"]
        if objective not in labels:
            labels[objective] = ensure_label(github, objective)
        label = labels[objective]
        issue = existing.get(metadata["key"])
        if issue is None:
            payload = {
                "title": finding["title"],
                "body": issue_body(github.repository, metadata),
                "labels": [label],
            }
            if output:
                write_json(
                    output / f"issue-{finding['task_id']}-request.json",
                    payload,
                )
            issue = github.api(
                "POST",
                "/issues",
                payload,
            )
            existing[metadata["key"]] = issue
        elif label not in [item["name"] for item in issue.get("labels", [])]:
            github.api("POST", f"/issues/{issue['number']}/labels", {"labels": [label]})
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


def pull_body(issue, description, card, tests, *, test_path):
    quality = card["maintainability"]["repository"]["score"]
    target = card["maintainability"]["target_quality"]
    module = card["maintainability"]["region"]["quality"]
    paragraphs = [description.strip(), f"Closes #{issue}.\n<!-- walleye-issue:{issue} -->"]
    validation = [
        "## Validation",
        f"Tests in `{test_path}` were run against both the original and patched code.",
    ]
    cases = card["correctness"].get("test_cases", [])
    if cases:
        rows = ["| Test | Before | After |", "| --- | --- | --- |"]
        rows.extend(
            f"| `{c['name'].replace('|', '&#124;')}` | {c['before']} | {c['after']} |"
            for c in cases
        )
        validation.append("\n".join(rows))
    else:
        counts = card["correctness"]["tests_passing"]
        validation.append(f"New tests passing: {counts['before']} before, {counts['after']} after.")
    validation.append(
        "Project checks:\n\n"
        + "\n".join(
            "- "
            + ("Passed" if t["exit_code"] == 0 else f"Failed (exit {t['exit_code']})")
            + ": `"
            + " ".join(t["command"])
            + "`"
            for t in tests
        )
    )
    validation.append("The patch and tests also passed an independent agent review.")
    paragraphs.append("\n\n".join(validation))
    scores = [
        "## Measured impact",
        "Quality scores run from 0–100; higher is better.",
        "| Scope | Before | After | Change |\n| --- | ---: | ---: | ---: |",
    ]
    for label, values in (
        ("Repository", quality),
        ("Changed module, including helpers", module),
        ("Changed function", target),
    ):
        scores[-1] += (
            f"\n| {label} | {values['before']:.4f} | {values['after']:.4f} | "
            f"{values['after'] - values['before']:+.4f} |"
        )
    region = card["maintainability"]["region"]
    scores.append(
        "Changed module: "
        + "; ".join(
            f"{label} {region[key]['before']} → {region[key]['after']}"
            for key, label in (("decisions", "decisions"), ("max_nesting", "maximum nesting"))
            if key in region
        )
        + "."
    )
    scores.append(
        f"Scores cover the same {card['scope']['source_files']} parsed source files "
        "and all helpers in the changed module. They measure code structure, not bug probability."
    )
    if comparison_rule := card["scope"].get("quality_comparison"):
        scores.append(comparison_rule + ". Raw scan scores remain available in the scorecard.")
    if comparison := card["scope"].get("comparison_url"):
        scores.append(f"[Exact commits used for this comparison]({comparison})")
    paragraphs.append("\n\n".join(scores))
    return "\n\n".join(paragraphs)
