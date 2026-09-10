from types import SimpleNamespace

import pytest

from walleye.github import Repository
from walleye.github_publication import (
    annotated_quote,
    ensure_label,
    finding_metadata,
    issue_body,
    publish_findings,
    read_metadata,
)


@pytest.fixture
def finding():
    return {
        "objective": "bug",
        "title": "Quoted path keys are split at an embedded closing bracket",
        "context": "The view renderer resolves object paths through pathParts.",
        "root_cause": (
            "The view renderer resolves object paths through pathParts. "
            "Its first-] search splits quoted keys before unquoting them."
        ),
        "trigger": 'Resolve a["x]y"] against {a: {"x]y": 1}}.',
        "impact": "The affected view cannot display this object's value.",
        "explanation": [
            {
                "text": "The first closing bracket is inside the quoted key, so the key is split.",
                "evidence": [1],
            }
        ],
        "expected_behavior": "Return the value 1.",
        "actual_behavior": "The split key does not match the object, so the value is missing.",
        "proposed_change": "Find the closing bracket after the quoted key.",
        "preserved_behavior": "Keep dot paths, numeric indexes, and invalid-path rejection.",
        "expected_benefit": "Views can display values stored under quoted keys containing ].",
        "validation": (
            "Assert the parsed key is x]y and the object lookup returns 1; cover both quote styles."
        ),
        "severity": "medium",
        "confidence": "high",
        "evidence": [
            {
                "path": "src/path.ts",
                "line": 9,
                "end_line": 9,
                "quote": 'const close = path.indexOf("]", cursor + 1);',
                "annotations": [{"quote_line": 1, "text": "Stops at ] inside a quoted key"}],
            }
        ],
        "task_id": "001",
    }


@pytest.fixture
def metadata(finding):
    return finding_metadata(
        Repository("acme", "views"),
        "a" * 40,
        "main",
        {"id": "target", "path": "src/path.ts", "line": 3, "end_line": 20, "name": "pathParts"},
        finding,
    )


def test_issue_contains_actionable_context_and_preserves_machine_record(metadata):
    repo = Repository("acme", "views")
    body = issue_body(repo, metadata)
    assert body.startswith("## Summary\n\n**Context:** The view renderer resolves object paths")
    assert "**Trigger:**" in body
    assert "## Explanation" in body and "[source 1]" in body
    assert "**Impact:** The affected view" in body
    assert "## Code with bug" in body and "## Recommended fix" in body
    assert metadata["finding"]["expected_benefit"] in body
    assert metadata["finding"]["validation"] in body
    assert "not yet been reproduced by running tests" in body
    assert f"/blob/{'a' * 40}/src/path.ts#L9-L9" in body
    assert "## Call context" not in body  # Missing graph evidence must not invent a caller.
    assert read_metadata(body, repo) == metadata
    assert ");  // <-- BUG 🔴 Stops at ] inside a quoted key" in body
    assert "<-- BUG" not in metadata["finding"]["evidence"][0]["quote"]


def test_architecture_issue_explains_benefit_without_bug_severity(metadata):
    metadata["finding"].update(
        objective="refactor", trigger="", actual_behavior="", expected_behavior=""
    )
    body = issue_body(Repository("acme", "views"), metadata)
    assert "**Expected benefit:**" in body and "## Proposed change" in body
    assert "**Trigger:**" not in body and "severity" not in body
    assert "## Code with bug" not in body
    assert "Architecture improvement" in body
    assert "// <-- ARCHITECTURE 🟡" in body and "BUG 🔴" not in body
    assert "proposed refactor and its tests have not been run" in body


def test_issue_links_call_context_and_handles_fences_in_source(finding, metadata):
    nodes = [
        {
            "id": "caller",
            "path": "src/view.ts",
            "qualified_name": "render",
            "line": 1,
            "end_line": 5,
        },
        {
            "id": "target",
            "path": "src/path.ts",
            "qualified_name": "pathParts",
            "line": 3,
            "end_line": 20,
        },
    ]
    graph = {"nodes": nodes, "edges": [{"source": "caller", "target": "target"}] * 10}
    repo = Repository("acme", "views")
    data = finding_metadata(repo, "a" * 40, "main", metadata["target"], finding, graph=graph)
    data["finding"]["evidence"][0]["quote"] = "const fence = '```';"
    data["finding"]["evidence"][0]["annotations"] = []
    body = issue_body(repo, data)
    assert len(data["graph"]["edges"]) == 6
    assert body.count("- [render]") == 1
    assert f"[pathParts]({repo.url}/blob/{'a' * 40}/src/path.ts#L3-L20)" in body
    assert "````typescript\nconst fence = '```';\n````" in body


@pytest.mark.parametrize("objective,name", [("bug", "bugs"), ("refactor", "architecture")])
def test_missing_category_label_is_created_once(objective, name):
    labels, calls = [], []

    def api(method, suffix, payload):
        calls.append((method, suffix, payload))
        labels.append(payload)
        return payload

    client = SimpleNamespace(pages=lambda _: iter(labels), api=api)
    assert ensure_label(client, objective) == name
    assert ensure_label(client, objective) == name
    assert len(calls) == 1 and calls[0][:2] == ("POST", "/labels")


def test_label_creation_race_is_reconciled_but_permission_errors_propagate():
    labels = []

    def raced(method, suffix, payload):
        labels.append({"name": "Architecture"})
        raise ValueError("HTTP 422")

    client = SimpleNamespace(pages=lambda _: iter(labels), api=raced)
    assert ensure_label(client, "refactor") == "Architecture"

    def denied(*args):
        raise ValueError("HTTP 403")

    client.api = denied
    with pytest.raises(ValueError, match="403"):
        ensure_label(client, "bug")


def test_existing_finding_gets_category_without_replacing_other_labels(metadata, tmp_path):
    repo = Repository("acme", "views")
    issue = {
        "number": 3,
        "html_url": repo.url + "/issues/3",
        "body": issue_body(repo, metadata),
        "labels": [{"name": "triage"}],
    }
    calls = []
    client = SimpleNamespace(
        repository=repo,
        pages=lambda suffix: iter([{"name": "bugs"}] if suffix == "/labels" else [issue]),
        api=lambda *args: calls.append(args),
    )
    workspace = SimpleNamespace(sha=metadata["commit"], base_branch="main")
    result = publish_findings(
        client,
        {"findings": [metadata["finding"]], "github": {"issues": []}},
        [{"task_id": "001", "target": metadata["target"]}],
        workspace,
        output=tmp_path,
    )
    assert result[0]["number"] == 3
    assert calls == [("POST", "/issues/3/labels", {"labels": ["bugs"]})]
    assert issue["labels"] == [{"name": "triage"}]


def test_no_findings_do_not_create_labels_or_issues():
    def unexpected(*args):
        pytest.fail("Empty reviews must not publish anything")

    client = SimpleNamespace(pages=lambda _: iter([]), api=unexpected)
    assert publish_findings(client, {"findings": []}, [], None) == []


@pytest.mark.parametrize("language,comment", [("python", "#"), ("rust", "//"), ("sql", "--")])
def test_callouts_anchor_to_quoted_lines_even_with_sparse_source_numbers(language, comment):
    item = {
        "quote": "12: first_statement()\n29: dangerous_statement()",
        "annotations": [{"quote_line": 2, "text": "Reuses the stale handle"}],
    }
    result = annotated_quote(item, language, "bug")
    assert result.splitlines()[0] == "12: first_statement()"
    assert result.splitlines()[1] == (
        f"29: dangerous_statement()  {comment} <-- BUG 🔴 Reuses the stale handle"
    )
    assert item["quote"] == "12: first_statement()\n29: dangerous_statement()"


def test_supporting_excerpt_is_left_unmarked_and_annotation_fences_cannot_escape(metadata):
    item = metadata["finding"]["evidence"][0]
    item["annotations"] = []
    assert annotated_quote(item, "typescript", "bug") == item["quote"]
    item["annotations"] = [{"quote_line": 1, "text": "Misreads the ``` delimiter"}]
    body = issue_body(Repository("acme", "views"), metadata)
    assert "````typescript\n" in body
    assert "// <-- BUG 🔴 Misreads the ``` delimiter\n````" in body
