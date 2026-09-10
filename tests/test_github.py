import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import jwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from walleye.git_workspace import Workspace, git
from walleye.github import GitHub, Repository, repository_input
from walleye.github_publication import finding_metadata, issue_body, publish_findings, read_metadata
from walleye.github_workflow import apply_edits, improve_issue
from walleye.project_tests import project_config, reproduced
from walleye.review import ReviewConfig, prepare_review


@pytest.mark.parametrize(
    "value", ["acme/demo", "https://github.com/acme/demo", "https://github.com/acme/demo.git"]
)
def test_repository_inputs(value):
    assert repository_input(value) == Repository("acme", "demo")
    assert repository_input("https://github.com/acme/demo/issues/42").issue == 42


def test_local_paths_win_and_unsafe_inputs_are_not_repositories(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "acme/demo").mkdir(parents=True)
    assert repository_input("acme/demo") is None
    for value in ["../demo", "/tmp/demo", "https://evil.example/acme/demo", "acme/.."]:
        assert repository_input(value) is None
    with pytest.raises(ValueError):
        repository_input("https://github.com/acme/demo?token=secret")


def test_app_signs_scoped_tokens_and_refreshes_without_persisting_them(tmp_path):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = tmp_path / "app.pem"
    pem.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    now = [1800000000]
    calls = []

    def request(method, path, token, payload=None):
        claims = jwt.decode(
            token,
            key.public_key(),
            algorithms=["RS256"],
            options={"verify_exp": False, "verify_iat": False},
        )
        assert claims == {"iat": now[0] - 60, "exp": now[0] + 540, "iss": "123"}
        calls.append((method, path, payload))
        if path.endswith("/installation"):
            return {"id": 99}
        assert payload == {"repositories": ["demo"]}
        from datetime import datetime, timezone

        return {
            "token": "installation-token-" + str(len(calls)),
            "expires_at": datetime.fromtimestamp(now[0] + 3600, timezone.utc).isoformat(),
        }

    client = GitHub(
        Repository("acme", "demo"),
        request=request,
        environ={
            "GITHUB_APP_ID": "123",
            "GITHUB_APP_PRIVATE_KEY_PATH": str(pem),
            "GITHUB_TOKEN": "do-not-use",
        },
        clock=lambda: now[0],
    )
    first = client.token(required=True)
    assert client.token() == first and len(calls) == 2
    now[0] += 3550
    assert client.token() != first and len(calls) == 3
    assert client.auth_kind == "app"


def test_missing_or_partial_credentials_fail_before_publishing():
    with pytest.raises(ValueError, match="both"):
        GitHub(Repository("acme", "demo"), environ={"GITHUB_APP_ID": "1"})
    client = GitHub(Repository("acme", "demo"), environ={})
    assert client.token() is None
    with pytest.raises(ValueError, match="credentials"):
        client.token(required=True)


@pytest.mark.parametrize("app", [False, True])
def test_commit_author_is_resolved_from_authenticated_user_or_app(app):
    calls = []
    login = "review-fish[bot]" if app else "owner"

    def request(method, path, token, payload=None):
        calls.append(path)
        if path == "/app":
            assert token == "signed-app-jwt"
            return {"slug": "review-fish"}
        assert path == ("/users/review-fish%5Bbot%5D" if app else "/user")
        assert token == "installation-token" if app else token == "user-token"
        return {"id": 123, "login": login, "name": None if app else "Repository Owner"}

    env = {"GITHUB_TOKEN": "user-token"}
    if app:
        env.update(GITHUB_APP_ID="42", GITHUB_APP_PRIVATE_KEY_PATH="unused.pem")
    client = GitHub(Repository("acme", "demo"), environ=env, request=request, clock=lambda: 0)
    if app:
        client._token, client.expires = "installation-token", 3600
        client.app_jwt = lambda: "signed-app-jwt"
    expected = {
        "name": login if app else "Repository Owner",
        "email": f"123+{login}@users.noreply.github.com",
    }
    assert client.commit_identity() == expected
    assert client.commit_identity() == expected
    assert len(calls) == (2 if app else 1)


def test_git_does_not_put_tokens_in_argv_or_remote(monkeypatch, tmp_path):
    def run(command, **kwargs):
        assert "secret-token" not in repr(command)
        assert "secret-token" not in repr(kwargs.get("cwd"))
        assert kwargs["env"]["GIT_CONFIG_KEY_0"] == "http.https://github.com/.extraheader"
        assert "GITHUB_TOKEN" not in kwargs["env"]
        return SimpleNamespace(returncode=0, stdout="ok")

    monkeypatch.setattr(subprocess, "run", run)
    assert git(tmp_path, "fetch", "origin", token="secret-token") == "ok"


def test_native_reproduction_rejects_errors_skips_and_unrelated_failures():
    case = {
        "name": "test_bug",
        "passed": False,
        "assertion_failure": True,
        "error": False,
        "skipped": False,
    }
    result = {"exit_code": 1, "cases": [case]}
    assert reproduced(result, ["test_bug"], "bug")
    for update in [
        {"error": True},
        {"skipped": True},
        {"assertion_failure": False},
        {"name": "test_other"},
    ]:
        assert not reproduced({"exit_code": 1, "cases": [{**case, **update}]}, ["test_bug"], "bug")
    assert not reproduced({"exit_code": 0, "cases": []}, [], "refactor")


def test_edits_are_exact_and_cannot_silently_hit_multiple_sites():
    assert apply_edits(b"abc", [{"old": "b", "new": "d"}]) == b"adc"
    for edits in [[], [{"old": "", "new": "x"}], [{"old": "a", "new": "a"}]]:
        with pytest.raises(ValueError):
            apply_edits(b"abc", edits)
    with pytest.raises(ValueError):
        apply_edits(b"aa", [{"old": "a", "new": "b"}])


def test_extracted_helpers_do_not_make_later_edits_ambiguous():
    original = b"def scan():\n    return build_report()\n"
    edits = [
        {"old": "def scan():", "new": "def finish():\n    return build_report()\n\ndef scan():"},
        {"old": "    return build_report()", "new": "    return finish()"},
    ]
    assert apply_edits(original, edits) == (
        b"def finish():\n    return build_report()\n\ndef scan():\n    return finish()\n"
    )
    with pytest.raises(ValueError, match="overlap"):
        apply_edits(b"abc", [{"old": "ab", "new": "x"}, {"old": "bc", "new": "y"}])


ORIGINAL = "def clamp(x):\n    if x > 10:\n        return 10\n    return x\n"
FIXED = "def clamp(x):\n    return max(0, min(10, x))\n"
TESTS = """from clamp import clamp


def test_negative():
    assert clamp(-1) == 0


def test_middle():
    assert clamp(5) == 5


def test_high():
    assert clamp(20) == 10
"""


@pytest.fixture
def native_repo(tmp_path, monkeypatch):
    source, bare = tmp_path / "source", tmp_path / "remote.git"
    source.mkdir()
    (source / "clamp.py").write_text(ORIGINAL)
    (source / ".gitignore").write_text("__pycache__/\n.pytest_cache/\n")
    (source / "tests").mkdir()
    (source / "tests/test_existing.py").write_text(
        "from clamp import clamp\ndef test_high():\n    assert clamp(20) == 10\n"
    )
    (source / ".walleye.json").write_text(
        json.dumps(
            {
                "setup": [],
                "checks": [[sys.executable, "-m", "pytest", "-q"]],
                "test_command": [
                    sys.executable,
                    "-m",
                    "pytest",
                    "-q",
                    "{test_file}",
                    "--junitxml={report}",
                ],
            }
        )
    )
    git(source, "init", "-b", "main")
    git(source, "add", ".")
    git(
        source,
        "-c",
        "user.name=Fixture",
        "-c",
        "user.email=fixture@example.com",
        "commit",
        "-m",
        "Fixture",
    )
    git(tmp_path, "clone", "--bare", str(source), str(bare))
    sha = git(source, "rev-parse", "HEAD")
    repo = Repository("acme", "demo")
    _, packets, _, _ = prepare_review(source, issues=1, output=tmp_path / "review")
    target = packets[0]["target"]
    finding = {
        "objective": "bug",
        "title": "Clamp negative inputs to zero",
        "root_cause": "The lower bound is missing",
        "severity": "medium",
        "confidence": "high",
        "trigger": "clamp(-1)",
        "expected_behavior": "Return 0",
        "actual_behavior": "Returns -1",
        "proposed_change": "Use the standard bounds operations",
        "preserved_behavior": "Values from zero through ten and the upper bound",
        "expected_benefit": "Correct both bounds with fewer branches",
        "validation": "Check negative, middle and high values",
        "evidence": [{"path": "clamp.py", "line": 1, "end_line": 4, "quote": ORIGINAL.strip()}],
        "source_sha256": target["sha256"],
        "task_id": "001",
    }
    body = issue_body(repo, finding_metadata(repo, sha, "main", target, finding))
    requests = []

    class FakeGitHub:
        repository = repo
        auth_kind = "fixture"

        def token(self, **kwargs):
            return None

        def commit_identity(self):
            return {"name": "Fixture Owner", "email": "123+fixture@users.noreply.github.com"}

        def api(self, method, suffix="", payload=None):
            requests.append((method, suffix, payload))
            if suffix == "":
                return {"default_branch": "main"}
            if suffix == "/issues/1":
                return {"state": "open", "html_url": repo.url + "/issues/1", "body": body}
            if suffix.startswith("/commits/"):
                return {"sha": sha}
            if method == "POST" and suffix == "/pulls":
                return {"number": 2, "html_url": repo.url + "/pull/2"}
            raise AssertionError((method, suffix))

        def pages(self, suffix):
            return iter([])

    import walleye.git_workspace as module

    original_git = module.git

    def local_git(directory, *args, **kwargs):
        args = tuple(str(bare) if a == repo.url + ".git" else a for a in args)
        return original_git(directory, *args, **kwargs)

    monkeypatch.setattr(module, "git", local_git)
    client = FakeGitHub()
    workspace = Workspace(client, directory=tmp_path / "job")
    return client, workspace, requests


def model(replacement=FIXED, reject=False):
    stages = []

    def invoke(prompt, config, limit, *, schema, instructions):
        properties = schema["properties"]
        result = {
            "status": "ready",
            "summary": "Tests were not run by the patch author.",
            "context_requests": [],
        }
        if "test_path" in properties:
            stages.append("tests")
            result.update(
                test_path="tests/test_regression.py",
                test_content=TESTS,
                regression_tests=["test_negative"],
            )
        elif "edits" in properties:
            stages.append("patch")
            result.update(
                title="Clamp negative values to zero",
                description="Clamp both bounds with standard operations.",
                edits=[{"old": ORIGINAL, "new": replacement}],
            )
        else:
            stages.append("review")
            assert "Review this exact patch independently" in prompt
            assert '"description":"Clamp both bounds with standard operations."' in prompt
            result["checks"] = [
                {
                    "criterion": c,
                    "passed": not reject,
                    "reason": "The contract is covered and the code is simple",
                }
                for c in ["correctness", "relevance", "readability", "simplicity"]
            ]
        assert "concise engineer" in instructions
        return {
            "response": result,
            "usage": {"input_tokens": 500, "output_tokens": 500},
            "error": None,
        }

    return stages, invoke


def test_native_worktree_pipeline_runs_real_tests_and_publishes_only_verified_patch(
    native_repo, tmp_path
):
    client, workspace, requests = native_repo
    stages, invoke = model()
    result, output = improve_issue(
        client,
        workspace,
        1,
        config=ReviewConfig(backend="codex"),
        output=tmp_path / "improve",
        invoke=invoke,
        progress=lambda x: None,
    )
    assert result["status"] == "pull-request", result.get("error")
    assert stages == ["tests", "patch", "review"]
    assert (workspace.repo / "clamp.py").read_text() == ORIGINAL
    assert (Path(result["worktree"]) / "clamp.py").read_text() == FIXED
    assert result["cost"]["spent_usd"] == 0.0021
    card = json.loads((output / "attempts/001/scorecard.json").read_text())
    assert card["maintainability"]["region"]["quality"]["delta"] > 0
    assert card["correctness"]["verified_resolutions"] == 1
    assert card["correctness"]["confirmed_open"] == {"before": 1, "after": 0, "delta": -1}
    pull = next(p for method, path, p in requests if method == "POST" and path == "/pulls")
    assert "Closes #1" in pull["body"] and pull["base"] == "main"
    assert pull["body"].startswith("Clamp both bounds with standard operations.")
    assert "Tests were not run by the patch author" not in pull["body"]
    assert "Changed module, including helpers" in pull["body"]
    assert "| `test_negative` | Assertion failed | Passed |" in pull["body"]
    assert "## Validation" in pull["body"] and "## Measured impact" in pull["body"]
    assert "tests/test_regression.py" in pull["body"]
    assert f"/compare/{workspace.sha}...{result['head_commit']}" in pull["body"]
    assert git(Path(result["worktree"]), "log", "-1", "--format=%an <%ae>") == (
        "Fixture Owner <123+fixture@users.noreply.github.com>"
    )
    assert git(workspace.repo, "rev-parse", "refs/remotes/origin/main") == workspace.sha


def test_native_readability_rejection_never_pushes_or_creates_a_pr(native_repo, tmp_path):
    client, workspace, requests = native_repo
    _, invoke = model(reject=True)
    result, _ = improve_issue(
        client,
        workspace,
        1,
        config=ReviewConfig(backend="codex"),
        output=tmp_path / "improve",
        invoke=invoke,
        progress=lambda x: None,
    )
    assert result["status"] == "stopped" and "repeated" in result["error"]
    assert not any(method == "POST" for method, _, _ in requests)
    assert (workspace.repo / "clamp.py").read_text() == ORIGINAL


def test_changed_test_collection_cannot_be_published_as_before_after_evidence(
    native_repo, tmp_path, monkeypatch
):
    from walleye.project_tests import ProjectTests

    original = ProjectTests.frozen

    def changed_cases(self, path, label):
        result = original(self, path, label)
        if label == "001-frozen":
            result["cases"][0]["name"] = "different_test"
        return result

    monkeypatch.setattr(ProjectTests, "frozen", changed_cases)
    client, workspace, requests = native_repo
    _, invoke = model()
    result, _ = improve_issue(
        client,
        workspace,
        1,
        config=ReviewConfig(backend="codex"),
        output=tmp_path / "improve",
        invoke=invoke,
        progress=lambda _: None,
    )
    assert result["status"] == "stopped"
    assert "Frozen test cases changed" in result["error"]
    assert not any(method == "POST" for method, _, _ in requests)


def test_invalid_patch_returns_feedback_without_rewriting_frozen_tests(native_repo, tmp_path):
    client, workspace, requests = native_repo
    stages, invoke = model()
    patches = 0

    def writer(prompt, config, limit, **kwargs):
        nonlocal patches
        result = invoke(prompt, config, limit, **kwargs)
        if "edits" in kwargs["schema"]["properties"]:
            patches += 1
            if patches == 1:
                result["response"]["edits"] = [{"old": "missing source", "new": FIXED}]
            else:
                assert "Edit 1 matches 0 times" in prompt
                assert (workspace.directory / "worktree/clamp.py").read_text() == ORIGINAL
        return result

    result, _ = improve_issue(
        client,
        workspace,
        1,
        config=ReviewConfig(backend="codex"),
        output=tmp_path / "improve",
        invoke=writer,
        progress=lambda x: None,
    )
    assert result["status"] == "pull-request", result.get("error")
    assert stages == ["tests", "patch", "patch", "review"]
    assert [a["passed"] for a in result["attempts"]] == [False, True]
    assert len([r for r in requests if r[0] == "POST"]) == 1


def test_issue_publication_is_recoverable_and_deduplicates_existing_records(native_repo, tmp_path):
    client, workspace, _ = native_repo
    data = read_metadata(client.api("GET", "/issues/1")["body"], client.repository)
    created = []
    labels = []
    original_api = client.api

    def api(method, suffix="", payload=None):
        if method == "POST" and suffix == "/labels":
            labels.append(payload)
            return payload
        if method == "POST" and suffix == "/issues":
            issue = {
                **payload,
                "labels": [{"name": label} for label in payload["labels"]],
                "number": 7,
                "html_url": client.repository.url + "/issues/7",
            }
            created.append(issue)
            return issue
        return original_api(method, suffix, payload)

    client.api = api
    client.pages = lambda suffix: iter(labels if suffix == "/labels" else created)
    manifest = {"findings": [data["finding"]], "github": {"issues": []}}
    packets = [{"task_id": "001", "target": data["target"]}]
    first = publish_findings(client, manifest, packets, workspace, output=tmp_path)
    second = publish_findings(client, manifest, packets, workspace, output=tmp_path)
    assert first == second and len(created) == 1
    assert [label["name"] for label in labels] == ["bugs"]
    assert created[0]["labels"] == [{"name": "bugs"}]
    assert (tmp_path / "issue-001-request.json").exists()
    assert json.loads((tmp_path / "review.json").read_text())["github"]["issues"][0]["number"] == 7
    with pytest.raises(ValueError, match="repository"):
        read_metadata(created[0]["body"], Repository("different", "repo"))


@pytest.mark.parametrize(
    "config",
    [
        {},
        {"setup": None, "checks": [], "test_command": []},
        {"setup": [], "checks": [["pytest"]], "test_command": ["pytest"]},
    ],
)
def test_bad_project_config_is_a_clear_error(tmp_path, config):
    (tmp_path / ".walleye.json").write_text(json.dumps(config))
    with pytest.raises(ValueError):
        project_config(tmp_path)


def test_native_packets_supply_config_and_tests_for_indirectly_tested_helpers(tmp_path):
    from walleye.project_context import native_packet

    (tmp_path / "calc.py").write_text(
        "def _internal(x):\n    if x: return x + 1\n    return 0\n"
        "def public(x):\n    return _internal(x)\n"
    )
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests/test_calc.py").write_text(
        "from calc import public\ndef test_value():\n    assert public(1) == 2\n"
    )
    (tmp_path / "pyproject.toml").write_text('[tool.pytest.ini_options]\ntestpaths = ["tests"]\n')
    _, packets, index, _ = prepare_review(tmp_path, issues=1, output=tmp_path / "review")
    packet = next(p for p in packets if p["target"]["name"] == "_internal")
    assert not any("tests/" in r["path"] for r in packet["resources"])
    native_packet(packet, index, 200000)
    paths = {r["path"] for r in packet["source"]}
    assert {"tests/test_calc.py", "pyproject.toml"} <= paths
    index.verify(packet["resources"])


def test_native_checks_cannot_stage_unrelated_files_into_the_pr(native_repo, tmp_path, monkeypatch):
    from walleye.project_tests import ProjectTests

    original_commands = ProjectTests.commands

    def commands(self, kind, label):
        results = original_commands(self, kind, label)
        if label == "001-checks":
            (self.root / "unrelated.md").write_text("Unrelated change")
            git(self.root, "add", "unrelated.md")
        return results

    monkeypatch.setattr(ProjectTests, "commands", commands)
    client, workspace, requests = native_repo
    _, invoke = model()
    result, _ = improve_issue(
        client,
        workspace,
        1,
        config=ReviewConfig(backend="codex"),
        output=tmp_path / "improve",
        invoke=invoke,
        progress=lambda x: None,
    )
    assert result["status"] == "stopped" and "outside" in result["error"]
    assert not any(method == "POST" for method, _, _ in requests)
