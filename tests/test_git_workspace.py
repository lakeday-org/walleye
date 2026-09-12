from types import SimpleNamespace

import pytest

import walleye.git_workspace as git_workspace


def github_client(default_branch="main"):
    return SimpleNamespace(
        repository=SimpleNamespace(url="https://github.com/owner/repo"),
        token=lambda: "access-token",
        api=lambda method: {"default_branch": default_branch},
    )


def test_rejects_revision_expression_before_resolution(tmp_path, monkeypatch):
    calls = []

    def fake_git(directory, *args, token=None):
        calls.append((directory, args, token))
        return ""

    monkeypatch.setattr(git_workspace, "git", fake_git)

    with pytest.raises(ValueError, match=r"^Invalid Git reference$"):
        git_workspace.Workspace(
            github_client(),
            directory=tmp_path / "workspace",
            ref="main~1",
        )

    assert not any(args[0] in {"rev-parse", "checkout"} for _, args, _ in calls)


def test_valid_ref_resolves_remote_branch_tip_and_checks_out_detached(tmp_path, monkeypatch):
    calls = []
    sha = "tip-sha"

    def fake_git(directory, *args, token=None):
        calls.append((directory, args, token))
        if args[:2] == ("rev-parse", "--verify"):
            return sha
        return ""

    monkeypatch.setattr(git_workspace, "git", fake_git)

    workspace = git_workspace.Workspace(
        github_client("develop"),
        directory=tmp_path / "workspace",
        ref="main",
    )

    assert workspace.base_branch == "main"
    assert workspace.sha == sha

    resolution_calls = [call for call in calls if call[1][0] == "rev-parse"]
    assert len(resolution_calls) == 1
    assert resolution_calls[0] == (
        workspace.repo,
        ("rev-parse", "--verify", "refs/remotes/origin/main^{commit}"),
        None,
    )

    checkout_calls = [call for call in calls if call[1][0] == "checkout"]
    assert len(checkout_calls) == 1
    assert checkout_calls[0] == (
        workspace.repo,
        ("checkout", "--detach", sha),
        None,
    )
