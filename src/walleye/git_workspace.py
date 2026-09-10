"""Pinned clones and worktrees in the caller's sandbox; no credentials in remotes."""

import base64
import os
import subprocess
from pathlib import Path
from uuid import uuid4


def git(directory, *args, token=None):
    env = {k: v for k, v in os.environ.items() if not k.startswith(("GIT_", "GITHUB_", "GH_"))}
    env.update(GIT_TERMINAL_PROMPT="0", GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull)
    if token:
        encoded = base64.b64encode(("x-access-token:" + token).encode()).decode()
        env.update(
            GIT_CONFIG_COUNT="1",
            GIT_CONFIG_KEY_0="http.https://github.com/.extraheader",
            GIT_CONFIG_VALUE_0="AUTHORIZATION: basic " + encoded,
        )
    command = ["git", "-c", "core.hooksPath=" + os.devnull, "-c", "credential.helper=", *args]
    try:
        result = subprocess.run(
            command, cwd=directory, env=env, capture_output=True, text=True, timeout=300
        )
    except subprocess.TimeoutExpired:
        raise ValueError(
            f"git {args[0]} timed out; inspect the saved workspace before retrying"
        ) from None
    if result.returncode:
        # Git stderr may echo authentication material; do not persist it.
        raise ValueError(f"git {args[0]} failed (exit {result.returncode})")
    return result.stdout.strip()


class Workspace:
    def __init__(self, github, directory=None, ref=None):
        self.github = github
        self.directory = (directory or Path.cwd() / ".walleye/jobs" / uuid4().hex).resolve()
        self.directory.mkdir(parents=True, exist_ok=False)
        self.repo = self.directory / "repo"
        info = github.api("GET")
        self.base_branch = ref or info["default_branch"]
        if not self.base_branch or self.base_branch.startswith("-"):
            raise ValueError("Invalid Git reference")
        git(
            self.directory,
            "clone",
            "--no-checkout",
            "--no-tags",
            github.repository.url + ".git",
            str(self.repo),
            token=github.token(),
        )
        # Resolve against the remote ref explicitly: a branch must not be confused with a path.
        self.sha = git(
            self.repo, "rev-parse", "--verify", f"refs/remotes/origin/{self.base_branch}^{{commit}}"
        )
        git(self.repo, "checkout", "--detach", self.sha)

    def worktree(self, issue):
        branch = f"walleye/issue-{issue}-{self.sha[:8]}-{uuid4().hex[:6]}"
        directory = self.directory / "worktree"
        git(self.repo, "worktree", "add", "-b", branch, str(directory), self.sha)
        return directory, branch

    def publish(self, directory, branch, paths, message):
        identity = self.github.commit_identity()
        git(directory, "add", "--", *paths)
        git(
            directory,
            "-c",
            "user.name=" + identity["name"],
            "-c",
            "user.email=" + identity["email"],
            "commit",
            "-m",
            message,
            "--only",
            "--",
            *paths,
        )
        commit = git(directory, "rev-parse", "HEAD")
        git(
            directory,
            "push",
            "origin",
            f"HEAD:refs/heads/{branch}",
            token=self.github.token(required=True),
        )
        return commit
