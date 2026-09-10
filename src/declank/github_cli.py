"""Remote command dispatch; local commands retain their existing behavior."""

import sys
from dataclasses import replace
from pathlib import Path

from .git_workspace import Workspace
from .github import GitHub, repository_input
from .github_publication import publish_findings, read_metadata
from .github_workflow import improve_issue
from .review import load_config, write_json


def prepare_remote(args):
    if args.command not in {"scan", "review", "improve"}:
        return None
    repository = repository_input(args.path)
    if not repository:
        if getattr(args, "ref", None) or getattr(args, "issue", None):
            raise ValueError("--ref and --issue require a GitHub repository input")
        args.path = Path(args.path)
        return None
    github = GitHub(repository)
    ref = getattr(args, "ref", None)
    if args.command == "improve":
        number = repository.issue or args.issue
        if not number:
            raise ValueError("Use a GitHub issue URL or OWNER/REPO --issue NUMBER")
        args.issue = number
        metadata = read_metadata(github.api("GET", f"/issues/{number}").get("body"), repository)
        ref = ref or metadata["base_branch"]
    elif repository.issue:
        raise ValueError("scan and review take a repository, not an issue URL")
    if args.command == "improve" or args.command == "review" and not args.prepare:
        github.token(required=True)
    print(
        f"GitHub: {repository.full_name} · auth={github.auth_kind} · cloning into sandbox",
        file=sys.stderr,
    )
    workspace = Workspace(github, ref=ref)
    args.path = workspace.repo
    return github, workspace


def publish_review(remote, manifest, packets, output, *, prepared):
    github, workspace = remote
    manifest["github"] = {
        "repository": github.repository.full_name,
        "commit": workspace.sha,
        "branch": workspace.base_branch,
        "auth": github.auth_kind,
        "issues": [],
    }
    write_json(output / "review.json", manifest)
    if not prepared:
        manifest["github"]["issues"] = publish_findings(
            github, manifest, packets, workspace, output=output
        )
        write_json(output / "review.json", manifest)
        for issue in manifest["github"]["issues"]:
            print(f"Issue: {issue['url']}")


def run_improve(args, github, workspace):
    config = load_config(args.config)
    if args.budget is not None:
        config = replace(config, budget_usd=args.budget)
    manifest, output = improve_issue(
        github, workspace, args.issue, config=config, output=args.output
    )
    print(f"Status: {manifest['status']}")
    if manifest.get("pull_request"):
        print(f"Pull request: {manifest['pull_request']['url']}")
    if manifest.get("error"):
        print(manifest["error"], file=sys.stderr)
    print(f"Workflow: {output / 'workflow.json'}")
    print(
        f"{manifest['usage']['calls']} model calls; reported API-equivalent cost "
        f"${manifest['cost']['spent_usd']:.6f}"
    )
    if manifest["cost"]["unknown"]:
        print("Total cost is unknown; generation stopped.")
    return 0 if manifest["status"] == "pull-request" else 2
