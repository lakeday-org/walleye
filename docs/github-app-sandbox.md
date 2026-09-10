# GitHub workflows

The CLI runs inside the sandbox supplied by your platform. It clones repositories
under `.declank/jobs/` in the current directory and pins each job to a commit.
It does not start a container, host a webhook endpoint, or merge pull requests.

```sh
# Public repositories can be scanned without credentials.
declank scan lakeday-org/declank

# Review findings become GitHub issues.
declank review lakeday-org/declank --issues 2 --objective refactor --budget 5

# An issue becomes a tested pull request.
declank improve https://github.com/lakeday-org/declank/issues/42 --budget 5
# Equivalent:
declank improve lakeday-org/declank --issue 42
```

`--ref BRANCH` chooses the scan/review branch. Improvement defaults to the branch
recorded in the issue. Existing local path commands continue to work; `apply`
remains a local operation. For GitHub, `improve` publishes the accepted worktree
branch and a PR automatically. Source on the base branch remains unchanged.

## Authentication

Supply credentials through the job environment, not command arguments:

- `GITHUB_APP_ID` (or `GITHUB_APP_CLIENT_ID`)
- `GITHUB_APP_PRIVATE_KEY_PATH`: path to the mounted private-key PEM
- `GITHUB_APP_INSTALLATION_ID`: optional; otherwise resolved for the repository

The installation needs repository Contents, Issues, and Pull requests permissions
set to read/write. The client signs RS256 JWTs, requests a token scoped to the
selected repository, and refreshes it before expiration. See GitHub's
[installation-token documentation](https://docs.github.com/en/apps/creating-github-apps/authenticating-with-a-github-app/generating-an-installation-access-token-for-a-github-app).

A platform can instead supply a preissued token in `GITHUB_TOKEN` or `GH_TOKEN`.
For local development only, `DECLANK_GITHUB_AUTH=gh` explicitly uses an existing
`gh` login. This publishes as that user, not as the App. App credentials take
priority. The CLI prints which authentication mode it selected.

Git credentials are supplied through per-process configuration, never embedded
in remote URLs or saved reports. Project commands receive a limited environment
without App, GitHub, or model credential variables. Your platform must provide
filesystem/process isolation and keep credential mounts inaccessible to repository
code; environment filtering alone is not a security boundary.

## Scan and review

Scanning remains static. Review ranks distinct functions, packages indexed source,
callers/callees and related tests, and invokes the configured model for each
investigation. Agents have no repository search or publishing tools. Source quotes
are validated before findings are published.

Each issue includes the concrete problem, expected behavior or refactor benefit,
validation proposal, source links pinned to the commit, and a hidden structured
finding record. That record lets another job recover the objective and verify
that the source still matches. Ordinary issues without this record currently need
a fresh declank review. Existing matching records are reused to avoid reposting
identical findings. Publication requests and returned URLs are saved locally;
ambiguous network failures do not trigger automatic mutation retries. Serialize
jobs for a repository if multiple workers could publish the same finding.

`review --prepare` writes packets without model calls or issue creation.
Issue and PR writing instructions require plain, specific engineering prose.

## Improvement and native tests

1. Read the finding issue and check its recorded source hash.
2. Create a new branch and Git worktree from the pinned base commit.
3. Install configured dependencies and run the existing project checks.
4. Ask for a new native regression or characterization test file. Format it,
   freeze its contents, and run it on the original code. Bugs require named
   assertion failures; refactors require passing characterization tests. Collection
   failures, skipped cases, and missing JUnit output cannot establish a bug.
5. Request exact text edits to the assigned production file. Other production
   files and existing tests cannot be changed. Cohesive helpers in that file are
   allowed. Format the candidate, rerun frozen tests, and run every project check.
6. Rescan the same parsed source cohort. Require improved structural quality
   across the changed file, no worse target/repository quality, and no increase
   in decisions, nesting, or resolved cycle counts. New helper functions count.
7. Run an independent review of correctness, relevance, readability, and simplicity.
   At most three attempts share one dollar budget. Rejections and unknown usage
   stop publication; tests cannot be rewritten to make a patch pass.
8. Check the base branch has not moved, commit only the source and frozen test
   file, push the branch, and open a PR referencing the issue. Record the branch
   before the PR request so an interrupted publication remains inspectable.

An existing open declank PR for an issue prevents another improvement run from
publishing a duplicate. Completed and failed worktrees are retained as job
artifacts; the platform can remove the disposable job directory after retention.
There is no automatic merge, issue closure, or update to an existing PR.

## Project configuration

Commit `.declank.json` once per repository. Commands are argument arrays, without
shell interpolation. The test command must produce JUnit XML and include both
placeholders. For pytest in a uv project:

```json
{
  "setup": [["uv", "sync", "--frozen"]],
  "checks": [["uv", "run", "pytest", "-q"]],
  "test_command": ["uv", "run", "pytest", "-q", "{test_file}", "--junitxml={report}"],
  "format": ["uv", "run", "ruff", "format", "{file}"]
}
```

`format` is optional. A repository with `uv.lock` and `pyproject.toml` defaults to
uv/pytest commands; other ecosystems must configure their native runner. Their
JUnit assertion failures must be identifiable as assertion errors. Scanning's
language inventory does not imply every language has executable test or complexity
support. Unsupported verification stays unverified. Coverage measurement remains
an enhancement, not part of this workflow.

`workflow.json` records the base/head commits, worktree, calls, spending, attempts,
and PR URL. Each attempt retains the diff, native test/check logs, scorecard, and
independent review. Scores on PRs describe the candidate; main's score changes only
when the merged commit is rescanned. Batch transport and a hosted job queue are
not implemented.
