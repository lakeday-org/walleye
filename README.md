# walleye

[![Build](https://github.com/lakeday-org/walleye/actions/workflows/tests.yml/badge.svg?branch=main)](https://github.com/lakeday-org/walleye/actions/workflows/tests.yml)
[![MIT](https://img.shields.io/badge/license-MIT-38b2ac)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.10%2B-3776ab)](https://www.python.org/downloads/)
[![Languages](https://img.shields.io/badge/languages-64-0e7490)](#commands)

Code quality scans and tested fixes for local and GitHub repositories. Supports 64 languages.

- Rank functions by maintainability, complexity, and call graph impact.
- Identify potential bugs and refactors with focused agent reviews.
- Write tests, verify fixes, and open pull requests with before/after scores.

## Install

Python 3.10+ and [uv](https://docs.astral.sh/uv/).

```sh
uv tool install git+https://github.com/lakeday-org/walleye.git
```

## AI access

For reviews and fixes, set an [OpenAI API key](https://developers.openai.com/api/docs/quickstart):

```sh
export OPENAI_API_KEY="your-api-key"
```

Or, with no API key set, sign in using an installed [Codex CLI](https://developers.openai.com/codex/auth):

```sh
codex login
```

Scans do not need AI access.

## Commands

```sh
walleye scan .
walleye scan owner/repo
walleye review owner/repo --issues 5 --budget 5
walleye improve https://github.com/owner/repo/issues/42
```

| Command | Result |
| --- | --- |
| `scan` | Scores, ranked functions, source lines, and call graphs. |
| `review` | Findings filed as GitHub issues. Local reviews save findings to disk. |
| `improve` | A worktree, tests, a fix, a rescan, and a PR. Requires passing tests and independent review. No automatic merge. |

`--issues 5` limits findings. `--budget 5` sets the run's dollar budget.
Codex spending is an API cost estimate, not a hard cap or subscription charge.

Architecture changes must improve quality. Bug fixes may keep quality flat, allowing up to a 0.01-point decrease on the 0–100 scale. Complexity, nesting, and dependency cycles cannot increase.

Fixes reuse frozen tests and revise the best patch until accepted, out of budget, or three revisions make no progress.

## GitHub App

Set `GITHUB_APP_ID` and `GITHUB_APP_PRIVATE_KEY_PATH` in the job environment.
Install the App with read/write access to Contents, Issues, and Pull requests.
Public scans need no credentials.

Run jobs inside your sandbox. Clones, worktrees, and results stay under `.walleye/`.
Python projects using uv and pytest work automatically; other test runners need a `.walleye.json` configuration.

## Scores

Quality and maintainability: **0–100, higher is better**. Risk: **higher means inspect first**.
Rankings also account for callers, dependents, and dependency cycles.

`n1`, `n2`: distinct operators and operands. `N1`, `N2`: total counts.
`L`: source lines. `M`: cyclomatic complexity. `d`: maximum nesting depth.

| Metric | Formula |
| --- | --- |
| Vocabulary | `n = n1 + n2` |
| Length | `N = N1 + N2` |
| Calculated length | `n1 log2(n1) + n2 log2(n2)` |
| Volume | `V = N log2(n)` |
| Difficulty | `D = (n1 / 2) × (N2 / n2)` |
| Effort | `E = D × V` |
| Estimated time | `T = E / 18` seconds |
| Historical bug estimate | `B = V / 3000` |
| Cyclomatic complexity | `M = 1 + decisions` |
| Maintainability | `MI = max(0, (171 − 5.2 ln(V) − 0.23 M − 16.2 ln(L)) × 100 / 171)` |
| Control score | `C = 100 × (1 − 0.7 min((M−1)/10, 1) − 0.3 min(d/5, 1))` |
| Quality | `Q = 0.65 MI + 0.35 C` |
| Risk | `100 − Q` |

Repository scores weight measured functions by source lines. Comments do not affect the score.
Fix comparisons hold the original weights fixed and include new helpers; raw scan scores are also saved.
Bug estimates and risk scores are not bug probabilities. Actual improvements are measured after tests and a rescan.

[MIT License](LICENSE)
