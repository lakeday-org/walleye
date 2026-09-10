# Focused review queue

```sh
declank review ../lakeday --issues 10 --budget 5
declank review ../lakeday --issues 10 --budget 5 --objective refactor
declank review ../lakeday --issues 10 --prepare
```

The default objective is `bug`. An issue count is an upper bound on accepted
findings, never a quota the model must fill. Review is sequential so a completed
finding or exhausted budget can stop further dispatch immediately.
`--budget 5` sets a $5 budget for the entire run, including context follow-ups.
The default is $5. Context selection is independent of this spending limit.

## Ranking: declank-review-v2

Rank functions within each language and lexical profile using midrank
percentiles. Zero-valued indicators exert zero pressure; unavailable indicators
are omitted and remaining weights are normalized. A one-function group has
neutral percentile 0.5. Every packet records raw indicators, percentiles, group
size, and the final score.

| Objective | Pressure weights |
| --- | --- |
| Bug | 50% uncapped decision count (CC − 1), 30% heaviest branch's descendant decisions, 20% nesting |
| Refactor | 40% maintainability deficit, 30% decisions, 15% Halstead difficulty, 15% cycle size |

Bug review requires a measured decision count greater than zero. Languages
without the complexity adapter are excluded from that queue, not assigned a
false low complexity. Refactor review can use lexical difficulty when structural
metrics are unavailable. These rankings do not detect simple bugs in straight-line
code and do not replace broader testing or semantic analysis.

```text
impact = 1 + min(4, log2(1 + transitive_dependents)) / 4
refactor impact also adds min(3, log2(max(1, cycle_size))) / 3
priority = 100 * pressure * impact * novelty
```

Novelty is `1 − 0.75 * greatest Jaccard overlap` between the candidate's closed
one-hop neighborhood and those of already-selected targets. Overlapping source
scopes and another member of an already-selected strongly connected component
are suppressed entirely for this run. Ties use path, line, and symbol ID for
deterministic selection. Selection favors distinct investigations. A large context
requirement no longer reduces priority; spending controls dispatch, not importance.
Scores are heuristics, not bug probabilities, severities, measured improvement,
or predictions of real refactor cost. The scan's existing 25% improvement scenario
is not presented as a measured refactor gain.

## Context and retrieval

Review captures the successfully parsed bytes during the scan. Each packet has:

- One function target, objective, branch locations, and selection rationale.
- Complete target source with inclusive line ranges, exact text, and source SHA-256.
- Local resolved call sites and their endpoint metadata; relevant complete
  caller and callee functions when they fit in the context window.
- Matching imports and lexically relevant declaration candidates, enclosing
  function/guard context where available, and candidate test files.
- A bounded resource catalog for indexed expansion, unresolved call samples,
  omission counts, and explicit context gaps.

The target is supplied whole. A target that cannot fit is skipped with a recorded
reason rather than dispatched as fragments. Related bodies are included whole
when possible; fallback signatures and call-site windows are marked incomplete
and can be expanded. The default input window is 200,000 estimated tokens, with
an allowance for instructions. This is a context capacity setting, not a spending
budget or a target amount to fill. Estimates use `ceil(UTF-8 bytes / 3)`; API
dispatch additionally counts the complete prompt with the model tokenizer.
No source is silently truncated to fit the remaining dollars.

Tests are excluded from rankings but indexed once for context. AST identifier
matches supply **test candidates**, not proven test-to-function linkage or test
coverage. All eligible tests are indexed, subject to normal per-file discovery
limits; failures are recorded. Dynamic dispatch, package aliases, type resolution, macros, and external
contracts may remain unknown. Referenced declaration candidates are not type
checker results.

The agent cannot perform repository search. `needs_context` requests specify a
resource ID already in the packet and an inclusive line range. The coordinator
serves those bytes from the snapshot, up to three requests per round, then starts
another call with the original packet and **all** previously requested source.
There is no default line cap, expansion token cap, or fixed round limit. Requests
must add new context. Unknown resources and invalid ranges are rejected. Exhausted
funds or a full context window leave the investigation unresolved rather than
removing earlier source. Each follow-up's complete input and output is charged.

Before dispatch and before accepting a response, all catalogued source files are
checked against their snapshot hashes. Changed, missing, or redirected source
requires a rescan. Full repository graphs and full file contents are not included
in every agent prompt.

## Configuration and budgets

Optional `~/.config/declank/config.json` (respects `XDG_CONFIG_HOME`):

```json
{
  "model": "gpt-5.6-luna",
  "reasoning_effort": "max",
  "backend": "auto",
  "budget_usd": 5,
  "codex": "codex",
  "context_tokens": 200000,
  "tasks_per_issue": 2,
  "max_output_tokens": 128000,
  "timeout_seconds": 600
}
```

The accepted finding limit and investigation limit are hard coordinator limits.
Each expansion counts as another model call and is included in usage accounting.
No hidden retries are made after runner errors. `no_finding` consumes an
investigation but not an issue slot. Response-shape failures, mismatched quotes,
missing objective-specific evidence, and duplicate findings are recorded rather
than added to the accepted issue count.

`--budget` overrides `budget_usd`. Legacy `total_tokens`, `per_call_tokens`,
`expansion_tokens`, and `max_expansions` are optional compatibility controls whose
defaults are now `null` (unlimited). Existing configuration files that set them
still apply; remove those keys to use dollar budgeting and unrestricted expansion.

With `backend: "auto"`, `OPENAI_API_KEY` or `CODEX_API_KEY` selects the direct API;
otherwise the installed Codex CLI uses its existing login. `backend: "api"` can
require API billing explicitly and fails if neither key is available. Credentials
are read from the environment and are never saved in review artifacts.

API execution counts the complete input using
[`POST /responses/input_tokens`](https://developers.openai.com/api/reference/typescript/resources/responses/subresources/input_tokens/methods/count),
reserves input at the maximum applicable cache-write rate, and sets
`max_output_tokens` to what remains affordable, up to the model output ceiling.
That output bound includes reasoning. Requests use standard service pricing,
no tools, no automatic retries, and disabled input truncation. Less than 1,024
affordable output tokens stops dispatch. The reservation is saved before the paid
request, then replaced with cost from its usage report. Cached input and cache
writes are charged separately; reasoning is already included in output usage.
Missing usage or timeout stops the run and retains the uncertain reservation.

The built-in price card for `gpt-5.6-luna`, checked **2026-09-09**, uses these
[standard API prices](https://developers.openai.com/api/docs/pricing), in USD per
million tokens:

| Input | Cached input | Cache write | Output (including reasoning) |
| --- | --- | --- | --- |
| $0.20 | $0.02 | $0.25 | $1.20 |

For input above 272,000 tokens, input rates double and output rates multiply by
1.5, following the [model's long-context pricing](https://developers.openai.com/api/docs/models/gpt-5.6-luna).
The spending bound is at this recorded price card; account discounts, taxes,
future price changes, and other processes using the API key are outside it.
Review does not change an account-wide billing limit. A different model requires
an explicit `pricing` object with all keys from `LUNA_PRICING` in
[`review_cost.py`](../src/declank/review_cost.py); there is no silent substitution.

The Codex backend uses [non-interactive mode](https://developers.openai.com/codex/noninteractive)
with structured output and JSON usage events, verified against CLI 0.153.4.
Its API-equivalent dollar estimate uses the same price card, but token allowance
enforcement is soft and usage arrives after the turn. It can overshoot, and it
does not represent subscription billing. Use the API backend for preflight dollar
reservations and a response output bound. The Codex runner ignores user
configuration that could inject tools, hooks, or unrelated context. Runs use a
temporary working directory and read-only sandbox with shell, browser, image,
apps, plugins, and delegation features disabled. Scanned code is data; it is
never executed as part of review. The tool neither installs Codex nor logs in
automatically, and never silently substitutes another model.

## Findings and artifacts

`review.json` records configuration, the ranked queue, source fingerprint, scan
coverage/diagnostics, every investigation result, accepted findings, measured
usage, recorded pricing, spent/reserved dollars, cost uncertainty, and the stop
reason. Writes are atomic and checkpointed before dispatch and after each model
call. Each `tasks/NNN.json` has a corresponding `.prompt.txt`; expansion excerpts
and the response schema are saved alongside them. A prepared run has zero model
calls and contains no claimed findings. Preparation is an inspectable handoff;
there is currently no saved-run resume command.

Bug findings require a reachable trigger, expected and actual behavior, source
evidence, and a proposed regression test. Refactor findings require a cohesive
change, preserved behavior, expected benefit, evidence, and validation. Source
quotes must match supplied lines, with evidence inside the assigned target.
Duplicates are detected using normalized root-cause text or overlapping evidence
locations. This is conservative deduplication, not semantic equivalence checking.
Accepted findings are model recommendations with checked source references;
their proposed tests have not been run and patches have not been applied.

Exit 0 means preparation/review finished normally, including stopping at an
issue or dollar budget with fewer findings than requested. Exit 2 means invalid
configuration, missing/unsupported Codex controls, incomplete source scanning,
no eligible packets, stale source, or runner failure. Partial artifacts are
retained and the exact stop reason is printed.
