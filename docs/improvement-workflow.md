# Verified improvement workflow

```sh
# Scan, investigate, reproduce, propose, verify, and measure in one run.
declank improve ../lakeday --issues 1 --budget 5

# Revisit an existing finding using a fresh baseline and new test/patch agent calls.
declank improve path/to/review.json --issues 1 --budget 5

# After inspecting a verified candidate, reverify, apply, and rescan.
declank apply path/to/run/proposals/001
```

`improve` uses the configured model/backend (default Luna/max, existing Codex
login when no API key is set). Discovery, test planning, patch generation, independent review, and
context follow-ups share one dollar budget. Reusing a saved review starts a new
budget and records the historical review path; its original model calls are not
charged again. API output is bounded by reserved dollars; Codex accounting is a
soft API-equivalent estimate. Batch submission is not implemented.

The initial scan is recorded in `baseline.json`. Existing findings must match
their recorded source hashes, and their source quotes are checked again against
fresh packets. A changed finding must be reviewed again. Preparation includes
complete targets and indexed caller/callee context, with no repository-search
tools available to the agents.

## Test-first gates

1. The first agent produces **tests only**: JSON arguments, expected outcomes,
   and an explanation of the intended contract for each case.
2. The coordinator validates and hashes that test plan, then runs it against
   the original source. Bug regression cases must fail and normal-behavior
   controls must pass. An unavailable dependency or runtime error in the harness
   is not accepted as evidence of a defect. Refactors instead require passing
   characterization tests on the original.
3. Only after that gate does a separate agent call produce replacement source.
   It can replace the assigned function while preserving its signature; imports,
   other top-level declarations, other files, and test edits are excluded.
4. The coordinator runs the **same frozen tests**, each in a fresh runtime,
   against the candidate. Every case must pass. A failing candidate is retained
   for inspection without improvement credit; tests are never rewritten to pass.
5. The candidate is rescanned in a temporary snapshot containing exactly the
   baseline's successfully parsed source files. Source cohort and scoring/parser
   profiles must match. Baseline exclusions and unresolved graph coverage remain
   visible. The original repository is unchanged.
6. The measured gate requires increased structural quality across the entire changed
   function, including nested functions, with no decrease in target or repository
   quality and no increase in decisions, nesting, or resolved cycle counts. Region
   decisions sum `cyclomatic - 1` across exclusive function scopes; extracting a
   closure cannot hide added branches. MI and every component remain visible;
   individual components can trade off within the structural quality formula.
7. A fresh model call independently assesses correctness, relevance, readability,
   and simplicity. All four require an explicit, explained pass. It receives the
   actual patch, source packet, frozen cases, test results, and scorecard, without
   the writer's rationale or previous critiques. Passing tests or smaller metrics
   alone cannot approve unreadable code.
8. A failed candidate is returned to the writer with concrete feedback. There are
   at most three distinct patch attempts, using the same tests and run budget.
   Repeated candidates stop early; unknown spending stops further calls. No
   accepted candidate means zero resolution credit and no apply instruction.

Agents can request additional indexed source during any stage. Every request
must add information, earlier context is retained, and each call consumes the
same run budget. Unknown spending stops further generation.

## Verification coverage

The first executable adapter supports synchronous, named top-level functions
in JavaScript, TypeScript, and TSX whose local dependencies can run as plain
JavaScript. It extracts the actual function and referenced local declarations;
it does not ask the agent to reimplement the function for testing.

Execution uses QuickJS with time, memory, and stack limits and no host callbacks,
filesystem, network, environment, or process access. TypeScript is stripped using
Node's built-in [`stripTypeScriptTypes`](https://nodejs.org/api/module.html#modulestriptypescripttypescode-options)
(Node 22.13+); project modules and build
configuration are not executed. JSON values, `undefined`, and named thrown
errors are supported. DOM/React rendering, external imports, asynchronous calls,
class instances, and other languages currently remain explicitly **unverified**.
Scanning still supports the existing language inventory.

These are isolated function checks, not an integration-test suite or proof of
equivalence for all inputs. Contracts and tests still need review. Failed controls,
unreproduced reports, missing context, unsupported execution, syntax errors, and
failed candidates retain their exact status and logs. They do not count as fixes.

## Three dimensions and actual changes

The versioned `declank-health-v1` scorecard separates:

- **Maintainability:** measured structural quality, MI, control complexity,
  nesting, SLOC, and Halstead volume. Existing overall structural scores keep the
  same formula. Target, changed region, and repository changes are shown separately.
  A test-passing patch with worse structural quality is rejected, not awarded
  improvement credit.
- **Correctness:** confirmed open findings, verified candidate resolutions,
  applied resolutions, and frozen-test results. One root cause remains one
  finding regardless of the number of test variants. The scope is this run's
  investigated findings; all other source stays unknown. There is no invented
  correctness percentage or repository-wide claim of zero bugs.
- **Architecture:** resolved function/module cycles, dependency edges, and
  call-resolution coverage. Cycle health is `100 * (1 - cyclic_functions /
  graph_functions)` for multi-function cycles. It is explicitly a narrow cycle
  signal, not an architecture grade. Layer rules are unassessed, unresolved
  calls remain unknown, and high centrality alone is not a defect.

Rank percentiles and the old hypothetical 25% refactor scenario are not used
as measured improvement. Different source cohorts or changed scoring profiles
cannot produce an accepted comparison. A candidate score is always a preview;
the repository score changes only after application and a fresh scan.

## Artifacts and application

`workflow.json` records stages, spending, proposal statuses, and the confirmed
finding ledger. `report.md` and terminal tables summarize before/after changes.
Each `proposals/NNN/` directory contains:

- `proposal.json`: state transitions, source hashes, and assurance limitations.
- Agent prompts, schemas, responses, and additional requested context.
- `tests.json` and its hash, original results, and candidate verification results.
- `before.source`, `candidate.source`, executable bundles, and `patch.diff`.
- `scorecard.json`: measured local/repository changes with graph coverage.
- `acceptance.json`: measured gates and independent review, bound to the exact
  candidate, frozen tests, and baseline fingerprint.
- `attempts/NNN/`: each evaluated candidate with its tests, metrics, and rejection
  or acceptance evidence. A rejected candidate remains measured, never verified.

`apply` accepts only verified candidates under `declank-improve-v2` and the current
acceptance policy. Old v1 proposals must be regenerated. It checks all captured source hashes,
rejects symlinks and path escapes, checks frozen tests and candidate integrity,
reruns original/candidate checks and measured gates, verifies the bound independent
review, and confirms the repository cohort has not
changed. It then atomically replaces the one source file while preserving its
permissions. Regression specifications are saved under `.declank/regressions/`;
they are data-driven cases and are not automatically integrated into the project's
native test runner. The fresh scan and actual scorecard are saved alongside the
proposal, and its state changes to `applied`.

Candidates in a multi-finding run each compare independently with the same
baseline. Applying one can invalidate the others' snapshots; rerun them against
the updated code rather than applying stale assumptions. No commit, merge,
deployment, or automatic rollback is performed.

Exit 0 means a complete scan produced a verified candidate, or application
succeeded. Exit 2 retains partial/unverified results, including a useful verified
candidate from an otherwise incomplete source scan.
