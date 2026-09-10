# declank

Scan any local codebase, calculate Halstead metrics, and rank the functions and
control-flow trees that deserve debugging first, using callgraph impact to
prioritize refactors. A standalone Python CLI with
no server or repository-specific setup. Scanning and review packet preparation
run locally; agent reviews use the OpenAI API or your installed Codex CLI. File reports
remain available with `--level file`.

**64 automatically detected code languages. 173 bundled Tree-sitter grammars.**
SQL uses SQLFluff's dialect-aware parser; other code uses Tree-sitter with a
bundled Babel fallback for modern JavaScript/TypeScript syntax. The scanner
reads syntax trees and never imports or executes scanned code.

## Install and run

Requires Python 3.10+. Git enables Git-aware discovery; plain folders also work.
From this checkout, use [uv](https://docs.astral.sh/uv/):

```sh
uv sync
uv run declank scan ../lakeday

# Install the CLI for use from any directory.
uv tool install .
declank scan /path/to/any/repo
```

Alternatively, `pip install .` installs the package and the `declank` command.
The package is built locally; it has not been published to PyPI.

```sh
# Show function hotspots, control-flow structure and repository scores (default).
declank scan /path/to/repo --top 30

# Inspect the function containing a source line, with caller/callee evidence.
declank scan /path/to/repo --focus src/service.ts:120

# Find up to ten supported bug findings with focused Luna/max investigations.
declank review /path/to/repo --issues 10 --budget 5

# Target behavior-preserving refactors instead.
declank review /path/to/repo --issues 10 --budget 5 --objective refactor

# Inspect bounded packets without starting agents.
declank review /path/to/repo --issues 10 --prepare

# Keep the original one-row-per-file report when that view is useful.
declank scan /path/to/repo --level file --sort bugs

# Compare within a language; difficulty emphasizes repetition and vocabulary.
declank scan /path/to/repo --language rust --sort difficulty

# Full machine-readable reports.
declank scan /path/to/repo --format json -o report.json
declank scan /path/to/repo --format csv -o report.csv

# Customize scanning; options may be repeated.
declank scan . --exclude 'legacy/**' --exclude '**/migrations/**'
declank scan . --include-tests --include-vendor --include-generated
declank scan . --map .h=cpp --map .m=objc --map .v=verilog
declank scan . --language python --language typescript

# Specify the SQL dialect when known; auto also handles SQLite without a flag.
declank scan . --language sql --sql-dialect sqlite
declank scan . --language sql --sql-dialect postgres

# CI: fail if any record exceeds a threshold, even outside --top.
declank scan . --fail-above bugs=5 --fail-above difficulty=100

# Inspect supported extensions, or every bundled grammar.
declank languages
declank languages --all --format json
```

Function reports are the default scan level. Tables default to the first 20
records; JSON and CSV default to **all** selected records. `--top 0` means all;
an explicit `--top` also limits exports. The JSON `function_hotspots` list is a
separate, risk-ranked debugging view (up to 50 rows) and is independent of
`--top` and `--sort`. Summaries, scores, directory aggregates and threshold
evaluation always cover the full scan.

Sort options: `refactor_priority` (default), `risk_score`, `bugs`,
`complexity_score`, `cyclomatic_complexity`, `max_nesting`,
`maintainability_index`, `effort`, `difficulty`, `volume`, `sloc`, `length`, and
`time_seconds`. `refactor_priority` weights risk by static dependency impact;
use `--sort risk_score` for risk alone. Risk ties are resolved by raw
cyclomatic complexity, nesting, volume, path, and line. A metric unavailable
for an unsupported adapter sorts after measured values and remains `null` in
JSON.

## Focused agent reviews

To reproduce findings, generate a test-first candidate patch, and compare measured
scores, run:

```sh
declank improve /path/to/repo --issues 1 --budget 5
# Or start with an existing finding:
declank improve path/to/review.json --issues 1 --budget 5
# Apply only after inspecting the verified candidate:
declank apply path/to/run/proposals/001
```

The workflow freezes regression/control cases before asking the agent for a fix,
checks the original and candidate, and rescans the same source cohort. Terminal
tables and `report.md` show correctness, maintainability, and dependency changes.
Acceptance requires better measured structural quality, no increase in decision
complexity or nesting (including nested helpers), and a fresh independent review
of correctness, relevance, readability, and simplicity. Up to three patch attempts
share the same frozen tests and dollar budget; rejected attempts remain visible.
Candidate source stays separate until `apply` rechecks it and updates the repository.
Executable verification currently covers isolated synchronous JS/TS/TSX functions;
unsupported cases remain unverified. See the [workflow and its gates](docs/improvement-workflow.md).
The intended hosted execution boundary is documented in the
[GitHub App sandbox design](docs/github-app-sandbox.md); the App service is not implemented yet.

`review` selects distinct functions, packages exact line-numbered source and a
local call graph, and invokes **gpt-5.6-luna with max reasoning**.
Each task has one objective and can return one finding, no finding, or a request
for specific indexed source lines. Shell, browser, connector, plugin, and
delegation tools are disabled. Review does not apply patches or run the target's tests.
Source-quote validation is not proof of a defect: findings include proposed
regression tests for subsequent verification.

`--issues 10` means **at most ten accepted findings**, with up to twenty distinct
investigations by default. Selection suppresses overlapping scopes and repeated
members of a dependency cycle, and discounts overlapping neighborhoods. Bug
and refactor objectives use different within-language/profile percentile
rankings, adjusted for dependency impact. Large functions are not penalized for
needing more context. Halstead B
is not used as a bug probability.

`--budget 5` means **a $5 budget for the whole run**, including context expansion
calls. This is also the default. Agents receive complete target functions, relevant
callers/callees, declarations, and test context. They can request more indexed source
while funds and the model context window permit; there is no default 6,000-token
packet cap or one-round expansion limit. A smaller dollar budget stops reviews
earlier without shrinking their source packets.

With `OPENAI_API_KEY` (or `CODEX_API_KEY`) configured, the runner uses the API
directly: count the complete input, reserve dollars, and bound output using the
remaining funds. Actual usage releases unused reservations. Cost includes cached
input, cache writes, and output/reasoning at the recorded model prices. An unknown
bill stops further calls. Without an API key, the existing Codex login is used;
its displayed API-equivalent cost is a **soft estimate**, not subscription billing
or a guaranteed dollar cap.

Packets and results are written under `.declank/reviews/` in the invoking
directory. `review.json` contains the queue, findings, usage, stop reason, and
scan diagnostics; `tasks/*.json` and `tasks/*.prompt.txt` contain the actual
handoffs. `--prepare` produces these artifacts without model calls. Use
`--output NEW_DIRECTORY` to choose another location. Outputs are never written
into the scanned repository unless that is also your invoking directory or you
explicitly choose a location there.

The Codex fallback was tested with CLI **0.153.4**. Older versions may not support
the required tool controls and rollout-budget settings and will fail visibly.
Configure less common limits once in `~/.config/declank/config.json` (or pass
`--config FILE`). No configuration file is required. See
[review ranking, packets, budgets, and configuration](docs/review.md).

## What the numbers mean

The default terminal output always includes maintainability and all twelve
Halstead fields for each displayed record, followed by its callgraph and branch
evidence. Terminal width no longer hides maintainability. The header includes
the running version and the number of files handled by each parser.

`Risk=100` means the capped maintenance-pressure heuristic has saturated; it
does not mean a 100% probability of a defect. `Priority` weights that risk by
dependency impact. `Cyclomatic=162` means `1 + 161` counted control/logical
decisions, and `Nesting=8` means eight nested control-flow levels. Halstead volume
is a lexical information-size estimate, not measured Shannon entropy. No bug
probability or entropy metric is computed. Maintainability uses the Visual Studio
variant documented on Radon's page; Radon's comment-adjusted MI is a different
variant. The Halstead formulas match Radon, with the counting differences below.

See [callgraph refactor ranking and parser compatibility](docs/refactor-graph.md)
for line-level output, graph coverage, the hypothetical improvement formula,
and modern TypeScript/Bash parser handling. Additional graph sort keys are
`scenario_gain`, `fan_in`, and `dependent_count`.

These are the [Halstead formulas documented by Radon](https://radon.readthedocs.io/en/latest/intro.html#halstead-metrics):

| Metric | Formula |
| --- | --- |
| Distinct operators / operands | `η₁`, `η₂` |
| Total operators / operands | `N₁`, `N₂` |
| Vocabulary | `η = η₁ + η₂` |
| Length | `N = N₁ + N₂` |
| Calculated length | `η₁ log₂ η₁ + η₂ log₂ η₂` |
| Volume | `V = N log₂ η` |
| Difficulty | `D = (η₁ / 2) × (N₂ / η₂)` |
| Effort | `E = D × V` |
| Estimated programming time (seconds) | `T = E / 18` |
| Estimated delivered bugs | `B = V / 3000` |

**B is a historical estimate, not observed bugs, a calibrated prediction, or a
probability.** Ranking by B is identical to ranking by volume, and favors large
files. The default risk ranking combines function size with control-flow
pressure; use `--sort bugs` when you specifically want the historical B order.
Use difficulty and structure hotspots to investigate dense code. T is a
theoretical estimate, not a schedule or a developer performance measure. No
defect history, test coverage, or issue tracker data is used.

The formulas match Radon. **The counts are not Radon's Python AST counts.**
Non-SQL code uses `tree-sitter-lexical-v1`, a versioned convention over Tree-sitter
concrete syntax trees:

- Ignore comment subtrees, shebangs, whitespace, and missing nodes.
- Count static strings, regexes, and character literals as single operands;
  visit interpolation expressions as code. Identifiers, types, numbers and
  other named terminal tokens are operands. Literal booleans and nulls are
  operands even when a grammar marks them anonymous.
- Anonymous syntax tokens (keywords and punctuation), explicitly recognized
  symbolic operators, and named operator tokens are operators.
- Count each opening `(`, `[` or `{` once as `()`, `[]` or `{}`. Ignore the closing
  counterpart and standalone quote/interpolation delimiters. Commas, colons,
  semicolons, declaration keywords and import syntax participate in counts.
- Distinctness uses exact token bytes within each record. It is lexical, not
  resolved symbol identity. No type checking, macro expansion or name resolution.
- Function records include their signature but exclude nested function subtrees.
  File records count each token once, including functions and top-level code.
- SLOC counts nonblank physical lines touched by non-comment syntax. A mixed
  code/comment line contributes to both SLOC and comment lines. Blank lines
  inside multiline literals do not count as SLOC.
- Define `0 log₂ 0 = 0`; set difficulty to zero when there are no operands.

SQL uses the separate `sqlfluff-lexical-v1` profile: keywords and symbols are
operators; identifiers, data types and literals are operands. Keyword operators
are normalized to uppercase, while operand text is preserved. Comments and
whitespace are ignored, quoted literals remain single operands, and bracket pairs
use the same opening-only convention. The Halstead formulas are shared. JSON and
CSV rows record the parser, profile and SQL dialect. SQL results from 0.1.0 should
not be compared directly to results from the new SQL profile.

Grammars tokenize constructs differently, so **prefer comparisons within the
same language and profile version**. Small vocabularies and declarative languages
can produce unusual scores. All 64 detected languages have a parsing/metrics
fixture; this does not imply exhaustive dialect support.

## Function debugging and repository scores

Function rows add a qualified lexical name, an inclusive source line range,
exclusive ownership metadata, and a structure view. For example, a nested
`Service.outer.inner` closure has `parent_function: Service.outer` and its own
Halstead, control-flow and line metrics. The outer row excludes the inner body;
the closure row owns that body. This lets a difficult nested closure affect the
repository aggregate exactly once while still making the ownership visible.

For Python, Rust, JavaScript, TypeScript/TSX, Go, Java, C and C++, declank uses the
explicit adapter profile `core-tree-sitter-v1`. Cyclomatic complexity is
`M = 1 + control_branch_count + logical_branch_count`: predicates and loops,
comprehension filters, catches, match guards, and non-default switch/match arms
contribute control branches;
short-circuit `and`/`or`/`&&`/`||`/`??` contribute logical branches. `try`,
`switch`, and `match` containers, default arms, and bare Rust `loop` nodes stay
in `structure_hotspots` for nesting/debugging but do not add a decision. Each
structure hotspot includes its type, line range, nesting, and branch counts in
the subtree. This is a documented adapter convention built on the standard
McCabe form, not an observed defect count.

Rows expose a standard-form maintainability index when control-flow coverage is
available:

`MI = MAX(0, (171 - 5.2 ln(V) - 0.23 M - 16.2 ln(SLOC)) * 100 / 171)`

where `V` is Halstead volume. This is the normalized formula described by
[Microsoft's Visual Studio code-metrics documentation](https://learn.microsoft.com/en-us/visualstudio/code-quality/code-metrics-maintainability-index-range-and-meaning?view=vs-2022).
Comment lines are reported separately and do not affect MI or structural quality.
Missing comments incur no numerical penalty. The independent readability review
can flag missing explanations of non-obvious behavior; comment quantity earns no bonus.
Empty units use 100. An unsupported control-flow adapter yields `null` MI and
complexity scores rather than silently assuming `M = 1`; its separately named
`halstead_risk_score` is only a size-pressure ranking aid.

The JSON `scores` object combines two transparent 0..100 components:

`overall_score = 0.65 * maintainability_index + 0.35 * complexity_score`

The maintainability component is an SLOC-weighted mean over exclusive function
units, with files that contain no recognized functions as file units. The
complexity component is an SLOC-weighted mean over those same units, using
`100 * (1 - 0.70*min((M-1)/10,1) - 0.30*min(max_nesting/5,1))`. A unit with no
trusted complexity adapter is excluded from that component and is counted in
the reported `unsupported` coverage; the overall `status` becomes `partial`
or `unsupported` accordingly. These are directional maintainability heuristics,
not calibrated bug predictions or quality guarantees. Other detected languages
continue to receive Halstead/file metrics and function names where their grammar
supports them, while their control-flow status is explicitly `unsupported`
until an adapter is validated.

`scores.risk_distribution` exposes p50, p90, p95, maximum and the count/share
above the fixed risk threshold of 70. `scores.ownership` explains how nested
functions are included without double-counting. `scores.by_language` repeats
the components and coverage for each language; grammar and style differences
make cross-language comparisons weak. The top-level `coverage` object reports
parsed files, recognized functions, nesting ownership and complexity status.

## SQL dialects

`--sql-dialect auto` (default) tries SQLite, ANSI, PostgreSQL and MySQL in that
order and accepts the first complete parse. This is parser selection, not database
detection. Explicit selection is preferable for a known database and also enables
other [SQLFluff dialects](https://docs.sqlfluff.com/en/stable/reference/dialects.html),
such as BigQuery, Snowflake and T-SQL. Invalid dialect names print the choices.

SQLite coverage includes `PRAGMA`, `CHECK`, `STRICT` tables with `ANY` columns,
triggers, upserts, parameter placeholders, transaction modes and savepoints.
A local dialect extension completes SQLite transaction/savepoint syntax missing
in SQLFluff 4.3; it does not modify SQLFluff's global dialects or rewrite input.
SQLFluff's lexer and parser run directly, with no template rendering, lint rules,
local `.sqlfluff` configuration, or inline configuration processing. SQL is never
submitted to a database. Files that cannot be parsed fully still receive no score.
SQL function-level extraction and parsing procedural bodies inside quoted strings
are not implemented; use file-level SQL reports.

## Discovery and coverage

In Git checkouts, `git ls-files` selects tracked and nonignored untracked files.
Declank reads **current working-tree contents**, including uncommitted edits.
Deleted files are skipped. Git-ignored files aren't enumerated or included in
skip totals; tracked files remain eligible even if an ignore rule matches.
Submodules are not recursively scanned; scan their directories explicitly.

Outside Git, traversal respects root and nested `.gitignore` files.
`--no-gitignore` uses filesystem traversal without ignore rules. Symlinks are not
followed. Unsupported extensions appear in the skipped summary; use `--map` to
associate extensions with any grammar from `declank languages --all`.
Ambiguous defaults include `.h=c`, `.m=matlab`, and `.v=v`.

Defaults omit:

- VCS internals, `node_modules`, Python environments and bytecode caches.
- Common vendor, build, generated, test, fixture and benchmark directories.
- Test/fixture filenames, minified filenames, generated filenames, and `.d.ts`.
- Files with a recognized generated-code comment in the first 2 KiB.
- JS/TS/TSX that looks minified: a line longer than 5,000 bytes **and** a mean
  nonblank line length above 200 bytes. Use `--include-generated` to override.
- Rust items marked `#[cfg(test)]`, `#[test]` or a namespaced test attribute.
  Other inline test conventions and compound `cfg` expressions are retained.

The corresponding `--include-*` flags undo optional exclusions, but do not undo
Git ignore rules. `--exclude` adds Git-ignore-style patterns relative to the scan
root. See `src/declank/discovery.py` for exact directory/name rules.

Source must be UTF-8, without NUL bytes, and at most 2 MiB per file by default
(`--max-bytes` changes the limit). Read failures, encoding problems, size limits
and parse errors appear in diagnostics. **Files with parse errors receive no
score.** A parser error can mean unsupported grammar/dialect syntax, not
necessarily an error in your code.

Embedded languages are not injected into raw-text regions; skipped raw text is
reported as `opaque_bytes`. Markup/config grammars are available through `--map`
but may have little meaningful Halstead interpretation. Function discovery
recognizes common grammar node kinds, including TS/JS, Python, Rust, C/C++, Java,
Go, Ruby, Kotlin and Swift. Some languages express functions as calls or other
constructs (e.g. Elixir macros) and need dedicated adapters for function reports.
File-level metrics still work. Files without recognized functions are counted
explicitly in function reports.

JSON includes dependency/profile versions, UTC time, options, a content
fingerprint, coverage and skipped counts, diagnostics, ranked records, function
hotspots, scores, threshold breaches and directory totals. `complete` means no
parse/read/size diagnostics within the selected scope; it does not mean
excluded files or embedded languages were analyzed. Directory metrics are
**sums of file record values**, not a recomputed whole-program vocabulary.
Quality aggregates use exclusive function ownership and report unowned file
scope in coverage, so function totals can differ from file totals without
double-counting nested bodies. Reports contain paths and function names, but no
source text or token vocabularies. CSV contains metric rows; diagnostics go to
stderr.
Terminal diagnostics are grouped by file (first issue plus the additional count);
JSON retains the individual diagnostics. Files are written atomically.

Exit status: **0** completed; **1** threshold exceeded; **2** invalid input,
no records, or incomplete analysis. `--allow-partial` accepts nonempty partial
scans without suppressing diagnostics. Incomplete analysis takes precedence
over threshold breaches; breaches are still included in JSON.

## Parser and development

[Tree-sitter](https://tree-sitter.github.io/tree-sitter/) is the open-source parser
foundation. [tree-sitter-language-pack 0.13.0](https://pypi.org/project/tree-sitter-language-pack/0.13.0/)
bundles all 173 grammars locally. This version is intentional: later releases use
on-demand grammar downloads. The Python binding is pinned to 0.25.2 because
0.26.0 crashed while walking the validation repository. Validate grammar and
counting-rule upgrades against the multilingual tests before changing pins.
SQL uses [SQLFluff 4.3.0](https://docs.sqlfluff.com/en/stable/reference/api.html),
also installed locally for offline scanning.

```sh
uv sync
uv run pytest
uv run ruff check src tests
uv run ruff format --check src tests
uv build
```

The package separates language detection, discovery, metric counting, scanning
and CLI presentation. `declank.scanner.scan(Path(...), ScanOptions(...))` exposes
the same data to Python callers; sorting is handled by the CLI.

Tests load every bundled grammar and exercise minimal code in all 64 detected
languages, hand-calculated formulas, comments/literals/interpolation, nested
functions, ignore rules, working-tree discovery, malformed input, JSON/CSV and
CI thresholds. Real-world validation uses `../lakeday`; its generated reports
live in the ignored `reports/` directory.
