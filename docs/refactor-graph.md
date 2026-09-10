# Callgraph refactor ranking

The default command shows exact function line ranges and a breakdown for every
displayed function: caller/callee locations and call-site lines, the heaviest
control-flow branches with line ranges, direct and transitive dependents, cycles,
and suggested refactor targets. The terminal shows up to three call sites per
direction and three branch hotspots per function; JSON contains the full resolved
call graph and every unresolved call site. `--focus PATH:LINE` narrows selected
records to scopes containing that line; repository scores and graph context stay
available. File rankings remain available with `--level file`.

`callgraph` contains separate directed function-call and module-dependency graphs.
Edges point from caller to callee, or importer to dependency. NetworkX computes
[strongly connected components](https://networkx.org/documentation/stable/reference/algorithms/generated/networkx.algorithms.components.strongly_connected_components.html)
for cycles and [betweenness centrality](https://networkx.org/documentation/stable/reference/algorithms/generated/networkx.algorithms.centrality.betweenness_centrality.html)
for nodes that connect dependency paths. Transitive dependent counts use exact
reachability on the condensed graph. Betweenness uses at most 64 deterministic
samples to keep larger scans practical. Recursion is reported separately from
multi-function cycles; a cycle is evidence to investigate, not automatically a bug.

The versioned `declank-refactor-graph-v1` scenario is explicit:

```text
weight = 1 + log2(1 + transitive_dependents)
         + 2 * (betweenness / maximum_betweenness) + log2(SCC_size)
priority = risk_score * weight
graph_quality = 100 - sum(weight * risk_score) / sum(weight)
scenario_gain = 0.25 * risk_score * weight / sum(weight)
```

The betweenness term is zero when all betweenness values are zero. Isolated
nodes have weight 1. **Gain assumes one candidate's risk falls by 25%, with
the graph and all other risk values fixed.** It measures a hypothetical change
in the displayed graph-weighted quality baseline, separately from the existing
MI/control-flow overall score. File and function scenarios have separate
denominators. This is a prioritization heuristic, not a prediction of actual
bugs fixed, a refactor cost estimate, or proof a suggested extraction is valid.
Review the candidate and rescan an actual patch to measure changes.

Call extraction has tested adapters for Python, JavaScript, TypeScript, TSX,
Rust, Go, C, C++ and Java. Cross-file resolution covers relative JS/TS imports,
Python modules and import aliases, and local Rust module paths and grouped
imports. Bare names resolve in lexical scope; arbitrary `object.method()` calls
are never connected to unrelated methods just because their names match.
Dynamic dispatch, callbacks, package/tsconfig aliases, re-export chains, compiler
macros and ambiguous symbols remain unresolved. Callgraph coverage is narrower
than the 64-language metrics coverage and is reported explicitly; unsupported
graph metrics are `null`. Filters and parse exclusions also limit graph reach.

`declank review` builds bounded packets for the Codex Luna/max review stage.
Use `review --prepare` to inspect handoffs without invoking a model. The full
scan JSON remains an analysis export, not an efficient per-agent prompt.
Scanning stays local; live review invokes Codex and reports findings without
applying fixes. See [review queue and budgets](review.md).

## Parser compatibility

If Tree-sitter rejects JS/TS/TSX, the scanner tries the bundled
[@babel/parser](https://babeljs.io/docs/babel-parser) 7.28.4 inside QuickJS. It
uses a complete AST plus parser tokens with UTF-16 offsets translated back to
UTF-8 source positions. This covers type-only star exports, generic import types,
generic call signatures, raw ampersands in JSX text, and reserved import aliases.
Babel results use `babel-lexical-v1`; compare profiles separately. Error recovery
is disabled: malformed code still receives no score. No Node.js installation,
project Babel configuration or runtime downloads are involved.

For two known Bash grammar defects (a here-string following another redirect,
and bracket trimming in parameter expansions), a compatibility adapter first
validates the untouched source using installed Bash in parse-only mode. It then
uses position-preserving grammar substitutions internally and restores original
token text, including `<<<`. These rows use `tree-sitter-bash-compat-v1` and
`tree-sitter+bash-validation`. Scripts, startup files and scanned commands are
never executed or rewritten. If Bash is unavailable or validation fails, the
normal parse diagnostics remain. JSON rows and tool metadata identify every
parser/profile used.
