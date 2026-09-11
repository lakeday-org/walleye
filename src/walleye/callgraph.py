"""Conservative static call graphs and inspectable refactor impact scenarios.

Edges point from caller to callee. Dynamic dispatch is left unresolved, never
connected by a repository-wide guess based on a common method name.
"""

import ast
import posixpath
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from math import log2

import networkx as nx

from .metrics import is_comment, is_function, walk

PROFILE = "declank-refactor-graph-v1"
CALL_LANGUAGES = frozenset(
    {"python", "javascript", "typescript", "tsx", "rust", "go", "c", "cpp", "java"}
)
IMPORT_LANGUAGES = frozenset({"python", "javascript", "typescript", "tsx", "rust"})
CALL_TYPES = frozenset({"call", "call_expression", "method_invocation"})


@dataclass
class Facts:
    path: str
    language: str
    symbols: list[dict] = field(default_factory=list)
    calls: list[dict] = field(default_factory=list)
    imports: list[dict] = field(default_factory=list)
    shadowed: dict[str, set[str]] = field(default_factory=dict)


def _text(node) -> str:
    return node.text.decode("utf-8") if node is not None else ""


def _use_names(node, prefix=""):
    """Expand Rust's grouped use trees without interpreting source with regexes."""
    if node.type == "scoped_use_list":
        path = _text(node.child_by_field_name("path"))
        yield from _use_names(node.child_by_field_name("list"), prefix + path + "::")
    elif node.type == "use_list":
        for child in node.named_children:
            yield from _use_names(child, prefix)
    elif node.type == "use_as_clause":
        path = prefix + _text(node.child_by_field_name("path"))
        yield path.removesuffix("::self"), _text(node.child_by_field_name("alias"))
    elif node.type != "use_wildcard":
        path = (prefix + _text(node)).removesuffix("::self")
        yield path, path.rsplit("::", 1)[-1]


def _python_imports(source: bytes) -> list[dict]:
    try:
        module = ast.parse(source)
    except SyntaxError:
        return []  # Python newer than this interpreter can still use Tree-sitter.
    imports = []
    for node in module.body:
        if isinstance(node, ast.Import):
            for alias in node.names:
                imports.append(
                    dict(
                        module=alias.name,
                        name=None,
                        alias=alias.asname or alias.name,
                        line=node.lineno,
                    )
                )
        elif isinstance(node, ast.ImportFrom):
            for alias in node.names:
                imports.append(
                    dict(
                        module="." * node.level + (node.module or ""),
                        name=alias.name,
                        alias=alias.asname or alias.name,
                        line=node.lineno,
                    )
                )
    return imports


def _tree_sitter_imports(root) -> list[dict]:
    imports = []
    for node in root.named_children:
        if node.type in {"import_statement", "export_statement"}:
            source_node = node.child_by_field_name("source")
            if source_node is None:
                continue
            module = _text(source_node)[1:-1]
            imports.append(
                dict(module=module, name=None, alias=None, line=node.start_point.row + 1)
            )
            if node.type == "export_statement":
                # Re-exports contribute module edges; calls aren't guessed through barrels.
                continue
            for item in walk(node):
                name = alias = None
                if item.type == "import_specifier":
                    name = _text(item.child_by_field_name("name"))
                    alias = _text(item.child_by_field_name("alias")) or name
                elif item.type == "namespace_import":
                    alias = _text(item.named_children[-1])
                elif item.type == "identifier" and item.parent.type == "import_clause":
                    alias, name = _text(item), "default"
                if alias:
                    imports.append(
                        dict(module=module, name=name, alias=alias, line=item.start_point.row + 1)
                    )
        elif node.type == "use_declaration":
            argument = node.child_by_field_name("argument")
            if argument is not None:
                for name, alias in _use_names(argument):
                    module, _, symbol = name.rpartition("::")
                    imports.append(
                        dict(
                            module=module or name,
                            name=symbol if module else None,
                            alias=alias,
                            line=node.start_point.row + 1,
                        )
                    )
        elif node.type == "mod_item" and node.child_by_field_name("body") is None:
            imports.append(
                dict(
                    module="self::" + _text(node.child_by_field_name("name")),
                    name=None,
                    alias=None,
                    line=node.start_point.row + 1,
                )
            )
    return imports


def _imports(root, source: bytes, language: str) -> list[dict]:
    if language == "python":
        return _python_imports(source)
    return _tree_sitter_imports(root)


def collect_facts(root, source, path, language, nodes, records, excluded) -> Facts:
    facts = Facts(path, language)
    if language not in CALL_LANGUAGES:
        return facts
    owners = {}
    for node, row in zip(nodes, records, strict=True):
        row["graph_id"] = f"function:{path}:{node.start_byte}"
        owners[node.id] = row
        facts.symbols.append(row)
        parameters = node.child_by_field_name("parameters")
        facts.shadowed[row["graph_id"]] = (
            {_text(n) for n in walk(parameters) if n.type == "identifier"} if parameters else set()
        )
    facts.imports = _imports(root, source, language)
    stack = [(root, None)]
    while stack:
        node, owner = stack.pop()
        if (
            node.id in excluded
            or is_comment(node)
            or node.type
            in {
                "type_alias_declaration",
                "interface_declaration",
                "type_annotation",
                "type_arguments",
            }
            or node.type.startswith("t_s_")
        ):
            continue
        owner = owners.get(node.id, owner)
        if node.type in CALL_TYPES:
            callee = node.child_by_field_name("function")
            if node.type == "method_invocation":
                obj = _text(node.child_by_field_name("object"))
                reference = (obj + "." if obj else "") + _text(node.child_by_field_name("name"))
            else:
                while callee is not None and callee.type in {
                    "generic_function",
                    "instantiation_expression",
                }:
                    callee = callee.child_by_field_name("function")
                reference = _text(callee)
            normalized = reference.replace("::", ".")
            if len(reference) > 256 or not all(
                part.isidentifier() for part in normalized.split(".")
            ):
                # Reports contain identifiers and locations, never an inline function body
                # or string arguments from a dynamically computed callee.
                reference = "<dynamic>"
            facts.calls.append(
                {
                    "source": owner["graph_id"] if owner else f"file:{path}",
                    "scope": owner["qualified_name"] if owner else "",
                    "reference": reference,
                    "path": path,
                    "line": node.start_point.row + 1,
                    "column": node.start_point.column + 1,
                }
            )
        if owner and node.type in {"assignment", "variable_declarator", "let_declaration"}:
            left = (
                node.child_by_field_name("left")
                or node.child_by_field_name("name")
                or node.child_by_field_name("pattern")
            )
            value = node.child_by_field_name("value") or node.child_by_field_name("right")
            if (
                left is not None
                and left.type == "identifier"
                and not (value and is_function(value))
            ):
                facts.shadowed[owner["graph_id"]].add(_text(left))
        stack.extend((child, owner) for child in reversed(node.named_children))
    return facts


class Resolver:
    def __init__(self, facts: list[Facts]):
        self.paths = {f.path for f in facts}
        self.symbols = defaultdict(list)
        self.parents = {}
        for f in facts:
            for row in f.symbols:
                self.symbols[f.path, row["qualified_name"]].append(row["graph_id"])
                self.parents[f.path, row["qualified_name"]] = row.get("parent_function") or ""
        self.module_cache = {}

    def _file(self, base: str, language: str) -> str | None:
        base = posixpath.normpath(base)
        if base in self.paths:
            return base
        if language == "python":
            choices = [base + ".py", base + "/__init__.py"]
        elif language == "rust":
            choices = [base + ".rs", base + "/mod.rs"]
        else:
            stem, ext = posixpath.splitext(base)
            if ext in {".js", ".mjs", ".cjs"}:
                base = stem
            choices = [base + ext for ext in (".ts", ".tsx", ".js", ".jsx", ".mts", ".cts")]
            choices += [base + "/index" + ext for ext in (".ts", ".tsx", ".js", ".jsx")]
        matches = [p for p in choices if p in self.paths]
        return matches[0] if len(matches) == 1 else None

    def module(self, f: Facts, module: str) -> str | None:
        key = f.path, module
        if key in self.module_cache:
            return self.module_cache[key]
        parent = posixpath.dirname(f.path)
        result = None
        if f.language == "python":
            if module.startswith("."):
                level = len(module) - len(module.lstrip("."))
                for _ in range(level - 1):
                    parent = posixpath.dirname(parent)
                result = self._file(
                    posixpath.join(parent, module[level:].replace(".", "/")), "python"
                )
            else:
                # Search lexical project roots outward, never unrelated packages by basename.
                while True:
                    result = self._file(posixpath.join(parent, module.replace(".", "/")), "python")
                    if result or not parent:
                        break
                    parent = posixpath.dirname(parent)
        elif f.language == "rust":
            parts = f.path.split("/")
            if "src" in parts:
                index = len(parts) - 1 - parts[::-1].index("src")
                crate = "/".join(parts[: index + 1])
            else:
                crate = parent
            current = (
                parent
                if posixpath.basename(f.path) in {"lib.rs", "main.rs", "mod.rs"}
                else f.path[:-3]
            )
            names = module.split("::")
            if names[0] == "crate":
                base, names = crate, names[1:]
            elif names[0] in {"self", "super"}:
                base = current
                while names and names[0] in {"self", "super"}:
                    if names.pop(0) == "super":
                        base = posixpath.dirname(base)
            else:
                base = crate
            result = self._file(posixpath.join(base, *names), "rust")
        elif module.startswith("."):
            result = self._file(posixpath.join(parent, module), f.language)
        self.module_cache[key] = result
        return result

    def symbol(self, path: str, name: str) -> str | None:
        candidates = self.symbols[path, name]
        return candidates[0] if len(candidates) == 1 else None

    def call(self, f: Facts, call: dict) -> str | None:
        reference = call["reference"].replace("::", ".")
        if not reference or not all(part.isidentifier() for part in reference.split(".")):
            return None
        first = reference.split(".")[0]
        scope = call["scope"]
        lexical = scope
        while lexical:
            owners = self.symbols[f.path, lexical]
            if first not in {"self", "this", "Self"} and any(
                first in f.shadowed.get(owner, ()) for owner in owners
            ):
                return None
            lexical = self.parents.get((f.path, lexical), "")
        if first in {"self", "this", "Self"}:
            container = scope.rpartition(".")[0]
            return self.symbol(f.path, container + "." + reference.partition(".")[2])
        lexical = scope
        while True:
            candidate = self.symbol(f.path, (lexical + "." if lexical else "") + reference)
            if candidate:
                return candidate
            if not lexical:
                break
            lexical = (
                lexical.rpartition(".")[0]
                if f.language in {"java", "cpp"}
                else self.parents.get((f.path, lexical), "")
            )
        for imported in f.imports:
            alias = imported["alias"]
            if alias and (reference == alias or reference.startswith(alias + ".")):
                target = self.module(f, imported["module"])
                suffix = reference[len(alias) :].lstrip(".")
                name = ".".join(x for x in (imported["name"], suffix) if x)
                if target and name and name != "default":
                    return self.symbol(target, name)
        if f.language == "rust" and "::" in call["reference"]:
            module, _, name = call["reference"].rpartition("::")
            target = self.module(f, module)
            if target:
                return self.symbol(target, name)
        return None


def structural_metrics(graph: nx.DiGraph) -> tuple[dict, dict]:
    """SCC condensation + bitsets give exact reach without recursive DFS per node."""
    if not graph:
        return {}, {"nodes": 0, "edges": 0, "cycles": [], "betweenness_samples": 0}
    components = list(nx.strongly_connected_components(graph))
    dag = nx.condensation(graph, components)
    mapping = dag.graph["mapping"]
    members = defaultdict(int)
    for index, node in enumerate(graph):
        members[mapping[node]] |= 1 << index
    upstream = dict(members)
    for component in nx.topological_sort(dag):
        for child in dag.successors(component):
            upstream[child] |= upstream[component]
    samples = min(64, len(graph))
    between = nx.betweenness_centrality(
        graph, k=samples if samples < len(graph) else None, normalized=True, seed=0
    )
    cycles = sorted([sorted(c) for c in components if len(c) > 1], key=lambda c: (-len(c), c))
    result = {}
    max_between = max(between.values(), default=0)
    for node in graph:
        component = mapping[node]
        size = len(components[component])
        reach = upstream[component].bit_count() - 1
        brokerage = between[node] / max_between if max_between else 0.0
        result[node] = {
            "fan_in": len(set(graph.predecessors(node)) - {node}),
            "fan_out": len(set(graph.successors(node)) - {node}),
            "dependent_count": reach,
            "betweenness": round(between[node], 8),
            "cycle_size": size if size > 1 else 0,
            "recursive": graph.has_edge(node, node),
            "impact_weight": 1.0 + log2(1 + reach) + 2 * brokerage + log2(size),
        }
    return result, {
        "nodes": len(graph),
        "edges": graph.number_of_edges(),
        "cycles": cycles,
        "betweenness_samples": samples,
    }


def _rank(graph, rows, reduction):
    measurements, summary = structural_metrics(graph)
    weight_sum = sum(item["impact_weight"] for item in measurements.values())
    risk_sum = 0.0
    for node, row in rows.items():
        data = measurements[node]
        risk = row["risk_score"]
        risk_sum += risk * data["impact_weight"]
        gain = risk * reduction * data["impact_weight"] / weight_sum if weight_sum else 0.0
        row.update(data)
        row["impact_weight"] = round(data["impact_weight"], 6)
        row["refactor_priority"] = round(risk * data["impact_weight"], 6)
        row["scenario_gain"] = round(gain, 8)
        reasons = [
            f"Risk {risk:.1f}/100; {data['fan_in']} direct dependents; "
            f"{data['dependent_count']} transitive dependents"
        ]
        actions = []
        if data["cycle_size"]:
            actions.append(
                {
                    "kind": "break-cycle",
                    "line": row["line"],
                    "reason": f"Review boundaries in this {data['cycle_size']}-node cycle",
                }
            )
        hotspots = sorted(
            row.get("structure_hotspots", []),
            key=lambda item: (-item.get("subtree_branches", 0), item["line"]),
        )
        if hotspots:
            branch = hotspots[0]
            actions.append(
                {
                    "kind": "simplify-control-flow",
                    "line": branch["line"],
                    "end_line": branch["end_line"],
                    "reason": "Extract a cohesive decision branch or flatten nested conditions",
                }
            )
        if data["fan_in"]:
            actions.append(
                {
                    "kind": "stabilize-shared-interface",
                    "line": row["line"],
                    "reason": "Simplify this shared implementation while preserving its contract",
                }
            )
        row["refactor_reasons"] = reasons
        row["refactor_actions"] = actions
    summary["quality_score"] = round(100 - risk_sum / weight_sum, 6) if weight_sum else None
    return summary


def build_graph(facts: list[Facts], files: list[dict], functions: list[dict], reduction=0.25):
    resolver = Resolver(facts)
    function_rows = {row["graph_id"]: row for f in facts for row in f.symbols}
    file_rows = {f"file:{row['path']}": row for row in files if row["language"] in CALL_LANGUAGES}
    calls, modules = nx.DiGraph(), nx.DiGraph()
    calls.add_nodes_from(function_rows)
    modules.add_nodes_from(file_rows)
    edges, unresolved = [], []
    counts = Counter()
    for f in facts:
        for imported in f.imports:
            target = resolver.module(f, imported["module"])
            if target and target != f.path and f"file:{target}" in file_rows:
                modules.add_edge(f"file:{f.path}", f"file:{target}")
            counts["imports_resolved" if target else "imports_unresolved"] += 1
        for call in f.calls:
            target = resolver.call(f, call)
            if target is None:
                unresolved.append(
                    {key: call[key] for key in ("source", "path", "line", "column", "reference")}
                )
                continue
            counts["resolved_calls"] += 1
            edge = {key: call[key] for key in ("source", "path", "line", "column")}
            edge.update(target=target, resolution="static-lexical")
            edges.append(edge)
            if call["source"] in function_rows:
                calls.add_edge(call["source"], target)
            target_path = function_rows[target]["path"]
            if target_path != f.path:
                modules.add_edge(f"file:{f.path}", f"file:{target_path}")
    incoming, outgoing = defaultdict(list), defaultdict(list)
    for edge in edges:
        incoming[edge["target"]].append(edge)
        outgoing[edge["source"]].append(edge)
    unresolved_counts = Counter(item["source"] for item in unresolved)
    for node, row in function_rows.items():
        row["callers"] = incoming[node]
        row["callees"] = outgoing[node]
        row["unresolved_calls"] = unresolved_counts[node]
        row["graph_status"] = "partial-static"
    function_summary = _rank(calls, function_rows, reduction)
    module_summary = _rank(modules, file_rows, reduction)
    for row in files:
        row["graph_id"] = f"file:{row['path']}"
        row["graph_status"] = (
            "partial-static" if row["language"] in CALL_LANGUAGES else "unsupported"
        )
        if row["graph_status"] == "unsupported":
            row.update(
                refactor_priority=None,
                scenario_gain=None,
                fan_in=None,
                fan_out=None,
                dependent_count=None,
            )
    for row in functions:
        if "refactor_priority" not in row:
            row.update(
                graph_status="unsupported",
                refactor_priority=None,
                scenario_gain=None,
                fan_in=None,
                fan_out=None,
                dependent_count=None,
                callers=[],
                callees=[],
            )
    nodes = [
        {"id": node, **{key: row[key] for key in ("path", "name", "line", "end_line", "language")}}
        for node, row in function_rows.items()
    ]
    return {
        "profile": PROFILE,
        "direction": "caller -> callee / importer -> dependency",
        "functions": function_summary,
        "modules": module_summary,
        "nodes": nodes,
        "edges": edges,
        "module_edges": [{"source": a, "target": b} for a, b in sorted(modules.edges)],
        "unresolved": unresolved,
        "coverage": {
            **dict(counts),
            "call_sites": len(edges) + len(unresolved),
            "unresolved_calls": len(unresolved),
            "call_languages": sorted(CALL_LANGUAGES),
            "import_languages": sorted(IMPORT_LANGUAGES),
            "functions_analyzed": len(function_rows),
            "functions_unsupported": len(functions) - len(function_rows),
        },
        "scenario": {
            "risk_reduction": reduction,
            "quality_formula": "100 - sum(impact_weight * risk) / sum(impact_weight)",
            "weight_formula": (
                "1 + log2(1 + transitive_dependents) + 2 * relative_betweenness + log2(SCC_size)"
            ),
            "gain_formula": "risk * risk_reduction * impact_weight / sum(impact_weight)",
            "assumptions": (
                "One candidate changes; graph and all other risks stay fixed. "
                "File and function scenarios are separate."
            ),
        },
        "limitations": [
            "Static evidence is incomplete: dynamic dispatch, callbacks, reflection and macros "
            "are unresolved.",
            "Import resolution covers relative JS/TS, lexical Python modules and local Rust "
            "modules; package aliases and re-exports are unresolved.",
            "Refactor gains are hypothetical graph-weighted score changes, not predicted "
            "defects removed or measured improvements.",
            "Betweenness uses up to 64 deterministic samples; reach and SCC membership "
            "are exact for resolved edges.",
        ],
    }
