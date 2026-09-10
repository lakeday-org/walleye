"""Control-flow measurements used by declank's debugging reports.

The Halstead walker is intentionally language independent.  Control flow is a
different problem: counting a node named ``if_statement`` is only meaningful if
the grammar's shape has been checked.  This module therefore has a small,
explicit adapter registry.  The core adapters cover the languages for which we
exercise the shape in the test suite; other languages report ``unsupported``
instead of presenting a zero that looks like a measurement.
"""

from dataclasses import dataclass

from tree_sitter import Node

from .metrics import is_function

# These are the grammars whose control-flow node shapes are covered by the
# adapter tests.  Keep this list conservative: a plausible node name is not
# enough to claim that a language's complexity is measured correctly.
SUPPORTED_LANGUAGES = frozenset(
    {
        "c",
        "cpp",
        "go",
        "java",
        "javascript",
        "python",
        "rust",
        "typescript",
        "tsx",
    }
)

CONTROL_NODE_TYPES = frozenset(
    {
        # Branching and loops shared by the core grammars.
        "if_statement",
        "if_expression",
        "elif_clause",
        "for_statement",
        "for_expression",
        "for_in_statement",
        "for_range_loop",
        "for_in_clause",
        "foreach_statement",
        "enhanced_for_statement",
        "while_statement",
        "while_expression",
        "do_statement",
        "repeat_statement",
        "loop_statement",
        "loop_expression",
        "try_statement",
        "catch_clause",
        "except_clause",
        "conditional_expression",
        "ternary_expression",
        "switch_statement",
        "switch_expression",
        "expression_switch_statement",
        "type_switch_statement",
        "select_statement",
        "match_expression",
        "match_statement",
        # Python, Go, Java, C and Rust alternatives.
        "case_statement",
        "type_case",
        "switch_case",
        "switch_label",
        "expression_case",
        "default_case",
        "case_clause",
        "switch_default",
        "match_arm",
        "when_entry",
        "communication_case",
        "if_clause",
    }
)

# Structural nodes make the nesting tree readable, but they do not all add a
# McCabe decision.  In particular, a try container, a switch/match container,
# and a bare Rust ``loop`` are structural.  Their catches/arms, when present,
# are the decision points.
DECISION_NODE_TYPES = frozenset(
    {
        "if_statement",
        "if_expression",
        "elif_clause",
        "for_statement",
        "for_expression",
        "for_in_statement",
        "for_range_loop",
        "for_in_clause",
        "foreach_statement",
        "enhanced_for_statement",
        "while_statement",
        "while_expression",
        "do_statement",
        "repeat_statement",
        "catch_clause",
        "except_clause",
        "conditional_expression",
        "ternary_expression",
        "if_clause",
    }
)
ARM_NODE_TYPES = frozenset(
    {
        "case_statement",
        "type_case",
        "switch_case",
        "switch_label",
        "expression_case",
        "default_case",
        "case_clause",
        "switch_default",
        "match_arm",
        "when_entry",
        "communication_case",
    }
)

LOGICAL_NODE_TYPES = frozenset(
    {
        "boolean_operator",  # Python: and/or
        "binary_expression",  # C-family, Go, Rust and JS/TS
        "binary_operator",
        "logical_expression",
        "infix_expression",
    }
)
LOGICAL_OPERATORS = frozenset({"and", "or", "&&", "||", "??"})

_BRANCH_LABELS = {
    "if_statement": "if",
    "if_expression": "if",
    "elif_clause": "elif",
    "for_statement": "for",
    "for_expression": "for",
    "for_in_statement": "for-in",
    "for_range_loop": "for-range",
    "for_in_clause": "for-in",
    "foreach_statement": "foreach",
    "enhanced_for_statement": "for-each",
    "while_statement": "while",
    "while_expression": "while",
    "do_statement": "do-while",
    "repeat_statement": "repeat",
    "loop_statement": "loop",
    "loop_expression": "loop",
    "try_statement": "try",
    "catch_clause": "catch",
    "except_clause": "except",
    "conditional_expression": "conditional",
    "ternary_expression": "ternary",
    "switch_statement": "switch",
    "switch_expression": "switch",
    "expression_switch_statement": "switch",
    "type_switch_statement": "type-switch",
    "select_statement": "select",
    "match_expression": "match",
    "match_statement": "match",
    "case_statement": "case",
    "type_case": "case",
    "switch_case": "case",
    "switch_label": "case",
    "expression_case": "case",
    "default_case": "default",
    "case_clause": "case",
    "match_arm": "arm",
    "when_entry": "when",
    "communication_case": "case",
    "if_clause": "if",
}


@dataclass(frozen=True)
class Complexity:
    """A control-flow measurement, or an explicit unsupported result."""

    status: str
    cyclomatic: int | None
    control_branches: int | None
    logical_branches: int | None
    max_nesting: int | None
    structure_hotspots: tuple[dict, ...]
    adapter: str | None = None

    def to_dict(self) -> dict:
        return {
            "complexity_status": self.status,
            "complexity_adapter": self.adapter,
            "cyclomatic_complexity": self.cyclomatic,
            "control_branch_count": self.control_branches,
            "logical_branch_count": self.logical_branches,
            "max_nesting": self.max_nesting,
            "structure_hotspots": list(self.structure_hotspots),
        }


def _operator_text(node: Node) -> str:
    operator = node.child_by_field_name("operator")
    if operator is not None:
        return operator.text.decode("utf-8", errors="replace").strip()
    # Some grammars do not expose an operator field.  Their anonymous child is
    # still unambiguous for the supported logical operators.
    for child in node.children:
        if not child.is_named:
            value = child.text.decode("utf-8", errors="replace").strip()
            if value in LOGICAL_OPERATORS:
                return value
    return ""


def _is_logical(node: Node) -> bool:
    return node.type in LOGICAL_NODE_TYPES and _operator_text(node) in LOGICAL_OPERATORS


def _is_else_if(node: Node) -> bool:
    """Treat a direct ``else if`` child as a sibling for nesting depth.

    Tree-sitter represents an else-if chain as an ``if_statement`` below an
    ``else_clause`` in C-family grammars.  The branch is still counted, but an
    else-if should not make a flat decision chain appear deeply nested.
    """

    if node.type == "elif_clause":
        return node.parent is not None and node.parent.type in {"if_statement", "elif_clause"}
    if node.type not in {"if_statement", "if_expression"}:
        return False
    # A direct child means ``else if``.  ``else { if (...) { ... } }`` has a
    # block between the else clause and the if and remains genuinely nested.
    parent = node.parent
    if parent is None:
        return False
    if parent.type == "else_clause":
        return True
    # Go stores ``else if`` directly in the parent's ``alternative`` field,
    # without an else_clause wrapper.  An if below a block is still nested.
    return parent.type in {"if_statement", "if_expression"} and (
        parent.child_by_field_name("alternative") is not None
        and parent.child_by_field_name("alternative").id == node.id
    )


def _has_match_guard(node: Node) -> bool:
    if node.type != "match_arm":
        return False
    pattern = node.child_by_field_name("pattern")
    return pattern is not None and pattern.child_by_field_name("condition") is not None


def _is_default_arm(node: Node) -> bool:
    if node.type in {"default_case", "switch_default"}:
        return True
    text = node.text.decode("utf-8", errors="replace").lstrip()
    if text.startswith("default"):
        return True
    if node.type == "match_arm":
        pattern = node.child_by_field_name("pattern")
        if pattern is not None:
            return pattern.text.decode("utf-8", errors="replace").strip() == "_"
        # Rust grammar versions have exposed the pattern as an unnamed first
        # child.  Keep the wildcard check narrow so a guarded ``_ if ...``
        # arm remains a real decision.
        before_arrow = text.split("=>", 1)[0].strip()
        return before_arrow == "_"
    return False


def _counts_as_decision(node: Node) -> bool:
    return node.type in DECISION_NODE_TYPES or (
        node.type in ARM_NODE_TYPES and not _is_default_arm(node)
    )


def _end_line(node: Node) -> int:
    return node.end_point.row + (1 if node.end_point.column else 0)


def _unsupported() -> Complexity:
    return Complexity("unsupported", None, None, None, None, (), None)


def measure_complexity(
    root: Node,
    language: str,
    *,
    exclude_nested: bool = True,
    excluded_nodes: set[int] | None = None,
) -> Complexity:
    """Measure a supported function/file subtree.

    Cyclomatic complexity follows McCabe's ``M = 1 + decisions`` form.  The
    adapter counts predicates, loops, catches and non-default switch/match
    arms as control branches; short-circuit ``and``/``or`` operators add one
    logical branch.  Try/switch/match containers, default arms and bare loops
    remain visible in the structure tree but do not add a decision.  This
    definition is intentionally explicit because language variants differ.
    """

    if language not in SUPPORTED_LANGUAGES:
        return _unsupported()

    excluded_nodes = excluded_nodes or set()

    # Each item is mutable only while this traversal accumulates descendants;
    # immutable dictionaries are emitted once the counts are complete.
    branches: list[dict] = []
    logical_count = 0
    decision_count = 0
    max_nesting = 0
    stack: list[tuple[Node, tuple[int, ...]]] = [(root, ())]
    while stack:
        node, ancestors = stack.pop()
        if node.id in excluded_nodes:
            continue
        if node is not root and exclude_nested and is_function(node):
            continue
        effective_ancestors = ancestors
        if (
            node.type in {"if_statement", "if_expression", "elif_clause"}
            and ancestors
            and _is_else_if(node)
            and branches[ancestors[-1]]["node_type"]
            in {"if_statement", "if_expression", "elif_clause"}
        ):
            effective_ancestors = ancestors[:-1]
        if node.type in CONTROL_NODE_TYPES:
            index = len(branches)
            nesting = len(effective_ancestors) + 1
            counts_as_decision = _counts_as_decision(node)
            guard_branches = int(_has_match_guard(node))
            branch_decisions = int(counts_as_decision) + guard_branches
            decision_count += branch_decisions
            item = {
                "node_type": node.type,
                "label": _BRANCH_LABELS.get(node.type, node.type),
                "line": node.start_point.row + 1,
                "end_line": max(node.start_point.row + 1, _end_line(node)),
                "nesting": nesting,
                "counts_toward_cyclomatic": counts_as_decision,
                "guard_branches": guard_branches,
                "subtree_branches": branch_decisions,
                "subtree_control_nodes": 1,
                "subtree_logical_branches": 0,
                "subtree_max_nesting": nesting,
            }
            branches.append(item)
            for parent_index in effective_ancestors:
                branches[parent_index]["subtree_branches"] += branch_decisions
                branches[parent_index]["subtree_control_nodes"] += 1
                branches[parent_index]["subtree_max_nesting"] = max(
                    branches[parent_index]["subtree_max_nesting"], nesting
                )
            max_nesting = max(max_nesting, nesting)
            next_ancestors = tuple(effective_ancestors) + (index,)
            branch_stack_for_children = next_ancestors
        else:
            branch_stack_for_children = tuple(effective_ancestors)

        if _is_logical(node):
            logical_count += 1
            for parent_index in effective_ancestors:
                branches[parent_index]["subtree_logical_branches"] += 1

        # ``branch_stack`` is path-local rather than global: use the path
        # carried in the stack item when pushing children.
        stack.extend((child, branch_stack_for_children) for child in reversed(node.children))

    control_count = decision_count
    hotspots = []
    for item in branches:
        hotspot = {
            "type": item["label"],
            "node_type": item["node_type"],
            "line": item["line"],
            "end_line": item["end_line"],
            "nesting": item["nesting"],
            "counts_toward_cyclomatic": item["counts_toward_cyclomatic"],
            "guard_branches": item["guard_branches"],
            "subtree_branches": item["subtree_branches"],
            "subtree_control_nodes": item["subtree_control_nodes"],
            "subtree_logical_branches": item["subtree_logical_branches"],
            "subtree_max_nesting": item["subtree_max_nesting"],
        }
        hotspots.append(hotspot)
    hotspots.sort(
        key=lambda item: (
            -item["subtree_branches"],
            -item["subtree_logical_branches"],
            -item["subtree_control_nodes"],
            -item["nesting"],
            item["line"],
            item["end_line"],
        )
    )
    return Complexity(
        "supported",
        1 + control_count + logical_count,
        control_count,
        logical_count,
        max_nesting,
        tuple(hotspots[:24]),
        "core-tree-sitter-v1",
    )
