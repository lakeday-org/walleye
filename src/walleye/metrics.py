"""Versioned, language-independent lexical Halstead counting over Tree-sitter CSTs.

This is not Radon's Python AST visitor. The derived formulas match Radon; token
classification is the explicitly documented tree-sitter-lexical-v1 convention.
"""

from collections import Counter
from dataclasses import asdict, dataclass
from math import log2

from tree_sitter import Node

PROFILE = "tree-sitter-lexical-v1"


@dataclass(frozen=True)
class Halstead:
    distinct_operators: int
    distinct_operands: int
    total_operators: int
    total_operands: int
    vocabulary: int
    length: int
    calculated_length: float
    volume: float
    difficulty: float
    effort: float
    time_seconds: float
    bugs: float

    def to_dict(self) -> dict:
        return asdict(self)


def halstead(n1: int, n2: int, N1: int, N2: int) -> Halstead:
    """Radon formulas, with x*log2(x)=0 at x=0 and D=0 when n2=0."""
    counts = (n1, n2, N1, N2)
    if any(not isinstance(n, int) or isinstance(n, bool) or n < 0 for n in counts):
        raise ValueError("Halstead counts must be nonnegative integers")
    if n1 > N1 or n2 > N2 or bool(n1) != bool(N1) or bool(n2) != bool(N2):
        raise ValueError("Distinct and total counts are inconsistent")
    vocabulary, length = n1 + n2, N1 + N2
    calculated = sum(n * log2(n) for n in (n1, n2) if n)
    volume = length * log2(vocabulary) if vocabulary else 0.0
    difficulty = n1 / 2 * N2 / n2 if n2 else 0.0
    effort = difficulty * volume
    return Halstead(
        n1,
        n2,
        N1,
        N2,
        vocabulary,
        length,
        calculated,
        volume,
        difficulty,
        effort,
        effort / 18,
        volume / 3000,
    )


# Whole static literals are operands; interpolation is visited as code.
STRING_TYPES = frozenset(
    {
        "string",
        "string_literal",
        "raw_string_literal",
        "interpreted_string_literal",
        "verbatim_string_literal",
        "character_literal",
        "char_literal",
        "char",
        "template_string",
        "regex",
        "regex_literal",
        "heredoc_body",
        "string_lit",
        "str_lit",
        "quoted_atom",
        "sigil",
    }
)
INTERPOLATION_TYPES = frozenset(
    {
        "interpolation",
        "string_interpolation",
        "template_substitution",
        "interpolation_expression",
        "embedded_expression",
        "command_substitution",
        "simple_expansion",
        "expansion",
    }
)
LITERAL_WORDS = frozenset(
    {
        "true",
        "false",
        "True",
        "False",
        "TRUE",
        "FALSE",
        "nil",
        "null",
        "None",
        "nullptr",
        "undefined",
        "NULL",
    }
)
IGNORED_TOKENS = frozenset(
    {
        ")",
        "]",
        "}",
        '"',
        "'",
        "`",
        '"""',
        "'''",
        "${",
        "#{",
        "\\",
    }
)
OPEN_PAIRS = {"(": "()", "[": "[]", "{": "{}"}
SYMBOLIC_OPERATORS = frozenset(
    {
        "+",
        "-",
        "*",
        "/",
        "%",
        "**",
        "=",
        "==",
        "===",
        "!=",
        "!==",
        "<",
        ">",
        "<=",
        ">=",
        "&&",
        "||",
        "!",
        "&",
        "|",
        "^",
        "~",
        "<<",
        ">>",
        ">>>",
        "+=",
        "-=",
        "*=",
        "/=",
        "%=",
        "++",
        "--",
        "=>",
        "->",
        "<-",
        "::",
        ":=",
        "?",
        "??",
        "?.",
        ".",
        ",",
        ";",
        ":",
        "...",
        "..",
        "..=",
        "|>",
        "<>",
    }
)

# Node kinds represent executable function scopes (never call sites or types).
FUNCTION_TYPES = frozenset(
    {
        "function_definition",
        "function_declaration",
        "function_item",
        "method_definition",
        "method_declaration",
        "method",
        "singleton_method",
        "constructor_declaration",
        "constructor_definition",
        "arrow_function",
        "function_expression",
        "generator_function",
        "generator_function_declaration",
        "lambda",
        "lambda_expression",
        "lambda_literal",
        "anonymous_function",
        "anonymous_function_expression",
        "closure_expression",
        "function_literal",
        "func_literal",
        "local_function_statement",
        "function",
        "function_expression_body",
        "procedure_definition",
        "procedure_declaration",
        "subroutine",
        "subroutine_definition",
        "subroutine_declaration",
        "function_declarator",
    }
)
# Declarators also occur inside C/C++ function_definition. Exclude those below.
DECLARATOR_TYPES = frozenset({"function_declarator"})


def is_comment(node: Node) -> bool:
    return "comment" in node.type or node.type in {"shebang", "hash_bang_line"}


def walk(node: Node):
    """Iterative traversal handles very deeply nested source without Python recursion."""
    stack = [node]
    while stack:
        item = stack.pop()
        yield item
        stack.extend(reversed(item.children))


def is_function(node: Node) -> bool:
    return node.type in FUNCTION_TYPES and node.type not in DECLARATOR_TYPES


def function_name(node: Node, source: bytes) -> str:
    name = node.child_by_field_name("name")
    if name is None:
        declarator = node.child_by_field_name("declarator")
        if declarator:
            name = next(
                (
                    n
                    for n in walk(declarator)
                    if n.type
                    in {
                        "identifier",
                        "field_identifier",
                        "operator_name",
                        "destructor_name",
                    }
                ),
                None,
            )
    if name is None and node.parent:
        parent = node.parent
        if parent.type in {
            "variable_declarator",
            "assignment",
            "assignment_expression",
            "binary_operator",
            "pair",
            "property_declaration",
            "lexical_declaration",
        }:
            for field in ("name", "left", "lhs", "key"):
                name = parent.child_by_field_name(field)
                if name is not None:
                    break
    if name is None and node.type not in {
        "arrow_function",
        "function_expression",
        "lambda",
        "lambda_expression",
        "lambda_literal",
        "anonymous_function",
        "anonymous_function_expression",
        "closure_expression",
        "function_literal",
        "func_literal",
    }:
        name = next(
            (
                n
                for n in node.named_children
                if n.type
                in {
                    "identifier",
                    "simple_identifier",
                    "variable",
                    "name",
                }
            ),
            None,
        )
    return source[name.start_byte : name.end_byte].decode("utf-8")[:160] if name else "<anonymous>"


# Lexical containers used when making a function name useful for debugging.
# These are deliberately separate from FUNCTION_TYPES: a class or namespace is
# a scope, but its body is not a callable unit.
SCOPE_TYPES = frozenset(
    {
        "class_definition",
        "class_declaration",
        "class_specifier",
        "struct_specifier",
        "interface_declaration",
        "trait_item",
        "impl_item",
        "namespace_definition",
        "internal_module",
        "module_definition",
        "object_declaration",
        "object",
    }
)
SCOPE_NAME_TYPES = frozenset(
    {
        "identifier",
        "type_identifier",
        "namespace_identifier",
        "property_identifier",
        "field_identifier",
        "simple_identifier",
    }
)


def _scope_name(node: Node, source: bytes) -> str | None:
    """Return a lexical container's declared name when the grammar exposes it."""

    name = node.child_by_field_name("name")
    if name is None and node.type == "impl_item":
        name = node.child_by_field_name("type")
    if name is None:
        name = next(
            (child for child in node.named_children if child.type in SCOPE_NAME_TYPES), None
        )
    if name is None:
        return None
    return source[name.start_byte : name.end_byte].decode("utf-8", errors="replace")[:160]


def _receiver_name(node: Node, source: bytes) -> str | None:
    receiver = node.child_by_field_name("receiver")
    if receiver is None:
        return None
    # Go's method receiver is a parameter list.  The final type identifier is
    # the stable lexical owner; pointers and generic wrappers are ignored.
    for child in reversed(list(walk(receiver))):
        if child.type in {"type_identifier", "identifier", "simple_identifier"}:
            return source[child.start_byte : child.end_byte].decode("utf-8", errors="replace")[:160]
    return None


def qualified_function_name(node: Node, source: bytes) -> str:
    """Build a deterministic lexical path such as ``Service.fetch`` or ``f.g``."""

    parts: list[str] = []
    parent = node.parent
    while parent is not None:
        if is_function(parent):
            parts.append(function_name(parent, source))
        elif parent.type in SCOPE_TYPES:
            name = _scope_name(parent, source)
            if name:
                parts.append(name)
        parent = parent.parent
    receiver = _receiver_name(node, source)
    if receiver and receiver not in parts:
        parts.append(receiver)
    parts.reverse()
    parts.append(function_name(node, source))
    return ".".join(part for part in parts if part) or "<anonymous>"


def function_parent(node: Node, source: bytes) -> str | None:
    """Return the nearest enclosing function's qualified name, if any."""

    parent = node.parent
    while parent is not None:
        if is_function(parent):
            return qualified_function_name(parent, source)
        parent = parent.parent
    return None


def function_owner(node: Node, source: bytes) -> str | None:
    """Return the outermost enclosing callable's qualified name."""

    owner = None
    parent = node.parent
    while parent is not None:
        if is_function(parent):
            owner = qualified_function_name(parent, source)
        parent = parent.parent
    return owner


def function_depth(node: Node) -> int:
    """Count enclosing callable scopes; top-level functions have depth zero."""

    depth = 0
    parent = node.parent
    while parent is not None:
        if is_function(parent):
            depth += 1
        parent = parent.parent
    return depth


def _line_numbers(node: Node) -> range:
    end = node.end_point.row + (1 if node.end_point.column else 0)
    return range(node.start_point.row, max(node.start_point.row + 1, end))


@dataclass
class Measurement:
    halstead: Halstead
    sloc: int
    comment_lines: int
    opaque_bytes: int


def measure(
    root: Node,
    source: bytes,
    *,
    exclude_nested: bool = False,
    nonblank: set[int] | None = None,
    excluded_nodes: set[int] | None = None,
) -> Measurement:
    operators: Counter[bytes] = Counter()
    operands: Counter[bytes] = Counter()
    code_lines: set[int] = set()
    comments: set[int] = set()
    opaque_bytes = 0
    stack = [root]
    while stack:
        node = stack.pop()
        if excluded_nodes and node.id in excluded_nodes:
            continue
        if exclude_nested and node.id != root.id and is_function(node):
            continue
        if is_comment(node):
            comments.update(_line_numbers(node))
            continue
        if node.is_missing or not node.end_byte > node.start_byte:
            continue
        if node.type in {"raw_text", "jsx_text", "html_text"}:
            opaque_bytes += node.end_byte - node.start_byte
            continue
        if node.type in STRING_TYPES and not any(n.type in INTERPOLATION_TYPES for n in walk(node)):
            operands[source[node.start_byte : node.end_byte]] += 1
            code_lines.update(_line_numbers(node))
            continue
        if node.child_count:
            stack.extend(reversed(node.children))
            continue
        raw = source[node.start_byte : node.end_byte]
        token = raw.decode("utf-8").strip()
        if not token:
            continue
        code_lines.update(_line_numbers(node))
        if token in IGNORED_TOKENS:
            continue
        if token in OPEN_PAIRS:
            operators[OPEN_PAIRS[token].encode()] += 1
        elif token in LITERAL_WORDS:
            operands[raw] += 1
        elif (
            not node.is_named
            or token in SYMBOLIC_OPERATORS
            or node.type
            in {
                "operator",
                "binary_operator_token",
                "unary_operator_token",
            }
        ):
            operators[raw] += 1
        else:
            operands[raw] += 1
    if nonblank is None:
        nonblank = {i for i, line in enumerate(source.splitlines()) if line.strip()}
    return Measurement(
        halstead(len(operators), len(operands), operators.total(), operands.total()),
        len(code_lines & nonblank),
        len(comments),
        opaque_bytes,
    )
