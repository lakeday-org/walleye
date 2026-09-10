"""Offline Babel fallback for valid JS/TS syntax absent from Tree-sitter grammars.

Only the bundled parser runs inside QuickJS; scanned source is passed as data.
The adapter retains Babel's complete AST and token stream, with byte positions
and the small node interface consumed by our metrics/control-flow visitors.
"""

import json
import re
from bisect import bisect_right
from functools import lru_cache
from pathlib import Path
from types import SimpleNamespace

import quickjs

PROFILE = "babel-lexical-v1"
VERSION = "7.28.4"
NODE_TYPES = {
    "Program": "program",
    "Identifier": "identifier",
    "PrivateName": "private_property_identifier",
    "FunctionDeclaration": "function_declaration",
    "FunctionExpression": "function_expression",
    "ArrowFunctionExpression": "arrow_function",
    "ClassMethod": "method_definition",
    "ClassPrivateMethod": "method_definition",
    "ObjectMethod": "method_definition",
    "ClassDeclaration": "class_declaration",
    "ClassExpression": "class",
    "VariableDeclarator": "variable_declarator",
    "BlockStatement": "statement_block",
    "CallExpression": "call_expression",
    "OptionalCallExpression": "call_expression",
    "MemberExpression": "member_expression",
    "OptionalMemberExpression": "member_expression",
    "ThisExpression": "this",
    "BinaryExpression": "binary_expression",
    "LogicalExpression": "binary_expression",
    "ConditionalExpression": "ternary_expression",
    "StringLiteral": "string",
    "NumericLiteral": "number",
    "RegExpLiteral": "regex",
    "BooleanLiteral": "boolean",
    "NullLiteral": "null",
    "TemplateLiteral": "template_string",
    "TemplateElement": "string_fragment",
    "JSXText": "jsx_text",
    "CommentLine": "comment",
    "CommentBlock": "comment",
    "ImportDefaultSpecifier": "import_default_specifier",
    "ImportDeclaration": "import_statement",
    "ImportSpecifier": "import_specifier",
    "ExportNamedDeclaration": "export_statement",
    "ExportAllDeclaration": "export_statement",
    "ImportNamespaceSpecifier": "namespace_import",
    "ObjectProperty": "pair",
    "IfStatement": "if_statement",
    "ForStatement": "for_statement",
    "ForOfStatement": "for_in_statement",
    "ForInStatement": "for_in_statement",
    "WhileStatement": "while_statement",
    "DoWhileStatement": "do_statement",
    "TryStatement": "try_statement",
    "CatchClause": "catch_clause",
    "SwitchStatement": "switch_statement",
    "SwitchCase": "switch_case",
}
FIELDS = {
    "id": "name",
    "key": "name",
    "callee": "function",
    "init": "value",
    "params": "parameters",
    "imported": "name",
    "local": "alias",
}


@lru_cache(maxsize=1)
def _runtime():
    context = quickjs.Context()
    context.set_memory_limit(256 * 1024 * 1024)
    context.set_max_stack_size(8 * 1024 * 1024)
    context.eval(
        "var exports = {};\n"
        + Path(__file__).with_name("vendor").joinpath("babel-parser.cjs").read_text()
    )
    context.eval("""
        function declankParse(source, language) {
          try {
            const plugins = ["decorators-legacy", "importAttributes"];
            if (language !== "javascript") plugins.push("typescript");
            if (language !== "typescript") plugins.push("jsx");
            const tree = exports.parse(source, {
              sourceType: "unambiguous", plugins, tokens: true, attachComment: false,
              createParenthesizedExpressions: true, errorRecovery: false
            });
            return JSON.stringify({tree});
          } catch (error) {
            return JSON.stringify({error: {
              message: error.reasonCode || "JavaScript/TypeScript parse error",
              line: error.loc ? error.loc.line : 1,
              column: error.loc ? error.loc.column + 1 : 1
            }});
          }
        }
    """)
    return context, context.get("declankParse")


class Node:
    """Adapter, not a Tree-sitter tree or a rewritten copy of the source."""

    def __init__(self, kind, start, end, source, line_starts, *, named=True):
        self.type = kind
        self.start_byte, self.end_byte = start, end
        self.text = source[start:end]
        self.id = id(self)
        self.is_named, self.is_missing, self.is_error, self.has_error = named, False, False, False
        start_row = bisect_right(line_starts, start) - 1
        end_row = bisect_right(line_starts, end) - 1
        self.start_point = SimpleNamespace(row=start_row, column=start - line_starts[start_row])
        self.end_point = SimpleNamespace(row=end_row, column=end - line_starts[end_row])
        self.children = []
        self.parent = None
        self.fields = {}

    @property
    def child_count(self):
        return len(self.children)

    @property
    def named_children(self):
        return [node for node in self.children if node.is_named]

    def child_by_field_name(self, name):
        return self.fields.get(name)


def parse(source: bytes, language: str, path: str):
    context, parser = _runtime()
    context.set_time_limit(10)
    try:
        result = json.loads(parser(source.decode("utf-8"), language))
    except (quickjs.JSException, MemoryError) as error:
        raise ValueError("Babel parser exceeded its execution/memory limit") from error
    if "error" in result:
        return None, [{"path": path, "kind": "parse", "parser": "babel", **result["error"]}]
    tree = result["tree"]
    # Babel's offsets count UTF-16 code units. All other adapters use UTF-8 bytes.
    offsets, byte = [0], 0
    for character in source.decode("utf-8"):
        if ord(character) > 0xFFFF:
            offsets.append(byte)
        byte += len(character.encode("utf-8"))
        offsets.append(byte)
    lines = [0] + [i + 1 for i, value in enumerate(source) if value == 10]
    tokens = []
    for token in tree["tokens"]:
        token_type = token["type"]
        label = token_type["label"] if isinstance(token_type, dict) else token_type
        if label == "eof":
            continue
        kind = {
            "name": "identifier_token",
            "num": "number",
            "string": "string",
            "jsxText": "jsx_text",
            "CommentLine": "comment",
            "CommentBlock": "comment",
        }.get(label, label)
        tokens.append(
            Node(
                kind,
                offsets[token["start"]],
                offsets[token["end"]],
                source,
                lines,
                named=label
                in {
                    "name",
                    "num",
                    "string",
                    "regexp",
                    "jsxName",
                    "jsxText",
                    "CommentLine",
                    "CommentBlock",
                },
            )
        )
    starts = [node.start_byte for node in tokens]

    def with_tokens(children, start, end):
        assembled = []
        cursor = max(0, bisect_right(starts, start) - 1)
        consumed = start
        for child in [*children, None]:
            boundary = child.start_byte if child else end
            while cursor < len(tokens) and tokens[cursor].start_byte < boundary:
                token = tokens[cursor]
                if token.start_byte >= consumed and token.end_byte <= boundary:
                    assembled.append(token)
                cursor += 1
            if child:
                assembled.append(child)
                consumed = child.end_byte
        return assembled

    def convert(raw):
        original_type = raw["type"]
        kind = NODE_TYPES.get(original_type, re.sub(r"(?<!^)(?=[A-Z])", "_", original_type).lower())
        node = Node(kind, offsets[raw["start"]], offsets[raw["end"]], source, lines)
        children = []
        for key, value in raw.items():
            values = value if isinstance(value, list) else [value]
            converted = [
                convert(item)
                for item in values
                if isinstance(item, dict) and "type" in item and "start" in item
            ]
            if original_type == "TemplateLiteral" and key == "expressions":
                wrapped = []
                for child in converted:
                    group = Node(
                        "template_substitution", child.start_byte, child.end_byte, source, lines
                    )
                    group.children = [child]
                    child.parent = group
                    wrapped.append(group)
                converted = wrapped
            if key == "params" and converted:
                group = Node(
                    "formal_parameters",
                    converted[0].start_byte,
                    converted[-1].end_byte,
                    source,
                    lines,
                )
                group.children = with_tokens(converted, group.start_byte, group.end_byte)
                for child in group.children:
                    child.parent = group
                converted = [group]
            if converted:
                node.fields[FIELDS.get(key, key)] = converted[0]
                children.extend(converted)
        children.sort(key=lambda child: (child.start_byte, -child.end_byte))
        nonoverlapping = []
        end = node.start_byte
        for child in children:
            if child.start_byte >= end:
                nonoverlapping.append(child)
                end = child.end_byte
        # Place each parser token in the deepest enclosing AST node, once.
        assembled = with_tokens(nonoverlapping, node.start_byte, node.end_byte)
        # Terminal AST literals/identifiers carry their own complete token text.
        node.children = assembled if nonoverlapping or kind == "program" else []
        if (
            not children
            and len(assembled) > 1
            and kind not in {"string", "regex", "jsx_text", "identifier", "number"}
        ):
            node.children = assembled
        for child in node.children:
            child.parent = node
        return node

    tree["program"]["start"], tree["program"]["end"] = 0, len(offsets) - 1
    try:
        root = convert(tree["program"])
    except RecursionError as error:
        raise ValueError("Babel AST exceeds adapter nesting limit") from error
    return root, []
