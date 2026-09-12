from walleye.metrics import qualified_function_name
from walleye.scanner import parser_for


def _qualified_rust_function_names(source: str) -> list[str]:
    encoded = source.encode()
    root = parser_for("rust").parse(encoded).root_node
    pending = [root]
    names = []
    while pending:
        node = pending.pop()
        if node.type == "function_item":
            names.append(qualified_function_name(node, encoded))
        pending.extend(reversed(node.named_children))
    return names


def _impl_source(*scope_names: str) -> str:
    return "\n".join(f"impl {scope_name} {{\n    fn fetch() {{}}\n}}" for scope_name in scope_names)


def test_long_scope_names_remain_distinct_in_qualified_function_names():
    prefix = "T" + "x" * 159
    scopes = (prefix + "A", prefix + "B")

    assert _qualified_rust_function_names(_impl_source(*scopes)) == [
        f"{scopes[0]}.fetch",
        f"{scopes[1]}.fetch",
    ]


def test_160_character_scope_name_is_preserved():
    scope = "T" + "x" * 159

    assert _qualified_rust_function_names(_impl_source(scope)) == [f"{scope}.fetch"]
