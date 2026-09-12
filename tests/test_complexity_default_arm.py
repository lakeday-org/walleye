from walleye.complexity import _counts_as_decision, _is_default_arm
from walleye.scanner import parser_for


def _nodes_of_type(source: str, language: str, node_type: str):
    root = parser_for(language).parse(source.encode()).root_node

    def visit(node):
        matches = [node] if node.type == node_type else []
        for child in node.children:
            matches.extend(visit(child))
        return matches

    return visit(root)


def test_non_wildcard_match_arm_starting_with_default_is_a_decision():
    arms = _nodes_of_type(
        """fn classify(value: Option<i32>) -> i32 {
    match value {
        default_value @ Some(_) => 1,
        _ => 0,
    }
}
""",
        "rust",
        "match_arm",
    )

    assert len(arms) == 2
    assert _is_default_arm(arms[0]) is False
    assert _counts_as_decision(arms[0]) is True


def test_wildcard_match_arm_remains_a_default():
    arms = _nodes_of_type(
        """fn classify(value: Option<i32>) -> i32 {
    match value {
        _ => 0,
    }
}
""",
        "rust",
        "match_arm",
    )

    assert len(arms) == 1
    assert _is_default_arm(arms[0]) is True
    assert _counts_as_decision(arms[0]) is False


def test_switch_default_remains_a_default():
    defaults = _nodes_of_type(
        """function classify(value) {
    switch (value) {
        default:
            return 0;
    }
}
""",
        "javascript",
        "switch_default",
    )

    assert len(defaults) == 1
    assert _is_default_arm(defaults[0]) is True
    assert _counts_as_decision(defaults[0]) is False
