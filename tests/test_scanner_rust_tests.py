from walleye.metrics import walk
from walleye.scanner import _rust_test_nodes, analyze_units, parser_for


def test_rust_test_nodes_excludes_preceding_outer_attribute():
    source = b"#[allow(dead_code)]\n#[test]\nfn ignored() {}\n"
    root = parser_for("rust").parse(source).root_node
    nodes = list(walk(root))
    attributes = [node for node in nodes if node.type == "attribute_item"]
    function = next(node for node in nodes if node.type == "function_item")

    excluded = _rust_test_nodes(root, source)

    assert len(attributes) == 2
    expected_ids = {node.id for node in attributes}
    expected_ids.add(function.id)
    assert expected_ids <= excluded


def test_analyze_units_respects_include_tests_for_rust_test_items():
    source = b"#[allow(dead_code)]\n#[test]\nfn included() {}\n"

    _, functions, errors = analyze_units(
        source,
        "rust",
        "sample.rs",
        include_tests=False,
    )
    assert errors == []
    assert [record["name"] for record in functions] == []

    _, functions, errors = analyze_units(
        source,
        "rust",
        "sample.rs",
        include_tests=True,
    )
    assert errors == []
    assert [record["name"] for record in functions] == ["included"]
