from walleye.javascript import parse


def _tree(source):
    root, errors = parse(source.encode("utf-8"), "javascript", "fixture.js")
    assert errors == []
    assert root is not None
    return root


def _walk(node):
    yield node
    for child in node.children:
        yield from _walk(child)


def _find(root, node_type):
    return next(node for node in _walk(root) if node.type == node_type)


def _assert_empty_parameter_group(function):
    parameters = function.child_by_field_name("parameters")
    assert parameters is not None
    assert parameters.type == "formal_parameters"
    assert parameters.named_children == []
    assert parameters.text == b"()"
    assert [child.text for child in parameters.children] == [b"(", b")"]
    assert all(child.parent is parameters for child in parameters.children)


def test_zero_parameter_function_exposes_empty_formal_parameters():
    root = _tree("function f() {}")
    _assert_empty_parameter_group(_find(root, "function_declaration"))


def test_zero_parameter_arrow_exposes_empty_formal_parameters():
    root = _tree("() => 1")
    _assert_empty_parameter_group(_find(root, "arrow_function"))


def test_zero_parameter_class_method_exposes_empty_formal_parameters():
    root = _tree("class A { method() {} }")
    _assert_empty_parameter_group(_find(root, "method_definition"))


def test_non_empty_parameters_keep_formal_parameter_identifier():
    root = _tree("function f(value) {}")
    parameters = _find(root, "function_declaration").child_by_field_name("parameters")
    assert parameters is not None
    assert parameters.type == "formal_parameters"
    assert [child.type for child in parameters.named_children] == ["identifier"]
    assert parameters.named_children[0].text == b"value"
