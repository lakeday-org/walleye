from walleye import shell
from walleye.metrics import walk
from walleye.scanner import parser_for


def test_here_string_leaf_start_point_matches_original_source():
    source = b'cat >/dev/null <<<"value"'
    root = shell.parse_compatible(source, parser_for("bash"))
    assert root is not None

    leaf = next(node for node in walk(root) if node.text == b"<<<" and node.child_count == 0)
    start = source.index(b"<<<")

    assert leaf.start_byte == start
    assert leaf.start_point.row == 0
    assert leaf.start_point.column == start


def test_parse_compatible_returns_none_when_proxy_is_unchanged():
    assert shell.parse_compatible(b"echo value", parser_for("bash")) is None
