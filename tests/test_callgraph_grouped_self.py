from walleye.callgraph import _imports
from walleye.scanner import parser_for


def imports_for(source: str) -> list[dict]:
    encoded = source.encode()
    root = parser_for("rust").parse(encoded).root_node
    return _imports(root, encoded, "rust")


def test_direct_import_binding_is_unchanged():
    assert imports_for("use std::io;\n") == [
        {"module": "std", "name": "io", "alias": "io", "line": 1}
    ]


def test_grouped_members_are_preserved_and_wildcards_are_omitted():
    assert imports_for("use std::io::{Read, *};\n") == [
        {"module": "std::io", "name": "Read", "alias": "Read", "line": 1}
    ]


def test_grouped_self_matches_direct_import():
    expected = [{"module": "std", "name": "io", "alias": "io", "line": 1}]

    assert imports_for("use std::io;\n") == expected
    assert imports_for("use std::io::{self};\n") == expected


def test_mixed_grouped_self_keeps_parent_binding_and_omits_wildcard():
    assert imports_for("use std::io::{self, Read, *};\n") == [
        {"module": "std", "name": "io", "alias": "io", "line": 1},
        {"module": "std::io", "name": "Read", "alias": "Read", "line": 1},
    ]


def test_nested_grouped_self_keeps_parent_binding():
    assert imports_for("use std::{io::{self}};\n") == [
        {"module": "std", "name": "io", "alias": "io", "line": 1}
    ]


def test_aliased_grouped_self_keeps_parent_binding():
    assert imports_for("use std::io::{self as io_alias};\n") == [
        {"module": "std", "name": "io", "alias": "io_alias", "line": 1}
    ]
